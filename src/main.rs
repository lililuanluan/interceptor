use std::time::{Duration, Instant};

use anyhow::{Result, ensure};
use bollard::Docker;
use chrono::Utc;
use interceptor::block_monitor::BlockLog;
use interceptor::configs::Config as AppConfig;
use interceptor::docker::ensure_docker;
use interceptor::logger::Logger;
use interceptor::message::MessageLog;
use interceptor::network::{Network, NetworkEvent};
use interceptor::node::Node as MyNode;
use interceptor::resource_manager::ResourceManager;
use interceptor::testnet;
use interceptor::transaction::{Tx, TxLog, TxSubmitter};
use tokio::signal::unix::{SignalKind, signal};

// timeout 默认发送SIGTERM
// timeout -s INT 指定发送SIGINT
// Ctrl+c 发送SIGINT
// kill PID 发送 SIGTERM
// kill -9 PID SIGKILL 不能捕获
// 我这里要同时处理SIGINT和SIGTERM

#[tokio::main]
async fn main() -> Result<()> {
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;

    let (config, _used_file, _format) = AppConfig::parse_info();
    println!("Config: {:?}", config);
    ensure!(config.num_nodes >= 4, "num nodes must >= 4");

    let target_height = config.max_block as u64;
    let tx = Tx::load(
        config.tx_config_yaml.as_deref(),
        config.num_nodes as usize,
        target_height,
    )?;
    let tx_client = reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(3))
        .build()?;

    let image = &config.docker_image;
    let docker = Docker::connect_with_local_defaults()?; // 创建客户端对象
    let mut rm = ResourceManager::new(docker.clone());
    let auto_pull = config.auto_pull_image.unwrap_or(true);

    ensure_docker(&docker, image, auto_pull).await?;

    let run_paths = &config.get_run_paths()?;
    let scripts_dir = config.get_scripts_dir()?;

    println!("run paths: {:?}", run_paths);

    testnet::init_testnet_config(
        &docker,
        run_paths.testnet_config_dir.clone(),
        config.num_nodes,
        &config.docker_image,
        &scripts_dir.join("init-testnet.sh"),
    )
    .await?;
    // 生成：
    // /tmp/cluster-id/config/node[0-4]/
    // config.toml  genesis.json  node_key.json  priv_validator_key.json

    // 这三个初始化要在容器初始化之前
    let mut logger = Logger::new(&run_paths.test_log_dir)?;
    let blocks_writer = logger.writer::<BlockLog>()?;
    let message_writer = logger.writer::<MessageLog>()?;
    let tx_writer = logger.writer::<TxLog>()?;

    let mut nodes = Vec::new();
    for i in 0..config.num_nodes {
        let node = match MyNode::new(
            &docker,
            &run_paths.testnet_config_dir.join(format!("node{i}")),
            image,
            &mut rm,
            &scripts_dir.join("start-node.sh"),
        )
        .await
        {
            Ok(node) => node,
            Err(err) => {
                // 这里如果创建节点容器失败则清理并退出
                if let Err(e) = rm.cleanup().await {
                    eprintln!("cleanup failed: {e:#}");
                }
                return Err(err);
            }
        };
        nodes.push(node);
    }

    let started_at = Utc::now(); // 墙钟起点时间
    let started_instant = Instant::now(); // 单调起点：算 _us 用
    println!("run started at {started_at}");
    let mut network = Network::new(nodes, started_instant);
    let tx_submitter = TxSubmitter::new(
        tx,
        tx_client,
        tx_writer,
        started_instant,
        network.get_shutdown_token(),
    );

    let mut run_result: Result<()> = async {
        network
            .subscribe_blocks_all(target_height, blocks_writer, tx_submitter.clone())
            .await?;
        network.connect_all(&mut rm, message_writer).await?;
        // interceptor正常退出，收到ctrlc，收到term，这三件事都有可能发生，这里就是询问这三个句柄哪个发生了就执行哪个
        tokio::select! {
            event = network.wait_event() => {
                match event? {
                    NetworkEvent::AllBlocksReached => {
                        println!("all nodes reached block {target_height}, exiting");
                    }
                    NetworkEvent::InterceptorStopped => {
                        println!("an interceptor task stopped");
                    }
                }
            }
            // tokio:select 任意一个分支执行，就会直接结束继续往下走
            // 这里如果收到信号，则直接退出，所以无论是正常结束还是收到信号，都会执行后面的
            _ = sigint.recv() => {
                println!("received SIGINT");
            }
            _ = sigterm.recv()=> {
                println!("received SIGTERM");
            }
        }

        Ok(())
    }
    .await;

    network.shutdown();

    // 等待所有block monitor退出（已经shutdown了，那边应该很快退出）
    // 它们退出后将手里writer释放，这样channel就会关闭，可以让csv flush
    if let Err(err) = network.join_block_tasks().await {
        eprintln!("block monitor failed: {err:#}");

        if run_result.is_ok() {
            run_result = Err(err);
        }
    }

    tx_submitter.wait().await;
    // 这里退出之后，可能是interceptor正常退出，也可能是收到了信号
    // 如果是先收到信号，interceptor_task还在跑着，这里通过执行清理socket以及容器之后，interceptor_task就会自己报错，然后就结束了
    let cleanup_result = rm.cleanup().await;
    // 等待所有interceptor任务退出
    while let Some(result) = network.join_next().await {
        // 这些报错基本都是 shutdown + cleanup 关掉socket后 read 失败导致的
        match result {
            Ok(Ok(())) => {}
            // 主动关闭 socket 通常会使 read_exact 返回错误。
            Ok(Err(err)) if is_expected_shutdown_error(&err) => {}
            Ok(Err(err)) => {
                eprintln!("interceptor failed: {err:#}");
            }
            Err(err) => {
                eprintln!("interceptor task error：{err}");
                if run_result.is_ok() {
                    run_result = Err(err.into());
                }
            }
        }
    }

    let logger_result = logger.wait().await;

    run_result?;
    cleanup_result?;
    logger_result
}
// cleanup 主动 shutdown socket 后，阻塞中的 read/write 会返回这几类错误 —— 属于预期退出
fn is_expected_shutdown_error(err: &anyhow::Error) -> bool {
    err.chain().any(|e| {
        e.downcast_ref::<std::io::Error>().is_some_and(|io| {
            matches!(
                io.kind(),
                std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::NotConnected
            )
        })
    })
}
