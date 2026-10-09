use anyhow::{Context, Result, ensure};
use bollard::Docker;
use interceptor::docker::ensure_docker;
use interceptor::p2p::connect_as;
use interceptor::p2p::send_complete_message;
use interceptor::p2p::split_connection;
use interceptor::resource_manager::{self, ResourceManager};
use interceptor::{configs::Config as AppConfig, p2p::receive_packet};
use interceptor::{message::MessageBuilder, node::Node as MyNode};
use interceptor::{p2p::send_packet, testnet};

use tendermint_proto::p2p::{Packet, PacketMsg, PacketPing, PacketPong, packet::Sum};
use tokio::signal::unix::{SignalKind, signal};
use tokio::task::JoinSet;

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

    let (config, used_file, format) = AppConfig::parse_info();
    println!("Config: {:?}", config);
    ensure!(config.num_nodes >= 4, "num nodes must >= 4");

    let image = &config.docker_image;
    let docker = Docker::connect_with_local_defaults()?; // 创建客户端对象
    let mut rm = ResourceManager::new(docker.clone());
    let auto_pull = config.auto_pull_image.unwrap_or(true);

    ensure_docker(&docker, &image, auto_pull).await?;

    let run_paths = &config.get_run_paths()?;

    println!("run paths: {:?}", run_paths);

    testnet::init_testnet_config(
        &docker,
        run_paths.testnet_config_dir.clone(),
        config.num_nodes,
        &config.docker_image,
    )
    .await?;
    // 生成：
    // /tmp/cluster-id/config/node[0-4]/
    // config.toml  genesis.json  node_key.json  priv_validator_key.json

    let mut interceptor_tasks = JoinSet::new();

    let mut run_result: Result<()> = async {
        let node0 = MyNode::new(
            &docker,
            &run_paths.testnet_config_dir.join("node0"),
            &image,
            &mut rm,
        )
        .await?;
        let node1 = MyNode::new(
            &docker,
            &run_paths.testnet_config_dir.join("node1"),
            &image,
            &mut rm,
        )
        .await?;

        let connection_with_1 = connect_as(&node0, &node1, &mut rm).await?;
        let (reader1, writer1) = split_connection(connection_with_1)?;
        let connection_with_0 = connect_as(&node1, &node0, &mut rm).await?;
        let (reader0, writer0) = split_connection(connection_with_0)?;


        // 创建多个blocking任务，将句柄保存起来，后面可以等待它们结束
        for (mut reader, mut writer, direction) in
            [(reader0, writer1, "0->1"), (reader1, writer0, "1->0")]
        {
            interceptor_tasks.spawn_blocking(move || -> Result<()> {
                let mut message_builder = MessageBuilder::new();

                loop {
                    let packet = receive_packet(&mut reader)?;

                    match packet.sum.context("packet has no payload")? {
                        Sum::PacketMsg(fragment) => {
                            if let Some(message) = message_builder.handle_packet(fragment)? {
                                // 转发给对应的writer
                                send_complete_message(&mut writer, &message)?;

                                println!(
                                    "Complete message [{direction}]: channel={:#x}, bytes={}, packets={}",
                                    message.channel_id,
                                    message.data.len(),
                                    message.packet_count,
                                );
                            }
                        }
                        // 剩下两个消息类型都匹配到这个分支：
                        pingpong => {
                            let packet = Packet {
                                sum: Some(pingpong),
                            };
                            send_packet(&mut writer, &packet)?;
                        }
                    }
                }
            });
        }

        // interceptor正常退出，收到ctrlc，收到term，这三件事都有可能发生，这里就是询问这三个句柄哪个发生了就执行哪个
        tokio::select! {
            // interceptor_tasks是一个任务集合，哪个任务先返回就被join_next()取出
            result = interceptor_tasks.join_next() => {
                let result = result.context("no interceptor task")?;
                result??; //这里如果出错，错误信息也会传导到run_result
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

    // 这里退出之后，可能是interceptor正常退出，也可能是收到了信号
    // 如果是先收到信号，interceptor_task还在跑着，这里通过执行清理socket以及容器之后，interceptor_task就会自己报错，然后就结束了
    let cleanup_result = rm.cleanup().await;
    // 等待所有interceptor任务退出
    while let Some(result) = interceptor_tasks.join_next().await {
        // 所以这里分支是通过信号杀死的
        match result {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                // 主动关闭 socket 通常会使 read_exact 返回错误。
                eprintln!("interceptor returns on exit (expected) ：{err:#}");
            }
            Err(err) => {
                eprintln!("interceptor task error：{err}");
                if run_result.is_ok() {
                    run_result = Err(err.into());
                }
            }
        }
    }

    run_result?;
    cleanup_result
}
