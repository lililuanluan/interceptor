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

// timeout sends SIGTERM by default.
// timeout -s INT sends SIGINT instead.
// Ctrl+C sends SIGINT.
// kill PID sends SIGTERM.
// kill -9 PID sends SIGKILL, which cannot be caught.
// Handle both SIGINT and SIGTERM here.

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
    let docker = Docker::connect_with_local_defaults()?; // Create the client object.
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
    // Generated files:
    // /tmp/cluster-id/config/node[0-4]/
    // config.toml  genesis.json  node_key.json  priv_validator_key.json

    // Initialize the logger and its writers before initializing node containers.
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
                // If creating a node container fails, clean up and exit.
                if let Err(e) = rm.cleanup().await {
                    eprintln!("cleanup failed: {e:#}");
                }
                return Err(err);
            }
        };
        nodes.push(node);
    }

    let started_at = Utc::now(); // Wall-clock start time.
    let started_instant = Instant::now(); // Monotonic starting point for calculating _us values.
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
        // Handle whichever happens first: an interceptor completion event, Ctrl+C, or SIGTERM.
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
            // Once any tokio::select! branch runs, continue after the select.
            // A signal also leaves the select, so cleanup below runs after either completion or a signal.
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

    // Wait for all block monitors to exit; shutdown has been requested, so they should exit promptly.
    // Exiting releases their writers, allowing channels to close and CSV output to flush.
    if let Err(err) = network.join_block_tasks().await {
        eprintln!("block monitor failed: {err:#}");

        if run_result.is_ok() {
            run_result = Err(err);
        }
    }

    tx_submitter.wait().await;
    // We may arrive here after normal interceptor completion or after a signal.
    // If a signal arrived first, interception tasks may still be running; cleaning up sockets
    // and containers makes their I/O fail so they can exit.
    let cleanup_result = rm.cleanup().await;
    // Wait for all interceptor tasks to exit.
    while let Some(result) = network.join_next().await {
        // Most errors here are read failures caused by shutdown/cleanup closing sockets.
        match result {
            Ok(Ok(())) => {}
            // Explicitly closing a socket typically makes read_exact return an error.
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
// After cleanup shuts down a socket, blocked reads/writes may return these errors as part of normal shutdown.
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
