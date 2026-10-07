use anyhow::{Context, Result, ensure};
use bollard::{
    Docker,
    plugin::{ContainerInspectResponse, Node},
};
use interceptor::node::Node as MyNode;
use interceptor::testnet;
use interceptor::{configs::Config as AppConfig, p2p::receive_packet};
use interceptor::{docker::ensure_docker, p2p::connect_as};
use std::net::TcpStream;

use tendermint_p2p::{secret_connection::SecretConnection, transport::Connection};
use tendermint_proto::p2p::DefaultNodeInfo;

#[tokio::main]
async fn main() -> Result<()> {
    let (config, used_file, format) = AppConfig::parse_info();
    println!("Config: {:?}", config);
    ensure!(config.num_nodes >= 4, "num nodes must >= 4");

    let image = &config.docker_image;
    let docker = Docker::connect_with_local_defaults()?; // 创建客户端对象
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

    // /tmp/cluster-id/config/node[0-4]/
    // config.toml  genesis.json  node_key.json  priv_validator_key.json

    let node0 = MyNode::new(&docker, &run_paths.testnet_config_dir.join("node0"), &image).await?;
    let node1 = MyNode::new(&docker, &run_paths.testnet_config_dir.join("node1"), &image).await?;
    let mut connection = connect_as(&node0, &node1).await?;

    println!(
        "SecretConnection + NodeInfo exchange OK, remote ID: {}",
        connection.get_ref().remote_pubkey().peer_id()
    );

    let packet = receive_packet(&mut connection)?;
    println!("received new packet: {:?}", packet);

    node0.stop(&docker).await?;
    node0.remove(&docker).await?;
    node1.stop(&docker).await?;
    node1.remove(&docker).await?;
    Ok(())
}
