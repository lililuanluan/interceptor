use std::net::TcpStream;

use crate::configs::Config as AppConfig;
use crate::docker::ensure_docker;
use anyhow::{Context, Result, ensure};
use bollard::{Docker, plugin::ContainerInspectResponse};

use interceptor::*;
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

    for i in 0..config.num_nodes {
        let key_path = run_paths
            .testnet_config_dir
            .join(format!("node{i}/node_key.json"));
        let node = p2p::NodeID::load(&key_path)?;
        // println!("node{i}: {node:?}");
    }

    let node0_dir = run_paths.testnet_config_dir.join("node0");
    let node0_identity = p2p::NodeID::load(&node0_dir.join("node_key.json"))?;

    println!("Expected node0 ID: {}", node0_identity.id);

    let node0_container_id = node::start_node(&docker, &node0_dir, &config.docker_image).await?;

    println!("Started container: {node0_container_id}");

    let node0_container_inspect: ContainerInspectResponse = docker
        .inspect_container(&node0_container_id, None)
        .await
        .context(format!("container id {node0_container_id} inspect failed"))?;

    // 获取端口映射
    // 返回option的时候，用.context转换位result，.with_context是提供闭包，而context只需要提供字符串
    let node0_port_map = node0_container_inspect
        .network_settings
        .context("Missing network_settings")?
        .ports
        .context("Missing ports")?;
    println!("{:?}", node0_port_map);

    // {"26657/tcp": Some([PortBinding { host_ip: Some("127.0.0.1"), host_port: Some("45500") }])}
    let node0_rpc_port = node0_port_map
        .get("26657/tcp") // 节点的rpc端口，26656/tcp是p2p端口
        .and_then(|bindings| bindings.as_ref())
        .and_then(|bindings| bindings.first())
        .and_then(|binding| binding.host_port.as_deref())
        .context("Missing RPC port")?;

    let node0_info = node::fetch_node_info(&format!("http://127.0.0.1:{node0_rpc_port}")).await?;
    println!("{node0_info}");

    let node0_p2p_port = node0_port_map
        .get("26656/tcp")
        .and_then(|bindings| bindings.as_ref())
        .and_then(|bindings| bindings.first())
        .and_then(|binding| binding.host_port.as_deref())
        .context("Missing p2p port")?;
    let node0_p2p_addr: std::net::SocketAddr = format!("127.0.0.1:{node0_p2p_port}").parse()?;

    // 假装自己是node1，尝试和node0建立连接
    let node1_dir = run_paths.testnet_config_dir.join("node1");
    let node1_identity = p2p::NodeID::load(&node1_dir.join("node_key.json"))?;
    let node1_rpc_port = node1_port_map
        .get("26657/tcp") // 节点的rpc端口，26656/tcp是p2p端口
        .and_then(|bindings| bindings.as_ref())
        .and_then(|bindings| bindings.first())
        .and_then(|binding| binding.host_port.as_deref())
        .context("Missing RPC port")?;
    let node1_info = node::fetch_node_info(&format!("http://127.0.0.1:{node1_rpc_port}")).await?;
    let expect_node0_id = node0_identity.id.clone();
    let received_node0_info = tokio::task::spawn_blocking(move || {
        // 等待网络和握手会阻塞线程
        let mut connection =
            p2p::make_secret_connection(node0_p2p_addr, &node1_identity, &expect_node0_id)?;

        p2p::exchanged_node_info(&mut connection, &DefaultNodeInfo::from(node1_info))?
    })
    .await??;

    println!("SecretConnection to {remote_id} OK");
    println!("received node_info from node0: {:?}", received_node0_info);

    Ok(())
}
