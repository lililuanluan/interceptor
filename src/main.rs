use crate::configs::Config as AppConfig;
use crate::docker::ensure_docker;
use anyhow::{Context, Result, ensure};
use bollard::{Docker, plugin::ContainerInspectResponse};

use interceptor::*;

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
    let identity = p2p::NodeID::load(&node0_dir.join("node_key.json"))?;

    println!("Expected node0 ID: {}", identity.id);

    let container_id = node::start_node(&docker, &node0_dir, &config.docker_image).await?;

    println!("Started container: {container_id}");

    let container_inspect: ContainerInspectResponse = docker
        .inspect_container(&container_id, None)
        .await
        .context(format!("container id {container_id} inspect failed"))?;

    // 获取端口映射
    // 返回option的时候，用.context转换位result，.with_context是提供闭包，而context只需要提供字符串
    let port_map = container_inspect
        .network_settings
        .context("Missing network_settings")?
        .ports
        .context("Missing ports")?;
    println!("{:?}", port_map);

    // {"26657/tcp": Some([PortBinding { host_ip: Some("127.0.0.1"), host_port: Some("45500") }])}
    let port = port_map
        .get("26657/tcp") // 节点的rpc端口，26656/tcp是p2p端口
        .and_then(|bindings| bindings.as_ref())
        .and_then(|bindings| bindings.first())
        .and_then(|binding| binding.host_port.as_deref())
        .context("Missing RPC port")?;

    let node_info = p2p::fetch_node_info(&format!("http://127.0.0.1:{port}")).await?;
    println!("{node_info}");

    Ok(())
}
