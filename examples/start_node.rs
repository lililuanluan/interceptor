//! Generate a testnet, load its P2P identities, and start only node0.
//! This example intentionally leaves node0 running for further experiments.

use anyhow::{Result, ensure};
use bollard::Docker;
use chrono::Local;
use interceptor::{configs::Config, docker::ensure_docker, node, p2p, testnet};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    // Existing helpers read scripts/ relative to the working directory.
    std::env::set_current_dir(env!("CARGO_MANIFEST_DIR"))?;
    let (mut config, _used_file, _format) = Config::parse_info();
    // Avoid same-second collisions between parallel invocations of this example.
    if config.cluster_id.is_none() {
        config.cluster_id = Some(format!(
            "{}-{}",
            Local::now().format("%Y-%m-%d-%H-%M-%S-%f"),
            std::process::id()
        ));
    }
    println!("Config: {config:?}");
    ensure!(config.num_nodes >= 4, "num nodes must >= 4");

    let docker = Docker::connect_with_local_defaults()?;
    ensure_docker(
        &docker,
        &config.docker_image,
        config.auto_pull_image.unwrap_or(true),
    )
    .await?;
    let run_paths = config.get_run_paths()?;
    println!("run paths: {run_paths:?}");

    testnet::init_testnet_config(
        &docker,
        run_paths.testnet_config_dir.clone(),
        config.num_nodes,
        &config.docker_image,
    )
    .await?;

    for i in 0..config.num_nodes {
        let key_path = run_paths
            .testnet_config_dir
            .join(format!("node{i}/node_key.json"));
        let identity = p2p::NodeID::load(&key_path)?;
        println!("node{i} ID: {}", identity.id);
    }
    let node0_dir = run_paths.testnet_config_dir.join("node0");
    let identity = p2p::NodeID::load(&node0_dir.join("node_key.json"))?;
    println!("Expected node0 ID: {}", identity.id);
    let container_id = node::start_node(&docker, &node0_dir, &config.docker_image).await?;
    println!("Started container: {container_id}");

    // Stable single-line output for automation. Never include private keys.
    println!(
        "EXAMPLE_RESULT {}",
        json!({
            "example": "start-node",
            "container_id": container_id,
            "node_id": identity.id,
            "config_dir": run_paths.testnet_config_dir,
        })
    );
    Ok(())
}
