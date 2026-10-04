mod configs;
mod filepath;
mod node;
mod p2p;
mod testnet;

use anyhow::{Context, Result, bail, ensure};
use bollard::Docker;
use bollard::query_parameters::{CreateImageOptionsBuilder, ListImagesOptionsBuilder};
use configs::Config as AppConfig;
use futures_util::TryStreamExt;
use serde_json::Value;
use std::fs;

pub async fn ensure_docker(docker: &Docker, image: &str, auto_pull: bool) -> Result<()> {
    docker.ping().await?; // 确认docker deamon可用

    match docker.inspect_image(image).await {
        Ok(_) => {
            println!("Using local image {:?}", image);
            return Ok(());
        }
        Err(bollard::errors::Error::DockerResponseServerError {
            status_code: 404, ..
        }) => {
            // ensure如果不满足条件立刻返回
            ensure!(
                auto_pull,
                "Auto-pull image disabled, image {:?} not found",
                image
            );
        }
        Err(err) => {
            return Err(err).with_context(|| format!("Failed to inspect image {image:?}"));
        }
    }

    // 拉取镜像
    let options = CreateImageOptionsBuilder::default()
        .from_image(image)
        .build();

    let mut progress = docker.create_image(Some(options), None, None);

    // 消费完stream，等待拉取完成
    while let Some(update) = progress
        .try_next()
        .await
        .with_context(|| format!("Failed to pull image {image:?}"))?
    {
        if let Some(status) = update.status {
            match update.id {
                Some(id) => println!("{id}: {status}"),
                None => println!("{status}"),
            }
        }
    }

    // 拉取完成后，确认这个镜像确实存在
    docker
        .inspect_image(image)
        .await
        .with_context(|| format!("Pull completed, but image {image:?} cannot be inspected"))?;

    println!("Image ready: {image}");
    Ok(())
}
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
    Ok(())
}
