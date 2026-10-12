use std::fs;
use std::io::{self, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use bollard::Docker;
use bollard::models::{ContainerCreateBody, HostConfig};
use bollard::query_parameters::{
    LogsOptionsBuilder, RemoveContainerOptionsBuilder, WaitContainerOptionsBuilder,
};
use futures_util::TryStreamExt;

use crate::configs::{Config, RunPaths};
use crate::filepath::*;

pub async fn init_testnet_config(
    docker: &Docker,
    testnet_config_dir: PathBuf,
    num_nodes: u32,
    image: &str,
    init_script: &Path,
) -> Result<()> {
    ensure!(num_nodes >= 4, "num nodes");

    create_shared_dir(&testnet_config_dir).context(format!(
        "create testnet config dir {}",
        testnet_config_dir.display()
    ))?;

    // 使用绝对路径mount
    let output_dir = fs::canonicalize(&testnet_config_dir)?;
    let output_str = output_dir.to_str().context("must be UTF8")?;

    // 取宿主机输出目录所有者的 UID 和 GID，提供给docker，让容器中的进程以这个 UID/GID 运行。它通过挂载目录创建的文件，可以直接管理，不必sudo
    let metadata = fs::metadata(&output_dir)?;
    let user = format!("{}:{}", metadata.uid(), metadata.gid());

    let script = std::fs::read_to_string(init_script)
        .with_context(|| format!("Failed to read script: {}", init_script.display()))?;

    let body = ContainerCreateBody {
        image: Some(image.to_owned()),
        user: Some(user),
        working_dir: Some("/tmp".into()), // 这是容器内的工作路径

        // 命令自身的home
        env: Some(vec!["CMTHOME=/tmp/init-home".into()]),
        cmd: Some(vec![
            "-c".into(),
            script,
            "testnet-init".into(), // scritp的$0
            num_nodes.to_string(), // $1
        ]),
        entrypoint: Some(vec!["/bin/sh".into()]),

        host_config: Some(HostConfig {
            // 将容器内的/out路径挂载到宿主
            binds: Some(vec![format!("{output_str}:/out:rw")]),
            network_mode: Some("none".into()),

            // 不要自动删除，而是成功时手动删除，失败则保留
            auto_remove: Some(false),
            ..Default::default()
        }),

        ..Default::default()
    };

    // 创建用于生成密钥等配置的容器
    let container = docker
        .create_container(None, body)
        .await
        .context("Failed to create testnet init container")?;

    // 启动容器
    let id = container.id;
    docker
        .start_container(&id, None)
        .await
        .with_context(|| format!("Failed to start init container {id}"))?;

    // 等待容器退出
    let exit = docker
        .wait_container(
            &id,
            Some(
                WaitContainerOptionsBuilder::default()
                    .condition("not-running")
                    .build(),
            ),
        )
        .try_next()
        .await
        .with_context(|| format!("Initializer {id} failed; inspect with: docker logs {id}"))?
        .with_context(|| format!("Initializer {id} returned no exit status"))?;

    ensure!(
        exit.status_code == 0,
        "Initializer {id} exited with status {}; inspect with: docker logs {id}",
        exit.status_code
    );

    // 到这里容器成功退出，将容器移除，但是宿主机上生成的配置文件保留
    docker
        .remove_container(
            &id,
            Some(RemoveContainerOptionsBuilder::default().v(true).build()),
        ) // 删除匿名卷
        .await
        .with_context(|| format!("failed to remove container {id}"))?;

    println!("Testnet configs generated at {}", output_dir.display());

    Ok(())
}
