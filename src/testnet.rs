use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use bollard::Docker;
use bollard::models::{ContainerCreateBody, HostConfig};
use bollard::query_parameters::{RemoveContainerOptionsBuilder, WaitContainerOptionsBuilder};
use futures_util::TryStreamExt;

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

    // Use an absolute path for the mount.
    let output_dir = fs::canonicalize(&testnet_config_dir)?;
    let output_str = output_dir.to_str().context("must be UTF8")?;

    // Run the container process with the host output directory owner's UID/GID so files
    // created through the mount can be managed on the host without sudo.
    let metadata = fs::metadata(&output_dir)?;
    let user = format!("{}:{}", metadata.uid(), metadata.gid());

    let script = std::fs::read_to_string(init_script)
        .with_context(|| format!("Failed to read script: {}", init_script.display()))?;

    let body = ContainerCreateBody {
        image: Some(image.to_owned()),
        user: Some(user),
        working_dir: Some("/tmp".into()), // Working directory inside the container.

        // The command's own CometBFT home directory.
        env: Some(vec!["CMTHOME=/tmp/init-home".into()]),
        cmd: Some(vec![
            "-c".into(),
            script,
            "testnet-init".into(), // The script's $0.
            num_nodes.to_string(), // $1
        ]),
        entrypoint: Some(vec!["/bin/sh".into()]),

        host_config: Some(HostConfig {
            // Bind the host output directory to /out inside the container.
            binds: Some(vec![format!("{output_str}:/out:rw")]),
            network_mode: Some("none".into()),

            // Remove explicitly after success rather than automatically; retain the container on failure.
            auto_remove: Some(false),
            ..Default::default()
        }),

        ..Default::default()
    };

    // Create the container that generates keys and other configuration.
    let container = docker
        .create_container(None, body)
        .await
        .context("Failed to create testnet init container")?;

    // Start the container.
    let id = container.id;
    docker
        .start_container(&id, None)
        .await
        .with_context(|| format!("Failed to start init container {id}"))?;

    // Wait for the container to exit.
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

    // The container exited successfully; remove it while retaining the generated configuration on the host.
    docker
        .remove_container(
            &id,
            Some(RemoveContainerOptionsBuilder::default().v(true).build()),
        ) // Remove anonymous volumes.
        .await
        .with_context(|| format!("failed to remove container {id}"))?;

    println!("Testnet configs generated at {}", output_dir.display());

    Ok(())
}
