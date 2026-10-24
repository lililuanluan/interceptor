use anyhow::{Context, Result, ensure};
use bollard::Docker;
use bollard::query_parameters::CreateImageOptionsBuilder;
use futures_util::TryStreamExt;
pub async fn ensure_docker(docker: &Docker, image: &str, auto_pull: bool) -> Result<()> {
    docker.ping().await?; // Check that the Docker daemon is available.

    match docker.inspect_image(image).await {
        Ok(_) => {
            println!("Using local image {:?}", image);
            return Ok(());
        }
        Err(bollard::errors::Error::DockerResponseServerError {
            status_code: 404, ..
        }) => {
            // ensure returns immediately if the condition is false.
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

    // Pull the image.
    let options = CreateImageOptionsBuilder::default()
        .from_image(image)
        .build();

    let mut progress = docker.create_image(Some(options), None, None);

    // Consume the stream to wait for the pull to finish.
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

    // After pulling, verify that the image actually exists.
    docker
        .inspect_image(image)
        .await
        .with_context(|| format!("Pull completed, but image {image:?} cannot be inspected"))?;

    println!("Image ready: {image}");
    Ok(())
}
