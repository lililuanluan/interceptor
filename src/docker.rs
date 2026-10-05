use anyhow::{Context, Result, ensure};
use bollard::{Docker, query_parameters::CreateImageOptionsBuilder};
use futures_util::TryStreamExt;
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
