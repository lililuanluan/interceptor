use anyhow::Result;
use bollard::{
    Docker,
    models::{ContainerCreateBody, HostConfig, PortBinding},
};
use std::{collections::HashMap, fs, path::Path};

// 启动一个节点容器
pub async fn start_node(docker: &Docker, config_dir: &Path, image: &str) -> Result<String> {
    let script = fs::read_to_string("scripts/start-node.sh")?;
    let config_dir = fs::canonicalize(config_dir)?;

    let config_toml_path = config_dir.join("config.toml");

    let mut config: toml::Value = fs::read_to_string(&config_toml_path)?.parse()?;
    // 在容器中，addrbook是用于记录peer的地址的，默认是写在容器中的config路径的
    // 但是外部挂载将config设置为只读了，所以这里配置一个新的地址
    config["p2p"]["addr_book_file"] = "/tmp/validator/data/addrbook.json".into();

    fs::write(&config_toml_path, toml::to_string_pretty(&config)?)?;

    let port_bindings = HashMap::from([(
        "26657/tcp".to_owned(), // 容器内部的tcp 26657 端口，用于rpc，to_owned()转换为String，将其绑定到宿主的 ip:port
        Some(vec![PortBinding {
            host_ip: Some("127.0.0.1".into()), // 宿主的ip就是127.0.0.1
            host_port: Some(String::new()), // 注意！这里提供""，让docker自动分配端口，这样可以并行开多个cluster
        }]),
    )]);

    let body = ContainerCreateBody {
        image: Some(image.to_owned()),
        working_dir: Some("/tmp".into()),

        entrypoint: Some(vec!["/bin/sh".into()]),
        cmd: Some(vec!["-c".into(), script, "arg0-placeholder".into()]),

        exposed_ports: Some(vec!["26657/tcp".into()]),
        host_config: Some(HostConfig {
            binds: Some(vec![format!("{}:/input-config:ro", config_dir.display())]), // 将 config_dir（/tmp/cluster-id/config/） 映射到容器内的 /input-config/ ro只读
            port_bindings: Some(port_bindings),
            auto_remove: Some(false),
            ..Default::default()
        }),

        ..Default::default()
    };

    let container = docker.create_container(None, body).await?;
    let id = container.id;

    println!("Node container: {id}");
    docker.start_container(&id, None).await?;

    Ok(id)
}
