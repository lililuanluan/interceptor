use crate::p2p::{NodeID, PeerConnection, exchanged_node_info, make_secret_connection};
use anyhow::Context;
use anyhow::Result;
use anyhow::ensure;
use bollard::models::ContainerInspectResponse;
use bollard::{
    Docker,
    models::{ContainerCreateBody, HostConfig, PortBinding},
};
use serde_json::Value;
use std::time::Duration;
use std::{
    collections::HashMap,
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
};
use tendermint_proto::p2p::{DefaultNodeInfo, DefaultNodeInfoOther, ProtocolVersion};

pub struct Node {
    pub identity: NodeID,
    pub config_dir: PathBuf, // Path不是固定大小类型
    pub container_id: String,
    pub port_map: HashMap<String, Option<Vec<PortBinding>>>,
    pub node_info: DefaultNodeInfo,
    pub p2p_port: u16,
    pub p2p_addr: SocketAddr,
    pub rpc_url: String,
    pub rpc_port: u16,
}

impl Node {
    pub async fn stop(&self, docker: &Docker) -> Result<()> {
        docker.stop_container(&self.container_id, None).await?;
        Ok(())
    }

    pub async fn remove(&self, docker: &Docker) -> Result<()> {
        docker.remove_container(&self.container_id, None).await?;
        Ok(())
    }

    pub async fn new(docker: &Docker, config_dir: &Path, image: &str) -> Result<Self> {
        let container_id = start_node(docker, config_dir, image).await?;
        let node_id = NodeID::load(&config_dir.join("node_key.json"))?;
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

        let p2p_port: u16 = port_map
            .get("26656/tcp")
            .and_then(|bindings| bindings.as_ref())
            .and_then(|bindings| bindings.first())
            .and_then(|binding| binding.host_port.as_deref())
            .context("Missing p2p port")?
            .parse()?;

        let rpc_port = port_map
            .get("26657/tcp") // 节点的rpc端口，26656/tcp是p2p端口
            .and_then(|bindings| bindings.as_ref())
            .and_then(|bindings| bindings.first())
            .and_then(|binding| binding.host_port.as_deref())
            .context("Missing RPC port")?
            .parse()?;

        let rpc_url = format!("http://127.0.0.1:{rpc_port}");
        let node_info = fetch_node_info(&rpc_url).await?;
        let node_info = parse_node_info(&node_info)?;
        let addr = SocketAddr::from(([127, 0, 0, 1], p2p_port));

        Ok(Self {
            identity: node_id,
            config_dir: config_dir.to_path_buf(), // Path不是固定大小类型
            container_id: container_id,
            port_map: port_map,
            node_info: node_info,
            p2p_port: p2p_port,
            rpc_port: rpc_port,
            rpc_url: rpc_url,
            p2p_addr: addr,
        })
    }
}

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

    let port_bindings = HashMap::from([
        (
            "26657/tcp".to_owned(), // 容器内部的tcp 26657 端口，用于rpc，to_owned()转换为String，将其绑定到宿主的 ip:port
            Some(vec![PortBinding {
                host_ip: Some("127.0.0.1".into()), // 宿主的ip就是127.0.0.1
                host_port: Some(String::new()), // 注意！这里提供""，让docker自动分配端口，这样可以并行开多个cluster
            }]),
        ),
        (
            "26656/tcp".to_owned(), // 26656为p2p端口，用于节点之间连接的
            Some(vec![PortBinding {
                host_ip: Some("127.0.0.1".into()),
                host_port: Some(String::new()),
            }]),
        ),
    ]);

    let body = ContainerCreateBody {
        image: Some(image.to_owned()),
        working_dir: Some("/tmp".into()),

        entrypoint: Some(vec!["/bin/sh".into()]),
        cmd: Some(vec!["-c".into(), script, "arg0-placeholder".into()]),

        exposed_ports: Some(vec!["26657/tcp".into(), "26656/tcp".into()]),
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
pub async fn fetch_node_info(rpc_url: &str) -> Result<Value> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()?;

    let url = format!("{}/status", rpc_url.trim_end_matches('/')); // http://127.0.0.1:26657/status CometBFT 定义的接口路径

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let result: Result<Value> = async {
                let response: Value = client
                    .get(&url) // 构造向这个接口的GET请求
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;

                response
                    .get("result")
                    .and_then(|res| res.get("node_info"))
                    .cloned()
                    .context("RPC response doesn't contain node_info")
            }
            .await;

            match result {
                Ok(info) => return Ok(info),
                Err(err) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    })
    .await
    .context("RPC not ready within 10 seconds")?
}

fn parse_node_info(info: &Value) -> Result<DefaultNodeInfo> {
    // 用 JSON 路径提取字符串，例如 /other/tx_index。
    let text = |path: &str| -> Result<&str> {
        info.pointer(path)
            .and_then(Value::as_str)
            .with_context(|| format!("Missing or invalid field: {path}"))
    };

    // 同时接受 JSON 数字和数字字符串。
    let number = |path: &str| -> Result<u64> {
        if let Some(n) = info.pointer(path).and_then(Value::as_u64) {
            return Ok(n);
        }

        Ok(text(path)?.parse()?)
    };

    Ok(DefaultNodeInfo {
        default_node_id: text("/id")?.to_owned(),
        listen_addr: text("/listen_addr")?.to_owned(),
        network: text("/network")?.to_owned(),
        version: text("/version")?.to_owned(),
        moniker: text("/moniker")?.to_owned(),

        channels: hex::decode(text("/channels")?)?,

        protocol_version: Some(ProtocolVersion {
            p2p: number("/protocol_version/p2p")?,
            block: number("/protocol_version/block")?,
            app: number("/protocol_version/app")?,
        }),

        other: Some(DefaultNodeInfoOther {
            tx_index: text("/other/tx_index")?.to_owned(),
            rpc_address: text("/other/rpc_address")?.to_owned(),
        }),
    })
}
