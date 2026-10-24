use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use bollard::Docker;
use bollard::models::{ContainerCreateBody, ContainerInspectResponse, HostConfig, PortBinding};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tendermint_proto::p2p::{DefaultNodeInfo, DefaultNodeInfoOther, ProtocolVersion};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use crate::p2p::NodeID;
use crate::resource_manager::ResourceManager;

pub struct Node {
    pub identity: NodeID,
    pub config_dir: PathBuf, // Path is dynamically sized; store an owned PathBuf.
    pub container_id: String,
    pub port_map: HashMap<String, Option<Vec<PortBinding>>>,
    pub node_info: DefaultNodeInfo,
    pub p2p_port: u16,
    pub p2p_addr: SocketAddr,
    pub rpc_url: String,
    pub rpc_port: u16,
    block_subscribed: bool,
}

impl Node {
    pub async fn subscribe_blocks(&mut self) -> Result<BlockSocket> {
        ensure!(!self.block_subscribed, "Block subscription already created");
        let response = subscribe_blocks(&self.rpc_port).await?;
        self.block_subscribed = true;
        // The block monitor owns this socket.
        Ok(response)
    }

    pub async fn stop(&self, docker: &Docker) -> Result<()> {
        docker.stop_container(&self.container_id, None).await?;
        Ok(())
    }

    pub async fn remove(&self, docker: &Docker) -> Result<()> {
        docker.remove_container(&self.container_id, None).await?;
        Ok(())
    }

    pub async fn new(
        docker: &Docker,
        config_dir: &Path,
        image: &str,
        rm: &mut ResourceManager,
        script: &Path,
    ) -> Result<Self> {
        let container_id = start_node(docker, config_dir, image, rm, script).await?;
        let node_id = NodeID::load(&config_dir.join("node_key.json"))?;
        let container_inspect: ContainerInspectResponse = docker
            .inspect_container(&container_id, None)
            .await
            .context(format!("container id {container_id} inspect failed"))?;

        // Retrieve the port mappings.
        // Convert Option to Result with context; with_context takes a closure, while context takes the message directly.
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
            .get("26657/tcp") // The node's RPC port; 26656/tcp is the P2P port.
            .and_then(|bindings| bindings.as_ref())
            .and_then(|bindings| bindings.first())
            .and_then(|binding| binding.host_port.as_deref())
            .context("Missing RPC port")?
            .parse()?;

        let rpc_url = format!("http://127.0.0.1:{rpc_port}");
        let node_info = fetch_node_info(&rpc_url).await?;
        let node_info = parse_node_info(&node_info)?;
        let p2p_addr = SocketAddr::from(([127, 0, 0, 1], p2p_port));

        Ok(Self {
            identity: node_id,
            config_dir: config_dir.to_path_buf(), // Path is dynamically sized; store an owned PathBuf.
            container_id,
            port_map,
            node_info,
            p2p_port,
            rpc_port,
            rpc_url,
            p2p_addr,
            block_subscribed: false,
        })
    }
}

struct RpcMessage {
    pub observed_at: DateTime<Utc>, // Absolute timestamp.
    // Record a monotonic observation time for each block update. Instants are used to calculate
    // differences and cannot be directly converted to or from absolute timestamps.
    pub observed_instant: Instant,
    pub value: Value,
}
async fn receive_rpc_response(socket: &mut BlockSocket) -> Result<RpcMessage> {
    // The next frame may be Ping/Pong rather than an RPC reply, or the peer may close the connection.
    loop {
        let message = socket
            .next()
            .await
            .context("Block websocket disconnected")??;

        match message {
            Message::Text(text) => {
                // Record the observation time immediately after receiving the message.
                let observed_at = Utc::now();
                let observed_instant = Instant::now();
                let response: Value = serde_json::from_str(text.as_str())?;

                if let Some(error) = response.get("error") {
                    ensure!(error.is_null(), "WebSocket RPC error: {error}");
                }
                return Ok(RpcMessage {
                    observed_at,
                    observed_instant,
                    value: response,
                });
            }
            Message::Ping(_) => {
                // Tungstenite automatically queues a Pong on receiving Ping; flush it here.
                socket.flush().await?;
            }

            Message::Close(frame) => {
                bail!("Block websocket closed: {frame:?}");
            }

            Message::Binary(_) => {
                bail!("Expected JSON format");
            }

            _ => {}
        }
    }
}

// Subscribe to a node's block event stream.
pub type BlockSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
pub async fn subscribe_blocks(rpc_port: &u16) -> Result<BlockSocket> {
    // Send a subscription request to the node and wait for confirmation.
    tokio::time::timeout(Duration::from_secs(5), async {
        let url = format!("ws://127.0.0.1:{}/websocket", rpc_port);
        let (mut socket, _) = connect_async(url.as_str()).await?;

        let request = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "subscribe",
            "params": {
                "query": "tm.event='NewBlock'"
            }
        });

        socket
            .send(Message::Text(request.to_string().into()))
            .await?;

        let response = receive_rpc_response(&mut socket).await?;
        let response = &response.value;
        // Expect a response like this:
        /*
                {
                  "jsonrpc": "2.0",
                  "id": 1,  <-- The same ID as in the request above.
                  "result": {}
                }
        */
        ensure!(
            response.get("id") == Some(&json!(1)) && response.get("result") == Some(&json!({})),
            "Unexpected subscription response: {response}"
        );
        Ok(socket)
    })
    .await
    .context("Block subscription timed out")?
}

// Receive a subscription notification for a newly published block.
#[derive(Debug)]
pub struct NewBlockEvent {
    pub height: u64,
    pub hash: String,
    pub block_time: String, // Like Ripple's close_time, block_time is part of consensus.
    pub observed_at: DateTime<Utc>, // Local receipt time of this subscription notification.
    pub observed_instant: Instant,
}
pub async fn receive_block(socket: &mut BlockSocket) -> Result<NewBlockEvent> {
    let RpcMessage {
        observed_at,
        observed_instant,
        value,
    } = receive_rpc_response(socket).await?;

    // Value::get accesses one field; Value::pointer follows a JSON path to the nested value. Both return Option.
    let value = value
        .pointer("/result/data/value")
        .context("Missing block event value")?;

    let height: u64 = value
        .pointer("/block/header/height")
        .and_then(Value::as_str) // For Some(x), and_then(f) returns f(x).
        .context("Missing block height")?
        .parse()?;

    let hash: String = value
        .pointer("/block_id/hash")
        .and_then(Value::as_str)
        .context("Missing block hash")?
        .to_string();

    let block_time = value
        .pointer("/block/header/time")
        .and_then(Value::as_str)
        .context("Missing block time")?
        .to_string();

    Ok(NewBlockEvent {
        height,
        hash,
        block_time,
        observed_at,
        observed_instant,
    })
}

// Start a node container.
pub async fn start_node(
    docker: &Docker,
    config_dir: &Path,
    image: &str,
    rm: &mut ResourceManager,
    start_script: &Path,
) -> Result<String> {
    let script = fs::read_to_string(start_script)
        .with_context(|| format!("Failed to read script: {}", start_script.display()))?;
    let config_dir = fs::canonicalize(config_dir)?;

    let config_toml_path = config_dir.join("config.toml");

    let mut config: toml::Value = fs::read_to_string(&config_toml_path)?.parse()?;
    // The address book stores peer addresses, normally under the container's config directory.
    // That directory is mounted read-only, so configure a writable location instead.
    config["p2p"]["addr_book_file"] = "/tmp/validator/data/addrbook.json".into();

    fs::write(&config_toml_path, toml::to_string_pretty(&config)?)?;

    let port_bindings = HashMap::from([
        (
            "26657/tcp".to_owned(), // Container RPC port; to_owned() creates a String for binding it to a host IP/port.
            Some(vec![PortBinding {
                host_ip: Some("127.0.0.1".into()), // Bind on the host's loopback address.
                host_port: Some(String::new()), // An empty string lets Docker allocate a port, allowing parallel clusters.
            }]),
        ),
        (
            "26656/tcp".to_owned(), // P2P port used for connections between nodes.
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
            binds: Some(vec![format!("{}:/input-config:ro", config_dir.display())]), // Mount the host config_dir at /input-config inside the container, read-only.
            port_bindings: Some(port_bindings),
            auto_remove: Some(false),
            ..Default::default()
        }),

        ..Default::default()
    };

    let container = docker.create_container(None, body).await?;
    let id = container.id;
    rm.register_container(id.clone());

    println!("Node container: {id}");
    docker.start_container(&id, None).await?;

    Ok(id)
}
pub async fn fetch_node_info(rpc_url: &str) -> Result<Value> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()?;

    let url = format!("{}/status", rpc_url.trim_end_matches('/')); // CometBFT's status endpoint, e.g. http://127.0.0.1:26657/status.

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let result: Result<Value> = async {
                let response: Value = client
                    .get(&url) // Build a GET request to this endpoint.
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
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    })
    .await
    .context("RPC not ready within 10 seconds")?
}

fn parse_node_info(info: &Value) -> Result<DefaultNodeInfo> {
    // Extract a string using a JSON path, e.g. /other/tx_index.
    let text = |path: &str| -> Result<&str> {
        info.pointer(path)
            .and_then(Value::as_str)
            .with_context(|| format!("Missing or invalid field: {path}"))
    };

    // Accept both JSON numbers and numeric strings.
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
