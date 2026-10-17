use std::fs;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::logger::{LogSchema, LogWriter};

#[derive(Clone, Deserialize)]
#[serde[deny_unknown_fields]]
pub struct TxEntry {
    pub node: usize,
    pub after_heights: Vec<u64>,
    pub kv: String,

    #[serde(deserialize_with = "to_seconds")]
    pub dt: Duration,
}

// 对同一节点提交多个相同transaction会被拒绝，这里加一个头区分
fn tag_kv(kv: &str, id: usize, height: u64) -> String {
    format!("{kv}[{id},{height}]")
}

fn to_seconds<'d, D>(deserializer: D) -> std::result::Result<Duration, D::Error>
// 这里需要用std::Result
where
    D: serde::Deserializer<'d>,
{
    let seconds = f64::deserialize(deserializer)?;
    Duration::try_from_secs_f64(seconds).map_err(serde::de::Error::custom)
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tx {
    pub transactions: Vec<TxEntry>,
}

impl Tx {
    pub fn load(path: Option<&str>, num_nodes: usize, max_block: u64) -> Result<Self> {
        if let Some(path) = path {
            let yaml = fs::read_to_string(path)
                .with_context(|| format!("read transaction config {path}"))?;
            let mut plan: Self = config::Config::builder()
                .add_source(config::File::from_str(&yaml, config::FileFormat::Yaml))
                .build()?
                .try_deserialize()
                .with_context(|| format!("parse transaction config {path}"))?;

            // 去掉node不在num_nodes范围的，和after_height不在max_block范围的
            plan.transactions.retain_mut(
                // 这里提供一个过滤函数，只有返回true的元素保留在transactions里
                |tx| {
                    if tx.node >= num_nodes {
                        return false;
                    }
                    tx.after_heights
                        .retain(|&height| height > 0 && height < max_block);

                    tx.after_heights.sort();
                    tx.after_heights.dedup();
                    !tx.after_heights.is_empty()
                },
            );
            return Ok(plan);
        }
        Ok(Self::default())
    }
}

#[derive(Serialize)]
pub struct TxLog {
    pub node: usize,
    pub after_height: u64,
    pub kv: String,
    pub tx_hash: String,
    pub dt: f64,
    pub submitted_at: Option<String>,
    pub submitted_us: Option<u128>,
    pub completed_us: u128,
    pub check_tx_code: Option<u32>,
}
impl LogSchema for TxLog {
    const FILE: &'static str = "tx.csv";
}

// 提交transaction的response的结果
#[derive(Deserialize)]
struct CheckTxResult {
    code: u32,
    hash: String,
    #[serde(default)]
    log: String,
}

async fn submit_tx(
    client: &reqwest::Client,
    rpc_url: &str,
    kv: &str,
    expected_hash: &str,
) -> Result<CheckTxResult> {
    let response: Value = client
        .post(rpc_url) // 创建一个http POST 请求
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "broadcast_tx_sync",
            "params": {
                "tx": STANDARD.encode(kv.as_bytes())
            },
        }))
        .send()
        .await? // 等待http响应
        .error_for_status()? // 检查http状态码，200则继续处理
        .json()
        .await?;

    ensure!(
        response.get("error").is_none_or(Value::is_null),
        "RPC error: {response}"
    );

    let result: CheckTxResult = serde_json::from_value(
        response
            .get("result")
            .cloned()
            .context("RPC response has no 'result'")?,
    )?;

    ensure!(
        result.hash.eq_ignore_ascii_case(expected_hash),
        "RPC responded with unexcepted hash {}",
        result.hash
    );
    Ok(result)
}

#[derive(Clone)]
pub struct TxSubmitter {
    entries: Arc<Vec<TxEntry>>,
    client: reqwest::Client,
    writer: LogWriter<TxLog>,
    started_instant: Instant,
    tasks: TaskTracker,
    shutdown: CancellationToken,
}

impl TxSubmitter {
    pub fn new(
        tx: Tx,
        client: reqwest::Client,
        writer: LogWriter<TxLog>,
        started_instant: Instant,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            entries: Arc::new(tx.transactions),
            client,
            writer,
            started_instant,
            tasks: TaskTracker::new(),
            shutdown,
        }
    }

    pub fn on_block_update(
        &self,
        node: usize,
        height: u64,
        rpc_url: &str,
        observed_instant: Instant,
    ) -> Result<()> {
        // 当收到某个节点的block更新之后，看自己的tx中有没有匹配的提交配置，如果有则设置一个定时器提交任务
        if self.shutdown.is_cancelled() {
            return Ok(());
        }

        for (id, entry) in self.entries.iter().enumerate() {
            if entry.node != node || !entry.after_heights.contains(&height) {
                continue;
            }
            let deadline = observed_instant
                .checked_add(entry.dt)
                .context("instant + dt")?;

            let payload = tag_kv(&entry.kv, id, height);
            let dt = entry.dt;

            let rpc_url = rpc_url.to_owned();
            let client = self.client.clone();
            let writer = self.writer.clone();
            let shutdown = self.shutdown.clone();
            let started_instant = self.started_instant;

            self.tasks.spawn(async move {
                let tx_hash = hex::encode_upper(Sha256::digest(payload.as_bytes()));
                let ready = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => false,
                    _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => true,
                };

                let mut submitted_at: Option<String> = None;
                let mut submitted_us: Option<u128> = None;
                let completed_us: u128;
                let mut check_tx_code: Option<u32> = None;

                if ready && !shutdown.is_cancelled() {
                    // 睡到了block update + dt的时刻，开始提交交易
                    submitted_us = Some(started_instant.elapsed().as_micros());
                    submitted_at = Some(Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true));

                    let response = submit_tx(&client, &rpc_url, &payload, &tx_hash).await;

                    completed_us = started_instant.elapsed().as_micros();

                    check_tx_code = match response {
                        Ok(response) => {
                            println!(
                                "[tx] node={node}, after_height={height},code={}",
                                response.code
                            );
                            Some(response.code)
                        }
                        Err(err) => {
                            eprintln!(
                                "[tx] node={node}, after_height={height}, hash={tx_hash}: {err:#}"
                            );
                            None
                        }
                    };
                } else {
                    completed_us = started_instant.elapsed().as_micros();
                }

                let record = TxLog {
                    node,
                    after_height: height,
                    kv: payload,
                    tx_hash,
                    dt: dt.as_secs_f64(),
                    submitted_at,
                    submitted_us,
                    completed_us,
                    check_tx_code,
                };
                if let Err(e) = writer.log(record).await {
                    eprint!("{e}");
                }
            });
        }
        Ok(())
    }

    pub async fn wait(self) {
        let tasks = self.tasks.clone();
        drop(self);
        tasks.close();
        tasks.wait().await;
    }
}
