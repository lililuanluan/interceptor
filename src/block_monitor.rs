use crate::logger::{LogSchema, LogWriter};
use crate::node::{BlockSocket, receive_block};
use anyhow::{Context, Result};
use chrono::SecondsFormat;
use serde::Serialize;
use std::time::Instant;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

#[derive(Serialize)]
pub struct BlockLog {
    pub node: usize,
    pub height: u64,
    pub hash: String,
    pub block_time: String,
    pub observed_at: String,
    pub observed_us: u128,
}

impl LogSchema for BlockLog {
    const FILE: &'static str = "blocks.csv";
}

// 跑一个节点的block subscription的监控循环
// TODO：这里显式提供target_height，以后应该将这部分移除，让外部控制当所有节点都到达target height之后shutdown
pub async fn run_block_height_monitor(
    node_id: usize,
    mut socket: BlockSocket, // Node::subscribe_blocks返回的socket
    target_height: u64,
    started_instant: Instant, // 测试的启动时间，用于计算elapsed
    blocks_writer: LogWriter<BlockLog>,
    shutdown: CancellationToken,
) -> Result<()> {
    loop {
        // 同时等待新的出块以及shutdown
        // 这里是异步的，所以返回时socket随即被释放，不需要resource manager管理
        let block_event = tokio::select! {
            _ = shutdown.cancelled() => {
                println!("[node {node_id}] block monitor stopped");
                return Ok(());
            }
            result = receive_block(&mut socket) => {
                result.with_context(|| format!("node {node_id}: receive block failed"))?
            }
        };

        let height = block_event.height;
        let record = BlockLog {
            node: node_id,
            height,
            hash: block_event.hash,
            block_time: block_event.block_time,
            observed_at: block_event
                .observed_at
                .to_rfc3339_opts(SecondsFormat::Micros, true),
            observed_us: block_event
                .observed_instant
                .saturating_duration_since(started_instant)
                .as_micros(),
        };

        blocks_writer
            .log(record)
            .await
            .with_context(|| format!("node {node_id}: submit block record failed"))?;

        if height >= target_height {
            println!("[node {node_id}] reached target height {target_height}, stop monitoring");
            return Ok(());
        }
    }
}

// 对外部提供一个等待所有monitor任务的接口
// 如果任意一个订阅连接失败，则shutdown，让所有block monitor任务退出
pub async fn join_monitors(
    tasks: &mut JoinSet<Result<()>>,
    shutdown: CancellationToken,
) -> Result<()> {
    let mut failure: Option<anyhow::Error> = None;
    while let Some(result) = tasks.join_next().await {
        let error = match result {
            Ok(Ok(())) => continue, // 正常达到目标高度或shutdown的退出
            Ok(Err(err)) => err,    // 监控任务自己的报错
            Err(err) => err.into(), // 任务panic或abort
        };
        shutdown.cancel();

        if failure.is_none() {
            failure = Some(error); // 留第一个错误
        }
    }

    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}
