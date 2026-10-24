use std::time::Instant;

use anyhow::{Context, Result};
use chrono::SecondsFormat;
use serde::Serialize;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::logger::{LogSchema, LogWriter};
use crate::node::{BlockSocket, receive_block};
use crate::transaction::TxSubmitter;

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

// Run the block subscription monitoring loop for one node.
// TODO: remove the explicit target_height parameter and let an external coordinator shut down once all nodes reach the target.
pub async fn run_block_height_monitor(
    node_id: usize,
    mut socket: BlockSocket, // The socket returned by Node::subscribe_blocks.
    target_height: u64,
    started_instant: Instant, // Test start time, used to calculate elapsed time.
    blocks_writer: LogWriter<BlockLog>,
    shutdown: CancellationToken,
    tx_submitter: TxSubmitter,
    rpc_url: String,
) -> Result<()> {
    let mut last_block_height = 0;
    loop {
        // Wait for either a new block or shutdown.
        // This is asynchronous: returning drops the socket, so ResourceManager need not manage it.
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
        let observed = block_event
            .observed_instant
            .saturating_duration_since(started_instant);
        println!(
            "[node {node_id}] block {height} updated at {}.{:03}s (hash {})",
            observed.as_secs(),
            observed.subsec_millis(),
            block_event.hash.get(..8).unwrap_or(&block_event.hash)
        );

        if height > last_block_height {
            tx_submitter.on_block_update(
                node_id,
                height,
                &rpc_url,
                block_event.observed_instant,
            )?;
            last_block_height = height;
        }
        let record = BlockLog {
            node: node_id,
            height,
            hash: block_event.hash,
            block_time: block_event.block_time,
            observed_at: block_event
                .observed_at
                .to_rfc3339_opts(SecondsFormat::Micros, true),
            observed_us: observed.as_micros(),
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

// Expose an interface for waiting for all monitor tasks.
// If any subscription fails, trigger shutdown so all block monitors exit.
pub async fn join_monitors(
    tasks: &mut JoinSet<Result<()>>,
    shutdown: CancellationToken,
) -> Result<()> {
    let mut failure: Option<anyhow::Error> = None;
    while let Some(result) = tasks.join_next().await {
        let error = match result {
            Ok(Ok(())) => continue, // Normal exit after reaching the target height or receiving shutdown.
            Ok(Err(err)) => err,    // An error returned by the monitor itself.
            Err(err) => err.into(), // The task panicked or was aborted.
        };
        shutdown.cancel();

        if failure.is_none() {
            failure = Some(error); // Keep the first error.
        }
    }

    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}
