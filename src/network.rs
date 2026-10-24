use std::collections::HashMap;
use std::io::Read;
use std::sync::mpsc::Sender;
use std::time::Instant;

use anyhow::{Context, Result};
use chrono::Utc;
use tendermint_proto::p2p::packet::Sum;
use tokio::sync::mpsc;
use tokio::task::{JoinError, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::block_monitor::{BlockLog, join_monitors, run_block_height_monitor};
use crate::controller::{Controller, Intercepted};
use crate::forwarder::{ForwardItem, Forwarder};
use crate::logger::LogWriter;
use crate::message::{MessageBuilder, MessageLog};
use crate::node::Node;
use crate::p2p::{connect_as, receive_packet, split_connection};
use crate::resource_manager::ResourceManager;
use crate::transaction::TxSubmitter;

pub struct Network {
    pub nodes: Vec<Node>,
    pub interceptor_tasks: JoinSet<Result<()>>,
    pub block_monitor_tasks: JoinSet<Result<()>>,
    shutdown: CancellationToken, // Notify all asynchronous tasks to exit.
    started_instant: Instant,
}

pub enum NetworkEvent {
    AllBlocksReached,
    InterceptorStopped, // An interceptor task stopped unexpectedly; normally these tasks keep forwarding.
}

// Intercept packets, assemble messages, and send them to the controller/forwarder.
fn run_intercept_task<R: Read + Send + 'static>(
    mut reader: R,
    from: usize,
    to: usize,
    to_controller: mpsc::UnboundedSender<Intercepted>,
    to_forwarder: Sender<ForwardItem>,
) -> Result<()> {
    let mut message_builder = MessageBuilder::new();
    loop {
        let packet = receive_packet(&mut reader)?;
        match packet.sum.context("packet has no payload")? {
            Sum::PacketMsg(fragment) => {
                if let Some(message) = message_builder.handle_packet(fragment)? {
                    // The moment the interceptor receives this message's final fragment.
                    let intercepted_at = Utc::now();
                    let intercepted_instant = Instant::now();

                    to_controller
                        .send(Intercepted {
                            from,
                            to,
                            channel_id: message.channel_id,
                            packets: message.packet_count,
                            data: message.data,
                            intercepted_at,
                            intercepted_instant,
                        })
                        .context("controller task")?;
                }
            }

            // Both remaining packet types match this branch.
            pingpong => {
                to_forwarder
                    .send(ForwardItem::PingPong {
                        from,
                        to,
                        sum: pingpong,
                    })
                    .context("forwarder task")?;
            }
        }
    }
}
impl Network {
    pub fn new(nodes: Vec<Node>, started_instant: Instant) -> Self {
        Self {
            nodes,
            interceptor_tasks: JoinSet::new(),
            block_monitor_tasks: JoinSet::new(),
            shutdown: CancellationToken::new(),
            started_instant,
        }
    }

    pub fn get_shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    // Subscribe to each node's NewBlock events and start its monitor task.
    // Call this before connect_all to avoid missing early blocks.
    pub async fn subscribe_blocks_all(
        &mut self,
        target_height: u64,
        blocks_writer: LogWriter<BlockLog>,
        tx_submitter: TxSubmitter,
    ) -> Result<()> {
        for id in 0..self.nodes.len() {
            let socket = self.nodes[id]
                .subscribe_blocks()
                .await
                .with_context(|| format!("node {id} subscribe block failed"))?;

            self.block_monitor_tasks.spawn(run_block_height_monitor(
                id,
                socket, // move
                target_height,
                self.started_instant, // Instant implements Copy; no explicit clone is needed.
                blocks_writer.clone(),
                self.shutdown.clone(),
                tx_submitter.clone(),
                self.nodes[id].rpc_url.clone(),
            ));
        }
        Ok(())
    }
    pub fn join_next(&mut self) -> impl Future<Output = Option<Result<Result<()>, JoinError>>> {
        self.interceptor_tasks.join_next()
    }

    pub async fn join_block_tasks(&mut self) -> Result<()> {
        // Wait for all block monitors to finish.
        // Monitors are producers; they must stop before the logger consumer can finish wait().
        join_monitors(&mut self.block_monitor_tasks, self.shutdown.clone()).await
    }

    pub async fn wait_event(&mut self) -> Result<NetworkEvent> {
        tokio::select! {
            result = join_monitors(&mut self.block_monitor_tasks, self.shutdown.clone()) => {
                result.context("block monitor failed")?;
                Ok(NetworkEvent::AllBlocksReached)
            }
            // join_next() takes whichever task in interceptor_tasks finishes first.
            result = self.interceptor_tasks.join_next() => {
                let result = result.context("No interceptor task")?;
                result??;
                Ok(NetworkEvent::InterceptorStopped)
            }
        }
    }

    pub async fn connect_all(
        &mut self,
        rm: &mut ResourceManager,
        message_writer: LogWriter<MessageLog>,
    ) -> Result<()> {
        // tx=transmit rx=receive
        // tx/rx are handles: tx can only send and rx can only receive, so ownership determines the role.

        // Create blocking tasks and keep their handles so they can be joined later.
        // Create interception tasks for each connection pair, one controller and one forwarder
        // for the network, and the channels connecting them.
        let (controller_tx, controller_rx) = mpsc::unbounded_channel::<Intercepted>();
        let (forwarder_tx, forwarder_rx) = std::sync::mpsc::channel::<ForwardItem>();

        // The forwarder owns the writers for all directions and ultimately sends messages to nodes.
        let mut writers = HashMap::new();

        for i in 0..self.nodes.len() {
            for j in (i + 1)..self.nodes.len() {
                let conn_with_i = connect_as(&self.nodes[j], &self.nodes[i], rm).await?;
                let conn_with_j = connect_as(&self.nodes[i], &self.nodes[j], rm).await?;
                let (reader_i, writer_i) = split_connection(conn_with_i)?;
                let (reader_j, writer_j) = split_connection(conn_with_j)?;

                for (reader, writer, from, to) in
                    [(reader_i, writer_j, i, j), (reader_j, writer_i, j, i)]
                {
                    // The spawn_blocking closure uses move; avoid self.xxx inside it to keep self out of the capture.
                    let controller_tx = controller_tx.clone();
                    let forwarder_tx = forwarder_tx.clone();

                    // Save the connection's write half for each direction for the forwarder.
                    writers.insert((from, to), writer);

                    self.interceptor_tasks.spawn_blocking(move || {
                        run_intercept_task(reader, from, to, controller_tx, forwarder_tx)
                    });
                }
            }
        }

        // controller_tx is no longer needed here, but forwarder_tx must go to the controller task.
        let started_instant = self.started_instant;
        let shutdown = self.shutdown.clone();
        self.interceptor_tasks.spawn_blocking(move || {
            let mut forwarder = Forwarder::new(
                forwarder_rx,
                writers,
                started_instant,
                message_writer,
                shutdown,
            );
            forwarder.run()
        });

        self.interceptor_tasks.spawn(async move {
            let mut controller = Controller::new(controller_rx, forwarder_tx);
            controller.run().await
        });

        Ok(())
    }
}
