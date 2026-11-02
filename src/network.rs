use std::collections::HashMap;
use std::io::Read;
use std::sync::mpsc::Sender;
use std::time::Instant;

use anyhow::{Context, Result};
use chrono::Utc;
use tendermint_proto::p2p::packet::Sum;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
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
    pub intercept_tasks: JoinSet<(usize, usize, Result<()>)>, // 两两节点之间收发消息的任务。一个节点断连退出应该继续跑测试
    pub relay_tasks: JoinSet<Result<()>>, // controller + forwarder组成的转发链路；任一退出则实验结束
    pub block_monitor_tasks: JoinSet<Result<()>>,
    shutdown: CancellationToken, // 通知所有异步任务退出
    started_instant: Instant,
}

pub enum NetworkEvent {
    AllBlocksReached,
    RelayStopped, // 正常情况下会一直循环转发任务
}

// 拦截packet，组装message，推送给controller/forwarder
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
                    // 拦截器收到这个消息最后一个分片的时刻
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

            // 剩下两个消息类型都匹配到这个分支：
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
            intercept_tasks: JoinSet::new(),
            relay_tasks: JoinSet::new(),
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

    // 为每个节点订阅NewBlock事件，启动监控任务
    // 必须在connect_all之前调用，这样不会漏掉早期的区块
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
                self.started_instant, // Instant是Copy，不用显式clone
                blocks_writer.clone(),
                self.shutdown.clone(),
                tx_submitter.clone(),
                self.nodes[id].rpc_url.clone(),
            ));
        }
        Ok(())
    }

    pub async fn join_block_tasks(&mut self) -> Result<()> {
        // 等待所有block monitor结束
        // monitor是生产者，只有它们停了，logger作为消费者才能wait()
        join_monitors(&mut self.block_monitor_tasks, self.shutdown.clone()).await
    }

    pub async fn wait_event(&mut self) -> Result<NetworkEvent> {
        let monitors = join_monitors(&mut self.block_monitor_tasks, self.shutdown.clone());
        tokio::pin!(monitors);

        loop {
            let has_intercept_tasks = !self.intercept_tasks.is_empty();
            tokio::select! {
                result = &mut monitors => {
                    result.context("block monitor failed")?;
                    return Ok(NetworkEvent::AllBlocksReached);
                }
                result = self.relay_tasks.join_next() => {
                    let result = result.context("no relay task")?;
                    result??;
                    return Ok(NetworkEvent::RelayStopped);
                }
                result = self.intercept_tasks.join_next(), if has_intercept_tasks => {
                    match result {
                        Some(Ok((from, to, Ok(())))) => println!("[direction {from}->{to}] closed"),
                        Some(Ok((from, to, Err(err)))) => {
                            eprintln!("[direction {from}->{to}] stopped: {err:#}");
                        }
                        Some(Err(err)) => eprintln!("direction task panicked: {err}"),
                        None => {}
                    }
                    continue;
                }
            }
        }
    }

    pub async fn connect_all(
        &mut self,
        rm: &mut ResourceManager,
        message_writer: LogWriter<MessageLog>,
        controller_connection: UnixStream,
    ) -> Result<()> {
        // tx=transmit rx=receive
        // tx/rx是句柄，拿到tx只能send()，拿到rx只能receive()，所以关键是谁持有它

        // 创建多个blocking任务，将句柄保存起来，后面可以等待它们结束
        // 对每一对连接，创建一个拦截任务，总体创建controller和forwarder任务，并创建它们之间的通信管道
        let (controller_tx, controller_rx) = mpsc::unbounded_channel::<Intercepted>();
        let (forwarder_tx, forwarder_rx) = std::sync::mpsc::channel::<ForwardItem>();

        // 所有方向上的writer都归forwarder持有，负责最终转发给节点
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
                    // spawn_blocking是move的，不能在里面使用self.xxx，否则会把self拖进去
                    let controller_tx = controller_tx.clone();
                    let forwarder_tx = forwarder_tx.clone();

                    // 把每个方向上的连接写端存下来给forwarder
                    writers.insert((from, to), writer);

                    self.intercept_tasks.spawn_blocking(move || {
                        (
                            from,
                            to,
                            run_intercept_task(reader, from, to, controller_tx, forwarder_tx),
                        )
                    });
                }
            }
        }

        // controller_tx没有用了，但是forwarder_tx还要给controller的run task
        let started_instant = self.started_instant;
        let shutdown = self.shutdown.clone();
        self.relay_tasks.spawn_blocking(move || {
            let mut forwarder = Forwarder::new(
                forwarder_rx,
                writers,
                started_instant,
                message_writer,
                shutdown,
            );
            forwarder.run()
        });

        let shutdown = self.shutdown.clone();
        let num_nodes = self.nodes.len();
        self.relay_tasks.spawn(async move {
            let controller = Controller::new(
                controller_rx,
                forwarder_tx,
                controller_connection,
                started_instant,
                shutdown,
                num_nodes,
            );
            controller.run().await
        });

        Ok(())
    }
}
