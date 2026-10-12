use std::time::Instant;

use anyhow::{Context, Result};
use chrono::{SecondsFormat, Utc};
use tendermint_proto::p2p::Packet;
use tendermint_proto::p2p::packet::Sum;
use tokio::task::{JoinError, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::block_monitor::{BlockLog, join_monitors, run_block_height_monitor};
use crate::logger::LogWriter;
use crate::message::{MessageBuilder, MessageLog, sha256_hex};
use crate::node::Node;
use crate::p2p::{
    connect_as, receive_packet, send_complete_message, send_packet, split_connection,
};
use crate::resource_manager::ResourceManager;

pub struct Network {
    pub nodes: Vec<Node>,
    pub interceptor_tasks: JoinSet<Result<()>>,
    pub block_monitor_tasks: JoinSet<Result<()>>,
    shutdown: CancellationToken, // 通知所有异步任务退出
    started_instant: Instant,
}

pub enum NetworkEvent {
    AllBlocksReached,
    InterceptorStopped, // interceptor_tasks结束，正常情况下会一直循环转发任务
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

    // 为每个节点订阅NewBlock事件，启动监控任务
    // 必须在connect_all之前调用，这样不会漏掉早期的区块
    pub async fn subscribe_blocks_all(
        &mut self,
        target_height: u64,
        blocks_writer: LogWriter<BlockLog>,
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
            ));
        }
        Ok(())
    }
    pub fn join_next(&mut self) -> impl Future<Output = Option<Result<Result<()>, JoinError>>> {
        self.interceptor_tasks.join_next()
    }

    pub async fn join_block_tasks(&mut self) -> Result<()> {
        // 等待所有block monitor结束
        // monitor是生产者，只有它们停了，logger作为消费者才能wait()
        join_monitors(&mut self.block_monitor_tasks, self.shutdown.clone()).await
    }

    pub async fn wait_event(&mut self) -> Result<NetworkEvent> {
        tokio::select! {
            result = join_monitors(&mut self.block_monitor_tasks, self.shutdown.clone()) => {
                result.context("block monitor failed")?;
                Ok(NetworkEvent::AllBlocksReached)
            }
            // interceptor_tasks是一个任务集合，哪个任务先返回就被join_next()取出
            result = self.interceptor_tasks.join_next() => {
                let result = result.context("No interceptor task")?;
                result??;
                Ok(NetworkEvent::InterceptorStopped)
            }
        }
    }
    // );
    pub async fn connect_all(
        &mut self,
        rm: &mut ResourceManager,
        message_writer: LogWriter<MessageLog>,
    ) -> Result<()> {
        // 创建多个blocking任务，将句柄保存起来，后面可以等待它们结束
        for i in 0..self.nodes.len() {
            for j in (i + 1)..self.nodes.len() {
                let conn_with_i = connect_as(&self.nodes[j], &self.nodes[i], rm).await?;
                let conn_with_j = connect_as(&self.nodes[i], &self.nodes[j], rm).await?;
                let (reader_i, writer_i) = split_connection(conn_with_i)?;
                let (reader_j, writer_j) = split_connection(conn_with_j)?;

                for (mut reader, mut writer, from, to) in
                    [(reader_i, writer_j, i, j), (reader_j, writer_i, j, i)]
                {
                    // spawn_blocking是move的，不能在里面使用self.xxx，否则会把self拖进去
                    let started_instant = self.started_instant;
                    let message_writer = message_writer.clone();
                    self.interceptor_tasks
                        .spawn_blocking(move || -> Result<()> {
                            let mut message_builder = MessageBuilder::new();
                            loop {
                                let packet = receive_packet(&mut reader)?;
                                match packet.sum.context("packet has no payload")? {
                                    Sum::PacketMsg(fragment) => {
                                        // 转发给对应的writer
                                        if let Some(message) =
                                            message_builder.handle_packet(fragment)?
                                        {
                                            // 拦截器收到这个消息最后一个分片的时刻
                                            let intercepted_at = Utc::now();
                                            let intercepted_us =
                                                started_instant.elapsed().as_micros();

                                            // 拦截器发送这个消息的时刻
                                            let forwarded_at = Utc::now();
                                            let forwarded_us =
                                                started_instant.elapsed().as_micros();
                                            send_complete_message(&mut writer, &message)?;

                                            // 发送日志条目到mpsc
                                            message_writer.log_blocking(MessageLog {
                                                from,
                                                to,
                                                channel_id: message.channel_id,
                                                len: message.data.len(),
                                                packets: message.packet_count,
                                                sha256: sha256_hex(&message.data),
                                                intercepted_at: intercepted_at
                                                    .to_rfc3339_opts(SecondsFormat::Micros, true),
                                                intercepted_us,
                                                forwarded_at: forwarded_at
                                                    .to_rfc3339_opts(SecondsFormat::Micros, true),
                                                forwarded_us,
                                                bytes_hex: hex::encode(&message.data),
                                            })?;
                                        }
                                    }

                                    // 剩下两个消息类型都匹配到这个分支：
                                    pingpong => {
                                        let packet = Packet {
                                            sum: Some(pingpong),
                                        };
                                        send_packet(&mut writer, &packet)?;
                                    }
                                }
                            }
                        });
                }
            }
        }
        Ok(())
    }
}
