use anyhow::{Context, Result};
use tendermint_proto::p2p::{Packet, packet::Sum};
use tokio::task::JoinError;
use tokio::task::JoinSet;

use crate::{
    message::MessageBuilder,
    node::Node,
    p2p::{connect_as, receive_packet, send_complete_message, send_packet, split_connection},
    resource_manager::ResourceManager,
};

pub struct Network {
    pub nodes: Vec<Node>,
    pub interceptor_tasks: JoinSet<Result<()>>,
}

impl Network {
    pub fn new(nodes: Vec<Node>) -> Self {
        Self {
            nodes: nodes,
            interceptor_tasks: JoinSet::new(),
        }
    }

    pub fn join_next(&mut self) -> impl Future<Output = Option<Result<Result<()>, JoinError>>> {
        return self.interceptor_tasks.join_next();
    }

    pub async fn connect_all(&mut self, rm: &mut ResourceManager) -> Result<()> {
        // 创建多个blocking任务，将句柄保存起来，后面可以等待它们结束
        for i in 0..self.nodes.len() {
            for j in (i + 1)..self.nodes.len() {
                let conn_with_i = connect_as(&self.nodes[j], &self.nodes[i], rm).await?;
                let conn_with_j = connect_as(&self.nodes[i], &self.nodes[j], rm).await?;
                let (reader_i, writer_i) = split_connection(conn_with_i)?;
                let (reader_j, writer_j) = split_connection(conn_with_j)?;

                for (mut reader, mut writer, direction) in [
                    (reader_i, writer_j, format!("{i}->{j}")),
                    (reader_j, writer_i, format!("{j}->{i}")),
                ] {
                    self.interceptor_tasks.spawn_blocking(move || -> Result<()> {
                        let mut message_builder = MessageBuilder::new();
                        loop {
                            let packet = receive_packet(&mut reader)?;
                            match packet.sum.context("packet has no payload")? {
                                Sum::PacketMsg(fragment) => {
                                // 转发给对应的writer
                                    if let Some(message) = message_builder.handle_packet(fragment)? {
                                        send_complete_message(&mut writer, &message)?;
                                        println!("Complete message [{direction}]: channel={:#x}, bytes={}, packets={}",
                                            message.channel_id,
                                            message.data.len(),
                                            message.packet_count,
                                        );
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
