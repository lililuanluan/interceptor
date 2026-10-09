// secrete_connection 是一条在interceptor-node之间的物理连接，tcp，双工
// cometbft在发送消息时，会根据消息的类型发在不同channel上
// channel是逻辑上的实现，物理上都在一个connection上发
// 在packet中有一个channel_id来标识消息在哪个channel
// 同一个channel上的消息是连续的，必须先发完一条消息的所有packet再发另一条，不同channel之间的消息可能穿插
// channel≠消息类型，但是可以理解为“大类”：同一个用途的多个类型消息通常发在同一个channel上
// 消息在发送时会拆成多个packet，需要从connection上接收之后，根据channel id，追加到之前的缓存中，在packet中有个efo:true/false，如果为true，说明这个packet是这条消息的最后一个包

use anyhow::{Context, Result, ensure};
use std::collections::HashMap;
use tendermint_proto::p2p::PacketMsg;

#[derive(Debug, Default)]
struct PartialMessage {
    data: Vec<u8>,
    packet_count: usize,
}

#[derive(Debug)]
pub struct CompleteMessage {
    pub channel_id: i32,
    pub data: Vec<u8>,
    pub packet_count: usize,
}

impl CompleteMessage {
    pub fn to_packets(&self) -> Result<Vec<PacketMsg>> {
        ensure!(
            (0..=255).contains(&self.channel_id),
            "Invalid channel id: {}",
            self.channel_id
        );

        if self.data.is_empty() {
            return Ok(vec![PacketMsg {
                channel_id: self.channel_id.clone(),
                eof: true,
                data: Vec::new(),
            }]);
        }

        let mut packets = Vec::new();
        const MAX_PAYLOAD: usize = 1024;

        let mut chunks = self.data.chunks(MAX_PAYLOAD).peekable(); // 创建一个可以查看但不消耗的迭代器

        while let Some(chunk) = chunks.next() {
            packets.push(PacketMsg {
                channel_id: self.channel_id,
                eof: chunks.peek().is_none(),
                data: chunk.to_vec(),
            })
        }
        Ok(packets)
    }
}

#[derive(Debug, Default)]
pub struct MessageBuilder {
    channels: HashMap<i32, PartialMessage>, // 每个channel维护一个正在重组的消息
}

impl MessageBuilder {
    pub fn new() -> Self {
        Self {
            channels: HashMap::new(),
        }
    }

    // 输入一个packet，如果能组成完整消息则输出（但是没有输出也不能算错，所以用Result<Option>
    pub fn handle_packet(&mut self, packet: PacketMsg) -> Result<Option<CompleteMessage>> {
        // 在protobuf中，channel_id是i32，但是cometbft要求必须是0-255
        let channel_id = packet.channel_id;
        ensure!(
            (0..=255).contains(&channel_id),
            "Invalid channel id: {channel_id}"
        );

        // 获取对应channel的之前积累下的消息片（或者为空）的可变引用
        let partial = self.channels.entry(channel_id).or_default();

        // 将新packet的数据追加到partial上
        partial.data.extend_from_slice(&packet.data);
        partial.packet_count += 1;

        // 如果当前packet的eof是true，说明是最后一个分片
        if packet.eof {
            // std::mem:take将变量的值移动给新变量，并将原来的位置放上默认值
            let tmp = std::mem::take(partial);
            Ok(Some(CompleteMessage {
                channel_id,
                data: tmp.data,
                packet_count: tmp.packet_count,
            }))
        } else {
            Ok(None)
        }
    }
}
