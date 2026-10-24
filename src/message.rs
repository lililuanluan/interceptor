// A SecretConnection is a full-duplex physical TCP connection between the interceptor and a node.
// CometBFT sends messages on different channels according to their type.
// Channels are logical; they share the same physical connection.
// Each packet carries a channel_id identifying its channel.
// On one channel, all packets of a message must be sent before the next message;
// packets from different channels may interleave.
// A channel is not a message type but a broader category: related message types often share one channel.
// Messages are split into packets. On receipt, append each packet to its channel's buffer.
// The eof flag marks whether this is the message's final packet.

use std::collections::HashMap;

use anyhow::{Result, ensure};
use serde::Serialize;
use tendermint_proto::p2p::PacketMsg;

use crate::logger::LogSchema;

#[derive(Serialize)]
pub struct MessageLog {
    pub from: usize, // Sender.
    pub to: usize,
    pub channel_id: i32,
    pub len: usize,     // Byte count.
    pub packets: usize, // Number of fragments received.
    // Hash of the message contents. TODO: keep only the hash here and index raw bytes in a database
    // to avoid logging duplicate broadcasts (90% of messages were duplicate gossip in a four-node run).
    pub sha256: String,
    // pub signer: Option<String>, // Only some messages are signed; record the original signer. TODO: implement extract_signer.
    pub intercepted_at: String,
    pub intercepted_us: u128,
    pub forwarded_at: String, // When the interceptor sends this message; do not confuse this with other timestamps.
    pub forwarded_us: u128,
    pub delay_us: u128, // Controller-assigned delay; compare it with forwarded_at - intercepted_at to measure jitter.
    pub forwarded_bytes_hex: String, // Sent bytes, encoded as hex.
    pub intercepted_bytes_hex: Option<String>, // None if the message was not mutated.
}

impl LogSchema for MessageLog {
    const FILE: &'static str = "message.csv";
}

pub fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

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
                channel_id: self.channel_id,
                eof: true,
                data: Vec::new(),
            }]);
        }

        let mut packets = Vec::new();
        const MAX_PAYLOAD: usize = 1024;

        let mut chunks = self.data.chunks(MAX_PAYLOAD).peekable(); // Allow inspecting the next item without consuming it.

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
    channels: HashMap<i32, PartialMessage>, // Keep one message under reassembly per channel.
}

impl MessageBuilder {
    pub fn new() -> Self {
        Self {
            channels: HashMap::new(),
        }
    }

    // Consume a packet and return a complete message if available; an incomplete message is not an error, hence Result<Option>.
    pub fn handle_packet(&mut self, packet: PacketMsg) -> Result<Option<CompleteMessage>> {
        // Protobuf uses i32 for channel_id, but CometBFT requires the range 0..=255.
        let channel_id = packet.channel_id;
        ensure!(
            (0..=255).contains(&channel_id),
            "Invalid channel id: {channel_id}"
        );

        // Get a mutable reference to this channel's accumulated fragments, or start with an empty buffer.
        let partial = self.channels.entry(channel_id).or_default();

        // Append the new packet's data to the partial message.
        partial.data.extend_from_slice(&packet.data);
        partial.packet_count += 1;

        // eof=true means this is the final fragment.
        if packet.eof {
            // std::mem::take moves the value out and replaces it with its default value.
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
