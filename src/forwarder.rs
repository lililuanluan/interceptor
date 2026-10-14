use std::collections::HashMap;
use std::io::Write;
use std::sync::mpsc::Receiver;
use std::time::Instant;

use anyhow::{Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use tendermint_proto::p2p::Packet;
use tendermint_proto::p2p::packet::Sum;

use crate::controller::Intercepted;
use crate::logger::LogWriter;
use crate::message::{CompleteMessage, MessageLog, sha256_hex};
use crate::p2p::{send_complete_message, send_packet};

// 从管道里读到的两类消息，一个是从controller发来的，一个是interceptor直塞的pingpong
#[derive(Debug)]
pub enum ForwardItem {
    Message(SendJob),
    PingPong { from: usize, to: usize, sum: Sum },
}

#[derive(Debug)]
pub struct SendJob {
    pub from: usize,
    pub to: usize,
    pub channel_id: i32,
    pub data: Vec<u8>,            // 要发出去的字节
    pub received: Option<Origin>, // 拦截到的数据，如果是凭空捏造则为None
}

#[derive(Debug)]
pub struct Origin {
    pub packets: usize,
    pub intercepted_at: DateTime<Utc>,
    pub intercepted_instant: Instant,
    pub data: Option<Vec<u8>>, // 如果没被篡改则为None
}

// forwarder负责两件事：给节点转发消息，写日志
pub struct Forwarder<W: Write> {
    inbox: Receiver<ForwardItem>, // 从controller接收消息以及pingpong直塞的管道
    writers: HashMap<(usize, usize), W>, // from,to -> socket
    started_instant: Instant,
    message_writer: LogWriter<MessageLog>,
}

impl<W: Write + Send + 'static> Forwarder<W> {
    pub fn new(
        inbox: Receiver<ForwardItem>,
        writers: HashMap<(usize, usize), W>,
        started_instant: Instant,
        message_writer: LogWriter<MessageLog>,
    ) -> Self {
        Self {
            inbox,
            writers,
            started_instant,
            message_writer,
        }
    }

    pub fn run(&mut self) -> Result<()> {
        while let Ok(item) = self.inbox.recv() {
            match item {
                ForwardItem::PingPong { from, to, sum } => self.send_pingpong(from, to, sum)?,
                ForwardItem::Message(job) => self.send_message(job)?,
            }
        }
        Ok(())
    }

    pub fn send_message(&mut self, job: SendJob) -> Result<()> {
        // 写日志+转发给节点
        let forwarded_at = Utc::now();
        let forwarded_instant = Instant::now();

        let SendJob {
            from,
            to,
            channel_id,
            data,
            received,
        } = job;
        let writer = self
            .writers
            .get_mut(&(from, to))
            .with_context(|| format!("unknown direction {from}->{to}"))?;

        let message = CompleteMessage {
            channel_id,
            data,
            packet_count: 0, // 这里不会用到
        };

        send_complete_message(writer, &message).with_context(|| format!("send {from} -> {to}"))?;

        // 写日志
        let (packets, intercepted_at, intercepted_us) = match &received {
            Some(origin) => (
                origin.packets,
                origin
                    .intercepted_at
                    .to_rfc3339_opts(SecondsFormat::Micros, true),
                origin
                    .intercepted_instant
                    .saturating_duration_since(self.started_instant)
                    .as_micros(),
            ),
            None => (0, String::new(), 0), // 凭空捏造的
        };

        self.message_writer.log_blocking(MessageLog {
            from,
            to,
            channel_id: message.channel_id,
            len: message.data.len(),
            packets,
            sha256: sha256_hex(&message.data),
            intercepted_at,
            intercepted_us,
            forwarded_at: forwarded_at.to_rfc3339_opts(SecondsFormat::Micros, true),
            forwarded_us: forwarded_instant
                .saturating_duration_since(self.started_instant)
                .as_micros(),
            forwarded_bytes_hex: hex::encode(&message.data),
            intercepted_bytes_hex: received
                .as_ref()
                .and_then(|origin| origin.data.as_ref())
                .map(hex::encode),
        })?;

        Ok(())
    }

    pub fn send_pingpong(&mut self, from: usize, to: usize, sum: Sum) -> Result<()> {
        let writer = self
            .writers
            .get_mut(&(from, to))
            .with_context(|| format!("unknown direction {from}->{to}"))?;
        send_packet(writer, &Packet { sum: Some(sum) })
            .with_context(|| format!("send control {from}->{to}"))
    }
}
