use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};
use std::io::Write;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use tendermint_proto::p2p::Packet;
use tendermint_proto::p2p::packet::Sum;
use tokio_util::sync::CancellationToken;

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
    pub delay: Duration,
}

#[derive(Debug)]
pub struct Origin {
    pub packets: usize,
    pub intercepted_at: DateTime<Utc>,
    pub intercepted_instant: Instant,
    pub data: Option<Vec<u8>>, // 如果没被篡改则为None
}

// deadline = intercepted_instant + delay
struct ScheduledJob {
    deadline: Instant,
    job: SendJob,
}

// 将ScheduledJob按deadline放入最小堆；forwarder 本身用 recv_timeout 睡到堆顶的deadline，
// 到期或被新消息唤醒后回到循环顶部发送

// 关系代数，等价关系
// 对称性：a==b => b==a
// 传递性：a==b, b==c => a==c
// 自反性：for all a: a==a
// PartialEq不要求自反性，而Eq要求。例如f64就是实现了PartialEq，但没有实现Eq，因为NAN不等于自身
// 不能直接实现Eq，因为Eq的定义依赖PartialEq，所以必须显式实现PartialEq
impl PartialEq for ScheduledJob {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline
    }
}
impl Eq for ScheduledJob {} // 自反性，等价关系，不用实现任何方法，只是提供一个承诺

// 对于比较，Ordering有三个值：Less,Equal,Greater
// Ord是所有元素都可比，返回Ordering，而PartialOrd不一定，所以返回Option<Ordering>
// 类似的，必须实现PartialOrd才能实现Ord，rust不会自动推导
impl PartialOrd for ScheduledJob {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other)) // 调用Ord::cmp
    }
}
impl Ord for ScheduledJob {
    fn cmp(&self, other: &Self) -> Ordering {
        self.deadline.cmp(&other.deadline)
    }
}

// forwarder负责两件事：给节点转发消息，写日志
pub struct Forwarder<W: Write> {
    inbox: Receiver<ForwardItem>, // 从controller接收消息以及pingpong直塞的管道
    node_writers: HashMap<(usize, usize), W>, // from,to -> socket
    started_instant: Instant,
    message_writer: LogWriter<MessageLog>,
    shutdown: CancellationToken,
    pending: BinaryHeap<Reverse<ScheduledJob>>,
}

impl<W: Write + Send + 'static> Forwarder<W> {
    pub fn new(
        inbox: Receiver<ForwardItem>,
        writers: HashMap<(usize, usize), W>,
        started_instant: Instant,
        message_writer: LogWriter<MessageLog>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            inbox,
            node_writers: writers,
            started_instant,
            message_writer,
            shutdown,
            pending: BinaryHeap::new(),
        }
    }

    pub fn run(&mut self) -> Result<()> {
        loop {
            if self.shutdown.is_cancelled() {
                return Ok(());
            }

            // 每次醒来之后，将堆顶到期的消息全部发掉
            while let Some(Reverse(top)) = self.pending.peek() {
                if top.deadline > Instant::now() {
                    break;
                }
                let Reverse(entry) = self.pending.pop().context("pending queue empty")?;
                self.send_message(entry.job)?;
            }

            // 睡到下一个deadline
            let next_deadline = self.pending.peek();
            let timeout = next_deadline
                .map(|Reverse(entry)| entry.deadline.saturating_duration_since(Instant::now())) // 如果是None，不会走map，直接返回None
                .unwrap_or(Duration::from_secs(3600)); // 堆空直接长睡，等新消息来的时候叫醒

            // recv_timeout(timeout) 等待inbox消息，如果timeout返回error（就是睡眠期间没有人打扰，也就是没有新消息过来，此时醒来的时候直接把堆顶发了）
            // 这里的阻塞由操作系统调度
            match self.inbox.recv_timeout(timeout) {
                Ok(ForwardItem::Message(job)) => {
                    let start = job
                        .received
                        .as_ref()
                        .map(|origin| origin.intercepted_instant)
                        .unwrap_or_else(Instant::now); // 如果origin不存在，则为凭空捏造的消息

                    let deadline = start.checked_add(job.delay).context("deadline deduction")?;
                    self.pending.push(Reverse(ScheduledJob { deadline, job })); // 进入下一次循环之后重新查看堆顶
                }
                Ok(ForwardItem::PingPong { from, to, sum }) => {
                    // 直接发送pingpong
                    self.send_pingpong(from, to, sum)?;
                }
                Err(RecvTimeoutError::Timeout) => {} // 重新进入循环，发送堆顶
                Err(RecvTimeoutError::Disconnected) => return Ok(()), // 上游全部结束
            }
        }
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
            delay,
        } = job;
        let writer = self
            .node_writers
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
            delay_us: delay.as_micros(),
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
            .node_writers
            .get_mut(&(from, to))
            .with_context(|| format!("unknown direction {from}->{to}"))?;
        send_packet(writer, &Packet { sum: Some(sum) })
            .with_context(|| format!("send control {from}->{to}"))
    }
}
