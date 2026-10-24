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

// The channel carries controller messages and Ping/Pong packets sent directly by the interceptor.
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
    pub data: Vec<u8>,            // Bytes to send.
    pub received: Option<Origin>, // Intercepted origin; None for a fabricated message.
    pub delay: Duration,
}

#[derive(Debug)]
pub struct Origin {
    pub packets: usize,
    pub intercepted_at: DateTime<Utc>,
    pub intercepted_instant: Instant,
    pub data: Option<Vec<u8>>, // None if the message was not mutated.
}

// deadline = intercepted_instant + delay
struct ScheduledJob {
    deadline: Instant,
    job: SendJob,
}

// Order ScheduledJob entries by deadline in a min-heap. The forwarder uses recv_timeout
// to wait until the earliest deadline, then returns to the loop's top to send due messages
// when the deadline arrives or a new message wakes it.

// Relations and equivalence relations:
// Symmetry: a == b => b == a.
// Transitivity: a == b and b == c => a == c.
// Reflexivity: for all a, a == a.
// PartialEq does not require reflexivity, but Eq does. For example, f64 implements PartialEq
// but not Eq because NaN is not equal to itself.
// Eq requires PartialEq, so PartialEq must be implemented explicitly first.
impl PartialEq for ScheduledJob {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline
    }
}
impl Eq for ScheduledJob {} // Promise reflexivity/equivalence; no additional methods are required.

// Ordering has three values: Less, Equal, and Greater.
// Ord makes all elements comparable and returns Ordering; PartialOrd may not, so it returns Option<Ordering>.
// Similarly, Ord requires PartialOrd; Rust does not infer that implementation automatically.
impl PartialOrd for ScheduledJob {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other)) // Call Ord::cmp.
    }
}
impl Ord for ScheduledJob {
    fn cmp(&self, other: &Self) -> Ordering {
        self.deadline.cmp(&other.deadline)
    }
}

// The forwarder has two responsibilities: forwarding messages to nodes and recording logs.
pub struct Forwarder<W: Write> {
    inbox: Receiver<ForwardItem>, // Receives controller messages and directly enqueued Ping/Pong packets.
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

            // On each wake-up, send all messages whose deadlines have passed.
            while let Some(Reverse(top)) = self.pending.peek() {
                if top.deadline > Instant::now() {
                    break;
                }
                let Reverse(entry) = self.pending.pop().context("pending queue empty")?;
                self.send_message(entry.job)?;
            }

            // Wait until the next deadline.
            let next_deadline = self.pending.peek();
            let timeout = next_deadline
                .map(|Reverse(entry)| entry.deadline.saturating_duration_since(Instant::now())) // None skips the closure and remains None.
                .unwrap_or(Duration::from_secs(3600)); // Use a long wait for an empty heap; new messages wake it.

            // recv_timeout waits for an inbox message. A timeout error means no message arrived
            // during the wait; the next iteration sends the due item at the heap's top.
            // The operating system schedules this blocking wait.
            match self.inbox.recv_timeout(timeout) {
                Ok(ForwardItem::Message(job)) => {
                    let start = job
                        .received
                        .as_ref()
                        .map(|origin| origin.intercepted_instant)
                        .unwrap_or_else(Instant::now); // No origin means the message was fabricated.

                    let deadline = start.checked_add(job.delay).context("deadline deduction")?;
                    self.pending.push(Reverse(ScheduledJob { deadline, job })); // Recheck the heap's top on the next iteration.
                }
                Ok(ForwardItem::PingPong { from, to, sum }) => {
                    // Forward Ping/Pong immediately.
                    self.send_pingpong(from, to, sum)?;
                }
                Err(RecvTimeoutError::Timeout) => {} // Loop again to send the due item at the heap's top.
                Err(RecvTimeoutError::Disconnected) => return Ok(()), // All upstream senders have exited.
            }
        }
    }

    pub fn send_message(&mut self, job: SendJob) -> Result<()> {
        // Forward the message to the node and record its log entry.
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
            packet_count: 0, // Not used here.
        };

        send_complete_message(writer, &message).with_context(|| format!("send {from} -> {to}"))?;

        // Record the log entry.
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
            None => (0, String::new(), 0), // A fabricated message.
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
