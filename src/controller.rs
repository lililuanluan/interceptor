use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rand::Rng;
use tokio::sync::mpsc;

use crate::forwarder::{ForwardItem, Origin, SendJob};

// intercepter -> controller -> forwarder
// Messages can arrive out of order, and the forwarder should not have to query the
// interceptor after receiving a controller decision. Pass all intercepted metadata,
// including timestamps, to the controller.

// interceptor -> controller
#[derive(Debug)]
pub struct Intercepted {
    pub from: usize,
    pub to: usize,
    pub channel_id: i32,
    pub packets: usize,
    pub data: Vec<u8>,                 // Raw bytes.
    pub intercepted_at: DateTime<Utc>, // wall clock
    pub intercepted_instant: Instant,  // Monotonic time for calculating elapsed durations.
}

pub struct Controller {
    inbox: mpsc::UnboundedReceiver<Intercepted>, // Messages supplied by the interceptor.
    to_forwarder: Sender<ForwardItem>,
}

impl Controller {
    pub fn new(
        inbox: mpsc::UnboundedReceiver<Intercepted>,
        to_forwarder: Sender<ForwardItem>,
    ) -> Self {
        Self {
            inbox,
            to_forwarder,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        // There is one input for now. After integrating Python, use two tokio::select! branches:
        // one for intercepted messages and one for decision replies, with no ordering between them.
        while let Some(msg) = self.inbox.recv().await {
            self.on_intercepted(msg)?;
        }
        Ok(())
    }

    fn on_intercepted(&mut self, msg: Intercepted) -> Result<()> {
        // Build a SendJob (using a random delay as a placeholder) and send it to the forwarder.
        let Intercepted {
            from,
            to,
            channel_id,
            packets,
            data,
            intercepted_at,
            intercepted_instant,
        } = msg;

        let job = SendJob {
            from,
            to,
            channel_id,
            data,
            received: Some(Origin {
                packets,
                intercepted_at,
                intercepted_instant,
                data: None, // Not mutated.
            }),
            delay: Duration::from_millis(rand::rng().random_range(0..=200_u64)),
        };

        self.to_forwarder
            .send(ForwardItem::Message(job))
            .context("controller cannot send: forwarder has stopped")?;
        Ok(())
    }
}
