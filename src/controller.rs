use std::sync::mpsc::Sender;
use std::time::Instant;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::forwarder::ForwardItem;
use crate::ipc::{decode_send_job, encode_intercepted, receive_json, send_json};

// intercepter -> controller -> forwarder
// 由于消息是乱序的，而且不要钱forwarder在收到controller消息之后还从interceptor里面查数据
// 所以intercepted的信息都要传递给controller，包括时间戳等

// interceptor -> controller
#[derive(Debug)]
pub struct Intercepted {
    pub from: usize,
    pub to: usize,
    pub channel_id: i32,
    pub packets: usize,
    pub data: Vec<u8>,                 // 原始字节
    pub intercepted_at: DateTime<Utc>, // wall clock
    pub intercepted_instant: Instant,  // 单调时钟，用于计算时间差
}

pub struct Controller {
    inbox: mpsc::UnboundedReceiver<Intercepted>, // interceptor塞进来的
    to_forwarder: Sender<ForwardItem>,
    connection: UnixStream,   // 和python端的通信socket
    started_instant: Instant, // 测试开始时间，用于重建时间信息
    shutdown: CancellationToken,
    num_nodes: usize, // 用于检查python返回的from/to
}

impl Controller {
    pub fn new(
        inbox: mpsc::UnboundedReceiver<Intercepted>,
        to_forwarder: Sender<ForwardItem>,
        connection: UnixStream,
        started_instant: Instant,
        shutdown: CancellationToken,
        num_nodes: usize,
    ) -> Self {
        Self {
            inbox,
            to_forwarder,
            connection,
            started_instant,
            shutdown,
            num_nodes,
        }
    }

    pub async fn run(self) -> Result<()> {
        // 拦截到消息后，发给python
        // 从python收到消息后，转发p2p
        // 这两个动作没有顺序依赖关系，从python端返回的数据应该是自包含的

        let Self {
            mut inbox,
            to_forwarder,
            connection,
            started_instant,
            shutdown,
            num_nodes,
        } = self; // asyn要move，先拷贝一份
        let (mut reader, mut writer) = connection.into_split();

        let sending = async {
            while let Some(message) = inbox.recv().await {
                // TODO: 接入block monitor的current height
                send_json(
                    &mut writer,
                    &encode_intercepted(message, started_instant, 0)?,
                )
                .await?;
            }
            Ok::<(), anyhow::Error>(())
        };

        let receiving = async {
            while !shutdown.is_cancelled() {
                let value = receive_json(&mut reader)
                    .await
                    .context("Python controller connection")?;
                let job = decode_send_job(value, started_instant, num_nodes)?;
                to_forwarder
                    .send(ForwardItem::Message(job))
                    .context("controller cannot send: forwarder has stopped")?;
            }
            Ok::<(), anyhow::Error>(())
        };

        tokio::select! {
            biased;
            _ = shutdown.cancelled() => Ok(()),
            result = async { tokio::try_join!(sending, receiving).map(|_| ())} => result,
        }
    }
}
