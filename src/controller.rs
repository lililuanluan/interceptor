use std::collections::HashMap;
use std::sync::mpsc::Sender;
use std::time::Instant;

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;

use crate::forwarder::{ForwardItem, Origin, SendJob};

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

    pub async fn run(&mut self) {
        // 现在是单输入；接 Python 后这里变成 tokio::select! 两个分支
        // （一个处理"拦到消息"，一个处理"决策回复"—— 两者之间没有任何顺序关系）
        while let Some(msg) = self.inbox.recv().await {
            self.on_intercepted(msg);
        }
    }

    fn on_intercepted(&mut self, msg: Intercepted) {
        // 直接转发给forwarder
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
                data: None, // 没被篡改
            }),
        };

        if self.to_forwarder.send(ForwardItem::Message(job)).is_err() {
            return;
        }
    }
}
