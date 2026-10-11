use anyhow::{Result, anyhow, ensure};
use chrono::{SecondsFormat, Utc};
use serde_json::json;
use std::{path::Path, time::Instant};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::filepath::create_shared_dir;

#[derive(Debug)]
pub enum LogEvent {
    Block {
        node: usize,
        height: u64,
        block_hash: String,
        block_time: String,
    },
    // 以后可以追加其它的事件，例如拦截到的消息
}

struct LogRecord {
    observed_at: String, // 绝对时间，utc格式
    elapsed_us: u128,    // 相对计时开始的时间
    event: LogEvent,
}

// 每个任务持有的一个writer句柄，负责提交要写的内容
// 实际有一个全局的任务负责写
// mpsc，可以split出sender和receiver，sender往队列中发送，单一receiver消费这些record
#[derive(Clone)]
pub struct LogWriter {
    sender: mpsc::Sender<LogRecord>,
    timer: Instant,
}

impl LogWriter {
    pub fn new(log_dir: &Path) -> Result<(Self, JoinHandle<Result<()>>)> {
        let timer = Instant::now(); // 是一个单调计时器，可以用timer.elapsed().as_micros();获取相对时间

        create_shared_dir(&log_dir)?;
    }
}

// 管理任务的生命周期以及提供接口
pub struct Logger {
    writer: Log
}
