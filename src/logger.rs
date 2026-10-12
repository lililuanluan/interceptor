use anyhow::{Context, Result, anyhow};
use serde::Serialize;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::filepath::{create_shared_dir, create_shared_file};

const QUEUE_CAPACITY: usize = 4096; // 等待队列的容量

// 将一个结构体，字段名解析为列明，字段值就是每一行的值，然后这个结构体再维护一个静态变量，就是对应的csv文件路径
pub trait LogSchema: Serialize + Send + 'static
// supertraits，实现了LogSchema的类必然实现了这三个
{
    // trait里可以放三类成员：方法，关联常量，关联类型
    // 方法要求类必须实现，用实例调用，T x; x.method()
    // 类：type Item = u64;  T::Item x;
    // 常量：const FILE: ...;  T::FILE
    const FILE: &'static str;
}

// mpsc，可以split出sender和receiver，sender往队列中发送，单一receiver消费这些record
// writer可以clone给多个任务，负责往队列中写，而只有一个后台任务负责取出队列中的记录并写到文件中
// T是一个LogSchema结构体
pub struct LogWriter<T> {
    sender: mpsc::Sender<T>,
}

// LogWriter<T>是应该可以被Clone的，但是T不需要，所以不能derive clone，这里手动实现一下
impl<T> Clone for LogWriter<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<T: LogSchema> LogWriter<T> {
    pub async fn log(&self, record: T) -> Result<()> {
        self.sender
            .send(record)
            .await // 队列满时这里会等待
            .map_err(|_| anyhow!("log {} has stopped", T::FILE))
    }

    pub fn log_blocking(&self, record: T) -> Result<()> {
        // 同步版本，在spawn_blocking中不能用异步的
        self.sender
            .blocking_send(record)
            .map_err(|_| anyhow!("log {} has stopped", T::FILE))
    }
}

// Logger持有所有日志的后台任务
pub struct Logger {
    log_dir: PathBuf,
    tasks: Vec<JoinHandle<Result<()>>>, // JoinSet是一组可以同时结束的任务，可以连坐取消；而这里JoinHanle的数组表示这些任务必须等它们独立跑完（把日志落盘）然后清理
}

impl Logger {
    pub fn new(log_dir: &Path) -> Result<Self> {
        create_shared_dir(log_dir).context(format!("create log dir {}", log_dir.display()))?;

        Ok(Self {
            log_dir: log_dir.to_path_buf(),
            tasks: Vec::new(),
        })
    }

    // 注册一种日志（用writer<LogSchema>）并获得提交句柄
    pub fn writer<T: LogSchema>(&mut self) -> Result<LogWriter<T>> {
        let path = self.log_dir.join(T::FILE);

        let file =
            create_shared_file(&path).context(format!("create log file {}", path.display()))?;

        let mut csv_writer = csv::Writer::from_writer(file); // 后台写任务，每种日志开一个单独的任务
        let (sender, mut receiver) = mpsc::channel::<T>(QUEUE_CAPACITY); // mpsc，其中sender句柄要暴露给外部

        let handle = tokio::task::spawn_blocking(move || -> Result<()> {
            // blocking_recv()在所有的sender端都释放后返回None，任务退出
            while let Some(record) = receiver.blocking_recv() {
                // csv的serialize会在第一次写csv的时候加上表头，后面不加
                // 表头是serde在序列化之后自动提供的字段名
                csv_writer.serialize(&record)?;
            }
            csv_writer.flush()?;
            Ok(())
        });

        self.tasks.push(handle);
        Ok(LogWriter { sender })
    }

    // 类似join monitors，但是这里不能用shutdown，而是等待所有后台写任务把队列里的东西都写完并flush
    // 注意要确保所有生产者都结束了
    pub async fn wait(self) -> Result<()> {
        let mut failure: Option<anyhow::Error> = None;

        for handle in self.tasks {
            let error = match handle.await {
                Ok(Ok(())) => continue,
                Ok(Err(err)) => err,
                Err(err) => err.into(),
            };
            if failure.is_none() {
                failure = Some(error); // 保留第一个错误
            }
        }

        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
