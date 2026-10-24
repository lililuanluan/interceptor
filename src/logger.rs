use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::filepath::{create_shared_dir, create_shared_file};

const QUEUE_CAPACITY: usize = 4096; // Capacity of the pending-record queue.

// Struct field names become column headers and field values form each row.
// The struct also supplies an associated constant specifying its CSV file path.
pub trait LogSchema: Serialize + Send + 'static
// These bounds require every LogSchema implementor to satisfy Serialize, Send, and 'static.
{
    // Traits can contain methods, associated constants, and associated types.
    // Required methods are implemented by the type and called on an instance: x.method().
    // Associated type: type Item = u64; refer to it as T::Item.
    // Associated constant: const FILE: ...; access it as T::FILE.
    const FILE: &'static str;
}

// An mpsc channel provides tx/rx handles: tx enqueues records and a single rx consumes them.
// Clone the writer for multiple producer tasks; one background task drains the queue into the file.
// T is a record struct implementing LogSchema.
pub struct LogWriter<T> {
    tx: mpsc::Sender<T>,
}

// LogWriter<T> must be cloneable without requiring T: Clone, so implement Clone manually rather than deriving it.
impl<T> Clone for LogWriter<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<T: LogSchema> LogWriter<T> {
    pub async fn log(&self, record: T) -> Result<()> {
        self.tx
            .send(record)
            .await // Wait here if the queue is full.
            .map_err(|_| anyhow!("log {} has stopped", T::FILE))
    }

    pub fn log_blocking(&self, record: T) -> Result<()> {
        // Synchronous variant for spawn_blocking, where async calls cannot be awaited.
        self.tx
            .blocking_send(record)
            .map_err(|_| anyhow!("log {} has stopped", T::FILE))
    }
}

// Logger owns all background logging tasks.
pub struct Logger {
    log_dir: PathBuf,
    // A JoinSet can cancel its tasks as a group. Keep individual JoinHandles here so each task
    // can finish independently and persist its logs before cleanup.
    tasks: Vec<JoinHandle<Result<()>>>,
}

impl Logger {
    pub fn new(log_dir: &Path) -> Result<Self> {
        create_shared_dir(log_dir).context(format!("create log dir {}", log_dir.display()))?;

        Ok(Self {
            log_dir: log_dir.to_path_buf(),
            tasks: Vec::new(),
        })
    }

    // Register a log record type with writer<LogSchema> and obtain its submission handle.
    pub fn writer<T: LogSchema>(&mut self) -> Result<LogWriter<T>> {
        let path = self.log_dir.join(T::FILE);

        let file =
            create_shared_file(&path).context(format!("create log file {}", path.display()))?;

        let mut csv_writer = csv::Writer::from_writer(file); // Use a separate background writer task for each log type.
        let (tx, mut rx) = mpsc::channel::<T>(QUEUE_CAPACITY); // Expose the mpsc sender to callers.

        let handle = tokio::task::spawn_blocking(move || -> Result<()> {
            // Once all tx handles (LogWriter instances) are dropped and the queue is drained,
            // blocking_recv() returns None and the task exits.
            while let Some(record) = rx.blocking_recv() {
                // CSV serialization writes a header only for the first record.
                // Serde supplies the struct field names used for that header.
                csv_writer.serialize(&record)?;
            }
            csv_writer.flush()?;
            Ok(())
        });

        self.tasks.push(handle);
        Ok(LogWriter { tx })
    }

    // Similar to joining monitors, but do not cancel: let each writer drain its queue and flush.
    // Ensure that all producers have finished first.
    pub async fn wait(self) -> Result<()> {
        let mut failure: Option<anyhow::Error> = None;

        for handle in self.tasks {
            let error = match handle.await {
                Ok(Ok(())) => continue,
                Ok(Err(err)) => err,
                Err(err) => err.into(),
            };
            if failure.is_none() {
                failure = Some(error); // Keep the first error.
            }
        }

        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
