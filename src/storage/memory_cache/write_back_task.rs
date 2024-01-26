//! Types and functions related to the write back task.

use std::collections::HashMap;
use std::mem;
use std::sync::Arc;
use std::time::Duration;

use clippy_utilities::OverflowArithmetic;
use datenlord::config::SoftLimit;
use hashlink::LinkedHashSet;
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

use crate::async_fuse::fuse::protocol::INum;
use crate::storage::error::StorageResult;
use crate::storage::policy::EvictPolicy;
use crate::storage::{Block, BlockCoordinate, BlockId, MemoryCache, Storage};

/// A sender to send the storage result back to the caller.
type StorageResultSender = oneshot::Sender<StorageResult<()>>;

/// A command sent from `MemoryCache` to the write back task.
pub enum Command {
    /// Store a block to the backend later.
    Store {
        /// The coordinate of the block
        coord: BlockCoordinate,
        /// The block
        block: Block,
        /// A sender to notify the `MemoryCache` that the block has been flushed
        /// to the backend
        sender: StorageResultSender,
    },
    /// Flush the file specified immediately.
    Flush(INum),
    /// Flush all the files
    FlushAll(oneshot::Sender<()>),
    /// Cancel the writing back of a file
    Cancel(INum),
}

/// The write back task running in the background.
pub(super) struct WriteBackTask<P, S> {
    /// The weak pointer of `MemoryCache`.
    storage: Arc<MemoryCache<P, S>>,
    /// The queue of block to be flushed.
    lru_queue: LinkedHashSet<BlockCoordinate>,
    /// The blocks to be flushed.
    pending_blocks: HashMap<INum, HashMap<BlockId, (Block, StorageResultSender)>>,
    /// Receiver to accept commands from `MemoryCache`.
    command_receiver: mpsc::Receiver<Command>,
    /// The soft limit of cache.
    limit: SoftLimit,
    /// The interval of its event loop.
    interval: Duration,
    /// The limit of command limit.
    command_queue_limit: usize,
}

impl<P, S> WriteBackTask<P, S> {
    /// Create a write back task.
    ///
    /// This task will smoothly write blocks back to the backend
    /// when the cache runs in write-back policy.
    ///
    /// And this task will evict blocks to the backend if the number of blocks
    /// hits the soft limit, whether the cache runs in write-back policy or
    /// not.
    pub(super) fn new(
        storage: Arc<MemoryCache<P, S>>,
        command_receiver: mpsc::Receiver<Command>,
        limit: SoftLimit,
        interval: Duration,
        command_queue_limit: usize,
    ) -> Self {
        Self {
            storage,
            lru_queue: LinkedHashSet::new(),
            pending_blocks: HashMap::new(),
            command_receiver,
            limit,
            interval,
            command_queue_limit,
        }
    }

    /// Calculate a new period for write back task's interval.
    fn calculate_period(&self) -> Duration {
        let pending_length = self.lru_queue.len();

        if pending_length > self.command_queue_limit.overflow_div(4).overflow_mul(3) {
            // If the number of pending blocks is greater than 75% of `command_queue_limit`,
            // the new period should be changed to `1/4`.
            self.interval / 4
        } else if pending_length > self.command_queue_limit.overflow_div(2) {
            // If the number of pending blocks is greater than 50% of `command_queue_limit`,
            // the new period should be changed to `1/2`.
            self.interval / 2
        } else {
            // Otherwise, restore the basic period.
            self.interval
        }
    }
}

impl<P, S> WriteBackTask<P, S>
where
    P: EvictPolicy<BlockCoordinate> + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    /// Flush a pending block to the backend
    async fn flush_a_block(&mut self) {
        if let Some(BlockCoordinate(ino, block_id)) = self.lru_queue.pop_front() {
            if let Some((block, sender)) = self
                .pending_blocks
                .get_mut(&ino)
                .and_then(|file_level_pending_blocks| file_level_pending_blocks.remove(&block_id))
            {
                let res = self.storage.backend().store(ino, block_id, block).await;
                sender.send(res).unwrap_or_else(|res| {
                    error!("The receiver is closed unexpectedly, with storage result: {res:?}");
                });
            }
        }
    }

    /// Flush a file, write blocks of it to backend immediately.
    async fn flush_file(&mut self, ino: INum) {
        let file_level_pending_blocks = self.pending_blocks.remove(&ino);
        self.lru_queue
            .retain_with_order(|&BlockCoordinate(i, _)| i != ino);

        if let Some(file_level_pending_blocks) = file_level_pending_blocks {
            for (block_id, (block, sender)) in file_level_pending_blocks {
                let res = self.storage.backend().store(ino, block_id, block).await;
                sender.send(res).unwrap_or_else(|res| {
                    error!("The receiver is closed unexpectedly, with storage result: {res:?}");
                });
            }
        }
    }

    /// Flush all files, write blocks of them to backend immediately.
    async fn flush_all(&mut self, sender: oneshot::Sender<()>) {
        self.lru_queue.clear();

        let blocks = mem::take(&mut self.pending_blocks);

        for (ino, file_level_pending_blocks) in blocks {
            for (block_id, (block, block_sender)) in file_level_pending_blocks {
                let res = self.storage.backend().store(ino, block_id, block).await;
                block_sender.send(res).unwrap_or_else(|res| {
                    error!("The receiver is closed unexpectedly, with storage result: {res:?}");
                });
            }
        }

        sender
            .send(())
            .unwrap_or_else(|()| warn!("The receiver of pending block is closed unexpectedly."));
    }

    /// Evict blocks from the cache, if it hits the soft limit.
    async fn do_evict(&self) {
        // `a` is a numerator while `b` is a denominator, which are to represent the
        // soft limit. If `size > capacity * a / b`, the soft limit is considered to
        // be hit. We use the equivalent inequation `size * b / a > capacity` to
        // avoid overflowing.
        let SoftLimit(a, b) = self.limit;
        let b = b.get();

        let policy = self.storage.policy();

        loop {
            let capacity = policy.capacity();
            let size = policy.size();

            // Soft limitation is hit, do evict.
            if size.overflow_mul(b).overflow_div(a) > capacity {
                if let Err(e) = self.storage.evict().await {
                    error!("Failed to evict a block: {e}");
                }
            } else {
                break;
            }
        }
    }

    /// Run the task
    pub(super) async fn run(mut self) {
        let mut interval = tokio::time::interval(self.interval);

        loop {
            // Tune the interval according the payload.
            let new_period = self.calculate_period();
            if interval.period() != new_period {
                interval = tokio::time::interval(new_period);
            }

            select! {
                command = self.command_receiver.recv() => {
                    match command {
                        Some(Command::Store { coord: BlockCoordinate(ino, block_id), block, sender }) => {
                            // Add the block to `lru_queue` and `pending_blocks`
                            self.lru_queue.insert(BlockCoordinate(ino, block_id));
                            self.pending_blocks.entry(ino).or_default().insert(block_id, (block, sender));
                        }
                        Some(Command::Flush(ino)) => {
                            self.flush_file(ino).await;
                        },
                        Some(Command::FlushAll(sender)) => {
                            self.flush_all(sender).await;
                        },
                        Some(Command::Cancel(ino)) => {
                            self.pending_blocks.remove(&ino);
                            self.lru_queue.retain_with_order(|&BlockCoordinate(i, _)| i != ino);
                        }
                        None => {
                            // The command sender is closed, meaning that the `Storage` is also dropped.
                            // Then the write back task should exit.
                            // But it seems to be impossible, because the task holds an `Arc` of cache, which
                            // holds the sender.
                            info!("Write back task exits.");
                            return;
                        },
                    }
                },
                _ = interval.tick() => {
                    // If the queue is empty, do evict;
                    // Otherwise, flush a block to the backend
                    if self.lru_queue.is_empty() {
                        self.do_evict().await;
                    } else {
                        self.flush_a_block().await;
                    }
                },
            }
        }
    }
}
