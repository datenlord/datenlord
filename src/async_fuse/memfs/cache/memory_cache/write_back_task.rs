//! Types and functions related to the write back task.

use std::collections::HashMap;
use std::mem;
use std::sync::{Arc, Weak};
use std::time::Duration;

use clippy_utilities::OverflowArithmetic;
use datenlord::config::SoftLimit;
use hashlink::LinkedHashSet;
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

use crate::async_fuse::fuse::protocol::INum;
use crate::async_fuse::memfs::cache::policy::EvictPolicy;
use crate::async_fuse::memfs::cache::{Block, BlockCoordinate, BlockId, MemoryCache, Storage};

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
        sender: oneshot::Sender<()>,
    },
    /// Flush the file specified immediately.
    Flush(INum),
    /// Flush all the files
    FlushAll(oneshot::Sender<()>),
}

/// Calculate a new period for write back task's interval.
fn calculate_period(
    pending_length: usize,
    command_queue_limit: usize,
    basic_period: Duration,
) -> Duration {
    if pending_length > command_queue_limit.overflow_div(4).overflow_mul(3) {
        // If the number of pending blocks is greater than 75% of `command_queue_limit`,
        // the new period should be changed to `1/4`.
        basic_period / 4
    } else if pending_length > command_queue_limit.overflow_div(2) {
        // If the number of pending blocks is greater than 50% of `command_queue_limit`,
        // the new period should be changed to `1/2`.
        basic_period / 2
    } else {
        // Otherwise, restore the basic period.
        basic_period
    }
}

/// Flush a pending block to the backend
async fn flush_a_block<P, S>(
    lru_queue: &mut LinkedHashSet<BlockCoordinate>,
    pending_blocks: &mut HashMap<INum, HashMap<BlockId, (Block, oneshot::Sender<()>)>>,
    cache: Arc<MemoryCache<P, S>>,
) where
    P: EvictPolicy<BlockCoordinate> + Send + Sync,
    S: Storage + Send + Sync,
{
    if let Some(BlockCoordinate(ino, block_id)) = lru_queue.pop_front() {
        if let Some((block, sender)) = pending_blocks
            .get_mut(&ino)
            .and_then(|file_level_pending_blocks| file_level_pending_blocks.remove(&block_id))
        {
            cache.backend().store(ino, block_id, block).await;
            sender.send(()).unwrap_or_else(|()| {
                warn!("The receiver of pending block is closed unexpectedly.");
            });
        }
    }
}

/// Flush a file, write blocks of it to backend immediately.
async fn flush_file<P, S>(
    lru_queue: &mut LinkedHashSet<BlockCoordinate>,
    pending_blocks: &mut HashMap<INum, HashMap<BlockId, (Block, oneshot::Sender<()>)>>,
    cache: Arc<MemoryCache<P, S>>,
    ino: INum,
) where
    P: EvictPolicy<BlockCoordinate> + Send + Sync,
    S: Storage + Send + Sync,
{
    let file_level_pending_blocks = pending_blocks.remove(&ino);
    lru_queue.retain_with_order(|&BlockCoordinate(i, _)| i != ino);

    if let Some(file_level_pending_blocks) = file_level_pending_blocks {
        for (block_id, (block, sender)) in file_level_pending_blocks {
            cache.backend().store(ino, block_id, block).await;
            sender.send(()).unwrap_or_else(|()| {
                warn!("The receiver of pending block is closed unexpectedly.");
            });
        }
    }
}

/// Flush all files, write blocks of them to backend immediately.
async fn flush_all<P, S>(
    lru_queue: &mut LinkedHashSet<BlockCoordinate>,
    pending_blocks: &mut HashMap<INum, HashMap<BlockId, (Block, oneshot::Sender<()>)>>,
    cache: Arc<MemoryCache<P, S>>,
    sender: oneshot::Sender<()>,
) where
    P: EvictPolicy<BlockCoordinate> + Send + Sync,
    S: Storage + Send + Sync,
{
    lru_queue.clear();

    let blocks = mem::take(pending_blocks);

    for (ino, file_level_pending_blocks) in blocks {
        for (block_id, (block, block_sender)) in file_level_pending_blocks {
            cache.backend().store(ino, block_id, block).await;
            block_sender.send(()).unwrap_or_else(|()| {
                warn!("The receiver of pending block is closed unexpectedly.");
            });
        }
    }

    sender
        .send(())
        .unwrap_or_else(|()| warn!("The receiver of pending block is closed unexpectedly."));
}

/// Evict blocks from the cache, if it hits the soft limit.
async fn do_evict<P, S>(limit: SoftLimit, cache: Arc<MemoryCache<P, S>>)
where
    P: EvictPolicy<BlockCoordinate> + Send + Sync,
    S: Storage + Send + Sync,
{
    // `a` is a numerator while `b` is a denominator, which are to represent the
    // soft limit. If `size > capacity * a / b`, the soft limit is considered to
    // be hit. We use the equivalent inequation `size * b / a > capacity` to
    // avoid overflowing.
    let SoftLimit(a, b) = limit;
    let b = b.get();

    loop {
        let capacity = cache.policy().capacity();
        let size = cache.policy().size();

        // Soft limitation is hit, do evict.
        if size.overflow_mul(b).overflow_div(a) > capacity {
            cache.evict().await;
        } else {
            break;
        }
    }
}

/// A function to run a write back task.
///
/// This task will smoothly write blocks back to the backend
/// when the cache runs in write-back policy.
///
/// And this task will evict blocks to the backend if the number of blocks hits
/// the soft limit, whether the cache runs in write-back policy or not.
pub(super) async fn run_write_back_task<P, S>(
    limit: SoftLimit,
    interval: Duration,
    storage: Weak<MemoryCache<P, S>>,
    mut command_receiver: mpsc::Receiver<Command>,
    command_queue_limit: usize,
) where
    P: EvictPolicy<BlockCoordinate> + Send + Sync,
    S: Storage + Send + Sync,
{
    let period = interval;
    let mut interval = tokio::time::interval(period);

    let mut lru_queue = LinkedHashSet::new();
    let mut pending_blocks: HashMap<INum, HashMap<BlockId, (Block, oneshot::Sender<()>)>> =
        HashMap::new();

    loop {
        // Tune the interval according the payload.
        let pending_length = lru_queue.len();
        let new_period = calculate_period(pending_length, command_queue_limit, period);
        if interval.period() != new_period {
            interval = tokio::time::interval(new_period);
        }

        select! {
            command = command_receiver.recv() => {
                match command {
                    Some(Command::Store { coord: BlockCoordinate(ino, block_id), block, sender }) => {
                        // Add the block to `lru_queue` and `pending_blocks`
                        lru_queue.insert(BlockCoordinate(ino, block_id));
                        pending_blocks.entry(ino).or_default().insert(block_id, (block, sender));
                    }
                    Some(Command::Flush(ino)) => {
                        if let Some(cache) = storage.upgrade() {
                            flush_file(&mut lru_queue, &mut pending_blocks, cache, ino).await;
                        } else {
                            panic!("Memory cache is dropped before being flushed.");
                        }
                    },
                    Some(Command::FlushAll(sender)) => {
                        if let Some(cache) = storage.upgrade() {
                            flush_all(&mut lru_queue, &mut pending_blocks, cache, sender).await;
                        } else {
                            panic!("Memory cache is dropped before being flushed.");
                        }
                    }
                    None => {
                        // The command sender is closed, meaning that the `Storage` is also dropped.
                        // Then the write back task should exit.
                        info!("Write back task exits.");
                        return;
                    },
                }
            },
            _ = interval.tick() => {
                // If the queue is empty, do evict;
                // Otherwise, flush a block to the backend
                if let Some(cache) = storage.upgrade() {
                    if lru_queue.is_empty() {
                        do_evict(limit, cache).await;
                    } else {
                        flush_a_block(&mut lru_queue, &mut pending_blocks, cache).await;
                    }
                } else {
                    // The command sender is closed, meaning that the `Storage` is also dropped.
                    // Then the write back task should exit.
                    info!("Write back task exits.");
                    return;
                }
            },
        }
    }
}
