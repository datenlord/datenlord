//! Types and functions related to the write back task.

use std::collections::{HashMap, VecDeque};
use std::mem;
use std::sync::{Arc, Weak};
use std::time::Duration;

use clippy_utilities::OverflowArithmetic;
use datenlord::config::SoftLimit;
use futures::future::OptionFuture;
use hashlink::LinkedHashSet;
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

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
    /// Cancel the writing back of a file
    Cancel(INum),
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

/// Write a block back to the backend.
async fn write_back_one_block<P, S>(
    cache: Arc<MemoryCache<P, S>>,
    ino: INum,
    block_id: BlockId,
    block: Block,
    sender: oneshot::Sender<()>,
) where
    P: EvictPolicy<BlockCoordinate> + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    cache.backend().store(ino, block_id, block).await;
    sender.send(()).unwrap_or_else(|()| {
        warn!("The receiver of pending block is closed unexpectedly.");
    });
}

/// Flush a pending block to the backend
///
/// # Panics
/// If this function is called out of the context of a tokio runtime,
/// it will panic.
fn flush_a_block<P, S>(
    lru_queue: &mut LinkedHashSet<BlockCoordinate>,
    pending_blocks: &mut HashMap<INum, HashMap<BlockId, (Block, oneshot::Sender<()>)>>,
    task_queue: &mut VecDeque<JoinHandle<()>>,
    cache: &Arc<MemoryCache<P, S>>,
) where
    P: EvictPolicy<BlockCoordinate> + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    if let Some(BlockCoordinate(ino, block_id)) = lru_queue.pop_front() {
        if let Some((block, sender)) = pending_blocks
            .get_mut(&ino)
            .and_then(|file_level_pending_blocks| file_level_pending_blocks.remove(&block_id))
        {
            let handle = tokio::spawn(write_back_one_block(
                Arc::clone(cache),
                ino,
                block_id,
                block,
                sender,
            ));
            task_queue.push_back(handle);
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
    P: EvictPolicy<BlockCoordinate> + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    let file_level_pending_blocks = pending_blocks.remove(&ino);
    lru_queue.retain_with_order(|&BlockCoordinate(i, _)| i != ino);

    let mut handles = vec![];

    if let Some(file_level_pending_blocks) = file_level_pending_blocks {
        for (block_id, (block, sender)) in file_level_pending_blocks {
            let handle = tokio::spawn(write_back_one_block(
                Arc::clone(&cache),
                ino,
                block_id,
                block,
                sender,
            ));
            handles.push(handle);
        }
    }

    for handle in handles {
        if let Err(e) = handle.await {
            error!("Failed to flush a block, the nested error is: {e}");
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
    P: EvictPolicy<BlockCoordinate> + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    lru_queue.clear();

    let blocks = mem::take(pending_blocks);

    let mut handles = vec![];

    for (ino, file_level_pending_blocks) in blocks {
        for (block_id, (block, block_sender)) in file_level_pending_blocks {
            let handle = tokio::spawn(write_back_one_block(
                Arc::clone(&cache),
                ino,
                block_id,
                block,
                block_sender,
            ));
            handles.push(handle);
        }
    }

    for handle in handles {
        if let Err(e) = handle.await {
            error!("Failed to flush a block, the nested error is: {e}");
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
///
/// TODO: Make the write back task into a struct is better, as it's going to be
/// more complex.
#[allow(clippy::pattern_type_mismatch)]
pub(super) async fn run_write_back_task<P, S>(
    limit: SoftLimit,
    interval: Duration,
    storage: Weak<MemoryCache<P, S>>,
    mut command_receiver: mpsc::Receiver<Command>,
    command_queue_limit: usize,
) where
    P: EvictPolicy<BlockCoordinate> + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    let period = interval;
    let mut interval = tokio::time::interval(period);

    let mut lru_queue = LinkedHashSet::new();
    let mut pending_blocks: HashMap<INum, HashMap<BlockId, (Block, oneshot::Sender<()>)>> =
        HashMap::new();
    // A queue of tasks for the being written blocks
    let mut task_queue = VecDeque::new();

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
                    },
                    Some(Command::Cancel(ino)) => {
                        pending_blocks.remove(&ino);
                        lru_queue.retain_with_order(|&BlockCoordinate(i, _)| i != ino);
                    }
                    None => {
                        // The command sender is closed, meaning that the `Storage` is also dropped.
                        // Then the write back task should exit.
                        info!("Write back task exits.");
                        return;
                    },
                }
            },
            // In fact, there is no need to await the join handles.
            // When we can return a error from `Storage` methods,
            // the error can be returned by the sender, and is not needed
            // to be handled here.
            Some(res) = OptionFuture::from(task_queue.front_mut()) => {
                // `res` is the output of polling the front of `task_queue`.
                // So we can drop the popped front element.
                let _: Option<JoinHandle<()>> = task_queue.pop_front();
                if let Err(e) = res {
                    error!("Failed to flush a block, the nested error is: {e}");
                }
            },
            _ = interval.tick() => {
                // If the queue is empty, do evict;
                // Otherwise, flush a block to the backend
                if let Some(cache) = storage.upgrade() {
                    if lru_queue.is_empty() {
                        do_evict(limit, cache).await;
                    } else {
                        flush_a_block(&mut lru_queue, &mut pending_blocks, &mut task_queue, &cache);
                    }
                } else {
                    // The command sender is closed, meaning that the `Storage` is also dropped.
                    // Then the write back task should exit.
                    info!("Write back task exits.");
                    return;
                }
            },
            else => unreachable!("One of the branches above must be matched."),
        }
    }
}
