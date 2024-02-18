//! The `MemoryCache` implementation.

use std::collections::{BTreeSet, HashMap as StdHashMap};
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use clippy_utilities::OverflowArithmetic;
use datenlord::metrics::CACHE_METRICS;
use lockfree_cuckoohash::{pin, LockFreeCuckooHash as HashMap};
use tokio::sync::{mpsc, oneshot, RwLock};
use tracing::warn;

use super::write_back_task::Command;
use crate::async_fuse::fuse::protocol::INum;
use crate::storage::error::StorageResult;
use crate::storage::policy::EvictPolicy;
use crate::storage::{Block, BlockCoordinate, BlockId, Storage, StorageError};

/// Merge the content from `src` to `dst`. This will set `dst` to be dirty.
fn merge_two_blocks(src: &Block, dst: &mut Block) -> Result<(), StorageError> {
    dst.set_dirty(true);
    dst.update(src)
}

/// The file-level cache.
type FileCache = Arc<RwLock<StdHashMap<BlockId, Block>>>;

/// The file-level truncate record.
type TruncateRecord = Arc<RwLock<BTreeSet<BlockId>>>;

/// A receiver for storage result, used for write back task.
type StorageResultReceiver = oneshot::Receiver<StorageResult<()>>;

/// The in-memory cache, implemented with lockfree hashmaps.
#[derive(Debug)]
pub struct MemoryCache<P, S> {
    /// The inner map where the cached blocks stored
    map: HashMap<INum, FileCache>,
    /// The evict policy
    policy: P,
    /// The backend storage
    backend: S,
    /// The block size
    block_size: usize,
    /// A flag that if the `MemoryCache` runs in writing-through policy
    write_through: bool,
    /// The pending written-back blocks.
    pending_write_back: RwLock<StdHashMap<INum, Vec<StorageResultReceiver>>>,
    /// A command sender for write back task
    command_sender: mpsc::Sender<Command>,
    /// A set of blocks to be removed, when a file is truncated.
    truncate_records: HashMap<INum, TruncateRecord>,
}

impl<P, S> MemoryCache<P, S> {
    /// The limit of retrying to insert a block into the cache.
    const INSERT_RETRY_LIMMIT: usize = 10;

    /// Create a new memory cache.
    pub(super) fn new(
        policy: P,
        backend: S,
        block_size: usize,
        write_through: bool,
        command_sender: mpsc::Sender<Command>,
    ) -> Self {
        MemoryCache {
            map: HashMap::new(),
            policy,
            backend,
            block_size,
            write_through,
            pending_write_back: RwLock::default(),
            command_sender,
            truncate_records: HashMap::new(),
        }
    }

    /// Get the `backend`.
    pub(super) fn backend(&self) -> &S {
        &self.backend
    }

    /// Get the `policy`.
    pub(super) fn policy(&self) -> &P {
        &self.policy
    }

    /// Get the file-level cache `HashMap`.
    pub(super) fn get_file_cache(&self, ino: INum) -> Option<FileCache> {
        let guard = pin();
        self.map.get(&ino, &guard).cloned()
    }

    /// Get a block from the in-memory cache without fetch it from backend.
    ///
    /// Notice that this method will also not change the position of the block
    /// in the policy.
    pub(super) async fn get_block_from_cache(&self, ino: INum, block_id: usize) -> Option<Block> {
        let block = if let Some(file_cache) = self.get_file_cache(ino) {
            file_cache.read().await.get(&block_id).cloned()
        } else {
            None
        };

        block
    }

    /// Send a block to the write back task, and it will be stored to the
    /// backend later.
    async fn send_block_to_write_back_task(&self, ino: INum, block_id: BlockId, block: Block) {
        let (tx, rx) = oneshot::channel();

        self.command_sender
            .send(Command::Store {
                coord: BlockCoordinate(ino, block_id),
                block,
                tx,
            })
            .await
            .unwrap_or_else(|_| {
                panic!("Command receiver in write back task is closed unexpectedly.")
            });
        self.pending_write_back
            .write()
            .await
            .entry(ino)
            .or_default()
            .push(rx);
    }

    /// Update a block into cache in place.
    /// Return the block if success (the destination existing in cache),
    /// otherwise returns `None`.
    async fn update_block(
        &self,
        ino: INum,
        block_id: usize,
        src: &Block,
    ) -> StorageResult<Option<Block>> {
        let res = if let Some(file_cache) = self.get_file_cache(ino) {
            let mut file_cache = file_cache.write().await;
            if let Some(block) = file_cache.get_mut(&block_id) {
                merge_two_blocks(src, block)?;
                if !self.write_through {
                    if let Some(truncate_record) = {
                        let guard = pin();
                        self.truncate_records.get(&ino, &guard).cloned()
                    } {
                        let mut truncate_record = truncate_record.write_owned().await;
                        truncate_record.remove(&block_id);
                    }
                }
                Some(block.clone())
            } else {
                None
            }
        } else {
            None
        };
        Ok(res)
    }

    /// Try to evict a block from the cache to backend, if needed.
    pub(super) async fn evict(&self) -> StorageResult<()>
    where
        P: EvictPolicy<BlockCoordinate> + Send + Sync,
        S: Storage + Send + Sync,
    {
        let evicted = self.policy.evict();
        // There is still a gap between the removal from policy and the lock on
        // file-level cache The evicted block may be modified and inserted to
        // the cache again during the gap This may cause the block evicted to
        // the backend incorrectly, but will not cause incorrect data fetched by
        // the user.
        if let Some(BlockCoordinate(ino, block_id)) = evicted {
            if let Some(file_cache) = self.get_file_cache(ino) {
                let mut file_cache = file_cache.write().await;

                // With the file-level write lock protected, there is no gap
                // between the removal of evicted block and writing to the backend
                if let Some(evicted) = file_cache.remove(&block_id) {
                    // A dirty block in cache must come from the backend (as it's already dirty when
                    // in backend), or be inserted into cache by storing, which
                    // will be written-through to the backend (if it's enabled).
                    // That means, if a block in cache is dirty, and the writing-through policy is
                    // enabled, it must have been shown in the backend.
                    // Therefore, we can just drop it when evicting.
                    if !self.write_through || !evicted.dirty() {
                        self.backend.store(ino, block_id, evicted).await?;
                    }
                }

                if file_cache.capacity() >= file_cache.len().overflow_mul(2) {
                    file_cache.shrink_to_fit();
                }
            }
        }

        Ok(())
    }

    /// Write a block into the cache without writing through.
    ///
    /// A block may be evicted to the backend.
    async fn write_block_into_cache(
        &self,
        ino: INum,
        block_id: usize,
        block: Block,
    ) -> StorageResult<()>
    where
        P: EvictPolicy<BlockCoordinate> + Send + Sync,
        S: Storage + Send + Sync,
    {
        let mut retry_times = 0;

        let mut file_cache = loop {
            if retry_times >= Self::INSERT_RETRY_LIMMIT {
                return Err(anyhow!("Gave up retrying to insert a block into the cache.").into());
            }

            let file_cache = {
                let guard = pin();
                Arc::clone(self.map.get_or_insert(ino, Arc::default(), &guard)).write_owned()
            }
            .await;

            let success = self.policy.try_put(BlockCoordinate(ino, block_id));

            if success {
                break file_cache;
            }
            drop(file_cache);
            self.evict().await?;
            retry_times = retry_times.overflow_add(1);
        };

        file_cache.insert(block_id, block);

        if !self.write_through {
            if let Some(truncate_record) = {
                let guard = pin();
                self.truncate_records.get(&ino, &guard).cloned()
            } {
                let mut truncate_record = truncate_record.write_owned().await;
                truncate_record.remove(&block_id);
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<P, S> Storage for MemoryCache<P, S>
where
    P: EvictPolicy<BlockCoordinate> + Send + Sync,
    S: Storage + Send + Sync,
{
    async fn load_from_self(&self, ino: INum, block_id: usize) -> StorageResult<Option<Block>> {
        let res = self.get_block_from_cache(ino, block_id).await;
        if res.is_some() {
            CACHE_METRICS.cache_hit_count_inc("memory");
            self.policy.touch(&BlockCoordinate(ino, block_id));
        }
        Ok(res)
    }

    async fn load_from_backend(&self, ino: INum, block_id: usize) -> StorageResult<Option<Block>> {
        let res = self.backend.load(ino, block_id).await;

        if let Ok(Some(_)) = res {
            // The cache is considered missed, only if the block exists in the backend.
            CACHE_METRICS.cache_miss_count_inc("memory");
        }

        res
    }

    async fn cache_block_from_backend(
        &self,
        ino: INum,
        block_id: usize,
        block: Block,
    ) -> StorageResult<()> {
        self.write_block_into_cache(ino, block_id, block).await?;
        Ok(())
    }

    async fn store(&self, ino: INum, block_id: usize, input: Block) -> StorageResult<()> {
        let start_offset = input.start();
        let end_offset = input.end();

        if end_offset > self.block_size {
            return Err(StorageError::OutOfRange {
                maximum: self.block_size,
                found: end_offset,
            });
        }

        // If the writing block is the whole block, then there is no need to fetch a
        // block from cache or backend, as the block in storage will be
        // overwritten directly.
        if start_offset == 0 && end_offset == self.block_size {
            self.write_block_into_cache(ino, block_id, input.clone())
                .await?;

            if self.write_through {
                self.backend.store(ino, block_id, input).await?;
            } else {
                self.send_block_to_write_back_task(ino, block_id, input)
                    .await;
            }

            return Ok(());
        }

        let dirty_block = if let Some(inserted) = self.update_block(ino, block_id, &input).await? {
            CACHE_METRICS.cache_hit_count_inc("memory");
            self.policy.touch(&BlockCoordinate(ino, block_id));
            inserted
        } else {
            CACHE_METRICS.cache_miss_count_inc("memory");
            let mut to_be_inserted = self.backend.load(ino, block_id).await?.unwrap_or_else(|| {
                // Create a new block for write, despite the offset is larger than file size.
                Block::new_zeroed(self.block_size)
            });
            merge_two_blocks(&input, &mut to_be_inserted)?;
            self.write_block_into_cache(ino, block_id, to_be_inserted.clone())
                .await?;

            to_be_inserted
        };

        if self.write_through {
            self.backend.store(ino, block_id, dirty_block).await?;
        } else {
            self.send_block_to_write_back_task(ino, block_id, dirty_block)
                .await;
        }

        Ok(())
    }

    async fn remove(&self, ino: INum) -> StorageResult<()> {
        if !self.write_through {
            let mut pending_write_back = self.pending_write_back.write().await;
            if self
                .command_sender
                .send(Command::Cancel(ino))
                .await
                .is_err()
            {
                warn!("Write back task is closed unexpectedly.");
            }
            pending_write_back.remove(&ino);
        }
        self.map.remove(&ino);
        self.backend.remove(ino).await?;

        Ok(())
    }

    async fn invalidate(&self, ino: INum) -> StorageResult<()> {
        self.map.remove(&ino);
        self.backend.invalidate(ino).await?;

        Ok(())
    }

    async fn flush(&self, ino: INum) -> StorageResult<()> {
        if !self.write_through {
            {
                let mut pending_write_back = self.pending_write_back.write().await;
                if let Ok(()) = self.command_sender.send(Command::Flush(ino)).await {
                    if let Some(file_level_pending) = pending_write_back.remove(&ino) {
                        drop(pending_write_back);

                        #[allow(clippy::let_underscore_must_use)]
                        for receiver in file_level_pending {
                            // It's not a big deal that the oneshot sender is closed.
                            if let Ok(Err(e)) = receiver.await {
                                return Err(e);
                            }
                        }
                    }
                } else {
                    warn!("Write back task is closed unexpectedly.");
                }
            }

            if let Some(truncate_record) = {
                let guard = pin();
                self.truncate_records
                    .remove_with_guard(&ino, &guard)
                    .cloned()
            } {
                let mut truncate_record = truncate_record.write_owned().await;

                if truncate_record.is_empty() {
                    self.backend.flush(ino).await?;
                    return Ok(());
                }

                let mut truncate_record_iter = truncate_record.iter().copied();

                let mut truncate_to = truncate_record_iter.next().unwrap_or(0);
                let mut last_block = truncate_to;

                for block_id in truncate_record_iter {
                    let truncate_from = last_block.overflow_add(1);

                    if block_id != truncate_from {
                        self.backend
                            .truncate(ino, truncate_from, truncate_to, self.block_size)
                            .await?;
                        truncate_to = block_id;
                    }

                    last_block = block_id;
                }
                let truncate_from = last_block.overflow_add(1);
                self.backend
                    .truncate(ino, truncate_from, truncate_to, self.block_size)
                    .await?;
                truncate_record.clear();
            }
        }

        self.backend.flush(ino).await?;

        Ok(())
    }

    async fn flush_all(&self) -> StorageResult<()> {
        if !self.write_through {
            let mut pending_write_back = self.pending_write_back.write().await;
            let (tx, rx) = oneshot::channel();

            if let Ok(()) = self.command_sender.send(Command::FlushAll(tx)).await {
                if rx.await.is_err() {
                    warn!("Receiver is closed unexpectedly.");
                }
                pending_write_back.clear();
            } else {
                warn!("Write back task is closed unexpectedly.");
            }
        }

        self.backend.flush_all().await?;

        Ok(())
    }

    async fn truncate(
        &self,
        ino: INum,
        from_block: usize,
        to_block: usize,
        fill_start: usize,
    ) -> StorageResult<()> {
        debug_assert!(from_block >= to_block);

        let fill_block_with_zeros = |block: &mut Block| {
            if fill_start >= block.end() {
                return;
            }

            block.set_start(block.start().min(fill_start));

            let fill_len = block.end().overflow_sub(fill_start);
            let fill_content = vec![0; fill_len];
            let write_start = fill_start.overflow_sub(block.start());

            block
                .make_mut_slice()
                .get_mut(write_start..)
                .unwrap_or_else(|| {
                    unreachable!("`fill_start` is checked to be less than block size.")
                })
                .copy_from_slice(&fill_content);

            block.set_dirty(true);
        };

        {
            if let Some(file_cache) = self.get_file_cache(ino) {
                let mut file_cache = file_cache.write().await;
                for block_id in to_block..from_block {
                    file_cache.remove(&block_id);
                }

                if to_block > 0 && fill_start < self.block_size {
                    let fill_block_id = to_block.overflow_sub(1);
                    if let Some(block) = file_cache.get_mut(&fill_block_id) {
                        CACHE_METRICS.cache_hit_count_inc("memory");
                        fill_block_with_zeros(block);
                    } else if !self.write_through {
                        drop(file_cache);
                        CACHE_METRICS.cache_miss_count_inc("memory");
                        let mut block = self
                            .load_from_backend(ino, fill_block_id)
                            .await?
                            .unwrap_or_else(|| Block::new_zeroed(self.block_size));
                        fill_block_with_zeros(&mut block);
                        self.write_block_into_cache(ino, fill_block_id, block.clone())
                            .await?;
                        self.send_block_to_write_back_task(ino, fill_block_id, block)
                            .await;
                    } else {
                        // This branch is not needed, but exists for clippy
                        // lint.
                    }
                }
            };
        }

        if self.write_through {
            self.backend
                .truncate(ino, from_block, to_block, fill_start)
                .await?;
        } else {
            let mut truncate_record = {
                let guard = &pin();
                Arc::clone(self.truncate_records.get_or_insert(
                    ino,
                    TruncateRecord::default(),
                    guard,
                ))
                .write_owned()
            }
            .await;

            truncate_record.extend(to_block..from_block);
        }

        Ok(())
    }
}
