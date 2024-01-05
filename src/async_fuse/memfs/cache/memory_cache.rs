//! The in-memory cache.

use std::collections::{BTreeSet, HashMap as StdHashMap};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use clippy_utilities::OverflowArithmetic;
use lockfree_cuckoohash::{pin, LockFreeCuckooHash as HashMap};
use tokio::sync::{mpsc, oneshot, RwLock};
use tracing::warn;

use super::policy::EvictPolicy;
use super::{write_back_task, Block, BlockCoordinate, BlockId, Command, SoftLimit, Storage};
use crate::async_fuse::fuse::protocol::INum;

/// Merge the content from `src` to `dst`. This will set `dst` to be dirty.
fn merge_two_blocks(src: &Block, dst: &mut Block) {
    dst.set_dirty(true);
    dst.update(src);
}

/// A builder to configure and build a `MemoryCache`.
pub struct MemoryCacheBuilder<P, S> {
    /// The `policy` used by the built `MemoryCache`
    policy: P,
    /// The `backend` of the built `MemoryCache`
    backend: S,
    /// The size of blocks
    block_size: usize,
    /// A flag that if the built `MemoryCache` runs in writing-through policy
    write_through: bool,
    /// The soft limit for the write back task. See [`SoftLimit`] for details.
    limit: SoftLimit,
    /// The interval of the write back task
    interval: Duration,
    /// The limitation of the message queue of the write back task
    command_queue_limit: usize,
}

impl<P, S> MemoryCacheBuilder<P, S>
where
    P: EvictPolicy<BlockCoordinate> + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    /// Create a builder.
    pub fn new(policy: P, backend: S, block_size: usize) -> Self {
        let limit = SoftLimit(
            3,
            NonZeroUsize::new(5).unwrap_or_else(|| unreachable!("5 is not zero.")),
        );
        Self {
            policy,
            backend,
            block_size,
            write_through: true,
            limit,
            interval: Duration::from_millis(100),
            command_queue_limit: 1000,
        }
    }

    /// Set write policy. Write through is enabled by default.
    pub fn write_through(mut self, write_through: bool) -> Self {
        self.write_through = write_through;
        self
    }

    /// Set the soft limit for the write back task.
    ///
    /// See [`SoftLimit`] for details.
    pub fn limit(mut self, limit: SoftLimit) -> Self {
        self.limit = limit;
        self
    }

    /// Set the interval for the write back task
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Set the limitation of the message queue of the write back task.
    pub fn command_queue_limit(mut self, limit: usize) -> Self {
        self.command_queue_limit = limit;
        self
    }

    /// Builds a `MemoryCache`. Make sure that this method is called in `tokio` runtime.
    ///
    /// # Panic
    /// This method will panic if it's not called in a context of `tokio` runtime.
    pub fn build(self) -> Arc<MemoryCache<P, S>> {
        let MemoryCacheBuilder {
            policy,
            backend,
            block_size,
            write_through,
            limit,
            interval,
            command_queue_limit,
        } = self;

        let (sender, receiver) = mpsc::channel(command_queue_limit);

        let cache = Arc::new(MemoryCache {
            map: HashMap::new(),
            policy,
            backend,
            block_size,
            write_through,
            pending_write_back: RwLock::default(),
            command_sender: sender,
            truncate_records: HashMap::new(),
        });

        let weak = Arc::downgrade(&cache);
        tokio::spawn(write_back_task::run_write_back_task(
            limit,
            interval,
            weak,
            receiver,
            command_queue_limit,
        ));

        cache
    }
}

/// The file-level cache.
type FileCache = Arc<RwLock<StdHashMap<BlockId, Block>>>;

/// The file-level truncate record.
type TruncateRecord = Arc<RwLock<BTreeSet<BlockId>>>;

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
    /// TODO: receive a `Result` to handle the error
    pending_write_back: RwLock<StdHashMap<INum, Vec<oneshot::Receiver<()>>>>,
    /// A command sender for write back task
    command_sender: mpsc::Sender<Command>,
    /// A set of blocks to be removed, when a file is truncated.
    truncate_records: HashMap<INum, TruncateRecord>,
}

impl<P, S> MemoryCache<P, S> {
    /// The limit of retrying to insert a block into the cache.
    const INSERT_RETRY_LIMMIT: usize = 10;

    /// Get the `backend`.
    pub(super) fn backend(&self) -> &S {
        &self.backend
    }

    /// Get the `policy`.
    pub(super) fn policy(&self) -> &P {
        &self.policy
    }

    /// Get the file-level cache `HashMap`.
    fn get_file_cache(&self, ino: INum) -> Option<FileCache> {
        let guard = pin();
        self.map.get(&ino, &guard).cloned()
    }

    /// Get a block from the in-memory cache without fetch it from backend.
    ///
    /// Notice that this method will also not change the position of the block
    /// in the policy.
    async fn get_block_from_cache(&self, ino: INum, block_id: usize) -> Option<Block> {
        let block = if let Some(file_cache) = self.get_file_cache(ino) {
            file_cache.read().await.get(&block_id).cloned()
        } else {
            None
        };

        block
    }

    /// Send a block to the write back task, and it will be stored to the backend later.
    async fn send_block_to_write_back_task(&self, ino: INum, block_id: BlockId, block: Block) {
        let (sender, receiver) = oneshot::channel();

        self.command_sender
            .send(Command::Store {
                coord: BlockCoordinate(ino, block_id),
                block,
                sender,
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
            .push(receiver);
    }

    /// Update a block into cache in place.
    /// Return the block if success (the destination existing in cache),
    /// otherwise returns `None`.
    async fn update_block(&self, ino: INum, block_id: usize, src: &Block) -> Option<Block> {
        let res = if let Some(file_cache) = self.get_file_cache(ino) {
            let mut file_cache = file_cache.write().await;
            if let Some(block) = file_cache.get_mut(&block_id) {
                merge_two_blocks(src, block);
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
        res
    }

    /// Try to evict a block from the cache to backend, if needed.
    pub(super) async fn evict(&self)
    where
        P: EvictPolicy<BlockCoordinate> + Send + Sync,
        S: Storage + Send + Sync,
    {
        let evicted = self.policy.evict();
        // There is still a gap between the removal from policy and the lock on
        // file-level cache The evicted block may be modified and inserted to
        // the cache again during the gap This may cause the block evicted to
        // the backend incorrectly, but will not cause incorrect data fetched by
        // the user. TODO: Maybe it's better to lock the `policy` via a `Mutex`.
        if let Some(BlockCoordinate(ino, block_id)) = evicted {
            if let Some(file_cache) = self.get_file_cache(ino) {
                let mut file_cache = file_cache.write().await;

                // With the file-level write lock protected, there is no gap
                // between the removal of evicted block and writing to the backend
                if let Some(evicted) = file_cache.remove(&block_id) {
                    // A dirty block in cache must come from the backend (as it's already dirty when
                    // in backend), or be inserted into cache by storing, which
                    // will be written-through to the backend (if it's enabled). That means, if a
                    // block in cache is dirty, and the writing-through policy is enabled,
                    // it must have been shown in the backend. Therefore, we can just drop it when evicting.
                    if !self.write_through || !evicted.dirty() {
                        self.backend.store(ino, block_id, evicted).await;
                    }
                }

                if file_cache.capacity() >= file_cache.len().overflow_mul(2) {
                    file_cache.shrink_to_fit();
                }
            }
        }
    }

    /// Write a block into the cache without writing through.
    ///
    /// A block may be evicted to the backend.
    async fn write_block_into_cache(&self, ino: INum, block_id: usize, block: Block)
    where
        P: EvictPolicy<BlockCoordinate> + Send + Sync,
        S: Storage + Send + Sync,
    {
        let mut retry_times = 0;

        let mut file_cache = loop {
            // TODO: returns error instead of `panic`.
            assert!(
                retry_times < Self::INSERT_RETRY_LIMMIT,
                "Gave up retrying to insert a block into the cache."
            );

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
            self.evict().await;
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
    }
}

#[async_trait]
impl<P, S> Storage for MemoryCache<P, S>
where
    P: EvictPolicy<BlockCoordinate> + Send + Sync,
    S: Storage + Send + Sync,
{
    async fn load_from_self(&self, ino: INum, block_id: usize) -> Option<Block> {
        let res = self.get_block_from_cache(ino, block_id).await;
        if res.is_some() {
            self.policy.touch(&BlockCoordinate(ino, block_id));
        }
        res
    }

    async fn load_from_backend(&self, ino: INum, block_id: usize) -> Option<Block> {
        self.backend.load(ino, block_id).await
    }

    async fn cache_block_from_backend(&self, ino: INum, block_id: usize, block: Block) {
        self.write_block_into_cache(ino, block_id, block).await;
    }

    async fn store(&self, ino: INum, block_id: usize, input: Block) {
        let start_offset = input.start();
        let end_offset = input.end();

        // TODO: Return error instead of panic.
        assert!(end_offset <= self.block_size, "out of range");

        // If the writing block is the whole block, then there is no need to fetch a
        // block from cache or backend, as the block in storage will be
        // overwritten directly.
        if start_offset == 0 && end_offset == self.block_size {
            self.write_block_into_cache(ino, block_id, input.clone())
                .await;

            if self.write_through {
                self.backend.store(ino, block_id, input).await;
            } else {
                self.send_block_to_write_back_task(ino, block_id, input)
                    .await;
            }

            return;
        }

        let dirty_block = if let Some(inserted) = self.update_block(ino, block_id, &input).await {
            self.policy.touch(&BlockCoordinate(ino, block_id));
            inserted
        } else {
            let mut to_be_inserted = self.backend.load(ino, block_id).await.unwrap_or_else(|| {
                // Create a new block for write, despite the offset is larger than file size.
                Block::new_zeroed(self.block_size)
            });
            merge_two_blocks(&input, &mut to_be_inserted);
            self.write_block_into_cache(ino, block_id, to_be_inserted.clone())
                .await;

            to_be_inserted
        };

        if self.write_through {
            self.backend.store(ino, block_id, dirty_block).await;
        } else {
            self.send_block_to_write_back_task(ino, block_id, dirty_block)
                .await;
        }
    }

    async fn remove(&self, ino: INum) {
        self.map.remove(&ino);
        self.backend.remove(ino).await;
    }

    async fn invalidate(&self, ino: INum) {
        self.map.remove(&ino);
        self.backend.invalidate(ino).await;
    }

    async fn flush(&self, ino: INum) {
        if !self.write_through {
            {
                let mut pending_write_back = self.pending_write_back.write().await;
                if let Ok(()) = self.command_sender.send(Command::Flush(ino)).await {
                    if let Some(file_level_pending) = pending_write_back.remove(&ino) {
                        drop(pending_write_back);

                        #[allow(clippy::let_underscore_must_use)]
                        for receiver in file_level_pending {
                            // It's not a big deal that the oneshot sender is closed.
                            let _: Result<(), oneshot::error::RecvError> = receiver.await;
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
                    self.backend.flush(ino).await;
                    return;
                }

                let mut truncate_record_iter = truncate_record.iter().copied();

                let mut truncate_to = truncate_record_iter.next().unwrap_or(0);
                let mut last_block = truncate_to;

                for block_id in truncate_record_iter {
                    let truncate_from = last_block.overflow_add(1);

                    if block_id != truncate_from {
                        self.backend
                            .truncate(ino, truncate_from, truncate_to, self.block_size)
                            .await;
                        truncate_to = block_id;
                    }

                    last_block = block_id;
                }
                let truncate_from = last_block.overflow_add(1);
                self.backend
                    .truncate(ino, truncate_from, truncate_to, self.block_size)
                    .await;
                truncate_record.clear();
            }
        }

        self.backend.flush(ino).await;
    }

    async fn flush_all(&self) {
        if !self.write_through {
            let mut pending_write_back = self.pending_write_back.write().await;
            let (sender, receiver) = oneshot::channel();

            if let Ok(()) = self.command_sender.send(Command::FlushAll(sender)).await {
                if receiver.await.is_err() {
                    warn!("Receiver is closed unexpectedly.");
                }
                pending_write_back.clear();
            } else {
                warn!("Write back task is closed unexpectedly.");
            }
        }

        self.backend.flush_all().await;
    }

    async fn truncate(&self, ino: INum, from_block: usize, to_block: usize, fill_start: usize) {
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
                        fill_block_with_zeros(block);
                    } else if !self.write_through {
                        drop(file_cache);
                        let mut block = self
                            .load_from_backend(ino, fill_block_id)
                            .await
                            .unwrap_or_else(|| Block::new_zeroed(self.block_size));
                        fill_block_with_zeros(&mut block);
                        self.write_block_into_cache(ino, fill_block_id, block.clone())
                            .await;
                        self.send_block_to_write_back_task(ino, fill_block_id, block)
                            .await;
                    } else {
                        // This branch is not needed, but exists for clippy lint.
                    }
                }
            };
        }

        if self.write_through {
            self.backend
                .truncate(ino, from_block, to_block, fill_start)
                .await;
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
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::{num::NonZeroUsize, sync::Arc, time::Duration};

    use super::{Block, BlockCoordinate, MemoryCache, MemoryCacheBuilder, SoftLimit, Storage};
    use crate::async_fuse::memfs::cache::mock::MemoryStorage;
    use crate::async_fuse::memfs::cache::policy::LruPolicy;

    const BLOCK_SIZE_IN_BYTES: usize = 8;
    const BLOCK_CONTENT: &[u8; BLOCK_SIZE_IN_BYTES] = b"foo bar ";
    const CACHE_CAPACITY_IN_BLOCKS: usize = 4;

    type MemoryCacheType = MemoryCache<LruPolicy<BlockCoordinate>, Arc<MemoryStorage>>;

    fn prepare_empty_storage() -> (Arc<MemoryStorage>, Arc<MemoryCacheType>) {
        let policy = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
        let backend = Arc::new(MemoryStorage::new(
            BLOCK_SIZE_IN_BYTES,
            Duration::from_millis(0),
        ));
        let cache =
            MemoryCacheBuilder::new(policy, Arc::clone(&backend), BLOCK_SIZE_IN_BYTES).build();

        (backend, cache)
    }

    #[tokio::test]
    async fn test_write_whole_block() {
        let ino = 0;
        let block_id = 0;

        let (backend, cache) = prepare_empty_storage();

        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        cache.store(ino, block_id, block).await;

        let loaded_from_cache = cache.load(ino, block_id).await.unwrap();
        assert_eq!(loaded_from_cache.as_slice(), BLOCK_CONTENT);

        // test write through
        let loaded_from_backend = backend.load(ino, block_id).await.unwrap();
        assert_eq!(loaded_from_backend.as_slice(), loaded_from_cache.as_slice());
    }

    #[tokio::test]
    async fn test_overwrite() {
        let ino = 0;
        let block_id = 0;

        let (_, cache) = prepare_empty_storage();

        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        cache.store(ino, block_id, block).await;

        let block = Block::from_slice_with_range(BLOCK_SIZE_IN_BYTES, 4, 7, b"foo ");
        cache.store(ino, block_id, block).await;
        let loaded = cache.load(ino, block_id).await.unwrap();
        assert_eq!(loaded.as_slice(), b"foo foo ");

        let block = Block::from_slice_with_range(BLOCK_SIZE_IN_BYTES, 0, 4, b"bar xxx ");
        cache.store(ino, block_id, block).await;
        let loaded = cache.load(ino, block_id).await.unwrap();
        assert_eq!(loaded.as_slice(), b"bar foo ");
    }

    #[tokio::test]
    async fn test_load_inexist_block() {
        let ino = 0;
        let block_id = 0;

        let (_, cache) = prepare_empty_storage();

        let block = cache.load(ino, 1).await;
        assert!(block.is_none());

        let block = cache.load(1, block_id).await;
        assert!(block.is_none());
    }

    #[tokio::test]
    async fn test_append() {
        let ino = 0;

        let (_, cache) = prepare_empty_storage();

        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        cache.store(ino, 0, block).await;

        let block = Block::from_slice_with_range(BLOCK_SIZE_IN_BYTES, 0, 4, b"xxx foo ");
        cache.store(ino, 1, block).await;

        let loaded = cache.load(ino, 1).await.unwrap();
        assert_eq!(loaded.as_slice(), b"xxx \0\0\0\0");
    }

    #[tokio::test]
    async fn test_remove() {
        let ino = 0;
        let block_id = 0;

        let (backend, cache) = prepare_empty_storage();

        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        cache.store(ino, block_id, block).await;

        cache.remove(ino).await;
        assert!(!backend.contains(ino, block_id));

        let loaded = cache.load(ino, block_id).await;
        assert!(loaded.is_none());
    }

    /// Prepare a backend and cache for test eviction.
    ///
    /// The backend does not contain any block.
    /// The cache contains block `[0, 4)` for file `ino=0`. All the blocks are
    /// not dirty.
    async fn prepare_data_for_evict() -> (Arc<MemoryStorage>, Arc<MemoryCacheType>) {
        let limit = SoftLimit(1, NonZeroUsize::new(1).unwrap());

        let policy = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
        let backend = Arc::new(MemoryStorage::new(
            BLOCK_SIZE_IN_BYTES,
            Duration::from_millis(0),
        ));
        let cache = MemoryCacheBuilder::new(policy, Arc::clone(&backend), BLOCK_SIZE_IN_BYTES)
            .limit(limit) // Manually disable the write back task
            .interval(Duration::from_secs(1))
            .build();

        // Fill the backend
        for block_id in 0..CACHE_CAPACITY_IN_BLOCKS {
            let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
            backend.store(0, block_id, block).await;
        }

        // Warn up the data
        for block_id in 0..CACHE_CAPACITY_IN_BLOCKS {
            cache.load(0, block_id).await;
        }

        // Clear the backend
        backend.remove(0).await;

        (backend, cache)
    }

    #[tokio::test]
    async fn test_evict() {
        let (backend, cache) = prepare_data_for_evict().await;

        // LRU in cache: (0, 0) -> (0, 1) -> (0, 2) -> (0, 3)
        // Insert a block, and (0, 0) will be evicted
        assert!(!backend.contains(0, 0));
        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        cache.store(1, 0, block).await;
        let loaded = backend.load(0, 0).await.unwrap();
        assert_eq!(loaded.as_slice(), BLOCK_CONTENT);
    }

    #[tokio::test]
    async fn test_touch_by_load() {
        let (backend, cache) = prepare_data_for_evict().await;

        // LRU in cache: (0, 0) -> (0, 1) -> (0, 2) -> (0, 3)
        // Touch (0, 0) by loading
        let _: Option<Block> = cache.load(0, 0).await;

        // LRU in cache: (0, 1) -> (0, 2) -> (0, 3) -> (0, 0)
        // Insert a block, and (0, 1) will be evicted
        assert!(!backend.contains(0, 1));
        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        cache.store(1, 0, block).await;
        let loaded = backend.load(0, 1).await.unwrap();
        assert_eq!(loaded.as_slice(), BLOCK_CONTENT);
    }

    #[tokio::test]
    async fn test_touch_by_store() {
        let (backend, cache) = prepare_data_for_evict().await;

        // LRU in cache: (0, 0) -> (0, 1) -> (0, 2) -> (0, 3)
        // Touch (0, 0) by storing
        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        cache.store(0, 0, block).await;

        // LRU in cache: (0, 1) -> (0, 2) -> (0, 3) -> (0, 0)
        // Insert a block, and (0, 1) will be evicted
        assert!(!backend.contains(0, 1));
        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        cache.store(1, 0, block).await;
        let loaded = backend.load(0, 1).await.unwrap();
        assert_eq!(loaded.as_slice(), BLOCK_CONTENT);
    }

    #[tokio::test]
    async fn test_evict_dirty_block() {
        let (backend, cache) = prepare_empty_storage();

        // Fill the cache
        for block_id in 0..CACHE_CAPACITY_IN_BLOCKS {
            let mut block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
            block.set_dirty(true); // This will be done by `StorageManager` in productive env
            cache.store(0, block_id, block).await;
        }

        // Clear the backend
        backend.remove(0).await;

        // LRU in cache: (0, 0) -> (0, 1) -> (0, 2) -> (0, 3)
        // All of them are dirty
        // Insert a block, and (0, 0) will be evicted.
        // Because it's dirty, it will be dropped directly
        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        cache.store(1, 0, block).await;
        assert!(!backend.contains(0, 0));
    }

    #[tokio::test]
    async fn test_flush() {
        let ino = 0;
        let block_id = 0;

        let (backend, cache) = prepare_empty_storage();

        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        cache.store(ino, block_id, block).await;

        cache.flush(ino).await;
        assert!(backend.flushed(ino));

        cache.flush_all().await;
        assert!(backend.flushed(ino));
    }

    #[tokio::test]
    async fn test_load_from_backend() {
        let ino = 0;
        let block_id = 0;

        let (_, cache) = prepare_empty_storage();

        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        cache.store(ino, block_id, block).await;

        cache.invalidate(ino).await;
        let loaded_from_cache = cache.get_block_from_cache(ino, block_id).await;
        assert!(loaded_from_cache.is_none());

        let loaded = cache.load(ino, block_id).await.unwrap();
        assert_eq!(loaded.as_slice(), BLOCK_CONTENT);
    }

    #[tokio::test]
    async fn test_write_missing_block_in_middle() {
        let (_, cache) = prepare_empty_storage();

        let block = Block::from_slice_with_range(BLOCK_SIZE_IN_BYTES, 4, 7, &BLOCK_CONTENT[4..7]);
        cache.store(0, 0, block).await;
        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, b"\0\0\0\0bar\0");
        let loaded = cache.load(0, 0).await.unwrap();
        assert_eq!(loaded.as_slice(), block.as_slice());
    }

    #[tokio::test]
    async fn test_truncate() {
        let ino = 0;
        let from_block = 8;
        let to_block = 4;

        let (backend, cache) = prepare_empty_storage();

        for block_id in 0..from_block {
            cache
                .store(ino, block_id, Block::new_zeroed(BLOCK_SIZE_IN_BYTES))
                .await;
        }

        cache
            .truncate(ino, from_block, to_block, BLOCK_SIZE_IN_BYTES)
            .await;
        for block_id in to_block..from_block {
            assert!(!backend.contains(0, block_id));
            let block = cache.get_block_from_cache(ino, block_id).await;
            assert!(block.is_none());
        }
    }

    #[tokio::test]
    async fn test_truncate_may_fill() {
        let ino = 0;
        let from_block = 8;
        let to_block = 4;

        let (_, cache) = prepare_empty_storage();

        for block_id in 0..from_block {
            cache
                .store(0, block_id, Block::new_zeroed(BLOCK_SIZE_IN_BYTES))
                .await;
        }

        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);

        cache.store(ino, 3, block).await;
        cache.truncate(ino, from_block, to_block, 4).await;

        let loaded = cache.load(ino, 3).await.unwrap();
        assert_eq!(loaded.as_slice(), b"foo \0\0\0\0");
    }

    #[tokio::test]
    async fn test_truncate_in_the_same_block() {
        let (_, cache) = prepare_empty_storage();

        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);

        cache.store(0, 0, block).await;
        cache.truncate(0, 1, 1, 4).await;

        let loaded = cache.load(0, 0).await.unwrap();
        assert_eq!(loaded.as_slice(), b"foo \0\0\0\0");
    }

    #[tokio::test]
    #[should_panic(expected = "out of range")]
    async fn test_write_out_of_range() {
        let (_, cache) = prepare_empty_storage();

        let block = Block::new_zeroed(16);
        cache.store(0, 0, block).await;
    }

    fn prepare_empty_storage_with_write_back() -> (Arc<MemoryStorage>, Arc<MemoryCacheType>) {
        let policy = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
        let backend = Arc::new(MemoryStorage::new(
            BLOCK_SIZE_IN_BYTES,
            Duration::from_millis(0),
        ));
        let cache = MemoryCacheBuilder::new(policy, Arc::clone(&backend), BLOCK_SIZE_IN_BYTES)
            .write_through(false)
            .build();

        (backend, cache)
    }

    #[tokio::test]
    async fn test_store_write_back() {
        let (backend, cache) = prepare_empty_storage_with_write_back();

        let mut block = Block::new_zeroed(BLOCK_SIZE_IN_BYTES);
        block.set_dirty(true);
        cache.store(0, 0, block).await;
        let loaded = cache.load(0, 0).await;
        assert!(loaded.is_some());

        cache.flush(0).await;
        assert!(backend.contains(0, 0));

        let mut block = Block::new_zeroed(BLOCK_SIZE_IN_BYTES);
        block.set_dirty(true);
        cache.store(0, 1, block).await;
        cache.flush_all().await;
        assert!(backend.contains(0, 1));
    }

    #[tokio::test]
    async fn test_evict_dirty_block_with_write_back() {
        let (backend, cache) = prepare_empty_storage_with_write_back();

        // Fill the cache
        for block_id in 0..CACHE_CAPACITY_IN_BLOCKS {
            let mut block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
            block.set_dirty(true); // This will be done by `StorageManager` in productive env
            cache.store(0, block_id, block).await;
        }

        // Clear the backend
        backend.remove(0).await;

        // LRU in cache: (0, 0) -> (0, 1) -> (0, 2) -> (0, 3)
        // All of them are dirty
        // Insert a block, and (0, 0) will be evicted.
        // Because it's dirty, it will be dropped directly
        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        cache.store(1, 0, block).await;
        assert!(backend.contains(0, 0));
    }

    #[tokio::test]
    async fn test_truncate_write_back() {
        let (backend, cache) = prepare_empty_storage_with_write_back();

        for block_id in 0..4 {
            let mut block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
            block.set_dirty(true);
            cache.store(0, block_id, block).await;
        }

        cache.flush(0).await;

        cache.truncate(0, 4, 2, BLOCK_SIZE_IN_BYTES).await;

        assert!(backend.contains(0, 2));
        assert!(backend.contains(0, 3));

        cache.flush(0).await;
        assert!(!backend.contains(0, 2));
        assert!(!backend.contains(0, 3));
    }

    #[tokio::test]
    async fn test_truncate_in_middle_write_back() {
        let (backend, cache) = prepare_empty_storage_with_write_back();

        for block_id in 0..8 {
            let mut block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
            block.set_dirty(true);
            cache.store(0, block_id, block).await;
        }

        cache.flush(0).await;

        cache.truncate(0, 8, 2, BLOCK_SIZE_IN_BYTES).await;

        let mut block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        block.set_dirty(true);
        cache.store(0, 4, block).await;

        let mut block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        block.set_dirty(true);
        cache.store(0, 6, block).await;

        let loaded = cache.load(0, 4).await.unwrap();
        assert_eq!(loaded.as_slice(), BLOCK_CONTENT);
        let loaded = cache.load(0, 6).await.unwrap();
        assert_eq!(loaded.as_slice(), BLOCK_CONTENT);

        cache.flush(0).await;
        assert!(!backend.contains(0, 2));
        assert!(!backend.contains(0, 3));
        assert!(backend.contains(0, 4));
        assert!(!backend.contains(0, 5));
        assert!(backend.contains(0, 6));
        assert!(!backend.contains(0, 7));
    }

    #[tokio::test]
    async fn test_truncate_fill_write_back() {
        let (backend, cache) = prepare_empty_storage_with_write_back();

        for block_id in 0..8 {
            let mut block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
            block.set_dirty(true);
            cache.store(0, block_id, block).await;
        }
        cache.flush(0).await;

        cache.truncate(0, 8, 2, 4).await;

        let loaded = backend.load(0, 1).await.unwrap();
        assert_eq!(loaded.as_slice(), BLOCK_CONTENT);

        cache.flush(0).await;

        let loaded = backend.load(0, 1).await.unwrap();
        assert_eq!(loaded.as_slice(), b"foo \0\0\0\0");
    }

    #[tokio::test]
    async fn test_write_back_task() {
        // TODO: Should we test it in a UT?

        let (_, cache) = prepare_empty_storage();

        for block_id in 0..CACHE_CAPACITY_IN_BLOCKS {
            cache
                .store(0, block_id, Block::new_zeroed(BLOCK_SIZE_IN_BYTES))
                .await;
        }

        tokio::time::sleep(Duration::from_millis(250)).await;

        {
            let file_cache = cache.get_file_cache(0).unwrap().read_owned().await;
            assert!(!file_cache.contains_key(&0));
            assert!(!file_cache.contains_key(&1));
        }
    }
}
