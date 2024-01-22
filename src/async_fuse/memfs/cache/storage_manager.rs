//! The storage manager.

use std::sync::Arc;
use std::time::SystemTime;

use clippy_utilities::OverflowArithmetic;
use lockfree_cuckoohash::{pin, LockFreeCuckooHash as HashMap};
use tokio::task;

use super::{Block, Storage};
use crate::async_fuse::fuse::protocol::INum;

/// The storage manager, which exposes the interfaces to `FileSystem` for
/// interacting with the storage layers.
#[derive(Debug)]
pub struct StorageManager<S> {
    /// The top-level storage, `InMemoryCache` for example
    storage: Arc<S>,
    /// Block size in bytes
    block_size: usize,
    /// Last modified times of the cache (on file level)
    mtimes: HashMap<INum, SystemTime>,
}

impl<S> StorageManager<S>
where
    S: Storage + Send + Sync + 'static,
{
    /// Create a `StorageManager` with the top-level `Storage`.
    pub fn new(storage: S, block_size: usize) -> Self {
        StorageManager {
            storage: Arc::new(storage),
            block_size,
            mtimes: HashMap::new(),
        }
    }

    /// Convert offset in byte to block id via the equation:
    ///
    /// `block_id = offset / block_size`
    fn offset_to_block_id(&self, offset: usize) -> usize {
        offset.overflow_div(self.block_size)
    }

    /// Load blocks from the storage concurrently.
    async fn load_blocks(&self, ino: INum, start_block: usize, end_block: usize) -> Vec<Block> {
        let mut handles = vec![];

        for block_id in start_block..end_block {
            let storage = Arc::clone(&self.storage);
            let handle = task::spawn(async move { storage.load(ino, block_id).await });
            handles.push(handle);
        }

        let mut blocks = vec![];

        for (handle, block_id) in handles.into_iter().zip(start_block..end_block) {
            let block = handle
                .await
                .unwrap_or_else(|e| panic!("Fails when awaiting on loading: {e}"));
            if let Some(block) = block {
                blocks.push(block);
            } else {
                let mut zero_filled = Block::new_zeroed(self.block_size);
                zero_filled.set_dirty(true);
                self.storage.store(ino, block_id, zero_filled.clone()).await;
                blocks.push(zero_filled);
            }
        }

        blocks
    }

    /// Store blocks into the storage concurrently.
    async fn store_blocks(
        &self,
        ino: INum,
        start_block: usize,
        io_blocks: impl Iterator<Item = Block>,
    ) {
        let mut handles = vec![];

        for (mut io_block, block_id) in io_blocks.zip(start_block..) {
            io_block.set_dirty(true);
            let storage = Arc::clone(&self.storage);
            let handle = task::spawn(async move { storage.store(ino, block_id, io_block).await });
            handles.push(handle);
        }

        for handle in handles {
            handle
                .await
                .unwrap_or_else(|e| panic!("Fails when awaiting on storing: {e}"));
        }
    }

    /// Convert slice to `Block`s.
    fn make_blocks_from_slice(&self, offset: usize, data: &[u8]) -> Vec<Block> {
        let data_len = data.len();
        let block_num = {
            let end_offset = offset.overflow_add(data_len);
            let end_block_id = self.offset_to_block_id(end_offset.overflow_sub(1));
            let start_block_id = self.offset_to_block_id(offset);
            end_block_id.overflow_sub(start_block_id).overflow_add(1)
        };
        let mut blocks = vec![];

        // Handle the first block
        let first_block_offset = offset.overflow_rem(self.block_size);
        let first_block_end = first_block_offset
            .overflow_add(data_len)
            .min(self.block_size);
        let first_block_length = first_block_end.overflow_sub(first_block_offset);

        let (first_block_data, rest_data) = data.split_at(first_block_length);

        let first_block = Block::from_slice_with_range(
            self.block_size,
            first_block_offset,
            first_block_end,
            first_block_data,
        );
        blocks.push(first_block);

        // Handle the rest blocks
        if data_len > first_block_length {
            for (i, chunk) in rest_data.chunks(self.block_size).enumerate() {
                let end = if i == block_num.overflow_sub(2) {
                    first_block_offset
                        .overflow_add(data_len)
                        .overflow_sub(1)
                        .overflow_rem(self.block_size)
                        .overflow_add(1)
                } else {
                    self.block_size
                };

                let block = Block::from_slice_with_range(self.block_size, 0, end, chunk);
                blocks.push(block);
            }
        }

        blocks
    }

    /// Load data from storage.
    pub async fn load(
        &self,
        ino: INum,
        offset: usize,
        len: usize,
        mtime: SystemTime,
    ) -> Vec<Block> {
        // Check if the cache is valid.
        let invalid = {
            let guard = pin();
            let cache_mtime = self.mtimes.get(&ino, &guard);
            cache_mtime != Some(&mtime)
        };

        if invalid {
            self.storage.invalidate(ino).await;
        }

        if len == 0 {
            return vec![];
        }

        // Calculate the `[start_block, end_block)` range.
        let start_block = self.offset_to_block_id(offset);
        let end_block = self
            .offset_to_block_id(offset.overflow_add(len).overflow_sub(1))
            .overflow_add(1);

        let mut blocks = self.load_blocks(ino, start_block, end_block).await;

        // If the cache is invalidated, it must be re-fetched from backend.
        // So the mtime of the cache should be updated to the passed-in one.
        if invalid {
            self.mtimes.insert(ino, mtime);
        }

        if let Some(first_block) = blocks.first_mut() {
            first_block.set_start(offset.overflow_rem(self.block_size));
        }

        if let Some(last_block) = blocks.last_mut() {
            last_block.set_end(
                offset
                    .overflow_add(len)
                    .overflow_sub(1)
                    .overflow_rem(self.block_size)
                    .overflow_add(1),
            );
        }

        blocks
    }

    /// Store data into storage.
    pub async fn store(
        &self,
        ino: INum,
        offset: usize,
        data: &[u8],
        mtime: SystemTime,
    ) -> SystemTime {
        // Check if the cache is valid.
        let invalid = {
            let guard = pin();
            let cache_mtime = self.mtimes.get(&ino, &guard);
            cache_mtime != Some(&mtime)
        };

        if invalid {
            self.storage.invalidate(ino).await;
        }

        if data.is_empty() {
            // The cache is invalid, but no new blocks will be loaded into the cache
            // thus the mtime of this file is removed.
            if invalid {
                self.mtimes.remove(&ino);
            }
            // No data will be written, so the passed-in mtime will be passed-out
            // changelessly.
            return mtime;
        }

        let start_block = self.offset_to_block_id(offset);

        let io_blocks = self.make_blocks_from_slice(offset, data);

        self.store_blocks(ino, start_block, io_blocks.into_iter())
            .await;

        // As the cache is overwritten, the cache mtime should be set to now.
        let new_mtime = SystemTime::now();
        self.mtimes.insert(ino, new_mtime);

        new_mtime
    }

    /// Remove a file from the storage.
    pub async fn remove(&self, ino: INum) {
        self.mtimes.remove(&ino);
        self.storage.remove(ino).await;
    }

    /// Flush the cache to the persistent layer.
    pub async fn flush(&self, ino: INum) {
        self.storage.flush(ino).await;
    }

    /// Flush all file in the cache to persistent layer.
    pub async fn flush_all(&self) {
        self.storage.flush_all().await;
    }

    /// Truncate a file, from size of `from` to size of `to`.
    /// Both `from` and `to` are in bytes, and are excluded from the range.
    /// After the truncating, the valid range of a file in the storage
    /// is `[0, to)`.
    ///
    /// This method performs a store operation, therefore it also takes a
    /// `mtime` and returns a new `mtime` like `store` method.
    pub async fn truncate(
        &self,
        ino: INum,
        from: usize,
        to: usize,
        mtime: SystemTime,
    ) -> SystemTime {
        let invalid = {
            let guard = pin();
            let cache_mtime = self.mtimes.get(&ino, &guard);
            cache_mtime != Some(&mtime)
        };

        if invalid {
            self.storage.invalidate(ino).await;
        }

        if from <= to {
            if invalid {
                self.mtimes.remove(&ino);
            }

            return mtime;
        }

        let from_block = self
            .offset_to_block_id(from.overflow_sub(1))
            .overflow_add(1);

        let to_block = if to == 0 {
            0
        } else {
            self.offset_to_block_id(to.overflow_sub(1)).overflow_add(1)
        };

        let fill_start = if to == 0 {
            0
        } else {
            to.overflow_sub(1)
                .overflow_rem(self.block_size)
                .overflow_add(1)
        };

        self.storage
            .truncate(ino, from_block, to_block, fill_start)
            .await;

        let new_mtime = SystemTime::now();
        self.mtimes.insert(ino, new_mtime);

        new_mtime
    }
}

#[cfg(test)]
#[allow(clippy::indexing_slicing)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use clippy_utilities::OverflowArithmetic;

    use crate::async_fuse::memfs::cache::mock::MemoryStorage;
    use crate::async_fuse::memfs::cache::policy::LruPolicy;
    use crate::async_fuse::memfs::cache::{
        Block, BlockCoordinate, MemoryCache, MemoryCacheBuilder, Storage, StorageManager,
    };

    const BLOCK_SIZE_IN_BYTES: usize = 8;
    const BLOCK_CONTENT: &[u8; BLOCK_SIZE_IN_BYTES] = b"foo bar ";
    const CACHE_CAPACITY_IN_BLOCKS: usize = 4;

    type MemoryCacheType = MemoryCache<LruPolicy<BlockCoordinate>, Arc<MemoryStorage>>;

    fn create_storage() -> (Arc<MemoryStorage>, StorageManager<Arc<MemoryCacheType>>) {
        let backend = Arc::new(MemoryStorage::new(
            BLOCK_SIZE_IN_BYTES,
            Duration::from_millis(0),
        ));
        let lru = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
        let cache = MemoryCacheBuilder::new(lru, Arc::clone(&backend), BLOCK_SIZE_IN_BYTES).build();
        let storage = StorageManager::new(cache, BLOCK_SIZE_IN_BYTES);

        (backend, storage)
    }

    #[tokio::test]
    async fn test_read_write_single_block() {
        let ino = 0;
        let offset = 0;
        let mtime = SystemTime::now();

        let (_, storage) = create_storage();

        let new_mtime = storage.store(ino, offset, BLOCK_CONTENT, mtime).await;

        let loaded = storage.load(ino, 4, 4, new_mtime).await;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].as_slice(), b"bar ");

        let loaded = storage.load(ino, 0, BLOCK_SIZE_IN_BYTES, new_mtime).await;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].as_slice(), BLOCK_CONTENT);
    }

    #[tokio::test]
    async fn test_read_write_miltiple_blocks() {
        let ino = 0;
        let offset = 0;
        let mut mtime = SystemTime::now();

        let (_, storage) = create_storage();

        let content = BLOCK_CONTENT.repeat(3);
        mtime = storage.store(ino, offset, content.as_slice(), mtime).await;

        let loaded = storage
            .load(ino, 0, BLOCK_SIZE_IN_BYTES.overflow_mul(3), mtime)
            .await;
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[0].as_slice(), b"foo bar ");
        assert_eq!(loaded[1].as_slice(), b"foo bar ");
        assert_eq!(loaded[2].as_slice(), b"foo bar ");
    }

    #[tokio::test]
    async fn test_overwrite_between_blocks() {
        let ino = 0;
        let offset = 0;
        let mut mtime = SystemTime::now();

        let (_, storage) = create_storage();

        let content = BLOCK_CONTENT.repeat(3);
        mtime = storage.store(ino, offset, content.as_slice(), mtime).await;

        // ori: b"foo bar foo bar foo bar "
        //                   "foo bar "
        // res: b"foo bar foo foo bar bar "
        mtime = storage.store(ino, 12, BLOCK_CONTENT, mtime).await;

        let loaded = storage
            .load(ino, 8, BLOCK_SIZE_IN_BYTES.overflow_mul(2), mtime)
            .await;
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].as_slice(), b"foo foo ");
        assert_eq!(loaded[1].as_slice(), b"bar bar ");

        let loaded = storage.load(ino, 12, BLOCK_SIZE_IN_BYTES, mtime).await;
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].as_slice(), b"foo ");
        assert_eq!(loaded[1].as_slice(), b"bar ");
    }

    #[tokio::test]
    async fn test_overwrite_second_blocks() {
        let ino = 0;
        let offset = 0;
        let mut mtime = SystemTime::now();

        let (_, storage) = create_storage();

        let content = BLOCK_CONTENT.repeat(3);
        mtime = storage.store(ino, offset, content.as_slice(), mtime).await;

        // ori: b"foo bar foo bar foo bar "
        //               "2000"
        // res: b"foo bar 2000bar foo bar "
        let new_mtime = storage.store(ino, 8, b"2000", mtime).await;
        let loaded = storage
            .load(ino, 0, BLOCK_SIZE_IN_BYTES.overflow_add(4), new_mtime)
            .await;
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].as_slice(), b"foo bar ");
        assert_eq!(loaded[1].as_slice(), b"2000");
    }

    #[tokio::test]
    async fn test_read_write_inexist_block() {
        const ZEROED_BLOCK: &[u8; BLOCK_SIZE_IN_BYTES] = &[0_u8; BLOCK_SIZE_IN_BYTES];

        let ino = 0;
        let offset = 0;
        let mtime = SystemTime::now();

        let (_, storage) = create_storage();

        let loaded = storage.load(ino, offset, BLOCK_SIZE_IN_BYTES, mtime).await;

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].as_slice(), ZEROED_BLOCK);

        let loaded = storage
            .load(
                ino,
                BLOCK_SIZE_IN_BYTES,
                BLOCK_SIZE_IN_BYTES.overflow_mul(2),
                mtime,
            )
            .await;
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].as_slice(), ZEROED_BLOCK);
        assert_eq!(loaded[0].as_slice(), ZEROED_BLOCK);
    }

    #[tokio::test]
    async fn test_zero_size_read_write() {
        let ino = 0;
        let offset = 0;
        let mtime = SystemTime::UNIX_EPOCH;

        let (_, storage) = create_storage();

        let new_mtime = storage.store(ino, offset, BLOCK_CONTENT, mtime).await;
        assert_ne!(mtime, new_mtime);

        let loaded = storage.load(ino, offset, 0, new_mtime).await;
        assert!(loaded.is_empty());

        let just_now = SystemTime::now();
        let mtime_from_store = storage.store(ino, offset, b"", just_now).await;
        assert_eq!(just_now, mtime_from_store);
    }

    #[tokio::test]
    async fn test_flush() {
        let (backend, storage) = create_storage();

        storage.flush(0).await;
        assert!(backend.flushed(0));

        storage.store(0, 0, b"foo bar ", SystemTime::now()).await;
        storage.flush_all().await;
        assert!(backend.flushed(0));
    }

    #[tokio::test]
    async fn test_invalid_cache_on_read() {
        let ino = 0;
        let offset = 0;

        let (backend, storage) = create_storage();

        let mtime = storage
            .store(ino, offset, BLOCK_CONTENT, SystemTime::now())
            .await;
        let loaded = storage.load(ino, offset, BLOCK_SIZE_IN_BYTES, mtime).await;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].as_slice(), b"foo bar ");

        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, b"bar foo ");

        // Simulating a modify on another node
        backend.store(ino, 0, block).await;
        // If we use the old mtime for loading, this node won't load the newest data
        let loaded = storage.load(ino, 0, BLOCK_SIZE_IN_BYTES, mtime).await;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].as_slice(), b"foo bar ");

        // Then we can use a new mtime to invalidate the cache
        let loaded = storage
            .load(ino, 0, BLOCK_SIZE_IN_BYTES, mtime + Duration::from_secs(10))
            .await;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].as_slice(), b"bar foo ");
    }

    #[tokio::test]
    async fn test_invalid_cache_on_write() {
        let ino = 0;
        let offset = 0;

        let (backend, storage) = create_storage();

        let mtime = storage
            .store(ino, offset, BLOCK_CONTENT, SystemTime::now())
            .await;
        let loaded = storage.load(ino, offset, BLOCK_SIZE_IN_BYTES, mtime).await;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].as_slice(), b"foo bar ");

        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, b"bar foo ");

        // Simulating a modify on another node
        backend.store(ino, 0, block).await;
        // Use a new mtime to invalidate the cache
        let mtime = storage.store(ino, 0, b"foo ", SystemTime::now()).await;
        let loaded = storage.load(ino, offset, BLOCK_SIZE_IN_BYTES, mtime).await;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].as_slice(), b"foo foo ");
    }

    #[tokio::test]
    async fn test_remove() {
        let ino = 0;
        let offset = 0;

        let (_, storage) = create_storage();

        let mtime = storage
            .store(ino, offset, BLOCK_CONTENT, SystemTime::now())
            .await;
        let loaded = storage.load(ino, offset, BLOCK_SIZE_IN_BYTES, mtime).await;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].as_slice(), b"foo bar ");

        let zeroed_block = Block::new_zeroed(storage.block_size);
        storage.remove(ino).await;
        let loaded = storage.load(ino, offset, BLOCK_SIZE_IN_BYTES, mtime).await;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].as_slice(), zeroed_block.as_slice());
    }

    #[tokio::test]
    async fn test_truncate() {
        let ino = 0;
        let offset = 0;
        let content = BLOCK_CONTENT.repeat(8);
        let truncate_from = content.len();
        let truncate_to = 30;

        let (backend, storage) = create_storage();

        let mtime = storage
            .store(ino, offset, &content, SystemTime::now())
            .await;

        let mtime = storage
            .truncate(ino, truncate_from, truncate_to, mtime)
            .await;
        assert!(backend.contains(ino, 3));

        for block_id in 4..8 {
            assert!(!backend.contains(ino, block_id));
        }

        // Zeros are filled in the last block.
        let loaded = storage.load(ino, truncate_to, 2, mtime).await;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].as_slice(), b"\0\0");

        let epoch = storage.truncate(ino, 0, 4, SystemTime::UNIX_EPOCH).await;
        assert_eq!(epoch, SystemTime::UNIX_EPOCH);
    }

    #[tokio::test]
    async fn test_truncate_remove() {
        let (backend, storage) = create_storage();

        let mtime = storage.store(0, 0, BLOCK_CONTENT, SystemTime::now()).await;

        let _: SystemTime = storage.truncate(0, BLOCK_SIZE_IN_BYTES, 0, mtime).await;

        assert!(!backend.contains(0, 0));
    }

    #[allow(clippy::unwrap_used)]
    mod concurrency {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::time::{Duration, SystemTime};

        use clippy_utilities::OverflowArithmetic;
        use crossbeam_utils::atomic::AtomicCell;

        use super::{
            Block, BlockCoordinate, LruPolicy, MemoryCacheBuilder, MemoryStorage, Storage,
            StorageManager,
        };
        use crate::async_fuse::fuse::fuse_reply::AsIoVec;

        const BLOCK_SIZE: usize = 16;
        const REQUEST_SIZE: usize = 8;
        const TOTAL_SIZE: usize = 0x10_0000;
        const REQUEST_NUM: usize = TOTAL_SIZE.wrapping_div(REQUEST_SIZE);

        const CONTENT: &[u8; 8] = b"foo bar ";

        async fn create_writer<S>(
            storage: Arc<StorageManager<S>>,
            write_pointer: Arc<AtomicUsize>,
            mtime: Arc<AtomicCell<SystemTime>>,
            byte_offset: usize,
        ) where
            S: Storage + Send + Sync + 'static,
        {
            for request_id in 0..REQUEST_NUM {
                let write_offset = request_id
                    .overflow_mul(REQUEST_SIZE)
                    .overflow_add(byte_offset);
                let n_mtime = storage.store(0, write_offset, CONTENT, mtime.load()).await;
                mtime.store(n_mtime);
                write_pointer.store(request_id.overflow_add(1), Ordering::Release);
            }
        }

        async fn create_reader<S>(
            storage: Arc<StorageManager<S>>,
            write_pointer: Arc<AtomicUsize>,
            mtime: Arc<AtomicCell<SystemTime>>,
            request_offset: usize,
            byte_offset: usize,
            step: usize,
        ) where
            S: Storage + Send + Sync + 'static,
        {
            for request_id in (request_offset..REQUEST_NUM).step_by(step) {
                let read_offset = request_id
                    .overflow_mul(REQUEST_SIZE)
                    .overflow_add(byte_offset);

                // Wait until the writer has written the block.
                while write_pointer.load(Ordering::Acquire) <= request_id {
                    tokio::task::yield_now().await;
                }

                let loaded = storage
                    .load(0, read_offset, REQUEST_SIZE, mtime.load())
                    .await;
                assert!(!loaded.is_empty());
                let read_size: usize = loaded.iter().map(Block::len).sum();
                assert_eq!(read_size, REQUEST_SIZE);
            }
        }

        fn create_storage_for_concurrent_test() -> Arc<StorageManager<impl Storage>> {
            let backend = Arc::new(MemoryStorage::new(BLOCK_SIZE, Duration::from_millis(0)));
            let lru = LruPolicy::<BlockCoordinate>::new(TOTAL_SIZE.overflow_div(BLOCK_SIZE));
            let cache = MemoryCacheBuilder::new(lru, Arc::clone(&backend), BLOCK_SIZE)
                .write_through(false)
                .command_queue_limit(500)
                .build();
            Arc::new(StorageManager::new(cache, BLOCK_SIZE))
        }

        #[tokio::test]
        async fn test_concurrency_aligned() {
            let byte_offset = 0;

            let storage = create_storage_for_concurrent_test();

            let write_pointer = Arc::new(AtomicUsize::new(0));
            let mtime = Arc::new(AtomicCell::new(SystemTime::now()));

            // writer
            let writer = {
                let storage = Arc::clone(&storage);
                let write_pointer = Arc::clone(&write_pointer);
                let mtime = Arc::clone(&mtime);
                tokio::spawn(create_writer(storage, write_pointer, mtime, byte_offset))
            };

            // reader 1
            let reader1 = {
                let storage = Arc::clone(&storage);
                let write_pointer = Arc::clone(&write_pointer);
                let mtime = Arc::clone(&mtime);
                tokio::spawn(create_reader(
                    storage,
                    write_pointer,
                    mtime,
                    0,
                    byte_offset,
                    2,
                ))
            };

            // reader 2
            let reader2 = {
                let storage = Arc::clone(&storage);
                let write_pointer = Arc::clone(&write_pointer);
                let mtime = Arc::clone(&mtime);
                tokio::spawn(create_reader(
                    storage,
                    write_pointer,
                    mtime,
                    1,
                    byte_offset,
                    2,
                ))
            };

            writer.await.unwrap();
            reader1.await.unwrap();
            reader2.await.unwrap();
        }

        #[tokio::test]
        async fn test_concurrency_unaligned() {
            let byte_offset = 4;

            let storage = create_storage_for_concurrent_test();

            let write_pointer = Arc::new(AtomicUsize::new(0));
            let mtime = Arc::new(AtomicCell::new(SystemTime::now()));

            // writer
            let writer = {
                let storage = Arc::clone(&storage);
                let write_pointer = Arc::clone(&write_pointer);
                let mtime = Arc::clone(&mtime);
                tokio::spawn(create_writer(storage, write_pointer, mtime, byte_offset))
            };

            // reader 1
            let reader1 = {
                let storage = Arc::clone(&storage);
                let write_pointer = Arc::clone(&write_pointer);
                let mtime = Arc::clone(&mtime);
                tokio::spawn(create_reader(
                    storage,
                    write_pointer,
                    mtime,
                    0,
                    byte_offset,
                    2,
                ))
            };

            // reader 2
            let reader2 = {
                let storage = Arc::clone(&storage);
                let write_pointer = Arc::clone(&write_pointer);
                let mtime = Arc::clone(&mtime);
                tokio::spawn(create_reader(
                    storage,
                    write_pointer,
                    mtime,
                    1,
                    byte_offset,
                    2,
                ))
            };

            writer.await.unwrap();
            reader1.await.unwrap();
            reader2.await.unwrap();
        }
    }

    #[allow(clippy::unwrap_used)]
    mod latency {
        use std::num::NonZeroUsize;
        use std::sync::Arc;
        use std::time::{Duration, SystemTime};

        use clippy_utilities::OverflowArithmetic;
        use datenlord::config::SoftLimit;

        use super::{
            BlockCoordinate, LruPolicy, MemoryCacheBuilder, MemoryStorage, Storage, StorageManager,
            BLOCK_CONTENT, BLOCK_SIZE_IN_BYTES,
        };

        macro_rules! elapsed {
            ($body:expr) => {{
                let now = tokio::time::Instant::now();
                let result = $body;
                let latency = now.elapsed();

                (latency, result)
            }};
        }

        fn create_storage_with_latency(write_through: bool) -> Arc<StorageManager<impl Storage>> {
            let limit = SoftLimit(1, NonZeroUsize::new(1).unwrap());

            let backend = Arc::new(MemoryStorage::new(
                BLOCK_SIZE_IN_BYTES,
                Duration::from_millis(100),
            ));
            let lru = LruPolicy::<BlockCoordinate>::new(16);
            let cache = MemoryCacheBuilder::new(lru, Arc::clone(&backend), BLOCK_SIZE_IN_BYTES)
                .write_through(write_through)
                .limit(limit)
                .build();
            Arc::new(StorageManager::new(cache, BLOCK_SIZE_IN_BYTES))
        }

        #[tokio::test]
        async fn test_write_through_latency() {
            let storage = create_storage_with_latency(true);

            let (latency, mtime) =
                elapsed!(storage.store(0, 0, BLOCK_CONTENT, SystemTime::now()).await);
            assert!(latency.as_millis() >= 100, "latency = {latency:?}");

            let (latency, _) = elapsed!(storage.load(0, 0, BLOCK_SIZE_IN_BYTES, mtime).await);
            assert!(latency.as_millis() < 2, "latency = {latency:?}");

            // Update
            let (latency, _) = elapsed!(storage.store(0, 0, &BLOCK_CONTENT[..4], mtime).await);
            assert!(latency.as_millis() >= 100, "latency = {latency:?}");

            // Invalidate the cache, and update
            let (latency, mtime) = elapsed!(
                storage
                    .store(0, 0, &BLOCK_CONTENT[..4], SystemTime::now())
                    .await
            );
            assert!(latency.as_millis() >= 200, "latency = {latency:?}");

            let (latency, _) = elapsed!(storage.truncate(0, BLOCK_SIZE_IN_BYTES, 4, mtime).await);
            assert!(latency.as_millis() >= 100, "latency = {latency:?}");

            let (latency, _) = elapsed!(storage.truncate(0, 4, 1, SystemTime::now()).await);
            assert!(latency.as_millis() >= 100, "latency = {latency:?}");

            // Invalid the cache, and load
            let (latency, _) = elapsed!(
                storage
                    .load(0, 0, BLOCK_SIZE_IN_BYTES, SystemTime::now())
                    .await
            );
            assert!(latency.as_millis() >= 100, "latency = {latency:?}");
        }

        #[tokio::test]
        async fn test_write_back_latency() {
            let storage = create_storage_with_latency(false);

            let (latency, mtime) =
                elapsed!(storage.store(0, 0, BLOCK_CONTENT, SystemTime::now()).await);
            assert!(latency.as_millis() < 2, "latency = {latency:?}");

            let (latency, _) = elapsed!(storage.load(0, 0, BLOCK_SIZE_IN_BYTES, mtime).await);
            assert!(latency.as_millis() < 2, "latency = {latency:?}");

            // Update
            let (latency, _) = elapsed!(storage.store(0, 0, &BLOCK_CONTENT[..4], mtime).await);
            assert!(latency.as_millis() < 2, "latency = {latency:?}");

            // Invalidate the cache, and update
            let (latency, mtime) = elapsed!(
                storage
                    .store(0, 0, &BLOCK_CONTENT[..4], SystemTime::now())
                    .await
            );
            assert!(latency.as_millis() >= 100, "latency = {latency:?}");

            let (latency, _) = elapsed!(storage.truncate(0, BLOCK_SIZE_IN_BYTES, 4, mtime).await);
            assert!(latency.as_millis() < 2, "latency = {latency:?}");

            let (latency, _) = elapsed!(storage.truncate(0, 4, 1, SystemTime::now()).await);
            assert!(latency.as_millis() < 2, "latency = {latency:?}");

            // Invalidate the cache, and load
            let (latency, _) = elapsed!(
                storage
                    .load(0, 0, BLOCK_SIZE_IN_BYTES, SystemTime::now())
                    .await
            );
            assert!(latency.as_millis() >= 100, "latency = {latency:?}");
        }

        #[tokio::test]
        async fn test_write_back_flush_latency() {
            let storage = create_storage_with_latency(false);
            let mut mtime = SystemTime::now();
            for block_id in 0..8 {
                let offset = block_id.overflow_mul(BLOCK_SIZE_IN_BYTES);
                mtime = storage.store(0, offset, BLOCK_CONTENT, mtime).await;
            }

            // Wait for all blocks being flushed
            tokio::time::sleep(Duration::from_millis(900)).await;

            let (latency, ()) = elapsed!(storage.flush(0).await);
            assert!(latency.as_millis() < 10, "latency = {latency:?}");
        }
    }
}
