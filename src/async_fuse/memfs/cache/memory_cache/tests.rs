#![allow(clippy::unwrap_used)]

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use datenlord::config::SoftLimit;

use super::{MemoryCache, MemoryCacheBuilder};
use crate::async_fuse::memfs::cache::mock::MemoryStorage;
use crate::async_fuse::memfs::cache::policy::LruPolicy;
use crate::async_fuse::memfs::cache::{Block, BlockCoordinate, Storage};

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
    let cache = MemoryCacheBuilder::new(policy, Arc::clone(&backend), BLOCK_SIZE_IN_BYTES).build();

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
/// The cache contains block `[0, 4)` for file `ino=0`. All the blocks are not
/// dirty.
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
    let (_, cache) = prepare_empty_storage();

    for block_id in 0..CACHE_CAPACITY_IN_BLOCKS {
        cache
            .store(0, block_id, Block::new_zeroed(BLOCK_SIZE_IN_BYTES))
            .await;
    }

    // The first tow blocks should be evicted.
    tokio::time::sleep(Duration::from_millis(250)).await;

    {
        let file_cache = cache.get_file_cache(0).unwrap().read_owned().await;
        assert!(!file_cache.contains_key(&0));
        assert!(!file_cache.contains_key(&1));
    }
}
