#![allow(clippy::indexing_slicing, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use clippy_utilities::OverflowArithmetic;

use super::{BLOCK_CONTENT, BLOCK_SIZE_IN_BYTES, CACHE_CAPACITY_IN_BLOCKS};
use crate::storage::mock::MemoryStorage;
use crate::storage::policy::LruPolicy;
use crate::storage::{
    Block, BlockCoordinate, MemoryCache, MemoryCacheBuilder, Storage, StorageManager,
};

type MemoryCacheType = MemoryCache<LruPolicy<BlockCoordinate>, Arc<MemoryStorage>>;

async fn create_storage() -> (Arc<MemoryStorage>, StorageManager<Arc<MemoryCacheType>>) {
    let backend = Arc::new(MemoryStorage::new(
        BLOCK_SIZE_IN_BYTES,
        Duration::from_millis(0),
    ));
    let lru = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
    let cache = MemoryCacheBuilder::new(lru, Arc::clone(&backend), BLOCK_SIZE_IN_BYTES)
        .build()
        .await;
    let storage = StorageManager::new(cache, BLOCK_SIZE_IN_BYTES);

    (backend, storage)
}

#[tokio::test]
async fn test_read_write_single_block() {
    let ino = 0;
    let offset = 0;
    let mtime = SystemTime::now();

    let (_, storage) = create_storage().await;

    let new_mtime = storage
        .store(ino, offset, BLOCK_CONTENT, mtime)
        .await
        .unwrap();

    let loaded = storage.load(ino, 4, 4, new_mtime).await.unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].as_slice(), b"bar ");

    let loaded = storage
        .load(ino, 0, BLOCK_SIZE_IN_BYTES, new_mtime)
        .await
        .unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].as_slice(), BLOCK_CONTENT);
}

#[tokio::test]
async fn test_read_write_miltiple_blocks() {
    let ino = 0;
    let offset = 0;
    let mut mtime = SystemTime::now();

    let (_, storage) = create_storage().await;

    let content = BLOCK_CONTENT.repeat(3);
    mtime = storage
        .store(ino, offset, content.as_slice(), mtime)
        .await
        .unwrap();

    let loaded = storage
        .load(ino, 0, BLOCK_SIZE_IN_BYTES.overflow_mul(3), mtime)
        .await
        .unwrap();
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

    let (_, storage) = create_storage().await;

    let content = BLOCK_CONTENT.repeat(3);
    mtime = storage
        .store(ino, offset, content.as_slice(), mtime)
        .await
        .unwrap();

    // ori: b"foo bar foo bar foo bar "
    //                   "foo bar "
    // res: b"foo bar foo foo bar bar "
    mtime = storage.store(ino, 12, BLOCK_CONTENT, mtime).await.unwrap();

    let loaded = storage
        .load(ino, 8, BLOCK_SIZE_IN_BYTES.overflow_mul(2), mtime)
        .await
        .unwrap();
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].as_slice(), b"foo foo ");
    assert_eq!(loaded[1].as_slice(), b"bar bar ");

    let loaded = storage
        .load(ino, 12, BLOCK_SIZE_IN_BYTES, mtime)
        .await
        .unwrap();
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].as_slice(), b"foo ");
    assert_eq!(loaded[1].as_slice(), b"bar ");
}

#[tokio::test]
async fn test_overwrite_second_blocks() {
    let ino = 0;
    let offset = 0;
    let mut mtime = SystemTime::now();

    let (_, storage) = create_storage().await;

    let content = BLOCK_CONTENT.repeat(3);
    mtime = storage
        .store(ino, offset, content.as_slice(), mtime)
        .await
        .unwrap();

    // ori: b"foo bar foo bar foo bar "
    //               "2000"
    // res: b"foo bar 2000bar foo bar "
    let new_mtime = storage.store(ino, 8, b"2000", mtime).await.unwrap();
    let loaded = storage
        .load(ino, 0, BLOCK_SIZE_IN_BYTES.overflow_add(4), new_mtime)
        .await
        .unwrap();
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

    let (_, storage) = create_storage().await;

    let loaded = storage
        .load(ino, offset, BLOCK_SIZE_IN_BYTES, mtime)
        .await
        .unwrap();

    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].as_slice(), ZEROED_BLOCK);

    let loaded = storage
        .load(
            ino,
            BLOCK_SIZE_IN_BYTES,
            BLOCK_SIZE_IN_BYTES.overflow_mul(2),
            mtime,
        )
        .await
        .unwrap();
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].as_slice(), ZEROED_BLOCK);
    assert_eq!(loaded[0].as_slice(), ZEROED_BLOCK);
}

#[tokio::test]
async fn test_zero_size_read_write() {
    let ino = 0;
    let offset = 0;
    let mtime = SystemTime::UNIX_EPOCH;

    let (_, storage) = create_storage().await;

    let new_mtime = storage
        .store(ino, offset, BLOCK_CONTENT, mtime)
        .await
        .unwrap();
    assert_ne!(mtime, new_mtime);

    let loaded = storage.load(ino, offset, 0, new_mtime).await.unwrap();
    assert!(loaded.is_empty());

    let just_now = SystemTime::now();
    let mtime_from_store = storage.store(ino, offset, b"", just_now).await.unwrap();
    assert_eq!(just_now, mtime_from_store);
}

#[tokio::test]
async fn test_flush() {
    let (backend, storage) = create_storage().await;

    storage.flush(0).await.unwrap();
    assert!(backend.flushed(0));

    storage
        .store(0, 0, b"foo bar ", SystemTime::now())
        .await
        .unwrap();
    storage.flush_all().await.unwrap();
    assert!(backend.flushed(0));
}

#[tokio::test]
async fn test_invalid_cache_on_read() {
    let ino = 0;
    let offset = 0;

    let (backend, storage) = create_storage().await;

    let mtime = storage
        .store(ino, offset, BLOCK_CONTENT, SystemTime::now())
        .await
        .unwrap();
    let loaded = storage
        .load(ino, offset, BLOCK_SIZE_IN_BYTES, mtime)
        .await
        .unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].as_slice(), b"foo bar ");

    let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, b"bar foo ");

    // Simulating a modify on another node
    backend.store(ino, 0, block).await.unwrap();
    // If we use the old mtime for loading, this node won't load the newest data
    let loaded = storage
        .load(ino, 0, BLOCK_SIZE_IN_BYTES, mtime)
        .await
        .unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].as_slice(), b"foo bar ");

    // Then we can use a new mtime to invalidate the cache
    let loaded = storage
        .load(ino, 0, BLOCK_SIZE_IN_BYTES, mtime + Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].as_slice(), b"bar foo ");
}

#[tokio::test]
async fn test_invalid_cache_on_write() {
    let ino = 0;
    let offset = 0;

    let (backend, storage) = create_storage().await;

    let mtime = storage
        .store(ino, offset, BLOCK_CONTENT, SystemTime::now())
        .await
        .unwrap();
    let loaded = storage
        .load(ino, offset, BLOCK_SIZE_IN_BYTES, mtime)
        .await
        .unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].as_slice(), b"foo bar ");

    let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, b"bar foo ");

    // Simulating a modify on another node
    backend.store(ino, 0, block).await.unwrap();
    // Use a new mtime to invalidate the cache
    let mtime = storage
        .store(ino, 0, b"foo ", SystemTime::now())
        .await
        .unwrap();
    let loaded = storage
        .load(ino, offset, BLOCK_SIZE_IN_BYTES, mtime)
        .await
        .unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].as_slice(), b"foo foo ");
}

#[tokio::test]
async fn test_remove() {
    let ino = 0;
    let offset = 0;

    let (_, storage) = create_storage().await;

    let mtime = storage
        .store(ino, offset, BLOCK_CONTENT, SystemTime::now())
        .await
        .unwrap();
    let loaded = storage
        .load(ino, offset, BLOCK_SIZE_IN_BYTES, mtime)
        .await
        .unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].as_slice(), b"foo bar ");

    let zeroed_block = Block::new_zeroed(BLOCK_SIZE_IN_BYTES);
    storage.remove(ino).await.unwrap();
    let loaded = storage
        .load(ino, offset, BLOCK_SIZE_IN_BYTES, mtime)
        .await
        .unwrap();
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

    let (backend, storage) = create_storage().await;

    let mtime = storage
        .store(ino, offset, &content, SystemTime::now())
        .await
        .unwrap();

    let mtime = storage
        .truncate(ino, truncate_from, truncate_to, mtime)
        .await
        .unwrap();
    assert!(backend.contains(ino, 3));

    for block_id in 4..8 {
        assert!(!backend.contains(ino, block_id));
    }

    // Zeros are filled in the last block.
    let loaded = storage.load(ino, truncate_to, 2, mtime).await.unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].as_slice(), b"\0\0");

    let epoch = storage
        .truncate(ino, 0, 4, SystemTime::UNIX_EPOCH)
        .await
        .unwrap();
    assert_eq!(epoch, SystemTime::UNIX_EPOCH);
}

#[tokio::test]
async fn test_truncate_remove() {
    let (backend, storage) = create_storage().await;

    let mtime = storage
        .store(0, 0, BLOCK_CONTENT, SystemTime::now())
        .await
        .unwrap();

    let _: SystemTime = storage
        .truncate(0, BLOCK_SIZE_IN_BYTES, 0, mtime)
        .await
        .unwrap();

    assert!(!backend.contains(0, 0));
}
