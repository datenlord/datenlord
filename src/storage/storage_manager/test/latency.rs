#![allow(clippy::unwrap_used)]

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use clippy_utilities::OverflowArithmetic;
use datenlord::config::SoftLimit;

use super::{BLOCK_CONTENT, BLOCK_SIZE_IN_BYTES};
use crate::storage::policy::LruPolicy;
use crate::storage::{BlockCoordinate, MemoryCacheBuilder, MemoryStorage, Storage, StorageManager};

macro_rules! elapsed {
    ($body:expr) => {{
        let now = tokio::time::Instant::now();
        let result = $body;
        let latency = now.elapsed();

        (latency, result)
    }};
}

async fn create_storage_with_latency(write_through: bool) -> Arc<StorageManager<impl Storage>> {
    let limit = SoftLimit(1, NonZeroUsize::new(1).unwrap());

    let backend = Arc::new(MemoryStorage::new(
        BLOCK_SIZE_IN_BYTES,
        Duration::from_millis(100),
    ));
    let lru = LruPolicy::<BlockCoordinate>::new(16);
    let cache = MemoryCacheBuilder::new(lru, Arc::clone(&backend), BLOCK_SIZE_IN_BYTES)
        .write_through(write_through)
        .limit(limit)
        .build()
        .await;
    Arc::new(StorageManager::new(cache, BLOCK_SIZE_IN_BYTES))
}

#[tokio::test]
async fn test_write_through_latency() {
    let storage = create_storage_with_latency(true).await;

    let (latency, mtime) = elapsed!(storage
        .store(0, 0, BLOCK_CONTENT, SystemTime::now())
        .await
        .unwrap());
    assert!(latency.as_millis() >= 100, "latency = {latency:?}");

    let (latency, _) = elapsed!(storage
        .load(0, 0, BLOCK_SIZE_IN_BYTES, mtime)
        .await
        .unwrap());
    assert!(latency.as_millis() < 2, "latency = {latency:?}");

    // Update
    let (latency, _) = elapsed!(storage
        .store(0, 0, &BLOCK_CONTENT[..4], mtime)
        .await
        .unwrap());
    assert!(latency.as_millis() >= 100, "latency = {latency:?}");

    // Invalidate the cache, and update
    let (latency, mtime) = elapsed!(storage
        .store(0, 0, &BLOCK_CONTENT[..4], SystemTime::now())
        .await
        .unwrap());
    assert!(latency.as_millis() >= 200, "latency = {latency:?}");

    let (latency, _) = elapsed!(storage
        .truncate(0, BLOCK_SIZE_IN_BYTES, 4, mtime)
        .await
        .unwrap());
    assert!(latency.as_millis() >= 100, "latency = {latency:?}");

    let (latency, _) = elapsed!(storage.truncate(0, 4, 1, SystemTime::now()).await.unwrap());
    assert!(latency.as_millis() >= 100, "latency = {latency:?}");

    // Invalid the cache, and load
    let (latency, _) = elapsed!(storage
        .load(0, 0, BLOCK_SIZE_IN_BYTES, SystemTime::now())
        .await
        .unwrap());
    assert!(latency.as_millis() >= 100, "latency = {latency:?}");
}

#[tokio::test]
async fn test_write_back_latency() {
    let storage = create_storage_with_latency(false).await;

    let (latency, mtime) = elapsed!(storage
        .store(0, 0, BLOCK_CONTENT, SystemTime::now())
        .await
        .unwrap());
    assert!(latency.as_millis() < 2, "latency = {latency:?}");

    let (latency, _) = elapsed!(storage
        .load(0, 0, BLOCK_SIZE_IN_BYTES, mtime)
        .await
        .unwrap());
    assert!(latency.as_millis() < 2, "latency = {latency:?}");

    // Update
    let (latency, _) = elapsed!(storage
        .store(0, 0, &BLOCK_CONTENT[..4], mtime)
        .await
        .unwrap());
    assert!(latency.as_millis() < 2, "latency = {latency:?}");

    // Invalidate the cache, and update
    let (latency, mtime) = elapsed!(storage
        .store(0, 0, &BLOCK_CONTENT[..4], SystemTime::now())
        .await
        .unwrap());
    assert!(latency.as_millis() >= 100, "latency = {latency:?}");

    let (latency, _) = elapsed!(storage
        .truncate(0, BLOCK_SIZE_IN_BYTES, 4, mtime)
        .await
        .unwrap());
    assert!(latency.as_millis() < 2, "latency = {latency:?}");

    let (latency, _) = elapsed!(storage.truncate(0, 4, 1, SystemTime::now()).await.unwrap());
    assert!(latency.as_millis() < 2, "latency = {latency:?}");

    // Invalidate the cache, and load
    let (latency, _) = elapsed!(storage
        .load(0, 0, BLOCK_SIZE_IN_BYTES, SystemTime::now())
        .await
        .unwrap());
    assert!(latency.as_millis() >= 100, "latency = {latency:?}");
}

#[tokio::test]
async fn test_write_back_flush_latency() {
    let storage = create_storage_with_latency(false).await;
    let mut mtime = SystemTime::now();
    for block_id in 0..8 {
        let offset = block_id.overflow_mul(BLOCK_SIZE_IN_BYTES);
        mtime = storage
            .store(0, offset, BLOCK_CONTENT, mtime)
            .await
            .unwrap();
    }

    // Wait for all blocks being flushed
    let (latency, ()) = elapsed!(storage.flush(0).await.unwrap());
    // TODO: this latency is 110?
    assert!(latency.as_millis() < 500, "latency = {latency:?}");
}
