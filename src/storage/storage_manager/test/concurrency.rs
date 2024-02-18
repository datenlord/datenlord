#![allow(clippy::unwrap_used)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use clippy_utilities::OverflowArithmetic;
use crossbeam_utils::atomic::AtomicCell;

use crate::async_fuse::fuse::fuse_reply::AsIoVec;
use crate::storage::policy::LruPolicy;
use crate::storage::{
    Block, BlockCoordinate, MemoryCacheBuilder, MemoryStorage, Storage, StorageManager,
};

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
        let n_mtime = storage
            .store(0, write_offset, CONTENT, mtime.load())
            .await
            .unwrap();
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
            .await
            .unwrap();
        assert!(!loaded.is_empty());
        let read_size: usize = loaded.iter().map(Block::len).sum();
        assert_eq!(read_size, REQUEST_SIZE);
    }
}

async fn create_storage_for_concurrent_test() -> Arc<StorageManager<impl Storage>> {
    let backend = Arc::new(MemoryStorage::new(BLOCK_SIZE, Duration::from_millis(0)));
    let lru = LruPolicy::<BlockCoordinate>::new(TOTAL_SIZE.overflow_div(BLOCK_SIZE));
    let cache = MemoryCacheBuilder::new(lru, Arc::clone(&backend), BLOCK_SIZE)
        .write_through(false)
        .command_queue_limit(500)
        .build()
        .await;
    Arc::new(StorageManager::new(cache, BLOCK_SIZE))
}

#[tokio::test]
async fn test_concurrency_aligned() {
    let byte_offset = 0;

    let storage = create_storage_for_concurrent_test().await;

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

    let storage = create_storage_for_concurrent_test().await;

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
