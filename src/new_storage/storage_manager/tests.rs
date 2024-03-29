#![allow(
    clippy::integer_division,
    clippy::as_conversions,
    clippy::unwrap_used,
    clippy::big_endian_bytes,
    clippy::indexing_slicing,
    clippy::float_arithmetic,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::BufMut;
use clippy_utilities::Cast;
use datenlord::metrics::LossyCast;
use parking_lot::Mutex;
use rand::distributions::Distribution;
use rand::{thread_rng, Rng};
use rand_distr::Zipf;

use super::super::backend::memory_backend::MemoryBackend;
use super::super::{Backend, MemoryCache, OpenFlag, Storage, StorageManager, BLOCK_SIZE};
use crate::new_storage::format_path;

// Bench time : default is 30s
const BENCH_TIME: u64 = 30;

// Backend latency : default is 0ms
const BACKEND_LATENCY: Duration = Duration::from_millis(0);

// Scan read thread num
const SCAN_READ_THREAD_NUM: u64 = 4;

// Random get thread num
const RANDOM_GET_THREAD_NUM: u64 = 4;

// Total test pages : default is 256
const TOTAL_TEST_BLOCKS: usize = 256;
// mb
const TOTAL_SIZE: usize = TOTAL_TEST_BLOCKS * BLOCK_SIZE / 1024 / 1024;

const IO_SIZE: usize = 1024;

/// Fd Allocator
static CURRENT_FD: AtomicU64 = AtomicU64::new(4);

fn modify_data(data: &mut Vec<u8>, user_index: u64) {
    let mut rng = thread_rng();
    let seed = rng.gen();
    data.put_u64(seed);
    data.put_u64(user_index);
    data.extend_from_slice(&[1; IO_SIZE - 16]);
    let index = 16 + seed % (IO_SIZE as u64 - 16);
    data[index as usize] = seed as u8;
    assert_eq!(data.len(), IO_SIZE);
}

fn check_data(data: &[u8], user_index: u64) {
    // To ensure that `data` is long enough to contain 2 `u64` and an extra byte
    assert!(data.len() > 17, "Data does not have enough bytes.");

    // get `seed` and `block_id`
    let seed = u64::from_be_bytes(data[0..8].try_into().unwrap());
    let stored_block_id = u64::from_be_bytes(data[8..16].try_into().unwrap());

    // check if the `block_id` matches
    assert_eq!(
        user_index, stored_block_id,
        "Block ID does not match. Expected {user_index}, found {stored_block_id}."
    );

    // Check if the byte at the position calculatd by `seed` matches
    let expected_byte = seed as u8;
    let index = 16 + seed % (IO_SIZE as u64 - 16);
    let actual_byte = data[index as usize];
    assert_eq!(expected_byte, actual_byte, "Data at calculated position does not match. Expected {expected_byte}, found {actual_byte}.");
}

// Open a read file handle ,read to end, but don't close the handle
async fn warm_up(storage: Arc<StorageManager>, ino: u64) {
    let flag = OpenFlag::Read;
    let fh = CURRENT_FD.fetch_add(1, Ordering::SeqCst);
    storage.open(ino, fh, flag);
    for i in 0..TOTAL_TEST_BLOCKS {
        let buf = storage
            .read(ino, fh, (i * IO_SIZE) as u64, IO_SIZE)
            .await
            .unwrap();
        assert_eq!(buf.len(), IO_SIZE);
    }
}

async fn seq_read(storage: Arc<StorageManager>, ino: u64) {
    let flag = OpenFlag::Read;
    let fh = CURRENT_FD.fetch_add(1, Ordering::SeqCst);
    storage.open(ino, fh, flag);
    for i in 0..TOTAL_TEST_BLOCKS {
        let buf = storage
            .read(10, fh, (i * IO_SIZE) as u64, IO_SIZE)
            .await
            .unwrap();
        assert_eq!(buf.len(), IO_SIZE);
        check_data(&buf, i as u64);
    }
    storage.close(fh).await;
}

async fn create_a_file(storage: Arc<StorageManager>, ino: u64) {
    let flag = OpenFlag::Write;
    let fh = CURRENT_FD.fetch_add(1, Ordering::SeqCst);
    storage.open(ino, fh, flag);
    let start = std::time::Instant::now();
    for i in 0..TOTAL_TEST_BLOCKS {
        let mut content = Vec::new();
        modify_data(&mut content, i as u64);
        storage
            .write(ino, fh, (i * IO_SIZE) as u64, &content)
            .await
            .unwrap();
    }
    storage.flush(ino, fh).await.unwrap();
    storage.close(fh).await;
    let end = std::time::Instant::now();
    let throughput = TOTAL_SIZE.lossy_cast() / (end - start).as_secs_f64();
    println!(
        "Create a file ino {} cost {:?} thoughput: {} MB/s",
        ino,
        end - start,
        throughput
    );
    let size = storage.len();
    println!("Cache size: {size}");
}

async fn concurrency_read() {
    println!("Concurrency read test");
    println!("Write 1GB data to the storage");
    println!("Then warm up the cache");
    println!("Then do the concurrency read test, we don't close the reader handle");
    println!("In order to keep the cache warm");
    let backend = Arc::new(MemoryBackend::new(BACKEND_LATENCY));
    // Only 1 file, 256 blocks , the cache will never miss
    let manager = Arc::new(Mutex::new(MemoryCache::new(500)));
    let storage = Arc::new(StorageManager::new(manager, backend));
    create_a_file(Arc::clone(&storage), 10).await;
    warm_up(Arc::clone(&storage), 10).await;
    // Concurrency read ,thread num : 1,2,4,8
    for i in 0..5 {
        let mut tasks = vec![];
        let start = tokio::time::Instant::now();
        for _ in 0..2_usize.pow(i) {
            let storage = Arc::clone(&storage);
            tasks.push(tokio::spawn(async move {
                seq_read(storage, 10).await;
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        let end = tokio::time::Instant::now();
        // throuput = 1GB/ time * thread num
        let throuput =
            TOTAL_SIZE.lossy_cast() / (end - start).as_secs_f64() * 2_usize.pow(i).lossy_cast();
        println!(
            "thread num : {}, read Time: {:?} thoughput: {} MB/s",
            2_usize.pow(i),
            end - start,
            throuput
        );
        // assert!(storage.len() == 0);
    }
}

async fn concurrency_read_with_write() {
    println!("Concurrency read with write test");
    println!("Write 1GB data to the storage");
    println!("Then warm up the cache");
    println!(
        "Then do the concurrency read test, and a worker will write another file to the storage"
    );
    let backend = Arc::new(MemoryBackend::new(BACKEND_LATENCY));
    // Only 1 file, 256 blocks , the cache will never miss
    let manager = Arc::new(Mutex::new(MemoryCache::new(2 * TOTAL_TEST_BLOCKS + 10)));
    let storage = Arc::new(StorageManager::new(manager, backend));
    create_a_file(Arc::clone(&storage), 10).await;
    warm_up(Arc::clone(&storage), 10).await;
    // Concurrency read ,thread num : 1,2,4,8
    for i in 0..5 {
        let mut tasks = vec![];
        let start = tokio::time::Instant::now();
        for _ in 0..2_usize.pow(i) {
            let storage = Arc::clone(&storage);
            tasks.push(tokio::spawn(async move {
                seq_read(storage, 10).await;
            }));
        }
        let write_handle = tokio::spawn(create_a_file(Arc::clone(&storage), 11));
        for task in tasks {
            task.await.unwrap();
        }
        let end = tokio::time::Instant::now();
        // throuput = 1GB/ time * thread num
        let throuput =
            TOTAL_SIZE.lossy_cast() / (end - start).as_secs_f64() * 2_usize.pow(i).lossy_cast();
        println!(
            "thread num : {}, read Time: {:?} thoughput: {} MB/s",
            2_usize.pow(i),
            end - start,
            throuput
        );
        // assert!(storage.len() == 0);
        write_handle.await.unwrap();
    }
}

async fn scan_worker(storage: Arc<StorageManager>, ino: u64, time: u64) -> usize {
    let flag = OpenFlag::Read;
    let fh = CURRENT_FD.fetch_add(1, Ordering::SeqCst);
    storage.open(ino, fh, flag);
    let start = tokio::time::Instant::now();
    let mut i = 0;
    let mut scan_cnt = 0;
    while tokio::time::Instant::now() - start < tokio::time::Duration::from_secs(time) {
        let buf = storage
            .read(ino, fh, (i * IO_SIZE) as u64, IO_SIZE)
            .await
            .unwrap();
        assert_eq!(buf.len(), IO_SIZE);
        check_data(&buf, i as u64);
        i = (i + 1) % TOTAL_TEST_BLOCKS;
        scan_cnt += 1;
    }
    scan_cnt
}

async fn get_worker(storage: Arc<StorageManager>, ino: u64, time: u64) -> usize {
    let flag = OpenFlag::Read;
    let fh = CURRENT_FD.fetch_add(1, Ordering::SeqCst);
    storage.open(ino, fh, flag);
    let start = tokio::time::Instant::now();

    // 初始化 Zipfian 分布
    let zipf = Zipf::new(TOTAL_TEST_BLOCKS as u64, 1.5_f64).unwrap();

    let mut get_cnt = 0;
    while tokio::time::Instant::now() - start < tokio::time::Duration::from_secs(time) {
        // 使用 Zipfian 分布来选择数据块 ID
        let i = zipf.sample(&mut thread_rng()) as usize % TOTAL_TEST_BLOCKS;

        let buf = storage
            .read(ino, fh, (i * IO_SIZE) as u64, IO_SIZE)
            .await
            .unwrap();
        assert_eq!(buf.len(), IO_SIZE);
        check_data(&buf, i as u64);
        get_cnt += 1;
    }
    get_cnt
}

async fn real_workload() {
    println!("Real workload test");
    let backend = Arc::new(MemoryBackend::new(BACKEND_LATENCY));
    // A 4GB cache
    let manager = Arc::new(Mutex::new(MemoryCache::new(1024 + 10)));
    let storage = Arc::new(StorageManager::new(manager, backend));
    // 100 is for scan worker
    create_a_file(Arc::clone(&storage), 100).await;
    let mut create_tasks = vec![];
    for i in 0..RANDOM_GET_THREAD_NUM {
        let storage = Arc::clone(&storage);
        create_tasks.push(tokio::spawn(create_a_file(storage, i.cast())));
    }
    for task in create_tasks {
        task.await.unwrap();
    }
    let mut scan_tasks = vec![];
    let mut get_tasks = vec![];
    for _ in 0..SCAN_READ_THREAD_NUM {
        let storage = Arc::clone(&storage);
        scan_tasks.push(tokio::spawn(scan_worker(storage, 100, BENCH_TIME)));
    }
    for i in 0..RANDOM_GET_THREAD_NUM {
        let storage = Arc::clone(&storage);
        get_tasks.push(tokio::spawn(get_worker(storage, i, BENCH_TIME)));
    }

    let mut total_scan = 0;
    let mut total_get = 0;
    for task in scan_tasks {
        total_scan += task.await.unwrap();
    }
    for task in get_tasks {
        total_get += task.await.unwrap();
    }
    println!("Total scan cnt: {total_scan}, total get cnt: {total_get}");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // concurrency_read().await;
    real_workload().await;
    // concurrency_read_with_write().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn storage_concurrency_read_test() {
    concurrency_read().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn storage_concurrency_read_with_write_test() {
    concurrency_read_with_write().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn storage_real_workload_test() {
    real_workload().await;
}

#[tokio::test]
async fn test_truncate() {
    let cache = Arc::new(Mutex::new(MemoryCache::new(1024)));
    let backend = Arc::new(MemoryBackend::new(BACKEND_LATENCY));
    let backend_clone = Arc::clone(&backend);
    let storage = StorageManager::new(cache, backend_clone);

    let content = vec![6_u8; BLOCK_SIZE * 2];
    let ino = 0;
    let block_size: u64 = BLOCK_SIZE.cast();
    let mut buffer = vec![0; BLOCK_SIZE];

    let fh = CURRENT_FD.fetch_add(1, Ordering::SeqCst);
    storage.open(ino, fh, OpenFlag::ReadAndWrite);
    storage.write(ino, fh, 0, &content).await.unwrap();
    storage.close(fh).await;
    let size = backend
        .read(&format_path(ino, 1), &mut buffer)
        .await
        .unwrap();
    assert_eq!(size, BLOCK_SIZE);
    let size = backend
        .read(&format_path(ino, 2), &mut buffer)
        .await
        .unwrap();
    assert_eq!(size, 0);

    // Truncate to a greater size, noop
    storage
        .truncate(ino, block_size * 2, block_size * 3)
        .await
        .unwrap();
    let size = backend
        .read(&format_path(ino, 2), &mut buffer)
        .await
        .unwrap();
    assert_eq!(size, 0);

    storage
        .truncate(ino, block_size * 2, block_size)
        .await
        .unwrap();
    let size = backend
        .read(&format_path(ino, 1), &mut buffer)
        .await
        .unwrap();
    assert_eq!(size, 0);

    storage
        .truncate(ino, block_size, block_size / 2)
        .await
        .unwrap();
    let size = backend
        .read(&format_path(ino, 0), &mut buffer)
        .await
        .unwrap();
    assert_eq!(size, BLOCK_SIZE);
    let mut truncated_content = vec![6_u8; BLOCK_SIZE / 2];
    truncated_content.resize(BLOCK_SIZE, 0);
    assert_eq!(truncated_content, buffer);

    storage.truncate(ino, block_size / 2, 0).await.unwrap();
}

#[tokio::test]
async fn test_remove() {
    let cache = Arc::new(Mutex::new(MemoryCache::new(1024)));
    let backend = Arc::new(MemoryBackend::new(BACKEND_LATENCY));
    let backend_clone = Arc::clone(&backend);
    let storage = StorageManager::new(cache, backend_clone);

    let content = vec![6_u8; BLOCK_SIZE * 2];
    let ino = 0;
    let mut buffer = vec![0; BLOCK_SIZE];

    let fh = CURRENT_FD.fetch_add(1, Ordering::SeqCst);
    storage.open(ino, fh, OpenFlag::ReadAndWrite);
    storage.write(ino, fh, 0, &content).await.unwrap();
    storage.close(fh).await;

    storage.remove(ino).await.unwrap();

    let size = backend
        .read(&format_path(ino, 0), &mut buffer)
        .await
        .unwrap();
    assert_eq!(size, 0);
    let size = backend
        .read(&format_path(ino, 1), &mut buffer)
        .await
        .unwrap();
    assert_eq!(size, 0);
}
