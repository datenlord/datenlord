use std::fs;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use clippy_utilities::OverflowArithmetic;
use datenlord::common::task_manager::{TaskName, TASK_MANAGER};
use datenlord::config::{
    MemoryCacheConfig, SoftLimit, StorageConfig, StorageParams, StorageS3Config,
};
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info}; // warn, error

use crate::async_fuse::fuse::{mount, session};
use crate::async_fuse::memfs;
use crate::async_fuse::memfs::kv_engine::{KVEngine, KVEngineType};
use crate::common::logger::{init_logger, LogRole};
use crate::new_storage::{BackendBuilder, MemoryCache, StorageManager, BLOCK_SIZE};

pub const TEST_NODE_ID: &str = "test_node";
pub const TEST_ETCD_ENDPOINT: &str = "127.0.0.1:2379";

/// The default capacity in bytes for test, 1GB
const CACHE_DEFAULT_CAPACITY: usize = 1024 * 1024 * 1024;

fn test_storage_config(is_s3: bool) -> StorageConfig {
    let params = if is_s3 {
        let s3_config = StorageS3Config {
            endpoint_url: "http://127.0.0.1:9000".to_owned(),
            access_key_id: "test".to_owned(),
            secret_access_key: "test1234".to_owned(),
            bucket_name: "fuse-test-bucket".to_owned(),
        };

        StorageParams::S3(s3_config)
    } else {
        StorageParams::Fs("/tmp/datenlord_backend".to_owned())
    };

    let soft_limit = SoftLimit(
        3,
        NonZeroUsize::new(5).unwrap_or_else(|| unreachable!("5 is not 0.")),
    );
    StorageConfig {
        block_size: BLOCK_SIZE,
        memory_cache_config: MemoryCacheConfig {
            capacity: CACHE_DEFAULT_CAPACITY,
            command_queue_limit: 1000,
            write_back: true,
            soft_limit,
            write_back_interval: Duration::from_millis(200),
            write_back_dirty_limit: 10,
        },
        params,
    }
}

async fn run_fs(mount_point: &Path, is_s3: bool, token: CancellationToken) -> anyhow::Result<()> {
    let storage_config = test_storage_config(is_s3);
    let kv_engine: Arc<memfs::kv_engine::etcd_impl::EtcdKVEngine> =
        Arc::new(KVEngineType::new(vec![TEST_ETCD_ENDPOINT.to_owned()]).await?);

    let storage = {
        let storage_param = &storage_config.params;
        let memory_cache_config = &storage_config.memory_cache_config;

        let block_size = storage_config.block_size;
        let capacity_in_blocks = memory_cache_config.capacity.overflow_div(block_size);

        let cache = Arc::new(Mutex::new(MemoryCache::new(capacity_in_blocks, block_size)));
        let backend = Arc::new(BackendBuilder::new(storage_param.clone()).build()?);
        StorageManager::new(cache, backend, block_size)
    };

    let fs: memfs::MemFs<memfs::S3MetaData> = memfs::MemFs::new(
        mount_point
            .as_os_str()
            .to_str()
            .unwrap_or_else(|| panic!("failed to convert to utf8 string")),
        CACHE_DEFAULT_CAPACITY,
        kv_engine,
        TEST_NODE_ID,
        &storage_config,
        storage,
    )
    .await?;
    let ss = session::new_session_of_memfs(mount_point, fs).await?;
    ss.run(token).await?;

    Ok(())
}

#[allow(clippy::let_underscore_must_use)]
pub async fn setup(mount_dir: &Path, is_s3: bool) -> anyhow::Result<()> {
    init_logger(LogRole::Test);
    debug!("setup started with mount_dir: {:?}", mount_dir);
    if mount_dir.exists() {
        debug!("mount_dir {:?} exists ,try umount", mount_dir);
        let result = mount::umount(mount_dir).await;
        if result.is_ok() {
            debug!("Successfully umounted {:?} before setup", mount_dir);
        } else {
            info!(
                "Failed to umount {:?}, reason: {:?}",
                mount_dir,
                result.err()
            );
        }
        debug!("remove mount_dir {:?} before setup", mount_dir);
        if let Err(e) = fs::remove_dir_all(mount_dir) {
            info!("Failed to remove mount_dir {:?}, reason: {}", mount_dir, e);
            return Err(e.into());
        }
    }

    debug!("Creating directory {:?}", mount_dir);
    fs::create_dir_all(mount_dir)?;
    let abs_root_path = fs::canonicalize(mount_dir)?;

    TASK_MANAGER
        .spawn(TaskName::AsyncFuse, |token| async move {
            if let Err(e) = run_fs(&abs_root_path, is_s3, token).await {
                panic!(
                    "failed to run filesystem, the error is: {}",
                    crate::common::util::format_anyhow_error(&e),
                );
            }
        })
        .await?;

    debug!("async_fuse task spawned");
    let seconds = 2;
    info!("sleeping {} seconds for setup", seconds);
    tokio::time::sleep(Duration::new(seconds, 0)).await;

    info!("setup finished");
    Ok(())
}

pub async fn teardown(mount_dir: &Path) -> anyhow::Result<()> {
    info!("begin teardown");
    let seconds = 1;
    debug!("sleep {} seconds for teardown", seconds);
    tokio::time::sleep(Duration::new(seconds, 0)).await;

    mount::umount(mount_dir).await.unwrap_or_else(|err| {
        panic!(
            "failed to un-mount {:?}, the error is: {}",
            mount_dir,
            crate::common::util::format_anyhow_error(&err),
        )
    });
    let abs_mount_path = fs::canonicalize(mount_dir)?;
    fs::remove_dir_all(abs_mount_path)?;

    Ok(())
}
