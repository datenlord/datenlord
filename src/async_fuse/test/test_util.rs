use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use datenlord::config::{StorageConfig, StorageParams, StorageS3Config};
use tracing::{debug, info}; // warn, error

use crate::async_fuse::fuse::{mount, session};
use crate::async_fuse::memfs;
use crate::async_fuse::memfs::kv_engine::{KVEngine, KVEngineType};
use crate::async_fuse::memfs::s3_wrapper::DoNothingImpl;
use crate::common::logger::init_logger;

pub const TEST_NODE_IP: &str = "127.0.0.1";
pub const TEST_NODE_ID: &str = "test_node";
pub const TEST_PORT: u16 = 8888;
pub const TEST_ETCD_ENDPOINT: &str = "127.0.0.1:2379";

/// The default capacity in bytes for test, 1GB
const CACHE_DEFAULT_CAPACITY: usize = 1024 * 1024 * 1024;

fn test_storage_config() -> StorageConfig {
    let s3_config = StorageS3Config {
        endpoint_url: "http://127.0.0.1:9000".to_owned(),
        access_key_id: "test".to_owned(),
        secret_access_key: "test1234".to_owned(),
        bucket_name: "fuse-test-bucket".to_owned(),
    };
    StorageConfig {
        cache_capacity: CACHE_DEFAULT_CAPACITY,
        params: StorageParams::S3(s3_config),
    }
}

#[allow(clippy::let_underscore_must_use)]
// TODO : Remove `is_s3` arg due too we only support s3 now
pub async fn setup(mount_dir: &Path, is_s3: bool) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    init_logger();
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

    let fs_task = tokio::task::spawn(async move {
        async fn run_fs(mount_point: &Path, is_s3: bool) -> anyhow::Result<()> {
            let storage_config = test_storage_config();
            let kv_engine = Arc::new(KVEngineType::new(vec![TEST_ETCD_ENDPOINT.to_owned()]).await?);
            if is_s3 {
                let fs: memfs::MemFs<memfs::S3MetaData<DoNothingImpl>> = memfs::MemFs::new(
                    mount_point
                        .as_os_str()
                        .to_str()
                        .unwrap_or_else(|| panic!("failed to convert to utf8 string")),
                    CACHE_DEFAULT_CAPACITY,
                    TEST_NODE_IP,
                    TEST_PORT,
                    kv_engine,
                    TEST_NODE_ID,
                    &storage_config,
                )
                .await?;
                let ss = session::new_session_of_memfs(mount_point, fs).await?;
                ss.run().await?;
            } else {
                let fs: memfs::MemFs<memfs::S3MetaData<DoNothingImpl>> = memfs::MemFs::new(
                    mount_point
                        .as_os_str()
                        .to_str()
                        .unwrap_or_else(|| panic!("failed to convert to utf8 string")),
                    CACHE_DEFAULT_CAPACITY,
                    TEST_NODE_IP,
                    TEST_PORT,
                    Arc::<KVEngineType>::clone(&kv_engine),
                    TEST_NODE_ID,
                    &storage_config,
                )
                .await?;
                let ss = session::new_session_of_memfs(mount_point, fs).await?;
                ss.run().await?;
            };

            Ok(())
        }
        if let Err(e) = run_fs(&abs_root_path, is_s3).await {
            panic!(
                "failed to run filesystem, the error is: {}",
                crate::common::util::format_anyhow_error(&e),
            );
        }
    });

    debug!("Spawning main thread");
    let th = tokio::task::spawn(async {
        fs_task.await.unwrap_or_else(|e| {
            panic!("fs_task failed to join for error {e}");
        });
        debug!("spawned a thread for futures::executor::block_on()");
    });

    debug!("async_fuse task spawned");
    let seconds = 2;
    info!("sleeping {} seconds for setup", seconds);
    tokio::time::sleep(Duration::new(seconds, 0)).await;

    info!("setup finished");
    Ok(th)
}

pub async fn teardown(mount_dir: &Path, th: tokio::task::JoinHandle<()>) -> anyhow::Result<()> {
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
    fs::remove_dir_all(&abs_mount_path)?;

    #[allow(box_pointers)] // thread join result involves box point
    th.await.unwrap_or_else(|res| {
        panic!(
            "failed to wait the test setup thread to finish, \
            the thread result is: {res:?}",
        );
    });

    Ok(())
}
