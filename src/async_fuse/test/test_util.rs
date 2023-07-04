use crate::async_fuse::fuse::{file_system, session};
use crate::async_fuse::memfs::kv_engine::{KVEngine, KVEngineType};
use crate::common::etcd_delegate::EtcdDelegate;
use log::{debug, info}; // warn, error
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::async_fuse::fuse::mount;
use crate::async_fuse::memfs;
use crate::async_fuse::memfs::s3_wrapper::DoNothingImpl;

/*
pub const TEST_BUCKET_NAME: &str = "fuse-test-bucket";
pub const TEST_ENDPOINT: &str = "http://127.0.0.1:9000";
pub const TEST_ACCESS_KEY: &str = "test";
pub const TEST_SECRET_KEY: &str = "test1234";
*/
pub const TEST_VOLUME_INFO: &str = "fuse-test-bucket;http://127.0.0.1:9000;test;test1234";
pub const TEST_NODE_IP: &str = "127.0.0.1";
pub const TEST_NODE_ID: &str = "test_node";
pub const TEST_PORT: &str = "8888";
pub const TEST_ETCD_ENDPOINT: &str = "127.0.0.1:2379";

/// The default capacity in bytes for test, 1GB
const CACHE_DEFAULT_CAPACITY: usize = 1024 * 1024 * 1024;

#[allow(clippy::let_underscore_must_use)]
pub async fn setup(mount_dir: &Path, is_s3: bool) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    use env_logger::Builder;
    use log::LevelFilter;

    let mut builder = Builder::new();
    builder.filter(None, LevelFilter::Debug); // 设置全局日志级别为info
    builder.filter_module("h2", LevelFilter::Off); // 过滤掉特定模块的日志
    builder.filter_module("tower", LevelFilter::Off);
    builder.filter_module("typer", LevelFilter::Off);
    builder.filter_module("datenlord::async_fuse::fuse::session", LevelFilter::Off);
    let _ = builder.try_init();
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
            let etcd_delegate = EtcdDelegate::new(vec![TEST_ETCD_ENDPOINT.to_owned()]).await?;
            let kv_engine = Arc::new(KVEngineType::new(vec![TEST_ETCD_ENDPOINT.to_owned()]).await?);
            if is_s3 {
                let (fs, fs_ctrl): (
                    memfs::MemFs<memfs::S3MetaData<DoNothingImpl>>,
                    file_system::FsController,
                ) = memfs::MemFs::new(
                    TEST_VOLUME_INFO,
                    CACHE_DEFAULT_CAPACITY,
                    TEST_NODE_IP,
                    TEST_PORT,
                    etcd_delegate,
                    kv_engine,
                    TEST_NODE_ID,
                    TEST_VOLUME_INFO,
                )
                .await?;
                let ss = session::new_session_of_memfs(mount_point, fs, fs_ctrl).await?;
                ss.run().await?;
            } else {
                let (fs, fs_ctrl): (
                    memfs::MemFs<memfs::S3MetaData<DoNothingImpl>>,
                    file_system::FsController,
                ) = memfs::MemFs::new(
                    mount_point
                        .as_os_str()
                        .to_str()
                        .unwrap_or_else(|| panic!("failed to convert to utf8 string")),
                    CACHE_DEFAULT_CAPACITY,
                    TEST_NODE_IP,
                    TEST_PORT,
                    etcd_delegate.clone(),
                    Arc::<KVEngineType>::clone(&kv_engine),
                    TEST_NODE_ID,
                    TEST_VOLUME_INFO,
                )
                .await?;
                let ss = session::new_session_of_memfs(mount_point, fs, fs_ctrl).await?;
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
