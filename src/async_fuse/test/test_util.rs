use crate::common::etcd_delegate::EtcdDelegate;
use log::{debug, info}; // warn, error
use std::fs;
use std::path::Path;
use std::time::Duration;

use crate::async_fuse::fuse::{mount, session::Session};
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

pub async fn setup(mount_dir: &Path, is_s3: bool) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let _log_init_res = env_logger::try_init();

    if mount_dir.exists() {
        let result = mount::umount(mount_dir).await;
        if result.is_ok() {
            debug!("umounted {:?} before setup", mount_dir);
        }

        fs::remove_dir_all(&mount_dir)?;
    }
    fs::create_dir_all(&mount_dir)?;
    let abs_root_path = fs::canonicalize(&mount_dir)?;

    let fs_task = tokio::task::spawn(async move {
        async fn run_fs(mount_point: &Path, is_s3: bool) -> anyhow::Result<()> {
            let etcd_delegate = EtcdDelegate::new(vec![TEST_ETCD_ENDPOINT.to_owned()]).await?;
            if is_s3 {
                let fs: memfs::MemFs<memfs::S3MetaData<DoNothingImpl>> = memfs::MemFs::new(
                    TEST_VOLUME_INFO,
                    CACHE_DEFAULT_CAPACITY,
                    TEST_NODE_IP,
                    TEST_PORT,
                    etcd_delegate,
                    TEST_NODE_ID,
                    TEST_VOLUME_INFO,
                )
                .await?;
                let ss = Session::new(mount_point, fs).await?;
                ss.run().await?;
            } else {
                let fs: memfs::MemFs<memfs::DefaultMetaData> = memfs::MemFs::new(
                    mount_point
                        .as_os_str()
                        .to_str()
                        .unwrap_or_else(|| panic!("failed to convert to utf8 string")),
                    CACHE_DEFAULT_CAPACITY,
                    TEST_NODE_IP,
                    TEST_PORT,
                    etcd_delegate.clone(),
                    TEST_NODE_ID,
                    TEST_VOLUME_INFO,
                )
                .await?;
                let ss = Session::new(mount_point, fs).await?;
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
    // do not block main thread
    let th = tokio::task::spawn(async {
        fs_task.await.unwrap_or_else(|e| {
            panic!("fs_task failed to join for error {}", e);
        });
        debug!("spawned a thread for futures::executor::block_on()");
    });
    debug!("async_fuse task spawned");
    let seconds = 2;
    debug!("sleep {} seconds for setup", seconds);
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
            the thread result is: {:?}",
            res,
        );
    });

    Ok(())
}
