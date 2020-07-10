use log::{debug, info}; // warn, error
use smol::Task;
use std::fs;
use std::path::Path;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use super::super::{mount, session::Session};

pub const DEFAULT_MOUNT_DIR: &str = "../fuse_test";
pub const FILE_CONTENT: &str = "0123456789ABCDEF";

pub fn setup(mount_path: impl AsRef<Path>) -> anyhow::Result<JoinHandle<()>> {
    env_logger::init();
    let mut mount_dir = mount_path.as_ref().to_path_buf();
    mount_dir = smol::block_on(async move {
        let result = mount::umount(&mount_dir).await;
        if result.is_ok() {
            debug!("umounted {:?} before setup", mount_dir);
        }
        mount_dir
    });

    if mount_dir.exists() {
        fs::remove_dir_all(&mount_dir)?;
    }
    fs::create_dir_all(&mount_dir)?;
    let abs_root_path = fs::canonicalize(&mount_dir)?;

    let fs_task = Task::spawn(async move {
        async fn run_fs(mount_point: impl AsRef<Path>) -> anyhow::Result<()> {
            let ss = Session::new(mount_point).await?;
            ss.run().await?;
            Ok(())
        };
        if let Err(e) = run_fs(&abs_root_path).await {
            panic!("failed to run filesystem, the error is: {}", e);
        }
    });
    // do not block main thread
    let th = thread::spawn(|| {
        smol::run(async { fs_task.await });
        debug!("spawned a thread for smol::run()");
    });
    debug!("async_fuse task spawned");
    let seconds = 2;
    debug!("sleep {} seconds for setup", seconds);
    thread::sleep(Duration::new(seconds, 0));

    info!("setup finished");
    Ok(th)
}

pub fn teardown(mount_dir: impl AsRef<Path>, th: JoinHandle<()>) -> anyhow::Result<()> {
    info!("begin teardown");
    let seconds = 1;
    debug!("sleep {} seconds for teardown", seconds);
    thread::sleep(Duration::new(seconds, 0));

    smol::block_on(async {
        mount::umount(&mount_dir).await.unwrap(); // TODO: remove unwrap()
    });
    let abs_mount_path = fs::canonicalize(mount_dir)?;
    fs::remove_dir_all(&abs_mount_path)?;
    th.join().unwrap(); // TODO: remove unwrap()
    Ok(())
}
