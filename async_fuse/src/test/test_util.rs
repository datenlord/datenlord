use log::{debug, info}; // warn, error
use smol::Task;
use std::fs;
use std::path::Path;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::fuse::{mount, session::Session};
use crate::util;

pub fn setup(mount_dir: &Path) -> anyhow::Result<JoinHandle<()>> {
    let _log_init_res = env_logger::try_init();

    if mount_dir.exists() {
        smol::block_on(async move {
            let result = mount::umount(mount_dir).await;
            if result.is_ok() {
                debug!("umounted {:?} before setup", mount_dir);
            }
        });

        fs::remove_dir_all(&mount_dir)?;
    }
    fs::create_dir_all(&mount_dir)?;
    let abs_root_path = fs::canonicalize(&mount_dir)?;

    let fs_task = Task::spawn(async move {
        async fn run_fs(mount_point: &Path) -> anyhow::Result<()> {
            let ss = Session::new(mount_point).await?;
            ss.run().await?;
            Ok(())
        };
        if let Err(e) = run_fs(&abs_root_path).await {
            panic!(
                "failed to run filesystem, the error is: {}",
                util::format_anyhow_error(&e),
            );
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

pub fn teardown(mount_dir: &Path, th: JoinHandle<()>) -> anyhow::Result<()> {
    info!("begin teardown");
    let seconds = 1;
    debug!("sleep {} seconds for teardown", seconds);
    thread::sleep(Duration::new(seconds, 0));

    smol::block_on(async {
        mount::umount(mount_dir).await.unwrap_or_else(|err| {
            panic!(
                "failed to un-mount {:?}, the error is: {}",
                mount_dir,
                util::format_anyhow_error(&err),
            )
        });
    });
    let abs_mount_path = fs::canonicalize(mount_dir)?;
    fs::remove_dir_all(&abs_mount_path)?;

    #[allow(box_pointers)] // thread join result involves box point
    th.join().unwrap_or_else(|res| {
        panic!(
            "failed to wait the test setup thread to finish, \
                the thread result is: {:?}",
            res,
        );
    });
    Ok(())
}
