//! The implementation of FUSE mount and un-mount

use std::fs;
use std::os::unix::io::RawFd;
use std::path::Path;

use anyhow::Context;
use nix::fcntl::{self, OFlag};
use nix::sys::stat::{self, Mode};
use tracing::{debug, info};

// Linux mount flags, check the following link for details
// <https://github.com/torvalds/linux/blob/master/include/uapi/linux/mount.h#L11>

/// Linux un-mount
#[cfg(target_os = "linux")]
pub async fn umount(short_path: &Path) -> anyhow::Result<()> {
    use std::process::Command;

    use nix::unistd;

    let mount_path = short_path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mntpnt = mount_path.as_os_str();

        if unistd::geteuid().is_root() {
            // Direct un-mount
            nix::mount::umount(&mount_path).context(format!(
                // nix::mount::umount2(
                //     &mount_path,
                //     nix::mount::MntFlags::MNT_FORCE,
                // ).context(format!(
                "failed to un-mount FUSE, path={mount_path:?}",
            ))
        } else {
            // Use fusermount to un-mount
            let umount_handle = Command::new("fusermount")
                .arg("-uz") // lazy un-mount
                .arg(mntpnt)
                .output()
                .context("fusermount command failed to start")?;
            if umount_handle.status.success() {
                Ok(())
            } else {
                let stderr = String::from_utf8_lossy(&umount_handle.stderr);
                debug!("fusermount failed to umount, the error is: {}", &stderr);
                Err(anyhow::anyhow!(
                    "fusermount failed to umount fuse device, the error is: {}",
                    &stderr,
                ))
            }
        }
    })
    .await?
}

/// Linux mount
#[cfg(target_os = "linux")]
pub async fn mount(mount_point: &Path) -> anyhow::Result<RawFd> {
    use nix::unistd;

    if unistd::geteuid().is_root() {
        // Direct umount
        direct_mount(mount_point).await
    } else {
        // Use fusermount to mount
        fuser_mount(mount_point).await
    }
}

/// Linux fusermount
#[cfg(target_os = "linux")]
async fn fuser_mount(mount_point: &Path) -> anyhow::Result<RawFd> {
    use std::io::IoSliceMut;
    use std::os::fd::AsRawFd;
    use std::process::Command;

    use nix::cmsg_space;
    use nix::sys::socket::{
        self, AddressFamily, ControlMessageOwned, MsgFlags, SockFlag, SockType,
    };

    let mount_path = mount_point.to_path_buf();

    let (local, remote) = tokio::task::spawn_blocking(|| {
        socket::socketpair(
            AddressFamily::Unix,
            SockType::Stream,
            None,
            SockFlag::empty(),
        )
    })
    .await?
    .context("failed to create socket pair")?;

    let mount_handle = tokio::task::spawn_blocking(move || {
        Command::new("fusermount")
            .arg("-o")
            // fusermount option allow_other only allowed if user_allow_other is set in
            // /etc/fuse.conf
            .arg("nosuid,nodev,allow_other,default_permissions") // rw,async,noatime,noexec,auto_unmount,allow_other
            .arg(mount_path.as_os_str())
            .env("_FUSE_COMMFD", remote.as_raw_fd().to_string())
            .output()
    })
    .await?
    .context("fusermount command failed to start")?;

    assert!(
        mount_handle.status.success(),
        "failed to run fusermount, the error is: {}",
        String::from_utf8_lossy(&mount_handle.stderr),
    );
    info!(
        "fusermount path={:?} to FUSE device successfully!",
        mount_point,
    );

    tokio::task::spawn_blocking(move || {
        let mut buf = [0_u8; 5];
        let mut iov = [IoSliceMut::new(&mut buf[..])];

        #[allow(clippy::arithmetic_side_effects)]
        let mut cmsgspace = cmsg_space!([RawFd; 1]);
        let msg: socket::RecvMsg<'_, '_, ()> = socket::recvmsg(
            local.as_raw_fd(),
            &mut iov,
            Some(&mut cmsgspace),
            MsgFlags::empty(),
        )
        .context("failed to receive from fusermount")?;

        let mount_fd = if let Some(cmsg) = msg.cmsgs().next() {
            if let ControlMessageOwned::ScmRights(fds) = cmsg {
                debug_assert_eq!(fds.len(), 1);
                *fds.first()
                    .unwrap_or_else(|| panic!("failed to get the only fd"))
            } else {
                panic!("unexpected cmsg")
            }
        } else {
            panic!("failed to get cmsgs")
        };

        Ok(mount_fd)
    })
    .await?
}

/// Linux directly mount
#[cfg(target_os = "linux")]
async fn direct_mount(mount_point: &Path) -> anyhow::Result<RawFd> {
    use nix::mount::MsFlags;
    use nix::sys::stat::SFlag;
    use nix::unistd;

    let devpath = Path::new("/dev/fuse");

    let dev_fd =
        tokio::task::spawn_blocking(move || fcntl::open(devpath, OFlag::O_RDWR, Mode::empty()))
            .await?
            .context("failed to open fuse device")?;
    let mount_path = mount_point.to_path_buf();
    let full_path = tokio::task::spawn_blocking(move || fs::canonicalize(mount_path)).await??;
    let target_path = full_path.clone();
    let fstype = "fuse";
    let fsname = "/dev/fuse";

    let mnt_sb = tokio::task::spawn_blocking(move || stat::stat(&full_path))
        .await?
        .context(format!(
            "failed to get the file stat of mount point={mount_point:?}",
        ))?;
    let opts = format!(
        "fd={},rootmode={:o},user_id={},group_id={}",
        dev_fd,
        mnt_sb.st_mode & SFlag::S_IFMT.bits(),
        unistd::getuid().as_raw(),
        unistd::getgid().as_raw(),
    );

    debug!("direct mount opts={:?}", &opts);
    tokio::task::spawn_blocking(move || {
        nix::mount::mount(
            Some(fsname),
            &target_path,
            Some(fstype),
            MsFlags::MS_NOSUID | MsFlags::MS_NODEV,
            Some(opts.as_str()),
        )
    })
    .await?
    .context(format!("failed to direct mount {mount_point:?}"))?;

    Ok(dev_fd)
}
