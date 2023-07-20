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

/// macOS mount flags, check the link for details
/// <https://github.com/apple/darwin-xnu/blob/master/bsd/sys/mount.h#L288>
#[cfg(target_os = "macos")]
mod param {
    // TODO: use mount flags from libc
    // /// read only filesystem
    // pub const MNT_RDONLY: i32 = 0x00000001;
    /// Do not honor setuid bits on fs
    pub const MNT_NOSUID: i32 = 0x0000_0008;
    /// Do not interpret special files
    pub const MNT_NODEV: i32 = 0x0000_0010;
    /// Force unmount or readonly change
    pub const MNT_FORCE: i32 = 0x0008_0000;
    /// Do not allow user extended attributes
    pub const MNT_NOUSERXATTR: i32 = 0x0100_0000;
    /// Disable update of file access time
    pub const MNT_NOATIME: i32 = 0x1000_0000;

    /// Page size
    pub const PAGE_SIZE: u32 = 4096;

    /// FUSE default block size
    pub const FUSE_DEFAULT_BLOCKSIZE: u32 = 4096;
    /// FUSE default time out seconds
    pub const FUSE_DEFAULT_DAEMON_TIMEOUT: u32 = 60; // seconds
    /// FUSE default IO size
    pub const FUSE_DEFAULT_IOSIZE: u32 = 16 * PAGE_SIZE;

    /// FUSE ioctl magic number
    pub const FUSE_IOC_MAGIC: u8 = b'F';
    /// FUSE ioctl type mode
    pub const FUSE_IOC_TYPE_MODE: u8 = 5;

    /// FUSE unknow filesystem sub-type
    pub const FUSE_FSSUBTYPE_UNKNOWN: u32 = 0;
    /// FUSE debug mode
    pub const FUSE_MOPT_DEBUG: u64 = 0x0000_0000_0000_0040;
    /// FUSE filesystem name
    pub const FUSE_MOPT_FSNAME: u64 = 0x0000_0000_0000_1000;
    /// FUSE no macOS extended attributes
    pub const FUSE_MOPT_NO_APPLEXATTR: u64 = 0x0000_0000_0080_0000;

    /// The length of fs type name including null
    pub const MFSTYPENAMELEN: libc::size_t = 16;
    /// Max path length
    pub const MAXPATHLEN: libc::size_t = 1024;

    /// FUSE mount arguments
    #[repr(C)]
    pub struct FuseMountArgs {
        /// path to the mount point
        pub mntpath: [u8; MAXPATHLEN],
        /// file system description string
        pub fsname: [u8; MAXPATHLEN],
        /// file system type name
        pub fstypename: [u8; MFSTYPENAMELEN],
        /// volume name
        pub volname: [u8; MAXPATHLEN],
        /// see mount-time flags below
        pub altflags: u64,
        /// fictitious block size of our "storage"
        pub blocksize: u32,
        /// timeout in seconds for upcalls to daemon
        pub daemon_timeout: u32,
        /// optional custom value for part of fsid[0]
        pub fsid: u32,
        /// file system sub type id
        pub fssubtype: u32,
        /// maximum size for reading or writing
        pub iosize: u32,
        /// random "secret" from device
        pub random: u32,
        /// dev_t for the /dev/osxfuse{n} in question
        pub rdev: u32,
    }
}

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
    use std::process::Command;

    use nix::cmsg_space;
    use nix::sys::socket::{
        self, AddressFamily, ControlMessageOwned, MsgFlags, SockFlag, SockType,
    };
    use nix::sys::uio::IoVec;

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
            .arg("nosuid,nodev,allow_other") // rw,async,noatime,noexec,auto_unmount,allow_other
            .arg(mount_path.as_os_str())
            .env("_FUSE_COMMFD", remote.to_string())
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
        let iov = [IoVec::from_mut_slice(&mut buf[..])];

        #[allow(clippy::integer_arithmetic)]
        let mut cmsgspace = cmsg_space!([RawFd; 1]);
        let msg = socket::recvmsg(local, &iov, Some(&mut cmsgspace), MsgFlags::empty())
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

/// macOS un-mount
#[cfg(target_os = "macos")]
pub async fn umount(mount_path: &Path) -> anyhow::Result<()> {
    let mount_point = mount_path.to_path_buf();
    tokio::task::spawn_blocking(|| {
        let mntpnt = mount_point.as_os_str();
        let res = unsafe { libc::unmount(utilities::cast_to_ptr(mntpnt), param::MNT_FORCE) };
        if res < 0 {
            Err(nix::Error::last())
        } else {
            Ok(())
        }
    })
    .await?
    .context(format!("failed to un-mount {:?}", mount_path))
}

/// macOS mount
#[allow(clippy::integer_arithmetic)] // ioctl macro involves integer arithmetic
#[cfg(target_os = "macos")]
pub async fn mount(mount_path: &Path) -> anyhow::Result<RawFd> {
    use std::ffi::CString;

    use param::{
        FuseMountArgs, FUSE_DEFAULT_BLOCKSIZE, FUSE_DEFAULT_DAEMON_TIMEOUT, FUSE_DEFAULT_IOSIZE,
        FUSE_FSSUBTYPE_UNKNOWN, FUSE_IOC_MAGIC, FUSE_IOC_TYPE_MODE, FUSE_MOPT_DEBUG,
        FUSE_MOPT_FSNAME, FUSE_MOPT_NO_APPLEXATTR, MAXPATHLEN, MFSTYPENAMELEN, MNT_NOATIME,
        MNT_NODEV, MNT_NOSUID, MNT_NOUSERXATTR,
    };
    use utilities::Cast;

    /// Copy slice
    fn copy_slice<T: Copy>(from: &[T], to: &mut [T]) {
        debug_assert!(to.len() >= from.len());
        to.get_mut(..from.len())
            .unwrap_or_else(|| panic!("failed to get {} elements of the target array", from.len()))
            .copy_from_slice(from);
    }

    let mount_point = mount_path.to_path_buf();
    let devpath = Path::new("/dev/osxfuse1");

    let fd = tokio::task::spawn_blocking(|| fcntl::open(devpath, OFlag::O_RDWR, Mode::empty()))
        .await?
        .context("failed to open fuse device")?;

    let sb = tokio::task::spawn_blocking(|| {
        stat::fstat(fd).context("failed to get the file stat of fuse device")
    })
    .await??;

    // use ioctl to read device random secret
    // osxfuse/support/mount_osxfuse/mount_osxfuse.c#L1099
    // result = ioctl(fd, FUSEDEVIOCGETRANDOM, &drandom);
    // FUSEDEVIOCGETRANDOM // osxfuse/common/fuse_ioctl.h#L43
    let drandom = tokio::task::spawn_blocking(|| {
        let mut drandom: u32 = 0;
        nix::ioctl_read!(fuse_read_random, FUSE_IOC_MAGIC, FUSE_IOC_TYPE_MODE, u32);
        let result = unsafe { fuse_read_random(fd, utilities::cast_to_mut_ptr(&mut drandom))? };
        debug_assert_eq!(result, 0);
        debug!("successfully read drandom={}", drandom);
        Ok::<_, anyhow::Error>(drandom)
    })
    .await??;

    let full_path = tokio::task::spawn_blocking(|| fs::canonicalize(mount_point)).await??;
    let cstr_path = full_path.to_str().context(format!(
        "failed to convert full mount path={:?} to string",
        full_path
    ))?;

    let mntpath = CString::new(cstr_path)?;
    let fstype = CString::new("osxfuse")?;
    let fsname = CString::new("macfuse")?;
    let fstypename = CString::new("")?;
    let volname = CString::new("OSXFUSE Volume 0 (macfuse)")?;

    // (fuse_mount_args) args = {
    //     mntpath = "/private/tmp/hello"
    //     fsname = "macfuse@osxfuse0"
    //     fstypename = ""
    //     volname = "OSXFUSE Volume 0 (macfuse)"
    //     altflags = 64
    //     blocksize = 4096
    //     daemon_timeout = 60
    //     fsid = 0
    //     fssubtype = 0
    //     iosize = 65536
    //     random = 1477356727
    //     rdev = 587202560
    //   }
    let mut mntpath_slice = [0_u8; MAXPATHLEN];
    copy_slice(mntpath.as_bytes(), &mut mntpath_slice);
    let mut fsname_slice = [0_u8; MAXPATHLEN];
    copy_slice(fsname.as_bytes(), &mut fsname_slice);
    let mut fstypename_slice = [0_u8; MFSTYPENAMELEN];
    copy_slice(fstypename.as_bytes(), &mut fstypename_slice);
    let mut volname_slice = [0_u8; MAXPATHLEN];
    copy_slice(volname.as_bytes(), &mut volname_slice);

    let mut mnt_args = FuseMountArgs {
        mntpath: mntpath_slice,
        fsname: fsname_slice,
        fstypename: fstypename_slice,
        volname: volname_slice,
        altflags: FUSE_MOPT_DEBUG | FUSE_MOPT_FSNAME | FUSE_MOPT_NO_APPLEXATTR,
        blocksize: FUSE_DEFAULT_BLOCKSIZE,
        daemon_timeout: FUSE_DEFAULT_DAEMON_TIMEOUT,
        fsid: 0,
        fssubtype: FUSE_FSSUBTYPE_UNKNOWN,
        iosize: FUSE_DEFAULT_IOSIZE,
        random: drandom,
        rdev: sb.st_rdev.cast(),
    };

    tokio::task::spawn_blocking(|| {
        let result = unsafe {
            libc::mount(
                fstype.as_ptr(),
                mntpath.as_ptr(),
                MNT_NOSUID | MNT_NODEV | MNT_NOUSERXATTR | MNT_NOATIME,
                utilities::cast_to_mut_ptr(&mut mnt_args),
            )
        };
        if 0 == result {
            info!(
                "mount path={:?} to FUSE device={:?} successfully!",
                mntpath, devpath
            );
            Ok(fd)
        } else {
            // Err(anyhow!(
            //     "failed to mount to fuse device, the error is: {}",
            //     nix::Error::last(),
            // ));
            crate::util::build_error_result_from_errno(
                nix::errno::Errno::last(),
                "failed to mount to fuse device".to_owned(),
            )
        }
    })
    .await?
}
