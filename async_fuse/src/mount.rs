//! The implementation of FUSE mount and un-mount

use anyhow::{self, Context};
use log::{debug, info};
use nix::fcntl::{self, OFlag};
use nix::sys::stat::{self, Mode};
use smol::blocking;
use std::ffi::CString;
use std::fs;
use std::os::unix::io::RawFd;
use std::path::Path;

// use param::*;

/// Linux mount flags, check the following link for details
/// <https://github.com/torvalds/linux/blob/master/include/uapi/linux/mount.h#L11>
#[cfg(target_os = "linux")]
mod param {
    // TODO: use mount flags from libc
    // /// Mount read-only
    // pub const MS_RDONLY: u64 = 1;
    /// Ignore suid and sgid bits
    pub const MS_NOSUID: u64 = 2;
    /// Disallow access to device special files
    pub const MS_NODEV: u64 = 4;
    /// Force un-mount
    pub const MNT_FORCE: i32 = 1;
}
/// macOS mount flags, check the link for details
/// <https://github.com/apple/darwin-xnu/blob/master/bsd/sys/mount.h#L288>
#[cfg(target_os = "macos")]
mod param {
    // TODO: use mount flags from libc
    // /// read only filesystem
    // pub const MNT_RDONLY: i32 = 0x00000001;
    /// don't honor setuid bits on fs
    pub const MNT_NOSUID: i32 = 0x00000008;
    /// don't interpret special files
    pub const MNT_NODEV: i32 = 0x00000010;
    /// force unmount or readonly change
    pub const MNT_FORCE: i32 = 0x00080000;
    /// Don't allow user extended attributes
    pub const MNT_NOUSERXATTR: i32 = 0x01000000;
    /// disable update of file access time
    pub const MNT_NOATIME: i32 = 0x10000000;

    pub const PAGE_SIZE: u32 = 4096;

    pub const FUSE_DEFAULT_BLOCKSIZE: u32 = 4096;
    pub const FUSE_DEFAULT_DAEMON_TIMEOUT: u32 = 60; // seconds
    pub const FUSE_DEFAULT_IOSIZE: u32 = 16 * PAGE_SIZE;

    pub const FUSE_IOC_MAGIC: u8 = b'F';
    pub const FUSE_IOC_TYPE_MODE: u8 = 5;

    pub const FUSE_FSSUBTYPE_UNKNOWN: u32 = 0;
    pub const FUSE_MOPT_DEBUG: u64 = 0x0000000000000040;
    pub const FUSE_MOPT_FSNAME: u64 = 0x0000000000001000;
    pub const FUSE_MOPT_NO_APPLEXATTR: u64 = 0x0000000000800000;

    /// length of fs type name including null
    pub const MFSTYPENAMELEN: lib::size_t = 16;
    ///PATH_MAX
    pub const MAXPATHLEN: size_t = 1024;

    #[repr(C)]
    pub struct FuseMountArgs {
        pub mntpath: [u8; MAXPATHLEN],        // path to the mount point
        pub fsname: [u8; MAXPATHLEN],         // file system description string
        pub fstypename: [u8; MFSTYPENAMELEN], // file system type name
        pub volname: [u8; MAXPATHLEN],        // volume name
        pub altflags: u64,                    // see mount-time flags below
        pub blocksize: u32,                   // fictitious block size of our "storage"
        pub daemon_timeout: u32,              // timeout in seconds for upcalls to daemon
        pub fsid: u32,                        // optional custom value for part of fsid[0]
        pub fssubtype: u32,                   // file system sub type id
        pub iosize: u32,                      // maximum size for reading or writing
        pub random: u32,                      // random "secret" from device
        pub rdev: u32,                        // dev_t for the /dev/osxfuse{n} in question
    }
}

/// Linux un-mount
#[cfg(target_os = "linux")]
pub async fn umount(short_path: &Path) -> anyhow::Result<()> {
    use nix::unistd;
    use std::process::Command;

    let mount_path = short_path.to_path_buf();
    blocking!(
        let mntpnt = mount_path.as_os_str();

        if unistd::geteuid().is_root() {
            // direct umount
            #[cfg(target_arch = "aarch64")]
            let result = unsafe { libc::umount2(utilities::cast_to_ptr(mntpnt), MNT_FORCE) };
            #[cfg(target_arch = "x86_64")]
            let result =
                unsafe { libc::umount2(utilities::cast_to_ptr(mntpnt), param::MNT_FORCE) };

            if result == 0 {
                Ok(())
            } else {
                Err(anyhow::anyhow!(
                    "failed to umount fuse device, the error is: {}",
                    nix::Error::last(),
                ))
            }
        } else {
            // use fusermount to umount
            let umount_handle = Command::new("fusermount")
                .arg("-uz") // lazy umount
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
    )
}

/// Linux mount
#[cfg(target_os = "linux")]
pub async fn mount(mount_point: &Path) -> anyhow::Result<RawFd> {
    use nix::unistd;

    if unistd::geteuid().is_root() {
        // direct umount
        direct_mount(mount_point).await
    } else {
        // use fusermount to mount
        fuser_mount(mount_point).await
    }
}

/// Linux fusermount
#[cfg(target_os = "linux")]
async fn fuser_mount(mount_point: &Path) -> anyhow::Result<RawFd> {
    use nix::cmsg_space;
    use nix::sys::socket::{
        self, AddressFamily, ControlMessageOwned, MsgFlags, SockFlag, SockType,
    };
    use nix::sys::uio::IoVec;
    use std::process::Command;

    let mount_path = mount_point.to_path_buf();

    let (local, remote) = blocking!(socket::socketpair(
        AddressFamily::Unix,
        SockType::Stream,
        None,
        SockFlag::empty(),
    ))
    .context("failed to create socket pair")?;

    let mount_handle = blocking!(Command::new("fusermount")
        .arg("-o")
        // fusermount option allow_other only allowed if user_allow_other is set in /etc/fuse.conf
        .arg("nosuid,nodev,noexec,nonempty") // rw,async,noatime,auto_unmount,allow_other
        .arg(mount_path.as_os_str())
        .env("_FUSE_COMMFD", remote.to_string())
        .output())
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

    blocking!(
        let mut buf = [0_u8; 5];
        let iov = [IoVec::from_mut_slice(&mut buf[..])];

        #[allow(clippy::integer_arithmetic)]
        let mut cmsgspace = cmsg_space!([RawFd; 1]);
        let msg = socket::recvmsg(local, &iov, Some(&mut cmsgspace), MsgFlags::empty())
            .context("failed to receive from fusermount")?;

        let mount_fd = if let Some(cmsg) = msg.cmsgs().next() {
            if let ControlMessageOwned::ScmRights(fds) = cmsg {
                debug_assert_eq!(fds.len(), 1);
                *fds.get(0).unwrap_or_else(|| panic!("failed to get the only fd"))
            } else {
                panic!("unexpected cmsg")
            }
        } else {
            panic!("failed to get cmsgs")
        };

        // let mut mount_fd = -1;
        // for cmsg in msg.cmsgs() {
        //     if let ControlMessageOwned::ScmRights(fd) = cmsg {
        //         debug_assert_eq!(fd.len(), 1);
        //         mount_fd = fd[0];
        //     } else {
        //         panic!("unexpected cmsg");
        //     }
        // }

        // debug_assert_ne!(mount_fd, -1);
        Ok(mount_fd)
    )
}

/// Linux directly mount
#[cfg(target_os = "linux")]
async fn direct_mount(mount_point: &Path) -> anyhow::Result<RawFd> {
    use nix::sys::stat::SFlag;
    use nix::unistd;

    let mount_point = mount_point.to_path_buf();

    let devpath = Path::new("/dev/fuse");

    let dev_fd = blocking!(fcntl::open(devpath, OFlag::O_RDWR, Mode::empty()))
        .context("failed to open fuse device")?;

    let full_path = blocking!(fs::canonicalize(mount_point))?;
    let path_str = full_path
        .to_str()
        .context("full mount path to string failed")?;

    let mntpath =
        CString::new(path_str).context(format!("CString::new(path_str={}) failed", path_str))?;
    let fstype = CString::new("fuse").context("CString::new failed")?;
    let fsname = CString::new("/dev/fuse").context("CString::new failed")?;

    let mnt_sb =
        blocking!(stat::stat(&full_path)).context("failed to get the file stat of mount point")?;

    let opts = format!(
        "fd={},rootmode={:o},user_id={},group_id={}",
        dev_fd,
        mnt_sb.st_mode & SFlag::S_IFMT.bits(),
        unistd::getuid().as_raw(),
        unistd::getgid().as_raw(),
    );
    let opts =
        CString::new(opts.as_str()).context(format!("CString::new(opts={}) failed", opts))?;
    debug!("direct mount opts={:?}", &opts);
    blocking!(
        let result = unsafe { libc::mount(
            fsname.as_ptr(),
            mntpath.as_ptr(),
            fstype.as_ptr(),
            param::MS_NOSUID | param::MS_NODEV,
            utilities::cast_to_ptr(&*opts.as_ptr()),
        )};
        if result == 0 {
            info!("mount path={:?} to FUSE device={:?} successfully!", mntpath, devpath);
            Ok(dev_fd)
        } else {
            // let e = Errno::from_i32(errno::errno());
            // debug!("errno={},the error is: {:?}", errno::errno(), e);
            // let mount_fail_str = "mount failed!";
            // #[cfg(target_arch = "aarch64")]
            // libc::perror(mount_fail_str.as_ptr());
            // #[cfg(target_arch = "x86_64")]
            // libc::perror(mount_fail_str.as_ptr() as *const i8);

            Err(anyhow::anyhow!(
                "failed to mount to fuse device, the error is: {}",
                nix::Error::last(),
            ))
        }
    )
}

/// macOS un-mount
#[cfg(target_os = "macos")]
pub async fn umount(mount_point: impl AsRef<Path>) -> nix::Result<()> {
    let mntpnt = mount_point.as_ref().to_path_buf();
    blocking!(
        let mntpnt = mntpnt.as_os_str();
        let res = unsafe { libc::unmount(utilities::cast_to_ptr(mntpnt), MNT_FORCE) };
        if res < 0 {
            Err(nix::Error::last())
        } else {
            Ok(())
        }
    )
}

/// macOS mount
#[cfg(target_os = "macos")]
pub async fn mount(mount_point: impl AsRef<Path>) -> anyhow::Result<RawFd> {
    let mount_point = mount_point.as_ref().to_path_buf();
    let devpath = Path::new("/dev/osxfuse1");

    let fd = blocking!(fcntl::open(devpath, OFlag::O_RDWR, Mode::empty()))
        .context("failed to open fuse device")?;

    let sb = blocking!(stat::fstat(fd).context("failed to get the file stat of fuse device"))?;

    // use ioctl to read device random secret
    // osxfuse/support/mount_osxfuse/mount_osxfuse.c#L1099
    // result = ioctl(fd, FUSEDEVIOCGETRANDOM, &drandom);
    // FUSEDEVIOCGETRANDOM // osxfuse/common/fuse_ioctl.h#L43
    let drandom = blocking!(
        let mut drandom: u32 = 0;
        nix::ioctl_read!(fuse_read_random, FUSE_IOC_MAGIC, FUSE_IOC_TYPE_MODE, u32);
        let result = unsafe { fuse_read_random(fd, utilities::cast_to_mut_ptr(&mut drandom))? };
        debug_assert_eq!(result, 0);
        debug!("successfully read drandom={}", drandom);
        Ok::<_, anyhow::Error>(drandom)
    )?;

    let full_path = blocking!(fs::canonicalize(mount_point))?;
    let cstr_path = full_path
        .to_str()
        .expect("full mount path to string failed");

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
    fn copy_slice<T: Copy>(from: &[T], to: &mut [T]) {
        debug_assert!(to.len() >= from.len());
        to[..from.len()].copy_from_slice(&from);
    }
    let mut mntpath_slice = [0u8; MAXPATHLEN];
    copy_slice(mntpath.as_bytes(), &mut mntpath_slice);
    let mut fsname_slice = [0u8; MAXPATHLEN];
    copy_slice(fsname.as_bytes(), &mut fsname_slice);
    let mut fstypename_slice = [0u8; MFSTYPENAMELEN];
    copy_slice(fstypename.as_bytes(), &mut fstypename_slice);
    let mut volname_slice = [0u8; MAXPATHLEN];
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
        rdev: sb.st_rdev as u32,
    };

    blocking!(
        let result = unsafe {
            libc::mount(
                fstype.as_ptr(),
                mntpath.as_ptr(),
                MNT_NOSUID | MNT_NODEV | MNT_NOUSERXATTR | MNT_NOATIME,
                utilities::cast_to_mut_ptr(&mut mnt_args),
            )
        };
        if result == 0 {
            info!("mount path={:?} to FUSE device={:?} successfully!", mntpath, devpath);
            Ok(fd)
        } else {
            // let e = Errno::from_i32(errno::errno());
            // error!("errno={}, the error is: {:?}", errno::errno(), e);
            // let mount_fail_str = "mount failed!";
            // unsafe { libc::perror(mount_fail_str.as_ptr() as *const i8); }

            Err(anyhow::anyhow!(
                "failed to mount to fuse device, the error is: {}",
                nix::Error::last(),
            ))
        }
    )
}
