use log::{debug, error};
use nix::errno::{self, Errno};
use nix::fcntl::{self, OFlag};
use nix::sys::stat::{self, FileStat, Mode};
use std::ffi::CString;
use std::fs;
use std::os::raw::c_void;
use std::os::unix::io::RawFd;
use std::path::Path;

use param::*;

#[cfg(target_os = "linux")]
mod param {
    // https://github.com/torvalds/linux/blob/master/include/uapi/linux/mount.h#L11
    // TODO: use mount flags from libc
    // pub const MS_RDONLY: u64 = 1; // Mount read-only
    pub const MS_NOSUID: u64 = 2; // Ignore suid and sgid bits
    pub const MS_NODEV: u64 = 4; // Disallow access to device special files
    pub const MNT_FORCE: i32 = 1; // Force un-mount
}

#[cfg(target_os = "macos")]
mod param {
    // https://github.com/apple/darwin-xnu/blob/master/bsd/sys/mount.h#L288
    // TODO: use mount flags from libc
    // pub const MNT_RDONLY: i32 = 0x00000001; // read only filesystem
    pub const MNT_NOSUID: i32 = 0x00000008; // don't honor setuid bits on fs
    pub const MNT_NODEV: i32 = 0x00000010; // don't interpret special files
    pub const MNT_FORCE: i32 = 0x00080000; // force unmount or readonly change
    pub const MNT_NOUSERXATTR: i32 = 0x01000000; // Don't allow user extended attributes
    pub const MNT_NOATIME: i32 = 0x10000000; // disable update of file access time

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

    use libc::size_t;
    pub const MFSTYPENAMELEN: size_t = 16; // length of fs type name including null
    pub const MAXPATHLEN: size_t = 1024; //PATH_MAX

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

#[cfg(target_os = "linux")]
pub fn umount(short_path: &Path) -> i32 {
    use nix::unistd;
    use std::process::Command;

    let mntpnt = short_path.as_os_str();

    if unistd::geteuid().is_root() {
        // direct umount
        #[cfg(target_arch = "aarch64")]
        let result = unsafe { libc::umount2(mntpnt as *const _ as *const u8, MNT_FORCE) };
        #[cfg(target_arch = "x86_64")]
        let result =
            unsafe { libc::umount2(mntpnt as *const _ as *const u8 as *const i8, MNT_FORCE) };

        result
    } else {
        // use fusermount to umount
        let umount_handle = Command::new("fusermount")
            .arg("-uz") // lazy umount
            .arg(mntpnt)
            .output()
            .expect("fusermount command failed to start");
        if umount_handle.status.success() {
            0
        } else {
            // should be safe to use unwrap() here
            let stderr = String::from_utf8(umount_handle.stderr).unwrap();
            debug!("fusermount failed to umount: {}", stderr);
            -1
        }
    }
}

#[cfg(target_os = "linux")]
pub fn mount(mount_point: &Path) -> RawFd {
    use nix::unistd;

    if unistd::geteuid().is_root() {
        // direct umount
        direct_mount(mount_point)
    } else {
        // use fusermount to mount
        fuser_mount(mount_point)
    }
}

#[cfg(target_os = "linux")]
fn fuser_mount(mount_point: &Path) -> RawFd {
    use nix::cmsg_space;
    use nix::sys::socket::{
        self, AddressFamily, ControlMessageOwned, MsgFlags, SockFlag, SockType,
    };
    use nix::sys::uio::IoVec;
    use std::process::Command;

    let (local, remote) = socket::socketpair(
        AddressFamily::Unix,
        SockType::Stream,
        None,
        SockFlag::empty(),
    )
    .expect("failed to create socket pair");

    let mount_handle = Command::new("fusermount")
        .arg("-o")
        .arg("nosuid,nodev,noexec,nonempty") // rw,async,noatime,auto_unmount
        .arg(mount_point.as_os_str())
        .env("_FUSE_COMMFD", remote.to_string())
        .output()
        .expect("fusermount command failed to start");

    assert!(mount_handle.status.success());

    let mut buf = [0u8; 5];
    let iov = [IoVec::from_mut_slice(&mut buf[..])];
    let mut cmsgspace = cmsg_space!([RawFd; 1]);
    let msg = socket::recvmsg(local, &iov, Some(&mut cmsgspace), MsgFlags::empty())
        .expect("failed to receive from fusermount");

    let mut mount_fd = -1;
    for cmsg in msg.cmsgs() {
        if let ControlMessageOwned::ScmRights(fd) = cmsg {
            assert_eq!(fd.len(), 1);
            mount_fd = fd[0];
        } else {
            panic!("unexpected cmsg");
        }
    }

    mount_fd
}

#[cfg(target_os = "linux")]
fn direct_mount(mount_point: &Path) -> RawFd {
    use nix::sys::stat::SFlag;
    use nix::unistd;

    let devpath = Path::new("/dev/fuse");

    let dev_fd: RawFd;
    let result = fcntl::open(devpath, OFlag::O_RDWR, Mode::empty());
    match result {
        Ok(fd) => {
            debug!("open fuse device successfully");
            dev_fd = fd;
        }
        Err(e) => {
            error!("open fuse device failed! {}", e);
            return -1;
        }
    }

    let full_path = fs::canonicalize(mount_point).expect("fail to get full path of mount point");
    let cstr_path = full_path
        .to_str()
        .expect("full mount path to string failed");

    let mnt_sb: FileStat;
    let result = stat::stat(&full_path);
    match result {
        Ok(sb) => mnt_sb = sb,
        Err(e) => {
            error!("get mount point stat failed! {}", e);
            return -1;
        }
    }

    let mntpath = CString::new(cstr_path).expect("CString::new failed");
    let fstype = CString::new("fuse").expect("CString::new failed");
    let fsname = CString::new("/dev/fuse").expect("CString::new failed");

    let opts = format!(
        "fd={},rootmode={:o},user_id={},group_id={}",
        dev_fd,
        mnt_sb.st_mode & SFlag::S_IFMT.bits(),
        unistd::getuid().as_raw(),
        unistd::getgid().as_raw()
    );
    let opts = CString::new(&*opts).expect("CString::new failed");
    debug!("direct mount opts: {:?}", &opts);
    unsafe {
        let result = libc::mount(
            fsname.as_ptr(),
            mntpath.as_ptr(),
            fstype.as_ptr(),
            MS_NOSUID | MS_NODEV,
            opts.as_ptr() as *const c_void,
        );
        if result == 0 {
            debug!("mount {:?} to {:?} successfully!", mntpath, devpath);
            return dev_fd;
        } else {
            let e = Errno::from_i32(errno::errno());
            debug!("errno={}, {:?}", errno::errno(), e);
            let mount_fail_str = "mount failed!";
            #[cfg(target_arch = "aarch64")]
            libc::perror(mount_fail_str.as_ptr());
            #[cfg(target_arch = "x86_64")]
            libc::perror(mount_fail_str.as_ptr() as *const i8);

            return -1;
        }
    }
}

#[cfg(any(target_os = "macos"))]
pub fn umount(mount_point: &Path) -> i32 {
    let mntpnt = mount_point.as_os_str();
    unsafe { libc::unmount(mntpnt as *const _ as *const u8 as *const i8, MNT_FORCE) }
}

#[cfg(any(target_os = "macos"))]
pub fn mount(mount_point: &Path) -> RawFd {
    let devpath = Path::new("/dev/osxfuse1");
    let fd: RawFd;
    let res = fcntl::open(devpath, OFlag::O_RDWR, Mode::empty());
    match res {
        Ok(f) => {
            fd = f;
        }
        Err(e) => {
            error!("open fuse device failed, {}", e);
            return -1;
        }
    };

    let sb: FileStat;
    let result = stat::fstat(fd);
    match result {
        Ok(s) => sb = s,
        Err(e) => {
            error!("get fuse device stat failed! {}", e);
            return -1;
        }
    }

    // use ioctl to read device random secret
    // osxfuse/support/mount_osxfuse/mount_osxfuse.c#L1099
    // result = ioctl(fd, FUSEDEVIOCGETRANDOM, &drandom);
    // FUSEDEVIOCGETRANDOM // osxfuse/common/fuse_ioctl.h#L43
    use nix::ioctl_read;
    let mut drandom: u32 = 0;
    ioctl_read!(fuse_read_random, FUSE_IOC_MAGIC, FUSE_IOC_TYPE_MODE, u32);
    let result = unsafe { fuse_read_random(fd, &mut drandom as *mut _).unwrap() };
    if result == 0 {
        debug!("successfully read drandom={}", drandom);
    } else {
        let ioctl_fail_str = "ioctl read random secret failed!";
        unsafe {
            libc::perror(ioctl_fail_str.as_ptr() as *const i8);
        }
        return -1;
    }

    let full_path =
        fs::canonicalize(mount_point).expect("fail to get full path of mount point, {}");
    let cstr_path = full_path
        .to_str()
        .expect("full mount path to string failed");

    let mntpath = CString::new(cstr_path).expect("CString::new failed");
    let fstype = CString::new("osxfuse").expect("CString::new failed");
    let fsname = CString::new("macfuse").expect("CString::new failed");
    let fstypename = CString::new("").expect("CString::new failed");
    let volname = CString::new("OSXFUSE Volume 0 (macfuse)").expect("CString::new failed");

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

    unsafe {
        let result = libc::mount(
            fstype.as_ptr(),
            mntpath.as_ptr(),
            MNT_NOSUID | MNT_NODEV | MNT_NOUSERXATTR | MNT_NOATIME,
            &mut mnt_args as *mut _ as *mut c_void,
        );
        if result == 0 {
            debug!("mount {:?} to {:?} successfully!", mntpath, devpath);
            return fd;
        } else {
            let e = Errno::from_i32(errno::errno());
            debug!("errno={}, {:?}", errno::errno(), e);
            let mount_fail_str = "mount failed!";
            libc::perror(mount_fail_str.as_ptr() as *const i8);

            return -1;
        }
    }
}
