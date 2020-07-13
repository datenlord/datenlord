use anyhow::{self, Context};
use futures::lock::Mutex;
use lazy_static::lazy_static;
use log::{debug, error, info, warn};
use nix::errno::Errno;
use nix::unistd;
use smol::{self, blocking, Task};
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
    Arc,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::fs::*;
use super::fuse_reply::*;
use super::fuse_request::*;
use super::mount;
use super::protocol::*;

/// We generally support async reads
#[cfg(not(target_os = "macos"))]
const INIT_FLAGS: u32 = FUSE_ASYNC_READ;
// TODO: Add FUSE_EXPORT_SUPPORT and FUSE_BIG_WRITES (requires ABI 7.10)

/// On macOS, we additionally support case insensitiveness, volume renames and xtimes
/// TODO: we should eventually let the filesystem implementation decide which flags to set
#[cfg(target_os = "macos")]
const INIT_FLAGS: u32 = FUSE_ASYNC_READ | FUSE_CASE_INSENSITIVE | FUSE_VOL_RENAME | FUSE_XTIMES;
// TODO: Add FUSE_EXPORT_SUPPORT and FUSE_BIG_WRITES (requires ABI 7.10)

/// The max size of write requests from the kernel. The absolute minimum is 4k,
/// FUSE recommends at least 128k, max 16M. The FUSE default is 16M on macOS
/// and 128k on other systems.
#[cfg(not(target_os = "macos"))]
const MAX_WRITE_SIZE: u32 = 128 * 1024;
#[cfg(target_os = "macos")]
const MAX_WRITE_SIZE: u32 = 16 * 1024 * 1024;

/// Size of the buffer for reading a request from the kernel. Since the kernel may send
/// up to MAX_WRITE_SIZE bytes in a write request, we use that value plus some extra space.
const BUFFER_SIZE: usize = MAX_WRITE_SIZE as usize + 512;

// const MAX_BACKGROUND: u16 = 100;

lazy_static! {
    /// Static variable to indicate whether FUSE is initialized or not
    static ref FUSE_INITIALIZED: AtomicBool = AtomicBool::new(false);
    /// Static variable to indicate whether FUSE is destroyed or not
    static ref FUSE_DESTROYED: AtomicBool = AtomicBool::new(false);
}

#[derive(Debug)]
pub(crate) struct Session {
    mountpoint: PathBuf,
    fuse_fd: RawFd,
    /// FUSE protocol major version
    proto_major: AtomicU32,
    /// FUSE protocol minor version
    proto_minor: AtomicU32,
    /// The underlying FUSE file system
    filesystem: Arc<Mutex<FileSystem>>,
}

impl Drop for Session {
    fn drop(&mut self) {
        if !FUSE_DESTROYED.load(Ordering::Acquire) {
            let res = smol::block_on(async { mount::umount(&self.mountpoint).await });
            match res {
                Ok(..) => info!("successfully umount {:?}", self.mountpoint),
                Err(e) => error!(
                    "failed to umount {:?}, the error is: {}",
                    self.mountpoint, e,
                ),
            };
        }
    }
}

impl Session {
    pub fn fd(&self) -> RawFd {
        self.fuse_fd
    }

    pub async fn new(mountpoint: impl AsRef<Path>) -> anyhow::Result<Session> {
        if !mountpoint.as_ref().is_dir() {
            panic!("the input mount path is not a directory");
        }
        let mountpoint = mountpoint.as_ref().to_path_buf();
        let full_mountpoint = mountpoint
            .canonicalize()
            .context(format!("failed to find the mount path={:?}", mountpoint))?;
        let filesystem = FileSystem::new(&full_mountpoint).await?;
        // Must create filesystem before mount
        let fuse_fd = mount::mount(&full_mountpoint)
            .await
            .context("failed to mount fuse device")?;
        Ok(Session {
            mountpoint,
            fuse_fd,
            proto_major: AtomicU32::new(7),
            proto_minor: AtomicU32::new(8),
            filesystem: Arc::new(Mutex::new(filesystem)),
        })
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        // let chan = Channel::new(self).await?;
        // let fuse_fd = chan.fd();
        let fuse_fd = self.fuse_fd; // TODO: use Channel once fixed
        let mut byte_vec = vec![0u8; BUFFER_SIZE];
        let read_result = blocking!(
            let res = unistd::read(fuse_fd, &mut *byte_vec);
            (res, byte_vec)
        );
        byte_vec = read_result.1;
        if let Ok(read_size) = read_result.0 {
            debug!("read successfully {} byte data from FUSE device", read_size);
            if let Ok(req) = Request::new(&byte_vec) {
                if let Operation::Init { arg } = req.operation() {
                    let filesystem = self.filesystem.clone();
                    self.init(arg, &req, filesystem).await?;
                }
            }
        }
        debug_assert!(FUSE_INITIALIZED.load(Ordering::Acquire));

        loop {
            unsafe {
                // reset buffer size to its capacity, otherwise no enough buffer to read
                byte_vec.set_len(byte_vec.capacity());
            }
            let read_result = blocking!(
                let res = unistd::read(fuse_fd, &mut *byte_vec);
                (res, byte_vec)
            );
            byte_vec = read_result.1;
            match read_result.0 {
                Ok(read_size) => {
                    debug!("read successfully {} byte data from FUSE device", read_size);
                    unsafe {
                        // set buffer size to read size, otherwise encounter panic
                        byte_vec.set_len(read_size);
                    }

                    let fs = self.filesystem.clone();
                    byte_vec = Task::spawn(async move {
                        let req = match Request::new(&byte_vec) {
                            // Dispatch request
                            Ok(r) => r,
                            // Quit on illegal request
                            Err(e) => {
                                // TODO: graceful handle request build failure
                                panic!("failed to build FUSE request, the error is: {}", e);
                            }
                        };
                        debug!("{}", req);
                        let res = dispatch(&req, fuse_fd, fs).await;
                        if let Err(e) = res {
                            error!("failed to process request, the error is: {}", e);
                            let unique = req.unique();
                            let reply_error_to_fuse = ReplyEmpty::new(unique, fuse_fd);
                            let error_num = match e.downcast_ref::<nix::Error>() {
                                Some(nix_error) => match nix_error.as_errno() {
                                    Some(nix_errno) => {
                                        let std_io_error = std::io::Error::from(nix_errno);
                                        match std_io_error.raw_os_error() {
                                            Some(error_num) => error_num,
                                            // TODO: consider more meaningful error code
                                            None => libc::EINVAL,
                                        }
                                    }
                                    None => libc::EINVAL,
                                },
                                _ => libc::EINVAL,
                            };
                            // TODO: there is a bug!
                            // If the error from dispatch() is related to IO error with FUSE device,
                            // then it'll fail to reply error to FUSE again.
                            reply_error_to_fuse
                                .error(error_num)
                                .await
                                .unwrap_or_else(|_| {
                                    panic!(
                                        "failed to send error reply for request, unique={}",
                                        unique
                                    )
                                });
                            // TODO: this panic is for fast fail, can be removed when stable
                            panic!("failed to process request, the error is: {}", e);
                        }
                        byte_vec
                    })
                    .await; // TODO: consider use detach() to run in concurrency
                }
                Err(err) => {
                    error!("receive failed, the error is: {:?}", err);
                    match err.as_errno() {
                        // Operation interrupted. Accordingly to FUSE, this is safe to retry
                        Some(Errno::ENOENT) => {
                            info!("operation interrupted, retry.");
                        }
                        // Interrupted system call, retry
                        Some(Errno::EINTR) => {
                            info!("interrupted system call, retry");
                        }
                        // Explicitly try again
                        Some(Errno::EAGAIN) => info!("Explicitly retry"),
                        // Filesystem was unmounted, quit the loop
                        Some(Errno::ENODEV) => {
                            if FUSE_DESTROYED.load(Ordering::Acquire) {
                                info!("FUSE unmounted, quit the run loop");
                            } else {
                                error!("something wrong with FUSE device");
                            }
                            break;
                        }
                        // Unhandled error
                        _ => {
                            error!(
                                "non-recoverable io error when read FUSE device, \
                                    the error is: {}",
                                err,
                            );
                            break;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn init<'a>(
        &self,
        arg: &'a FuseInitIn,
        req: &'a Request<'a>,
        fs: Arc<Mutex<FileSystem>>,
    ) -> anyhow::Result<()> {
        debug!("Init args={:?}", arg);
        let fd = self.fuse_fd;
        // TODO: rewrite init based on do_init() in fuse_lowlevel.c
        // https://github.com/libfuse/libfuse/blob/master/lib/fuse_lowlevel.c#L1892
        let reply = ReplyInit::new(req.unique(), fd);
        // We don't support ABI versions before 7.8
        if arg.major < 7 || (arg.major == 7 && arg.minor < 8) {
            error!("Unsupported FUSE ABI version={}.{}", arg.major, arg.minor);
            reply.error(libc::EPROTO).await?;
            return Err(anyhow::anyhow!("FUSE ABI version too low"));
        }
        // Call filesystem init method and give it a chance to return an error
        let mut filesystem = fs.lock().await;
        let res = filesystem.init(&req);
        if let Err(err) = res {
            reply.error(libc::ENOSYS).await?;
            return Err(anyhow::anyhow!(
                "user defined init failed, the error is: {}",
                err
            ));
        }
        debug_assert!(
            arg.max_readahead <= MAX_WRITE_SIZE,
            "the max readahead={} larger than max write size 16M={}",
            arg.max_readahead,
            MAX_WRITE_SIZE,
        );
        let flags = arg.flags & INIT_FLAGS; // TODO: handle init flags properly
        #[cfg(not(feature = "abi-7-13"))]
        let unused = 0u32;
        #[cfg(feature = "abi-7-13")]
        let congestion_threshold = 100u16; // TODO: set congestion threshold
        #[cfg(feature = "abi-7-23")]
        let time_gran = 1u32; // TODO: set time_gran
        #[cfg(all(feature = "abi-7-23", not(feature = "abi-7-28")))]
        let unused = [0u32; 9];
        #[cfg(feature = "abi-7-28")]
        let max_pages = 0u16; // TODO: max_pages = (max_write - 1) / getpagesize() + 1;
        #[cfg(feature = "abi-7-28")]
        let padding = 0u16;
        #[cfg(feature = "abi-7-28")]
        let unused = [0u32; 8];
        // Reply with our desired version and settings. If the kernel supports a
        // larger major version, it'll re-send a matching init message. If it
        // supports only lower major versions, we replied with an error above.
        reply
            .init(
                FUSE_KERNEL_VERSION,
                FUSE_KERNEL_MINOR_VERSION, // Do not change minor version, otherwise unknown panic
                arg.max_readahead,         // accept FUSE kernel module max_readahead
                flags, // TODO: use features given in INIT_FLAGS and reported as capable
                #[cfg(not(feature = "abi-7-13"))]
                unused,
                #[cfg(feature = "abi-7-13")]
                MAX_BACKGROUND,
                #[cfg(feature = "abi-7-13")]
                congestion_threshold,
                MAX_WRITE_SIZE,
                #[cfg(feature = "abi-7-23")]
                time_gran,
                #[cfg(all(feature = "abi-7-23", not(feature = "abi-7-28")))]
                unused,
                #[cfg(feature = "abi-7-28")]
                max_pages,
                #[cfg(feature = "abi-7-28")]
                padding,
                #[cfg(feature = "abi-7-28")]
                unused,
            )
            .await?;
        debug!(
            "INIT response: ABI version={}.{}, flags={:#x}, max readahead={}, max write={}",
            FUSE_KERNEL_VERSION,
            FUSE_KERNEL_MINOR_VERSION,
            flags,
            arg.max_readahead,
            MAX_WRITE_SIZE,
        );

        // Store the kernel FUSE major and minor version
        self.proto_major.store(arg.major, Ordering::Relaxed);
        self.proto_minor.store(arg.minor, Ordering::Relaxed);

        FUSE_INITIALIZED.store(true, Ordering::Relaxed);

        Ok(())
    }
}

/// Dispatch request to the filesystem
/// This calls the appropriate filesystem operation method for the
/// request and sends back the returned reply to the kernel
async fn dispatch<'a>(
    req: &'a Request<'a>,
    fd: RawFd,
    fs: Arc<Mutex<FileSystem>>,
) -> anyhow::Result<()> {
    // TODO: consider remove this global lock to filesystem
    let mut filesystem = fs.lock().await;

    match req.operation() {
        // Filesystem initialization
        Operation::Init { .. } => panic!("FUSE should have already initialized"),
        // Any operation is invalid before initialization
        _ if !FUSE_INITIALIZED.load(Ordering::Acquire) => {
            warn!("ignoring FUSE operation before init, the request={}", req);
            let reply = ReplyEmpty::new(req.unique(), fd);
            reply.error(libc::EIO).await?;
        }
        // Filesystem destroyed
        Operation::Destroy => {
            filesystem.destroy(&req);
            FUSE_DESTROYED.fetch_or(true, Ordering::Release);
            let reply = ReplyEmpty::new(req.unique(), fd);
            reply.ok().await?;
        }
        // Any operation is invalid after destroy
        _ if FUSE_DESTROYED.load(Ordering::Acquire) => {
            warn!("ignoring FUSE operation after destroy, the request={}", req);
            let reply = ReplyEmpty::new(req.unique(), fd);
            reply.error(libc::EIO).await?;
        }

        Operation::Interrupt { .. } => {
            // TODO: handle FUSE_INTERRUPT
            let reply = ReplyEmpty::new(req.unique(), fd);
            reply.error(libc::ENOSYS).await?;
        }

        Operation::Lookup { name } => {
            let reply = ReplyEntry::new(req.unique(), fd);
            filesystem.lookup(&req, req.nodeid(), &name, reply).await?;
        }
        Operation::Forget { arg } => {
            filesystem.forget(&req, req.nodeid(), arg.nlookup); // no reply
        }
        Operation::GetAttr => {
            let reply = ReplyAttr::new(req.unique(), fd);
            filesystem.getattr(&req, req.nodeid(), reply).await?;
        }
        Operation::SetAttr { arg } => {
            let mode = match arg.valid & FATTR_MODE {
                0 => None,
                _ => Some(arg.mode),
            };
            let uid = match arg.valid & FATTR_UID {
                0 => None,
                _ => Some(arg.uid),
            };
            let gid = match arg.valid & FATTR_GID {
                0 => None,
                _ => Some(arg.gid),
            };
            let size = match arg.valid & FATTR_SIZE {
                0 => None,
                _ => Some(arg.size),
            };
            let atime = match arg.valid & FATTR_ATIME {
                0 => None,
                _ => Some(UNIX_EPOCH + Duration::new(arg.atime, arg.atimensec)),
            };
            let mtime = match arg.valid & FATTR_MTIME {
                0 => None,
                _ => Some(UNIX_EPOCH + Duration::new(arg.mtime, arg.mtimensec)),
            };
            let fh = match arg.valid & FATTR_FH {
                0 => None,
                _ => Some(arg.fh),
            };
            #[cfg(target_os = "macos")]
            #[inline]
            fn get_macos_setattr(
                arg: &FuseSetAttrIn,
            ) -> (
                Option<SystemTime>,
                Option<SystemTime>,
                Option<SystemTime>,
                Option<u32>,
            ) {
                let crtime = match arg.valid & FATTR_CRTIME {
                    0 => None,
                    _ => Some(
                        match UNIX_EPOCH.checked_add(Duration::new(arg.crtime, arg.crtimensec)) {
                            Some(crt) => crt,
                            None => SystemTime::now(),
                        },
                    ), // _ => Some(UNIX_EPOCH + Duration::new(arg.crtime, arg.crtimensec)),
                };
                let chgtime = match arg.valid & FATTR_CHGTIME {
                    0 => None,
                    _ => Some(
                        match UNIX_EPOCH.checked_add(Duration::new(arg.chgtime, arg.chgtimensec)) {
                            Some(cht) => cht,
                            None => SystemTime::now(),
                        },
                    ), // _ => Some(UNIX_EPOCH + Duration::new(arg.chgtime, arg.chgtimensec)),
                };
                let bkuptime = match arg.valid & FATTR_BKUPTIME {
                    0 => None,
                    _ => Some(
                        match UNIX_EPOCH.checked_add(Duration::new(arg.bkuptime, arg.bkuptimensec))
                        {
                            Some(bkt) => bkt,
                            None => SystemTime::now(),
                        },
                    ), // _ => Some(UNIX_EPOCH + Duration::new(arg.bkuptime, arg.bkuptimensec)),
                };
                let flags = match arg.valid & FATTR_FLAGS {
                    0 => None,
                    _ => Some(arg.flags),
                };
                (crtime, chgtime, bkuptime, flags)
            }
            #[cfg(not(target_os = "macos"))]
            #[inline]
            fn get_macos_setattr(
                _arg: &FuseSetAttrIn,
            ) -> (
                Option<SystemTime>,
                Option<SystemTime>,
                Option<SystemTime>,
                Option<u32>,
            ) {
                (None, None, None, None)
            }
            let (crtime, chgtime, bkuptime, flags) = get_macos_setattr(arg);
            let reply = ReplyAttr::new(req.unique(), fd);
            filesystem
                .setattr(
                    &req,
                    req.nodeid(),
                    mode,
                    uid,
                    gid,
                    size,
                    atime,
                    mtime,
                    fh,
                    crtime,
                    chgtime,
                    bkuptime,
                    flags,
                    reply,
                )
                .await?;
        }
        Operation::ReadLink => {
            let reply = ReplyData::new(req.unique(), fd);
            filesystem.readlink(&req, req.nodeid(), reply).await?;
        }
        Operation::MkNod { arg, name } => {
            let reply = ReplyEntry::new(req.unique(), fd);
            filesystem
                .mknod(&req, req.nodeid(), &name, arg.mode, arg.rdev, reply)
                .await?;
        }
        Operation::MkDir { arg, name } => {
            let reply = ReplyEntry::new(req.unique(), fd);
            filesystem
                .mkdir(&req, req.nodeid(), name, arg.mode, reply)
                .await?;
        }
        Operation::Unlink { name } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem.unlink(&req, req.nodeid(), &name, reply).await?;
        }
        Operation::RmDir { name } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem.rmdir(&req, req.nodeid(), &name, reply).await?;
        }
        Operation::SymLink { name, link } => {
            let reply = ReplyEntry::new(req.unique(), fd);
            filesystem
                .symlink(&req, req.nodeid(), &name, &Path::new(link), reply)
                .await?;
        }
        Operation::Rename {
            arg,
            oldname,
            newname,
        } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem
                .rename(&req, req.nodeid(), &oldname, arg.newdir, &newname, reply)
                .await?;
        }
        Operation::Link { arg, name } => {
            let reply = ReplyEntry::new(req.unique(), fd);
            filesystem
                .link(&req, arg.oldnodeid, req.nodeid(), &name, reply)
                .await?;
        }
        Operation::Open { arg } => {
            let reply = ReplyOpen::new(req.unique(), fd);
            filesystem
                .open(&req, req.nodeid(), arg.flags, reply)
                .await?;
        }
        Operation::Read { arg } => {
            let reply = ReplyData::new(req.unique(), fd);
            filesystem
                .read(
                    &req,
                    req.nodeid(),
                    arg.fh,
                    arg.offset as i64,
                    arg.size,
                    reply,
                )
                .await?;
        }
        Operation::Write { arg, data } => {
            assert_eq!(data.len(), arg.size as usize);
            let reply = ReplyWrite::new(req.unique(), fd);
            filesystem
                .write(
                    &req,
                    req.nodeid(),
                    arg.fh,
                    arg.offset as i64,
                    data.to_vec(), // TODO: consider zero copy
                    arg.write_flags,
                    reply,
                )
                .await?;
        }
        Operation::Flush { arg } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem
                .flush(&req, req.nodeid(), arg.fh, arg.lock_owner, reply)
                .await?;
        }
        Operation::Release { arg } => {
            let flush = match arg.release_flags & FUSE_RELEASE_FLUSH {
                0 => false,
                _ => true,
            };
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem
                .release(
                    &req,
                    req.nodeid(),
                    arg.fh,
                    arg.flags,
                    arg.lock_owner,
                    flush,
                    reply,
                )
                .await;
        }
        Operation::FSync { arg } => {
            let datasync = match arg.fsync_flags & 1 {
                0 => false,
                _ => true,
            };
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem
                .fsync(&req, req.nodeid(), arg.fh, datasync, reply)
                .await?;
        }
        Operation::OpenDir { arg } => {
            let reply = ReplyOpen::new(req.unique(), fd);
            filesystem
                .opendir(&req, req.nodeid(), arg.flags, reply)
                .await?;
        }
        Operation::ReadDir { arg } => {
            let reply = ReplyDirectory::new(req.unique(), fd, arg.size as usize);
            filesystem
                .readdir(&req, req.nodeid(), arg.fh, arg.offset as i64, reply)
                .await?;
        }
        Operation::ReleaseDir { arg } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem
                .releasedir(&req, req.nodeid(), arg.fh, arg.flags, reply)
                .await;
        }
        Operation::FSyncDir { arg } => {
            let datasync = match arg.fsync_flags & 1 {
                0 => false,
                _ => true,
            };
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem
                .fsyncdir(&req, req.nodeid(), arg.fh, datasync, reply)
                .await?;
        }
        Operation::StatFs => {
            let reply = ReplyStatFs::new(req.unique(), fd);
            filesystem.statfs(&req, req.nodeid(), reply).await?;
        }
        Operation::SetXAttr { arg, name, value } => {
            assert!(value.len() == arg.size as usize);
            #[cfg(target_os = "macos")]
            #[inline]
            fn get_position(arg: &FuseSetXAttrIn) -> u32 {
                arg.position
            }
            #[cfg(not(target_os = "macos"))]
            #[inline]
            fn get_position(_arg: &FuseSetXAttrIn) -> u32 {
                0
            }
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem
                .setxattr(
                    &req,
                    req.nodeid(),
                    name,
                    value,
                    arg.flags,
                    get_position(arg),
                    reply,
                )
                .await?;
        }
        Operation::GetXAttr { arg, name } => {
            let reply = ReplyXAttr::new(req.unique(), fd);
            filesystem
                .getxattr(&req, req.nodeid(), name, arg.size, reply)
                .await?;
        }
        Operation::ListXAttr { arg } => {
            let reply = ReplyXAttr::new(req.unique(), fd);
            filesystem
                .listxattr(&req, req.nodeid(), arg.size, reply)
                .await?;
        }
        Operation::RemoveXAttr { name } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem
                .removexattr(&req, req.nodeid(), name, reply)
                .await?;
        }
        Operation::Access { arg } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem
                .access(&req, req.nodeid(), arg.mask, reply)
                .await?;
        }
        Operation::Create { arg, name } => {
            let reply = ReplyCreate::new(req.unique(), fd);
            filesystem
                .create(&req, req.nodeid(), &name, arg.mode, arg.flags, reply)
                .await?;
        }
        Operation::GetLk { arg } => {
            let reply = ReplyLock::new(req.unique(), fd);
            filesystem
                .getlk(
                    &req,
                    req.nodeid(),
                    arg.fh,
                    arg.owner,
                    arg.lk.start,
                    arg.lk.end,
                    arg.lk.typ,
                    arg.lk.pid,
                    reply,
                )
                .await?;
        }
        Operation::SetLk { arg } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem
                .setlk(
                    &req,
                    req.nodeid(),
                    arg.fh,
                    arg.owner,
                    arg.lk.start,
                    arg.lk.end,
                    arg.lk.typ,
                    arg.lk.pid,
                    false,
                    reply,
                )
                .await?;
        }
        Operation::SetLkW { arg } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem
                .setlk(
                    &req,
                    req.nodeid(),
                    arg.fh,
                    arg.owner,
                    arg.lk.start,
                    arg.lk.end,
                    arg.lk.typ,
                    arg.lk.pid,
                    true,
                    reply,
                )
                .await?;
        }
        Operation::BMap { arg } => {
            let reply = ReplyBMap::new(req.unique(), fd);
            filesystem
                .bmap(&req, req.nodeid(), arg.blocksize, arg.block, reply)
                .await?;
        }

        #[cfg(target_os = "macos")]
        Operation::SetVolName { name } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem.setvolname(&req, name, reply).await?;
        }
        #[cfg(target_os = "macos")]
        Operation::Exchange {
            arg,
            oldname,
            newname,
        } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem
                .exchange(
                    &req,
                    arg.olddir,
                    &oldname,
                    arg.newdir,
                    &newname,
                    arg.options,
                    reply,
                )
                .await?;
        }
        #[cfg(target_os = "macos")]
        Operation::GetXTimes => {
            let reply = ReplyXTimes::new(req.unique(), fd);
            filesystem.getxtimes(&req, req.nodeid(), reply).await?;
        }
    }
    Ok(())
}
