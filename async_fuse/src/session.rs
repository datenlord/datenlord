use anyhow::{self, Context};
use futures::stream::StreamExt;
use lazy_static::lazy_static;
use log::{debug, error, info, warn};
use smol::{self, blocking, Task};
use std::os::unix::io::{FromRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::filesystem::*;
use super::fuse_read::*;
use super::fuse_reply::*;
use super::fuse_request::*;
use super::mount;
use super::protocal::*;

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
const MAX_WRITE_SIZE: u32 = 16 * 1024 * 1024;

/// Size of the buffer for reading a request from the kernel. Since the kernel may send
/// up to MAX_WRITE_SIZE bytes in a write request, we use that value plus some extra space.
const BUFFER_SIZE: usize = MAX_WRITE_SIZE as usize + 4096;

const MAX_BACKGROUND: u16 = 100;

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
}

impl Session {
    pub async fn new(mountpoint: impl AsRef<Path>) -> anyhow::Result<Session> {
        let mountpoint = mountpoint.as_ref().to_path_buf();
        let mountpoint = mountpoint
            .canonicalize()
            .context(format!("failed to find the mount path: {:?}", mountpoint))?;
        let fuse_fd = mount::mount(&mountpoint)
            .await
            .context("failed to mount fuse device")?;
        Ok(Session {
            mountpoint,
            fuse_fd,
            proto_major: AtomicU32::new(7),
            proto_minor: AtomicU32::new(8),
        })
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let fuse_fd = self.fuse_fd;
        let fuse_reader = blocking!(unsafe { std::fs::File::from_raw_fd(fuse_fd) });
        let fuse_reader = smol::reader(fuse_reader);
        let mut frs = FuseBufReadStream::with_capacity(BUFFER_SIZE, fuse_reader);
        if let Some(Ok(byte_vec)) = frs.next().await {
            if let Ok(req) = Request::new(&byte_vec) {
                if let Operation::Init { arg } = req.operation() {
                    let fs = FileSystem::new(); // TODO: refactor this
                    self.init(arg, &req, fs).await?;
                }
            }
        }
        debug_assert!(FUSE_INITIALIZED.load(Ordering::Acquire));

        // frs.for_each_concurrent(MAX_BACKGROUND as usize, |res| async move {
        while let Some(res) = frs.next().await {
            match res {
                Ok(byte_vec) => {
                    debug!("receive successfully {} byte data", byte_vec.len());
                    let fs = FileSystem::new(); // TODO: refactor this
                    Task::spawn(dispatch(byte_vec, fuse_fd, fs)).detach();
                }
                Err(err) => {
                    error!("receive failed, the error is: {:?}", err);
                    match err.raw_os_error() {
                        // Operation interrupted. Accordingly to FUSE, this is safe to retry
                        Some(libc::ENOENT) => {
                            info!("Operation interrupted, retry.");
                        }
                        // Interrupted system call, retry
                        Some(libc::EINTR) => {
                            info!("Interrupted system call, retry");
                        }
                        // Explicitly try again
                        Some(libc::EAGAIN) => info!("Explicitly retry"),
                        // Filesystem was unmounted, quit the loop
                        Some(libc::ENODEV) => {
                            panic!("FUSE unmounted, quit the run loop");
                        }
                        // Unhandled error
                        _ => unimplemented!(),
                    }
                }
            }
        }
        // ).await;

        mount::umount(&self.mountpoint).await?;
        Ok(())
    }

    async fn init<'a>(
        &self,
        arg: &'a FuseInitIn,
        req: &'a Request<'a>,
        mut filesystem: FileSystem,
    ) -> anyhow::Result<()> {
        debug!("Init args: {:?}", arg);
        let fd = self.fuse_fd;
        // TODO: rewrite init based on do_init() in fuse_lowlevel.c
        // https://github.com/libfuse/libfuse/blob/master/lib/fuse_lowlevel.c#L1892
        let reply = ReplyInit::new(req.unique(), fd);
        // We don't support ABI versions before 7.8
        if arg.major < 7 || (arg.major == 7 && arg.minor < 8) {
            error!("Unsupported FUSE ABI version {}.{}", arg.major, arg.minor);
            reply.error(libc::EPROTO).await;
            return Err(anyhow::anyhow!("FUSE ABI version too low"));
        }
        // Call filesystem init method and give it a chance to return an error
        let res = filesystem.init(&req);
        if let Err(err) = res {
            reply.error(libc::ENOSYS).await;
            return Err(anyhow::anyhow!(
                "user defined init failed, the error is: {}",
                err
            ));
        }
        let max_readahead = if (BUFFER_SIZE as u32) < arg.max_readahead {
            BUFFER_SIZE as u32
        } else {
            arg.max_readahead
        };
        let flags = arg.flags & INIT_FLAGS;
        #[cfg(not(feature = "abi-7-13"))]
        let unused = 0u32;
        #[cfg(feature = "abi-7-13")]
        let congestion_threshold = 100u16;
        #[cfg(feature = "abi-7-23")]
        let time_gran = 1u32;
        #[cfg(all(feature = "abi-7-23", not(feature = "abi-7-28")))]
        let unused = [0u32; 9];
        #[cfg(feature = "abi-7-28")]
        let max_pages = 0u16; // max_pages = (max_write - 1) / getpagesize() + 1;
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
                19,            //FUSE_KERNEL_MINOR_VERSION,
                max_readahead, // TODO: adjust BUFFER_SIZE according to max_readahead
                flags,         // use features given in INIT_FLAGS and reported as capable
                #[cfg(not(feature = "abi-7-13"))]
                unused,
                #[cfg(feature = "abi-7-13")]
                MAX_BACKGROUND,
                #[cfg(feature = "abi-7-13")]
                congestion_threshold,
                MAX_WRITE_SIZE, // TODO: use a max write size that fits into the session's buffer
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
            .await;
        debug!(
            "INIT response: ABI {}.{}, flags {:#x}, max readahead {}, max write {}",
            FUSE_KERNEL_VERSION, FUSE_KERNEL_MINOR_VERSION, flags, max_readahead, MAX_WRITE_SIZE,
        );

        // Store the kernel FUSE major and minor version
        self.proto_major.store(arg.major, Ordering::Relaxed);
        self.proto_minor.store(arg.minor, Ordering::Relaxed);

        FUSE_INITIALIZED.store(true, Ordering::Relaxed);

        Ok(())
    }
}

/// Dispatch request to the given filesystem.
/// This calls the appropriate filesystem operation method for the
/// request and sends back the returned reply to the kernel
async fn dispatch<'a>(byte_vec: Vec<u8>, fd: RawFd, mut filesystem: FileSystem) {
    let req = match Request::new(&byte_vec) {
        // Dispatch request
        Ok(r) => r,
        // Quit on illegal request
        Err(e) => {
            panic!("failed to build FUSE request, the error is: {}", e);
            // TODO: graceful handle
        }
    };
    debug!("{}", req);

    match req.operation() {
        // Filesystem initialization
        Operation::Init { .. } => unreachable!("FUSE should have already initialized"),
        // Any operation is invalid before initialization
        _ if !FUSE_INITIALIZED.load(Ordering::Acquire) => {
            warn!("ignoring FUSE operation before init: {}", req);
            let reply = ReplyEmpty::new(req.unique(), fd);
            reply.error(libc::EIO);
        }
        // Filesystem destroyed
        Operation::Destroy => {
            filesystem.destroy(&req);
            FUSE_DESTROYED.fetch_or(true, Ordering::Release);
            let reply = ReplyEmpty::new(req.unique(), fd);
            reply.ok();
        }
        // Any operation is invalid after destroy
        _ if FUSE_DESTROYED.load(Ordering::Acquire) => {
            warn!("ignoring FUSE operation after destroy: {}", req);
            let reply = ReplyEmpty::new(req.unique(), fd);
            reply.error(libc::EIO);
        }

        Operation::Interrupt { .. } => {
            // TODO: handle FUSE_INTERRUPT
            let reply = ReplyEmpty::new(req.unique(), fd);
            reply.error(libc::ENOSYS);
        }

        Operation::Lookup { name } => {
            let reply = ReplyEntry::new(req.unique(), fd);
            filesystem.lookup(&req, req.nodeid(), &name, reply);
        }
        Operation::Forget { arg } => {
            filesystem.forget(&req, req.nodeid(), arg.nlookup); // no reply
        }
        Operation::GetAttr => {
            let reply = ReplyAttr::new(req.unique(), fd);
            filesystem.getattr(&req, req.nodeid(), reply).await;
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
            filesystem.setattr(
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
            );
        }
        Operation::ReadLink => {
            let reply = ReplyData::new(req.unique(), fd);
            filesystem.readlink(&req, req.nodeid(), reply);
        }
        Operation::MkNod { arg, name } => {
            let reply = ReplyEntry::new(req.unique(), fd);
            filesystem.mknod(&req, req.nodeid(), &name, arg.mode, arg.rdev, reply);
        }
        Operation::MkDir { arg, name } => {
            let reply = ReplyEntry::new(req.unique(), fd);
            filesystem.mkdir(&req, req.nodeid(), &name, arg.mode, reply);
        }
        Operation::Unlink { name } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem.unlink(&req, req.nodeid(), &name, reply);
        }
        Operation::RmDir { name } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem.rmdir(&req, req.nodeid(), &name, reply);
        }
        Operation::SymLink { name, link } => {
            let reply = ReplyEntry::new(req.unique(), fd);
            filesystem.symlink(&req, req.nodeid(), &name, &Path::new(link), reply);
        }
        Operation::Rename {
            arg,
            oldname,
            newname,
        } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem.rename(&req, req.nodeid(), &oldname, arg.newdir, &newname, reply);
        }
        Operation::Link { arg, name } => {
            let reply = ReplyEntry::new(req.unique(), fd);
            filesystem.link(&req, arg.oldnodeid, req.nodeid(), &name, reply);
        }
        Operation::Open { arg } => {
            let reply = ReplyOpen::new(req.unique(), fd);
            filesystem.open(&req, req.nodeid(), arg.flags, reply);
        }
        Operation::Read { arg } => {
            let reply = ReplyData::new(req.unique(), fd);
            filesystem.read(
                &req,
                req.nodeid(),
                arg.fh,
                arg.offset as i64,
                arg.size,
                reply,
            );
        }
        Operation::Write { arg, data } => {
            assert_eq!(data.len(), arg.size as usize);
            let reply = ReplyWrite::new(req.unique(), fd);
            filesystem.write(
                &req,
                req.nodeid(),
                arg.fh,
                arg.offset as i64,
                data,
                arg.write_flags,
                reply,
            );
        }
        Operation::Flush { arg } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem.flush(&req, req.nodeid(), arg.fh, arg.lock_owner, reply);
        }
        Operation::Release { arg } => {
            let flush = match arg.release_flags & FUSE_RELEASE_FLUSH {
                0 => false,
                _ => true,
            };
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem.release(
                &req,
                req.nodeid(),
                arg.fh,
                arg.flags,
                arg.lock_owner,
                flush,
                reply,
            );
        }
        Operation::FSync { arg } => {
            let datasync = match arg.fsync_flags & 1 {
                0 => false,
                _ => true,
            };
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem.fsync(&req, req.nodeid(), arg.fh, datasync, reply);
        }
        Operation::OpenDir { arg } => {
            let reply = ReplyOpen::new(req.unique(), fd);
            filesystem.opendir(&req, req.nodeid(), arg.flags, reply);
        }
        Operation::ReadDir { arg } => {
            let reply = ReplyDirectory::new(req.unique(), fd, arg.size as usize);
            filesystem.readdir(&req, req.nodeid(), arg.fh, arg.offset as i64, reply);
        }
        Operation::ReleaseDir { arg } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem.releasedir(&req, req.nodeid(), arg.fh, arg.flags, reply);
        }
        Operation::FSyncDir { arg } => {
            let datasync = match arg.fsync_flags & 1 {
                0 => false,
                _ => true,
            };
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem.fsyncdir(&req, req.nodeid(), arg.fh, datasync, reply);
        }
        Operation::StatFs => {
            let reply = ReplyStatFs::new(req.unique(), fd);
            filesystem.statfs(&req, req.nodeid(), reply);
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
            filesystem.setxattr(
                &req,
                req.nodeid(),
                name,
                value,
                arg.flags,
                get_position(arg),
                reply,
            );
        }
        Operation::GetXAttr { arg, name } => {
            let reply = ReplyXAttr::new(req.unique(), fd);
            filesystem.getxattr(&req, req.nodeid(), name, arg.size, reply);
        }
        Operation::ListXAttr { arg } => {
            let reply = ReplyXAttr::new(req.unique(), fd);
            filesystem.listxattr(&req, req.nodeid(), arg.size, reply);
        }
        Operation::RemoveXAttr { name } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem.removexattr(&req, req.nodeid(), name, reply);
        }
        Operation::Access { arg } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem.access(&req, req.nodeid(), arg.mask, reply);
        }
        Operation::Create { arg, name } => {
            let reply = ReplyCreate::new(req.unique(), fd);
            filesystem.create(&req, req.nodeid(), &name, arg.mode, arg.flags, reply);
        }
        Operation::GetLk { arg } => {
            let reply = ReplyLock::new(req.unique(), fd);
            filesystem.getlk(
                &req,
                req.nodeid(),
                arg.fh,
                arg.owner,
                arg.lk.start,
                arg.lk.end,
                arg.lk.typ,
                arg.lk.pid,
                reply,
            );
        }
        Operation::SetLk { arg } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem.setlk(
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
            );
        }
        Operation::SetLkW { arg } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem.setlk(
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
            );
        }
        Operation::BMap { arg } => {
            let reply = ReplyBMap::new(req.unique(), fd);
            filesystem.bmap(&req, req.nodeid(), arg.blocksize, arg.block, reply);
        }

        #[cfg(target_os = "macos")]
        Operation::SetVolName { name } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem.setvolname(&req, name, reply);
        }
        #[cfg(target_os = "macos")]
        Operation::Exchange {
            arg,
            oldname,
            newname,
        } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem.exchange(
                &req,
                arg.olddir,
                &oldname,
                arg.newdir,
                &newname,
                arg.options,
                reply,
            );
        }
        #[cfg(target_os = "macos")]
        Operation::GetXTimes => {
            let reply = ReplyXTimes::new(req.unique(), fd);
            filesystem.getxtimes(&req, req.nodeid(), reply);
        }
    }
}
