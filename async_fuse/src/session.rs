//! The implementation of FUSE session

use aligned_bytes::AlignedBytes;
use anyhow::{anyhow, Context};
use crossbeam_channel::{Receiver, Sender};
use futures::lock::Mutex;
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
#[cfg(target_os = "macos")]
use std::time::SystemTime;
use std::time::{Duration, UNIX_EPOCH};
use utilities::Cast;

//use super::channel::Channel;
use super::fs::{self, FileLockParam, FileSystem, RenameParam, SetAttrParam};
#[cfg(target_os = "macos")]
use super::fuse_reply::ReplyXTimes;
use super::fuse_reply::{
    ReplyAttr, ReplyBMap, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyInit, ReplyLock, ReplyOpen, ReplyStatFs, ReplyWrite, ReplyXAttr,
};
use super::fuse_request::{Operation, Request};
use super::mount;
#[cfg(feature = "abi-7-23")]
use super::protocol::FATTR_CTIME;
#[cfg(feature = "abi-7-9")]
use super::protocol::FATTR_LOCKOWNER; // {FATTR_ATIME_NOW, FATTR_MTIME_NOW};
use super::protocol::{
    FuseInitIn, FuseSetXAttrIn, FATTR_ATIME, FATTR_FH, FATTR_GID, FATTR_MODE, FATTR_MTIME,
    FATTR_SIZE, FATTR_UID, FUSE_ASYNC_READ, FUSE_KERNEL_MINOR_VERSION, FUSE_KERNEL_VERSION,
    FUSE_RELEASE_FLUSH,
};
#[cfg(target_os = "macos")]
use super::protocol::{
    FATTR_BKUPTIME, FATTR_CHGTIME, FATTR_CRTIME, FATTR_FLAGS, FUSE_CASE_INSENSITIVE,
    FUSE_VOL_RENAME, FUSE_XTIMES,
};

/// We generally support async reads
#[cfg(target_os = "linux")]
const INIT_FLAGS: u32 = FUSE_ASYNC_READ;
// TODO: Add FUSE_EXPORT_SUPPORT and FUSE_BIG_WRITES (requires ABI 7.10)

/// On macOS, we additionally support case insensitiveness, volume renames and xtimes
/// TODO: we should eventually let the filesystem implementation decide which flags to set
#[cfg(target_os = "macos")]
const INIT_FLAGS: u32 = FUSE_ASYNC_READ | FUSE_CASE_INSENSITIVE | FUSE_VOL_RENAME | FUSE_XTIMES;
// TODO: Add FUSE_EXPORT_SUPPORT and FUSE_BIG_WRITES (requires ABI 7.10)

/// The max size of write requests from the kernel. The absolute minimum is 4k,
/// FUSE recommends at least 128k, max 16M. The FUSE default is  128k on Linux.
#[cfg(target_os = "linux")]
const MAX_WRITE_SIZE: u32 = 128 * 1024;
/// The FUSE default max size of write requests is 16M on macOS
#[cfg(target_os = "macos")]
const MAX_WRITE_SIZE: u32 = 16 * 1024 * 1024;

/// Size of the buffer for reading a request from the kernel. Since the kernel may send
/// up to `MAX_WRITE_SIZE` bytes in a write request, we use that value plus some extra space.
const BUFFER_SIZE: u32 = MAX_WRITE_SIZE + 512;

/// We use `PAGE_SIZE` (4 KiB) as the alignment of the buffer.
const PAGE_SIZE: usize = 4096;
/// Max background pending requests under processing, at least to be 4, otherwise deadlock
const MAX_BACKGROUND: u16 = 10; // TODO: set to larger value when release

/// Static variable to indicate whether FUSE is initialized or not
static FUSE_INITIALIZED: AtomicBool = AtomicBool::new(false);
/// Static variable to indicate whether FUSE is destroyed or not
static FUSE_DESTROYED: AtomicBool = AtomicBool::new(false);

/// FUSE session
#[derive(Debug)]
pub struct Session {
    /// FUSE mount point
    mountpoint: PathBuf,
    /// FUSE device fd
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
                Ok(..) => info!("Session::drop() successfully umount {:?}", self.mountpoint),
                Err(e) => error!(
                    "Session::drop() failed to umount {:?}, the error is: {}",
                    self.mountpoint, e,
                ),
            };
        }
    }
}

impl Session {
    /// Get FUSE device fd
    #[inline]
    pub const fn dev_fd(&self) -> RawFd {
        self.fuse_fd
    }

    /// Create FUSE session
    pub async fn new(mount_path: &Path) -> anyhow::Result<Self> {
        // let mount_path = Path::new(mount_point);
        assert!(
            mount_path.is_dir(),
            "the input mount path={:?} is not a directory",
            mount_path
        );

        let full_mount_path = mount_path
            .canonicalize()
            .with_context(|| format!("failed to find the mount path={:?}", mount_path))?;
        let filesystem = FileSystem::new(&full_mount_path).await?;
        // Must create filesystem before mount
        let fuse_fd = mount::mount(&full_mount_path)
            .await
            .context("failed to mount fuse device")?;
        Ok(Self {
            mountpoint: full_mount_path,
            fuse_fd,
            proto_major: AtomicU32::new(7),
            proto_minor: AtomicU32::new(8),
            filesystem: Arc::new(Mutex::new(filesystem)),
        })
    }

    /// Run the FUSE session
    pub async fn run(&self) -> anyhow::Result<()> {
        let (pool_sender, pool_receiver) = self
            .setup_buffer_pool()
            .await
            .context("failed to setup buffer pool")?;
        // let (pool_sender, pool_receiver) =
        //     crossbeam_channel::bounded::<(u16, AlignedBytes)>(MAX_BACKGROUND.into());

        // (0..MAX_BACKGROUND).for_each(|i| {
        //     let buf = AlignedBytes::new_zeroed(BUFFER_SIZE.cast(), PAGE_SIZE);
        //     let res = pool_sender.send((i, buf));
        //     if let Err(e) = res {
        //         panic!(
        //             "failed to insert buffer idx={} to buffer pool when initializing, the error is: {}",
        //             i, e,
        //         );
        //     }
        // });

        // let chan = Channel::new(self).await?;
        // let fuse_fd = chan.fd();
        // let (idx, mut byte_vec) = pool_receiver.recv()?;
        // let read_result = blocking!(
        //     let res = unistd::read(fuse_fd, &mut *byte_vec);
        //     (res, byte_vec)
        // );
        // byte_vec = read_result.1;
        // if let Ok(read_size) = read_result.0 {
        //     debug!("read successfully {} byte data from FUSE device", read_size);
        //     if let Ok(req) = Request::new(&byte_vec) {
        //         if let Operation::Init { arg } = req.operation() {
        //             let filesystem = Arc::clone(&self.filesystem);
        //             self.init(arg, &req, filesystem, fuse_fd).await?;
        //         }
        //     }
        // }
        // pool_sender.send((idx, byte_vec)).context(format!(
        //     "failed to put buffer idx={} back to buffer pool after FUSE init",
        //     idx,
        // ))?;
        // debug_assert!(FUSE_INITIALIZED.load(Ordering::Acquire));

        let fuse_dev_fd = self.dev_fd();
        loop {
            let (buffer_idx, mut byte_buffer) = pool_receiver.recv()?;

            let (res, byte_buffer) = blocking!(
                let res = unistd::read(fuse_dev_fd, &mut *byte_buffer);
                (res, byte_buffer)
            );

            match res {
                Ok(read_size) => {
                    debug!("read successfully {} byte data from FUSE device", read_size);

                    // let chan = Channel::new(self).await?;
                    let fuse_fd = fuse_dev_fd;
                    let fs = Arc::clone(&self.filesystem);
                    let sender = pool_sender.clone();
                    // Task::spawn(async move {
                    //     let bytes = byte_buffer.get(..read_size).unwrap_or_else(|| {
                    //         panic!(
                    //             "failed to read {} bytes from the {}-th buffer",
                    //             read_size, buffer_idx,
                    //         )
                    //     });
                    //     let fuse_req = match Request::new(bytes) {
                    //         // Dispatch request
                    //         Ok(r) => r,
                    //         // Quit on illegal request
                    //         Err(e) => {
                    //             // TODO: graceful handle request build failure
                    //             panic!("failed to build FUSE request, the error is: {}", e);
                    //         }
                    //     };
                    //     debug!("received {}", fuse_req);
                    //     let res = dispatch(&fuse_req, fuse_fd, fs).await;
                    //     if let Err(e) = res {
                    //         error!("failed to process request, the error is: {}", e);
                    //         let unique = fuse_req.unique();
                    //         let reply_error_to_fuse = ReplyEmpty::new(unique, fuse_fd);
                    //         let error_num = match e.downcast_ref::<nix::Error>() {
                    //             Some(nix_error) => match nix_error.as_errno() {
                    //                 Some(nix_errno) => {
                    //                     let std_io_error = std::io::Error::from(nix_errno);
                    //                     match std_io_error.raw_os_error() {
                    //                         Some(error_num) => error_num,
                    //                         // TODO: consider more meaningful error code
                    //                         None => libc::EINVAL,
                    //                     }
                    //                 }
                    //                 None => libc::EINVAL,
                    //             },
                    //             None => libc::EINVAL,
                    //         };
                    //         // TODO: there is a bug!
                    //         // If the error from dispatch() is related to IO error with FUSE device,
                    //         // then it'll fail to reply error to FUSE again.
                    //         reply_error_to_fuse
                    //             .error(error_num)
                    //             .await
                    //             .unwrap_or_else(|_| {
                    //                 panic!(
                    //                     "failed to send error reply for request, unique={}",
                    //                     unique
                    //                 );
                    //             });
                    //         // TODO: this panic is for fast fail, can be removed when stable
                    //         panic!("failed to process request, the error is: {}", e);
                    //     }
                    //     let res = sender.send((buffer_idx, byte_buffer));
                    //     if let Err(e) = res {
                    //         panic!(
                    //             "failed to put the {}-th buffer back to buffer pool, the error is: {}",
                    //             buffer_idx, e,
                    //         );
                    //     }
                    // })
                    Task::spawn(Self::process_fuse_request(
                        buffer_idx,
                        byte_buffer,
                        read_size,
                        fuse_fd,
                        fs,
                        sender,
                    ))
                    // .await; // Run in series
                    .detach(); // Run in parallel
                }
                Err(err) => {
                    let err_msg = fs::util::format_nix_error(err); // TODO: refactor format_nix_error()
                    error!(
                        "failed to receive from FUSE kernel, the error is: {}",
                        err_msg
                    );
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
                                info!("filesystem destroyed, quit the run loop");
                            } else {
                                error!("FUSE device got un-mounted");
                            }
                            break;
                        }
                        // Unhandled error
                        Some(..) | None => {
                            panic!(
                                "non-recoverable io error when read FUSE device, \
                                    the error is: {}",
                                err_msg,
                            );
                            // break;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Process one FUSE request
    async fn process_fuse_request(
        buffer_idx: u16,
        byte_buffer: AlignedBytes,
        read_size: usize,
        fuse_fd: RawFd,
        fs: Arc<Mutex<FileSystem>>,
        sender: Sender<(u16, AlignedBytes)>,
    ) {
        let bytes = byte_buffer.get(..read_size).unwrap_or_else(|| {
            panic!(
                "failed to read {} bytes from the {}-th buffer",
                read_size, buffer_idx,
            )
        });
        let fuse_req = match Request::new(bytes) {
            // Dispatch request
            Ok(r) => r,
            // Quit on illegal request
            Err(e) => {
                // TODO: graceful handle request build failure
                panic!("failed to build FUSE request, the error is: {}", e);
            }
        };
        debug!("received FUSE req={}", fuse_req);
        let res = dispatch(&fuse_req, fuse_fd, fs).await;
        if let Err(e) = res {
            panic!(
                "failed to process req={:?}, the error is: {}",
                fuse_req,
                fs::util::format_nix_error(e), // TODO: refactor format_nix_error()
            );
        }
        let res = sender.send((buffer_idx, byte_buffer));
        if let Err(e) = res {
            panic!(
                "failed to put the {}-th buffer back to buffer pool, the error is: {}",
                buffer_idx, e,
            );
        }
    }

    /// Setup buffer pool
    async fn setup_buffer_pool(
        &self,
    ) -> anyhow::Result<(Sender<(u16, AlignedBytes)>, Receiver<(u16, AlignedBytes)>)> {
        let (pool_sender, pool_receiver) =
            crossbeam_channel::bounded::<(u16, AlignedBytes)>(MAX_BACKGROUND.into());

        (0..MAX_BACKGROUND).for_each(|i| {
            let buf = AlignedBytes::new_zeroed(BUFFER_SIZE.cast(), PAGE_SIZE);
            let res = pool_sender.send((i, buf));
            if let Err(e) = res {
                panic!(
                    "failed to insert buffer idx={} to buffer pool when initializing, the error is: {}",
                    i, e,
                );
            }
        });

        let fuse_fd = self.dev_fd();
        let (idx, mut byte_vec) = pool_receiver.recv()?;
        let read_result = blocking!(
            let res = unistd::read(fuse_fd, &mut *byte_vec);
            (res, byte_vec)
        );
        byte_vec = read_result.1;
        if let Ok(read_size) = read_result.0 {
            debug!("read successfully {} byte data from FUSE device", read_size);
            if let Ok(req) = Request::new(&byte_vec) {
                if let Operation::Init { arg } = req.operation() {
                    let filesystem = Arc::clone(&self.filesystem);
                    self.init(arg, &req, filesystem, fuse_fd).await?;
                }
            }
        }
        pool_sender.send((idx, byte_vec)).context(format!(
            "failed to put buffer idx={} back to buffer pool after FUSE init",
            idx,
        ))?;
        debug_assert!(FUSE_INITIALIZED.load(Ordering::Acquire));

        Ok((pool_sender, pool_receiver))
    }

    /// Initialize FUSE session
    async fn init<'a>(
        &self,
        arg: &'a FuseInitIn,
        req: &'a Request<'a>,
        fs: Arc<Mutex<FileSystem>>,
        fd: RawFd,
    ) -> anyhow::Result<()> {
        debug!("Init args={:?}", arg);
        // TODO: rewrite init based on do_init() in fuse_lowlevel.c
        // https://github.com/libfuse/libfuse/blob/master/lib/fuse_lowlevel.c#L1892
        let reply = ReplyInit::new(req.unique(), fd);
        // We don't support ABI versions before 7.8
        if arg.major < 7 || (arg.major == 7 && arg.minor < 8) {
            error!("Unsupported FUSE ABI version={}.{}", arg.major, arg.minor);
            reply.error_code(libc::EPROTO).await?;
            return Err(anyhow!("FUSE ABI version too low"));
        }
        // Call filesystem init method and give it a chance to return an error
        let filesystem = fs.lock().await;
        let init_res = filesystem.init(req);
        if let Err(err) = init_res {
            reply.error_code(libc::ENOSYS).await?;
            return Err(anyhow!("user defined init failed, the error is: {}", err,));
        }
        debug_assert!(
            arg.max_readahead <= MAX_WRITE_SIZE,
            "the max readahead={} larger than max write size 16M={}",
            arg.max_readahead,
            MAX_WRITE_SIZE,
        );
        let flags = arg.flags & INIT_FLAGS; // TODO: handle init flags properly
        #[cfg(not(feature = "abi-7-13"))]
        let unused = 0_u32;
        #[cfg(feature = "abi-7-13")]
        let congestion_threshold = 10_u16; // TODO: set congestion threshold
        #[cfg(feature = "abi-7-23")]
        let time_gran = 1_u32; // TODO: set time_gran
        #[cfg(all(feature = "abi-7-23", not(feature = "abi-7-28")))]
        let unused = [0_u32; 9];
        #[cfg(feature = "abi-7-28")]
        let max_pages = 0_u16; // TODO: max_pages = (max_write - 1) / getpagesize() + 1;
        #[cfg(feature = "abi-7-28")]
        let padding = 0_u16;
        #[cfg(feature = "abi-7-28")]
        let unused = [0_u32; 8];
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
#[allow(clippy::too_many_lines)]
async fn dispatch<'a>(
    req: &'a Request<'a>,
    fd: RawFd,
    fs: Arc<Mutex<FileSystem>>,
) -> nix::Result<usize> {
    // TODO: consider remove this global lock to filesystem
    let mut filesystem = fs.lock().await;

    match req.operation() {
        // Filesystem initialization
        Operation::Init { .. } => panic!("FUSE should have already initialized"),
        // Any operation is invalid before initialization
        _ if !FUSE_INITIALIZED.load(Ordering::Acquire) => {
            warn!("ignoring FUSE operation before init, the request={}", req);
            let reply = ReplyEmpty::new(req.unique(), fd);
            reply.error_code(libc::EIO).await
        }
        // Filesystem destroyed
        Operation::Destroy => {
            filesystem.destroy(req);
            FUSE_DESTROYED.fetch_or(true, Ordering::Release);
            let reply = ReplyEmpty::new(req.unique(), fd);
            reply.ok().await
        }
        // Any operation is invalid after destroy
        _ if FUSE_DESTROYED.load(Ordering::Acquire) => {
            warn!("ignoring FUSE operation after destroy, the request={}", req);
            let reply = ReplyEmpty::new(req.unique(), fd);
            reply.error_code(libc::EIO).await
        }

        Operation::Interrupt { arg } => {
            filesystem.interrupt(req, arg.unique); // No reply
            Ok(0)
        }

        Operation::Lookup { name } => {
            let reply = ReplyEntry::new(req.unique(), fd);
            filesystem.lookup(req, req.nodeid(), name, reply).await
        }
        Operation::Forget { arg } => {
            filesystem.forget(req, arg.nlookup); // No reply
            Ok(0)
        }
        Operation::GetAttr => {
            let reply = ReplyAttr::new(req.unique(), fd);
            filesystem.getattr(req, reply).await
        }
        Operation::SetAttr { arg } => {
            let mode = match arg.valid & FATTR_MODE {
                0 => None,
                _ => Some(arg.mode),
            };
            let u_id = match arg.valid & FATTR_UID {
                0 => None,
                _ => Some(arg.uid),
            };
            let g_id = match arg.valid & FATTR_GID {
                0 => None,
                _ => Some(arg.gid),
            };
            let size = match arg.valid & FATTR_SIZE {
                0 => None,
                _ => Some(arg.size),
            };
            let a_time = match arg.valid & FATTR_ATIME {
                0 => None,
                _ => Some(UNIX_EPOCH + Duration::new(arg.atime, arg.atimensec)),
            };
            let m_time = match arg.valid & FATTR_MTIME {
                0 => None,
                _ => Some(UNIX_EPOCH + Duration::new(arg.mtime, arg.mtimensec)),
            };
            let fh = match arg.valid & FATTR_FH {
                0 => None,
                _ => Some(arg.fh),
            };
            // #[cfg(feature = "abi-7-9")]
            // let a_time = match arg.valid & FATTR_ATIME_NOW {
            //     0 => None,
            //     _ => Some(SystemTime::now()),
            // };
            // #[cfg(feature = "abi-7-9")]
            // let m_time = match arg.valid & FATTR_MTIME_NOW {
            //     0 => None,
            //     _ => Some(SystemTime::now()),
            // };
            #[cfg(feature = "abi-7-9")]
            let lock_owner = match arg.valid & FATTR_LOCKOWNER {
                0 => None,
                _ => Some(arg.lock_owner),
            };
            #[cfg(feature = "abi-7-23")]
            let c_time = match arg.valid & FATTR_CTIME {
                0 => None,
                _ => Some(UNIX_EPOCH + Duration::new(arg.ctime, arg.ctimensec)),
            };
            // Get extra file attributes especially for macOS
            #[cfg(target_os = "macos")]
            let (crtime, chgtime, bkuptime, flags) = {
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
            };

            let reply = ReplyAttr::new(req.unique(), fd);
            let param = SetAttrParam {
                valid: arg.valid,
                fh,
                mode,
                u_id,
                g_id,
                size,
                #[cfg(feature = "abi-7-9")]
                lock_owner,
                a_time,
                m_time,
                #[cfg(feature = "abi-7-23")]
                c_time,
                #[cfg(target_os = "macos")]
                crtime,
                #[cfg(target_os = "macos")]
                chgtime,
                #[cfg(target_os = "macos")]
                bkuptime,
                #[cfg(target_os = "macos")]
                flags,
            };
            filesystem.setattr(req, param, reply).await
        }
        Operation::ReadLink => {
            let reply = ReplyData::new(req.unique(), fd);
            filesystem.readlink(req, reply).await
        }
        Operation::MkNod { arg, name } => {
            let reply = ReplyEntry::new(req.unique(), fd);
            filesystem
                .mknod(req, req.nodeid(), name, arg.mode, arg.rdev, reply)
                .await
        }
        Operation::MkDir { arg, name } => {
            let reply = ReplyEntry::new(req.unique(), fd);
            filesystem
                .mkdir(req, req.nodeid(), name, arg.mode, reply)
                .await
        }
        Operation::Unlink { name } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem.unlink(req, req.nodeid(), name, reply).await
        }
        Operation::RmDir { name } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem.rmdir(req, req.nodeid(), name, reply).await
        }
        Operation::SymLink { name, link } => {
            let reply = ReplyEntry::new(req.unique(), fd);
            filesystem
                .symlink(req, req.nodeid(), name, Path::new(link), reply)
                .await
        }
        Operation::Rename {
            arg,
            oldname,
            newname,
        } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            let param = RenameParam {
                old_parent: req.nodeid(),
                old_name: oldname.to_os_string(),
                new_parent: arg.newdir,
                new_name: newname.to_os_string(),
                flags: 0,
            };
            filesystem.rename(req, param, reply).await
        }
        Operation::Link { arg, name } => {
            let reply = ReplyEntry::new(req.unique(), fd);
            filesystem.link(req, arg.oldnodeid, name, reply).await
        }
        Operation::Open { arg } => {
            let reply = ReplyOpen::new(req.unique(), fd);
            filesystem.open(req, arg.flags, reply).await
        }
        Operation::Read { arg } => {
            let reply = ReplyData::new(req.unique(), fd);
            filesystem
                .read(req, arg.fh, arg.offset.cast(), arg.size, reply)
                .await
        }
        Operation::Write { arg, data } => {
            assert_eq!(data.len(), arg.size.cast());
            let reply = ReplyWrite::new(req.unique(), fd);
            filesystem
                .write(
                    req,
                    arg.fh,
                    arg.offset.cast(),
                    data.to_vec(), // TODO: consider zero copy
                    arg.write_flags,
                    reply,
                )
                .await
        }
        Operation::Flush { arg } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem.flush(req, arg.fh, arg.lock_owner, reply).await
        }
        Operation::Release { arg } => {
            let flush = !matches!(arg.release_flags & FUSE_RELEASE_FLUSH, 0);
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem
                .release(req, arg.fh, arg.flags, arg.lock_owner, flush, reply)
                .await
        }
        Operation::FSync { arg } => {
            let datasync = !matches!(arg.fsync_flags & 1, 0);
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem.fsync(req, arg.fh, datasync, reply).await
        }
        Operation::OpenDir { arg } => {
            let reply = ReplyOpen::new(req.unique(), fd);
            filesystem.opendir(req, arg.flags, reply).await
        }
        Operation::ReadDir { arg } => {
            let reply = ReplyDirectory::new(req.unique(), fd, arg.size.cast());
            filesystem
                .readdir(req, arg.fh, arg.offset.cast(), reply)
                .await
        }
        Operation::ReleaseDir { arg } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem.releasedir(req, arg.fh, arg.flags, reply).await
        }
        Operation::FSyncDir { arg } => {
            let datasync = !matches!(arg.fsync_flags & 1, 0);
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem.fsyncdir(req, arg.fh, datasync, reply).await
        }
        Operation::StatFs => {
            let reply = ReplyStatFs::new(req.unique(), fd);
            filesystem.statfs(req, reply).await
        }
        Operation::SetXAttr { arg, name, value } => {
            /// Set the position of an extended attribute
            /// macOS only
            #[cfg(target_os = "macos")]
            #[inline]
            const fn get_position(arg: &FuseSetXAttrIn) -> u32 {
                arg.position
            }
            /// Set the position of an extended attribute
            /// zero for Linux
            #[cfg(target_os = "linux")]
            #[inline]
            const fn get_position(_arg: &FuseSetXAttrIn) -> u32 {
                0
            }
            assert!(value.len() == arg.size.cast());
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem
                .setxattr(req, name, value, arg.flags, get_position(arg), reply)
                .await
        }
        Operation::GetXAttr { arg, name } => {
            let reply = ReplyXAttr::new(req.unique(), fd);
            filesystem.getxattr(req, name, arg.size, reply).await
        }
        Operation::ListXAttr { arg } => {
            let reply = ReplyXAttr::new(req.unique(), fd);
            filesystem.listxattr(req, arg.size, reply).await
        }
        Operation::RemoveXAttr { name } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem.removexattr(req, name, reply).await
        }
        Operation::Access { arg } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem.access(req, arg.mask, reply).await
        }
        Operation::Create { arg, name } => {
            let reply = ReplyCreate::new(req.unique(), fd);
            filesystem
                .create(req, req.nodeid(), name, arg.mode, arg.flags, reply)
                .await
        }
        Operation::GetLk { arg } => {
            let reply = ReplyLock::new(req.unique(), fd);
            let lock_param = FileLockParam {
                fh: arg.fh,
                lock_owner: arg.owner,
                start: arg.lk.start,
                end: arg.lk.end,
                typ: arg.lk.typ,
                pid: arg.lk.pid,
            };
            filesystem.getlk(req, lock_param, reply).await
        }
        Operation::SetLk { arg } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            let lock_param = FileLockParam {
                fh: arg.fh,
                lock_owner: arg.owner,
                start: arg.lk.start,
                end: arg.lk.end,
                typ: arg.lk.typ,
                pid: arg.lk.pid,
            };
            filesystem.setlk(req, lock_param, false, reply).await
        }
        Operation::SetLkW { arg } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            let lock_param = FileLockParam {
                fh: arg.fh,
                lock_owner: arg.owner,
                start: arg.lk.start,
                end: arg.lk.end,
                typ: arg.lk.typ,
                pid: arg.lk.pid,
            };
            filesystem
                .setlk(
                    req, lock_param, true, // sleep
                    reply,
                )
                .await
        }
        Operation::BMap { arg } => {
            let reply = ReplyBMap::new(req.unique(), fd);
            filesystem.bmap(req, arg.blocksize, arg.block, reply).await
        }

        // #[cfg(feature = "abi-7-11")]
        Operation::IoCtl { arg, data } => {
            error!("IoCtl not implemented, arg={:?}, data={:?}", arg, data);
            filesystem.not_implement_helper(req, fd).await
        }
        // #[cfg(feature = "abi-7-11")]
        Operation::Poll { arg } => {
            error!("Poll not implemented, arg={:?}", arg);
            filesystem.not_implement_helper(req, fd).await
        }
        // #[cfg(feature = "abi-7-15")]
        Operation::NotifyReply { data } => {
            error!("NotifyReply not implemented, data={:?}", data);
            filesystem.not_implement_helper(req, fd).await
        }
        // #[cfg(feature = "abi-7-16")]
        Operation::BatchForget { arg, nodes } => {
            error!(
                "BatchForget not implemented, arg={:?}, nodes={:?}",
                arg, nodes
            );
            filesystem.not_implement_helper(req, fd).await
        }
        // #[cfg(feature = "abi-7-19")]
        Operation::FAllocate { arg } => {
            error!("FAllocate not implemented, arg={:?}", arg);
            filesystem.not_implement_helper(req, fd).await
        }
        // #[cfg(feature = "abi-7-21")]
        Operation::ReadDirPlus { arg } => {
            error!("ReadDirPlus not implemented, arg={:?}", arg);
            filesystem.not_implement_helper(req, fd).await
        }
        // #[cfg(feature = "abi-7-23")]
        Operation::Rename2 {
            arg,
            oldname,
            newname,
        } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            let param = RenameParam {
                old_parent: req.nodeid(),
                old_name: oldname.to_os_string(),
                new_parent: arg.newdir,
                new_name: newname.to_os_string(),
                #[cfg(target_os = "linux")]
                flags: arg.flags,
                #[cfg(target_os = "macos")]
                flags: arg.flags.cast(),
            };
            filesystem.rename(req, param, reply).await
        }
        // #[cfg(feature = "abi-7-24")]
        Operation::LSeek { arg } => {
            error!("LSeek not implemented, arg={:?}", arg);
            filesystem.not_implement_helper(req, fd).await
        }
        // #[cfg(feature = "abi-7-28")]
        Operation::CopyFileRange { arg } => {
            error!("ReadDirPlusCopyFileRange not implemented, arg={:?}", arg);
            filesystem.not_implement_helper(req, fd).await
        }

        #[cfg(target_os = "macos")]
        Operation::SetVolName { name } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            filesystem.setvolname(req, name, reply).await
        }
        #[cfg(target_os = "macos")]
        Operation::Exchange {
            arg,
            oldname,
            newname,
        } => {
            let reply = ReplyEmpty::new(req.unique(), fd);
            let param = RenameParam {
                old_parent: req.nodeid(),
                old_name: oldname.to_os_string(),
                new_parent: arg.newdir,
                new_name: newname.to_os_string(),
                flags: arg.options,
            };
            filesystem.exchange(req, param, reply).await
        }
        #[cfg(target_os = "macos")]
        Operation::GetXTimes => {
            let reply = ReplyXTimes::new(req.unique(), fd);
            filesystem.getxtimes(req, reply).await
        }

        #[cfg(feature = "abi-7-11")]
        Operation::CuseInit { arg } => {
            todo!("CuseInit arg={:?}", arg);
        }
    }
}
