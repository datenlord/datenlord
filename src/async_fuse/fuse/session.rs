//! The implementation of FUSE session

use std::fs::File;
use std::io::Read;
use std::os::fd::FromRawFd;
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, UNIX_EPOCH};

use aligned_utils::bytes::AlignedBytes;
use anyhow::{anyhow, Context};
use clippy_utilities::Cast;
use crossbeam_channel::{Receiver, Sender};
use crossbeam_utils::atomic::AtomicCell;
use datenlord::common::task_manager::{GcHandle, TaskName, TASK_MANAGER};
use nix::errno::Errno;
use nix::sys::stat::SFlag;
use nix::unistd;
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument};

use super::context::ProtoVersion;
use super::file_system::FileSystem;
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
    FuseInitIn, FuseInitOut, FuseSetXAttrIn, FATTR_ATIME, FATTR_FH, FATTR_GID, FATTR_MODE,
    FATTR_MTIME, FATTR_SIZE, FATTR_UID, FUSE_ASYNC_READ, FUSE_KERNEL_MINOR_VERSION,
    FUSE_KERNEL_VERSION, FUSE_RELEASE_FLUSH,
};
use crate::async_fuse::fuse::de::DeserializeError;
use crate::async_fuse::memfs::{
    CreateParam, FileLockParam, MemFs, MetaData, RenameParam, SetAttrParam,
};

/// We generally support async reads
#[cfg(target_os = "linux")]
const INIT_FLAGS: u32 = FUSE_ASYNC_READ;
// TODO: Add FUSE_EXPORT_SUPPORT and FUSE_BIG_WRITES (requires ABI 7.10)

/// The max size of write requests from the kernel. The absolute minimum is 4k,
/// FUSE recommends at least 128k, max 16M. The FUSE default is  128k on Linux.
#[cfg(target_os = "linux")]
const MAX_WRITE_SIZE: u32 = 128 * 1024;

/// Size of the buffer for reading a request from the kernel. Since the kernel
/// may send up to `MAX_WRITE_SIZE` bytes in a write request, we use that value
/// plus some extra space.
const BUFFER_SIZE: u32 = MAX_WRITE_SIZE + 512;

/// We use `PAGE_SIZE` (4 KiB) as the alignment of the buffer.
const PAGE_SIZE: usize = 4096;
/// Max background pending requests under processing, at least to be 4,
/// otherwise deadlock
const MAX_BACKGROUND: u16 = 10; // TODO: set to larger value when release
/// The max number of FUSE device reader threads.
const MAX_FUSE_READER: usize = 2; // TODO: make it custom

/// The implementation of fuse fd clone.
/// This module is just for avoiding the `missing_docs` of `ioctl_read` macro.
#[allow(missing_docs)] // Raised by `ioctl_read!`
mod _fuse_fd_clone {
    use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};

    use clippy_utilities::Cast;
    use nix::fcntl::{self, FcntlArg, FdFlag, OFlag};
    use nix::ioctl_read;
    use nix::sys::stat::Mode;
    ioctl_read!(fuse_fd_clone_impl, 229, 0, u32);

    /// Clones a FUSE session fd into a FUSE worker fd.
    ///
    /// # Safety
    /// Behavior is undefined if any of the following conditions are violated:
    ///
    /// - `session_fd` must be a valid file descriptor to an open FUSE device.
    #[allow(clippy::unnecessary_safety_comment)]
    pub unsafe fn fuse_fd_clone(session_fd: RawFd) -> nix::Result<RawFd> {
        let devname = "/dev/fuse";
        let cloned_fd = fcntl::open(devname, OFlag::O_RDWR | OFlag::O_CLOEXEC, Mode::empty())?;
        // use `OwnedFd` here is just to release the fd when error occurs
        // SAFETY: the `cloned_fd` is just opened
        let cloned_fd = OwnedFd::from_raw_fd(cloned_fd);

        fcntl::fcntl(cloned_fd.as_raw_fd(), FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC))?;

        let mut result_fd: u32 = session_fd.cast();
        // SAFETY: `cloned_fd` is ensured to be valid, and `&mut result_fd` is a valid
        // pointer to a value on stack
        fuse_fd_clone_impl(cloned_fd.as_raw_fd(), &mut result_fd)?;
        Ok(cloned_fd.into_raw_fd()) // use `into_raw_fd` to transfer the
                                    // ownership of the fd
    }
}

use _fuse_fd_clone::fuse_fd_clone;

/// A loop to read requests from FUSE device continuously
#[allow(clippy::needless_pass_by_value)]
fn fuse_device_reader(
    buffer_tx: Sender<(File, AlignedBytes)>,
    buffer_rx: Receiver<(File, AlignedBytes)>,
    fuse_request_spawn_handle: GcHandle,
    runtime_handle: Handle,
    proto_version: ProtoVersion,
    fs: Arc<dyn FileSystem + Send + Sync>,
) {
    loop {
        let Ok((mut file, mut buffer)) = buffer_rx.recv() else {
            error!("The channel of buffer is exhausted and closed.");
            return;
        };

        let size = match file.read(&mut buffer) {
            Ok(size) => size,
            Err(e) => {
                let errno = e.raw_os_error().map(Errno::from_raw);
                match errno {
                    None => {
                        panic!("Failed to get the errno on reading failure. The error is {e}");
                    }
                    // Operation interrupted. Accordingly to FUSE, this is safe to retry
                    Some(Errno::ENOENT) => {
                        info!("operation interrupted, retry.");
                        buffer_tx.send((file, buffer)).unwrap_or_else(|_| {
                            unreachable!("The buffer channel is not to be closed.")
                        });
                        continue;
                    }
                    // Interrupted system call, retry
                    Some(Errno::EINTR) => {
                        info!("interrupted system call, retry");
                        buffer_tx.send((file, buffer)).unwrap_or_else(|_| {
                            unreachable!("The buffer channel is not to be closed.")
                        });
                        continue;
                    }
                    // Explicitly try again
                    Some(Errno::EAGAIN) => {
                        info!("Explicitly retry");
                        buffer_tx.send((file, buffer)).unwrap_or_else(|_| {
                            unreachable!("The buffer channel is not to be closed.")
                        });
                        continue;
                    }
                    // Filesystem was unmounted, quit the loop
                    Some(Errno::ENODEV) => {
                        info!("filesystem destroyed, quit the run loop");
                        return;
                    }
                    // Unhandled error
                    Some(errno) => {
                        panic!(
                            "non-recoverable io error when read FUSE device, \
                            the error is: {errno}",
                        );
                    }
                }
            }
        };

        let spawn_result = runtime_handle.block_on(fuse_request_spawn_handle.spawn(|_| {
            process_fuse_request(
                buffer,
                size,
                file,
                Arc::clone(&fs),
                buffer_tx.clone(),
                proto_version,
            )
        }));
        if spawn_result.is_err() {
            info!("Try to spawn task of `FuseRequest` after shutdow.");
            return;
        }
    }
}

/// Process one FUSE request
async fn process_fuse_request(
    byte_buffer: AlignedBytes,
    read_size: usize,
    mut file: File,
    fs: Arc<dyn FileSystem + Send + Sync + 'static>,
    sender: Sender<(File, AlignedBytes)>,
    proto_version: ProtoVersion,
) {
    let bytes = byte_buffer
        .get(..read_size)
        .unwrap_or_else(|| panic!("failed to read {read_size} bytes from the buffer",));
    let fuse_req = match Request::new(bytes, proto_version) {
        // Dispatch request
        Ok(r) => r,
        // Quit on illegal request
        Err(e) => {
            if let &DeserializeError::UnknownOpCode { code, unique } = &e {
                error!("Unknown operation code found: {code}, with context: {e}");
                let unique = unique.unwrap_or_else(|| {
                    unreachable!("A `unique` must be filled in by deserializer.")
                });
                ReplyEmpty::new(unique, &mut file)
                    .error_code(Errno::ENOSYS)
                    .await
                    .unwrap_or_else(|reply_err| {
                        panic!("Failed to reply an error code: {reply_err}.")
                    });
                sender.send((file, byte_buffer)).unwrap_or_else(|_| {
                    error!("The buffer pool is closed.");
                });
                return;
            }

            // TODO: graceful handle request build failure
            panic!("failed to build FUSE request, the error is: {e}");
        }
    };
    debug!("received FUSE req={}", fuse_req);
    let res = dispatch(&fuse_req, &mut file, fs).await;
    if let Err(e) = res {
        panic!(
            "failed to process req={:?}, the error is: {}",
            fuse_req,
            crate::async_fuse::util::format_nix_error(e), // TODO: refactor format_nix_error()
        );
    }
    let res = sender.send((file, byte_buffer));
    if let Err(e) = res {
        panic!("failed to put the buffer back to buffer pool, the error is: {e}",);
    }
}

/// FUSE session
#[allow(missing_debug_implementations)]
pub struct Session<F: FileSystem + Send + Sync + 'static> {
    /// FUSE device fd
    fuse_fd: Arc<FuseFd>,
    /// Kernel FUSE protocol version
    proto_version: AtomicCell<ProtoVersion>,
    /// Mount path (relative)
    mount_path: PathBuf,
    /// The underlying FUSE file system
    filesystem: Arc<F>,
    /// A handle to spawn FUSE Resuest tasks
    fuse_request_spawn_handle: GcHandle,
}

/// FUSE device fd
#[derive(Debug)]
struct FuseFd(RawFd);

impl Drop for FuseFd {
    fn drop(&mut self) {
        unistd::close(self.0).ok();
    }
}

impl<F: FileSystem + Send + Sync + 'static> Drop for Session<F> {
    fn drop(&mut self) {
        futures::executor::block_on(async {
            let mount_path = &self.mount_path;
            let res = mount::umount(mount_path).await;
            match res {
                Ok(..) => info!("Session::drop() successfully umount {:?}", mount_path),
                Err(e) => error!(
                    "Session::drop() failed to umount {:?}, the error is: {}",
                    mount_path, e,
                ),
            };
        });
    }
}

/// Create FUSE session
#[allow(clippy::clone_on_ref_ptr)] // allow this clone to transform trait to sub-trait
pub async fn new_session_of_memfs<M>(
    mount_path: &Path,
    fs: MemFs<M>,
) -> anyhow::Result<Session<MemFs<M>>>
where
    M: MetaData + Send + Sync + 'static,
{
    // let mount_path = Path::new(mount_point);
    assert!(
        mount_path.is_dir(),
        "the input mount path={mount_path:?} is not a directory"
    );

    // Must create filesystem before mount
    let fuse_fd = mount::mount(mount_path)
        .await
        .context("failed to mount fuse device")?;

    let fuse_request_spawn_handle = TASK_MANAGER
        .get_gc_handle(TaskName::FuseRequest)
        .await
        .unwrap_or_else(|| unreachable!("`FuseRequest` must be GC task."));

    let fsarc = Arc::new(fs);
    Ok(Session {
        fuse_fd: Arc::new(FuseFd(fuse_fd)),
        proto_version: AtomicCell::new(ProtoVersion::UNSPECIFIED),
        mount_path: mount_path.to_owned(),
        fuse_request_spawn_handle,
        filesystem: fsarc,
    })
}

impl<F: FileSystem + Send + Sync + 'static> Session<F> {
    /// Get FUSE device fd
    #[inline]
    pub fn dev_fd(&self) -> RawFd {
        self.fuse_fd.0
    }

    /// Run the FUSE session
    #[allow(clippy::arithmetic_side_effects, clippy::pattern_type_mismatch)] // The `select!` macro will generate code that goes against these rules.
    pub async fn run(self, token: CancellationToken) -> anyhow::Result<()> {
        // For recycling the buffers used by process_fuse_request.
        let (pool_sender, pool_receiver) = self
            .setup_buffer_pool()
            .await
            .context("failed to setup buffer pool")?;

        for _ in 0..MAX_FUSE_READER {
            let pool_tx = pool_sender.clone();
            let pool_rx = pool_receiver.clone();
            let gc_handle = self.fuse_request_spawn_handle.clone();
            let handle = Handle::current();
            let fs = Arc::clone(&self.filesystem);
            let protocol_version = self.proto_version.load();
            // The `JoinHandle` is ignored
            thread::spawn(move || {
                fuse_device_reader(pool_tx, pool_rx, gc_handle, handle, protocol_version, fs);
            });
        }

        drop(pool_receiver);

        token.cancelled().await;
        info!("Async FUSE session exits.");

        Ok(())
    }

    /// Setup buffer pool
    async fn setup_buffer_pool(
        &self,
    ) -> anyhow::Result<(Sender<(File, AlignedBytes)>, Receiver<(File, AlignedBytes)>)> {
        let (pool_sender, pool_receiver) =
            crossbeam_channel::bounded::<(File, AlignedBytes)>(MAX_BACKGROUND.into());

        for _ in 0..MAX_BACKGROUND {
            let buf = AlignedBytes::new_zeroed(BUFFER_SIZE.cast(), PAGE_SIZE);
            let session_fd = self.dev_fd();

            let file = unsafe {
                // SAFETY: We assume that the fuse session fd is valid.
                let worker_fd = fuse_fd_clone(session_fd)?;
                // SAFETY: The worker fd is just cloned.
                File::from_raw_fd(worker_fd)
            };

            let res = pool_sender.send((file, buf));
            if let Err(e) = res {
                panic!(
                    "failed to insert buffer to buffer pool when initializing, the error is: {e}",
                );
            }
        }

        let (mut file, mut byte_buf) = pool_receiver.recv()?;
        let (read_result, mut file, byte_buf) = tokio::task::spawn_blocking(move || {
            let res = file.read(&mut byte_buf);
            (res, file, byte_buf)
        })
        .await?;
        if let Ok(read_size) = read_result {
            debug!("read successfully {} byte data from FUSE device", read_size);
            let bytes = byte_buf.get(..read_size).unwrap_or_else(|| {
                panic!(
                    "read_size is greater than buffer size: read_size = {}, buffer size = {}",
                    read_size,
                    byte_buf.len()
                )
            });
            if let Ok(req) = Request::new(bytes, self.proto_version.load()) {
                if let Operation::Init { arg } = *req.operation() {
                    let filesystem = Arc::clone(&self.filesystem);
                    self.init(arg, &req, &*filesystem, &mut file).await?;
                }
            }
        }
        pool_sender
            .send((file, byte_buf))
            .context("failed to put buffer back to buffer pool after FUSE init")?;

        Ok((pool_sender, pool_receiver))
    }

    /// Initialize FUSE session
    #[allow(single_use_lifetimes)] // false positive
    async fn init<'a>(
        &self,
        arg: &'_ FuseInitIn,
        req: &'_ Request<'a>,
        fs: &'_ (dyn FileSystem + Send + Sync + 'static),
        file: &mut File,
    ) -> anyhow::Result<()> {
        debug!("Init args={:?}", arg);
        // TODO: rewrite init based on do_init() in fuse_lowlevel.c
        // https://github.com/libfuse/libfuse/blob/master/lib/fuse_lowlevel.c#L1892
        let reply = ReplyInit::new(req.unique(), file);
        // We don't support ABI versions before 7.8
        if arg.major < 7 || (arg.major == 7 && arg.minor < 8) {
            error!("Unsupported FUSE ABI version={}.{}", arg.major, arg.minor);
            reply.error_code(Errno::EPROTO).await?;
            return Err(anyhow!("FUSE ABI version too low"));
        }
        // Call filesystem init method and give it a chance to return an error
        let filesystem = fs;
        let init_res = filesystem.init(req).await;
        if let Err(err) = init_res {
            reply.error_code(Errno::ENOSYS).await?;
            return Err(anyhow!("user defined init failed, the error is: {}", err,));
        }
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
            .init(FuseInitOut {
                major: FUSE_KERNEL_VERSION,
                minor: FUSE_KERNEL_MINOR_VERSION, /* Do not change minor version, otherwise
                                                   * unknown panic */
                max_readahead: arg.max_readahead, // accept FUSE kernel module max_readahead
                flags,                            /* TODO: use features given in INIT_FLAGS and
                                                   * reported as capable */
                #[cfg(not(feature = "abi-7-13"))]
                unused,
                #[cfg(feature = "abi-7-13")]
                max_background: MAX_BACKGROUND,
                #[cfg(feature = "abi-7-13")]
                congestion_threshold,
                max_write: MAX_WRITE_SIZE,
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
            })
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
        self.proto_version.store(ProtoVersion {
            major: arg.major,
            minor: arg.minor,
        });

        Ok(())
    }
}

/// Dispatch request to the filesystem
/// This calls the appropriate filesystem operation method for the
/// request and sends back the returned reply to the kernel
#[allow(clippy::too_many_lines)]
#[instrument(name="request",skip(req, file, fs), fields(fuse_id =req.unique(),ino=req.nodeid(), op=%req.operation()),ret)]
async fn dispatch<'a>(
    req: &'a Request<'a>,
    file: &mut File,
    fs: Arc<dyn FileSystem + Send + Sync + 'static>,
) -> nix::Result<usize> {
    let result = match *req.operation() {
        // Filesystem initialization
        Operation::Init { .. } => panic!("FUSE should have already initialized"),

        // Filesystem destroyed
        Operation::Destroy => {
            fs.destroy(req).await;
            let reply = ReplyEmpty::new(req.unique(), file);
            reply.ok().await
        }

        Operation::Interrupt { arg } => {
            fs.interrupt(req, arg.unique).await; // No reply
            Ok(0)
        }

        Operation::Lookup { name } => {
            let reply = ReplyEntry::new(req.unique(), file);
            fs.lookup(req, req.nodeid(), name, reply).await
        }
        Operation::Forget { arg } => {
            fs.forget(req, arg.nlookup).await; // No reply
            Ok(0)
        }
        Operation::GetAttr => {
            let reply = ReplyAttr::new(req.unique(), file);
            fs.getattr(req, reply).await
        }
        Operation::SetAttr { arg } => {
            #[cfg(feature = "abi-7-9")]
            use std::time::SystemTime;

            #[cfg(feature = "abi-7-9")]
            use super::protocol::{FATTR_ATIME_NOW, FATTR_MTIME_NOW};

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

            #[cfg(feature = "abi-7-9")]
            let a_time = match arg.valid & FATTR_ATIME_NOW {
                0 => a_time,
                _ => Some(SystemTime::now()),
            };
            #[cfg(feature = "abi-7-9")]
            let m_time = match arg.valid & FATTR_MTIME_NOW {
                0 => m_time,
                _ => Some(SystemTime::now()),
            };

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

            let reply = ReplyAttr::new(req.unique(), file);
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
            };
            fs.setattr(req, param, reply).await
        }
        Operation::ReadLink => {
            let reply = ReplyData::new(req.unique(), file);
            fs.readlink(req, reply).await
        }
        Operation::MkNod { arg, name } => {
            let param = CreateParam {
                parent: req.nodeid(),
                name: name.to_owned(),
                mode: arg.mode,
                rdev: arg.rdev,
                uid: req.uid(),
                gid: req.gid(),
                node_type: SFlag::S_IFREG,
                link: None,
            };
            let reply = ReplyEntry::new(req.unique(), file);
            fs.mknod(req, param, reply).await
        }
        Operation::MkDir { arg, name } => {
            let reply = ReplyEntry::new(req.unique(), file);
            fs.mkdir(req, req.nodeid(), name, arg.mode, reply).await
        }
        Operation::Unlink { name } => {
            let reply = ReplyEmpty::new(req.unique(), file);
            fs.unlink(req, req.nodeid(), name, reply).await
        }
        Operation::RmDir { name } => {
            let reply = ReplyEmpty::new(req.unique(), file);
            fs.rmdir(req, req.nodeid(), name, reply).await
        }
        Operation::SymLink { name, link } => {
            let reply = ReplyEntry::new(req.unique(), file);
            fs.symlink(req, req.nodeid(), name, Path::new(link), reply)
                .await
        }
        Operation::Rename {
            arg,
            oldname,
            newname,
        } => {
            let reply = ReplyEmpty::new(req.unique(), file);
            let param = RenameParam {
                old_parent: req.nodeid(),
                old_name: oldname.to_owned(),
                new_parent: arg.newdir,
                new_name: newname.to_owned(),
                flags: 0,
            };
            fs.rename(req, param, reply).await
        }
        Operation::Link { arg, name } => {
            let reply = ReplyEntry::new(req.unique(), file);
            fs.link(req, arg.oldnodeid, name, reply).await
        }
        Operation::Open { arg } => {
            let reply = ReplyOpen::new(req.unique(), file);
            fs.open(req, arg.flags, reply).await
        }
        Operation::Read { arg } => {
            let reply = ReplyData::new(req.unique(), file);
            fs.read(req, arg.fh, arg.offset.cast(), arg.size, reply)
                .await
        }
        Operation::Write { arg, data } => {
            assert_eq!(data.len(), arg.size.cast::<usize>());
            let reply = ReplyWrite::new(req.unique(), file);
            fs.write(
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
            let reply = ReplyEmpty::new(req.unique(), file);
            fs.flush(req, arg.fh, arg.lock_owner, reply).await
        }
        Operation::Release { arg } => {
            let flush = !matches!(arg.release_flags & FUSE_RELEASE_FLUSH, 0);
            let reply = ReplyEmpty::new(req.unique(), file);
            fs.release(req, arg.fh, arg.flags, arg.lock_owner, flush, reply)
                .await
        }
        Operation::FSync { arg } => {
            let datasync = !matches!(arg.fsync_flags & 1, 0);
            let reply = ReplyEmpty::new(req.unique(), file);
            fs.fsync(req, arg.fh, datasync, reply).await
        }
        Operation::OpenDir { arg } => {
            let reply = ReplyOpen::new(req.unique(), file);
            fs.opendir(req, arg.flags, reply).await
        }
        Operation::ReadDir { arg } => {
            let reply = ReplyDirectory::new(req.unique(), file, arg.size.cast());
            fs.readdir(req, arg.fh, arg.offset.cast(), reply).await
        }
        Operation::ReleaseDir { arg } => {
            let reply = ReplyEmpty::new(req.unique(), file);
            fs.releasedir(req, arg.fh, arg.flags, reply).await
        }
        Operation::FSyncDir { arg } => {
            let datasync = !matches!(arg.fsync_flags & 1, 0);
            let reply = ReplyEmpty::new(req.unique(), file);
            fs.fsyncdir(req, arg.fh, datasync, reply).await
        }
        Operation::StatFs => {
            let reply = ReplyStatFs::new(req.unique(), file);
            fs.statfs(req, reply).await
        }
        Operation::SetXAttr { arg, name, value } => {
            /// Set the position of an extended attribute
            /// zero for Linux
            #[cfg(target_os = "linux")]
            #[inline]
            const fn get_position(_arg: &FuseSetXAttrIn) -> u32 {
                0
            }
            assert!(value.len() == arg.size.cast::<usize>());
            let reply = ReplyEmpty::new(req.unique(), file);
            fs.setxattr(req, name, value, arg.flags, get_position(arg), reply)
                .await
        }
        Operation::GetXAttr { arg, name } => {
            let reply = ReplyXAttr::new(req.unique(), file);
            fs.getxattr(req, name, arg.size, reply).await
        }
        Operation::ListXAttr { arg } => {
            let reply = ReplyXAttr::new(req.unique(), file);
            fs.listxattr(req, arg.size, reply).await
        }
        Operation::RemoveXAttr { name } => {
            let reply = ReplyEmpty::new(req.unique(), file);
            fs.removexattr(req, name, reply).await
        }
        Operation::Access { arg } => {
            let reply = ReplyEmpty::new(req.unique(), file);
            fs.access(req, arg.mask, reply).await
        }
        Operation::Create { arg, name } => {
            let reply = ReplyCreate::new(req.unique(), file);
            fs.create(req, req.nodeid(), name, arg.mode, arg.flags, reply)
                .await
        }
        Operation::GetLk { arg } => {
            let reply = ReplyLock::new(req.unique(), file);
            let lock_param = FileLockParam {
                fh: arg.fh,
                lock_owner: arg.owner,
                start: arg.lk.start,
                end: arg.lk.end,
                typ: arg.lk.typ,
                pid: arg.lk.pid,
            };
            fs.getlk(req, lock_param, reply).await
        }
        Operation::SetLk { arg } => {
            let reply = ReplyEmpty::new(req.unique(), file);
            let lock_param = FileLockParam {
                fh: arg.fh,
                lock_owner: arg.owner,
                start: arg.lk.start,
                end: arg.lk.end,
                typ: arg.lk.typ,
                pid: arg.lk.pid,
            };
            fs.setlk(req, lock_param, false, reply).await
        }
        Operation::SetLkW { arg } => {
            let reply = ReplyEmpty::new(req.unique(), file);
            let lock_param = FileLockParam {
                fh: arg.fh,
                lock_owner: arg.owner,
                start: arg.lk.start,
                end: arg.lk.end,
                typ: arg.lk.typ,
                pid: arg.lk.pid,
            };
            fs.setlk(
                req, lock_param, true, // sleep
                reply,
            )
            .await
        }
        Operation::BMap { arg } => {
            let reply = ReplyBMap::new(req.unique(), file);
            fs.bmap(req, arg.blocksize, arg.block, reply).await
        }

        #[cfg(feature = "abi-7-11")]
        Operation::IoCtl { arg, data } => {
            error!("IoCtl not implemented, arg={:?}, data={:?}", arg, data);
            not_implement_helper(req, file).await
        }
        #[cfg(feature = "abi-7-11")]
        Operation::Poll { arg } => {
            error!("Poll not implemented, arg={:?}", arg);
            not_implement_helper(req, file).await
        }
        #[cfg(feature = "abi-7-15")]
        Operation::NotifyReply { data } => {
            error!("NotifyReply not implemented, data={:?}", data);
            not_implement_helper(req, file).await
        }
        #[cfg(feature = "abi-7-16")]
        Operation::BatchForget { arg, nodes } => {
            error!(
                "BatchForget not implemented, arg={:?}, nodes={:?}",
                arg, nodes
            );
            not_implement_helper(req, file).await
        }
        #[cfg(feature = "abi-7-19")]
        Operation::FAllocate { arg } => {
            error!("FAllocate not implemented, arg={:?}", arg);
            not_implement_helper(req, file).await
        }
        #[cfg(feature = "abi-7-21")]
        Operation::ReadDirPlus { arg } => {
            error!("ReadDirPlus not implemented, arg={:?}", arg);
            not_implement_helper(req, file).await
        }
        #[cfg(feature = "abi-7-23")]
        Operation::Rename2 {
            arg,
            oldname,
            newname,
        } => {
            let reply = ReplyEmpty::new(req.unique(), file);
            let param = RenameParam {
                old_parent: req.nodeid(),
                old_name: oldname.to_owned(),
                new_parent: arg.newdir,
                new_name: newname.to_owned(),
                flags: arg.flags,
            };
            fs.rename(req, param, reply).await
        }
        // #[cfg(feature = "abi-7-24")]
        Operation::LSeek { arg } => {
            error!("LSeek not implemented, arg={:?}", arg);
            not_implement_helper(req, file).await
        }
        // #[cfg(feature = "abi-7-28")]
        Operation::CopyFileRange { arg } => {
            error!("ReadDirPlusCopyFileRange not implemented, arg={:?}", arg);
            not_implement_helper(req, file).await
        }
        #[cfg(feature = "abi-7-11")]
        Operation::CuseInit { arg } => {
            panic!("unsupported CuseInit arg={arg:?}");
        }
    };

    result
}

/// Replies ENOSYS
async fn not_implement_helper(req: &Request<'_>, file: &mut File) -> nix::Result<usize> {
    let reply = ReplyEmpty::new(req.unique(), file);
    reply.error_code(Errno::ENOSYS).await
}
