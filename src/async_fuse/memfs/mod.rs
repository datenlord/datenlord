//! The implementation of user space file system
mod fs_util;
pub mod id_alloc;
mod id_alloc_used;
/// The KV engine module
#[macro_use]
pub mod kv_engine;
/// Dir entry module
pub mod direntry;
/// fs metadata module
mod metadata;
mod node;
/// Opened files
mod open_file;
/// fs metadata with S3 backend module
mod s3_metadata;
mod s3_node;

/// Serializable types module
pub mod serial;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use clippy_utilities::{Cast, OverflowArithmetic};
use datenlord::config::StorageConfig;
use datenlord::metrics::FILESYSTEM_METRICS;
pub use metadata::MetaData;
use nix::errno::Errno;
use nix::sys::stat::SFlag;
pub use s3_metadata::S3MetaData;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, instrument, warn};

use self::kv_engine::KVEngineType;
use crate::async_fuse::fuse::file_system::FileSystem;
use crate::async_fuse::fuse::fuse_reply::{
    ReplyAttr, ReplyBMap, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyLock, ReplyOpen, ReplyStatFs, ReplyWrite, ReplyXAttr,
};
use crate::async_fuse::fuse::fuse_request::Request;
use crate::async_fuse::fuse::protocol::{INum, FUSE_ROOT_ID};
use crate::async_fuse::memfs::metadata::ReqContext;
use crate::async_fuse::util::build_error_result_from_errno;
use crate::common::error::{Context, DatenLordResult};
use crate::new_storage::{Storage, StorageManager};

/// The type of storage
pub type StorageType = StorageManager;

/// In-memory file system
#[derive(Debug)]
pub struct MemFs<M: MetaData + Send + Sync + 'static> {
    /// Fs metadata
    metadata: Arc<M>,
    /// Storage manager
    storage: StorageType,
}

/// Set attribute parameters
#[derive(Debug)]
pub struct SetAttrParam {
    /// FUSE set attribute bit mask
    pub valid: u32,
    /// File handler
    pub fh: Option<u64>,
    /// File mode
    pub mode: Option<u32>,
    /// User ID
    pub u_id: Option<u32>,
    /// Group ID
    pub g_id: Option<u32>,
    /// File size
    pub size: Option<u64>,
    /// Lock owner
    #[cfg(feature = "abi-7-9")]
    pub lock_owner: Option<u64>,
    /// Access time
    pub a_time: Option<SystemTime>,
    /// Content modified time
    pub m_time: Option<SystemTime>,
    /// Meta-data changed time seconds
    #[cfg(feature = "abi-7-23")]
    pub c_time: Option<SystemTime>,
}

/// Create parameters
#[derive(Debug)]
pub struct CreateParam {
    /// Parent directory i-number
    pub parent: INum,
    /// File name
    pub name: String,
    /// File mode
    pub mode: u32,
    /// File flags
    pub rdev: u32,
    /// User ID
    pub uid: u32,
    /// Group ID
    pub gid: u32,
    /// Type
    pub node_type: SFlag,
    /// For symlink
    pub link: Option<PathBuf>,
}

/// Rename parameters
#[derive(Serialize, Deserialize, Debug)]
pub struct RenameParam {
    /// Old parent directory i-number
    pub old_parent: INum,
    /// Old name
    pub old_name: String,
    /// New parent directory i-number
    pub new_parent: INum,
    /// New name
    pub new_name: String,
    /// Rename flags
    pub flags: u32,
}

/// POSIX file lock parameters
#[derive(Debug)]
pub struct FileLockParam {
    /// File handler
    pub fh: u64,
    /// Lock owner
    pub lock_owner: u64,
    /// Start offset
    pub start: u64,
    /// End offset
    pub end: u64,
    /// Lock type
    pub typ: u32,
    /// The process ID of the lock
    pub pid: u32,
}

/// MAX NAME LEN
const MAX_NAME_LEN: usize = 255;

/// Check the name length is valid or not
pub fn check_name_length(name: &str) -> DatenLordResult<()> {
    if name.len() > MAX_NAME_LEN {
        error!("name = {} is too long ,size = {}", name, name.len());
        build_error_result_from_errno(Errno::ENAMETOOLONG, "name too long ".to_owned())
    } else {
        Ok(())
    }
}

/// Check if `file_type` is supported. We only support
/// 1. Regular file
/// 2. Directory
/// 3. Symbolic link
pub fn check_type_supported(file_type: &SFlag) -> DatenLordResult<()> {
    match *file_type {
        SFlag::S_IFREG | SFlag::S_IFDIR | SFlag::S_IFLNK => Ok(()),
        _ => {
            error!("type = {:?} is not supported", file_type);
            build_error_result_from_errno(Errno::ENOTSUP, "type not supported".to_owned())
        }
    }
}

impl<M: MetaData + Send + Sync + 'static> MemFs<M> {
    /// Create `FileSystem`
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        mount_point: &str,
        capacity: usize,
        kv_engine: Arc<KVEngineType>,
        node_id: &str,
        storage_config: &StorageConfig,
        storage: StorageType,
    ) -> anyhow::Result<Self> {
        info!(
            "mount_point: ${}$, capacity: ${}$, node_id: {}, storage_config: {:?}",
            mount_point, capacity, node_id, storage_config
        );
        let metadata = M::new(kv_engine, node_id).await?;
        Ok(Self { metadata, storage })
    }
}

#[async_trait]
impl<M: MetaData + Send + Sync + 'static> FileSystem for MemFs<M> {
    // Implemented FUSE operations

    /// Initialize filesystem.
    /// Called before any other filesystem method.
    async fn init(&self, req: &Request<'_>) -> nix::Result<()> {
        debug!("init(req={:?}), cache size={}", req, 0_i32);
        Ok(())
    }

    /// Clean up filesystem.
    /// Called on filesystem exit.
    async fn destroy(&self, req: &Request<'_>) {
        debug!("destroy(req={:?}), cache size={}", req, 0_i32);
    }

    /// Look up a directory entry by name and get its attributes.
    async fn lookup(
        &self,
        req: &Request<'_>,
        parent: INum,
        name: &str,
        reply: ReplyEntry<'_>,
    ) -> nix::Result<usize> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("lookup");
        debug!("lookup(parent={}, name={:?}, req={:?})", parent, name, req,);
        // check the dir_name is valid
        if let Err(e) = check_name_length(name) {
            return reply.error(e).await;
        }
        let context = ReqContext {
            uid: req.uid(),
            gid: req.gid(),
        };
        let lookup_res = self.metadata.lookup_helper(context, parent, name).await;
        match lookup_res {
            Ok((ttl, fuse_attr, generation)) => reply.entry(ttl, fuse_attr, generation).await,
            Err(e) => reply.error(e).await,
        }
    }

    /// Get file attributes.
    async fn getattr(&self, req: &Request<'_>, reply: ReplyAttr<'_>) -> nix::Result<usize> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("getattr");
        let ino = req.nodeid();
        debug!("getattr(ino={}, req={:?})", ino, req);
        match self.metadata.getattr(ino).await {
            Ok((ttl, fuse_attr)) => {
                debug!(
                    "getattr() successfully got the attr={:?} of ino={}",
                    fuse_attr, ino,
                );
                reply.attr(ttl, fuse_attr).await
            }
            Err(err) => {
                // In the previous version ,this panic will never happen.
                // Later, we will
                panic!("getattr() failed to get the attr of ino={ino}, the error is: {err}",);
            }
        }
    }

    /// Open a file.
    /// Open flags (with the exception of `O_CREAT`, `O_EXCL`, `O_NOCTTY` and
    /// `O_TRUNC`) are available in flags. Filesystem may store an arbitrary
    /// file handle (pointer, index, etc) in fh, and use self in other all
    /// other file operations (read, write, flush, release, fsync).
    /// Filesystem may also implement stateless file I/O and not store
    /// anything in fh. There are also some flags (`direct_io`, `keep_cache`)
    /// which the filesystem may set, to change the way the file is opened.
    /// See `fuse_file_info` structure in `fuse_common.h` for more details.
    async fn open(
        &self,
        req: &Request<'_>,
        flags: u32,
        reply: ReplyOpen<'_>,
    ) -> nix::Result<usize> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("open");
        let ino = req.nodeid();
        debug!("open(ino={}, flags={}, req={:?})", ino, flags, req);

        let context = ReqContext {
            uid: req.uid(),
            gid: req.gid(),
        };

        match self.metadata.open(context, ino, flags).await {
            Ok(fd) => {
                self.storage.open(ino, fd.cast(), flags.into());
                reply.opened(fd.cast(), flags).await // TODO: Fix the type
                                                     // of fd
            }
            Err(e) => {
                debug!("open() failed, the error is: {:?}", e);
                reply.error(e).await
            }
        }
    }

    /// Forget about an inode.
    /// The nlookup parameter indicates the number of lookups previously
    /// performed on self inode. If the filesystem implements inode
    /// lifetimes, it is recommended that inodes acquire a single reference
    /// on each lookup, and lose nlookup references on each forget. The
    /// filesystem may ignore forget calls, if the inodes don't need to have
    /// a limited lifetime. On unmount it is not guaranteed, that all referenced
    /// inodes will receive a forget message.
    #[instrument(skip(self))]
    async fn forget(&self, req: &Request<'_>, nlookup: u64) {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("forget");
        let ino = req.nodeid();
        let deleted = self
            .metadata
            .forget(ino, nlookup)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        if deleted {
            self.storage
                .remove(ino)
                .await
                .unwrap_or_else(|e| panic!("{e}"));
        }
    }

    /// Set file attributes.
    async fn setattr(
        &self,
        req: &Request<'_>,
        param: SetAttrParam,
        reply: ReplyAttr<'_>,
    ) -> nix::Result<usize> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("setattr");
        let ino = req.nodeid();
        let valid = param.valid;

        #[cfg(feature = "abi-7-9")]
        let _lock_owner = param.lock_owner;
        #[cfg(feature = "abi-7-23")]
        let _c_time = param.c_time;
        if 0 == valid {
            warn!("setattr() encountered valid=0, the req={:?}", req);
        };
        let context = ReqContext {
            uid: req.uid(),
            gid: req.gid(),
        };
        let set_res = self
            .metadata
            .setattr_helper(context, ino, &param, &self.storage)
            .await;
        match set_res {
            Ok((ttl, fuse_attr)) => reply.attr(ttl, fuse_attr).await,
            Err(e) => reply.error(e).await,
        }
    }

    /// Create file node.
    /// Create a regular file, character device, block device, fifo or socket
    /// node.
    async fn mknod(
        &self,
        req: &Request<'_>,
        param: CreateParam,
        reply: ReplyEntry<'_>,
    ) -> nix::Result<usize> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("mknod");
        debug!("mknod param = {:?}, req = {:?}", param, req);
        let mknod_res = self.metadata.mknod(param).await;
        match mknod_res {
            Ok((ttl, fuse_attr, generation)) => reply.entry(ttl, fuse_attr, generation).await,
            Err(e) => {
                info!("mknod() failed , the error is: {:?}", e);
                reply.error(e).await
            }
        }
    }

    /// Create a directory.
    async fn mkdir(
        &self,
        req: &Request<'_>,
        parent: INum,
        name: &str,
        mode: u32,
        reply: ReplyEntry<'_>,
    ) -> nix::Result<usize> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("mkdir");
        debug!(
            "mkdir(parent={}, name={:?}, mode={}, req={:?})",
            parent, name, mode, req,
        );
        let param = CreateParam {
            parent,
            name: name.to_owned(),
            mode,
            rdev: 0,
            uid: req.uid(),
            gid: req.gid(),
            node_type: SFlag::S_IFDIR,
            link: None,
        };
        let mkdir_res = self
            .metadata
            .mknod(param)
            .await
            .add_context(format!(
                "mkdir() failed to create a directory name={name:?} and mode={mode:?} under parent ino={parent}",
            ));
        match mkdir_res {
            Ok((ttl, fuse_attr, generation)) => reply.entry(ttl, fuse_attr, generation).await,
            Err(e) => {
                debug!(
                    "mkdir() failed to create a directory name={:?} and mode={:?} under parent ino={}, \
                        the error is: {:?}",
                    name,
                    mode,
                    parent,
                    e,
                );
                reply.error(e).await
            }
        }
    }

    /// Remove a file.
    #[instrument(skip(self), err, ret)]
    async fn unlink(
        &self,
        req: &Request<'_>,
        parent: INum,
        name: &str,
        reply: ReplyEmpty<'_>,
    ) -> nix::Result<usize> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("unlink");
        debug!("unlink(parent={}, name={:?}, req={:?}", parent, name, req,);
        // check the dir_name is valid
        if let Err(e) = check_name_length(name) {
            return reply.error(e).await;
        }

        let context = ReqContext {
            uid: req.uid(),
            gid: req.gid(),
        };

        match self.metadata.unlink(context, parent, name).await {
            Ok(result) => {
                if let Some(ino) = result {
                    match self.storage.remove(ino).await {
                        Ok(()) => {}
                        Err(e) => {
                            return reply.error(e.into()).await;
                        }
                    }
                }
                reply.ok().await
            }
            Err(e) => reply.error(e).await,
        }
    }

    /// Remove a directory.
    #[instrument(skip(self), err, ret)]
    async fn rmdir(
        &self,
        req: &Request<'_>,
        parent: INum,
        dir_name: &str,
        reply: ReplyEmpty<'_>,
    ) -> nix::Result<usize> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("rmdir");
        // check the dir_name is valid
        if let Err(e) = check_name_length(dir_name) {
            return reply.error(e).await;
        }

        let context = ReqContext {
            uid: req.uid(),
            gid: req.gid(),
        };

        let rmdir_res = self
            .metadata
            .unlink(context, parent, dir_name)
            .await
            .add_context(format!(
            "rmdir() failed to remove sub-directory name={dir_name:?} under parent ino={parent}",
        ));
        match rmdir_res {
            // We don't store dir information in the persistent storage, so we don't need to remove
            // it
            Ok(_) => reply.ok().await,
            Err(e) => reply.error(e).await,
        }
    }

    /// Rename a file
    ///
    /// If the target exists it should be atomically replaced. If
    /// the target's inode's lookup count is non-zero, the file
    /// system is expected to postpone any removal of the inode
    /// until the lookup count reaches zero (see description of the
    /// forget function).
    ///
    /// *flags* may be `RENAME_EXCHANGE` or `RENAME_NOREPLACE`. If
    /// `RENAME_NOREPLACE` is specified, the filesystem must not
    /// overwrite *newname* if it exists and return an error
    /// instead. If `RENAME_EXCHANGE` is specified, the filesystem
    /// must atomically exchange the two files, i.e. both must
    /// exist and neither may be deleted.
    async fn rename(
        &self,
        req: &Request<'_>,
        param: RenameParam,
        reply: ReplyEmpty<'_>,
    ) -> nix::Result<usize> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("rename");
        let context = ReqContext {
            uid: req.uid(),
            gid: req.gid(),
        };
        match self.metadata.rename(context, param).await {
            Ok(()) => reply.ok().await,
            Err(e) => {
                debug!("rename() failed, the error is: {:?}", e);
                reply.error(e).await
            }
        }
    }

    /// Read data.
    /// Read should send exactly the number of bytes requested except on EOF or
    /// error, otherwise the rest of the data will be substituted with
    /// zeroes. An exception to self is when the file has been opened in
    /// `direct_io` mode, in which case the return value of the read system
    /// call will reflect the return value of self operation. fh will
    /// contain the value set by the open method, or will be undefined
    /// if the open method didn't set any value.
    async fn read(
        &self,
        req: &Request<'_>,
        fh: u64,
        offset: i64,
        size: u32,
        reply: ReplyData<'_>,
    ) -> nix::Result<usize> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("read");
        let ino = req.nodeid();
        let offset: u64 = offset.cast();

        let (file_size, _) = match self.metadata.read_helper(ino).await {
            Ok((file_size, mtime)) => (file_size, mtime),
            Err(e) => {
                return reply.error(e).await;
            }
        };

        if offset >= file_size {
            return reply.data(Vec::<u8>::new()).await;
        }

        // Ensure `offset + size` is le than the size of the file
        let read_size: u64 = if offset.overflow_add(size.cast::<u64>()) > file_size {
            file_size.overflow_sub(offset.cast::<u64>())
        } else {
            size.cast()
        };

        let result = self.storage.read(ino, fh, offset, read_size.cast()).await;
        // Check the load result
        match result {
            Ok(content) => reply.data(content).await,
            Err(e) => reply.error(e.into()).await,
        }
    }

    /// Write data.
    /// Write should return exactly the number of bytes requested except on
    /// error. An exception to self is when the file has been opened in
    /// `direct_io` mode, in which case the return value of the write system
    /// call will reflect the return value of self operation. fh will
    /// contain the value set by the open method, or will be undefined if
    /// the open method did not set any value.
    #[instrument(skip(self, data), err, ret)]
    async fn write(
        &self,
        req: &Request<'_>,
        fh: u64,
        offset: i64,
        data: Vec<u8>,
        _flags: u32,
        reply: ReplyWrite<'_>,
    ) -> nix::Result<usize> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("write");
        let ino = req.nodeid();
        let data_len: u64 = data.len().cast();

        let (old_size, _) = self.metadata.mtime_and_size(ino);
        let result = self.storage.write(ino, fh, offset.cast(), &data).await;

        let new_mtime = match result {
            Ok(()) => SystemTime::now(),
            Err(e) => {
                return reply.error(e.into()).await;
            }
        };

        let new_size = old_size.max(offset.cast::<u64>().overflow_add(data_len));

        let write_result = self.metadata.write_helper(ino, new_mtime, new_size).await;
        match write_result {
            Ok(()) => reply.written(data_len.cast()).await,
            Err(e) => reply.error(e).await,
        }
    }

    /// Flush method.
    /// This is called on each close() of the opened file. Since file
    /// descriptors can be duplicated (dup, dup2, fork), for one open call
    /// there may be many flush calls. Filesystems should not assume that
    /// flush will always be called after some writes, or that if will be
    /// called at all. fh will contain the value set by the open method, or
    /// will be undefined if the open method did not set any value.
    /// NOTE: the name of the method is misleading, since (unlike fsync) the
    /// filesystem is not forced to flush pending writes. One reason to
    /// flush data, is if the filesystem wants to return write errors. If
    /// the filesystem supports file locking operations (setlk, getlk) it
    /// should remove all locks belonging to `lock_owner`.
    #[instrument(skip(self), err, ret)]
    async fn flush(
        &self,
        req: &Request<'_>,
        fh: u64,
        lock_owner: u64,
        reply: ReplyEmpty<'_>,
    ) -> nix::Result<usize> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("flush");
        let ino = req.nodeid();
        debug!(
            "flush(ino={}, fh={}, lock_owner={}, req={:?})",
            ino, fh, lock_owner, req,
        );
        // This is called from every close on an open file, so call the
        // close on the underlying filesystem.	But since flush may be
        // called multiple times for an open file, self must not really
        // close the file. This is important if used on a network
        // filesystem like NFS which flush the data/metadata on close()
        match self.storage.flush(ino, fh).await {
            Ok(()) => reply.ok().await,
            Err(e) => reply.error(e.into()).await,
        }
    }

    /// Release an open file.
    /// Release is called when there are no more references to an open file: all
    /// file descriptors are closed and all memory mappings are unmapped.
    /// For every open call there will be exactly one release call. The
    /// filesystem may reply with an error, but error values are not
    /// returned to close() or munmap() which triggered the release. fh will
    /// contain the value set by the open method, or will be undefined
    /// if the open method didn't set any value. flags will contain the same
    /// flags as for open.
    #[instrument(skip(self), err, ret)]
    async fn release(
        &self,
        req: &Request<'_>,
        fh: u64,
        flags: u32, // same as the open flags
        lock_owner: u64,
        flush: bool,
        reply: ReplyEmpty<'_>,
    ) -> nix::Result<usize> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("release");
        let ino = req.nodeid();
        match self.storage.close(fh).await {
            Ok(()) => {}
            Err(e) => {
                return reply.error(e.into()).await;
            }
        }
        self.metadata
            .release(ino, fh, flags, lock_owner, flush)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        reply.ok().await
    }

    /// Synchronize file contents.
    /// If the datasync parameter is non-zero, then only the user data should be
    /// flushed, not the meta data.
    #[instrument(skip(self), err, ret)]
    async fn fsync(
        &self,
        req: &Request<'_>,
        fh: u64,
        _datasync: bool,
        reply: ReplyEmpty<'_>,
    ) -> nix::Result<usize> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("fsync");
        let ino = req.nodeid();
        match self.storage.flush(ino, fh).await {
            Ok(()) => reply.ok().await,
            Err(e) => reply.error(e.into()).await,
        }
    }

    /// Open a directory.
    /// Filesystem may store an arbitrary file handle (pointer, index, etc) in
    /// fh, and use self in other all other directory stream operations
    /// (readdir, releasedir, fsyncdir). Filesystem may also implement
    /// stateless directory I/O and not store anything in fh, though that
    /// makes it impossible to implement standard conforming
    /// directory stream operations in case the contents of the directory can
    /// change between opendir and releasedir.
    async fn opendir(
        &self,
        req: &Request<'_>,
        flags: u32,
        reply: ReplyOpen<'_>,
    ) -> nix::Result<usize> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("opendir");
        let ino = req.nodeid();
        debug!("opendir(ino={}, flags={}, req={:?})", ino, flags, req,);
        let context = ReqContext {
            uid: req.uid(),
            gid: req.gid(),
        };
        let o_flags = fs_util::parse_oflag(flags);
        match self.metadata.opendir(context, ino, flags).await {
            Ok(new_fd) => {
                debug!(
                    "opendir() successfully duplicated the file handler of \
                        ino={}  with flags={:?}, the new fd={}",
                    ino, o_flags, new_fd,
                );
                reply.opened(new_fd.cast(), flags).await
            }
            Err(e) => {
                debug!(
                    "opendir() failed to duplicate the file handler of ino={} with flags={:?}, \
                        the error is: {:?}",
                    ino, o_flags, e
                );
                reply.error(e).await
            }
        }
    }

    /// Read directory.
    /// Send a buffer filled using buffer.fill(), with size not exceeding the
    /// requested size. Send an empty buffer on end of stream. fh will contain
    /// the value set by the opendir method, or will be undefined if the
    /// opendir method didn't set any value.
    async fn readdir(
        &self,
        req: &Request<'_>,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory<'_>,
    ) -> nix::Result<usize> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("readdir");
        let ino = req.nodeid();
        debug!(
            "readdir(ino={}, fh={}, offset={}, req={:?})",
            ino, fh, offset, req,
        );

        let context = ReqContext {
            uid: req.uid(),
            gid: req.gid(),
        };
        match self
            .metadata
            .readdir(context, ino, fh, offset, &mut reply)
            .await
        {
            Ok(()) => reply.ok().await,
            Err(e) => {
                debug!("readdir() failed, the error is: {:?}", e);
                reply.error(e).await
            }
        }
    }

    /// Release an open directory.
    /// For every opendir call there will be exactly one releasedir call. fh
    /// will contain the value set by the opendir method, or will be
    /// undefined if the opendir method didn't set any value.
    async fn releasedir(
        &self,
        req: &Request<'_>,
        fh: u64,
        flags: u32,
        reply: ReplyEmpty<'_>,
    ) -> nix::Result<usize> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("releasedir");
        let ino = req.nodeid();
        debug!(
            "releasedir(ino={}, fh={}, flags={}, req={:?})",
            ino, fh, flags, req,
        );
        // TODO: handle flags
        self.metadata
            .releasedir(ino, fh)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        reply.ok().await
    }

    /// Synchronize directory contents.
    /// If the datasync parameter is set, then only the directory contents
    /// should be flushed, not the meta data. fh will contain the value set
    /// by the opendir method, or will be undefined if the opendir method
    /// didn't set any value.
    #[instrument(skip(self), err, ret)]
    async fn fsyncdir(
        &self,
        req: &Request<'_>,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty<'_>,
    ) -> nix::Result<usize> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("fsyncdir");

        // Similarity to rmdir, we don't store dir information in the persistent
        // storage, so we don't need to flush it
        reply.ok().await
    }

    /// Get file system statistics.
    /// The `f_favail`, `f_fsid` and `f_flag` fields are ignored
    async fn statfs(&self, req: &Request<'_>, reply: ReplyStatFs<'_>) -> nix::Result<usize> {
        let ino = if req.nodeid() == 0 {
            FUSE_ROOT_ID
        } else {
            req.nodeid()
        };
        debug!("statfs(ino={}, req={:?})", ino, req);
        let context = ReqContext {
            uid: req.uid(),
            gid: req.gid(),
        };
        match self.metadata.statfs(context, ino).await {
            Ok(statvfs) => {
                debug!(
                    "statfs() successfully read the statvfs of ino={} the statvfs={:?}",
                    ino, statvfs,
                );
                reply.statfs(statvfs).await
            }
            Err(e) => {
                debug!(
                    "statfs() failed to read the statvfs of ino={}  the error is: {:?}",
                    ino, e
                );
                reply.error(e).await
            }
        }
    }

    /// Read symbolic link.
    async fn readlink(&self, req: &Request<'_>, reply: ReplyData<'_>) -> nix::Result<usize> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("readlink");
        let ino = req.nodeid();
        debug!("readlink(ino={}, req={:?})", ino, req,);
        reply
            .data(
                self.metadata
                    .readlink(ino)
                    .await
                    .unwrap_or_else(|e| panic!("{e}")),
            )
            .await
    }

    /// Create a symbolic link.
    async fn symlink(
        &self,
        req: &Request<'_>,
        parent: INum,
        name: &str,
        target_path: &Path,
        reply: ReplyEntry<'_>,
    ) -> nix::Result<usize> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("symlink");
        debug!(
            "symlink(parent={}, name={:?}, target_path={:?}, req={:?})",
            parent, name, target_path, req
        );
        let param = CreateParam {
            parent,
            name: name.to_owned(),
            mode: 0o777,
            rdev: 0,
            uid: req.uid(),
            gid: req.gid(),
            node_type: SFlag::S_IFLNK,
            link: Some(target_path.to_owned()),
        };
        let symlink_res = self.metadata.mknod(
            param
        )
        .await
        .add_context(format!(
            "symlink() failed to create a symlink name={name:?} to target path={target_path:?} under parent ino={parent}",
        ));
        match symlink_res {
            Ok((ttl, fuse_attr, generation)) => reply.entry(ttl, fuse_attr, generation).await,
            Err(e) => {
                debug!(
                    "symlink() failed to create a symlink name={:?} to target path={:?} under parent ino={}, \
                        the error is: {:?}",
                    name,
                    target_path,
                    parent,
                    e,
                );
                reply.error(e).await
            }
        }
    }

    // Un-implemented FUSE operations

    /// Interrupt another FUSE request
    async fn interrupt(&self, req: &Request<'_>, unique: u64) {
        debug!("interrupt(req={:?}), cache size={}", req, 0_i32);
        // TODO: handle FUSE_INTERRUPT
        warn!(
            "FUSE INTERRUPT received, request w/ unique={} interrupted",
            unique
        );
    }

    /// Create a hard link.
    async fn link(
        &self,
        _req: &Request<'_>,
        _newparent: u64,
        _newname: &str,
        reply: ReplyEntry<'_>,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// Set an extended attribute.
    async fn setxattr(
        &self,
        _req: &Request<'_>,
        _name: &str,
        _value: &[u8],
        _flags: u32,
        _position: u32,
        reply: ReplyEmpty<'_>,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// Get an extended attribute.
    /// If `size` is 0, the size of the value should be sent with
    /// `reply.size()`. If `size` is not 0, and the value fits, send it with
    /// `reply.data()`, or `reply.error(ERANGE)` if it doesn't.
    async fn getxattr(
        &self,
        _req: &Request<'_>,
        _name: &str,
        _size: u32,
        reply: ReplyXAttr<'_>,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// List extended attribute names.
    /// If `size` is 0, the size of the value should be sent with
    /// `reply.size()`. If `size` is not 0, and the value fits, send it with
    /// `reply.data()`, or `reply.error(ERANGE)` if it doesn't.
    async fn listxattr(
        &self,
        _req: &Request<'_>,
        _size: u32,
        reply: ReplyXAttr<'_>,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// Remove an extended attribute.
    async fn removexattr(
        &self,
        _req: &Request<'_>,
        _name: &str,
        reply: ReplyEmpty<'_>,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// Check file access permissions.
    /// This will be called for the `access()` system call. If the
    /// `default_permissions` mount option is given, self method is not
    /// called. This method is not called under Linux kernel versions 2.4.x
    async fn access(
        &self,
        _req: &Request<'_>,
        _mask: u32,
        reply: ReplyEmpty<'_>,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// Create and open a file.
    /// If the file does not exist, first create it with the specified mode, and
    /// then open it. Open flags (with the exception of `O_NOCTTY`) are
    /// available in flags. Filesystem may store an arbitrary file handle
    /// (pointer, index, etc) in fh, and use self in other all other file
    /// operations (read, write, flush, release, fsync). There are also some
    /// flags (`direct_io`, `keep_cache`) which the filesystem may set, to
    /// change the way the file is opened. See `fuse_file_info` structure in
    /// `fuse_common.h` for more details. If self method is not implemented
    /// or under Linux kernel versions earlier than 2.6.15, the mknod()
    /// and open() methods will be called instead.
    async fn create(
        &self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &str,
        _mode: u32,
        _flags: u32,
        reply: ReplyCreate<'_>,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// Test for a POSIX file lock.
    async fn getlk(
        &self,
        _req: &Request<'_>,
        _lk_param: FileLockParam,
        reply: ReplyLock<'_>,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// Acquire, modify or release a POSIX file lock.
    /// For POSIX threads (NPTL) there's a 1-1 relation between pid and owner,
    /// but otherwise self is not always the case.  For checking lock
    /// ownership, `fi->owner` must be used. The `l_pid` field in `struct
    /// flock` should only be used to fill in self field in `getlk()`. Note:
    /// if the locking methods are not implemented, the kernel will still
    /// allow file locking to work locally. Hence these are only interesting
    /// for network filesystems and similar.
    async fn setlk(
        &self,
        _req: &Request<'_>,
        _lk_param: FileLockParam,
        _sleep: bool,
        reply: ReplyEmpty<'_>,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// Map block index within file to block index within device.
    /// Note: This makes sense only for block device backed filesystems mounted
    /// with the `blkdev` option
    async fn bmap(
        &self,
        _req: &Request<'_>,
        _blocksize: u32,
        _idx: u64,
        reply: ReplyBMap<'_>,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }
}

#[cfg(test)]
mod test {

    use std::fs::File;

    use nix::sys::stat::SFlag;
    use nix::sys::statvfs;

    use crate::async_fuse::memfs::check_type_supported;
    #[test]
    fn test_statfs() -> anyhow::Result<()> {
        let file = File::open(".")?;
        let statvfs = statvfs::fstatvfs(&file)?;
        println!(
            "blocks={}, bfree={}, bavail={}, files={}, \
                ffree={}, bsize={}, namelen={}, frsize={}",
            statvfs.blocks(),
            statvfs.blocks_free(),
            statvfs.blocks_available(),
            statvfs.files(),
            statvfs.files_free(),
            statvfs.block_size(),
            statvfs.name_max(),
            statvfs.fragment_size(),
        );
        Ok(())
    }

    #[test]
    #[allow(clippy::assertions_on_result_states)]
    fn test_support_file_type() {
        // Currently, we only support regular file, directory,and symlink
        assert!(check_type_supported(&SFlag::S_IFREG).is_ok());
        assert!(check_type_supported(&SFlag::S_IFDIR).is_ok());
        assert!(check_type_supported(&SFlag::S_IFLNK).is_ok());

        assert!(check_type_supported(&SFlag::S_IFBLK).is_err());
        assert!(check_type_supported(&SFlag::S_IFCHR).is_err());
        assert!(check_type_supported(&SFlag::S_IFIFO).is_err());
        assert!(check_type_supported(&SFlag::S_IFSOCK).is_err());
    }
}
