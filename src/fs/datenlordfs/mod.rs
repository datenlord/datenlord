//! The implementation of user space file system
/// Dir entry module
pub mod direntry;
pub mod id_alloc;
mod id_alloc_used;
/// fs metadata module
mod metadata;
/// Opened files
mod open_file;
/// fs metadata with S3 backend module
mod s3_metadata;
pub mod s3_node;

/// Serializable types module
pub mod serial;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::metrics::FILESYSTEM_METRICS;
use async_trait::async_trait;
use clippy_utilities::{Cast, OverflowArithmetic};
pub use metadata::MetaData;
use nix::errno::Errno;
use nix::sys::stat::SFlag;
pub use s3_metadata::S3MetaData;
use tracing::{debug, error, instrument, warn};

use crate::common::error::DatenLordResult;
use crate::fs::datenlordfs::metadata::ReqContext;
use crate::fs::fs_util::{self, build_error_result_from_errno, ROOT_ID};
use crate::new_storage::{Storage, StorageManager};

use super::fs_util::{CreateParam, FileAttr, INum, RenameParam, SetAttrParam};
use super::virtualfs::VirtualFs;
use super::{datenlordfs::direntry::DirEntry, fs_util::StatFsParam};

/// The type of storage
pub type StorageType = StorageManager;

/// In-memory file system
#[derive(Debug)]
pub struct DatenLordFs<M: MetaData + Send + Sync + 'static> {
    /// Fs metadata
    metadata: Arc<M>,
    /// Storage manager
    storage: StorageType,
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

impl<M: MetaData + Send + Sync + 'static> DatenLordFs<M> {
    /// Create `DatenLordFs`
    pub fn new(metadata: Arc<M>, storage: StorageType) -> Self {
        Self { metadata, storage }
    }
}

#[async_trait]
impl<M: MetaData + Send + Sync + 'static> VirtualFs for DatenLordFs<M> {
    // Implemented VirtualFs operations

    /// Initialize filesystem.
    /// Called before any other filesystem method.
    async fn init(&self) -> DatenLordResult<()> {
        Ok(())
    }

    /// Clean up filesystem.
    /// Called on filesystem exit.
    async fn destroy(&self) -> DatenLordResult<()> {
        Ok(())
    }

    /// Look up a directory entry by name and get its attributes.
    async fn lookup(
        &self,
        uid: u32,
        gid: u32,
        parent: INum,
        name: &str,
    ) -> DatenLordResult<(Duration, FileAttr, u64)> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("lookup");
        debug!(
            "lookup(parent={}, name={:?}, uid={:?}, gid={:?})",
            parent, name, uid, gid
        );
        // check the dir_name is valid
        check_name_length(name)?;

        let context = ReqContext { uid, gid };
        match self.metadata.lookup_helper(context, parent, name).await {
            Ok(result) => {
                debug!(
                    "datenlordfs calllookup() success, the result is: {:?}",
                    result
                );
                Ok(result)
            }
            Err(e) => {
                debug!("datenlordfs calllookup() failed, the error is: {:?}", e);
                Err(e)
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
    #[instrument(level = "debug", skip(self))]
    async fn forget(&self, ino: u64, nlookup: u64) {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("forget");
        let deleted = match self.metadata.forget(ino, nlookup).await {
            Ok(deleted) => {
                debug!(
                    "datenlordfs callforget() success, the result is: {:?}",
                    deleted
                );
                deleted
            }
            Err(e) => {
                debug!("datenlordfs callforget() failed, the error is: {:?}", e);
                false
            }
        };
        if deleted {
            match self.storage.remove(ino).await {
                Ok(()) => {
                    debug!("datenlordfs callforget() defer deletion success");
                }
                Err(e) => {
                    debug!(
                        "datenlordfs callforget() defer deletion failed, the error is: {:?}",
                        e
                    );
                }
            }
        }
    }

    /// Get file attributes.
    async fn getattr(&self, ino: u64) -> DatenLordResult<(Duration, FileAttr)> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("getattr");
        debug!("getattr(ino={})", ino);
        match self.metadata.getattr(ino).await {
            Ok(result) => {
                debug!(
                    "datenlordfs callgetattr() success, the result is: {:?}",
                    result
                );
                Ok(result)
            }
            Err(e) => {
                debug!("datenlordfs callgetattr() failed, the error is: {:?}", e);
                Err(e)
            }
        }
    }

    /// Set file attributes.
    async fn setattr(
        &self,
        uid: u32,
        gid: u32,
        ino: u64,
        param: SetAttrParam,
    ) -> DatenLordResult<(Duration, FileAttr)> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("setattr");
        let valid = param.valid;

        #[cfg(feature = "abi-7-9")]
        let _lock_owner = param.lock_owner;
        #[cfg(feature = "abi-7-23")]
        let _c_time = param.c_time;
        if 0 == valid {
            warn!("setattr() encountered valid=0, the req={:?}", param);
        };
        let context = ReqContext { uid, gid };
        match self
            .metadata
            .setattr_helper(context, ino, &param, &self.storage)
            .await
        {
            Ok(result) => {
                debug!(
                    "datenlordfs callsetattr() success, the result is: {:?}",
                    result
                );
                Ok(result)
            }
            Err(e) => {
                debug!("datenlordfs callsetattr() failed, the error is: {:?}", e);
                Err(e)
            }
        }
    }

    /// Read symbolic link.
    async fn readlink(&self, ino: u64) -> DatenLordResult<Vec<u8>> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("readlink");
        debug!("readlink(ino={})", ino);

        match self.metadata.readlink(ino).await {
            Ok(result) => {
                debug!(
                    "datenlordfs callreadlink() success, the result is: {:?}",
                    result
                );
                Ok(result)
            }
            Err(e) => {
                debug!("datenlordfs callreadlink() failed, the error is: {:?}", e);
                Err(e)
            }
        }
    }

    /// Create file node.
    /// Create a regular file, character device, block device, fifo or socket
    /// node.
    async fn mknod(&self, param: CreateParam) -> DatenLordResult<(Duration, FileAttr, u64)> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("mknod");
        debug!("mknod param = {:?}", param);
        match self.metadata.mknod(param).await {
            Ok(result) => {
                debug!(
                    "datenlordfs callmknod() success, the result is: {:?}",
                    result
                );
                Ok(result)
            }
            Err(e) => {
                debug!("datenlordfs callmknod() failed, the error is: {:?}", e);
                Err(e)
            }
        }
    }

    /// Create a directory.
    async fn mkdir(&self, param: CreateParam) -> DatenLordResult<(Duration, FileAttr, u64)> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("mkdir");
        debug!("mkdir(param={:?}", param);
        let param = CreateParam {
            parent: param.parent,
            name: param.name,
            mode: param.mode,
            rdev: 0,
            uid: param.uid,
            gid: param.gid,
            node_type: SFlag::S_IFDIR,
            link: None,
        };
        match self.metadata.mknod(param).await {
            Ok(result) => {
                debug!(
                    "datenlordfs callmkdir() success, the result is: {:?}",
                    result
                );
                Ok(result)
            }
            Err(e) => {
                debug!("datenlordfs callmkdir() failed, the error is: {:?}", e);
                Err(e)
            }
        }
    }

    /// Remove a file.
    #[instrument(level = "debug", skip(self), err, ret)]
    async fn unlink(&self, uid: u32, gid: u32, parent: INum, name: &str) -> DatenLordResult<()> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("unlink");
        debug!("unlink(parent={}, name={:?}", parent, name);
        // check the dir_name is valid
        check_name_length(name)?;

        let context = ReqContext { uid, gid };

        match self.metadata.unlink(context, parent, name).await {
            Ok(result) => {
                if let Some(ino) = result {
                    self.storage.remove(ino).await?;
                }
                debug!(
                    "datenlordfs callunlink() success, the result is: {:?}",
                    result
                );
                Ok(())
            }
            Err(e) => {
                debug!("datenlordfs callunlink() failed, the error is: {:?}", e);
                Err(e)
            }
        }
    }

    /// Remove a directory.
    #[instrument(level = "debug", skip(self), err, ret)]
    async fn rmdir(
        &self,
        uid: u32,
        gid: u32,
        parent: INum,
        dir_name: &str,
    ) -> DatenLordResult<Option<INum>> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("rmdir");
        // check the dir_name is valid
        check_name_length(dir_name)?;

        let context = ReqContext { uid, gid };

        match self.metadata.unlink(context, parent, dir_name).await {
            Ok(result) => {
                // We don't store dir information in the persistent storage, so we don't need to remove
                // it
                debug!(
                    "datenlordfs callrmdir() success, the result is: {:?}",
                    result
                );
                Ok(result)
            }
            Err(e) => {
                debug!("datenlordfs callrmdir() failed, the error is: {:?}", e);
                Err(e)
            }
        }
    }

    /// Create a symbolic link.
    async fn symlink(
        &self,
        uid: u32,
        gid: u32,
        parent: INum,
        name: &str,
        target_path: &Path,
    ) -> DatenLordResult<(Duration, FileAttr, u64)> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("symlink");
        debug!(
            "symlink(parent={}, name={:?}, target_path={:?})",
            parent, name, target_path
        );
        let param = CreateParam {
            parent,
            name: name.to_owned(),
            mode: 0o777,
            rdev: 0,
            uid,
            gid,
            node_type: SFlag::S_IFLNK,
            link: Some(target_path.to_owned()),
        };
        match self.metadata.mknod(param).await {
            Ok(result) => {
                debug!(
                    "datenlordfs callsymlink() success, the result is: {:?}",
                    result
                );
                Ok(result)
            }
            Err(e) => {
                debug!("datenlordfs callsymlink() failed, the error is: {:?}", e);
                Err(e)
            }
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
    async fn rename(&self, uid: u32, gid: u32, param: RenameParam) -> DatenLordResult<()> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("rename");
        let context = ReqContext { uid, gid };
        match self.metadata.rename(context, param).await {
            Ok(()) => {
                debug!("datenlordfs callrename() success");
                Ok(())
            }
            Err(e) => {
                debug!("datenlordfs callrename() failed, the error is: {:?}", e);
                Err(e)
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
    async fn open(&self, uid: u32, gid: u32, ino: u64, flags: u32) -> DatenLordResult<u64> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("open");
        debug!("open(ino={}, flags={}", ino, flags);

        let context = ReqContext { uid, gid };

        match self.metadata.open(context, ino, flags).await {
            Ok(fd) => {
                self.storage.open(ino, fd.cast(), flags.into());
                debug!("datenlordfs callopen() success, the result is: {:?}", fd);
                Ok(fd)
            }
            Err(e) => {
                debug!("datenlordfs callopen() failed, the error is: {:?}", e);
                return Err(e);
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
        ino: u64,
        fh: u64,
        offset: u64,
        size: u32,
        buf: &mut [u8],
    ) -> DatenLordResult<usize> {
        let start_time = tokio::time::Instant::now();
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("read");
        let offset: u64 = offset.cast();

        let (file_size, _) = match self.metadata.read_helper(ino).await {
            Ok((file_size, mtime)) => (file_size, mtime),
            Err(e) => {
                return Err(e);
            }
        };

        if offset >= file_size {
            return Ok(0);
        }

        // Ensure `offset + size` is le than the size of the file
        let read_size: u64 = if offset.overflow_add(size.cast::<u64>()) > file_size {
            file_size.overflow_sub(offset.cast::<u64>())
        } else {
            size.cast()
        };

        // TODO: use same buffer to avoid copy
        let result = self.storage.read(ino, fh, offset, read_size.cast(), buf).await;
        match result {
            Ok(()) => {
                // error!("content length: {:?}, buf length: {:?}", content.len(), buf.len());
                // assert!(content.len() <= buf.len());
                // #[allow(clippy::needless_range_loop)]
                // buf[..content.len()].copy_from_slice(&content);
                // debug!(
                //     "datenlordfs callread() success, the result is: content length {:?}",
                //     content.len()
                // );
                error!("ino: {:?} offset: {:?} read duration: {:?}", ino, offset, start_time.elapsed());
                Ok(buf.len())
            }
            Err(e) => {
                debug!("datenlordfs callread() failed, the error is: {:?}", e);
                return Err(e.into());
            }
        }
    }

    /// Write data.
    /// Write should return exactly the number of bytes requested except on
    /// error. An exception to self is when the file has been opened in
    /// `direct_io` mode, in which case the return value of the write system
    /// call will reflect the return value of self operation. fh will
    /// contain the value set by the open method, or will be undefined if
    /// the open method did not set any value.
    #[instrument(level = "debug", skip(self, data), err, ret)]
    async fn write(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _flags: u32,
    ) -> DatenLordResult<()> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("write");
        let data_len: u64 = data.len().cast();

        debug!(
            "write(ino={}, fh={}, offset={}, data_len={})",
            ino, fh, offset, data_len
        );

        let (old_size, _) = self.metadata.mtime_and_size(ino);
        let result = self.storage.write(ino, fh, offset.cast(), &data).await;

        let new_mtime = match result {
            Ok(()) => SystemTime::now(),
            Err(e) => {
                return Err(e.into());
            }
        };

        let new_size = old_size.max(offset.cast::<u64>().overflow_add(data_len));

        match self.metadata.write_helper(ino, new_mtime, new_size).await {
            Ok(()) => {
                debug!("datenlordfs callwrite() success");
                Ok(())
            }
            Err(e) => {
                debug!("datenlordfs callwrite() failed, the error is: {:?}", e);
                Err(e)
            }
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
    #[instrument(level = "debug", skip(self), err, ret)]
    async fn flush(&self, ino: u64, fh: u64, lock_owner: u64) -> DatenLordResult<()> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("flush");
        debug!("flush(ino={}, fh={}, lock_owner={})", ino, fh, lock_owner,);
        // This is called from every close on an open file, so call the
        // close on the underlying filesystem.	But since flush may be
        // called multiple times for an open file, self must not really
        // close the file. This is important if used on a network
        // filesystem like NFS which flush the data/metadata on close()
        match self.storage.flush(ino, fh).await {
            Ok(()) => {
                debug!("datenlordfs callflush() success");
                Ok(())
            }
            Err(e) => {
                debug!("datenlordfs callflush() failed, the error is: {:?}", e);
                Err(e.into())
            }
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
    #[instrument(level = "debug", skip(self), err, ret)]
    async fn release(
        &self,
        ino: u64,
        fh: u64,
        flags: u32, // same as the open flags
        lock_owner: u64,
        flush: bool,
    ) -> DatenLordResult<()> {
        debug!(
            "release(ino={}, fh={}, flags={}, lock_owner={}, flush={})",
            ino, fh, flags, lock_owner, flush
        );
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("release");
        match self.storage.close(fh).await {
            Ok(()) => {}
            Err(e) => {
                return Err(e.into());
            }
        }
        debug!("datenlordfs callrelease() close the file handler success");
        match self
            .metadata
            .release(ino, fh, flags, lock_owner, flush)
            .await
        {
            Ok(()) => {
                debug!("datenlordfs callrelease() success");
                Ok(())
            }
            Err(e) => {
                debug!("datenlordfs callrelease() failed, the error is: {:?}", e);
                Err(e)
            }
        }
    }

    /// Synchronize file contents.
    /// If the datasync parameter is non-zero, then only the user data should be
    /// flushed, not the meta data.
    #[instrument(level = "debug", skip(self), err, ret)]
    async fn fsync(&self, ino: u64, fh: u64, _datasync: bool) -> DatenLordResult<()> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("fsync");
        match self.storage.flush(ino, fh).await {
            Ok(()) => {
                debug!("datenlordfs callfsync() success");
                Ok(())
            }
            Err(e) => {
                debug!("datenlordfs callfsync() failed, the error is: {:?}", e);
                Err(e.into())
            }
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
    async fn opendir(&self, uid: u32, gid: u32, ino: u64, flags: u32) -> DatenLordResult<u64> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("opendir");
        debug!("opendir(ino={:?}, flags={:?})", ino, flags);
        let context = ReqContext { uid, gid };
        let o_flags = fs_util::parse_oflag(flags);
        match self.metadata.opendir(context, ino, flags).await {
            Ok(fd) => {
                debug!(
                    "datenlordfs callopendir() successfully duplicated the file handler of \
                        ino={}  with flags={:?}, the new fd={}",
                    ino, o_flags, fd,
                );
                Ok(fd)
            }
            Err(e) => {
                debug!("datenlordfs callopendir() failed, the error is: {:?}", e);
                Err(e)
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
        uid: u32,
        gid: u32,
        ino: u64,
        fh: u64,
        offset: i64,
    ) -> DatenLordResult<Vec<DirEntry>> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("readdir");
        debug!("readdir(ino={}, fh={}, offset={})", ino, fh, offset,);

        let context = ReqContext { uid, gid };
        match self.metadata.readdir(context, ino, fh, offset).await {
            Ok(result) => {
                debug!(
                    "datenlordfs callreaddir() success, the result is: {:?}",
                    result
                );
                Ok(result)
            }
            Err(e) => {
                debug!("datenlordfs callreaddir() failed, the error is: {:?}", e);
                Err(e)
            }
        }
    }

    /// Release an open directory.
    /// For every opendir call there will be exactly one releasedir call. fh
    /// will contain the value set by the opendir method, or will be
    /// undefined if the opendir method didn't set any value.
    async fn releasedir(&self, ino: u64, fh: u64, flags: u32) -> DatenLordResult<()> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("releasedir");
        debug!("releasedir(ino={}, fh={}, flags={}", ino, fh, flags,);
        // TODO: handle flags
        match self.metadata.releasedir(ino, fh).await {
            Ok(()) => {
                debug!("datenlordfs callreleasedir() success");
                Ok(())
            }
            Err(e) => {
                debug!("datenlordfs callreleasedir() failed, the error is: {:?}", e);
                Err(e)
            }
        }
    }

    /// Synchronize directory contents.
    /// If the datasync parameter is set, then only the directory contents
    /// should be flushed, not the meta data. fh will contain the value set
    /// by the opendir method, or will be undefined if the opendir method
    /// didn't set any value.
    #[instrument(level = "debug", skip(self), err, ret)]
    async fn fsyncdir(&self, ino: u64, _fh: u64, _datasync: bool) -> DatenLordResult<()> {
        let _timer = FILESYSTEM_METRICS.start_storage_operation_timer("fsyncdir");

        // Similarity to rmdir, we don't store dir information in the persistent
        // storage, so we don't need to flush it
        debug!("fsyncdir(ino={})", ino);
        Ok(())
    }

    /// Get file system statistics.
    /// The `f_favail`, `f_fsid` and `f_flag` fields are ignored
    async fn statfs(&self, uid: u32, gid: u32, ino: u64) -> DatenLordResult<StatFsParam> {
        let ino = if ino == 0 { ROOT_ID } else { ino };
        debug!("statfs(ino={:?})", ino);
        let context = ReqContext { uid, gid };
        match self.metadata.statfs(context, ino).await {
            Ok(result) => {
                debug!(
                    "datenlordfs callstatfs() success, the result is: {:?}",
                    result
                );
                Ok(result)
            }
            Err(e) => {
                debug!("datenlordfs callstatfs() failed, the error is: {:?}", e);
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod test {

    use std::fs::File;

    use nix::sys::stat::SFlag;
    use nix::sys::statvfs;

    use crate::fs::datenlordfs::check_type_supported;
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
