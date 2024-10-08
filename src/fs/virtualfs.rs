//! The `FileSystem` trait
use std::{path::Path, time::Duration};

use async_trait::async_trait;
use tracing::warn;

use crate::{
    common::error::{DatenLordError, DatenLordResult},
    fs::fs_util::{CreateParam, FileLockParam, RenameParam, SetAttrParam},
};

use super::{
    datenlordfs::direntry::DirEntry,
    fs_util::{FileAttr, INum, StatFsParam},
};

/// Virtual filesystem trait
#[async_trait]
pub trait VirtualFs: Sync + Send {
    /// Initialize filesystem
    async fn init(&self) -> DatenLordResult<()> {
        Err(DatenLordError::Unimplemented {
            context: vec!["init unimplemented".to_owned()],
        })
    }

    /// Clean up filesystem
    async fn destroy(&self) -> DatenLordResult<()> {
        Err(DatenLordError::Unimplemented {
            context: vec!["destroy unimplemented".to_owned()],
        })
    }

    /// Interrupt another request, especially for FUSE
    /// This is a no-op for other filesystems
    async fn interrupt(&self, unique: u64) {
        warn!(
            "INTERRUPT received, request w/ unique={} interrupted",
            unique
        );
    }

    /// Look up a directory entry by name and get its attributes.
    async fn lookup(
        &self,
        uid: u32,
        gid: u32,
        parent: INum,
        name: &str,
    ) -> DatenLordResult<(Duration, FileAttr, u64)>;

    /// Forget about an inode
    async fn forget(&self, ino: u64, nlookup: u64);

    /// Get file attributes.
    async fn getattr(&self, ino: u64) -> DatenLordResult<(Duration, FileAttr)>;

    /// Set file attributes.
    async fn setattr(
        &self,
        uid: u32,
        gid: u32,
        ino: u64,
        param: SetAttrParam,
    ) -> DatenLordResult<(Duration, FileAttr)>;

    /// Read symbolic link.
    async fn readlink(&self, ino: u64) -> DatenLordResult<Vec<u8>>;

    /// Create file node.
    async fn mknod(&self, param: CreateParam) -> DatenLordResult<(Duration, FileAttr, u64)>;

    /// Create a directory
    async fn mkdir(&self, param: CreateParam) -> DatenLordResult<(Duration, FileAttr, u64)>;

    /// Remove a file
    async fn unlink(&self, uid: u32, gid: u32, parent: INum, name: &str) -> DatenLordResult<()>;

    /// Remove a directory
    async fn rmdir(
        &self,
        uid: u32,
        gid: u32,
        parent: INum,
        dir_name: &str,
    ) -> DatenLordResult<Option<INum>>;

    /// Create a symbolic link
    async fn symlink(
        &self,
        uid: u32,
        gid: u32,
        parent: INum,
        name: &str,
        target_path: &Path,
    ) -> DatenLordResult<(Duration, FileAttr, u64)>;

    /// Rename a file
    async fn rename(&self, uid: u32, gid: u32, param: RenameParam) -> DatenLordResult<()>;

    /// Create a hard link
    #[allow(unused_variables)]
    async fn link(&self, newparent: u64, newname: &str) -> DatenLordResult<()> {
        Err(DatenLordError::Unimplemented {
            context: vec!["link unimplemented".to_owned()],
        })
    }

    /// Open a file
    async fn open(&self, uid: u32, gid: u32, ino: u64, flags: u32) -> DatenLordResult<u64>;

    /// Read data with the given buffer, return current offset and the number of bytes read
    async fn read(
        &self,
        ino: u64,
        fh: u64,
        offset: u64,
        size: u32,
        buf: &mut [u8],
    ) -> DatenLordResult<usize>;

    /// Write data
    async fn write(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        flags: u32,
    ) -> DatenLordResult<()>;

    /// Flush method
    async fn flush(&self, ino: u64, fh: u64, lock_owner: u64) -> DatenLordResult<()>;

    /// Release an open file
    async fn release(
        &self,
        ino: u64,
        fh: u64,
        flags: u32, // same as the open flags
        lock_owner: u64,
        flush: bool,
    ) -> DatenLordResult<()>;

    /// Synchronize file contents
    async fn fsync(&self, ino: u64, fh: u64, datasync: bool) -> DatenLordResult<()>;

    /// Open a directory
    async fn opendir(&self, uid: u32, gid: u32, ino: u64, flags: u32) -> DatenLordResult<u64>;

    /// Read directory
    async fn readdir(
        &self,
        uid: u32,
        gid: u32,
        ino: u64,
        fh: u64,
        offset: i64,
    ) -> DatenLordResult<Vec<DirEntry>>;

    /// Release an open directory
    async fn releasedir(&self, ino: u64, fh: u64, flags: u32) -> DatenLordResult<()>;

    /// Synchronize directory contents
    async fn fsyncdir(&self, ino: u64, fh: u64, datasync: bool) -> DatenLordResult<()>;

    /// Get file system statistics
    async fn statfs(&self, uid: u32, gid: u32, ino: u64) -> DatenLordResult<StatFsParam>;

    /// Set an extended attribute
    #[allow(unused_variables)]
    async fn setxattr(
        &self,
        ino: u64,
        name: &str,
        value: &[u8],
        flags: u32,
        position: u32,
    ) -> DatenLordResult<()> {
        Err(DatenLordError::Unimplemented {
            context: vec!["setxattr unimplemented".to_owned()],
        })
    }

    /// Get an extended attribute
    #[allow(unused_variables)]
    async fn getxattr(&self, ino: u64, name: &str, size: u32) -> DatenLordResult<()> {
        Err(DatenLordError::Unimplemented {
            context: vec!["getxattr unimplemented".to_owned()],
        })
    }

    /// Get an extended attribute
    #[allow(unused_variables)]
    async fn listxattr(&self, ino: u64, size: u32) -> DatenLordResult<()> {
        Err(DatenLordError::Unimplemented {
            context: vec!["listxattr unimplemented".to_owned()],
        })
    }

    /// Remove an extended attribute
    #[allow(unused_variables)]
    async fn removexattr(&self, ino: u64, name: &str) -> DatenLordResult<()> {
        Err(DatenLordError::Unimplemented {
            context: vec!["removexattr unimplemented".to_owned()],
        })
    }

    /// Check file access permissions
    ///
    /// For FUSE:
    /// This will be called for the `access()` system call. If the
    /// `default_permissions` mount option is given, self method is not
    /// called. This method is not called under Linux kernel versions 2.4.x
    #[allow(unused_variables)]
    async fn access(&self, uid: u32, gid: u32, ino: u64, mask: u32) -> DatenLordResult<()> {
        Err(DatenLordError::Unimplemented {
            context: vec!["access unimplemented".to_owned()],
        })
    }

    /// Create and open a file
    ///
    /// For FUSE:
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
    #[allow(unused_variables)]
    async fn create(
        &self,
        uid: u32,
        gid: u32,
        ino: u64,
        parent: u64,
        name: &str,
        mode: u32,
        flags: u32,
    ) -> DatenLordResult<()> {
        Err(DatenLordError::Unimplemented {
            context: vec!["create unimplemented".to_owned()],
        })
    }

    /// Test for a POSIX file lock
    #[allow(unused_variables)]
    async fn getlk(
        &self,
        uid: u32,
        gid: u32,
        ino: u64,
        lk_param: FileLockParam,
    ) -> DatenLordResult<()> {
        Err(DatenLordError::Unimplemented {
            context: vec!["getlk unimplemented".to_owned()],
        })
    }

    /// Acquire, modify or release a POSIX file lock
    ///
    /// For FUSE:
    /// For POSIX threads (NPTL) there's a 1-1 relation between pid and owner,
    /// but otherwise self is not always the case.  For checking lock
    /// ownership, `fi->owner` must be used. The `l_pid` field in `struct
    /// flock` should only be used to fill in self field in `getlk()`. Note:
    /// if the locking methods are not implemented, the kernel will still
    /// allow file locking to work locally. Hence these are only interesting
    /// for network filesystems and similar.
    #[allow(unused_variables)]
    async fn setlk(
        &self,
        uid: u32,
        gid: u32,
        ino: u64,
        lk_param: FileLockParam,
        sleep: bool,
    ) -> DatenLordResult<()> {
        Err(DatenLordError::Unimplemented {
            context: vec!["setlk unimplemented".to_owned()],
        })
    }

    /// Map block index within file to block index within device
    /// Note: This makes sense only for block device backed filesystems mounted
    /// with the `blkdev` option
    #[allow(unused_variables)]
    async fn bmap(
        &self,
        uid: u32,
        gid: u32,
        ino: u64,
        blocksize: u32,
        idx: u64,
    ) -> DatenLordResult<()> {
        Err(DatenLordError::Unimplemented {
            context: vec!["bmap unimplemented".to_owned()],
        })
    }
}
