use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;

use super::kv_engine::KVEngineType;
use super::node::Node;
use super::{CreateParam, RenameParam, SetAttrParam, StorageType};
use crate::async_fuse::fuse::fuse_reply::{ReplyDirectory, StatFsParam};
use crate::async_fuse::fuse::protocol::{FuseAttr, INum};
use crate::common::error::DatenLordResult;

pub(crate) mod error {
    //! A module containing helper functions to build errors.

    use tracing::error;

    use super::INum;
    use crate::common::error::DatenLordError;

    /// A helper function to build [`DatenLordError::InconsistentFS`] with
    /// default context.
    pub(crate) fn build_inconsistent_fs(ino: INum, fn_name: &str) -> DatenLordError {
        error!(
            "{}() found fs is inconsistent, the inode ino={} is not in cache.",
            fn_name, ino
        );
        DatenLordError::InconsistentFS {
            context: vec![format!(
                "{ino}() found fs is inconsistent, the inode ino={fn_name} is not in cache.",
            )],
        }
    }
}

/// The context of a request contains the uid and gid
#[derive(Debug, Clone)]
pub struct ReqContext {
    /// The uid of the user who sends the request
    pub uid: u32,
    /// The gid of the user who sends the request
    pub gid: u32,
}

/// MetaData of fs
#[async_trait]
pub trait MetaData {
    /// Node type
    type N: Node + Send + Sync + 'static;

    /// Create `MetaData`
    #[allow(clippy::too_many_arguments)]
    async fn new(kv_engine: Arc<KVEngineType>, node_id: &str) -> DatenLordResult<Arc<Self>>;

    /// Helper function to create node
    async fn mknod(&self, param: CreateParam) -> DatenLordResult<(Duration, FuseAttr, u64)>;

    /// Helper function to lookup
    async fn lookup_helper(
        &self,
        context: ReqContext,
        parent: INum,
        name: &str,
    ) -> DatenLordResult<(Duration, FuseAttr, u64)>;

    /// Rename helper to exchange on disk
    async fn rename(&self, context: ReqContext, param: RenameParam) -> DatenLordResult<()>;

    /// Helper function to write data
    async fn write_helper(
        &self,
        ino: u64,
        new_mtime: SystemTime,
        new_size: u64,
    ) -> DatenLordResult<()>;

    /// Helper function to get a open file's size and mtime
    /// # Return
    /// Return a tuple of (file_size, modified_time)
    /// It will not change the file's atime
    fn mtime_and_size(&self, ino: u64) -> (u64, SystemTime);

    /// Set fuse fd into `MetaData`
    async fn set_fuse_fd(&self, fuse_fd: RawFd);

    /// Set Node's attribute
    async fn setattr_helper(
        &self,
        context: ReqContext,
        ino: u64,
        param: &SetAttrParam,
        storage: &StorageType,
    ) -> DatenLordResult<(Duration, FuseAttr)>;

    /// Helper function to unlink
    /// # Return
    /// Return the ino of the removed file
    async fn unlink(
        &self,
        context: ReqContext,
        parent: INum,
        name: &str,
    ) -> DatenLordResult<Option<INum>>;

    /// Get attribute of i-node by ino
    async fn getattr(&self, ino: u64) -> DatenLordResult<(Duration, FuseAttr)>;

    /// Open a file or directory by ino and flags
    async fn open(&self, context: ReqContext, ino: u64, flags: u32) -> DatenLordResult<u64>;

    /// Forget a i-node by ino
    /// # Return
    /// Return true if the file is removed
    /// Return false if the file is not removed
    async fn forget(&self, ino: u64, nlookup: u64) -> DatenLordResult<bool>;

    /// Helper function to read data
    /// # Return
    /// Return a tuple of (file_size, modified_time)
    async fn read_helper(&self, ino: u64) -> DatenLordResult<(u64, SystemTime)>;

    /// Helper function to release dir
    async fn releasedir(&self, ino: u64, fh: u64) -> DatenLordResult<()>;

    /// Statfs helper
    async fn statfs(&self, context: ReqContext, ino: u64) -> DatenLordResult<StatFsParam>;

    /// Helper function to readlink
    async fn readlink(&self, ino: u64) -> DatenLordResult<Vec<u8>>;

    /// Helper function to opendir
    async fn opendir(&self, context: ReqContext, ino: u64, flags: u32) -> DatenLordResult<u64>;

    /// Helper function to readdir
    async fn readdir(
        &self,
        context: ReqContext,
        ino: u64,
        fh: u64,
        offset: i64,
        reply: &mut ReplyDirectory,
    ) -> DatenLordResult<()>;

    /// Helper function to release
    async fn release(
        &self,
        ino: u64,
        fh: u64,
        flags: u32,
        lock_owner: u64,
        flush: bool,
    ) -> DatenLordResult<()>;
}
