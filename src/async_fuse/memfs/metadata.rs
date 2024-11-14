use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use super::kv_engine::KVEngineType;
use super::node::Node;
use super::{CreateParam, FileAttr, RenameParam, SetAttrParam, StorageType};
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

    /// Helper function to write remote data
    async fn write_remote_size_helper(&self, ino: u64, size: u64) -> DatenLordResult<()>;

    /// Set fuse fd into `MetaData`
    async fn set_fuse_fd(&self, fuse_fd: RawFd);

    /// Set Node's attribute
    async fn setattr_helper<M: MetaData + Send + Sync + 'static>(
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

    /// Get attribute of i-node by ino from remote
    async fn get_remote_attr(&self, ino: u64) -> DatenLordResult<(Duration, FileAttr)>;

    /// Open a file or directory by ino and flags from local, and return a file handler
    async fn open_local(&self, context: ReqContext, ino: u64, flags: u32) -> DatenLordResult<u64>;

    /// Open a file or directory by ino and flags from remote, and return a file handler
    /// This function will be called at the first time when open a file
    async fn open_remote(
        &self,
        context: ReqContext,
        ino: u64,
        flags: u32,
    ) -> DatenLordResult<(u64, FileAttr)>;

    /// Forget a i-node by ino
    /// # Return
    /// Return true if the file is removed
    /// Return false if the file is not removed
    async fn forget(&self, ino: u64, nlookup: u64) -> DatenLordResult<bool>;

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
