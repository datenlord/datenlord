use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nix::sys::stat::SFlag;

use super::cache::IoMemBlock;
use super::dist::server::CacheServer;
use super::kv_engine::KVEngineType;
use super::node::Node;
use super::{CreateParam, RenameParam, SetAttrParam};
use crate::async_fuse::fuse::fuse_reply::{ReplyDirectory, StatFsParam};
use crate::async_fuse::fuse::protocol::{FuseAttr, INum};
use crate::common::error::DatenLordResult;
use crate::common::etcd_delegate::EtcdDelegate;

pub(crate) mod error {
    //! A module containing helper functions to build errors.

    use std::fmt::Display;

    use super::{INum, SFlag};
    use crate::common::error::DatenLordError;

    /// A helper function to build [`DatenLordError::InconsistentFS`] with default context.
    pub(crate) fn build_inconsistent_fs(ino: INum, fn_name: &str) -> DatenLordError {
        DatenLordError::InconsistentFS {
            context: vec![format!(
                "{ino}() found fs is inconsistent, the inode ino={fn_name} is not in cache.",
            )],
        }
    }

    /// A helper function to build [`DatenLordError::InconsistentFS`] with custom context.
    pub(crate) fn build_inconsistent_fs_with_context<C: Display>(
        fn_name: &str,
        context: C,
    ) -> DatenLordError {
        DatenLordError::InconsistentFS {
            context: vec![format!("{fn_name}() found fs is inconsistent: {context}",)],
        }
    }

    /// A helper function to build [`DatenLordError::UnsupportedINodeType`].
    pub(crate) fn build_unsupported_inode_type(
        node_type: SFlag,
        context: String,
    ) -> DatenLordError {
        DatenLordError::UnsupportedINodeType {
            node_type,
            context: vec![context],
        }
    }
}

/// The context of a request contains the uid and gid
#[derive(Debug, Clone)]
pub struct ReqContext {
    /// The uid of the user who sends the request
    pub user_id: u32,
    /// The gid of the user who sends the request
    pub group_id: u32,
}

/// MetaData of fs
#[async_trait]
pub trait MetaData {
    /// Node type
    type N: Node + Send + Sync + 'static;

    /// Create `MetaData`
    #[allow(clippy::too_many_arguments)]
    async fn new(
        root_path: &str,
        capacity: usize,
        ip: &str,
        port: &str,
        etcd_client: EtcdDelegate,
        kv_engine: Arc<KVEngineType>,
        node_id: &str,
        volume_info: &str,
    ) -> DatenLordResult<(Arc<Self>, Option<CacheServer>)>;

    /// Helper function to create node
    async fn create_node_helper(
        &self,
        param: CreateParam,
    ) -> DatenLordResult<(Duration, FuseAttr, u64)>;

    /// Helper function to remove node
    async fn remove_node_helper(
        &self,
        context: ReqContext,
        parent: INum,
        node_name: &str,
        node_type: SFlag,
    ) -> DatenLordResult<()>;

    /// Helper function to lookup
    async fn lookup_helper(
        &self,
        context: ReqContext,
        parent: INum,
        name: &str,
    ) -> DatenLordResult<(Duration, FuseAttr, u64)>;

    /// Rename helper to exchange on disk
    #[cfg(feature = "abi-7-23")]
    async fn rename_exchange_helper(
        &self,
        context: ReqContext,
        param: RenameParam,
    ) -> DatenLordResult<()>;

    /// Rename helper to move on disk, it may replace destination entry
    async fn rename_may_replace_helper(
        &self,
        context: ReqContext,
        param: RenameParam,
    ) -> DatenLordResult<()>;

    /// Helper function of fsync
    async fn fsync_helper(&self, ino: u64, fh: u64, datasync: bool) -> DatenLordResult<()>;

    /// Try to delete node that is marked as deferred deletion
    async fn try_delete_node(&self, ino: INum) -> DatenLordResult<bool>;

    /// Helper function to write data
    async fn write_helper(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        data: Vec<u8>,
        flags: u32,
    ) -> DatenLordResult<usize>;

    /// Set fuse fd into `MetaData`
    async fn set_fuse_fd(&self, fuse_fd: RawFd);

    /// Set Node's attribute
    async fn setattr_helper(
        &self,
        context: ReqContext,
        ino: u64,
        param: SetAttrParam,
    ) -> DatenLordResult<(Duration, FuseAttr)>;

    /// Helper function to unlink
    async fn unlink(&self, context: ReqContext, parent: INum, name: &str) -> DatenLordResult<()>;

    /// Get attribute of i-node by ino
    async fn getattr(&self, ino: u64) -> DatenLordResult<(Duration, FuseAttr)>;

    /// Open a file or directory by ino and flags
    async fn open(&self, context: ReqContext, ino: u64, flags: u32) -> DatenLordResult<RawFd>;

    /// Forget a i-node by ino
    async fn forget(&self, ino: u64, nlookup: u64) -> DatenLordResult<()>;

    /// Helper function to read data
    async fn read_helper(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
    ) -> DatenLordResult<Vec<IoMemBlock>>;

    /// Helper function to flush node by ino
    async fn flush(&self, ino: u64, fh: u64) -> DatenLordResult<()>;

    /// Helper function to release dir
    async fn releasedir(&self, ino: u64, fh: u64) -> DatenLordResult<()>;

    /// Statfs helper
    async fn statfs(&self, context: ReqContext, ino: u64) -> DatenLordResult<StatFsParam>;

    /// Helper function to readlink
    async fn readlink(&self, ino: u64) -> DatenLordResult<Vec<u8>>;

    /// Helper function to opendir
    async fn opendir(&self, context: ReqContext, ino: u64, flags: u32) -> DatenLordResult<RawFd>;

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
