//! The implementation of filesystem node

use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use nix::fcntl::OFlag;
use nix::sys::stat::{Mode, SFlag};
use parking_lot::RwLock;

use super::cache::{GlobalCache, IoMemBlock};
use super::direntry::DirEntry;
use super::fs_util::FileAttr;
use super::kv_engine::MetaTxn;
use super::CreateParam;
use crate::async_fuse::fuse::fuse_reply::StatFsParam;
use crate::async_fuse::fuse::protocol::INum;
use crate::common::error::DatenLordResult;

/// Fs node trait
#[async_trait]
pub trait Node: Sized {
    /// Get inode number
    fn get_ino(&self) -> INum;
    /// Set inode number
    fn set_ino(&mut self, ino: INum);
    /// Get fd
    fn get_fd(&self) -> RawFd;
    /// Get parent inode number
    fn get_parent_ino(&self) -> INum;
    /// Set parent inode number
    fn set_parent_ino(&mut self, parent: u64) -> INum;
    /// Get node name
    fn get_name(&self) -> &str;
    /// Set node name
    fn set_name(&mut self, name: &str);
    /// Get node type
    fn get_type(&self) -> SFlag;
    /// Get node attr
    fn get_attr(&self) -> FileAttr;
    /// Set node attr
    fn set_attr(&mut self, new_attr: FileAttr) -> FileAttr;
    /// Get node attr and increase lookup count
    fn lookup_attr(&self) -> FileAttr;
    /// Get node open count
    fn get_open_count(&self) -> i64;
    /// Decrease node open count
    fn dec_open_count(&self) -> i64;
    /// Get node lookup count
    fn get_lookup_count(&self) -> i64;
    /// Decrease node lookup count
    fn dec_lookup_count_by(&self, nlookup: u64) -> i64;
    /// Duplicate fd
    async fn dup_fd(&self, oflags: OFlag) -> DatenLordResult<RawFd>;
    /// check whether to load directory entry data or not
    fn need_load_dir_data(&self) -> bool;
    /// Check whether to load file content data or not
    async fn need_load_file_data(&self, offset: usize, len: usize) -> bool;
    /// Create symlink in a directory
    async fn create_child_symlink<T: MetaTxn + ?Sized>(
        &mut self,
        inum: INum,
        child_symlink_name: &str,
        target_path: PathBuf,
        txn: &mut T,
    ) -> DatenLordResult<Self>;
    /// Create sub-directory in a directory
    async fn create_child_dir<T: MetaTxn + ?Sized>(
        &mut self,
        inum: INum,
        child_dir_name: &str,
        mode: Mode,
        user_id: u32,
        group_id: u32,
        txn: &mut T,
    ) -> DatenLordResult<Self>;
    /// Open file in a directory
    async fn open_child_file(
        &self,
        child_file_name: &str,
        child_attr: Arc<RwLock<FileAttr>>,
        oflags: OFlag,
        global_cache: Arc<GlobalCache>,
    ) -> DatenLordResult<Self>;
    #[allow(clippy::too_many_arguments)]
    /// Create file in a directory
    async fn create_child_file<T: MetaTxn + ?Sized>(
        &mut self,
        inum: INum,
        child_file_name: &str,
        oflags: OFlag,
        mode: Mode,
        uid: u32,
        gid: u32,
        global_cache: Arc<GlobalCache>,
        txn: &mut T,
    ) -> DatenLordResult<Self>;
    /// Load data from directory, file or symlink target.
    async fn load_data(&self, offset: usize, len: usize) -> DatenLordResult<usize>;
    /// Unlink directory entry from both cache and disk
    async fn unlink_entry(&mut self, removed_entry: &DirEntry) -> DatenLordResult<()>;
    /// Get symlink target path
    fn get_symlink_target(&self) -> &Path;
    /// Get fs stat
    async fn statefs(&self) -> DatenLordResult<StatFsParam>;
    /// Get file data
    async fn get_file_data(&self, offset: usize, len: usize) -> Vec<IoMemBlock>;
    /// Write to file
    async fn write_file(
        &mut self,
        fh: u64,
        offset: i64,
        data: Vec<u8>,
        oflags: OFlag,
        write_to_disk: bool,
    ) -> DatenLordResult<usize>;
    /// Close file
    async fn close(&mut self);
    /// Close dir
    async fn closedir(&self);
    /// Mark as deferred deletion
    fn mark_deferred_deletion(&self);
    /// If node is marked as deferred deletion
    fn is_deferred_deletion(&self) -> bool;

    /// Create child node
    async fn create_child_node<T: MetaTxn + ?Sized>(
        &mut self,
        create_param: &CreateParam,
        new_inum: INum,
        global_cache: Arc<GlobalCache>,
        txn: &mut T,
    ) -> DatenLordResult<Self>;
}
