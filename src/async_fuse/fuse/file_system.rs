//! The `FileSystem` trait

use super::fuse_reply::{
    ReplyAttr, ReplyBMap, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyLock, ReplyOpen, ReplyStatFs, ReplyWrite, ReplyXAttr,
};
use super::fuse_request::Request;
use super::protocol::INum;

use crate::async_fuse::memfs::{FileLockParam, RenameParam, SetAttrParam};
use std::os::unix::io::RawFd;

use std::path::Path;

use async_trait::async_trait;
use tokio::task::JoinHandle;

/// FUSE filesystem trait
#[async_trait]
pub trait FileSystem {
    /// Initialize filesystem
    async fn init(&self, req: &Request<'_>) -> nix::Result<()>;

    /// Clean up filesystem
    async fn destroy(&self, req: &Request<'_>);

    /// Interrupt another FUSE request
    async fn interrupt(&self, req: &Request<'_>, unique: u64);

    /// Look up a directory entry by name and get its attributes.
    async fn lookup(
        &self,
        req: &Request<'_>,
        parent: INum,
        name: &str,
        reply: ReplyEntry,
    ) -> nix::Result<usize>;

    /// Forget about an inode
    async fn forget(&self, req: &Request<'_>, nlookup: u64);

    /// Get file attributes.
    async fn getattr(&self, req: &Request<'_>, reply: ReplyAttr) -> nix::Result<usize>;

    /// Set file attributes.
    async fn setattr(
        &self,
        req: &Request<'_>,
        param: SetAttrParam,
        reply: ReplyAttr,
    ) -> nix::Result<usize>;

    /// Read symbolic link.
    async fn readlink(&self, req: &Request<'_>, reply: ReplyData) -> nix::Result<usize>;

    /// Create file node.
    async fn mknod(
        &self,
        req: &Request<'_>,
        parent: INum,
        name: &str,
        mode: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) -> nix::Result<usize>;

    /// Create a directory
    async fn mkdir(
        &self,
        req: &Request<'_>,
        parent: INum,
        name: &str,
        mode: u32,
        reply: ReplyEntry,
    ) -> nix::Result<usize>;

    /// Remove a file
    async fn unlink(
        &self,
        req: &Request<'_>,
        parent: INum,
        name: &str,
        reply: ReplyEmpty,
    ) -> nix::Result<usize>;

    /// Remove a directory
    async fn rmdir(
        &self,
        req: &Request<'_>,
        parent: INum,
        name: &str,
        reply: ReplyEmpty,
    ) -> nix::Result<usize>;

    /// Create a symbolic link
    async fn symlink(
        &self,
        req: &Request<'_>,
        parent: INum,
        name: &str,
        target_path: &Path,
        reply: ReplyEntry,
    ) -> nix::Result<usize>;

    /// Rename a file
    async fn rename(
        &self,
        req: &Request<'_>,
        param: RenameParam,
        reply: ReplyEmpty,
    ) -> nix::Result<usize>;

    /// Create a hard link
    async fn link(
        &self,
        _req: &Request<'_>,
        _newparent: u64,
        _newname: &str,
        reply: ReplyEntry,
    ) -> nix::Result<usize>;

    /// Open a file
    async fn open(&self, req: &Request<'_>, flags: u32, reply: ReplyOpen) -> nix::Result<usize>;

    /// Read data
    async fn read(
        &self,
        req: &Request<'_>,
        fh: u64,
        offset: i64,
        size: u32,
        reply: ReplyData,
    ) -> nix::Result<usize>;

    /// Write data
    async fn write(
        &self,
        req: &Request<'_>,
        fh: u64,
        offset: i64,
        data: Vec<u8>,
        flags: u32,
        reply: ReplyWrite,
    ) -> nix::Result<usize>;

    /// Flush method
    async fn flush(
        &self,
        req: &Request<'_>,
        fh: u64,
        lock_owner: u64,
        reply: ReplyEmpty,
    ) -> nix::Result<usize>;

    /// Release an open file
    async fn release(
        &self,
        req: &Request<'_>,
        fh: u64,
        flags: u32, // same as the open flags
        lock_owner: u64,
        flush: bool,
        reply: ReplyEmpty,
    ) -> nix::Result<usize>;

    /// Synchronize file contents
    async fn fsync(
        &self,
        req: &Request<'_>,
        fh: u64,
        datasync: bool,
        reply: ReplyEmpty,
    ) -> nix::Result<usize>;

    /// Open a directory
    async fn opendir(&self, req: &Request<'_>, flags: u32, reply: ReplyOpen) -> nix::Result<usize>;

    /// Read directory
    async fn readdir(
        &self,
        req: &Request<'_>,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) -> nix::Result<usize>;

    /// Release an open directory
    async fn releasedir(
        &self,
        req: &Request<'_>,
        fh: u64,
        flags: u32,
        reply: ReplyEmpty,
    ) -> nix::Result<usize>;

    /// Synchronize directory contents
    async fn fsyncdir(
        &self,
        req: &Request<'_>,
        fh: u64,
        datasync: bool,
        reply: ReplyEmpty,
    ) -> nix::Result<usize>;

    /// Get file system statistics
    async fn statfs(&self, req: &Request<'_>, reply: ReplyStatFs) -> nix::Result<usize>;

    /// Set an extended attribute
    async fn setxattr(
        &self,
        _req: &Request<'_>,
        _name: &str,
        _value: &[u8],
        _flags: u32,
        _position: u32,
        reply: ReplyEmpty,
    ) -> nix::Result<usize>;

    /// Get an extended attribute
    async fn getxattr(
        &self,
        _req: &Request<'_>,
        _name: &str,
        _size: u32,
        reply: ReplyXAttr,
    ) -> nix::Result<usize>;

    /// Get an extended attribute
    async fn listxattr(
        &self,
        _req: &Request<'_>,
        _size: u32,
        reply: ReplyXAttr,
    ) -> nix::Result<usize>;

    /// Remove an extended attribute
    async fn removexattr(
        &self,
        _req: &Request<'_>,
        _name: &str,
        reply: ReplyEmpty,
    ) -> nix::Result<usize>;

    /// Check file access permissions
    async fn access(&self, _req: &Request<'_>, _mask: u32, reply: ReplyEmpty)
        -> nix::Result<usize>;

    /// Create and open a file
    async fn create(
        &self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &str,
        _mode: u32,
        _flags: u32,
        reply: ReplyCreate,
    ) -> nix::Result<usize>;

    /// Test for a POSIX file lock
    async fn getlk(
        &self,
        _req: &Request<'_>,
        _lk_param: FileLockParam,
        reply: ReplyLock,
    ) -> nix::Result<usize>;

    /// Acquire, modify or release a POSIX file lock
    async fn setlk(
        &self,
        _req: &Request<'_>,
        _lk_param: FileLockParam,
        _sleep: bool,
        reply: ReplyEmpty,
    ) -> nix::Result<usize>;

    /// Map block index within file to block index within device
    async fn bmap(
        &self,
        _req: &Request<'_>,
        _blocksize: u32,
        _idx: u64,
        reply: ReplyBMap,
    ) -> nix::Result<usize>;

    /// Set fuse fd into `FileSystem`
    async fn set_fuse_fd(&self, fuse_fd: RawFd);

    /// Stop all async tasks
    fn stop_all_async_tasks(&self);
}

/// create channel of communication from async task to main loop
pub(crate) fn new_fs_async_result_chan() -> (FsAsyncResultSender, FsAsyncResultReceiver) {
    tokio::sync::mpsc::channel(10)
}
/// result of fs async result
pub type FsAsyncResult = anyhow::Result<()>;
/// sender for async tasks to send msg(mainly refers to error) to session main loop
pub type FsAsyncResultSender = tokio::sync::mpsc::Sender<FsAsyncResult>;
/// receiver to receive msg from async tasks
pub(crate) type FsAsyncResultReceiver = tokio::sync::mpsc::Receiver<FsAsyncResult>;
/// Some state held by session to communicate with and control the fs.
#[allow(missing_debug_implementations)]
pub struct FsController {
    /// channel to receive async task msg
    _async_res_receiver: FsAsyncResultReceiver,
    /// all async task handles to join when session deref (the end of main loop)
    async_task_join_handles: Vec<JoinHandle<()>>,
}
impl FsController {
    /// new `FsUniqueController`
    ///  pass in the join handle and msg receiver,
    ///  which should be owned by main loop
    pub(crate) fn new(
        async_res_receiver: FsAsyncResultReceiver,
        async_task_join_handles: Vec<JoinHandle<()>>,
    ) -> FsController {
        FsController {
            _async_res_receiver:async_res_receiver,
            async_task_join_handles,
        }
    }
    /// before calling this, make sure just all task will break their loop.
    pub(crate) async fn join_all_async_tasks(&mut self) {
        while let Some(task) = self.async_task_join_handles.pop() {
            task.await
                .unwrap_or_else(|e| panic!("join async task error {e}"));
        }
    }
    // async read a result from async task
    // #[allow(dead_code)]
    // pub(crate) async fn recv_async_task_res(&mut self) -> anyhow::Result<()> {
    //     if let Some(res) = self.async_res_receiver.recv().await {
    //         res
    //     } else {
    //         // Only happens when channel sender destroyed,
    //         //  but channel sender destroyed when Session drop,
    //         //  so this only happens when there's a logic bug.
    //         panic!("fs async task channel was destroyed unexpectedly")
    //     }
    // }
}