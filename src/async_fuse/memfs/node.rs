//! The implementation of filesystem node

use std::collections::{BTreeMap, VecDeque};
use std::os::unix::io::{FromRawFd, IntoRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::atomic::{self, AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::Context;
use async_trait::async_trait;
use clippy_utilities::{Cast, OverflowArithmetic};
use nix::errno::Errno;
use nix::fcntl::{self, FcntlArg, OFlag};
use nix::sys::stat::{self, Mode, SFlag};
use nix::sys::time::TimeSpec;
use nix::unistd;
use parking_lot::RwLock;
use tokio::sync::RwLockWriteGuard;
use tracing::debug;

use super::cache::{GlobalCache, IoMemBlock};
use super::dir::DirEntry;
use super::fs_util::{self, FileAttr};
use super::metadata::DefaultMetaData;
use super::SetAttrParam;
use crate::async_fuse::fuse::fuse_reply::{AsIoVec, StatFsParam};
use crate::async_fuse::fuse::protocol::INum;
use crate::async_fuse::metrics;
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
    /// Get node full path
    fn get_full_path(&self) -> &str;
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
    /// Flush node data
    async fn flush(&mut self, ino: INum, fh: u64);
    /// Duplicate fd
    async fn dup_fd(&self, oflags: OFlag) -> DatenLordResult<RawFd>;
    /// Check whether a node is an empty file or an empty directory
    fn is_node_data_empty(&self) -> bool;
    /// check whether to load directory entry data or not
    fn need_load_dir_data(&self) -> bool;
    /// Check whether to load file content data or not
    async fn need_load_file_data(&self, offset: usize, len: usize) -> bool;
    /// Get a directory entry by name
    fn get_entry(&self, name: &str) -> Option<&DirEntry>;
    /// Create symlink in a directory
    async fn create_child_symlink(
        &mut self,
        inum: INum,
        child_symlink_name: &str,
        target_path: PathBuf,
    ) -> DatenLordResult<Self>;
    /// Read symlink itself in a directory, not follow symlink
    async fn load_child_symlink(
        &self,
        child_symlink_name: &str,
        child_attr: Arc<RwLock<FileAttr>>,
    ) -> DatenLordResult<Self>;
    /// Open sub-directory in a directory
    async fn open_child_dir(
        &self,
        child_dir_name: &str,
        child_attr: Arc<RwLock<FileAttr>>,
    ) -> DatenLordResult<Self>;
    /// Create sub-directory in a directory
    async fn create_child_dir(
        &mut self,
        inum: INum,
        child_dir_name: &str,
        mode: Mode,
        user_id: u32,
        group_id: u32,
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
    async fn create_child_file(
        &mut self,
        inum: INum,
        child_file_name: &str,
        oflags: OFlag,
        mode: Mode,
        uid: u32,
        gid: u32,
        global_cache: Arc<GlobalCache>,
    ) -> DatenLordResult<Self>;
    /// Load data from directory, file or symlink target.
    async fn load_data(&mut self, offset: usize, len: usize) -> DatenLordResult<usize>;
    /// Insert directory entry for rename()
    fn insert_entry_for_rename(&mut self, child_entry: DirEntry) -> Option<DirEntry>;
    /// Remove directory entry from cache only for rename()
    fn remove_entry_for_rename(&mut self, child_name: &str) -> Option<DirEntry>;
    /// Unlink directory entry from both cache and disk
    async fn unlink_entry(&mut self, child_name: &str) -> DatenLordResult<DirEntry>;
    /// Read directory
    fn read_dir(&self, func: &mut dyn FnMut(&BTreeMap<String, DirEntry>) -> usize) -> usize;
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
    async fn close(&mut self, ino: INum, fh: u64, flush: bool);
    /// Close dir
    async fn closedir(&self, ino: INum, fh: u64);
    /// Precheck before set attr
    async fn setattr_precheck(
        &self,
        param: SetAttrParam,
        uid: u32,
        gid: u32,
    ) -> DatenLordResult<(bool, FileAttr)>;
    /// Mark as deferred deletion
    fn mark_deferred_deletion(&self);
    /// If node is marked as deferred deletion
    fn is_deferred_deletion(&self) -> bool;
}

/// A file node data or a directory node data
#[derive(Debug)]
pub enum DefaultNodeData {
    /// Directory entry data
    Directory(BTreeMap<String, DirEntry>),
    /// File content data
    RegFile(Arc<GlobalCache>),
    /// Symlink target data
    // SymLink(Box<SymLinkData>),
    SymLink(PathBuf),
}

/// A file node or a directory node
#[derive(Debug)]
pub struct DefaultNode {
    /// Parent node i-number
    parent: u64,
    /// DefaultNode name
    name: String,
    /// Full abstract path
    full_path: String,
    /// DefaultNode attribute
    attr: Arc<RwLock<FileAttr>>,
    /// DefaultNode data
    data: DefaultNodeData,
    /// DefaultNode fd
    fd: RawFd,
    /// DefaultNode open counter
    open_count: AtomicI64,
    /// DefaultNode lookup counter
    lookup_count: AtomicI64,
    /// If DefaultNode has been marked as deferred deletion
    deferred_deletion: AtomicBool,
    /// Shared metadata
    meta: Arc<DefaultMetaData>,
}

impl Drop for DefaultNode {
    fn drop(&mut self) {
        // TODO: check unsaved data in cache
        unistd::close(self.fd).unwrap_or_else(|err| {
            panic!(
                "DefaultNode::drop() failed to clode the file handler \
                    of the node name={:?} ino={}, the error is: {}",
                self.name,
                self.attr.read().ino,
                err,
            );
        });
    }
}

impl DefaultNode {
    /// Create `DefaultNode`
    fn new(
        parent: u64,
        name: &str,
        full_path: String,
        attr: Arc<RwLock<FileAttr>>,
        data: DefaultNodeData,
        fd: RawFd,
        meta: Arc<DefaultMetaData>,
    ) -> Self {
        Self {
            parent,
            name: name.to_owned(),
            full_path,
            attr,
            data,
            fd,
            // open count set to 0 by creation
            open_count: AtomicI64::new(0),
            // open count set to 1 by creation
            lookup_count: AtomicI64::new(1),
            deferred_deletion: AtomicBool::new(false),
            meta,
        }
    }

    /// Get full path
    fn full_path(&self) -> &str {
        self.full_path.as_ref()
    }

    /// Set full path
    fn set_full_path(&mut self, full_path: String) {
        self.full_path = full_path;
    }

    /// Update mtime and ctime to now
    fn update_mtime_ctime_to_now(&mut self) {
        let mut attr = self.get_attr();
        let st_now = SystemTime::now();
        attr.mtime = st_now;
        attr.ctime = st_now;
        self.set_attr(attr);
    }

    /// Increase node lookup count
    fn inc_lookup_count(&self) -> i64 {
        self.lookup_count.fetch_add(1, atomic::Ordering::Relaxed)
    }

    /// Helper function to check need to load node data or not
    fn need_load_node_data_helper(&self) -> bool {
        if !self.is_node_data_empty() {
            debug!(
                "need_load_node_data_helper() found node data of name={:?} \
                    and ino={} is in cache, no need to load",
                self.get_name(),
                self.get_ino(),
            );
            false
        } else if self.get_attr().size > 0 {
            debug!(
                "need_load_node_data_helper() found node size of name={:?} \
                    and ino={} is non-zero, need to load",
                self.get_name(),
                self.get_ino(),
            );
            true
        } else {
            debug!(
                "need_load_node_data_helper() found node size of name={:?} \
                    and ino={} is zero, no need to load",
                self.get_name(),
                self.get_ino(),
            );
            false
        }
    }

    /// Get directory data
    fn get_dir_data(&self) -> &BTreeMap<String, DirEntry> {
        match self.data {
            DefaultNodeData::Directory(ref dir_data) => dir_data,
            DefaultNodeData::RegFile(..) | DefaultNodeData::SymLink(..) => {
                panic!("forbidden to get DirData from non-directory node")
            }
        }
    }

    /// Get mutable directory data
    fn get_dir_data_mut(&mut self) -> &mut BTreeMap<String, DirEntry> {
        match self.data {
            DefaultNodeData::Directory(ref mut dir_data) => dir_data,
            DefaultNodeData::RegFile(..) | DefaultNodeData::SymLink(..) => {
                panic!("forbidden to get DirData from non-directory node")
            }
        }
    }

    /// Get child symlink fd of dir
    async fn get_child_symlink_fd(
        dir_fd: i32,
        child_symlink_name: &str,
    ) -> Result<i32, nix::Error> {
        #[cfg(target_os = "macos")]
        let open_res = {
            use std::os::unix::ffi::OsStrExt;
            let symlink_name_cstr =
                std::ffi::CString::new(child_symlink_name.as_os_str().as_bytes())?;
            let fd_res = tokio::task::spawn_blocking(|| unsafe {
                libc::openat(
                    dir_fd,
                    symlink_name_cstr.as_ptr(),
                    libc::O_SYMLINK | libc::O_NOFOLLOW,
                )
            })
            .await
            .unwrap_or_else(|e| {
                panic!("failed to join libc::openat task for error {}", e);
            });
            if 0 == fd_res {
                debug!(
                    "create_or_load_child_symlink_helper() successfully opened symlink={:?} itself",
                    child_symlink_name
                );
                Ok(fd_res)
            } else {
                crate::util::build_error_result_from_errno(
                    nix::errno::Errno::last(),
                    format!("failed to open symlink={:?} itself", child_symlink_name,),
                )
            }
        };

        let child_symlink_name_string = child_symlink_name.to_owned();
        tokio::task::spawn_blocking(move || {
            fcntl::openat(
                dir_fd,
                child_symlink_name_string.as_str(),
                OFlag::O_PATH | OFlag::O_NOFOLLOW,
                Mode::all(),
            )
        })
        .await
        .unwrap_or_else(|e| {
            panic!("failed to join fcntl::openat task for error {e}");
        })
    }

    /// Helper function to create or read symlink itself in a directory
    #[cfg(target_os = "macos")]
    async fn create_or_load_child_symlink_helper(
        &mut self,
        child_symlink_name: &str,
        target_path_opt: Option<PathBuf>, // If not None, create symlink
    ) -> DatenLordResult<Self> {
        let ino = self.get_ino();
        let fd = self.fd;
        let dir_data = self.get_dir_data_mut();
        if let Some(ref target_path) = target_path_opt {
            debug_assert!(
                !dir_data.contains_key(child_symlink_name),
                "create_or_load_child_symlink_helper() cannot create duplicated symlink name={:?}",
                child_symlink_name,
            );
            let child_symlink_name_string = child_symlink_name.to_string();
            let target_path_clone = target_path.clone();
            tokio::task::spawn_blocking(move || {
                unistd::symlinkat(
                    &target_path_clone,
                    Some(fd),
                    child_symlink_name_string.as_str(),
                )
            })
            .await?
            .context(format!(
                "create_or_load_child_symlink_helper() failed to create symlink \
                    name={:?} to target path={:?} under parent ino={}",
                child_symlink_name, target_path, ino,
            ))?;
        };

        let child_fd = Self::get_child_symlink_fd(fd, child_symlink_name)
            .await
            .context(format!(
            "create_or_load_child_symlink_helper() failed to open symlink itself with name={:?} \
                under parent ino={}",
            child_symlink_name, ino,
        ))?;
        let child_attr = fs_util::load_attr(child_fd)
            // let child_attr = util::load_symlink_attr(fd, child_symlink_name.clone())
            .await
            .context(format!(
                "create_or_load_child_symlink_helper() failed to get the attribute of the new symlink={:?}",
                child_symlink_name,
            ))?;
        debug_assert_eq!(SFlag::S_IFLNK, child_attr.kind);

        let target_path = if let Some(target_path) = target_path_opt {
            // insert new entry to parent directory
            // TODO: support thread-safe
            let previous_value = dir_data.insert(
                child_symlink_name.to_string(),
                DirEntry::new(
                    child_attr.ino,
                    child_symlink_name.to_string(),
                    SFlag::S_IFLNK,
                ),
            );
            debug_assert!(previous_value.is_none()); // double check creation race
            target_path
        } else {
            let child_symlink_name_string = child_symlink_name.to_string();
            let target_path_osstr = tokio::task::spawn_blocking(move || {
                fcntl::readlinkat(fd, child_symlink_name_string.as_str())
            })
            .await?
            .context(format!(
                "create_or_load_child_symlink_helper() failed to open \
                            the new directory name={:?} under parent ino={}",
                child_symlink_name, ino,
            ))?;
            Path::new(&target_path_osstr).to_owned()
        };

        let mut full_path = self.full_path().to_owned();
        full_path.push_str(child_symlink_name);

        Ok(Self::new(
            self.get_ino(),
            child_symlink_name,
            full_path,
            child_attr,
            // DefaultNodeData::SymLink(Box::new(SymLinkData::new(child_fd, target_path).await)),
            DefaultNodeData::SymLink(target_path),
            child_fd,
            Arc::clone(&self.meta),
        ))
    }

    /// Increase node open count
    fn inc_open_count(&self) -> i64 {
        self.open_count.fetch_add(1, atomic::Ordering::Relaxed)
    }

    /// Open root node
    pub(crate) async fn open_root_node(
        root_ino: INum,
        name: &str,
        path: &str,
        meta: Arc<DefaultMetaData>,
    ) -> DatenLordResult<Self> {
        let dir_fd = fs_util::open_dir(Path::new(path)).await?;
        let mut attr = fs_util::load_attr(dir_fd).await?;
        attr.ino = root_ino; // replace root ino with 1

        let root_node = Self::new(
            root_ino,
            name,
            "/".to_owned(),
            Arc::new(RwLock::new(attr)),
            DefaultNodeData::Directory(BTreeMap::new()),
            dir_fd,
            meta,
        );
        // // load root directory data on open
        // root_node
        //     .load_data()
        //     .await
        //     .context("open_root_node() failed to load root directory entry data")?;

        Ok(root_node)
    }
}

#[async_trait]
impl Node for DefaultNode {
    /// Get node i-number
    #[inline]
    fn get_ino(&self) -> INum {
        self.get_attr().ino
    }

    /// Set node i-number
    #[inline]
    fn set_ino(&mut self, ino: INum) {
        self.attr.write().ino = ino;
    }

    /// Get node fd
    #[inline]
    fn get_fd(&self) -> RawFd {
        self.fd
    }

    /// Get parent node i-number
    #[inline]
    fn get_parent_ino(&self) -> INum {
        self.parent
    }

    /// Set node parent
    fn set_parent_ino(&mut self, parent: u64) -> INum {
        let old_parent = self.parent;
        self.parent = parent;
        old_parent
    }

    /// Get node name
    #[inline]
    fn get_name(&self) -> &str {
        self.name.as_str()
    }

    /// Get node full path
    #[inline]
    fn get_full_path(&self) -> &str {
        self.full_path.as_str()
    }

    /// Set node name
    #[inline]
    fn set_name(&mut self, name: &str) {
        self.name = name.to_owned();
    }

    /// Get node type, directory or file
    fn get_type(&self) -> SFlag {
        match self.data {
            DefaultNodeData::Directory(..) => SFlag::S_IFDIR,
            DefaultNodeData::RegFile(..) => SFlag::S_IFREG,
            DefaultNodeData::SymLink(..) => SFlag::S_IFLNK,
        }
    }

    /// Get node attribute
    #[inline]
    fn get_attr(&self) -> FileAttr {
        *self.attr.read()
    }

    /// Set node attribute
    fn set_attr(&mut self, new_attr: FileAttr) -> FileAttr {
        let old_attr = self.get_attr();
        match self.data {
            DefaultNodeData::Directory(..) => debug_assert_eq!(new_attr.kind, SFlag::S_IFDIR),
            DefaultNodeData::RegFile(..) => debug_assert_eq!(new_attr.kind, SFlag::S_IFREG),
            DefaultNodeData::SymLink(..) => debug_assert_eq!(new_attr.kind, SFlag::S_IFLNK),
        }
        *self.attr.write() = new_attr;
        old_attr
    }

    /// Get node attribute and increase lookup count
    fn lookup_attr(&self) -> FileAttr {
        let attr = self.get_attr();
        self.inc_lookup_count();
        attr
    }

    /// Get node open count
    fn get_open_count(&self) -> i64 {
        self.open_count.load(atomic::Ordering::Relaxed)
    }

    /// Decrease node open count
    fn dec_open_count(&self) -> i64 {
        self.open_count.fetch_sub(1, atomic::Ordering::Relaxed)
    }

    /// Get node lookup count
    fn get_lookup_count(&self) -> i64 {
        self.lookup_count.load(atomic::Ordering::Relaxed)
    }

    /// Decrease node lookup count
    fn dec_lookup_count_by(&self, nlookup: u64) -> i64 {
        debug_assert!(nlookup < std::i64::MAX.cast());
        self.lookup_count
            .fetch_sub(nlookup.cast(), atomic::Ordering::Relaxed)
    }

    /// Mark node as deferred deletion
    fn mark_deferred_deletion(&self) {
        self.deferred_deletion.store(true, Ordering::Relaxed);
    }

    /// If node is marked as deferred deletion
    fn is_deferred_deletion(&self) -> bool {
        self.deferred_deletion.load(Ordering::Relaxed)
    }

    /// flush node data
    async fn flush(&mut self, ino: INum, fh: u64) {
        if self.is_deferred_deletion() {
            return;
        }
        let new_fd = tokio::task::spawn_blocking(move || unistd::dup(fh.cast()))
            .await
            .unwrap_or_else(|e| {
                panic!("failed to join unistd::dup task for error {e}");
            })
            .context(format!(
                "flush() failed to duplicate the handler ino={ino} fh={fh:?}",
            ))
            .unwrap_or_else(|e| {
                panic!(
                    "flush() failed, the error is: {}",
                    crate::common::util::format_anyhow_error(&e)
                )
            });
        tokio::task::spawn_blocking(move || unistd::close(new_fd))
            .await
            .unwrap_or_else(|e| {
                panic!("failed to join unistd::close task for error {e}");
            })
            .context(format!(
                "flush() failed to close the duplicated file handler={new_fd} of ino={ino}",
            ))
            .unwrap_or_else(|e| {
                panic!(
                    "flush() failed, the error is: {}",
                    crate::common::util::format_anyhow_error(&e)
                )
            });
    }

    /// Duplicate fd
    async fn dup_fd(&self, oflags: OFlag) -> DatenLordResult<RawFd> {
        let raw_fd = self.fd;
        let ino = self.get_ino();
        let new_fd = tokio::task::spawn_blocking(move || unistd::dup(raw_fd))
            .await?
            .context(format!(
                "dup_fd() failed to duplicate the handler ino={ino} raw fd={raw_fd:?}",
            ))?;
        // increase open count once dup() success
        self.inc_open_count();

        let fcntl_oflags = FcntlArg::F_SETFL(oflags);
        tokio::task::spawn_blocking(move || fcntl::fcntl(new_fd, fcntl_oflags))
            .await?
            .context(format!(
                "dup_fd() failed to set the flags={oflags:?} of duplicated handler of ino={ino}",
            ))
            .unwrap_or_else(|err| {
                panic!(
                    "failed to duplicate fd, the error is: {}",
                    crate::common::util::format_anyhow_error(&err),
                )
            });
        // blocking!(unistd::dup3(raw_fd, new_fd, oflags)).context(format!(
        //     "dup_fd() failed to set the flags={:?} of duplicated handler of ino={}",
        //     oflags, ino,
        // ))?;
        Ok(new_fd)
    }

    /// Check whether a node is an empty file or an empty directory
    fn is_node_data_empty(&self) -> bool {
        match self.data {
            DefaultNodeData::Directory(ref dir_node) => dir_node.is_empty(),
            DefaultNodeData::RegFile(..) => true, // always check the cache
            DefaultNodeData::SymLink(..) => panic!("forbidden to check symlink is empty or not"),
        }
    }

    /// check whether to load directory entry data or not
    fn need_load_dir_data(&self) -> bool {
        debug_assert_eq!(
            self.attr.read().kind,
            SFlag::S_IFDIR,
            "fobidden to check non-directory node need load data or not",
        );
        self.need_load_node_data_helper()
    }

    /// Check whether to load file content data or not
    async fn need_load_file_data(&self, offset: usize, len: usize) -> bool {
        debug_assert_eq!(
            self.attr.read().kind,
            SFlag::S_IFREG,
            "fobidden to check non-file node need load data or not",
        );

        if offset >= self.attr.read().size.cast() {
            return false;
        }

        let need_load = match self.data {
            DefaultNodeData::RegFile(ref cache) => {
                let file_cache = cache.get_file_cache(self.get_ino(), offset, len);
                let cache_miss = file_cache.is_empty()
                    || file_cache.iter().filter(|b| !(*b).can_convert()).count() != 0;
                if cache_miss {
                    metrics::CACHE_MISSES.inc();
                } else {
                    metrics::CACHE_HITS.inc();
                }
                cache_miss
            }
            DefaultNodeData::Directory(..) | DefaultNodeData::SymLink(..) => {
                panic!("need_load_file_data should handle regular file")
            }
        };

        need_load
    }

    /// Get a directory entry by name
    fn get_entry(&self, name: &str) -> Option<&DirEntry> {
        self.get_dir_data().get(name)
    }

    /// Create symlink in a directory
    async fn create_child_symlink(
        &mut self,
        _inum: INum,
        child_symlink_name: &str,
        target_path: PathBuf,
    ) -> DatenLordResult<Self> {
        let ino = self.get_ino();
        let fd = self.fd;
        let dir_data = self.get_dir_data_mut();
        debug_assert!(
            !dir_data.contains_key(child_symlink_name),
            "create_child_symlink() cannot create duplicated symlink name={child_symlink_name:?}",
        );
        let child_symlink_name_string = child_symlink_name.to_owned();
        let target_path_clone = target_path.clone();
        tokio::task::spawn_blocking(move || {
            unistd::symlinkat(
                &target_path_clone,
                Some(fd),
                child_symlink_name_string.as_str(),
            )
        })
        .await?
        .context(format!(
            "create_child_symlink() failed to create symlink \
                    name={child_symlink_name:?} to target path={target_path:?} under parent ino={ino}",
        ))?;

        let child_fd = Self::get_child_symlink_fd(fd, child_symlink_name)
            .await
            .context(format!(
                "create_child_symlink() failed to open symlink itself with name={child_symlink_name:?} \
                under parent ino={ino}",
            ))?;
        let child_attr = {
            let child_attr = fs_util::load_attr(child_fd)
            // let child_attr = util::load_symlink_attr(fd, child_symlink_name.clone())
            .await
            .context(format!(
                "create_child_symlink() failed to get the attribute of the new symlink={child_symlink_name:?}",
            ))?;
            debug_assert_eq!(SFlag::S_IFLNK, child_attr.kind);
            Arc::new(RwLock::new(child_attr))
        };

        let target_path = {
            // insert new entry to parent directory
            // TODO: support thread-safe
            let previous_value = dir_data.insert(
                child_symlink_name.to_owned(),
                DirEntry::new(child_symlink_name.to_owned(), Arc::clone(&child_attr)),
            );
            debug_assert!(previous_value.is_none()); // double check creation race
            target_path
        };
        let mut full_path = self.full_path().to_owned();
        full_path.push_str(child_symlink_name);

        self.update_mtime_ctime_to_now();
        Ok(Self::new(
            self.get_ino(),
            child_symlink_name,
            full_path,
            child_attr,
            // DefaultNodeData::SymLink(Box::new(SymLinkData::new(child_fd, target_path).await)),
            DefaultNodeData::SymLink(target_path),
            child_fd,
            Arc::clone(&self.meta),
        ))
    }

    /// Read symlink itself in a directory, not follow symlink
    async fn load_child_symlink(
        &self,
        child_symlink_name: &str,
        child_attr: Arc<RwLock<FileAttr>>,
    ) -> DatenLordResult<Self> {
        let ino = self.get_ino();
        let fd = self.fd;

        let child_fd = Self::get_child_symlink_fd(fd, child_symlink_name)
            .await
            .context(format!(
            "load_child_symlink() failed to open symlink itself with name={child_symlink_name:?} \
                under parent ino={ino}",
        ))?;
        *child_attr.write() = {
            let child_attr = fs_util::load_attr(child_fd)
                // let child_attr = util::load_symlink_attr(fd, child_symlink_name.clone())
                .await
                .context(format!(
                    "load_child_symlink() failed to get the attribute of the new symlink={child_symlink_name:?}",
                ))?;
            debug_assert_eq!(SFlag::S_IFLNK, child_attr.kind);
            child_attr
        };

        let target_path = {
            let child_symlink_name_string = child_symlink_name.to_owned();
            let target_path_osstr = tokio::task::spawn_blocking(move || {
                fcntl::readlinkat(fd, child_symlink_name_string.as_str())
            })
            .await?
            .context(format!(
                "load_child_symlink() failed to open \
                            the new directory name={child_symlink_name:?} under parent ino={ino}",
            ))?;
            Path::new(&target_path_osstr).to_owned()
        };

        let mut full_path = self.full_path().to_owned();
        full_path.push_str(child_symlink_name);

        Ok(Self::new(
            self.get_ino(),
            child_symlink_name,
            full_path,
            child_attr,
            // DefaultNodeData::SymLink(Box::new(SymLinkData::new(child_fd, target_path).await)),
            DefaultNodeData::SymLink(target_path),
            child_fd,
            Arc::clone(&self.meta),
        ))
    }

    /// Open sub-directory in a directory
    async fn open_child_dir(
        &self,
        child_dir_name: &str,
        child_attr: Arc<RwLock<FileAttr>>,
    ) -> DatenLordResult<Self> {
        let ino = self.get_ino();
        let fd = self.fd;

        let child_raw_fd = fs_util::open_dir_at(fd, child_dir_name)
            .await
            .context(format!(
                "open_child_dir() failed to open the new directory name={child_dir_name:?} \
                    under parent ino={ino}",
            ))?;

        *child_attr.write() = {
            // get new directory attribute
            let child_attr = fs_util::load_attr(child_raw_fd).await.context(format!(
                "open_child_dir() failed to get the attribute of the new child directory={child_dir_name:?}",
            ))?;
            debug_assert_eq!(SFlag::S_IFDIR, child_attr.kind);
            child_attr
        };

        let mut full_path = self.full_path().to_owned();
        full_path.push_str(child_dir_name);
        full_path.push('/');

        // lookup count and open count are increased to 1 by creation
        let child_node = Self::new(
            self.get_ino(),
            child_dir_name,
            full_path,
            child_attr,
            DefaultNodeData::Directory(BTreeMap::new()),
            child_raw_fd,
            Arc::clone(&self.meta),
        );

        Ok(child_node)
    }

    /// Create sub-directory in a directory
    async fn create_child_dir(
        &mut self,
        _inum: INum,
        child_dir_name: &str,
        mode: Mode,
        _uid: u32,
        _gid: u32,
    ) -> DatenLordResult<Self> {
        let ino = self.get_ino();
        let fd = self.fd;
        let dir_data = self.get_dir_data_mut();
        debug_assert!(
            !dir_data.contains_key(child_dir_name),
            "open_child_dir_helper() cannot create duplicated directory name={child_dir_name:?}"
        );
        let child_dir_name_string = child_dir_name.to_owned();
        tokio::task::spawn_blocking(move || {
            stat::mkdirat(fd, child_dir_name_string.as_str(), mode)
        })
        .await?
        .context(format!(
            "open_child_dir_helper() failed to create directory \
                        name={child_dir_name:?} under parent ino={ino}",
        ))?;

        let child_raw_fd = fs_util::open_dir_at(fd, child_dir_name)
            .await
            .context(format!(
                "open_child_dir_helper() failed to open the new directory name={child_dir_name:?} \
                    under parent ino={ino}",
            ))?;

        let child_attr = {
            // get new directory attribute
            let child_attr = fs_util::load_attr(child_raw_fd).await.context(format!(
                "open_child_dir_helper() failed to get the attribute of the new child directory={child_dir_name:?}",
            ))?;
            debug_assert_eq!(SFlag::S_IFDIR, child_attr.kind);
            Arc::new(RwLock::new(child_attr))
        };

        // insert new entry to parent directory
        // TODO: support thread-safe
        let previous_value = dir_data.insert(
            child_dir_name.to_owned(),
            DirEntry::new(child_dir_name.to_owned(), Arc::clone(&child_attr)),
        );
        debug_assert!(previous_value.is_none()); // double check creation race

        let mut full_path = self.full_path().to_owned();
        full_path.push_str(child_dir_name);
        full_path.push('/');

        // lookup count and open count are increased to 1 by creation
        let child_node = Self::new(
            self.get_ino(),
            child_dir_name,
            full_path,
            child_attr,
            DefaultNodeData::Directory(BTreeMap::new()),
            child_raw_fd,
            Arc::clone(&self.meta),
        );

        self.update_mtime_ctime_to_now();
        Ok(child_node)
    }

    /// Open file in a directory
    async fn open_child_file(
        &self,
        child_file_name: &str,
        child_attr: Arc<RwLock<FileAttr>>,
        oflags: OFlag,
        global_cache: Arc<GlobalCache>,
    ) -> DatenLordResult<Self> {
        let ino = self.get_ino();
        let fd = self.fd;
        let child_file_name_string = child_file_name.to_owned();
        let mode = Mode::empty();
        let child_fd = tokio::task::spawn_blocking(move || {
            fcntl::openat(fd, child_file_name_string.as_str(), oflags, mode)
        })
        .await?
        .context(format!(
            "open_child_file() failed to open a file name={child_file_name:?} \
                under parent ino={ino} with oflags={oflags:?} and mode={mode:?}",
        ))?;

        {
            // get new file attribute
            let child_attr_ = fs_util::load_attr(child_fd)
                .await
                .context("open_child_file() failed to get the attribute of the new child")?;
            debug_assert_eq!(SFlag::S_IFREG, child_attr_.kind);
            child_attr.write().clone_from(&child_attr_);
        };

        let mut full_path = self.full_path().to_owned();
        full_path.push_str(child_file_name);

        Ok(Self::new(
            self.get_ino(),
            child_file_name,
            full_path,
            child_attr,
            DefaultNodeData::RegFile(global_cache),
            child_fd,
            Arc::clone(&self.meta),
        ))
    }

    /// Create file in a directory
    async fn create_child_file(
        &mut self,
        _inum: INum,
        child_file_name: &str,
        oflags: OFlag,
        mode: Mode,
        _uid: u32,
        _gid: u32,
        global_cache: Arc<GlobalCache>,
    ) -> DatenLordResult<Self> {
        let ino = self.get_ino();
        let fd = self.fd;
        let dir_data = self.get_dir_data_mut();
        debug_assert!(
            !dir_data.contains_key(child_file_name),
            "create_child_file() cannot create duplicated file name={child_file_name:?}"
        );
        debug_assert!(oflags.contains(OFlag::O_CREAT));
        let child_file_name_string = child_file_name.to_owned();
        let child_fd = tokio::task::spawn_blocking(move || {
            fcntl::openat(fd, child_file_name_string.as_str(), oflags, mode)
        })
        .await?
        .context(format!(
            "create_child_file() failed to open a file name={child_file_name:?} \
                under parent ino={ino} with oflags={oflags:?} and mode={mode:?}",
        ))?;

        let child_attr = {
            // get new file attribute
            let child_attr = fs_util::load_attr(child_fd)
                .await
                .context("create_child_file() failed to get the attribute of the new child")?;
            debug_assert_eq!(SFlag::S_IFREG, child_attr.kind);
            Arc::new(RwLock::new(child_attr))
        };

        // insert new entry to parent directory
        // TODO: support thread-safe
        let previous_value = dir_data.insert(
            child_file_name.to_owned(),
            DirEntry::new(child_file_name.to_owned(), Arc::clone(&child_attr)),
        );
        debug_assert!(previous_value.is_none()); // double check creation race

        let mut full_path = self.full_path().to_owned();
        full_path.push_str(child_file_name);

        self.update_mtime_ctime_to_now();
        Ok(Self::new(
            self.get_ino(),
            child_file_name,
            full_path,
            child_attr,
            DefaultNodeData::RegFile(global_cache),
            child_fd,
            Arc::clone(&self.meta),
        ))
    }

    /// Load data from directory, file or symlink target.
    /// The `offset` and `len` is used for regular file
    async fn load_data(&mut self, offset: usize, len: usize) -> DatenLordResult<usize> {
        match self.data {
            DefaultNodeData::Directory(..) => {
                // let dir_entry_map = self.load_dir_data_helper().await?;
                let dir_entry_map = fs_util::load_dir_data(self.get_fd())
                    .await
                    .context("load_data() failed to load directory entry data")?;
                let entry_count = dir_entry_map.len();
                self.data = DefaultNodeData::Directory(dir_entry_map);
                debug!(
                    "load_data() successfully load {} directory entries",
                    entry_count
                );
                Ok(entry_count)
            }
            DefaultNodeData::RegFile(ref global_cache) => {
                let aligned_offset = global_cache.round_down(offset);
                let new_len_tmp =
                    global_cache.round_up(offset.overflow_sub(aligned_offset).overflow_add(len));

                let new_len =
                    if new_len_tmp.overflow_add(aligned_offset) > self.attr.read().size.cast() {
                        self.attr
                            .read()
                            .size
                            .cast::<usize>()
                            .overflow_sub(aligned_offset)
                    } else {
                        new_len_tmp
                    };

                let file_data_vec = fs_util::load_file_data(self.get_fd(), aligned_offset, new_len)
                    .await
                    .context("load_data() failed to load file content data")?;
                let read_size = file_data_vec.len();
                debug!(
                    "load_data() successfully load {} byte file content data",
                    read_size
                );

                global_cache
                    .write_or_update(
                        self.get_ino(),
                        aligned_offset,
                        read_size,
                        &file_data_vec,
                        false,
                    )
                    .await;

                Ok(read_size)
            }
            DefaultNodeData::SymLink(..) => {
                panic!("forbidden to load symlink target data");
            }
        }
    }

    /// Insert directory entry for rename()
    fn insert_entry_for_rename(&mut self, child_entry: DirEntry) -> Option<DirEntry> {
        let dir_data = self.get_dir_data_mut();
        let previous_entry = dir_data.insert(child_entry.entry_name().into(), child_entry);
        self.update_mtime_ctime_to_now();
        debug!(
            "insert_entry_for_rename() successfully inserted new entry \
                and replaced previous entry={:?}",
            previous_entry,
        );

        previous_entry
    }

    /// Remove directory entry from cache only for rename()
    fn remove_entry_for_rename(&mut self, child_name: &str) -> Option<DirEntry> {
        let dir_data = self.get_dir_data_mut();
        let remove_res = dir_data.remove(child_name);
        if remove_res.is_some() {
            self.update_mtime_ctime_to_now();
        }
        remove_res
    }

    /// Unlink directory entry from both cache and disk
    async fn unlink_entry(&mut self, child_name: &str) -> DatenLordResult<DirEntry> {
        let dir_data = self.get_dir_data_mut();
        let removed_entry = dir_data.remove(child_name).unwrap_or_else(|| {
            panic!(
                "unlink_entry() found fs is inconsistent, the entry of name={:?} \
                    is not in directory of name={:?} and ino={}",
                child_name,
                self.get_name(),
                self.get_ino(),
            );
        });
        let child_name_string = child_name.to_owned();
        let fd = self.fd;
        // delete from disk and close the handler
        match removed_entry.entry_type() {
            SFlag::S_IFDIR => {
                tokio::task::spawn_blocking(move || {
                    unistd::unlinkat(
                        Some(fd),
                        child_name_string.as_str(),
                        unistd::UnlinkatFlags::RemoveDir,
                    )
                })
                .await?
                .context(format!(
                    "unlink_entry() failed to delete the file name={child_name:?} from disk",
                ))?;
            }
            SFlag::S_IFREG | SFlag::S_IFLNK => {
                tokio::task::spawn_blocking(move || {
                    unistd::unlinkat(
                        Some(fd),
                        child_name_string.as_str(),
                        unistd::UnlinkatFlags::NoRemoveDir,
                    )
                })
                .await?
                .context(format!(
                    "unlink_entry() failed to delete the file name={child_name:?} from disk",
                ))?;
            }
            _ => panic!(
                "unlink_entry() found unsupported entry type={:?}",
                removed_entry.entry_type()
            ),
        }
        self.update_mtime_ctime_to_now();
        Ok(removed_entry)
    }

    /// Read directory
    fn read_dir(&self, func: &mut dyn FnMut(&BTreeMap<String, DirEntry>) -> usize) -> usize {
        let dir_data = self.get_dir_data();
        func(dir_data)
    }

    /// Get symlink target path
    fn get_symlink_target(&self) -> &Path {
        match self.data {
            DefaultNodeData::Directory(..) | DefaultNodeData::RegFile(..) => {
                panic!("forbidden to read target path from non-symlink node")
            }
            // DefaultNodeData::SymLink(symlink_data) => &symlink_data.target_path,
            DefaultNodeData::SymLink(ref target_path) => target_path,
        }
    }

    /// Get fs stat
    async fn statefs(&self) -> DatenLordResult<StatFsParam> {
        let fd = self.fd;
        tokio::task::spawn_blocking(move || {
            let file = unsafe { std::fs::File::from_raw_fd(fd) };
            let statvfs = nix::sys::statvfs::fstatvfs(&file); // statvfs is POSIX, whereas statfs is not
            let _fd = file.into_raw_fd(); // prevent fd to be closed by File
            statvfs
        })
        .await?
        .map(|statvfs| {
            StatFsParam {
                blocks: statvfs.blocks().cast(),
                bfree: statvfs.blocks_free().cast(),
                bavail: statvfs.blocks_available().cast(),
                files: statvfs.files().cast(),
                f_free: statvfs.files_free().cast(),
                bsize: statvfs.block_size().cast(), // TODO: consider use customized block size
                namelen: statvfs.name_max().cast(),
                frsize: statvfs.fragment_size().cast(),
            }
        })
        .map_err(Errno::into)
    }

    /// Get file data
    async fn get_file_data(&self, offset: usize, len: usize) -> Vec<IoMemBlock> {
        match self.data {
            DefaultNodeData::Directory(..) | DefaultNodeData::SymLink(..) => {
                panic!("forbidden to load FileData from non-file node")
            }
            DefaultNodeData::RegFile(ref cache) => {
                cache.get_file_cache(self.get_ino(), offset, len)
            }
        }
    }

    /// Write to file
    async fn write_file(
        &mut self,
        fh: u64,
        offset: i64,
        data: Vec<u8>,
        oflags: OFlag,
        write_to_disk: bool,
    ) -> DatenLordResult<usize> {
        let this: &Self = self;

        let ino = this.get_ino();
        if this.need_load_file_data(offset.cast(), data.len()).await {
            let load_res = self.load_data(offset.cast(), data.len()).await;
            if let Err(e) = load_res {
                debug!(
                    "read() failed to load file data of ino={} and name={:?}, the error is: {}",
                    ino,
                    self.get_name(),
                    e,
                );
                return Err(e);
            }
        }

        let cache = match self.data {
            DefaultNodeData::Directory(..) | DefaultNodeData::SymLink(..) => {
                panic!("forbidden to load FileData from non-file node")
            }
            DefaultNodeData::RegFile(ref file_data) => file_data,
        };

        cache
            .write_or_update(
                self.get_ino(),
                offset.cast(),
                data.len(),
                data.as_slice(),
                true,
            )
            .await;

        let fcntl_oflags = fcntl::FcntlArg::F_SETFL(oflags);
        let fd = fh.cast();
        fcntl::fcntl(fd, fcntl_oflags).context(format!(
            "write_file() failed to set the flags={oflags:?} to file handler={fd} of ino={ino}",
        ))?;
        let mut written_size = data.len();
        if write_to_disk {
            let data_len = data.len();
            written_size =
                tokio::task::spawn_blocking(move || nix::sys::uio::pwrite(fd, &data, offset))
                    .await?
                    .context("write_file() failed to write to disk")?;
            debug_assert_eq!(data_len, written_size);
        }

        // update the attribute of the written file
        {
            let mut attr_write = self.attr.write();
            attr_write.size = std::cmp::max(
                attr_write.size,
                (offset.cast::<u64>()).overflow_add(written_size.cast()),
            );
            debug!("file {:?} size = {:?}", self.name, attr_write.size);
        };
        self.update_mtime_ctime_to_now();

        Ok(written_size)
    }

    /// Close file
    async fn close(&mut self, ino: INum, fh: u64, flush: bool) {
        let fd = fh.cast();
        if flush && !self.is_deferred_deletion() {
            // TODO: double check the meaning of the flush flag
            tokio::task::spawn_blocking(move || unistd::fsync(fd))
                .await
                .unwrap_or_else(|e| {
                    panic!("failed to join unistd::fsync task for error {e}");
                })
                .context(format!(
                    "release() failed to flush the file of ino={} and name={:?}",
                    ino,
                    self.get_name(),
                ))
                .unwrap_or_else(|e| {
                    panic!(
                        "release() failed, the error is: {}",
                        crate::common::util::format_anyhow_error(&e)
                    );
                });
        }
        tokio::task::spawn_blocking(move || unistd::close(fd))
            .await
            .unwrap_or_else(|e| {
                panic!("failed to join unistd::close task for error {e}");
            })
            .context(format!(
                "release() failed to close the file handler={} of ino={} and name={:?}",
                fh,
                ino,
                self.get_name(),
            ))
            .unwrap_or_else(|e| {
                panic!(
                    "release() failed, the error is: {}",
                    crate::common::util::format_anyhow_error(&e)
                );
            });
        self.dec_open_count(); // decrease open count before reply in case reply
                               // failed
    }

    /// Close dir
    async fn closedir(&self, ino: INum, fh: u64) {
        tokio::task::spawn_blocking(move || unistd::close(fh.cast()))
            .await
            .unwrap_or_else(|e| {
                panic!("failed to join unistd::close task for error {e}");
            })
            .context(format!(
                "releasedir() failed to close the file handler={} of ino={} and name={:?}",
                fh,
                ino,
                self.get_name(),
            ))
            .unwrap_or_else(|e| {
                panic!(
                    "releasedir() failed, the error is: {}",
                    crate::common::util::format_anyhow_error(&e),
                );
            });
        self.dec_open_count();
        debug!(
            "releasedir() successfully closed the file handler={} of ino={} and name={:?}",
            fh,
            ino,
            self.get_name(),
        );
    }

    /// Precheck before set attr
    #[allow(clippy::too_many_lines)]
    async fn setattr_precheck(
        &self,
        param: SetAttrParam,
        _uid: u32,
        _gid: u32,
    ) -> DatenLordResult<(bool, FileAttr)> {
        let fd = self.get_fd();
        let mut attr = self.get_attr();

        let st_now = SystemTime::now();
        let mut attr_changed = false;
        let mut mtime_ctime_changed = false;
        if let Some(mode_bits) = param.mode {
            let nix_mode = fs_util::parse_mode(mode_bits);
            debug!(
                "setattr_helper() successfully parsed mode={:?} from bits={:#o}",
                nix_mode, mode_bits,
            );
            tokio::task::spawn_blocking(move || stat::fchmod(fd, nix_mode))
                .await?
                .context(format!(
                    "setattr_helper() failed to chmod with mode={mode_bits}",
                ))?;
            attr.perm = fs_util::parse_mode_bits(mode_bits);
            debug!(
                "setattr_helper() set permission={:#o}={} from input bits={:#o}={}",
                attr.perm, attr.perm, mode_bits, mode_bits,
            );
            let kind = fs_util::parse_sflag(mode_bits);
            debug_assert_eq!(kind, attr.kind);

            // Change mode also need to change ctime
            attr.ctime = st_now;
            attr_changed = true;
        }
        if param.u_id.is_some() || param.g_id.is_some() {
            let nix_user_id = param.u_id.map(unistd::Uid::from_raw);
            let nix_group_id = param.g_id.map(unistd::Gid::from_raw);
            tokio::task::spawn_blocking(move || unistd::fchown(fd, nix_user_id, nix_group_id))
                .await?
                .context(format!(
                    "setattr_helper() failed to set uid={nix_user_id:?} and gid={nix_group_id:?}",
                ))?;
            if let Some(raw_uid) = param.u_id {
                attr.uid = raw_uid;
            }
            if let Some(raw_gid) = param.g_id {
                attr.gid = raw_gid;
            }
            // Change uid or gid also need to change ctime
            attr.ctime = st_now;
            attr_changed = true;
        }
        if let Some(file_size) = param.size {
            tokio::task::spawn_blocking(move || unistd::ftruncate(fd, file_size.cast()))
                .await?
                .context(format!(
                    "setattr_helper() failed to truncate file size to {file_size}"
                ))?;
            attr.size = file_size;
            attr.mtime = st_now;
            attr.ctime = st_now;
            mtime_ctime_changed = true;
            attr_changed = true;
        }
        if param.a_time.is_some() || param.m_time.is_some() {
            if mtime_ctime_changed {
                panic!("setattr_helper() cannot change atime and mtime explicitly in the mean while with truncate");
            } else {
                let nix_access_time = param.a_time.map_or(
                    TimeSpec::from(libc::timespec {
                        tv_sec: 0,
                        tv_nsec: libc::UTIME_OMIT,
                    }),
                    |st_atime| {
                        let (seconds, nanoseconds) = fs_util::time_from_system_time(&st_atime);
                        TimeSpec::from(libc::timespec {
                            tv_sec: seconds.cast(),
                            tv_nsec: nanoseconds.cast(),
                        })
                    },
                );
                let nix_modify_time = param.a_time.map_or(
                    TimeSpec::from(libc::timespec {
                        tv_sec: 0,
                        tv_nsec: libc::UTIME_OMIT,
                    }),
                    |st_mtime| {
                        let (seconds, nanoseconds) = fs_util::time_from_system_time(&st_mtime);
                        TimeSpec::from(libc::timespec {
                            tv_sec: seconds.cast(),
                            tv_nsec: nanoseconds.cast(),
                        })
                    },
                );
                tokio::task::spawn_blocking(move || {
                    stat::futimens(fd, &nix_access_time, &nix_modify_time)
                })
                .await?
                .context(format!(
                    "setattr_helper() failed to update atime={:?} or mtime={:?}",
                    param.a_time, param.m_time
                ))?;
                if let Some(st_atime) = param.a_time {
                    attr.atime = st_atime;
                    // Change atime do not need to change ctime
                }
                if let Some(st_mtime) = param.a_time {
                    attr.mtime = st_mtime;
                    // Change mtime also need to change ctime
                    attr.ctime = st_now;
                }
                attr_changed = true;
            }
        }
        // TODO: change lock owner
        // #[cfg(feature = "abi-7-9")]
        // let lock_owner = param.lock_owner;
        #[cfg(feature = "abi-7-23")]
        if let Some(c_time) = param.c_time {
            attr.ctime = c_time;
            // TODO: how to change ctime directly on ext4?
        }
        Ok((attr_changed, attr))
    }
}

/// Rename all the files
pub fn rename_fullpath_recursive(
    ino: INum,
    parent: INum,
    cache: &mut RwLockWriteGuard<'_, BTreeMap<INum, DefaultNode>>,
) {
    let mut node_pool: VecDeque<(INum, INum)> = VecDeque::new();
    node_pool.push_back((ino, parent));

    while let Some((child, parent)) = node_pool.pop_front() {
        let parent_node = cache.get(&parent).unwrap_or_else(|| {
            panic!(
                "impossible case when rename, the parent i-node of ino={parent} should be in the cache"
            )
        });
        let mut parent_path = parent_node.full_path().to_owned();

        let child_node = cache.get_mut(&child).unwrap_or_else(|| {
            panic!(
                "impossible case when rename, the child i-node of ino={child} should be in the cache"
            )
        });
        child_node.set_parent_ino(parent);
        let new_path = match child_node.data {
            DefaultNodeData::Directory(ref dir_data) => {
                for grandchild_node in dir_data.values() {
                    node_pool.push_back((grandchild_node.ino(), child));
                }
                parent_path.push_str(child_node.get_name());
                parent_path.push('/');
                parent_path
            }
            DefaultNodeData::SymLink(..) | DefaultNodeData::RegFile(..) => {
                parent_path.push_str(child_node.get_name());
                parent_path
            }
        };

        child_node.set_full_path(new_path);
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use anyhow::Context;
    use nix::fcntl::{self, FcntlArg, OFlag};
    use nix::sys::stat::Mode;
    use nix::unistd;

    use crate::common::error::{DatenLordError, DatenLordResult};

    #[test]
    fn test_dup_fd() -> DatenLordResult<()> {
        let path = Path::new("/tmp/dup_fd_test.txt");
        let oflags = OFlag::O_CREAT | OFlag::O_TRUNC | OFlag::O_RDWR;
        let res = fcntl::open(path, oflags, Mode::from_bits_truncate(644));
        let fd = match res {
            Ok(fd) => fd,
            Err(e) => {
                return Err(DatenLordError::from(anyhow::anyhow!(
                    "failed to open file {:?}, the error is: {}",
                    path,
                    e,
                )));
            }
        };
        let res = unistd::unlink(path);
        if let Err(e) = res {
            unistd::close(fd)?;
            return Err(DatenLordError::from(anyhow::anyhow!(
                "failed to unlink file, the error is: {}",
                e
            )));
        }

        let dup_fd = unistd::dup(fd).context("failed to dup fd")?;
        let new_oflags = OFlag::O_WRONLY | OFlag::O_APPEND;
        let fcntl_oflags = FcntlArg::F_SETFL(new_oflags);
        fcntl::fcntl(dup_fd, fcntl_oflags).context(format!(
            "failed to set new flags={new_oflags:?} to the dup fd={dup_fd}",
        ))?;

        let file_content = "ABCDEFGHJKLMNOPQRSTUVWXYZ";
        {
            let write_size = unistd::write(dup_fd, file_content.as_bytes())?;
            assert_eq!(write_size, file_content.len(), "write size not match");

            unistd::close(dup_fd)?;
        }
        {
            unistd::lseek(fd, 0, unistd::Whence::SeekSet)?;
            let mut buffer: Vec<u8> = std::iter::repeat(0_u8).take(file_content.len()).collect();
            let read_size = unistd::read(fd, &mut buffer)?;
            assert_eq!(read_size, file_content.len(), "read size not match");
            let content = String::from_utf8(buffer).unwrap_or_else(|e| {
                panic!("failed to convert buffer to string, the error is: {e}")
            });
            assert_eq!(content, file_content, "file content not match");
            unistd::close(fd)?;
        }

        Ok(())
    }
}
