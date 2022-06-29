//! The implementation of filesystem node

use super::cache::{GlobalCache, IoMemBlock};
use super::dir::DirEntry;
use super::dist::client as dist_client;
use super::dist::etcd;
use super::fs_util::{self, FileAttr};
use super::node::Node;
use super::s3_metadata::S3MetaData;
use super::s3_wrapper::S3BackEnd;
use super::SetAttrParam;
use crate::async_fuse::fuse::fuse_reply::{AsIoVec, StatFsParam};
use crate::async_fuse::fuse::protocol::INum;
use crate::async_fuse::metrics;
use crate::common::etcd_delegate::EtcdDelegate;
use async_trait::async_trait;
use clippy_utilities::{Cast, OverflowArithmetic};
use log::{debug, warn};
use nix::fcntl::OFlag;
use nix::sys::stat::Mode;
use nix::sys::stat::SFlag;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{self, AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLockWriteGuard;

/// Block size constant
const BLOCK_SIZE: usize = 1024;

/// A file node data or a directory node data
#[derive(Debug)]
pub enum S3NodeData {
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
pub struct S3Node<S: S3BackEnd + Sync + Send + 'static> {
    /// S3 Backend
    s3_backend: Arc<S>,
    /// Parent node i-number
    parent: u64,
    /// S3Node name
    name: String,
    /// Full path
    full_path: String,
    /// S3Node attribute
    attr: FileAttr,
    /// S3Node data
    data: S3NodeData,
    /// S3Node open counter
    open_count: AtomicI64,
    /// S3Node lookup counter
    lookup_count: AtomicI64,
    /// If S3Node has been marked as deferred deletion
    deferred_deletion: AtomicBool,
    /// Shared metadata
    meta: Arc<S3MetaData<S>>,
}

impl<S: S3BackEnd + Send + Sync + 'static> S3Node<S> {
    /// Create `S3Node`
    fn new(
        parent: u64,
        name: &str,
        full_path: String,
        attr: FileAttr,
        data: S3NodeData,
        s3_backend: Arc<S>,
        meta: Arc<S3MetaData<S>>,
    ) -> Self {
        Self {
            s3_backend,
            parent,
            full_path,
            name: name.to_owned(),
            attr,
            data,
            // open count set to 0 by creation
            open_count: AtomicI64::new(0),
            // lookup count set to 1 by creation
            lookup_count: AtomicI64::new(1),
            deferred_deletion: AtomicBool::new(false),
            meta,
        }
    }

    /// Create child `S3Node` of parent node without open
    pub fn new_child_node_of_parent(
        parent: &Self,
        child_name: &str,
        child_attr: FileAttr,
        target_path: Option<PathBuf>,
    ) -> Self {
        let data = match child_attr.kind {
            SFlag::S_IFDIR => S3NodeData::Directory(BTreeMap::new()),
            SFlag::S_IFREG => {
                S3NodeData::RegFile(Arc::<GlobalCache>::clone(&parent.meta.data_cache))
            }
            SFlag::S_IFLNK => {
                if let Some(path) = target_path {
                    S3NodeData::SymLink(path)
                } else {
                    panic!("type is S_IFLNK, but target_path is None");
                }
            }
            _ => panic!("unsupported type {:?}", child_attr.kind),
        };
        let full_path = parent.absolute_path_of_child(child_name, child_attr.kind);
        Self {
            s3_backend: Arc::clone(&parent.s3_backend),
            parent: parent.get_ino(),
            full_path,
            name: child_name.to_owned(),
            attr: child_attr,
            data,
            // lookup count set to 0 for sync
            open_count: AtomicI64::new(0),
            // open count set to 0 for sync
            lookup_count: AtomicI64::new(0),
            deferred_deletion: AtomicBool::new(false),
            meta: Arc::clone(&parent.meta),
        }
    }

    /// Set node attribute
    pub(crate) async fn _set_attr(&mut self, new_attr: FileAttr, _broadcast: bool) -> FileAttr {
        let old_attr = self.get_attr();
        match self.data {
            S3NodeData::Directory(..) => debug_assert_eq!(new_attr.kind, SFlag::S_IFDIR),
            S3NodeData::RegFile(..) => debug_assert_eq!(new_attr.kind, SFlag::S_IFREG),
            S3NodeData::SymLink(..) => debug_assert_eq!(new_attr.kind, SFlag::S_IFLNK),
        }

        /*
        if broadcast {
            if let Err(e) = dist_client::push_attr(
                self.meta.etcd_client.clone(),
                &self.meta.node_id,
                &self.meta.volume_info,
                &self.full_path,
                &new_attr,
            )
            .await
            {
                panic!("failed to push attribute to others, error: {}", e);
            }
        }
        */
        self.attr = new_attr;
        old_attr
    }

    /// Get a new inode number
    async fn new_inode_num(&self) -> u64 {
        let lock_key = etcd::lock_inode_number(Arc::<EtcdDelegate>::clone(&self.meta.etcd_client))
            .await
            .unwrap_or_else(|e| panic!("failed to get etcd inode number lock, error is {:?}", e));
        let default = self.meta.cur_inum();
        let cur_inum = dist_client::get_ino_num(
            Arc::<EtcdDelegate>::clone(&self.meta.etcd_client),
            &self.meta.node_id,
            &self.meta.volume_info,
            default,
        )
        .await
        .unwrap_or_else(|e| {
            warn!("Load inode num from other node error: {}", e);
            default
        });

        let inum = if cur_inum > default {
            self.meta
                .cur_inum
                .store(cur_inum.overflow_add(1), atomic::Ordering::Relaxed);
            cur_inum.cast()
        } else {
            self.meta
                .cur_inum
                .fetch_add(1, atomic::Ordering::SeqCst)
                .cast()
        };
        etcd::unlock_inode_number(Arc::<EtcdDelegate>::clone(&self.meta.etcd_client), lock_key)
            .await
            .unwrap_or_else(|e| {
                panic!("failed to release etcd inode number lock, error is {:?}", e)
            });
        inum
    }

    /// Get fullpath of this node
    pub(crate) fn full_path(&self) -> &str {
        self.full_path.as_str()
    }

    /// Set fullpath of this node
    fn set_full_path(&mut self, full_path: String) {
        self.full_path = full_path;
    }

    /// Get new fd
    fn new_fd(&self) -> u32 {
        self.meta.cur_fd.fetch_add(1, atomic::Ordering::SeqCst)
    }

    /// Get absolute path of a child file
    fn absolute_path_with_child(&self, child: &str) -> String {
        format!("{}{}", self.full_path, child)
    }

    /// Get absolute path of a child dir
    fn absolute_dir_with_child(&self, child: &str) -> String {
        format!("{}{}/", self.full_path, child)
    }

    /// Get absolute path of child
    pub(crate) fn absolute_path_of_child(&self, child: &str, child_type: SFlag) -> String {
        match child_type {
            SFlag::S_IFDIR => self.absolute_dir_with_child(child),
            SFlag::S_IFREG | SFlag::S_IFLNK => self.absolute_path_with_child(child),
            _ => panic!(
                "absolute_path_of_child() found unsupported file type {:?}",
                child_type
            ),
        }
    }

    /// Update mtime and ctime to now
    async fn update_mtime_ctime_to_now(&mut self) {
        let mut attr = self.get_attr();
        let st_now = SystemTime::now();
        attr.mtime = st_now;
        attr.ctime = st_now;
        self.set_attr(attr).await;
    }

    /// Increase node lookup count
    fn inc_lookup_count(&self) -> i64 {
        self.lookup_count.fetch_add(1, atomic::Ordering::Relaxed)
    }

    /// Helper function to check need to load node data or not
    #[allow(dead_code)]
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
    pub(crate) fn get_dir_data(&self) -> &BTreeMap<String, DirEntry> {
        match self.data {
            S3NodeData::Directory(ref dir_data) => dir_data,
            S3NodeData::RegFile(..) | S3NodeData::SymLink(..) => {
                panic!("forbidden to get DirData from non-directory node")
            }
        }
    }

    /// Get mutable directory data
    pub(crate) fn get_dir_data_mut(&mut self) -> &mut BTreeMap<String, DirEntry> {
        match self.data {
            S3NodeData::Directory(ref mut dir_data) => dir_data,
            S3NodeData::RegFile(..) | S3NodeData::SymLink(..) => {
                panic!("forbidden to get DirData from non-directory node")
            }
        }
    }

    /// Increase node open count
    fn inc_open_count(&self) -> i64 {
        // TODO: add the usage
        self.open_count.fetch_add(1, atomic::Ordering::Relaxed)
    }

    /// Open root node
    pub(crate) async fn open_root_node(
        root_ino: INum,
        name: &str,
        s3_backend: Arc<S>,
        meta: Arc<S3MetaData<S>>,
    ) -> anyhow::Result<Self> {
        let now = SystemTime::now();
        let attr = FileAttr {
            ino: root_ino,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: SFlag::S_IFDIR,
            ..FileAttr::default()
        };

        let root_node = Self::new(
            root_ino,
            name,
            "/".to_owned(),
            attr,
            S3NodeData::Directory(BTreeMap::new()),
            s3_backend,
            meta,
        );

        Ok(root_node)
    }

    /// flush all data of a node
    async fn flush_all_data(&mut self) -> anyhow::Result<()> {
        if self.is_deferred_deletion() {
            return Ok(());
        }
        let data_cache = match self.data {
            S3NodeData::RegFile(ref data_cache) => Arc::<GlobalCache>::clone(data_cache),
            // Do nothing for Directory.
            // TODO: Sync dir data to S3 storage
            S3NodeData::Directory(..) => return Ok(()),
            S3NodeData::SymLink(..) => panic!("forbidden to flush data for link"),
        };

        let size = self.attr.size;
        if self.need_load_file_data(0, size.cast()).await {
            let load_res = self.load_data(0, size.cast()).await;
            if let Err(e) = load_res {
                debug!(
                    "failed to load data for file {} while flushing data, the error is: {}",
                    self.get_name(),
                    crate::common::util::format_anyhow_error(&e),
                );
                return Err(e);
            }
        }

        self.s3_backend
            .put_data_vec(
                &self.full_path,
                data_cache.get_file_cache(self.full_path.as_bytes(), 0, size.cast()),
            )
            .await?;

        Ok(())
    }
}

/// Rename all the files
pub async fn rename_fullpath_recursive<S: S3BackEnd + Send + Sync + 'static>(
    ino: INum,
    parent: INum,
    cache: &mut RwLockWriteGuard<'_, BTreeMap<INum, S3Node<S>>>,
) {
    let mut node_pool: VecDeque<(INum, INum)> = VecDeque::new();
    node_pool.push_back((ino, parent));

    while let Some((child, parent)) = node_pool.pop_front() {
        let parent_node = cache.get(&parent).unwrap_or_else(|| {
            panic!(
                "impossible case when rename, the parent i-node of ino={} should be in the cache",
                parent
            )
        });
        let parent_path = parent_node.full_path.clone();

        let child_node = cache.get_mut(&child).unwrap_or_else(|| {
            panic!(
                "impossible case when rename, the child i-node of ino={} should be in the cache",
                child
            )
        });
        child_node.set_parent_ino(parent);
        let old_path = child_node.full_path.clone();
        let new_path = match child_node.data {
            S3NodeData::Directory(ref dir_data) => {
                dir_data.values().into_iter().for_each(|grandchild_node| {
                    node_pool.push_back((grandchild_node.ino(), child));
                });
                format!("{}{}/", parent_path, child_node.get_name())
            }
            S3NodeData::SymLink(..) | S3NodeData::RegFile(..) => {
                format!("{}{}", parent_path, child_node.get_name())
            }
        };

        let is_reg = if let S3NodeData::RegFile(ref global_cache) = child_node.data {
            global_cache.rename(old_path.as_str().as_bytes(), new_path.as_str().as_bytes());
            true
        } else {
            false
        };

        if is_reg {
            // TODO: Should not flush data, remove this once the "real" cache rename is available
            if let Err(e) = child_node.flush_all_data().await {
                panic!(
                    "failed to flush all data of node {:?}, error is {:?}",
                    child_node.get_full_path(),
                    e
                );
            }
        }

        if let Err(e) = child_node.s3_backend.rename(&old_path, &new_path).await {
            panic!(
                "failed to rename from {:?} to {:?} in s3 backend, error is {:?}",
                old_path, new_path, e
            );
        }

        child_node.set_full_path(new_path);
    }
}

#[async_trait]
impl<S: S3BackEnd + Sync + Send + 'static> Node for S3Node<S> {
    /// Get node i-number
    #[inline]
    fn get_ino(&self) -> INum {
        self.get_attr().ino
    }

    #[inline]
    fn set_ino(&mut self, ino: INum) {
        self.attr.ino = ino;
    }

    /// Get node fd
    #[inline]
    fn get_fd(&self) -> RawFd {
        0_i32
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
            S3NodeData::Directory(..) => SFlag::S_IFDIR,
            S3NodeData::RegFile(..) => SFlag::S_IFREG,
            S3NodeData::SymLink(..) => SFlag::S_IFLNK,
        }
    }

    /// Get node attribute
    #[inline]
    fn get_attr(&self) -> FileAttr {
        self.attr
    }

    /// Set node attribute
    async fn set_attr(&mut self, new_attr: FileAttr) -> FileAttr {
        self._set_attr(new_attr, true).await
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

    /// Load attribute
    async fn load_attribute(&mut self) -> anyhow::Result<FileAttr> {
        let (content_len, last_modified) = self
            .s3_backend
            .get_meta(&self.full_path)
            .await
            .unwrap_or_else(|e| panic!("failed to get meta from s3 backend, error is {:?}", e));

        let attr = FileAttr {
            ino: self.get_ino(),
            kind: match self.data {
                S3NodeData::Directory(..) => SFlag::S_IFDIR,
                S3NodeData::RegFile(..) => SFlag::S_IFREG,
                S3NodeData::SymLink(..) => SFlag::S_IFLNK,
            },
            size: content_len.cast(),
            atime: last_modified,
            mtime: last_modified,
            ctime: last_modified,
            crtime: last_modified,
            ..FileAttr::default()
        };

        self.set_attr(attr).await;

        Ok(attr)
    }

    async fn flush(&mut self, _ino: INum, _fh: u64) {
        if let Err(e) = self.flush_all_data().await {
            panic!(
                "failed to flash all data of {:?}, error is {:?}",
                self.get_full_path(),
                e
            );
        }
    }

    /// Duplicate fd
    async fn dup_fd(&self, _oflags: OFlag) -> anyhow::Result<RawFd> {
        self.inc_open_count();
        Ok(self.new_fd().cast())
    }

    /// Check whether a node is an empty file or an empty directory
    fn is_node_data_empty(&self) -> bool {
        match self.data {
            S3NodeData::Directory(ref dir_node) => dir_node.is_empty(),
            S3NodeData::RegFile(..) => true, // always check the cache
            S3NodeData::SymLink(..) => panic!("forbidden to check symlink is empty or not"),
        }
    }

    /// check whether to load directory entry data or not
    fn need_load_dir_data(&self) -> bool {
        debug_assert_eq!(
            self.attr.kind,
            SFlag::S_IFDIR,
            "fobidden to check non-directory node need load data or not",
        );
        // Dir data is synced. Don't need to load
        false
        //self.need_load_node_data_helper()
    }

    /// Check whether to load file content data or not
    async fn need_load_file_data(&self, offset: usize, len: usize) -> bool {
        debug_assert_eq!(
            self.attr.kind,
            SFlag::S_IFREG,
            "fobidden to check non-file node need load data or not",
        );

        if offset >= self.attr.size.cast() {
            return false;
        }

        match self.data {
            S3NodeData::RegFile(ref cache) => {
                let file_cache = cache.get_file_cache(self.full_path.as_bytes(), offset, len);
                let cache_miss = file_cache.is_empty()
                    || file_cache.iter().filter(|b| !(*b).can_convert()).count() != 0;
                if cache_miss {
                    metrics::CACHE_MISSES.inc();
                } else {
                    metrics::CACHE_HITS.inc();
                }
                cache_miss
            }
            S3NodeData::Directory(..) | S3NodeData::SymLink(..) => {
                panic!("need_load_file_data should handle regular file")
            }
        }
    }

    /// Get a directory entry by name
    fn get_entry(&self, name: &str) -> Option<&DirEntry> {
        self.get_dir_data().get(name)
    }

    /// Create symlink in a directory
    async fn create_child_symlink(
        &mut self,
        child_symlink_name: &str,
        target_path: PathBuf,
    ) -> anyhow::Result<Self> {
        let absolute_path = self.absolute_path_with_child(child_symlink_name);
        let dir_data = self.get_dir_data();
        debug_assert!(
            !dir_data.contains_key(child_symlink_name),
            "create_child_symlink() cannot create duplicated symlink name={:?}",
            child_symlink_name,
        );
        let target_str = target_path
            .to_str()
            .unwrap_or_else(|| panic!("failed to convert {:?} to utf8 string", target_path));
        if let Err(e) = self
            .s3_backend
            .put_data(
                absolute_path.as_str(),
                target_str.as_bytes(),
                0,
                target_str.len(),
            )
            .await
        {
            panic!(
                "failed to put data of file {:?} to s3 backend, error is {:?}",
                absolute_path, e
            );
        }

        // get symbol file attribute
        let child_attr = FileAttr {
            ino: self.new_inode_num().await,
            kind: SFlag::S_IFLNK,
            size: target_path
                .to_str()
                .unwrap_or_else(|| panic!("failed to convert to utf8 string"))
                .len()
                .cast(),
            blocks: 0,
            perm: 0o777,
            ..FileAttr::now()
        };

        let target_path = {
            // insert new entry to parent directory
            let entry = DirEntry::new(
                child_attr.ino,
                child_symlink_name.to_owned(),
                SFlag::S_IFLNK,
            );
            let dir_data_mut = self.get_dir_data_mut();
            let previous_value = dir_data_mut.insert(child_symlink_name.to_owned(), entry);
            debug_assert!(previous_value.is_none()); // double check creation race
            target_path
        };

        self.update_mtime_ctime_to_now().await;
        Ok(Self::new(
            self.get_ino(),
            child_symlink_name,
            format!("{}{}", self.full_path, child_symlink_name),
            child_attr,
            S3NodeData::SymLink(target_path),
            Arc::clone(&self.s3_backend),
            Arc::clone(&self.meta),
        ))
    }

    /// Read symlink itself in a directory, not follow symlink
    async fn load_child_symlink(
        &self,
        child_symlink_name: &str,
        remote: Option<FileAttr>,
    ) -> anyhow::Result<Self> {
        let absolute_path = self.absolute_path_with_child(child_symlink_name);

        let child_attr = match remote {
            None => {
                let (len, last_modified) = self
                    .s3_backend
                    .get_meta(absolute_path.as_str())
                    .await
                    .unwrap_or_else(|e| {
                        panic!(
                            "failed to get meta of {:?} from s3 backend, error is {:?}",
                            absolute_path, e
                        )
                    });
                // get symbol file attribute
                FileAttr {
                    ino: self.new_inode_num().await,
                    kind: SFlag::S_IFLNK,
                    size: len.cast(),
                    blocks: 0,
                    perm: 0o777,
                    atime: last_modified,
                    mtime: last_modified,
                    ctime: last_modified,
                    crtime: last_modified,
                    ..FileAttr::default()
                }
            }
            Some(attr) => attr,
        };
        debug_assert_eq!(SFlag::S_IFLNK, child_attr.kind);

        let target_path = PathBuf::from(
            String::from_utf8(
                self.s3_backend
                    .get_data(&absolute_path)
                    .await
                    .unwrap_or_else(|e| {
                        panic!(
                            "failed to get data of {:?} from s3 backend, error is {:?}",
                            absolute_path, e
                        )
                    }),
            )
            .unwrap_or_else(|e| panic!("failed to convert to utf string, error is {:?}", e)),
        );

        Ok(Self::new(
            self.get_ino(),
            child_symlink_name,
            format!("{}{}", self.full_path, child_symlink_name),
            child_attr,
            S3NodeData::SymLink(target_path),
            Arc::clone(&self.s3_backend),
            Arc::clone(&self.meta),
        ))
    }

    /// Open sub-directory in a directory
    async fn open_child_dir(
        &self,
        child_dir_name: &str,
        remote: Option<FileAttr>,
    ) -> anyhow::Result<Self> {
        let absolute_path = self.absolute_dir_with_child(child_dir_name);

        // get new directory attribute
        let child_attr = match remote {
            None => {
                let last_modified = self
                    .s3_backend
                    .get_last_modified(absolute_path.as_str())
                    .await
                    .unwrap_or_else(|e| panic!("failed to get last modified of file {:?} from s3 backend, error is {:?}", absolute_path, e));
                FileAttr {
                    ino: self.new_inode_num().await,
                    kind: SFlag::S_IFDIR,
                    atime: last_modified,
                    mtime: last_modified,
                    ctime: last_modified,
                    crtime: last_modified,
                    ..FileAttr::default()
                }
            }
            Some(attr) => attr,
        };

        debug_assert_eq!(SFlag::S_IFDIR, child_attr.kind);

        // lookup count and open count are increased to 1 by creation
        let full_path = format!("{}{}/", self.full_path, child_dir_name);

        let child_node = Self::new(
            self.get_ino(),
            child_dir_name,
            full_path,
            child_attr,
            S3NodeData::Directory(BTreeMap::new()),
            Arc::clone(&self.s3_backend),
            Arc::clone(&self.meta),
        );

        Ok(child_node)
    }

    /// Create sub-directory in a directory
    async fn create_child_dir(&mut self, child_dir_name: &str, mode: Mode) -> anyhow::Result<Self> {
        let absolute_path = self.absolute_dir_with_child(child_dir_name);
        let dir_data = self.get_dir_data();
        // TODO return error
        debug_assert!(
            !dir_data.contains_key(child_dir_name),
            "create_child_dir() cannot create duplicated directory name={:?}",
            child_dir_name
        );
        if let Err(e) = self.s3_backend.create_dir(absolute_path.as_str()).await {
            panic!(
                "failed to create dir={:?} in s3 backend, error is {:?}",
                absolute_path, e
            );
        }

        // get new directory attribute
        let child_attr = FileAttr {
            ino: self.new_inode_num().await,
            kind: SFlag::S_IFDIR,
            perm: fs_util::parse_mode_bits(mode.bits()),
            ..FileAttr::now()
        };
        debug_assert_eq!(SFlag::S_IFDIR, child_attr.kind);

        // insert new entry to parent directory
        let entry = DirEntry::new(child_attr.ino, child_dir_name.to_owned(), SFlag::S_IFDIR);

        let dir_data_mut = self.get_dir_data_mut();
        let previous_value = dir_data_mut.insert(child_dir_name.to_owned(), entry);
        debug_assert!(previous_value.is_none()); // double check creation race

        // lookup count and open count are increased to 1 by creation
        let full_path = format!("{}{}/", self.full_path, child_dir_name);

        let child_node = Self::new(
            self.get_ino(),
            child_dir_name,
            full_path,
            child_attr,
            S3NodeData::Directory(BTreeMap::new()),
            Arc::clone(&self.s3_backend),
            Arc::clone(&self.meta),
        );

        self.update_mtime_ctime_to_now().await;
        Ok(child_node)
    }

    /// Open file in a directory
    async fn open_child_file(
        &self,
        child_file_name: &str,
        remote: Option<FileAttr>,
        _oflags: OFlag,
        global_cache: Arc<GlobalCache>,
    ) -> anyhow::Result<Self> {
        // get new file attribute
        let absolute_path = self.absolute_path_with_child(child_file_name);
        let child_attr = match remote {
            None => {
                let (content_len, last_modified) = self
                    .s3_backend
                    .get_meta(absolute_path.as_str())
                    .await
                    .unwrap_or_else(|e| {
                        panic!(
                            "failed to get meta of {:?} from s3 backend, error is {:?}",
                            absolute_path, e
                        )
                    });
                FileAttr {
                    ino: self.new_inode_num().await,
                    kind: SFlag::S_IFREG,
                    size: content_len.cast(),
                    blocks: content_len
                        .overflow_add(BLOCK_SIZE)
                        .overflow_sub(1)
                        .overflow_div(BLOCK_SIZE)
                        .cast(),
                    atime: last_modified,
                    mtime: last_modified,
                    ctime: last_modified,
                    crtime: last_modified,
                    ..FileAttr::default()
                }
            }
            Some(attr) => attr,
        };
        debug_assert_eq!(SFlag::S_IFREG, child_attr.kind);

        Ok(Self::new(
            self.get_ino(),
            child_file_name,
            format!("{}{}", self.full_path, child_file_name),
            child_attr,
            S3NodeData::RegFile(global_cache),
            Arc::clone(&self.s3_backend),
            Arc::clone(&self.meta),
        ))
    }

    /// Create file in a directory
    async fn create_child_file(
        &mut self,
        child_file_name: &str,
        oflags: OFlag,
        mode: Mode,
        global_cache: Arc<GlobalCache>,
    ) -> anyhow::Result<Self> {
        let absolute_path = self.absolute_path_with_child(child_file_name);
        let dir_data = self.get_dir_data();
        debug_assert!(
            !dir_data.contains_key(child_file_name),
            "open_child_file_helper() cannot create duplicated file name={:?}",
            child_file_name
        );
        debug_assert!(oflags.contains(OFlag::O_CREAT));
        if let Err(e) = self
            .s3_backend
            .put_data(absolute_path.as_str(), b"", 0, 0)
            .await
        {
            panic!(
                "failed to put data of file {:?} to s3 backend, error is {:?}",
                absolute_path, e
            );
        }

        // get new file attribute
        let child_attr = FileAttr {
            ino: self.new_inode_num().await,
            kind: SFlag::S_IFREG,
            perm: fs_util::parse_mode_bits(mode.bits()),
            size: 0,
            blocks: 0,
            ..FileAttr::now()
        };
        debug_assert_eq!(SFlag::S_IFREG, child_attr.kind);

        let entry = DirEntry::new(child_attr.ino, child_file_name.to_owned(), SFlag::S_IFREG);

        let dir_data_mut = self.get_dir_data_mut();
        // insert new entry to parent directory
        // TODO: support thread-safe
        let previous_value = dir_data_mut.insert(child_file_name.to_owned(), entry);
        debug_assert!(previous_value.is_none()); // double check creation race

        self.update_mtime_ctime_to_now().await;
        Ok(Self::new(
            self.get_ino(),
            child_file_name,
            format!("{}{}", self.full_path, child_file_name),
            child_attr,
            S3NodeData::RegFile(global_cache),
            Arc::clone(&self.s3_backend),
            Arc::clone(&self.meta),
        ))
    }

    /// Load data from directory, file or symlink target.
    /// The `offset` and `len` is used for regular file
    async fn load_data(&mut self, offset: usize, len: usize) -> anyhow::Result<usize> {
        match self.data {
            S3NodeData::Directory(..) => {
                // TODO: really read dir from S3
                let entries = match dist_client::load_dir(
                    Arc::<EtcdDelegate>::clone(&self.meta.etcd_client),
                    &self.meta.node_id,
                    &self.meta.volume_info,
                    &self.full_path,
                )
                .await?
                {
                    Some(entries) => entries,
                    None => BTreeMap::new(),
                };
                self.data = S3NodeData::Directory(entries);
                Ok(0)
            }
            S3NodeData::RegFile(ref global_cache) => {
                let aligned_offset = global_cache.round_down(offset);
                let new_len_tmp =
                    global_cache.round_up(offset.overflow_sub(aligned_offset).overflow_add(len));

                let new_len = if new_len_tmp.overflow_add(aligned_offset) > self.attr.size.cast() {
                    self.attr.size.cast::<usize>().overflow_sub(aligned_offset)
                } else {
                    new_len_tmp
                };

                // dist_client::read_data() won't get lock at remote, OK to put here.
                let file_data_vec = match dist_client::read_data(
                    Arc::<EtcdDelegate>::clone(&self.meta.etcd_client),
                    &self.meta.node_id,
                    &self.meta.volume_info,
                    &self.full_path,
                    aligned_offset
                        .overflow_div(global_cache.get_align().cast())
                        .cast(),
                    aligned_offset
                        .overflow_add(new_len.cast())
                        .overflow_sub(1)
                        .overflow_div(global_cache.get_align().cast())
                        .cast(),
                )
                .await?
                {
                    None => {
                        match self
                            .s3_backend
                            .get_partial_data(&self.full_path, aligned_offset, new_len)
                            .await
                        {
                            Ok(a) => a,
                            Err(e) => {
                                let anyhow_err: anyhow::Error = e.into();
                                return Err(anyhow_err
                                    .context("load_data() failed to load file content data"));
                            }
                        }
                    }
                    Some(data) => data,
                };

                let read_size = file_data_vec.len();
                debug!(
                    "load_data() successfully load {} byte file content data",
                    read_size
                );
                global_cache
                    .write_or_update(
                        self.full_path.as_bytes(),
                        aligned_offset,
                        read_size,
                        &file_data_vec,
                        false,
                    )
                    .await;
                Ok(read_size)
            }
            S3NodeData::SymLink(..) => {
                panic!("forbidden to load symlink target data");
            }
        }
    }

    /// Insert directory entry for rename()
    async fn insert_entry_for_rename(&mut self, child_entry: DirEntry) -> Option<DirEntry> {
        let dir_data = self.get_dir_data_mut();
        let previous_entry = dir_data.insert(child_entry.entry_name().into(), child_entry);

        self.update_mtime_ctime_to_now().await;
        debug!(
            "insert_entry_for_rename() successfully inserted new entry \
                and replaced previous entry={:?}",
            previous_entry,
        );

        previous_entry
    }

    /// Remove directory entry from cache only for rename()
    async fn remove_entry_for_rename(&mut self, child_name: &str) -> Option<DirEntry> {
        let dir_data = self.get_dir_data_mut();
        let remove_res = dir_data.remove(child_name);
        if remove_res.is_some() {
            self.update_mtime_ctime_to_now().await;
        }
        remove_res
    }

    /// Unlink directory entry from both cache and disk
    async fn unlink_entry(&mut self, child_name: &str) -> anyhow::Result<DirEntry> {
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
        // delete from disk and close the handler
        match removed_entry.entry_type() {
            SFlag::S_IFDIR | SFlag::S_IFREG | SFlag::S_IFLNK => {
                if let Err(e) = self
                    .s3_backend
                    .delete_data(&self.absolute_path_with_child(child_name))
                    .await
                {
                    panic!(
                        "failed to delete data of {:?} from s3 backend, error is {:?}",
                        self.absolute_path_with_child(child_name),
                        e
                    );
                }
            }
            _ => panic!(
                "unlink_entry() found unsupported entry type={:?}",
                removed_entry.entry_type()
            ),
        }
        self.update_mtime_ctime_to_now().await;
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
            S3NodeData::Directory(..) | S3NodeData::RegFile(..) => {
                panic!("forbidden to read target path from non-symlink node")
            }
            S3NodeData::SymLink(ref target_path) => target_path,
        }
    }

    /// Fake data for statefs
    /// TODO: handle some important data from S3 storage
    async fn statefs(&self) -> anyhow::Result<StatFsParam> {
        let inode_num = self
            .meta
            .cur_inum
            .load(atomic::Ordering::Relaxed)
            .overflow_sub(1);
        Ok(StatFsParam {
            blocks: 10_000_000_000,
            bfree: 10_000_000_000,
            bavail: 10_000_000_000,
            files: inode_num.cast(),
            f_free: 1_000_000,
            bsize: 4096, // TODO: consider use customized block size
            namelen: 1024,
            frsize: 4096,
        })
    }

    /// Get file data
    async fn get_file_data(&self, offset: usize, len: usize) -> Vec<IoMemBlock> {
        match self.data {
            S3NodeData::Directory(..) | S3NodeData::SymLink(..) => {
                panic!("forbidden to load FileData from non-file node")
            }
            S3NodeData::RegFile(ref cache) => {
                cache.get_file_cache(self.full_path.as_bytes(), offset, len)
            }
        }
    }

    /// Write to file
    async fn write_file(
        &mut self,
        _fh: u64,
        offset: i64,
        data: Vec<u8>,
        _oflags: OFlag,
        _write_to_disk: bool,
    ) -> anyhow::Result<usize> {
        let this: &Self = self;

        let ino = this.get_ino();
        if this.need_load_file_data(offset.cast(), data.len()).await {
            let load_res = self.load_data(offset.cast(), data.len()).await;
            if let Err(e) = load_res {
                debug!(
                    "read() failed to load file data of ino={} and name={:?}, the error is: {}",
                    ino,
                    self.get_name(),
                    crate::common::util::format_anyhow_error(&e),
                );
                return Err(e);
            }
        }

        let cache = match self.data {
            S3NodeData::Directory(..) | S3NodeData::SymLink(..) => {
                panic!("forbidden to load FileData from non-file node")
            }
            S3NodeData::RegFile(ref file_data) => file_data,
        };

        cache
            .write_or_update(
                self.full_path.as_bytes(),
                offset.cast(),
                data.len(),
                data.as_slice(),
                true,
            )
            .await;

        let written_size = data.len();

        // update the attribute of the written file
        self.attr.size = std::cmp::max(
            self.attr.size,
            offset.cast::<u64>().overflow_add(written_size.cast()),
        );

        debug!("file {:?} size = {:?}", self.name, self.attr.size);
        self.update_mtime_ctime_to_now().await;

        Ok(written_size)
    }

    async fn close(&mut self, _ino: INum, _fh: u64, _flush: bool) {
        if let Err(e) = self.flush_all_data().await {
            panic!(
                "failed to flush all data of {:?}, error is {:?}",
                self.full_path, e
            );
        }
        self.dec_open_count();
    }

    /// TODO: push dir data to s3
    async fn closedir(&self, _ino: INum, _fh: u64) {
        self.dec_open_count();
    }

    async fn setattr_precheck(&self, param: SetAttrParam) -> anyhow::Result<(bool, FileAttr)> {
        let mut attr = self.get_attr();

        let st_now = SystemTime::now();
        let mut attr_changed = false;
        let mut mtime_ctime_changed = false;
        if let Some(mode_bits) = param.mode {
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
