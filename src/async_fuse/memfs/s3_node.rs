//! The implementation of filesystem node

use std::collections::BTreeMap;
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::anyhow;
use async_trait::async_trait;
use clippy_utilities::{Cast, OverflowArithmetic};
use futures::future::{BoxFuture, FutureExt};
use nix::errno::Errno;
use nix::fcntl::OFlag;
use nix::sys::stat::{Mode, SFlag};
use nix::unistd;
use parking_lot::RwLock;
use tracing::debug;

use super::cache::{GlobalCache, IoMemBlock};
use super::dir::DirEntry;
use super::dist::client as dist_client;
use super::fs_util::{self, FileAttr};
use super::kv_engine::KVEngineType;
use super::node::Node;
use super::s3_metadata::S3MetaData;
use super::s3_wrapper::S3BackEnd;
use super::serial::{
    dir_entry_to_serial, file_attr_to_serial, serial_to_file_attr, SerialNode, SerialNodeData,
};
use super::SetAttrParam;
use crate::async_fuse::fuse::fuse_reply::{AsIoVec, StatFsParam};
use crate::async_fuse::fuse::protocol::{INum, FUSE_ROOT_ID};
use crate::async_fuse::metrics;
use crate::async_fuse::util::build_error_result_from_errno;
use crate::common::error::{DatenLordError, DatenLordResult};

/// S3's available fd count
static GLOBAL_S3_FD_CNT: AtomicU32 = AtomicU32::new(4);

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

impl S3NodeData {
    /// Serializes the node data
    pub fn serial(&self) -> SerialNodeData {
        match *self {
            Self::Directory(ref dir) => {
                let mut serial_dir = BTreeMap::new();
                for (name, dir_entry) in dir {
                    serial_dir.insert(name.clone(), dir_entry_to_serial(dir_entry));
                }
                SerialNodeData::Directory(serial_dir)
            }
            Self::RegFile(_) => SerialNodeData::File,
            Self::SymLink(ref target) => SerialNodeData::SymLink(target.clone()),
        }
    }
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
    /// S3Node attribute
    attr: Arc<RwLock<FileAttr>>,
    /// S3Node data
    data: S3NodeData,
    /// S3Node open counter
    open_count: AtomicI64,
    /// S3Node lookup counter
    lookup_count: AtomicI64,
    /// If S3Node has been marked as deferred deletion
    deferred_deletion: AtomicBool,
    /// KVEngine
    kv_engine: Arc<KVEngineType>,
    /// K8s node id
    k8s_node_id: Arc<str>,
    /// K8S volume_info
    k8s_volume_info: Arc<str>,
}

impl<S: S3BackEnd + Send + Sync + 'static> S3Node<S> {
    #[allow(clippy::too_many_arguments)]
    /// Create `S3Node`
    fn new(
        parent: u64,
        name: &str,
        attr: Arc<RwLock<FileAttr>>,
        data: S3NodeData,
        s3_backend: Arc<S>,
        kv_engine: &Arc<KVEngineType>,
        k8s_node_id: &Arc<str>,
        k8s_volume_info: &Arc<str>,
    ) -> Self {
        Self {
            s3_backend,
            parent,
            name: name.to_owned(),
            attr,
            data,
            // open count set to 0 by creation
            open_count: AtomicI64::new(0),
            // lookup count set to 1 by creation
            lookup_count: AtomicI64::new(1),
            deferred_deletion: AtomicBool::new(false),
            kv_engine: Arc::clone(kv_engine),
            k8s_node_id: Arc::clone(k8s_node_id),
            k8s_volume_info: Arc::clone(k8s_volume_info),
        }
    }

    #[allow(dead_code)]
    /// Deserialize `S3Node` from `SerialNode`
    // This function returns a `BoxFuture due` to its potential for recursive calls
    // (`get_node_from_kv_engine()`). Recursive async functions in Rust can lead
    // to 'infinite type' compilation errors because each async function
    // is compiled into a unique type that must know its size at compile time. When
    // the function is recursive, it embeds its own type within it for every
    // recursive call, leading to an 'infinite' type size.
    //
    // By using a BoxFuture, we can heap-allocate the future, which avoids these
    // issues and provides a type of a known size (the size of a pointer),
    // regardless of the depth or complexity of the recursion within the future.
    // This is crucial in enabling recursive async behavior in Rust.
    // For more information, see https://rust-lang.github.io/async-book/07_workarounds/04_recursion.html
    pub fn from_serial_node(
        serial_node: SerialNode,
        meta: &S3MetaData<S>,
    ) -> BoxFuture<'_, DatenLordResult<S3Node<S>>> {
        async move {
            // check if the node is a directory
            // if it is a directory, we need to fetch it's children's file attributes
            let dir_data = if let SerialNodeData::Directory(ref dir) = serial_node.data {
                let mut dir_entries = BTreeMap::new();
                for (name, serial_dir_entry) in dir {
                    let child_ino = serial_dir_entry.get_child_ino();
                    // Fetch the child node from kv
                    let child_node = meta.get_node_from_kv_engine(child_ino).await?;
                    // If the child_node is None , it means it has been deleted,skip
                    if let Some(child_node) = child_node {
                        let child_attr = *child_node.attr.read();
                        dir_entries.insert(
                            name.clone(),
                            DirEntry::new(name.clone(), Arc::new(RwLock::new(child_attr))),
                        );
                    }
                }
                S3NodeData::Directory(dir_entries)
            } else {
                serial_node
                    .data
                    .into_s3_nodedata(Arc::clone(&meta.data_cache))
            };
            Ok(Self {
                s3_backend: Arc::clone(&meta.s3_backend),
                parent: serial_node.parent,
                name: serial_node.name,
                attr: Arc::new(RwLock::new(serial_to_file_attr(&serial_node.attr))),
                data: dir_data,
                open_count: AtomicI64::new(serial_node.open_count),
                lookup_count: AtomicI64::new(serial_node.lookup_count),
                deferred_deletion: AtomicBool::new(serial_node.deferred_deletion),
                kv_engine: Arc::clone(&meta.kv_engine),
                k8s_node_id: Arc::clone(&meta.node_id),
                k8s_volume_info: Arc::clone(&meta.volume_info),
            })
        }
        .boxed()
    }

    /// This function is used to create a new `SerialNode` by `S3Node`
    pub fn into_serial_node(self) -> SerialNode {
        SerialNode {
            parent: self.parent,
            name: self.name,
            attr: file_attr_to_serial(&self.attr.read().clone()),
            data: self.data.serial(),
            open_count: self.open_count.load(Ordering::SeqCst),
            lookup_count: self.lookup_count.load(Ordering::SeqCst),
            deferred_deletion: self.deferred_deletion.load(Ordering::SeqCst),
        }
    }

    /// This function is used to create a new `SerialNode` by `S3Node` ref
    pub fn to_serial_node(&self) -> SerialNode {
        SerialNode {
            parent: self.parent,
            name: self.name.clone(),
            attr: file_attr_to_serial(&self.attr.read().clone()),
            data: self.data.serial(),
            open_count: self.open_count.load(Ordering::SeqCst),
            lookup_count: self.lookup_count.load(Ordering::SeqCst),
            deferred_deletion: self.deferred_deletion.load(Ordering::SeqCst),
        }
    }

    /// Create child `S3Node` of parent node without open
    pub fn new_child_node_of_parent(
        parent: &Self,
        child_name: &str,
        child_attr: Arc<RwLock<FileAttr>>,
        target_path: Option<PathBuf>,
        data_cache: &Arc<GlobalCache>,
    ) -> Self {
        let data = match child_attr.read().kind {
            SFlag::S_IFDIR => S3NodeData::Directory(BTreeMap::new()),
            SFlag::S_IFREG => S3NodeData::RegFile(Arc::<GlobalCache>::clone(data_cache)),
            SFlag::S_IFLNK => {
                if let Some(path) = target_path {
                    S3NodeData::SymLink(path)
                } else {
                    panic!("type is S_IFLNK, but target_path is None");
                }
            }
            _ => panic!("unsupported type {:?}", child_attr.read().kind),
        };
        Self {
            s3_backend: Arc::clone(&parent.s3_backend),
            parent: parent.get_ino(),
            name: child_name.to_owned(),
            attr: child_attr,
            data,
            // lookup count set to 0 for sync
            open_count: AtomicI64::new(0),
            // open count set to 0 for sync
            lookup_count: AtomicI64::new(0),
            deferred_deletion: AtomicBool::new(false),
            kv_engine: Arc::clone(&parent.kv_engine),
            k8s_node_id: Arc::clone(&parent.k8s_node_id),
            k8s_volume_info: Arc::clone(&parent.k8s_volume_info),
        }
    }

    /// Set node attribute
    pub(crate) fn _set_attr(&mut self, new_attr: FileAttr, _broadcast: bool) -> FileAttr {
        let old_attr = self.get_attr();
        match self.data {
            S3NodeData::Directory(..) => debug_assert_eq!(new_attr.kind, SFlag::S_IFDIR),
            S3NodeData::RegFile(..) => debug_assert_eq!(new_attr.kind, SFlag::S_IFREG),
            S3NodeData::SymLink(..) => debug_assert_eq!(new_attr.kind, SFlag::S_IFLNK),
        }

        self.attr.write().clone_from(&new_attr);
        old_attr
    }

    #[allow(clippy::unused_self)]
    /// Get new fd
    fn new_fd(&self) -> u32 {
        // Add global fd counter
        GLOBAL_S3_FD_CNT.fetch_add(1, Ordering::SeqCst)
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
        self.lookup_count.fetch_add(1, Ordering::AcqRel)
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
        self.open_count.fetch_add(1, Ordering::AcqRel)
    }

    /// Open root node
    #[allow(clippy::unnecessary_wraps)]
    pub(crate) async fn open_root_node(
        root_ino: INum,
        name: &str,
        s3_backend: Arc<S>,
        meta: Arc<S3MetaData<S>>,
    ) -> DatenLordResult<Self> {
        if let Some(root_node) = meta.get_node_from_kv_engine(FUSE_ROOT_ID).await? {
            Ok(root_node)
        } else {
            let now = SystemTime::now();
            let attr = Arc::new(RwLock::new(FileAttr {
                ino: root_ino,
                atime: now,
                mtime: now,
                ctime: now,
                crtime: now,
                kind: SFlag::S_IFDIR,
                uid: unistd::getuid().as_raw(),
                gid: unistd::getgid().as_raw(),
                ..FileAttr::default()
            }));

            let root_node = Self::new(
                root_ino,
                name,
                attr,
                S3NodeData::Directory(BTreeMap::new()),
                s3_backend,
                &meta.kv_engine,
                &meta.node_id,
                &meta.volume_info,
            );
            Ok(root_node)
        }
    }

    /// flush all data of a node
    async fn flush_all_data(&mut self) -> DatenLordResult<()> {
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

        let size = self.attr.read().size;
        if self.need_load_file_data(0, size.cast()).await {
            let load_res = self.load_data(0, size.cast()).await;
            if let Err(e) = load_res {
                debug!(
                    "failed to load data for file {} while flushing data, the error is: {:?}",
                    self.get_name(),
                    e,
                );
                return Err(e);
            }
        }

        let put_result = self
            .s3_backend
            .put_data_vec(
                self.get_ino(),
                data_cache.get_file_cache(self.get_ino(), 0, size.cast()),
            )
            .await;

        match put_result {
            Ok(_) => Ok(()),
            Err(e) => {
                debug!(
                    "flush_all_data() failed to flush data for file {}, the error is: {}",
                    self.get_name(),
                    e,
                );
                Err(DatenLordError::from(anyhow!(e)))
            }
        }
    }

    /// Check if given uid and gid can access this node
    pub fn open_pre_check(&self, flags: OFlag, user_id: u32, group_id: u32) -> DatenLordResult<()> {
        let attr = self.get_attr();
        let access_mode = match flags & (OFlag::O_RDONLY | OFlag::O_WRONLY | OFlag::O_RDWR) {
            OFlag::O_RDONLY => 4,
            OFlag::O_WRONLY => 2,
            _ => 6,
        };
        attr.check_perm(user_id, group_id, access_mode)
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
        self.attr.write().ino = ino;
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
        *self.attr.read()
    }

    /// Set node attribute
    fn set_attr(&mut self, new_attr: FileAttr) -> FileAttr {
        self._set_attr(new_attr, true)
    }

    /// Get node attribute and increase lookup count
    fn lookup_attr(&self) -> FileAttr {
        let attr = self.get_attr();
        self.inc_lookup_count();
        attr
    }

    /// Get node open count
    fn get_open_count(&self) -> i64 {
        self.open_count.load(Ordering::Acquire)
    }

    /// Decrease node open count
    fn dec_open_count(&self) -> i64 {
        debug_assert!(self.open_count.load(Ordering::Acquire) > 0);
        self.open_count.fetch_sub(1, Ordering::AcqRel)
    }

    /// Get node lookup count
    fn get_lookup_count(&self) -> i64 {
        self.lookup_count.load(Ordering::Acquire)
    }

    /// Decrease node lookup count
    fn dec_lookup_count_by(&self, nlookup: u64) -> i64 {
        debug_assert!(nlookup < std::i64::MAX.cast());
        self.lookup_count
            .fetch_sub(nlookup.cast(), Ordering::AcqRel)
    }

    /// Mark node as deferred deletion
    fn mark_deferred_deletion(&self) {
        self.deferred_deletion.store(true, Ordering::SeqCst);
    }

    /// If node is marked as deferred deletion
    fn is_deferred_deletion(&self) -> bool {
        self.deferred_deletion.load(Ordering::SeqCst)
    }

    async fn flush(&mut self, ino: INum, _fh: u64) {
        if let Err(e) = self.flush_all_data().await {
            panic!("failed to flash all data of {ino:?}, error is {e:?}");
        }
    }

    /// Duplicate fd
    async fn dup_fd(&self, _oflags: OFlag) -> DatenLordResult<RawFd> {
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
            self.attr.read().kind,
            SFlag::S_IFDIR,
            "fobidden to check non-directory node need load data or not",
        );
        // Dir data is synced. Don't need to load
        false
        // self.need_load_node_data_helper()
    }

    /// Check whether to load file content data or not
    async fn need_load_file_data(&self, offset: usize, len: usize) -> bool {
        debug_assert_eq!(
            self.attr.read().kind,
            SFlag::S_IFREG,
            "fobidden to check non-file node need load data or not",
        );

        if offset >= self.attr.read().size.cast() {
            debug!(
                "offset {} is larger than file size {}",
                offset,
                self.attr.read().size
            );
            return false;
        }

        match self.data {
            S3NodeData::RegFile(ref cache) => {
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
        inum: INum,
        child_symlink_name: &str,
        target_path: PathBuf,
    ) -> DatenLordResult<Self> {
        let dir_data = self.get_dir_data();
        debug_assert!(
            !dir_data.contains_key(child_symlink_name),
            "create_child_symlink() cannot create duplicated symlink name={child_symlink_name:?}",
        );
        let target_str = target_path
            .to_str()
            .unwrap_or_else(|| panic!("failed to convert {target_path:?} to utf8 string"));
        if let Err(e) = self
            .s3_backend
            .put_data(inum, target_str.as_bytes(), 0, target_str.len())
            .await
        {
            panic!("failed to put data of file {inum} to s3 backend, error is {e:?}");
        }

        // get symbol file attribute
        let child_attr = Arc::new(RwLock::new(FileAttr {
            ino: inum,
            kind: SFlag::S_IFLNK,
            size: target_path
                .to_str()
                .unwrap_or_else(|| panic!("failed to convert to utf8 string"))
                .len()
                .cast(),
            blocks: 0,
            perm: 0o777,
            ..FileAttr::now()
        }));

        let target_path = {
            // insert new entry to parent directory
            let entry = DirEntry::new(
                child_symlink_name.to_owned(),
                // SFlag::S_IFLNK,
                Arc::clone(&child_attr),
            );
            let dir_data_mut = self.get_dir_data_mut();
            let previous_value = dir_data_mut.insert(child_symlink_name.to_owned(), entry);
            debug_assert!(previous_value.is_none()); // double check creation race
            target_path
        };

        self.update_mtime_ctime_to_now();
        Ok(Self::new(
            self.get_ino(),
            child_symlink_name,
            child_attr,
            S3NodeData::SymLink(target_path),
            Arc::clone(&self.s3_backend),
            &self.kv_engine,
            &self.k8s_node_id,
            &self.k8s_volume_info,
        ))
    }

    /// Read symlink itself in a directory, not follow symlink
    async fn load_child_symlink(
        &self,
        child_symlink_name: &str,
        child_attr: Arc<RwLock<FileAttr>>,
    ) -> DatenLordResult<Self> {
        let inum = child_attr.read().ino;

        let target_path = PathBuf::from(
            String::from_utf8(self.s3_backend.get_data(inum).await.unwrap_or_else(|e| {
                panic!("failed to get data of {inum} from s3 backend, error is {e:?}")
            }))
            .unwrap_or_else(|e| panic!("failed to convert to utf string, error is {e:?}")),
        );

        Ok(Self::new(
            self.get_ino(),
            child_symlink_name,
            child_attr,
            S3NodeData::SymLink(target_path),
            Arc::clone(&self.s3_backend),
            &self.kv_engine,
            &self.k8s_node_id,
            &self.k8s_volume_info,
        ))
    }

    /// Create sub-directory in a directory
    async fn create_child_dir(
        &mut self,
        inum: INum,
        child_dir_name: &str,
        mode: Mode,
        user_id: u32,
        group_id: u32,
    ) -> DatenLordResult<Self> {
        let dir_data = self.get_dir_data();
        // TODO return error
        debug_assert!(
            !dir_data.contains_key(child_dir_name),
            "create_child_dir() cannot create duplicated directory name={child_dir_name:?}"
        );

        // get new directory attribute
        let child_attr = Arc::new(RwLock::new(FileAttr {
            ino: inum,
            kind: SFlag::S_IFDIR,
            perm: fs_util::parse_mode_bits(mode.bits()),
            uid: user_id,
            gid: group_id,
            nlink: 1,
            ..FileAttr::now()
        }));

        // insert new entry to parent directory
        let entry = DirEntry::new(child_dir_name.to_owned(), Arc::clone(&child_attr));

        let dir_data_mut = self.get_dir_data_mut();
        let previous_value = dir_data_mut.insert(child_dir_name.to_owned(), entry);
        debug_assert!(previous_value.is_none()); // double check creation race

        let child_node = Self::new(
            self.get_ino(),
            child_dir_name,
            child_attr,
            S3NodeData::Directory(BTreeMap::new()),
            Arc::clone(&self.s3_backend),
            &self.kv_engine,
            &self.k8s_node_id,
            &self.k8s_volume_info,
        );

        self.update_mtime_ctime_to_now();
        Ok(child_node)
    }

    /// Open file in a directory
    async fn open_child_file(
        &self,
        child_file_name: &str,
        child_attr: Arc<RwLock<FileAttr>>,
        _oflags: OFlag,
        global_cache: Arc<GlobalCache>,
    ) -> DatenLordResult<Self> {
        Ok(Self::new(
            self.get_ino(),
            child_file_name,
            child_attr,
            S3NodeData::RegFile(global_cache),
            Arc::clone(&self.s3_backend),
            &self.kv_engine,
            &self.k8s_node_id,
            &self.k8s_volume_info,
        ))
    }

    /// Create file in a directory
    async fn create_child_file(
        &mut self,
        inum: INum,
        child_file_name: &str,
        oflags: OFlag,
        mode: Mode,
        user_id: u32,
        group_id: u32,
        global_cache: Arc<GlobalCache>,
    ) -> DatenLordResult<Self> {
        let dir_data = self.get_dir_data();
        debug_assert!(
            !dir_data.contains_key(child_file_name),
            "open_child_file_helper() cannot create duplicated file name={child_file_name:?}"
        );
        debug_assert!(oflags.contains(OFlag::O_CREAT));
        if let Err(e) = self.s3_backend.put_data(inum, b"", 0, 0).await {
            panic!("failed to put data of file {inum} to s3 backend, error is {e:?}");
        }

        // get new file attribute
        let child_attr = Arc::new(RwLock::new(FileAttr {
            ino: inum,
            kind: SFlag::S_IFREG,
            perm: fs_util::parse_mode_bits(mode.bits()),
            size: 0,
            blocks: 0,
            uid: user_id,
            gid: group_id,
            nlink: 1,
            ..FileAttr::now()
        }));
        debug_assert_eq!(SFlag::S_IFREG, child_attr.read().kind);

        let entry = DirEntry::new(child_file_name.to_owned(), Arc::clone(&child_attr));

        let dir_data_mut = self.get_dir_data_mut();
        // insert new entry to parent directory
        // TODO: support thread-safe
        let previous_value = dir_data_mut.insert(child_file_name.to_owned(), entry);
        debug_assert!(previous_value.is_none()); // double check creation race

        self.update_mtime_ctime_to_now();
        Ok(Self::new(
            self.get_ino(),
            child_file_name,
            child_attr,
            S3NodeData::RegFile(global_cache),
            Arc::clone(&self.s3_backend),
            &self.kv_engine,
            &self.k8s_node_id,
            &self.k8s_volume_info,
        ))
    }

    /// Load data from directory, file or symlink target.
    /// The `offset` and `len` is used for regular file
    async fn load_data(&mut self, offset: usize, len: usize) -> DatenLordResult<usize> {
        match self.data {
            S3NodeData::Directory(..) => Ok(0),
            S3NodeData::RegFile(ref global_cache) => {
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
                debug!(
                    "load_data() offset={}, len={}, new_len={} , aligned_offset={}",
                    offset, len, new_len, aligned_offset
                );

                // dist_client::read_data() won't get lock at remote, OK to put here.
                let file_data_vec = match dist_client::read_data(
                    &self.kv_engine,
                    &self.k8s_node_id,
                    &self.k8s_volume_info,
                    self.get_ino(),
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
                            .get_partial_data(self.get_ino(), aligned_offset, new_len)
                            .await
                        {
                            Ok(a) => a,
                            Err(e) => {
                                let anyhow_err: anyhow::Error = e.into();
                                return Err(DatenLordError::from(
                                    anyhow_err
                                        .context("load_data() failed to load file content data"),
                                ));
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
                        self.get_ino(),
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

        // delete from disk and close the handler
        match removed_entry.entry_type() {
            SFlag::S_IFDIR | SFlag::S_IFREG | SFlag::S_IFLNK => {
                let ino = removed_entry.ino();
                if let Err(e) = self.s3_backend.delete_data(ino).await {
                    panic!(
                        "failed to delete data of {ino} from s3 backend, error is {e:?}"
                    );
                }
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
            S3NodeData::Directory(..) | S3NodeData::RegFile(..) => {
                panic!("forbidden to read target path from non-symlink node")
            }
            S3NodeData::SymLink(ref target_path) => target_path,
        }
    }

    /// Fake data for statefs
    /// TODO: handle some important data from S3 storage
    async fn statefs(&self) -> DatenLordResult<StatFsParam> {
        Ok(StatFsParam {
            blocks: 10_000_000_000,
            bfree: 10_000_000_000,
            bavail: 10_000_000_000,
            files: 0, // TODO: file count is  temporarily zero
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
            S3NodeData::RegFile(ref cache) => cache.get_file_cache(self.get_ino(), offset, len),
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
    ) -> DatenLordResult<usize> {
        let this: &Self = self;

        let ino = this.get_ino();
        if this.need_load_file_data(offset.cast(), data.len()).await {
            let load_res = self.load_data(offset.cast(), data.len()).await;
            if let Err(e) = load_res {
                debug!(
                    "read() failed to load file data of ino={} and name={:?}, the error is: {:?}",
                    ino,
                    self.get_name(),
                    e,
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
                self.get_ino(),
                offset.cast(),
                data.len(),
                data.as_slice(),
                true,
            )
            .await;

        let written_size = data.len();

        {
            let mut attr_write = self.attr.write();
            // update the attribute of the written file
            attr_write.size = std::cmp::max(
                attr_write.size,
                offset.cast::<u64>().overflow_add(written_size.cast()),
            );
        };

        debug!("file {:?} size = {:?}", self.name, self.attr.read().size);
        self.update_mtime_ctime_to_now();
        // FileAttr changed, remember to persist the directory after calling this fn

        Ok(written_size)
    }

    async fn close(&mut self, ino: INum, _fh: u64, _flush: bool) {
        if let Err(e) = self.flush_all_data().await {
            panic!("failed to flush all data of {ino}, error is {e:?}");
        }
        self.dec_open_count();
    }

    /// TODO: push dir data to s3
    async fn closedir(&self, _ino: INum, _fh: u64) {
        self.dec_open_count();
    }

    async fn setattr_precheck(
        &self,
        param: SetAttrParam,
        user_id: u32,
        group_id: u32,
    ) -> DatenLordResult<(bool, FileAttr)> {
        let mut dirty_attr = self.get_attr();
        let cur_attr = self.get_attr();

        let st_now = SystemTime::now();
        let mut attr_changed = false;

        if let Some(gid) = param.g_id {
            if user_id != 0 && cur_attr.uid != user_id {
                return build_error_result_from_errno(
                    Errno::EPERM,
                    "setattr() cannot change gid".to_owned(),
                );
            }

            if cur_attr.gid != gid {
                dirty_attr.gid = gid;
                attr_changed = true;
            }
        }

        if let Some(uid) = param.u_id {
            if cur_attr.uid != uid {
                if user_id != 0 {
                    return build_error_result_from_errno(
                        Errno::EPERM,
                        "setattr() cannot change uid".to_owned(),
                    );
                }
                dirty_attr.uid = uid;
                attr_changed = true;
            }
        }

        if let Some(mode) = param.mode {
            let mode: u16 = mode.cast();
            if mode != cur_attr.perm {
                if user_id != 0 && user_id != cur_attr.uid {
                    return build_error_result_from_errno(
                        Errno::EPERM,
                        "setattr() cannot change mode".to_owned(),
                    );
                }
                dirty_attr.perm = mode;
                attr_changed = true;
            }
        }

        if let Some(atime) = param.a_time {
            //   owner is root check the ctx_uid
            if cur_attr.uid == 0 && user_id != 0 {
                return build_error_result_from_errno(
                    Errno::EPERM,
                    "setattr() cannot change atime".to_owned(),
                );
            }
            self.attr.read().check_perm(user_id, group_id, 2)?;
            if user_id != cur_attr.uid {
                return build_error_result_from_errno(
                    Errno::EACCES,
                    "setattr() cannot change atime".to_owned(),
                );
            }
            if atime != cur_attr.atime {
                dirty_attr.atime = atime;
                attr_changed = true;
            }
        }

        if let Some(mtime) = param.m_time {
            //  owner is root check the user_id
            if cur_attr.uid == 0 && user_id != 0 {
                return build_error_result_from_errno(
                    Errno::EPERM,
                    "setattr() cannot change atime".to_owned(),
                );
            }
            self.attr.read().check_perm(user_id, group_id, 2)?;
            if user_id != cur_attr.uid {
                return build_error_result_from_errno(
                    Errno::EACCES,
                    "setattr() cannot change atime".to_owned(),
                );
            }
            if mtime != cur_attr.mtime {
                dirty_attr.mtime = mtime;
                attr_changed = true;
            }
        }

        #[cfg(feature = "abi-7-23")]
        if let Some(c_time) = param.c_time {
            dirty_attr.ctime = c_time;
            panic!("c_time is not supported in this version of statefs")
            // TODO: how to change ctime directly on ext4?
        }

        if let Some(file_size) = param.size {
            dirty_attr.size = file_size;
            dirty_attr.mtime = st_now;
            attr_changed = true;
        }

        if attr_changed {
            dirty_attr.ctime = st_now;
        }

        Ok((attr_changed, dirty_attr))
    }
}
