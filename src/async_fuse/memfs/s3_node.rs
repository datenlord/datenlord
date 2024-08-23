//! The implementation of filesystem node

use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use clippy_utilities::Cast;
use nix::errno::Errno;
use nix::fcntl::OFlag;
use nix::sys::stat::{Mode, SFlag};
use nix::unistd;
use parking_lot::RwLock;
use tracing::info;

use super::direntry::{DirEntry, FileType};
use super::fs_util::{self, FileAttr};
use super::kv_engine::{KVEngineType, KeyType, MetaTxn, ValueType};
use super::node::Node;
use super::s3_metadata::S3MetaData;
use super::serial::{file_attr_to_serial, serial_to_file_attr, SerialNode, SerialNodeData};
use super::CreateParam;
use crate::async_fuse::fuse::fuse_reply::StatFsParam;
use crate::async_fuse::fuse::protocol::{INum, FUSE_ROOT_ID};
use crate::async_fuse::util::build_error_result_from_errno;
use crate::common::error::DatenLordResult;

/// A file node data or a directory node data
#[derive(Debug)]
pub enum S3NodeData {
    /// Directory entry data
    Directory,
    /// File content data
    RegFile,
    /// Symlink target data
    // SymLink(Box<SymLinkData>),
    SymLink(PathBuf),
}

impl S3NodeData {
    /// Serializes the node data
    pub fn serial(&self) -> SerialNodeData {
        match *self {
            Self::Directory => SerialNodeData::Directory,
            Self::RegFile => SerialNodeData::File,
            Self::SymLink(ref target) => SerialNodeData::SymLink(target.clone()),
        }
    }
}

/// A file node or a directory node
#[derive(Debug)]
pub struct S3Node {
    /// Parent node i-number
    parent: u64,
    /// S3Node name
    name: String,
    /// S3Node attribute
    attr: Arc<RwLock<FileAttr>>,
    /// S3Node data
    data: S3NodeData,
    /// S3Node lookup counter
    lookup_count: AtomicI64,
    /// If S3Node has been marked as deferred deletion
    deferred_deletion: AtomicBool,
    /// KVEngine
    kv_engine: Arc<KVEngineType>,
    /// K8s node id
    k8s_node_id: Arc<str>,
}

impl S3Node {
    #[allow(clippy::too_many_arguments)]
    /// Create `S3Node`
    fn new(
        parent: u64,
        name: &str,
        attr: Arc<RwLock<FileAttr>>,
        data: S3NodeData,
        kv_engine: &Arc<KVEngineType>,
        k8s_node_id: &Arc<str>,
    ) -> Self {
        Self {
            parent,
            name: name.to_owned(),
            attr,
            data,
            // lookup count set to 1 by creation
            lookup_count: AtomicI64::new(1),
            deferred_deletion: AtomicBool::new(false),
            kv_engine: Arc::clone(kv_engine),
            k8s_node_id: Arc::clone(k8s_node_id),
        }
    }

    /// Deserialize `S3Node` from `SerialNode`
    pub fn from_serial_node(serial_node: SerialNode, meta: &S3MetaData) -> S3Node {
        let dir_data = serial_node.data.into_s3_nodedata();
        info!(
            "ino={},lookup_count={},attr={:?}",
            serial_node.attr.get_ino(),
            serial_node.lookup_count,
            serial_node.attr
        );
        Self {
            parent: serial_node.parent,
            name: serial_node.name,
            attr: Arc::new(RwLock::new(serial_to_file_attr(&serial_node.attr))),
            data: dir_data,
            lookup_count: AtomicI64::new(serial_node.lookup_count),
            deferred_deletion: AtomicBool::new(serial_node.deferred_deletion),
            kv_engine: Arc::clone(&meta.kv_engine),
            k8s_node_id: Arc::clone(&meta.node_id),
        }
    }

    /// This function is used to create a new `SerialNode` by `S3Node`
    pub fn to_serial_node(&self) -> SerialNode {
        SerialNode {
            parent: self.parent,
            name: self.name.clone(),
            attr: file_attr_to_serial(&self.attr.read().clone()),
            data: self.data.serial(),
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
    ) -> Self {
        let data = match child_attr.read().kind {
            SFlag::S_IFDIR => S3NodeData::Directory,
            SFlag::S_IFREG => S3NodeData::RegFile,
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
            parent: parent.get_ino(),
            name: child_name.to_owned(),
            attr: child_attr,
            data,
            // lookup count set to 0 for sync
            lookup_count: AtomicI64::new(0),
            deferred_deletion: AtomicBool::new(false),
            kv_engine: Arc::clone(&parent.kv_engine),
            k8s_node_id: Arc::clone(&parent.k8s_node_id),
        }
    }

    /// Update mtime and ctime to now
    pub fn update_mtime_ctime_to_now(&mut self) {
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

    /// Open root node
    #[allow(clippy::unnecessary_wraps)]
    pub(crate) async fn open_root_node(
        root_ino: INum,
        name: &str,
        meta: Arc<S3MetaData>,
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
                kind: SFlag::S_IFDIR,
                uid: unistd::getuid().as_raw(),
                gid: unistd::getgid().as_raw(),
                ..FileAttr::default()
            }));

            let root_node = Self::new(
                root_ino,
                name,
                attr,
                S3NodeData::Directory,
                &meta.kv_engine,
                &meta.node_id,
            );
            Ok(root_node)
        }
    }

    /// Check if node is a directory
    /// # Return
    /// - `Ok(())` if node is a directory
    /// - `Err` if node is not a directory
    pub fn check_is_dir(&self) -> DatenLordResult<()> {
        if self.get_attr().kind != SFlag::S_IFDIR {
            return build_error_result_from_errno(
                Errno::ENOTDIR,
                format!(
                    "check_is_dir() failed as the node {} is not a directory",
                    self.get_name()
                ),
            );
        }
        Ok(())
    }

    /// Check if given uid and gid can access this node
    pub fn open_pre_check(&self, flags: OFlag, uid: u32, gid: u32) -> DatenLordResult<()> {
        let attr = self.get_attr();
        let access_mode = match flags & (OFlag::O_RDONLY | OFlag::O_WRONLY | OFlag::O_RDWR) {
            OFlag::O_RDONLY => 4,
            OFlag::O_WRONLY => 2,
            _ => 6,
        };
        attr.check_perm(uid, gid, access_mode)
    }
}

#[async_trait]
impl Node for S3Node {
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
            S3NodeData::Directory => SFlag::S_IFDIR,
            S3NodeData::RegFile => SFlag::S_IFREG,
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
        let old_attr = self.get_attr();
        match self.data {
            S3NodeData::Directory => debug_assert_eq!(new_attr.kind, SFlag::S_IFDIR),
            S3NodeData::RegFile => debug_assert_eq!(new_attr.kind, SFlag::S_IFREG),
            S3NodeData::SymLink(..) => debug_assert_eq!(new_attr.kind, SFlag::S_IFLNK),
        }

        self.attr.write().clone_from(&new_attr);
        old_attr
    }

    /// Get node attribute and increase lookup count
    fn lookup_attr(&self) -> FileAttr {
        let attr = self.get_attr();
        self.inc_lookup_count();
        attr
    }

    /// Get node lookup count
    fn get_lookup_count(&self) -> i64 {
        self.lookup_count.load(Ordering::Acquire)
    }

    /// Decrease node lookup count
    fn dec_lookup_count_by(&self, nlookup: u64) -> i64 {
        #[cfg(debug)]
        {
            let current_cnt = self.lookup_count.load(Ordering::Acquire);
            debug_assert!(
                current_cnt >= nlookup.cast(),
                "current_cnt={} is less than nlookup={}",
                current_cnt,
                nlookup
            );
        }
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

    /// Create symlink in a directory
    async fn create_child_symlink<T: MetaTxn + ?Sized>(
        &mut self,
        inum: INum,
        child_symlink_name: &str,
        target_path: PathBuf,
        txn: &mut T,
    ) -> DatenLordResult<Self> {
        let target_str = target_path
            .to_str()
            .unwrap_or_else(|| panic!("failed to convert {target_path:?} to utf8 string"));

        // get symbol file attribute
        let child_attr = Arc::new(RwLock::new(FileAttr {
            ino: inum,
            kind: SFlag::S_IFLNK,
            size: target_str.len().cast(),
            blocks: 0,
            perm: 0o777,
            ..FileAttr::now()
        }));

        let new_entry = DirEntry::new(inum, child_symlink_name.to_owned(), FileType::Symlink);

        txn.set(
            &KeyType::DirEntryKey((self.get_ino(), child_symlink_name.to_owned())),
            &ValueType::DirEntry(new_entry),
        );

        self.update_mtime_ctime_to_now();
        Ok(Self::new(
            self.get_ino(),
            child_symlink_name,
            child_attr,
            S3NodeData::SymLink(target_path),
            &self.kv_engine,
            &self.k8s_node_id,
        ))
    }

    /// Create sub-directory in a directory
    async fn create_child_dir<T: MetaTxn + ?Sized>(
        &mut self,
        inum: INum,
        child_dir_name: &str,
        mode: Mode,
        uid: u32,
        gid: u32,
        txn: &mut T,
    ) -> DatenLordResult<Self> {
        // get new directory attribute
        let child_attr = Arc::new(RwLock::new(FileAttr {
            ino: inum,
            kind: SFlag::S_IFDIR,
            perm: fs_util::parse_mode_bits(mode.bits()),
            uid,
            gid,
            nlink: 1,
            ..FileAttr::now()
        }));

        let new_etnry = DirEntry::new(inum, child_dir_name.to_owned(), FileType::Dir);

        txn.set(
            &KeyType::DirEntryKey((self.get_ino(), child_dir_name.to_owned())),
            &ValueType::DirEntry(new_etnry),
        );

        let child_node = Self::new(
            self.get_ino(),
            child_dir_name,
            child_attr,
            S3NodeData::Directory,
            &self.kv_engine,
            &self.k8s_node_id,
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
    ) -> DatenLordResult<Self> {
        Ok(Self::new(
            self.get_ino(),
            child_file_name,
            child_attr,
            S3NodeData::RegFile,
            &self.kv_engine,
            &self.k8s_node_id,
        ))
    }

    /// Create file in a directory
    async fn create_child_file<T: MetaTxn + ?Sized>(
        &mut self,
        inum: INum,
        child_file_name: &str,
        oflags: OFlag,
        mode: Mode,
        uid: u32,
        gid: u32,
        txn: &mut T,
    ) -> DatenLordResult<Self> {
        debug_assert!(oflags.contains(OFlag::O_CREAT));

        // get new file attribute
        let child_attr = Arc::new(RwLock::new(FileAttr {
            ino: inum,
            kind: SFlag::S_IFREG,
            perm: fs_util::parse_mode_bits(mode.bits()),
            size: 0,
            blocks: 0,
            uid,
            gid,
            nlink: 1,
            ..FileAttr::now()
        }));
        debug_assert_eq!(SFlag::S_IFREG, child_attr.read().kind);

        let new_etnry = DirEntry::new(inum, child_file_name.to_owned(), FileType::File);

        txn.set(
            &KeyType::DirEntryKey((self.get_ino(), child_file_name.to_owned())),
            &ValueType::DirEntry(new_etnry),
        );

        self.update_mtime_ctime_to_now();
        Ok(Self::new(
            self.get_ino(),
            child_file_name,
            child_attr,
            S3NodeData::RegFile,
            &self.kv_engine,
            &self.k8s_node_id,
        ))
    }

    /// Get symlink target path
    fn get_symlink_target(&self) -> &Path {
        match self.data {
            S3NodeData::Directory | S3NodeData::RegFile => {
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

    /// Create child node
    /// TODO: refactor the create_child_xxx() functions
    /// to reduce the duplicated code
    async fn create_child_node<T: MetaTxn + ?Sized>(
        &mut self,
        param: &CreateParam,
        new_inum: INum,
        txn: &mut T,
    ) -> DatenLordResult<Self> {
        let child_name = param.name.as_str();
        let m_flags = fs_util::parse_mode(param.mode);
        match param.node_type {
            SFlag::S_IFDIR => {
                let child_node = self
                    .create_child_dir(new_inum, child_name, m_flags, param.uid, param.gid, txn)
                    .await?;
                Ok(child_node)
            }
            SFlag::S_IFREG => {
                let o_flags = OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_RDWR;
                let child_node = self
                    .create_child_file(
                        new_inum, child_name, o_flags, m_flags, param.uid, param.gid, txn,
                    )
                    .await?;
                Ok(child_node)
            }
            SFlag::S_IFLNK => {
                let target_path = match param.link {
                    Some(ref link) => link.to_owned(),
                    None => panic!("create_child_node() found link is None"),
                };
                let child_node = self
                    .create_child_symlink(new_inum, child_name, target_path, txn)
                    .await?;
                Ok(child_node)
            }
            _ => unreachable!("create_child_node() found unsupported node type"),
        }
    }
}
