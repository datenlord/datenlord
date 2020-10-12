//! The implementation of filesystem node

use anyhow::Context;
use log::debug;
use nix::fcntl::{self, FcntlArg, OFlag};
use nix::sys::stat::SFlag;
use nix::sys::stat::{self, Mode};
use nix::unistd;
use smol::blocking;
use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{self, AtomicI64};
use std::time::SystemTime;
use utilities::{Cast, OverflowArithmetic};

use super::super::protocol::INum;
use super::dir::DirEntry;
use super::util::{self, FileAttr};

// /// The symlink target node data
// #[derive(Debug)]
// pub enum SymLinkTargetData {
//     /// Target directory entry data
//     Dir(RawFd, FileAttr, BTreeMap<OsString, DirEntry>),
//     /// Target file content data
//     File(RawFd, FileAttr, Vec<u8>),
// }

// impl SymLinkTargetData {
//     /// Get the size of symlink target data
//     fn size(&self) -> usize {
//         match &self {
//             Self::Dir(_, _, m) => m.len(),
//             Self::File(_, _, v) => v.len(),
//         }
//     }

//     // /// Get the attribute of target node
//     // const fn get_attr(&self) -> FileAttr {
//     //     match self {
//     //         Self::Dir(_, attr, _) => *attr,
//     //         Self::File(_, attr, _) => *attr,
//     //     }
//     // }

//     /// Get symlink target file data
//     pub fn get_file_data(&self) -> &Vec<u8> {
//         match self {
//             Self::Dir(..) => panic!("forbidden to get file data from symlink target directory"),
//             Self::File(_, _, v) => v,
//         }
//     }

//     // /// Get symlink target directory data
//     // pub fn get_dir_data(&self) -> &BTreeMap<OsString, DirEntry> {
//     //     match self {
//     //         Self::Dir(_, _, m) => m,
//     //         Self::File(..) => panic!("forbidden to get directory data from symlink target file"),
//     //     }
//     // }
// }

// /// The symlink node data
// #[derive(Debug)]
// struct SymLinkData {
//     /// The target path of symlink
//     target_path: PathBuf,
//     /// The target node data of symlink, could be none for broken symlink
//     target_data: Option<SymLinkTargetData>,
// }

// impl SymLinkData {
//     /// Create `SymLinkData`
//     async fn new(symlink_fd: RawFd, target_path: PathBuf) -> Self {
//         let target_attr_res = util::load_symlink_target_attr(symlink_fd, target_path.clone()).await;
//         match target_attr_res {
//             Ok(target_attr) => {
//                 let target_data = match target_attr.kind {
//                     SFlag::S_IFDIR => {
//                         let oflags = util::get_dir_oflags();
//                         let target_path_clone = target_path.clone();
//                         let target_dir_fd = blocking!(fcntl::openat(
//                             symlink_fd,
//                             target_path_clone.as_os_str(),
//                             oflags,
//                             Mode::empty()
//                         ))
//                         .context(format!(
//                             "SymLinkData::new() failed to open symlink target directory={:?}",
//                             target_path,
//                         ))
//                         .unwrap_or_else(|err| {
//                             panic!(
//                                 "SymLinkData::new() failed, the error is: {}",
//                                 util::format_anyhow_error(&err)
//                             )
//                         });
//                         Some(SymLinkTargetData::Dir(
//                             target_dir_fd,
//                             target_attr,
//                             BTreeMap::new(),
//                         ))
//                     }
//                     SFlag::S_IFREG => {
//                         let oflags = OFlag::O_RDWR;
//                         let target_path_clone = target_path.clone();
//                         let target_file_fd = blocking!(fcntl::openat(
//                             symlink_fd,
//                             target_path_clone.as_os_str(),
//                             oflags,
//                             Mode::empty()
//                         ))
//                         .context(format!(
//                             "SymLinkData::new() failed to open symlink target file={:?}",
//                             target_path,
//                         ))
//                         .unwrap_or_else(|err| {
//                             panic!(
//                                 "SymLinkData::new() failed, the error is: {}",
//                                 util::format_anyhow_error(&err)
//                             )
//                         });
//                         Some(SymLinkTargetData::File(
//                             target_file_fd,
//                             target_attr,
//                             Vec::new(),
//                         ))
//                     }
//                     _ => {
//                         panic!("unsupported symlink target type={:?}", target_attr.kind);
//                         // None
//                     }
//                 };
//                 Self {
//                     target_path,
//                     target_data,
//                 }
//             }
//             Err(e) => {
//                 debug!(
//                     "SymLinkData::new() failed to get the symlink target node attribute, \
//                     the error is: {}",
//                     util::format_anyhow_error(&e),
//                 );
//                 Self {
//                     target_path,
//                     target_data: None,
//                 }
//             }
//         }
//     }

//     /// Build `SymLinkData`
//     const fn from(target_path: PathBuf, target_data: SymLinkTargetData) -> Self {
//         Self {
//             target_path,
//             target_data: Some(target_data),
//         }
//     }
// }

/// A file node data or a directory node data
#[derive(Debug)]
enum NodeData {
    /// Directory entry data
    Directory(BTreeMap<OsString, DirEntry>),
    /// File content data
    RegFile(Vec<u8>),
    /// Symlink target data
    // SymLink(Box<SymLinkData>),
    SymLink(PathBuf),
}

/// A file node or a directory node
#[derive(Debug)]
pub struct Node {
    /// Parent node i-number
    parent: u64,
    /// Node name
    name: OsString,
    /// Node attribute
    attr: FileAttr,
    /// Node data
    data: NodeData,
    /// Node fd
    fd: RawFd,
    /// Node open counter
    open_count: AtomicI64,
    /// Node lookup counter
    lookup_count: AtomicI64,
}

impl Drop for Node {
    fn drop(&mut self) {
        // if INVALID_RAW_FD == self.fd {
        //     debug_assert_eq!(
        //         self.get_type(),
        //         SFlag::S_IFLNK,
        //         "only symlink should have invalid fd, other than {:?} type",
        //         self.get_type(),
        //     );
        //     debug!("no need to close the fd of symlink");
        // } else {
        // TODO: check unsaved data in cache
        unistd::close(self.fd).unwrap_or_else(|err| {
            panic!(
                "Node::drop() failed to clode the file handler \
                    of the node name={:?} ino={}, the error is: {}",
                self.name, self.attr.ino, err,
            );
        });
        // }
    }
}

impl Node {
    /// Create `Node`
    const fn new(parent: u64, name: OsString, attr: FileAttr, data: NodeData, fd: RawFd) -> Self {
        Self {
            parent,
            name,
            attr,
            data,
            fd,
            // lookup count set to 1 by creation
            open_count: AtomicI64::new(1),
            // open count set to 1 by creation
            lookup_count: AtomicI64::new(1),
        }
    }

    /// Get node i-number
    #[inline]
    pub const fn get_ino(&self) -> INum {
        self.get_attr().ino
    }

    /// Get node fd
    #[inline]
    pub const fn get_fd(&self) -> RawFd {
        self.fd
    }

    /// Get parent node i-number
    pub const fn get_parent_ino(&self) -> INum {
        self.parent
    }

    /// Set node parent
    pub fn set_parent_ino(&mut self, parent: u64) -> INum {
        let old_parent = self.parent;
        self.parent = parent;
        old_parent
    }

    /// Get node name
    pub fn get_name(&self) -> &OsStr {
        self.name.as_os_str()
    }

    /// Set node name
    pub fn set_name(&mut self, name: OsString) {
        self.name = name;
    }

    /// Get node type, directory or file
    pub const fn get_type(&self) -> SFlag {
        match &self.data {
            NodeData::Directory(..) => SFlag::S_IFDIR,
            NodeData::RegFile(..) => SFlag::S_IFREG,
            NodeData::SymLink(..) => SFlag::S_IFLNK,
        }
    }

    /// Get node attribute
    pub const fn get_attr(&self) -> FileAttr {
        self.attr
    }

    /// Set node attribute
    pub fn set_attr(&mut self, new_attr: FileAttr) -> FileAttr {
        let old_attr = self.get_attr();
        match &self.data {
            NodeData::Directory(..) => debug_assert_eq!(new_attr.kind, SFlag::S_IFDIR),
            NodeData::RegFile(..) => debug_assert_eq!(new_attr.kind, SFlag::S_IFREG),
            NodeData::SymLink(..) => debug_assert_eq!(new_attr.kind, SFlag::S_IFLNK),
        }
        self.attr = new_attr;
        old_attr
    }

    /// Update mtime and ctime to now
    fn update_mtime_ctime_to_now(&mut self) {
        let mut attr = self.get_attr();
        let st_now = SystemTime::now();
        attr.mtime = st_now;
        attr.ctime = st_now;
        self.set_attr(attr);
    }

    /// Get node attribute and increase lookup count
    pub fn lookup_attr(&self) -> FileAttr {
        let attr = self.get_attr();
        self.inc_lookup_count();
        attr
    }

    /// Get node open count
    pub fn get_open_count(&self) -> i64 {
        self.open_count.load(atomic::Ordering::Relaxed)
    }

    /// Increase node open count
    fn inc_open_count(&self) -> i64 {
        self.open_count.fetch_add(1, atomic::Ordering::Relaxed)
    }

    /// Decrease node open count
    pub fn dec_open_count(&self) -> i64 {
        self.open_count.fetch_sub(1, atomic::Ordering::Relaxed)
    }

    /// Get node lookup count
    pub fn get_lookup_count(&self) -> i64 {
        self.lookup_count.load(atomic::Ordering::Relaxed)
    }

    /// Increase node lookup count
    fn inc_lookup_count(&self) -> i64 {
        self.lookup_count.fetch_add(1, atomic::Ordering::Relaxed)
    }

    /// Decrease node lookup count
    pub fn dec_lookup_count_by(&self, nlookup: u64) -> i64 {
        debug_assert!(nlookup < std::i64::MAX.cast());
        self.lookup_count
            .fetch_sub(nlookup.cast(), atomic::Ordering::Relaxed)
    }

    /// Load attribute
    pub async fn load_attribute(&self) -> anyhow::Result<FileAttr> {
        let attr = util::load_attr(self.fd).await.context(format!(
            "load_attribute() failed to get the attribute of the node ino={}",
            self.get_ino(),
        ))?;
        match &self.data {
            NodeData::Directory(..) => debug_assert_eq!(SFlag::S_IFDIR, attr.kind),
            NodeData::RegFile(..) => debug_assert_eq!(SFlag::S_IFREG, attr.kind),
            NodeData::SymLink(..) => debug_assert_eq!(SFlag::S_IFLNK, attr.kind),
        };
        Ok(attr)
    }

    /// Duplicate fd
    pub async fn dup_fd(&self, oflags: OFlag) -> anyhow::Result<RawFd> {
        let raw_fd = self.fd;
        let ino = self.get_ino();
        let new_fd = blocking!(unistd::dup(raw_fd)).context(format!(
            "dup_fd() failed to duplicate the handler ino={} raw fd={:?}",
            ino, raw_fd,
        ))?;
        // increase open count once dup() success
        self.inc_open_count();

        let fcntl_oflags = FcntlArg::F_SETFL(oflags);
        blocking!(fcntl::fcntl(new_fd, fcntl_oflags))
            .context(format!(
                "dup_fd() failed to set the flags={:?} of duplicated handler of ino={}",
                oflags, ino,
            ))
            .unwrap_or_else(|err| {
                panic!(
                    "failed to duplicate fd, the error is: {}",
                    util::format_anyhow_error(&err),
                )
            });
        // blocking!(unistd::dup3(raw_fd, new_fd, oflags)).context(format!(
        //     "dup_fd() failed to set the flags={:?} of duplicated handler of ino={}",
        //     oflags, ino,
        // ))?;
        Ok(new_fd)
    }

    /// Check whether a node is an empty file or an empty directory
    pub fn is_node_data_empty(&self) -> bool {
        match &self.data {
            NodeData::Directory(dir_node) => dir_node.is_empty(),
            NodeData::RegFile(file_node) => file_node.is_empty(),
            NodeData::SymLink(..) => panic!("forbidden to check symlink is empty or not"),
        }
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

    /// check whether to load directory entry data or not
    pub fn need_load_dir_data(&self) -> bool {
        debug_assert_eq!(
            self.attr.kind,
            SFlag::S_IFDIR,
            "fobidden to check non-directory node need load data or not",
        );
        self.need_load_node_data_helper()
    }

    /// Check whether to load file content data or not
    pub fn need_load_file_data(&self) -> bool {
        debug_assert_eq!(
            self.attr.kind,
            SFlag::S_IFREG,
            "fobidden to check non-file node need load data or not",
        );
        self.need_load_node_data_helper()
    }

    // /// Check whether to load symlink target data or not
    // pub fn need_load_symlink_target_data(&self) -> bool {
    //     debug_assert_eq!(
    //         self.attr.kind,
    //         SFlag::S_IFLNK,
    //         "fobidden to check non-symlink node need load data or not",
    //     );
    //     self.need_load_node_data_helper()
    // }

    // Directory only methods

    /// Get directory data
    fn get_dir_data(&self) -> &BTreeMap<OsString, DirEntry> {
        match &self.data {
            NodeData::Directory(dir_data) => dir_data,
            NodeData::RegFile(..) | NodeData::SymLink(..) => {
                panic!("forbidden to get DirData from non-directory node")
            }
        }
    }

    /// Get mutable directory data
    fn get_dir_data_mut(&mut self) -> &mut BTreeMap<OsString, DirEntry> {
        match &mut self.data {
            NodeData::Directory(dir_data) => dir_data,
            NodeData::RegFile(..) | NodeData::SymLink(..) => {
                panic!("forbidden to get DirData from non-directory node")
            }
        }
    }

    /// Get a directory entry by name
    pub fn get_entry(&self, name: &OsStr) -> Option<&DirEntry> {
        self.get_dir_data().get(name)
    }

    /// Helper function to create or read symlink itself in a directory
    pub async fn create_or_load_child_symlink_helper(
        &mut self,
        child_symlink_name: OsString,
        target_path_opt: Option<PathBuf>, // If not None, create symlink
    ) -> anyhow::Result<Self> {
        let ino = self.get_ino();
        let fd = self.fd;
        let dir_data = self.get_dir_data_mut();
        if let Some(target_path) = &target_path_opt {
            debug_assert!(
                !dir_data.contains_key(&child_symlink_name),
                "create_or_load_child_symlink_helper() cannot create duplicated symlink name={:?}",
                child_symlink_name,
            );
            let child_symlink_name_clone = child_symlink_name.clone();
            let target_path_clone = target_path.clone();
            blocking!(unistd::symlinkat(
                &target_path_clone,
                Some(fd),
                child_symlink_name_clone.as_os_str()
            ))
            .context(format!(
                "create_or_load_child_symlink_helper() failed to create symlink \
                    name={:?} to target path={:?} under parent ino={}",
                child_symlink_name, target_path, ino,
            ))?;
        };

        let child_symlink_name_clone = child_symlink_name.clone();
        let child_fd = blocking!(fcntl::openat(
            fd,
            child_symlink_name_clone.as_os_str(),
            OFlag::O_PATH | OFlag::O_NOFOLLOW,
            Mode::all(),
        ))
        .context(format!(
            "create_or_load_child_symlink_helper() failed to open symlink itself with name={:?} \
                under parent ino={}",
            child_symlink_name, ino,
        ))?;
        let child_attr = util::load_attr(child_fd)
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
                child_symlink_name.clone(),
                DirEntry::new(child_attr.ino, child_symlink_name.clone(), SFlag::S_IFLNK),
            );
            debug_assert!(previous_value.is_none()); // double check creation race
            target_path
        } else {
            let child_symlink_name_clone = child_symlink_name.clone();
            let target_path_osstr =
                blocking!(fcntl::readlinkat(fd, child_symlink_name_clone.as_os_str())).context(
                    format!(
                        "create_or_load_child_symlink_helper() failed to open \
                            the new directory name={:?} under parent ino={}",
                        child_symlink_name, ino,
                    ),
                )?;
            Path::new(&target_path_osstr).to_owned()
        };

        Ok(Self::new(
            self.get_ino(),
            child_symlink_name,
            child_attr,
            // NodeData::SymLink(Box::new(SymLinkData::new(child_fd, target_path).await)),
            NodeData::SymLink(target_path),
            child_fd,
        ))
    }

    /// Create symlink in a directory
    pub async fn create_child_symlink(
        &mut self,
        child_symlink_name: OsString,
        target_path: PathBuf,
    ) -> anyhow::Result<Self> {
        let create_res = self
            .create_or_load_child_symlink_helper(child_symlink_name, Some(target_path))
            .await;
        if create_res.is_ok() {
            self.update_mtime_ctime_to_now();
        }
        create_res
    }

    /// Read symlink itself in a directory, not follow symlink
    pub async fn load_child_symlink(
        &mut self,
        child_symlink_name: OsString,
    ) -> anyhow::Result<Self> {
        self.create_or_load_child_symlink_helper(child_symlink_name, None)
            .await
    }

    /// Helper function to create or open sub-directory in a directory
    async fn open_child_dir_helper(
        &mut self,
        child_dir_name: OsString,
        mode: Mode,
        create_dir: bool,
    ) -> anyhow::Result<Self> {
        let ino = self.get_ino();
        let fd = self.fd;
        let dir_data = self.get_dir_data_mut();
        if create_dir {
            debug_assert!(
                !dir_data.contains_key(&child_dir_name),
                "open_child_dir_helper() cannot create duplicated directory name={:?}",
                child_dir_name
            );
            let child_dir_name_clone = child_dir_name.clone();
            blocking!(stat::mkdirat(fd, child_dir_name_clone.as_os_str(), mode)).context(
                format!(
                    "open_child_dir_helper() failed to create directory \
                        name={:?} under parent ino={}",
                    child_dir_name, ino,
                ),
            )?;
        }

        let child_dir_name_clone = child_dir_name.clone();
        let child_raw_fd = util::open_dir_at(fd, child_dir_name_clone)
            .await
            .context(format!(
                "open_child_dir_helper() failed to open the new directory name={:?} \
                    under parent ino={}",
                child_dir_name, ino,
            ))?;

        // get new directory attribute
        let child_attr = util::load_attr(child_raw_fd).await.context(format!(
            "open_child_dir_helper() failed to get the attribute of the new child directory={:?}",
            child_dir_name,
        ))?;
        debug_assert_eq!(SFlag::S_IFDIR, child_attr.kind);

        if create_dir {
            // insert new entry to parent directory
            // TODO: support thread-safe
            let previous_value = dir_data.insert(
                child_dir_name.clone(),
                DirEntry::new(child_attr.ino, child_dir_name.clone(), SFlag::S_IFDIR),
            );
            debug_assert!(previous_value.is_none()); // double check creation race
        }

        // lookup count and open count are increased to 1 by creation
        let child_node = Self::new(
            self.get_ino(),
            child_dir_name,
            child_attr,
            NodeData::Directory(BTreeMap::new()),
            child_raw_fd,
        );

        // if !create_dir {
        //     // load directory data on open
        //     child_node
        //         .load_data()
        //         .await
        //         .context("open_child_dir_helper() failed to load child directory entry data")?;
        // }
        Ok(child_node)
    }

    /// Open sub-directory in a directory
    pub async fn open_child_dir(&mut self, child_dir_name: OsString) -> anyhow::Result<Self> {
        self.open_child_dir_helper(
            child_dir_name,
            Mode::empty(),
            false, // create_dir
        )
        .await
    }

    /// Create sub-directory in a directory
    pub async fn create_child_dir(
        &mut self,
        child_dir_name: OsString,
        mode: Mode,
    ) -> anyhow::Result<Self> {
        let create_res = self
            .open_child_dir_helper(
                child_dir_name,
                mode,
                true, // create_dir
            )
            .await;
        if create_res.is_ok() {
            self.update_mtime_ctime_to_now();
        }
        create_res
    }

    /// Helper function to open or create file in a directory
    async fn open_child_file_helper(
        &mut self,
        child_file_name: OsString,
        oflags: OFlag,
        mode: Mode,
        create_file: bool,
    ) -> anyhow::Result<Self> {
        let ino = self.get_ino();
        let fd = self.fd;
        let dir_data = self.get_dir_data_mut();
        if create_file {
            debug_assert!(
                !dir_data.contains_key(&child_file_name),
                "open_child_file_helper() cannot create duplicated file name={:?}",
                child_file_name
            );
            debug_assert!(oflags.contains(OFlag::O_CREAT));
        }
        let child_file_name_clone = child_file_name.clone();
        let child_fd = blocking!(fcntl::openat(
            fd,
            child_file_name_clone.as_os_str(),
            oflags,
            mode
        ))
        .context(format!(
            "open_child_file_helper() failed to open a file name={:?} \
                under parent ino={} with oflags={:?} and mode={:?}",
            child_file_name, ino, oflags, mode,
        ))?;

        // get new file attribute
        let child_attr = util::load_attr(child_fd).await.context(
            "open_child_file_helper() failed to get the attribute of the new child".to_string(),
        )?;
        debug_assert_eq!(SFlag::S_IFREG, child_attr.kind);

        if create_file {
            // insert new entry to parent directory
            // TODO: support thread-safe
            let previous_value = dir_data.insert(
                child_file_name.clone(),
                DirEntry::new(child_attr.ino, child_file_name.clone(), SFlag::S_IFREG),
            );
            debug_assert!(previous_value.is_none()); // double check creation race
        }

        Ok(Self::new(
            self.get_ino(),
            child_file_name,
            child_attr,
            NodeData::RegFile(Vec::new()),
            child_fd,
        ))
    }

    /// Open file in a directory
    pub async fn open_child_file(
        &mut self,
        child_file_name: OsString,
        oflags: OFlag,
    ) -> anyhow::Result<Self> {
        self.open_child_file_helper(
            child_file_name,
            oflags,
            Mode::empty(),
            false, // create
        )
        .await
    }

    /// Create file in a directory
    pub async fn create_child_file(
        &mut self,
        child_file_name: OsString,
        oflags: OFlag,
        mode: Mode,
    ) -> anyhow::Result<Self> {
        let create_res = self
            .open_child_file_helper(
                child_file_name,
                oflags,
                mode,
                true, // create
            )
            .await;
        if create_res.is_ok() {
            self.update_mtime_ctime_to_now();
        }
        create_res
    }

    // TODO: improve `load_data`, do not load all file content and directory entries at once
    /// Load data from directory, file or symlink target
    pub async fn load_data(&mut self) -> anyhow::Result<usize> {
        match &mut self.data {
            NodeData::Directory(..) => {
                // let dir_entry_map = self.load_dir_data_helper().await?;
                let dir_entry_map = util::load_dir_data(self.get_fd())
                    .await
                    .context("load_data() failed to load directory entry data")?;
                let entry_count = dir_entry_map.len();
                self.data = NodeData::Directory(dir_entry_map);
                debug!(
                    "load_data() successfully load {} directory entries",
                    entry_count
                );
                Ok(entry_count)
            }
            NodeData::RegFile(..) => {
                // let file_data_vec = self.load_file_data_helper().await?;
                let file_data_vec =
                    util::load_file_data(self.get_fd(), self.get_attr().size.cast())
                        .await
                        .context("load_data() failed to load file content data")?;
                let read_size = file_data_vec.len();
                self.data = NodeData::RegFile(file_data_vec);
                debug!(
                    "load_data() successfully load {} byte file content data",
                    read_size
                );
                Ok(read_size)
            }
            NodeData::SymLink(..) => {
                panic!("forbidden to load symlink target data");
                // let target_data = self
                //     .load_symlink_target_helper()
                //     .await
                //     .context("load_data() failed to load symlink target node data")?;
                // let data_size = target_data.size();
                // self.data = NodeData::SymLink(Box::new(SymLinkData::from(
                //     self.get_symlink_target().to_owned(),
                //     target_data,
                // )));
                // debug!("load_data() successfully load symlink target node data");
                // Ok(data_size)
            }
        }
    }

    /// Insert directory entry for rename()
    pub fn insert_entry_for_rename(&mut self, child_entry: DirEntry) -> Option<DirEntry> {
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
    pub fn remove_entry_for_rename(&mut self, child_name: &OsStr) -> Option<DirEntry> {
        let dir_data = self.get_dir_data_mut();
        let remove_res = dir_data.remove(child_name);
        if remove_res.is_some() {
            self.update_mtime_ctime_to_now();
        }
        remove_res
    }

    /// Unlink directory entry from both cache and disk
    pub async fn unlink_entry(&mut self, child_name: OsString) -> anyhow::Result<DirEntry> {
        let dir_data = self.get_dir_data_mut();
        let removed_entry = dir_data.remove(child_name.as_os_str()).unwrap_or_else(|| {
            panic!(
                "unlink_entry() found fs is inconsistent, the entry of name={:?} \
                    is not in directory of name={:?} and ino={}",
                child_name,
                self.get_name(),
                self.get_ino(),
            );
        });
        let child_name_clone = child_name.clone();
        let fd = self.fd;
        // delete from disk and close the handler
        match removed_entry.entry_type() {
            SFlag::S_IFDIR => {
                blocking!(unistd::unlinkat(
                    Some(fd),
                    child_name.as_os_str(),
                    unistd::UnlinkatFlags::RemoveDir,
                ))
                .context(format!(
                    "unlink_entry() failed to delete the file name={:?} from disk",
                    child_name_clone,
                ))?;
            }
            SFlag::S_IFREG | SFlag::S_IFLNK => {
                blocking!(unistd::unlinkat(
                    Some(fd),
                    child_name.as_os_str(),
                    unistd::UnlinkatFlags::NoRemoveDir,
                ))
                .context(format!(
                    "unlink_entry() failed to delete the file name={:?} from disk",
                    child_name_clone,
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
    pub fn read_dir(&self, func: impl FnOnce(&BTreeMap<OsString, DirEntry>) -> usize) -> usize {
        let dir_data = self.get_dir_data();
        func(dir_data)
    }

    // Symlink only methods

    /// Get symlink target path
    pub fn get_symlink_target(&self) -> &Path {
        match &self.data {
            NodeData::Directory(..) | NodeData::RegFile(..) => {
                panic!("forbidden to read target path from non-symlink node")
            }
            // NodeData::SymLink(symlink_data) => &symlink_data.target_path,
            NodeData::SymLink(target_path) => target_path,
        }
    }

    // /// Get symlink target node data
    // pub fn get_symlink_target_data(&self) -> Option<&SymLinkTargetData> {
    //     match &self.data {
    //         NodeData::Directory(..) | NodeData::RegFile(..) => {
    //             panic!("forbidden to get target data from non-symlink node")
    //         }
    //         NodeData::SymLink(symlink_data) => symlink_data.target_data.as_ref(),
    //     }
    // }

    // /// Helper function to load symlink target node data
    // async fn load_symlink_target_helper(&self) -> anyhow::Result<SymLinkTargetData> {
    //     let target_path = self.get_symlink_target().to_owned();
    //     let target_data_res = self.get_symlink_target_data();

    //     if let Some(target_data) = target_data_res {
    //         match target_data {
    //             SymLinkTargetData::Dir(target_dir_fd, target_attr, _) => {
    //                 debug_assert_eq!(
    //                     target_attr.kind,
    //                     SFlag::S_IFDIR,
    //                     "symlink target node should be a directory",
    //                 );
    //                 let target_dir_data =
    //                     util::load_dir_data(*target_dir_fd).await.context(format!(
    //                         "load_symlink_target_helper() failed to load entry data from \
    //                             symlink target directory={:?}",
    //                         target_path,
    //                     ))?;
    //                 Ok(SymLinkTargetData::Dir(
    //                     *target_dir_fd,
    //                     *target_attr,
    //                     target_dir_data,
    //                 ))
    //             }
    //             SymLinkTargetData::File(target_file_fd, target_attr, _) => {
    //                 debug_assert_eq!(
    //                     target_attr.kind,
    //                     SFlag::S_IFREG,
    //                     "symlink target node should be a file",
    //                 );
    //                 let target_file_data =
    //                     util::load_file_data(*target_file_fd, target_attr.size.cast())
    //                         .await
    //                         .context(format!(
    //                     "load_symlink_target_helper() failed to load file content data from \
    //                                 symlink target directory={:?}",
    //                     target_path,
    //                 ))?;
    //                 Ok(SymLinkTargetData::File(
    //                     *target_file_fd,
    //                     *target_attr,
    //                     target_file_data,
    //                 ))
    //             }
    //         }
    //     } else {
    //         util::build_error_result_from_errno(
    //             nix::errno::Errno::ENOENT,
    //             format!(
    //                 "load_symlink_target_helper() failed to open broken symlink target={:?}",
    //                 target_path,
    //             ),
    //         )
    //     }
    // }

    // /// Open symlink target path
    // pub async fn open_symlink_target(&self, oflags: OFlag) -> anyhow::Result<RawFd> {
    //     let target_path = self.get_symlink_target().to_owned();
    //     let fd = self.get_fd();
    //     let target_fd = blocking!(fcntl::openat(
    //         fd,
    //         target_path.as_os_str(),
    //         oflags,
    //         Mode::empty()
    //     ))
    //     .context(format!(
    //         "open_symlink_target() failed to open symlink target path={:?}",
    //         self.get_symlink_target(),
    //     ))?;
    //     Ok(target_fd)
    // }

    // File only methods

    /// Get file data
    pub fn get_file_data(&self) -> &Vec<u8> {
        match &self.data {
            NodeData::Directory(..) | NodeData::SymLink(..) => {
                panic!("forbidden to load FileData from non-file node")
            }
            NodeData::RegFile(file_data) => file_data,
        }
    }

    /// Write to file
    pub async fn write_file(
        &mut self,
        fh: u64,
        offset: i64,
        data: Vec<u8>,
        oflags: OFlag,
        write_to_disk: bool,
    ) -> anyhow::Result<usize> {
        let ino = self.get_ino();
        let file_data_vec = match &mut self.data {
            NodeData::Directory(..) | NodeData::SymLink(..) => {
                panic!("forbidden to load FileData from non-file node")
            }
            NodeData::RegFile(file_data) => file_data,
        };

        let size_after_write = data.len().overflow_add(offset.cast::<usize>());
        if file_data_vec.capacity() < size_after_write {
            let before_cap = file_data_vec.capacity();
            let extra_space_size = size_after_write.overflow_sub(file_data_vec.capacity());
            file_data_vec.reserve(extra_space_size);
            // TODO: handle OOM when reserving
            // let result = file_data.try_reserve(extra_space_size);
            // if result.is_err() {
            //     warn!(
            //         "write_file() cannot reserve enough space, \
            //            the write space needed {} bytes",
            //         extra_space_size);
            //     reply.error(ENOMEM);
            //     return;
            // }
            debug!(
                "write_file() enlarged the file data vector capacity from {} to {}",
                before_cap,
                file_data_vec.capacity(),
            );
        }
        match file_data_vec.len().cmp(&(offset.cast())) {
            std::cmp::Ordering::Greater => {
                file_data_vec.truncate(offset.cast());
                debug!(
                    "write_file() truncated the file of ino={} to size={}",
                    ino, offset
                );
            }
            std::cmp::Ordering::Less => {
                let zero_padding_size = offset.cast::<usize>().overflow_sub(file_data_vec.len());
                let mut zero_padding_vec = vec![0_u8; zero_padding_size];
                file_data_vec.append(&mut zero_padding_vec);
            }
            std::cmp::Ordering::Equal => (),
        }
        // TODO: consider zero copy
        file_data_vec.extend_from_slice(&data);

        let fcntl_oflags = fcntl::FcntlArg::F_SETFL(oflags);
        let fd = fh.cast();
        fcntl::fcntl(fd, fcntl_oflags).context(format!(
            "write_file() failed to set the flags={:?} to file handler={} of ino={}",
            oflags, fd, ino,
        ))?;
        let mut written_size = data.len();
        if write_to_disk {
            let data_len = data.len();
            written_size = blocking!(nix::sys::uio::pwrite(fd, &data, offset))
                .context("write_file() failed to write to disk")?;
            debug_assert_eq!(data_len, written_size);
        }
        // update the attribute of the written file
        self.attr.size = file_data_vec.len().cast();
        self.update_mtime_ctime_to_now();

        Ok(written_size)
    }

    /// Open root node
    pub async fn open_root_node(
        root_ino: INum,
        name: OsString,
        path: &Path,
    ) -> anyhow::Result<Self> {
        let dir_fd = util::open_dir(path).await?;
        let mut attr = util::load_attr(dir_fd).await?;
        attr.ino = root_ino; // replace root ino with 1

        let root_node = Self::new(
            root_ino,
            name,
            attr,
            NodeData::Directory(BTreeMap::new()),
            dir_fd,
        );
        // // load root directory data on open
        // root_node
        //     .load_data()
        //     .await
        //     .context("open_root_node() failed to load root directory entry data")?;

        Ok(root_node)
    }
}

#[cfg(test)]
mod test {
    use anyhow::{bail, Context};
    use nix::fcntl::{self, FcntlArg, OFlag};
    use nix::sys::stat::Mode;
    use nix::unistd;
    // use std::fs::File;
    // use std::io::prelude::*;
    // use std::os::unix::io::FromRawFd;
    use std::path::Path;

    #[test]
    fn test_dup_fd() -> anyhow::Result<()> {
        let path = Path::new("/tmp/dup_fd_test.txt");
        let oflags = OFlag::O_CREAT | OFlag::O_TRUNC | OFlag::O_RDWR;
        let res = fcntl::open(path, oflags, Mode::from_bits_truncate(644));
        let fd = match res {
            Ok(fd) => fd,
            Err(e) => bail!("failed to open file {:?}, the error is: {}", path, e),
        };
        let res = unistd::unlink(path);
        if let Err(e) = res {
            unistd::close(fd)?;
            bail!("failed to unlink file, the error is: {}", e);
        }

        let dup_fd = unistd::dup(fd).context("failed to dup fd")?;
        let new_oflags = OFlag::O_WRONLY | OFlag::O_APPEND;
        let fcntl_oflags = FcntlArg::F_SETFL(new_oflags);
        fcntl::fcntl(dup_fd, fcntl_oflags).context(format!(
            "failed to set new flags={:?} to the dup fd={}",
            new_oflags, dup_fd,
        ))?;

        let file_content = "ABCDEFGHJKLMNOPQRSTUVWXYZ";
        {
            let write_size = unistd::write(dup_fd, file_content.as_bytes())?;
            assert_eq!(write_size, file_content.len(), "write size not match");

            // unistd::lseek(dup_fd, 0, unistd::Whence::SeekSet)?;
            // let mut buffer: Vec<u8> = std::iter::repeat(0_u8).take(file_content.len()).collect();
            // let read_size = unistd::read(dup_fd, &mut *buffer)?;
            // assert_eq!(read_size, file_content.len(), "read size not match");
            // let content = String::from_utf8(buffer)?;
            // assert_eq!(content, file_content, "file content not match");
            unistd::close(dup_fd)?;
        }
        {
            unistd::lseek(fd, 0, unistd::Whence::SeekSet)?;
            let mut buffer: Vec<u8> = std::iter::repeat(0_u8).take(file_content.len()).collect();
            let read_size = unistd::read(fd, &mut *buffer)?;
            assert_eq!(read_size, file_content.len(), "read size not match");
            let content = String::from_utf8(buffer)?;
            assert_eq!(content, file_content, "file content not match");
            unistd::close(fd)?;
        }

        Ok(())
    }
}
