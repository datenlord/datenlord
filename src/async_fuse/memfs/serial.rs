use super::cache::GlobalCache;
use super::dir::DirEntry;
use super::fs_util::FileAttr;
use super::s3_node::S3NodeData;
use super::s3_wrapper::S3BackEnd;
use crate::async_fuse::fuse::protocol::INum;
use crate::common::etcd_delegate::EtcdDelegate;
use nix::sys::stat::SFlag;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::{sync::Arc, time::SystemTime};

/// Serializable `DirEntry`
#[derive(Serialize, Deserialize, Debug)]
pub struct SerialDirEntry {
    /// The entry name
    name: String,
    /// File attr
    file_attr: SerialFileAttr,
}

/// Serializable `FileAttr`
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SerialFileAttr {
    /// Inode number
    ino: INum,
    /// Size in bytes
    size: u64,
    /// Size in blocks
    blocks: u64,
    /// Time of last access
    atime: SystemTime,
    /// Time of last modification
    mtime: SystemTime,
    /// Time of last change
    ctime: SystemTime,
    /// Time of creation (macOS only)
    crtime: SystemTime,
    /// Kind of file (directory, file, pipe, etc)
    kind: SerialSFlag,
    /// Permissions
    perm: u16,
    /// Number of hard links
    nlink: u32,
    /// User id
    uid: u32,
    /// Group id
    gid: u32,
    /// Rdev
    rdev: u32,
    /// Flags (macOS only, see chflags(2))
    flags: u32,
}

/// Serializable `SFlag`
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SerialSFlag {
    /// Regular file
    Reg,
    /// Directory
    Dir,
    /// Symbolic link
    Lnk,
}

/// In order to derive Serialize and Deserialize,
/// Replace the 'BTreeMap<String, `DirEntry`>' with 'HashMap<String, `SerialDirEntry`>'
#[derive(Serialize, Deserialize, Debug)]
pub enum SerialNodeData {
    /// Directory data
    Directory(BTreeMap<String, SerialDirEntry>),
    /// File data is ignored ,because `Arc<GlobalCache>` is not serializable
    File,
    /// Symbolic link data
    SymLink(PathBuf),
}

impl SerialNodeData {
    /// Deserializes the node data
    pub fn deserialize_s3(self, data_cache: Arc<GlobalCache>) -> S3NodeData {
        match self {
            SerialNodeData::Directory(dir) => {
                let mut dir_entry_map = BTreeMap::new();
                for (name, entry) in dir {
                    dir_entry_map.insert(name, serial_to_dir_entry(&entry));
                }
                S3NodeData::Directory(dir_entry_map)
            }
            SerialNodeData::File => S3NodeData::RegFile(data_cache),
            SerialNodeData::SymLink(path) => S3NodeData::SymLink(path),
        }
    }
}
/// TODO: We should discuss the design about persist
/// Serializable 'Node'
#[derive(Serialize, Deserialize, Debug)]
pub struct SerialNode {
    /// Parent node i-number
    pub(crate) parent: u64,
    /// S3Node name
    pub(crate) name: String,
    /// Full path of S3Node
    pub(crate) full_path: String,
    /// Node attribute
    pub(crate) attr: SerialFileAttr,
    /// Node data
    pub(crate) data: SerialNodeData,
    /// S3Node open counter
    pub(crate) open_count: i64,
    /// S3Node lookup counter
    pub(crate) lookup_count: i64,
    /// If S3Node has been marked as deferred deletion
    pub(crate) deferred_deletion: bool,
}

#[allow(dead_code)]
#[derive(Debug)]
/// Deserializable 'Node' arguments
pub struct DeserialS3NodeArgs<S: S3BackEnd + Sync + Send + 'static> {
    /// The s3 backend
    pub(crate) s3_backend: Arc<S>,
    /// The etcd client
    pub(crate) etcd_client: Arc<EtcdDelegate>,
    /// The k8s node id
    pub(crate) k8s_node_id: String,
    /// The k8s volume info
    pub(crate) k8s_volume_info: String,
    /// The global cache(for file data)
    pub(crate) data_cache: Arc<GlobalCache>,
}

/// Convert `SFlag` to `SerialSFlag`
#[must_use]
pub fn entry_type_to_serial(entry_type: SFlag) -> SerialSFlag {
    match entry_type {
        SFlag::S_IFDIR => SerialSFlag::Dir,
        SFlag::S_IFREG => SerialSFlag::Reg,
        SFlag::S_IFLNK => SerialSFlag::Lnk,
        _ => panic!("unsupported entry type {entry_type:?}"),
    }
}

/// Convert `SerialSFlag` to `SFlag`
#[must_use]
pub const fn serial_to_entry_type(entry_type: &SerialSFlag) -> SFlag {
    match *entry_type {
        SerialSFlag::Dir => SFlag::S_IFDIR,
        SerialSFlag::Reg => SFlag::S_IFREG,
        SerialSFlag::Lnk => SFlag::S_IFLNK,
    }
}

/// Convert `DirEntry` to `SerialDirEntry`
#[must_use]
pub fn dir_entry_to_serial(entry: &DirEntry) -> SerialDirEntry {
    SerialDirEntry {
        name: entry.entry_name().to_owned(),
        file_attr: file_attr_to_serial(&entry.file_attr_arc_ref().read()),
    }
}

/// Convert `SerialDirEntry` to `DirEntry`
#[must_use]
pub fn serial_to_dir_entry(entry: &SerialDirEntry) -> DirEntry {
    DirEntry::new(
        entry.name.clone(),
        Arc::new(RwLock::new(serial_to_file_attr(&entry.file_attr))),
    )
}

/// Convert `FileAttr` to `SerialFileAttr`
#[must_use]
pub fn file_attr_to_serial(attr: &FileAttr) -> SerialFileAttr {
    SerialFileAttr {
        ino: attr.ino,
        size: attr.size,
        blocks: attr.blocks,
        atime: attr.atime,
        mtime: attr.mtime,
        ctime: attr.ctime,
        crtime: attr.crtime,
        kind: {
            if attr.kind == SFlag::S_IFREG {
                SerialSFlag::Reg
            } else if attr.kind == SFlag::S_IFDIR {
                SerialSFlag::Dir
            } else {
                SerialSFlag::Lnk
            }
        },
        perm: attr.perm,
        nlink: attr.nlink,
        uid: attr.uid,
        gid: attr.gid,
        rdev: attr.rdev,
        flags: attr.flags,
    }
}

/// Convert `SerialFileAttr` to `FileAttr`
#[must_use]
pub const fn serial_to_file_attr(attr: &SerialFileAttr) -> FileAttr {
    FileAttr {
        ino: attr.ino,
        size: attr.size,
        blocks: attr.blocks,
        atime: attr.atime,
        mtime: attr.mtime,
        ctime: attr.ctime,
        crtime: attr.crtime,
        kind: {
            match attr.kind {
                SerialSFlag::Lnk => SFlag::S_IFLNK,
                SerialSFlag::Dir => SFlag::S_IFDIR,
                SerialSFlag::Reg => SFlag::S_IFREG,
            }
        },
        perm: attr.perm,
        nlink: attr.nlink,
        uid: attr.uid,
        gid: attr.gid,
        rdev: attr.rdev,
        flags: attr.flags,
    }
}
