use super::super::dir::DirEntry;
use super::super::fs_util::FileAttr;
use crate::async_fuse::fuse::protocol::INum;
use libc::ino_t;
use nix::sys::stat::SFlag;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// Serializable `DirEntry`
#[derive(Serialize, Deserialize, Debug)]
pub struct SerialDirEntry {
    /// The i-number of the entry
    ino: ino_t,
    /// The `SFlag` type of the entry
    entry_type: SerialSFlag,
    /// The entry name
    name: String,
}

/// Serializable `FileAttr`
#[derive(Serialize, Deserialize, Debug)]
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
#[derive(Serialize, Deserialize, Debug)]
pub enum SerialSFlag {
    /// Regular file
    Reg,
    /// Directory
    Dir,
    /// Symbolic link
    Lnk,
}

/// Convert `SFlag` to `SerialSFlag`
#[must_use]
pub fn entry_type_to_serial(entry_type: SFlag) -> SerialSFlag {
    match entry_type {
        SFlag::S_IFDIR => SerialSFlag::Dir,
        SFlag::S_IFREG => SerialSFlag::Reg,
        SFlag::S_IFLNK => SerialSFlag::Lnk,
        _ => panic!("unsupported entry type {:?}", entry_type),
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
        ino: entry.ino(),
        entry_type: { entry_type_to_serial(entry.entry_type()) },
        name: entry.entry_name().to_owned(),
    }
}

/// Convert `SerialDirEntry` to `DirEntry`
#[must_use]
pub fn serial_to_dir_entry(entry: &SerialDirEntry) -> DirEntry {
    DirEntry::new(
        entry.ino,
        entry.name.to_owned(),
        serial_to_entry_type(&entry.entry_type),
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
