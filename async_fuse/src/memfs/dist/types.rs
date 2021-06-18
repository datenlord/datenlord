use super::super::dir::DirEntry;
use super::super::fs_util::FileAttr;
use crate::fuse::protocol::INum;
use libc::ino_t;
use nix::sys::stat::SFlag;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct SerialDirEntry {
    /// The i-number of the entry
    ino: ino_t,
    /// The `SFlag` type of the entry
    entry_type: SerialSFlag,
    /// The entry name
    name: String,
}

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

#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum SerialSFlag {
    Reg,
    Dir,
    Lnk,
}

pub(crate) fn dir_entry_to_serial(entry: &DirEntry) -> SerialDirEntry {
    SerialDirEntry {
        ino: entry.ino(),
        entry_type: {
            if entry.entry_type().contains(SFlag::S_IFDIR) {
                SerialSFlag::Dir
            } else if entry.entry_type().contains(SFlag::S_IFREG) {
                SerialSFlag::Reg
            } else {
                SerialSFlag::Lnk
            }
        },
        name: entry.entry_name().to_owned(),
    }
}

pub(crate) fn serial_to_dir_entry(entry: &SerialDirEntry) -> DirEntry {
    DirEntry::new(
        entry.ino,
        entry.name.to_owned(),
        match entry.entry_type {
            SerialSFlag::Dir => SFlag::S_IFDIR,
            SerialSFlag::Reg => SFlag::S_IFREG,
            SerialSFlag::Lnk => SFlag::S_IFLNK,
        },
    )
}

pub(crate) fn file_attr_to_serial(attr: &FileAttr) -> SerialFileAttr {
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

pub(crate) fn serial_to_file_attr(attr: &SerialFileAttr) -> FileAttr {
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
