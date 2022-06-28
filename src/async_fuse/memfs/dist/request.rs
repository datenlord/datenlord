//! Request between caches

use crate::async_fuse::fuse::protocol::INum;

use super::super::fs_util::FileAttr;
use super::super::RenameParam;
use super::types::{self, SerialFileAttr, SerialSFlag};
use log::info;
use nix::sys::stat::SFlag;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Distributed request
#[derive(Serialize, Deserialize, Debug)]
pub enum DistRequest {
    /// Invalidate cache request
    Invalidate(OpArgs),
    /// Check cache availibility request
    CheckAvailable(OpArgs),
    /// Read data request
    Read(OpArgs),
    /// Get file attr request
    GetFileAttr(String),
    /// Push file attr request
    PushFileAttr((String, SerialFileAttr)),
    /// Load dir data request
    LoadDir(String),
    /// Update dir data request
    UpdateDir(UpdateDirArgs),
    /// Remove dir entry request
    RemoveDirEntry(RemoveDirEntryArgs),
    /// Rename request
    Rename(RenameParam),
    /// Remove request
    Remove(RemoveArgs),
    /// Get inode number request
    GetInodeNum,
}

/// `UpdateDir` request args
#[derive(Serialize, Deserialize, Debug)]
pub struct UpdateDirArgs {
    /// Parent path
    pub parent_path: String,
    /// Child name
    pub child_name: String,
    /// Child attr
    pub child_attr: SerialFileAttr,
    /// Target path of symbolic link
    pub target_path: Option<PathBuf>,
}

/// `RemoveDirEntry` request args
#[derive(Serialize, Deserialize, Debug)]
pub struct RemoveDirEntryArgs {
    /// Parent path
    pub parent_path: String,
    /// Child name
    pub child_name: String,
}

/// `Remove` request args
#[derive(Serialize, Deserialize, Debug)]
pub struct RemoveArgs {
    /// Parent inode number
    pub parent: INum,
    /// Child name
    pub child_name: String,
    /// Child type
    pub child_type: SerialSFlag,
}

/// `Index` in a file
#[derive(Serialize, Deserialize, Debug)]
pub enum Index {
    /// Point index
    Point(usize),
    /// Range index
    Range(usize, usize),
}

/// File operation request args
#[derive(Serialize, Deserialize, Debug)]
pub struct OpArgs {
    /// File name
    pub file_name: Vec<u8>,
    /// Index of file
    pub index: Vec<Index>,
}

/// Serialize Invalidate file cache request
#[must_use]
pub fn invalidate(file_name: Vec<u8>, index: Vec<Index>) -> Vec<u8> {
    info!("dist invalidate");
    bincode::serialize(&DistRequest::Invalidate(OpArgs { file_name, index })).unwrap_or_else(|e| {
        panic!(
            "fail to serialize `Invalidate` distributed cache operationi, {}",
            e
        )
    })
}

/// Serialize Check cache availibility of file request
#[must_use]
pub fn check_available(file_name: Vec<u8>, index: Vec<Index>) -> Vec<u8> {
    bincode::serialize(&DistRequest::CheckAvailable(OpArgs { file_name, index })).unwrap_or_else(
        |e| {
            panic!(
                "fail to serialize `CheckAvailable` distributed cache operation, {}",
                e
            )
        },
    )
}

/// Serialize Read file data request
#[must_use]
pub fn read(file_name: Vec<u8>, index: Vec<Index>) -> Vec<u8> {
    bincode::serialize(&DistRequest::Read(OpArgs { file_name, index })).unwrap_or_else(|e| {
        panic!(
            "fail to serialize `Read` distributed cache operation, {}",
            e
        )
    })
}

/// Deserialize request
#[must_use]
pub fn deserialize_cache(bin: &[u8]) -> DistRequest {
    bincode::deserialize(bin)
        .unwrap_or_else(|e| panic!("fail to deserialize distributed cache operation, {}", e))
}

/// Serialize Load dir data request
#[must_use]
pub fn load_dir(path: &str) -> Vec<u8> {
    bincode::serialize(&DistRequest::LoadDir(path.to_owned())).unwrap_or_else(|e| {
        panic!(
            "fail to serialize `LoadDir` distributed meta operation, {}",
            e
        )
    })
}

/// Serialize Update dir data request
#[must_use]
pub fn update_dir(
    parent: &str,
    child: &str,
    child_attr: &FileAttr,
    target_path: Option<&Path>,
) -> Vec<u8> {
    let args = UpdateDirArgs {
        parent_path: parent.to_owned(),
        child_name: child.to_owned(),
        child_attr: types::file_attr_to_serial(child_attr),
        target_path: target_path.map(std::borrow::ToOwned::to_owned),
    };

    bincode::serialize(&DistRequest::UpdateDir(args)).unwrap_or_else(|e| {
        panic!(
            "fail to serialize `UpdateDir` distributed meta operation, {}",
            e
        )
    })
}

/// Serialize Remove dir entry request
#[must_use]
pub fn remove_dir_entry(parent: &str, child: &str) -> Vec<u8> {
    let args = RemoveDirEntryArgs {
        parent_path: parent.to_owned(),
        child_name: child.to_owned(),
    };

    bincode::serialize(&DistRequest::RemoveDirEntry(args)).unwrap_or_else(|e| {
        panic!(
            "fail to serialize `RemoveDirEntry` distributed meta operation, {}",
            e
        )
    })
}

/// Serialize Get file attr request
#[must_use]
pub fn get_file_attr(path: &str) -> Vec<u8> {
    bincode::serialize(&DistRequest::GetFileAttr(path.to_owned())).unwrap_or_else(|e| {
        panic!(
            "fail to serialize `GetFileAttr` distributed meta operation, {}",
            e
        )
    })
}

/// Serialize Push file attr request
#[must_use]
pub fn push_file_attr(path: &str, attr: SerialFileAttr) -> Vec<u8> {
    bincode::serialize(&DistRequest::PushFileAttr((path.to_owned(), attr))).unwrap_or_else(|e| {
        panic!(
            "fail to serialize `PushFileAttr` distributed meta operation, {}",
            e
        )
    })
}

/// Serialize Get inode number request
#[must_use]
pub fn get_ino_num() -> Vec<u8> {
    bincode::serialize(&DistRequest::GetInodeNum).unwrap_or_else(|e| {
        panic!(
            "fail to serialize `GetInodeNum` distributed meta operation, {}",
            e
        )
    })
}

/// Serialize Rename request
#[must_use]
pub fn rename(args: RenameParam) -> Vec<u8> {
    bincode::serialize(&DistRequest::Rename(args)).unwrap_or_else(|e| {
        panic!(
            "fail to serialize `GetInodeNum` distributed meta operation, {}",
            e
        )
    })
}

/// Serialize Remove request
#[must_use]
pub fn remove(parent: INum, child: &str, child_type: SFlag) -> Vec<u8> {
    let args = RemoveArgs {
        parent,
        child_name: child.to_owned(),
        child_type: types::entry_type_to_serial(child_type),
    };

    bincode::serialize(&DistRequest::Remove(args)).unwrap_or_else(|e| {
        panic!(
            "fail to serialize `RemoveDirEntry` distributed meta operation, {}",
            e
        )
    })
}
