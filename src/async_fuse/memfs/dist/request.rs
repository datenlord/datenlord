//! Request between caches

use crate::async_fuse::fuse::protocol::INum;

use super::super::serial::SerialSFlag;
use log::info;

use serde::{Deserialize, Serialize};

/// Distributed request
#[derive(Serialize, Deserialize, Debug)]
pub enum DistRequest {
    /// Invalidate cache request
    Invalidate(OpArgs),
    /// Check cache availibility request
    CheckAvailable(OpArgs),
    /// Read data request
    Read(OpArgs),
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
    /// File inode number
    pub file_ino: INum,
    /// Index of file
    pub index: Vec<Index>,
}

/// Serialize Invalidate file cache request
#[must_use]
pub fn invalidate(file_ino: INum, index: Vec<Index>) -> Vec<u8> {
    info!("dist invalidate");
    bincode::serialize(&DistRequest::Invalidate(OpArgs { file_ino, index })).unwrap_or_else(|e| {
        panic!("fail to serialize `Invalidate` distributed cache operationi, {e}")
    })
}

/// Serialize Check cache availibility of file request
#[must_use]
pub fn check_available(file_ino: INum, index: Vec<Index>) -> Vec<u8> {
    bincode::serialize(&DistRequest::CheckAvailable(OpArgs { file_ino, index })).unwrap_or_else(
        |e| panic!("fail to serialize `CheckAvailable` distributed cache operation, {e}"),
    )
}

/// Serialize Read file data request
#[must_use]
pub fn read(file_ino: INum, index: Vec<Index>) -> Vec<u8> {
    bincode::serialize(&DistRequest::Read(OpArgs { file_ino, index }))
        .unwrap_or_else(|e| panic!("fail to serialize `Read` distributed cache operation, {e}"))
}

/// Deserialize request
#[must_use]
pub fn deserialize_cache(bin: &[u8]) -> DistRequest {
    bincode::deserialize(bin)
        .unwrap_or_else(|e| panic!("fail to deserialize distributed cache operation, {e}"))
}
