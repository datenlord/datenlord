//! Error types for storage.

use thiserror::Error;

use super::BlockId;
use crate::async_fuse::fuse::protocol::INum;

/// The result of storage operation.
pub type StorageResult<T> = Result<T, StorageError>;

/// An error occurs in storage operation.
#[derive(Debug, Error)]
#[error("Storage Error on {operation:?}, inner is {inner}.")]
pub struct StorageError {
    /// The operation kind.
    pub operation: StorageOperation,
    /// The inner error.
    pub inner: StorageErrorInner,
}

/// The kind of storage operation.
#[derive(Debug)]
pub enum StorageOperation {
    /// Load operation.
    Load {
        /// The inode number of this error
        ino: INum,
        /// The block id of this error.
        block_id: BlockId,
    },
    /// Store operation.
    Store {
        /// The inode number of this error
        ino: INum,
        /// The block id of this error.
        block_id: BlockId,
    },
    /// Remove operation.
    Remove {
        /// The inode number of this error
        ino: INum,
    },
    /// Truncate operation.
    Truncate {
        /// The inode number of this error
        ino: INum,
        /// The start block id of truncate.
        from: BlockId,
        /// The end block id of truncate.
        to: BlockId,
    },
}

/// The inner details of `StorageError`
#[derive(Debug, Error)]
pub enum StorageErrorInner {
    /// The storage operation is out of range of a block
    #[error("{found} is out of range of {maximum}")]
    OutOfRange {
        /// The maximum size of the operated block
        maximum: usize,
        /// The size or offset found in argument
        found: usize,
    },
    /// An error caused by [`std::io::Error`]
    #[error("{0}")]
    StdIoError(#[from] std::io::Error),
    /// An error caused by [`opendal::Error`]
    #[error("{0}")]
    OpenDalError(#[from] opendal::Error),
    /// A internal storage error.
    #[error("{0}")]
    Internal(#[from] anyhow::Error),
}
