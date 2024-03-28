//! The storage related error.

use thiserror::Error;

/// The result of storage operation.
pub type StorageResult<T> = Result<T, StorageError>;

/// An error occurs in storage operation.
#[derive(Debug, Error)]
pub enum StorageError {
    /// The storage operation is out of range of a block
    #[error("{found} is out of range of {maximum}")]
    OutOfRange {
        /// The maximum size of the operated block
        maximum: usize,
        /// The size or offset found in argument
        found: usize,
    },
    /// The storage is run out of memory
    #[error("Out of memory")]
    OutOfMemory,
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
