use thiserror::Error;

/// The backend wrapper of `openDAL` operator.
pub mod backend;

/// The block module.
pub mod block;

/// The manager of memory cache module.
pub mod manager;

/// The policy module.
pub mod policy;

/// The snowflake module.
// pub mod snowflake;

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
