//! The storage related error.

use anyhow::anyhow;
use nix::errno::Errno;
use thiserror::Error;

use crate::common::error::DatenLordError;

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

impl From<StorageError> for DatenLordError {
    fn from(value: StorageError) -> Self {
        match value {
            StorageError::OutOfRange { maximum, found } => DatenLordError::InternalErr {
                source: anyhow!("IO out of range: the maximum is {maximum}, but {found} found."),
                context: vec![],
            },
            StorageError::OutOfMemory => DatenLordError::InternalErr {
                source: anyhow::Error::new(Errno::EIO),
                context: vec!["Cache is out of memory.".to_owned()],
            },
            StorageError::StdIoError(e) => {
                let errno = e.raw_os_error();
                DatenLordError::InternalErr {
                    source: errno.map_or(anyhow!("Io error: {e}"), |e| {
                        anyhow::Error::new(Errno::from_raw(e))
                    }),
                    context: vec![],
                }
            }
            StorageError::OpenDalError(e) => DatenLordError::InternalErr {
                source: anyhow::Error::new(e),
                context: vec![],
            },
            StorageError::Internal(e) => DatenLordError::InternalErr {
                source: e,
                context: vec![],
            },
        }
    }
}
