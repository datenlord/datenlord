//! `DatenLord` async fuse error of different mod
use thiserror::Error;

/// Error caused by `async_fuse::memfs::kv_engine`
#[derive(Error, Debug)]
pub enum KVEngineError {
    /// Error caused by std::io::Error
    #[error("Timeout arg in kv operation is <= 0")]
    WrongTimeoutArg,
}
