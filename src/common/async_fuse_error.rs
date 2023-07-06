//! `DatenLord` async fuse error of different mod
use thiserror::Error;

/// Error caused by `async_fuse::memfs::kv_engine`
#[derive(Error, Debug)]
pub enum KVEngineError {
    /// Timeout arg in kv operation is <= 0
    #[error("Timeout arg in kv operation is <= 0")]
    WrongTimeoutArg,

    /// Lease keep alive stream end
    #[error("Lease keep alive stream end")]
    LeaseKeepAliveStreamEnd,
}
