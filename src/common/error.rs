//! `DatenLord` Error Code
use std::path::PathBuf;

use grpcio::RpcStatusCode;
use nix::sys::stat::SFlag;
use thiserror::Error;

use super::async_fuse_error::KVEngineError;

/// Get function name inside a function.
#[macro_export]
macro_rules! function_name {
    () => {{
        use clippy_utilities::OverflowArithmetic;
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);
        // skip the suffix `::f`
        name.get(..name.len().overflow_sub(3))
            .unwrap_or_else(|| unreachable!("Suffix `::f` must exist."))
    }};
}

/// `DatenLord` Result type
pub type DatenLordResult<T> = Result<T, DatenLordError>;
/// `DatenLord` error code
#[derive(Error, Debug)]
pub enum DatenLordError {
    /// Error caused by std::io::Error
    #[error("IoErr, the error is {:?}, context is {:#?}", .source, .context)]
    IoErr {
        /// Error source
        source: std::io::Error,
        /// Context of the error
        context: Vec<String>,
    },

    /// Error caused by walkdir::Error
    #[error("WalkdirErr, the error is {:?}, context is {:#?}", .source, .context)]
    WalkdirErr {
        /// Error source
        source: walkdir::Error,
        /// Context of the error
        context: Vec<String>,
    },

    /// Snapshot is not found
    #[error("Snapshot ID={} not found, context is {:#?}", .snapshot_id, .context)]
    SnapshotNotFound {
        /// Snapshot ID
        snapshot_id: String,
        /// Context of the error
        context: Vec<String>,
    },
    /// Volume is not found
    #[error("Volume ID={} not found, context is {:#?}", .volume_id, .context)]
    VolumeNotFound {
        /// Volume ID
        volume_id: String,
        /// Context of the error
        context: Vec<String>,
    },
    /// Volume has already existed
    #[error("Volume ID={} already exists, context is {:#?}", .volume_id, .context)]
    VolumeAlreadyExist {
        /// Volume ID
        volume_id: String,
        /// Context of the error
        context: Vec<String>,
    },

    /// Snapshot is not ready
    #[error("Snapshot ID={} is not ready, context is {:#?}", .snapshot_id, .context)]
    SnapshotNotReady {
        /// Snapshot ID
        snapshot_id: String,
        /// Context of the error
        context: Vec<String>,
    },

    /// Snapshot has already existed
    #[error("Snapshot ID={} already exists, context is {:#?}", .snapshot_id, .context)]
    SnapshotAlreadyExist {
        /// Snapshot ID
        snapshot_id: String,
        /// Context of the error
        context: Vec<String>,
    },

    /// Node is not found
    #[error("Node ID={} not found, context is {:#?}", .node_id, .context)]
    NodeNotFound {
        /// Node ID
        node_id: String,
        /// Context of the error
        context: Vec<String>,
    },

    /// Argument is invalid
    #[error("Argument is invalid, context is {:#?}", .context)]
    ArgumentInvalid {
        /// Context of the error
        context: Vec<String>,
    },

    /// Starting token is invalid
    #[error("Starting token={} is invalid, context is {:#?}", .starting_token, .context)]
    StartingTokenInvalid {
        /// Starting token
        starting_token: String,
        /// Context of the error
        context: Vec<String>,
    },

    /// Argument is out of range
    #[error("Argument is out of range, context is {:#?}", .context)]
    ArgumentOutOfRange {
        /// Context of the error
        context: Vec<String>,
    },

    /// Error caused by std::path::StripPrefixError
    #[error("StripPrefixErr, the error is {:?}, context is {:#?}", .source, .context)]
    StripPrefixErr {
        /// Error source
        source: std::path::StripPrefixError,
        /// Context of the error
        context: Vec<String>,
    },

    /// Error caused by etcd_client::EtcdError,
    #[error("EtcdClientErr, the error is {:?}, context is {:#?}", .source, .context)]
    EtcdClientErr {
        /// Error source
        source: etcd_client::Error,
        /// Context of the error
        context: Vec<String>,
    },

    /// Error caused by async_fuse::memfs::kv_engine
    #[error("EtcdClientErr, the error is {:?}, context is {:#?}", .source, .context)]
    KVEngineErr {
        /// Error source
        source: KVEngineError,
        /// Context of the error
        context: Vec<String>,
    },

    /// Error caused by bincode::Error
    #[error("BincodeErr, the error is {:?}, context is {:#?}", .source, .context)]
    BincodeErr {
        /// Error source
        source: bincode::Error,
        /// Context of the error
        context: Vec<String>,
    },

    /// Error caused by nix::Error
    #[error("NixErr, the error is {:?}, context is {:#?}", .source, .context)]
    NixErr {
        /// Error source
        source: nix::Error,
        /// Context of the error
        context: Vec<String>,
    },

    /// Failed to mount
    #[error("MountErr, fail to mount {:?} to {:?}, context is {:#?}", .from, .target, .context)]
    MountErr {
        /// Source to mount
        from: PathBuf,
        /// Mount point
        target: PathBuf,
        /// Context of the error
        context: Vec<String>,
    },

    /// Failed to umount
    #[error("UmountErr, fail to umount {:?}, context is {:#?}", .target, .context)]
    UmountErr {
        /// Mount point to umount
        target: PathBuf,
        /// Context of the error
        context: Vec<String>,
    },

    /// Error caused by std::time::SystemTimeError
    #[error("SystemTimeErr, the error is {:?}, context is {:#?}", .source, .context)]
    SystemTimeErr {
        /// Error source
        source: std::time::SystemTimeError,
        /// Context of the error
        context: Vec<String>,
    },

    /// Error caused by grpcio::Error
    #[error("GrpcioErr, the error is {:?}, context is {:#?}", .source, .context)]
    GrpcioErr {
        /// Error source
        source: grpcio::Error,
        /// Context of the error
        context: Vec<String>,
    },

    /// Error caused by serde_json::Error
    #[error("serde_json::Error, the error is {:?}, context is {:#?}", .source, .context)]
    SerdeJsonErr {
        /// Error source
        source: serde_json::Error,
        /// Context of the error
        context: Vec<String>,
    },

    /// Error caused by tokio::task::JoinError given by tokio::task::spawn*
    #[error("tokio::task::JoinError, the error is {:?}, context is {:#?}", .source, .context)]
    JoinErr {
        /// Error source
        source: tokio::task::JoinError,
        /// Context of the error
        context: Vec<String>,
    },

    /// Error caused by module kv_engine's MetaTxn retry limit exceeded
    #[error("TransactionRetryLimitExceededErr, context is {:#?}", .context)]
    TransactionRetryLimitExceededErr {
        /// Context of the error
        context: Vec<String>,
    },

    /// Error caused by datenlord's internal logic
    #[error("InternalErr, the error is {:?} context is {:#?}", .source,.context)]
    InternalErr {
        /// Error source
        source: anyhow::Error,
        /// Context of the error
        context: Vec<String>,
    },

    /// API is not implemented
    #[error("Not implemented, context is {:#?}", .context)]
    Unimplemented {
        /// Context of the error
        context: Vec<String>,
    },
    /// FS is inconsistent, as some mentioned nodes are not in the cache.
    #[error("FS is inconsistent, context is {:#?}.", .context)]
    InconsistentFS {
        /// Context of the error
        context: Vec<String>,
    },
    /// An i-node type is not supported by the executing operation.
    #[error("Unsuported i-node type={:?}, context id {:#?}.", .node_type, .context)]
    UnsupportedINodeType {
        /// Type of the node
        node_type: SFlag,
        /// Context of the error
        context: Vec<String>,
    },
    // /// Error when doing s3 operation.
    // #[error("S3 error: {0}")]
    // S3Error(s3_wrapper::S3Error),

    // ///
    // #[error("persist error: {0}")]
    // PersistError(persist::PersistError),
}

/// Add context to `DatenLordResult`
pub trait Context<T, E> {
    /// Add context to `DatenLordResult`
    fn add_context<C>(self, ctx: C) -> DatenLordResult<T>
    where
        C: Into<String>;

    /// Add context to `DatenLordResult` lazily
    fn with_context<C, F>(self, f: F) -> DatenLordResult<T>
    where
        C: Into<String>,
        F: FnOnce() -> C;
}

impl<T, E> Context<T, E> for Result<T, E>
where
    E: std::error::Error + Into<DatenLordError>,
{
    #[inline]
    fn add_context<C>(self, ctx: C) -> DatenLordResult<T>
    where
        C: Into<String>,
    {
        self.map_err(|e| e.into().add_context(ctx))
    }

    #[inline]
    fn with_context<C, F>(self, f: F) -> DatenLordResult<T>
    where
        C: Into<String>,
        F: FnOnce() -> C,
    {
        self.map_err(|e| e.into().add_context(f()))
    }
}

impl DatenLordError {
    /// Add context for `DatenLordError`
    #[inline]
    #[must_use]
    pub fn add_context<C>(mut self, ctx: C) -> Self
    where
        C: Into<String>,
    {
        /// Append context for `DatenLordError`
        macro_rules! append_context {
            ($context: ident, [$($target:ident),*]) => {
                match self {
                    $(Self::$target { ref mut context, ..} => {
                        context.push($context.into());
                    },)*
                }
            }
        }
        append_context!(
            ctx,
            [
                IoErr,
                WalkdirErr,
                SnapshotNotFound,
                VolumeNotFound,
                VolumeAlreadyExist,
                SnapshotNotReady,
                SnapshotAlreadyExist,
                NodeNotFound,
                ArgumentInvalid,
                StartingTokenInvalid,
                ArgumentOutOfRange,
                StripPrefixErr,
                EtcdClientErr,
                KVEngineErr,
                BincodeErr,
                NixErr,
                MountErr,
                UmountErr,
                SystemTimeErr,
                GrpcioErr,
                SerdeJsonErr,
                JoinErr,
                TransactionRetryLimitExceededErr,
                InternalErr,
                Unimplemented,
                InconsistentFS,
                UnsupportedINodeType
            ]
        );
        self
    }

    /// Add context for `DatenLordError` lazily
    #[inline]
    #[must_use]
    pub fn with_context<C, F>(self, f: F) -> Self
    where
        C: Into<String>,
        F: FnOnce() -> C,
    {
        self.add_context(f())
    }
}

/// Implement from trait for `DatenLordError`
macro_rules! implement_from {
    ($source:path, $target:ident) => {
        impl From<$source> for DatenLordError {
            #[inline]
            fn from(error: $source) -> Self {
                Self::$target {
                    source: error,
                    context: vec![],
                }
            }
        }
    };
}
implement_from!(std::io::Error, IoErr);
implement_from!(walkdir::Error, WalkdirErr);
implement_from!(std::path::StripPrefixError, StripPrefixErr);
implement_from!(etcd_client::Error, EtcdClientErr);
implement_from!(bincode::Error, BincodeErr);
implement_from!(nix::Error, NixErr);
implement_from!(std::time::SystemTimeError, SystemTimeErr);
implement_from!(grpcio::Error, GrpcioErr);
implement_from!(serde_json::Error, SerdeJsonErr);
implement_from!(tokio::task::JoinError, JoinErr);
implement_from!(anyhow::Error, InternalErr);

impl From<DatenLordError> for RpcStatusCode {
    #[inline]
    fn from(error: DatenLordError) -> Self {
        match error {
            DatenLordError::IoErr { .. }
            | DatenLordError::StripPrefixErr { .. }
            | DatenLordError::EtcdClientErr { .. }
            | DatenLordError::BincodeErr { .. }
            | DatenLordError::NixErr { .. }
            | DatenLordError::MountErr { .. }
            | DatenLordError::UmountErr { .. }
            | DatenLordError::SystemTimeErr { .. }
            | DatenLordError::SerdeJsonErr { .. }
            | DatenLordError::WalkdirErr { .. }
            | DatenLordError::TransactionRetryLimitExceededErr { .. }
            | DatenLordError::InternalErr { .. }
            | DatenLordError::KVEngineErr { .. }
            | DatenLordError::JoinErr { .. }
            | DatenLordError::InconsistentFS { .. }
            | DatenLordError::UnsupportedINodeType { .. } => Self::INTERNAL,
            DatenLordError::GrpcioErr { source, .. } => match source {
                grpcio::Error::RpcFailure(ref s) => s.code(),
                grpcio::Error::Codec(..)
                | grpcio::Error::CallFailure(..)
                | grpcio::Error::RpcFinished(..)
                | grpcio::Error::RemoteStopped
                | grpcio::Error::ShutdownFailed
                | grpcio::Error::BindFail(..)
                | grpcio::Error::QueueShutdown
                | grpcio::Error::GoogleAuthenticationFailed
                | grpcio::Error::InvalidMetadata(..) => Self::INTERNAL,
            },
            DatenLordError::SnapshotNotFound { .. }
            | DatenLordError::VolumeNotFound { .. }
            | DatenLordError::NodeNotFound { .. } => Self::NOT_FOUND,
            DatenLordError::VolumeAlreadyExist { .. }
            | DatenLordError::SnapshotAlreadyExist { .. } => Self::ALREADY_EXISTS,
            DatenLordError::SnapshotNotReady { .. } => Self::UNAVAILABLE,
            DatenLordError::ArgumentInvalid { .. } => Self::INVALID_ARGUMENT,
            DatenLordError::ArgumentOutOfRange { .. } => Self::OUT_OF_RANGE,
            DatenLordError::StartingTokenInvalid { .. } => Self::ABORTED,
            DatenLordError::Unimplemented { .. } => Self::UNIMPLEMENTED,
        }
    }
}
