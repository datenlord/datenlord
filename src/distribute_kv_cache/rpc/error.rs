use std::fmt;

use thiserror::Error;

/// Error types for the RPC server and client
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum RpcError {
    /// The request is invalid.
    InvalidRequest(String),
    /// The response is invalid.
    InvalidResponse(String),
    /// The server/client meet an internal error.
    InternalError(String),
    /// The request is timeout
    Timeout(String),
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::InvalidRequest(ref msg) => write!(f, "Invalid request: {msg}"),
            Self::InvalidResponse(ref msg) => write!(f, "Invalid response: {msg}"),
            Self::InternalError(ref msg) => write!(f, "Internal error: {msg}"),
            Self::Timeout(ref msg) => write!(f, "Timeout: {msg}"),
        }
    }
}
