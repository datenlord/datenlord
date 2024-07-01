use std::fmt::{self, Error};

/// Error types for the RPC server and client
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum RpcError<T> {
    /// The request is invalid.
    InvalidRequest(T),
    /// The response is invalid.
    InvalidResponse(T),
    /// The server/client meet an internal error.
    InternalError(T),
}

impl<T> RpcError<T> {
    /// Convert the error to a string.
    pub fn to_string(self) -> T {
        match self {
            Self::InvalidRequest(msg) | Self::InvalidResponse(msg) | Self::InternalError(msg) => {
                msg
            }
        }
    }
}

impl<T> fmt::Display for RpcError<T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::InvalidRequest(ref msg) => write!(f, "Invalid request: {msg}"),
            Self::InvalidResponse(ref msg) => write!(f, "Invalid response: {msg}"),
            Self::InternalError(ref msg) => write!(f, "Internal error: {msg}"),
        }
    }
}

impl From<Error> for RpcError<String> {
    fn from(err: Error) -> Self {
        Self::InternalError(err.to_string())
    }
}
