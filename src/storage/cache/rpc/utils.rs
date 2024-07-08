use bytes::Buf;

use super::error::RpcError;

/// Read with timeout options from the environment variables
#[macro_export]
macro_rules! read_exact_timeout {
    ($self:expr, $dst:expr, $read_timeout:expr) => {
        async move {
            use tokio::time::timeout;

            match timeout($read_timeout, $self.read_exact($dst)).await {
                Ok(res) => match res {
                    Ok(size) => Ok(size),
                    Err(read_err) => Err(read_err),
                },
                Err(timeout_err) => Err(timeout_err.into()),
            }
        }
    };
}

/// Write with timeout options from the environment variables
#[macro_export]
macro_rules! write_all_timeout {
    ($self:expr, $src:expr, $write_timeout:expr) => {
        async move {
            use tokio::time::timeout;

            match timeout($write_timeout, $self.write_all($src)).await {
                Ok(res) => match res {
                    Ok(size) => Ok(size),
                    Err(write_err) => Err(write_err),
                },
                Err(timeout_err) => Err(timeout_err.into()),
            }
        }
    };
}

/// Connect a stream with timeout options from the environment variables
#[macro_export]
macro_rules! connect_timeout {
    ($addr:expr, $connect_timeout:expr) => {
        async move {
            use tokio::net::TcpStream;
            use tokio::time::timeout;

            match timeout($connect_timeout, TcpStream::connect($addr)).await {
                Ok(res) => match res {
                    Ok(stream) => Ok(stream),
                    Err(connect_err) => Err(connect_err),
                },
                Err(timeout_err) => Err(timeout_err.into()),
            }
        }
    };
}

/// Try to get u64 data from buffer with range
pub fn get_u64_from_buf(buf: &[u8], start: usize) -> Result<u64, RpcError<String>> {
    buf.get(start..start + 8)
        .ok_or_else(|| RpcError::InternalError("Invalid range data".to_owned()))
        .map(|mut slice| u64::from_be(slice.get_u64()))
}

/// Try to get u8 data from buffer with range
pub fn get_u8_from_buf(buf: &[u8], start: usize) -> Result<u8, RpcError<String>> {
    buf.get(start)
        .ok_or_else(|| RpcError::InternalError("Invalid range data".to_owned()))
        .map(|&byte| u8::from_be(byte))
}

/// Converts [`u64`] to [`usize`], and panic when the conversion fails.
#[allow(clippy::expect_used)] // We can ensure that this method won't panic if we followed the hints above
#[inline]
#[must_use]
pub const fn u64_to_usize(x: u64) -> usize {
    x as usize
}
