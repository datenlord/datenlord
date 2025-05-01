use bytes::{Buf, BytesMut};
use tracing::debug;

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

/// Write vectored with timeout options from the environment variables
#[macro_export]
macro_rules! write_vectored_timeout {
    ($self:expr, $src:expr, $write_timeout:expr) => {
        async move {
            use tokio::time::timeout;

            // TODO: check all the write_vectored is done
            match timeout($write_timeout, $self.write_vectored($src)).await {
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
pub fn get_u64_from_buf(buf: &[u8], start: usize) -> Result<u64, RpcError> {
    buf.get(start..start + 8)
        .ok_or_else(|| RpcError::InternalError("Invalid range data".to_owned()))
        .map(|mut slice| u64::from_le(slice.get_u64()))
}

/// Try to get u8 data from buffer with range
pub fn get_u8_from_buf(buf: &[u8], start: usize) -> Result<u8, RpcError> {
    buf.get(start)
        .ok_or_else(|| RpcError::InternalError("In valid range data".to_owned()))
        .map(|&byte| u8::from_le(byte))
}

/// Converts [`u64`] to [`usize`], and panic when the conversion fails.
#[inline]
#[must_use]
pub const fn u64_to_usize(x: u64) -> usize {
    #[allow(clippy::as_conversions)]
    #[allow(clippy::cast_possible_truncation)]
    #[cfg(not(target_pointer_width = "16"))]
    {
        x as usize
    }
}

/// Ensure buffer length
pub fn ensure_buffer_len(buffer: &mut BytesMut, len: usize) {
    let start = std::time::Instant::now();
    if buffer.capacity() < len {
        buffer.reserve(len + 1);
        debug!(
            "Client reserve buffer {:?} to size {:?} cost: {:?}",
            len,
            len,
            start.elapsed()
        );
    }
    // req_buffer.resize(u64_to_usize(len), 0);
    // req_buffer.resize(u64_to_usize(len), 0);
    unsafe {
        buffer.set_len(len);
    }
}
