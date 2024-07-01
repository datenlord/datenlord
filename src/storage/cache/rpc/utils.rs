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

/// Try to get data from buffer with range
pub fn get_from_buf(buf: &[u8], start: usize) -> Result<u64, RpcError<String>> {
    buf.get(start..start + 8)
        .ok_or_else(|| RpcError::InternalError("Buffer slice out of bounds".to_owned()))
        .and_then(|bytes| {
            bytes
                .try_into()
                .map_err(|_foo| RpcError::InternalError("Slice conversion failed".to_owned()))
        })
        .map(u64::from_be_bytes)
}

/// Converts [`u64`] to [`usize`], and panic when the conversion fails.
#[allow(clippy::missing_const_for_fn)] // <- false positive
#[allow(clippy::expect_used)] // We can ensure that this method won't panic if we followed the hints above
#[inline]
#[must_use]
pub fn u64_to_usize(x: u64) -> usize {
    usize::try_from(x).expect("number cast failed")
}
