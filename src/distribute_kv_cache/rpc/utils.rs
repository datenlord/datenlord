use std::mem;

use bytes::{Buf, BufMut, BytesMut};
use tracing::debug;

use super::error::RpcError;

/// Encode value to buffer
#[macro_export]
macro_rules! encode_to_buf {
    ($buf:expr, $value:expr) => {{
        let mut writer = $buf.writer();
        if let Err(err) = bincode::serialize_into(&mut writer, $value) {
            error!("Failed to serialize: {}", err);
        }
    }};
}

/// Read with timeout options from the environment variables
#[macro_export]
macro_rules! read_exact_timeout {
    ($reader:expr, $dst:expr, $read_timeout:expr) => {
        async move {
            use tokio::time::timeout;

            match timeout($read_timeout, $reader.read_exact($dst)).await {
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
    ($writer:expr, $src:expr, $write_timeout:expr) => {
        async move {
            use tokio::time::timeout;

            match timeout($write_timeout, $writer.write_all($src)).await {
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
    ($writer:expr, $src:expr, $write_timeout:expr) => {
        async move {
            use tokio::time::timeout;

            // TODO: check all the write_vectored is done
            match timeout($write_timeout, $writer.write_vectored($src)).await {
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
    unsafe {
        buffer.set_len(len);
    }
}

/// Zero copy conversion between num key and u8 buffers.
#[allow(clippy::mem_forget)]
#[allow(clippy::as_conversions)]
#[allow(dead_code)]
fn num_to_u8_buffer<K>(input: Vec<K>) -> Vec<u8>
where
    K: num::Num,
{
    let len = input.len() * mem::size_of::<K>();
    let ptr = input.as_ptr();
    let capacity = input.capacity();

    std::mem::forget(input);

    unsafe { Vec::from_raw_parts(ptr as *mut u8, len, capacity) }
}

/// Zero copy conversion between u8 and num key buffers.
#[allow(dead_code)]
#[allow(clippy::mem_forget)]
#[allow(clippy::as_conversions)]
#[allow(clippy::integer_division)]
fn u8_to_num_buffer<K>(input: Vec<u8>) -> Vec<K>
where
    K: num::Num,
{
    assert_eq!(
        input.len() % mem::size_of::<K>(),
        0,
        "Buffer length must be a multiple of key size"
    );
    let len = input.len() / mem::size_of::<K>();
    let ptr = input.as_ptr();
    let capacity = input.capacity() / mem::size_of::<K>();

    std::mem::forget(input);

    unsafe { Vec::from_raw_parts(ptr as *mut K, len, capacity) }
}

/// Zero copy conversion between u32 and u8 buffers.
#[allow(dead_code)]
#[allow(clippy::mem_forget)]
#[allow(clippy::as_conversions)]
#[allow(clippy::integer_division)]
fn u32_to_u8_buffer(input: Vec<u32>) -> Vec<u8> {
    let len = input.len() * 4;
    let ptr = input.as_ptr();
    let capacity = input.capacity() * 4;

    std::mem::forget(input);

    unsafe { Vec::from_raw_parts(ptr as *mut u8, len, capacity) }
}

/// Zero copy conversion between u8 and u32 buffers.
#[allow(dead_code)]
#[allow(clippy::mem_forget)]
#[allow(clippy::cast_ptr_alignment)]
#[allow(clippy::as_conversions)]
#[allow(clippy::integer_division)]
fn u8_to_u32_buffer(input: Vec<u8>) -> Vec<u32> {
    assert_eq!(input.len() % 4, 0, "Buffer length must be a multiple of 4");
    let len = input.len() / 4;
    let ptr = input.as_ptr();
    let capacity = input.capacity() / 4;

    std::mem::forget(input);

    unsafe { Vec::from_raw_parts(ptr as *mut u32, len, capacity) }
}
