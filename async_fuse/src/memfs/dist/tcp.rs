use crate::fuse::fuse_reply::AsIoVec;
use crate::memfs::cache::IoMemBlock;
use std::io::{Read, Write};
use std::net::TcpStream;
use utilities::Cast;

pub(crate) fn read_message(stream: &mut TcpStream, buf: &mut Vec<u8>) -> anyhow::Result<usize> {
    let mut local_buf: [u8; 8] = [0; 8];
    stream.read_exact(&mut local_buf)?;
    let len = u64::from_be_bytes(local_buf);

    Vec::reserve(buf, len.cast());
    unsafe {
        buf.set_len(len.cast());
    }
    stream.read_exact(buf)?;
    Ok(len.cast())
}

pub(crate) fn write_message(stream: &mut TcpStream, buf: &[u8]) -> anyhow::Result<usize> {
    let len: u64 = buf.len().cast();
    let len_buf = len.to_be_bytes();
    stream.write_all(&len_buf)?;
    stream.write_all(buf)?;

    Ok(len.cast())
}

pub(crate) fn write_message_vector(
    stream: &mut TcpStream,
    buf: Vec<IoMemBlock>,
) -> anyhow::Result<usize> {
    let len: u64 = buf.iter().map(|b| b.len()).sum::<usize>().cast();
    let len_buf = len.to_be_bytes();
    stream.write_all(&len_buf)?;

    for b in buf {
        stream.write_all(unsafe { b.as_slice() })?;
    }

    Ok(len.cast())
}

pub(crate) fn write_u32(stream: &mut TcpStream, num: u32) -> anyhow::Result<()> {
    let num_buf = num.to_be_bytes();
    stream.write_all(&num_buf)?;

    Ok(())
}

pub(crate) fn read_u32(stream: &mut TcpStream) -> anyhow::Result<u32> {
    let mut local_buf: [u8; 4] = [0; 4];
    stream.read_exact(&mut local_buf)?;

    Ok(u32::from_be_bytes(local_buf))
}
