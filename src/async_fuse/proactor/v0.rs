//! Proactor API version 0

use std::io;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::os::unix::io::RawFd;

use nix::unistd;
use ring_io::sqe::PrepareSqe;

use super::global::{IoRequest, Operation};
use super::small_box::SmallBox;
use crate::async_fuse::util::u32_to_usize;

/// An owned file descriptor
struct Fd(RawFd);

impl Drop for Fd {
    fn drop(&mut self) {
        debug_assert!(unistd::close(self.0).is_ok());
    }
}

impl Fd {
    /// Gets the wrapped [`RawFd`]. The raw fd is not closed.
    fn into_inner(self) -> RawFd {
        let this = ManuallyDrop::new(self);
        this.0
    }
}

/// Reads up to `count` bytes from file descriptor `fd` at `offset`
/// (from the start of the file) into the `buffer`.
/// The file offset is not changed.
///
/// **`fd` must be valid until the future returned by `read` is completed.**
/// It is a logic error to change `fd`'s state before the future is completed.
///
/// If the future has been dropped before the IO completes,
/// **`fd` will be closed after the IO finishes**.
pub async fn read(
    fd: RawFd,
    buffer: Vec<u8>,
    count: usize,
    offset: isize,
) -> (io::Result<usize>, (RawFd, Vec<u8>)) {
    /// Operation Read
    struct Read {
        /// fd
        fd: Fd,
        /// buffer
        buffer: Vec<u8>,
        /// count
        count: usize,
        /// offset
        offset: isize,
    }

    impl Operation for Read {
        unsafe fn prepare(
            &mut self,
            storage: &mut SmallBox,
            sqe: &mut MaybeUninit<ring_io::sqe::SQE>,
        ) {
            let iov_ptr = storage.put_unchecked([libc::iovec {
                iov_base: self.buffer.as_mut_ptr().cast(),
                iov_len: self.count,
            }]);
            sqe.prep_readv(self.fd.0, iov_ptr.cast(), 1, self.offset);
        }
    }

    assert!(count <= buffer.len());

    let io = IoRequest::new(Read {
        fd: Fd(fd),
        buffer,
        count,
        offset,
    });

    let (result, data) = io.await;

    (
        result.map(u32_to_usize),
        (data.fd.into_inner(), data.buffer),
    )
}

/// Writes up to `count` bytes from the `buffer`
/// to the file descriptor `fd` at `offset` (from the start of the file).
/// The file offset is not changed.
///
/// **`fd` must be valid until the future returned by `write` is completed.**
/// It is a logic error to change `fd`'s state before the future is completed.
///
/// If the future has been dropped before the IO completes,
/// **`fd` will be closed after the IO finishes**.
pub async fn write(
    fd: RawFd,
    buffer: Vec<u8>,
    count: usize,
    offset: isize,
) -> (io::Result<usize>, (RawFd, Vec<u8>)) {
    /// Operation Write
    struct Write {
        /// fd
        fd: Fd,
        /// buffer
        buffer: Vec<u8>,
        /// count
        count: usize,
        /// offset
        offset: isize,
    }

    impl Operation for Write {
        unsafe fn prepare(
            &mut self,
            storage: &mut SmallBox,
            sqe: &mut MaybeUninit<ring_io::sqe::SQE>,
        ) {
            let iov_ptr = storage.put_unchecked([libc::iovec {
                iov_base: self.buffer.as_mut_ptr().cast(),
                iov_len: self.count,
            }]);
            sqe.prep_writev(self.fd.0, iov_ptr.cast(), 1, self.offset);
        }
    }

    assert!(count <= buffer.len());

    let io = IoRequest::new(Write {
        fd: Fd(fd),
        buffer,
        count,
        offset,
    });

    let (result, data) = io.await;

    (
        result.map(u32_to_usize),
        (data.fd.into_inner(), data.buffer),
    )
}

#[cfg(test)]
mod tests {

    use std::fs;
    use std::os::unix::io::AsRawFd;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::common::logger::{init_logger, NodeType};

    #[tokio::test(flavor = "multi_thread")]
    async fn proactor_v0_test() -> anyhow::Result<()> {
        init_logger(NodeType::Node);

        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();

        let file_name = format!("/tmp/proactor_v0_test_{timestamp}");
        let content = "helloworld";
        let reversed_content = "dlrowolleh";

        fs::write(&file_name, content)?;

        let buf = vec![0; 64];

        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_name)?;

        let fd = file.as_raw_fd();

        let (ret, (_, mut buf)) = super::read(fd, buf, 64, 0).await;
        let nread = ret?;
        assert_eq!(buf.get(..nread), Some(content.as_bytes()));
        buf.truncate(nread);

        buf.reverse();

        let len = buf.len();
        let (ret, _) = super::write(fd, buf, len, 0).await;
        let nwritten = ret?;
        assert_eq!(nwritten, len);

        assert_eq!(fs::read_to_string(&file_name)?, reversed_content);

        Ok(())
    }
}
