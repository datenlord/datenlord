use futures::io::AsyncRead;
use futures::stream::Stream;
use futures::task::{Context, Poll};
use log::debug;
use pin_project_lite::pin_project;
use std::iter;
use std::pin::Pin;

pin_project! {
    #[derive(Debug)]
    pub(crate) struct FuseBufReadStream<R> {
        #[pin]
        reader: R,
        buf: Vec<u8>,
        cap: usize,
    }
}

impl<R: AsyncRead> FuseBufReadStream<R> {
    pub fn with_capacity(capacity: usize, reader: R) -> FuseBufReadStream<R> {
        FuseBufReadStream {
            reader,
            buf: iter::repeat(0u8).take(capacity).collect(),
            cap: capacity,
        }
    }
}

impl<R: AsyncRead> Stream for FuseBufReadStream<R> {
    type Item = std::io::Result<Vec<u8>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        unsafe {
            this.buf.set_len(this.buf.capacity());
        }
        let read_size = futures_core::ready!(this.reader.poll_read(cx, this.buf))?;
        debug!("FuseBufReadStream read {} bytes", read_size);
        if read_size == 0 {
            return Poll::Ready(None);
        }
        unsafe {
            this.buf.set_len(read_size);
        }

        Poll::Ready(Some(Ok(std::mem::replace(
            this.buf,
            // TODO: FIXME! It should preallocate all Vec<u8> buffers before reading
            iter::repeat(0u8).take(*this.cap).collect(),
        ))))
    }
}

pub(crate) trait FuseBufReadExt {
    fn fuse_read_stream(self, capacity: usize) -> FuseBufReadStream<Self>
    where
        Self: futures::io::AsyncRead + Unpin + Sized,
    {
        FuseBufReadStream::with_capacity(capacity, self)
    }
}

impl FuseBufReadExt for std::fs::File {}
