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
    #[allow(dead_code)]
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

#[cfg(test)]
mod test {
    use futures::prelude::*;
    use futures::stream::StreamExt;
    use smol::{self, blocking};
    use std::fs::{self, File};
    use std::io;

    use super::FuseBufReadStream;

    #[test]
    fn test_for_each_concurrent() -> io::Result<()> {
        smol::run(async move {
            let capacity = 1024;
            let dir = blocking!(fs::read_dir("./src"))?;
            let dir = smol::iter(dir);
            dir.try_for_each_concurrent(2, |entry| async move {
                let path = entry.path();
                if path.is_dir() {
                    println!("skip directory: {:?}", path);
                } else {
                    println!("read file: {:?}", path);
                    let file = blocking!(File::open(path))?;
                    let file = smol::reader(file);
                    FuseBufReadStream::with_capacity(capacity, file)
                        .for_each_concurrent(10, |res| async move {
                            match res {
                                Ok(byte_vec) => {
                                    println!("read {} bytes", byte_vec.len());
                                    let output_length = 16;
                                    if byte_vec.len() > output_length {
                                        println!(
                                            "first {} bytes: {:?}",
                                            output_length,
                                            &byte_vec[..output_length]
                                        );
                                    } else {
                                        println!("total bytes: {:?}", byte_vec);
                                    }
                                    // Ok::<(), Error>(())
                                }
                                Err(err) => {
                                    println!("read file failed, the error is: {:?}", err);
                                }
                            }
                        })
                        .await;
                }
                Ok(())
            })
            .await?;
            Ok(())
        })
    }
}
