//! Mock types for backend testing.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::Poll;

use async_trait::async_trait;
use futures::FutureExt;
use opendal::raw::oio::{BlockingRead, BlockingWrite, Read, Write};
use opendal::raw::{
    Accessor, Layer, LayeredAccessor, OpDelete, OpList, OpRead, OpWrite, RpDelete, RpList, RpRead,
    RpWrite,
};
use opendal::ErrorKind as OpenDalErrorKind;

fn arc_atom_true() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(true))
}

/// An `openDAL` layer to simulate IO errors of backend.
#[derive(Debug, Clone)]
pub struct FilterLayer {
    /// Writer related
    pub write: WriterAttr,
    /// Reader related
    pub read: ReaderAttr,
    /// To remove a file / blocks from backend.
    pub remove: Arc<AtomicBool>,
}

impl FilterLayer {
    pub fn remove(&self) -> bool {
        self.remove.load(Ordering::Acquire)
    }

    pub fn enable_remove(&self) -> &Self {
        self.remove.store(true, Ordering::Release);
        self
    }

    pub fn disable_remove(&self) -> &Self {
        self.remove.store(false, Ordering::Release);
        self
    }
}

impl Default for FilterLayer {
    fn default() -> Self {
        Self {
            write: WriterAttr::default(),
            read: ReaderAttr::default(),
            remove: arc_atom_true(),
        }
    }
}

impl<A: Accessor> Layer<A> for FilterLayer {
    type LayeredAccessor = FilteredAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        FilteredAccessor {
            attr: self.clone(),
            inner,
        }
    }
}

/// Writer related attributes
#[derive(Debug, Clone)]
pub struct WriterAttr {
    /// To get a writer.
    pub get: Arc<AtomicBool>,
    /// To write into the writer.
    pub write: Arc<AtomicBool>,
    /// To flush (close) the writer.
    pub close: Arc<AtomicBool>,
}

impl WriterAttr {
    pub fn get(&self) -> bool {
        self.get.load(Ordering::Acquire)
    }

    #[allow(dead_code)]
    pub fn enable_get(&self) -> &Self {
        self.get.store(true, Ordering::Release);
        self
    }

    pub fn disable_get(&self) -> &Self {
        self.get.store(false, Ordering::Release);
        self
    }

    pub fn write(&self) -> bool {
        self.write.load(Ordering::Acquire)
    }

    pub fn enable_write(&self) -> &Self {
        self.write.store(true, Ordering::Release);
        self
    }

    pub fn disable_write(&self) -> &Self {
        self.write.store(false, Ordering::Release);
        self
    }

    pub fn close(&self) -> bool {
        self.close.load(Ordering::Acquire)
    }

    pub fn enable_close(&self) -> &Self {
        self.close.store(true, Ordering::Release);
        self
    }

    pub fn disable_close(&self) -> &Self {
        self.close.store(false, Ordering::Release);
        self
    }
}

impl Default for WriterAttr {
    fn default() -> Self {
        Self {
            get: arc_atom_true(),
            write: arc_atom_true(),
            close: arc_atom_true(),
        }
    }
}

/// Reader related attributes
#[derive(Debug, Clone)]
pub struct ReaderAttr {
    /// To get a reader.
    pub get: Arc<AtomicBool>,
    /// To read from the reader.
    pub read: Arc<AtomicBool>,
}

impl ReaderAttr {
    pub fn get(&self) -> bool {
        self.get.load(Ordering::Acquire)
    }

    #[allow(dead_code)]
    pub fn enable_get(&self) -> &Self {
        self.get.store(true, Ordering::Release);
        self
    }

    pub fn disable_get(&self) -> &Self {
        self.get.store(false, Ordering::Release);
        self
    }

    pub fn read(&self) -> bool {
        self.read.load(Ordering::Acquire)
    }

    pub fn enable_read(&self) -> &Self {
        self.read.store(true, Ordering::Release);
        self
    }

    pub fn disable_read(&self) -> &Self {
        self.read.store(false, Ordering::Release);
        self
    }
}

impl Default for ReaderAttr {
    fn default() -> Self {
        Self {
            get: arc_atom_true(),
            read: arc_atom_true(),
        }
    }
}

pub struct ReaderWrapper<R> {
    attr: ReaderAttr,
    inner: R,
}

impl<R: Read> Read for ReaderWrapper<R> {
    fn poll_seek(
        &mut self,
        cx: &mut std::task::Context<'_>,
        pos: std::io::SeekFrom,
    ) -> Poll<opendal::Result<u64>> {
        self.inner.poll_seek(cx, pos)
    }

    fn poll_next(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<opendal::Result<hyper::body::Bytes>>> {
        let flag = self.attr.read();
        if flag {
            self.inner.poll_next(cx)
        } else {
            Poll::Ready(Some(Err(opendal::Error::new(
                OpenDalErrorKind::PermissionDenied,
                "Disabled",
            ))))
        }
    }

    fn poll_read(
        &mut self,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> Poll<opendal::Result<usize>> {
        let flag = self.attr.read();
        if flag {
            self.inner.poll_read(cx, buf)
        } else {
            Poll::Ready(Err(opendal::Error::new(
                OpenDalErrorKind::PermissionDenied,
                "Disabled",
            )))
        }
    }
}

impl<R: BlockingRead> BlockingRead for ReaderWrapper<R> {
    fn read(&mut self, buf: &mut [u8]) -> opendal::Result<usize> {
        let flag = self.attr.read();
        if flag {
            self.inner.read(buf)
        } else {
            Err(opendal::Error::new(
                OpenDalErrorKind::PermissionDenied,
                "Disabled",
            ))
        }
    }

    fn seek(&mut self, pos: std::io::SeekFrom) -> opendal::Result<u64> {
        self.inner.seek(pos)
    }

    fn next(&mut self) -> Option<opendal::Result<hyper::body::Bytes>> {
        let flag = self.attr.read();
        if flag {
            self.inner.next()
        } else {
            Some(Err(opendal::Error::new(
                OpenDalErrorKind::PermissionDenied,
                "Disabled",
            )))
        }
    }
}

pub struct WriterWrapper<W> {
    attr: WriterAttr,
    inner: W,
}
impl<W: Write> Write for WriterWrapper<W> {
    fn poll_write(
        &mut self,
        cx: &mut std::task::Context<'_>,
        bs: &dyn opendal::raw::oio::WriteBuf,
    ) -> Poll<opendal::Result<usize>> {
        let flag = self.attr.write();
        if flag {
            self.inner.poll_write(cx, bs)
        } else {
            Poll::Ready(Err(opendal::Error::new(
                OpenDalErrorKind::PermissionDenied,
                "Disabled",
            )))
        }
    }

    fn poll_close(&mut self, cx: &mut std::task::Context<'_>) -> Poll<opendal::Result<()>> {
        let flag = self.attr.close();
        if flag {
            self.inner.poll_close(cx)
        } else {
            Poll::Ready(Err(opendal::Error::new(
                OpenDalErrorKind::PermissionDenied,
                "Disabled",
            )))
        }
    }

    fn poll_abort(&mut self, cx: &mut std::task::Context<'_>) -> Poll<opendal::Result<()>> {
        self.inner.poll_abort(cx)
    }
}

impl<W: BlockingWrite> BlockingWrite for WriterWrapper<W> {
    fn write(&mut self, bs: &dyn opendal::raw::oio::WriteBuf) -> opendal::Result<usize> {
        let flag = self.attr.write();
        if flag {
            self.inner.write(bs)
        } else {
            Err(opendal::Error::new(
                OpenDalErrorKind::PermissionDenied,
                "Disabled",
            ))
        }
    }

    fn close(&mut self) -> opendal::Result<()> {
        let flag = self.attr.close();
        if flag {
            self.inner.close()
        } else {
            Err(opendal::Error::new(
                OpenDalErrorKind::PermissionDenied,
                "Disabled",
            ))
        }
    }
}

#[derive(Debug)]
pub struct FilteredAccessor<A> {
    attr: FilterLayer,
    inner: A,
}

#[async_trait]
impl<A: Accessor> LayeredAccessor for FilteredAccessor<A> {
    type BlockingLister = A::BlockingLister;
    type BlockingReader = ReaderWrapper<A::BlockingReader>;
    type BlockingWriter = WriterWrapper<A::BlockingWriter>;
    type Inner = A;
    type Lister = A::Lister;
    type Reader = ReaderWrapper<A::Reader>;
    type Writer = WriterWrapper<A::Writer>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> opendal::Result<(RpRead, Self::Reader)> {
        if self.attr.read.get() {
            self.inner
                .read(path, args)
                .map(|res| {
                    res.map(|(rp_read, reader)| {
                        (
                            rp_read,
                            ReaderWrapper {
                                attr: self.attr.read.clone(),
                                inner: reader,
                            },
                        )
                    })
                })
                .await
        } else {
            Err(opendal::Error::new(
                OpenDalErrorKind::PermissionDenied,
                "Disabled",
            ))
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> opendal::Result<(RpWrite, Self::Writer)> {
        if self.attr.write.get() {
            self.inner
                .write(path, args)
                .map(|res| {
                    res.map(|(rp_write, writer)| {
                        (
                            rp_write,
                            WriterWrapper {
                                attr: self.attr.write.clone(),
                                inner: writer,
                            },
                        )
                    })
                })
                .await
        } else {
            Err(opendal::Error::new(
                OpenDalErrorKind::PermissionDenied,
                "Disabled",
            ))
        }
    }

    async fn list(&self, path: &str, args: OpList) -> opendal::Result<(RpList, Self::Lister)> {
        self.inner.list(path, args).await
    }

    fn blocking_read(
        &self,
        path: &str,
        args: OpRead,
    ) -> opendal::Result<(RpRead, Self::BlockingReader)> {
        if self.attr.read.get() {
            self.inner
                .blocking_read(path, args)
                .map(|(rp_read, reader)| {
                    (
                        rp_read,
                        ReaderWrapper {
                            attr: self.attr.read.clone(),
                            inner: reader,
                        },
                    )
                })
        } else {
            Err(opendal::Error::new(
                OpenDalErrorKind::PermissionDenied,
                "Disabled",
            ))
        }
    }

    fn blocking_write(
        &self,
        path: &str,
        args: OpWrite,
    ) -> opendal::Result<(RpWrite, Self::BlockingWriter)> {
        if self.attr.write.get() {
            self.inner
                .blocking_write(path, args)
                .map(|(rp_write, writer)| {
                    (
                        rp_write,
                        WriterWrapper {
                            attr: self.attr.write.clone(),
                            inner: writer,
                        },
                    )
                })
        } else {
            Err(opendal::Error::new(
                OpenDalErrorKind::PermissionDenied,
                "Disabled",
            ))
        }
    }

    fn blocking_list(
        &self,
        path: &str,
        args: OpList,
    ) -> opendal::Result<(RpList, Self::BlockingLister)> {
        self.inner.blocking_list(path, args)
    }

    async fn delete(&self, path: &str, args: OpDelete) -> opendal::Result<RpDelete> {
        if self.attr.remove() {
            self.inner.delete(path, args).await
        } else {
            Err(opendal::Error::new(
                OpenDalErrorKind::PermissionDenied,
                "Disabled",
            ))
        }
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> opendal::Result<RpDelete> {
        if self.attr.remove() {
            self.inner.blocking_delete(path, args)
        } else {
            Err(opendal::Error::new(
                OpenDalErrorKind::PermissionDenied,
                "Disabled",
            ))
        }
    }
}
