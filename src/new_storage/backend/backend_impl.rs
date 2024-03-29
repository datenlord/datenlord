//! The general backend implementation with `openDAL`

use async_trait::async_trait;
use opendal::raw::oio::ReadExt;
use opendal::services::{Fs, Memory};
use opendal::{ErrorKind, Operator};
use tokio::io::AsyncWriteExt;

use super::{Backend, StorageResult};

/// The `BackendImpl` struct represents a backend storage system that implements
/// the `Backend` trait.
#[derive(Debug)]
pub struct BackendImpl {
    /// The inner operator
    operator: Operator,
}

impl BackendImpl {
    /// Creates a new `BackendImpl` instance with the given operator.
    #[must_use]
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }
}

#[async_trait]
impl Backend for BackendImpl {
    #[inline]
    async fn read(&self, path: &str, buf: &mut [u8]) -> StorageResult<usize> {
        let len = buf.len();
        let mut reader = self.operator.reader(path).await?;
        let mut read_size = 0;
        loop {
            let buf = buf
                .get_mut(read_size..len)
                .unwrap_or_else(|| unreachable!("The `buf` is ensured to be long enough."));
            let result = reader.read(buf).await;
            match result {
                Ok(size) => {
                    if size == 0 {
                        break;
                    }
                    read_size += size;
                }
                Err(e) => {
                    // If not found just return 0.
                    if e.kind() == ErrorKind::NotFound {
                        break;
                    }
                }
            }
        }
        Ok(read_size)
    }

    #[inline]
    async fn write(&self, path: &str, buf: &[u8]) -> StorageResult<()> {
        let mut writer = self.operator.writer(path).await?;
        writer.write_all(buf).await?;
        writer.close().await?;
        Ok(())
    }

    #[inline]
    async fn remove(&self, path: &str) -> StorageResult<()> {
        self.operator.delete(path).await?;
        Ok(())
    }

    /// Removes all files in the `dir`. Not supported in some kinds of backend,
    /// such as `Memory`.
    #[inline]
    async fn remove_all(&self, dir: &str) -> StorageResult<()> {
        self.operator.remove_all(dir).await?;
        Ok(())
    }
}

/// Creates a new `BackendImpl` instance with a memory backend.
pub fn memory_backend() -> StorageResult<BackendImpl> {
    let op = Operator::new(Memory::default())?.finish();
    Ok(BackendImpl::new(op))
}

/// Creates a new `BackendImpl` instance with a temporary file system backend.
pub fn tmp_fs_backend() -> StorageResult<BackendImpl> {
    let mut builder = Fs::default();
    builder.root("/tmp/backend/");
    let op = Operator::new(builder)?.finish();
    Ok(BackendImpl::new(op))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {

    use super::super::test_backend;
    use super::*;

    #[tokio::test]
    async fn test_memory_backend() {
        let backend = memory_backend().unwrap();
        test_backend(backend).await;
    }

    #[tokio::test]
    async fn test_fs_backend() {
        let backend = tmp_fs_backend().unwrap();
        test_backend(backend).await;
    }

    #[tokio::test]
    async fn test_remove_all() {
        let backend = tmp_fs_backend().unwrap();
        let mut buf = vec![0; 16];
        backend.write("a/1", &buf).await.unwrap();
        backend.write("a/2", &buf).await.unwrap();

        backend.remove_all("a/").await.unwrap();

        let size = backend.read("a/1", &mut buf).await.unwrap();
        assert_eq!(size, 0);
        let size = backend.read("a/2", &mut buf).await.unwrap();
        assert_eq!(size, 0);
    }
}
