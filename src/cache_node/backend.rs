use async_trait::async_trait;
use clippy_utilities::OverflowArithmetic;
use opendal::{raw::oio::ReadExt, services::Fs, ErrorKind, Operator};
use tokio::io::AsyncWriteExt;

use std::fmt::Debug;

use super::StorageResult;

/// The `Backend` trait represents a backend storage system.
#[async_trait]
pub trait Backend: Debug + Send + Sync {
    /// Reads data from the storage system into the given buffer.
    async fn read(&self, path: &str, buf: &mut [u8]) -> StorageResult<usize>;
    /// Stores data from the given buffer into the storage system.
    async fn store(&self, path: &str, buf: &[u8]) -> StorageResult<()>;
    /// Removes the data from the storage system.
    async fn remove(&self, path: &str) -> StorageResult<()>;
}

/// The `FSBackend` struct represents a backend storage system that uses the filesystem.
#[derive(Debug)]
pub struct FSBackend {
    /// The operator of the storage system.
    operator: Operator,
}

impl Default for FSBackend {
    /// Create a tmp backend
    #[inline]
    #[must_use]
    fn default() -> Self {
        let mut builder = Fs::default();
        builder.root("/tmp/backend/");
        // Create a new operator with the builder, map panic if failed, do not use unwrap()
        let operator = match Operator::new(builder) {
            Ok(operator) => operator.finish(),
            Err(e) => panic!("Failed to create operator: {e:?}"),
        };
        Self { operator }
    }
}

impl FSBackend {
    /// Creates a new `FSBackend` instance with the given `Operator`.
    #[inline]
    #[must_use]
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }
}

#[async_trait]
impl Backend for FSBackend {
    /// Reads data from the storage system into the given buffer.
    #[inline]
    async fn read(&self, path: &str, buf: &mut [u8]) -> StorageResult<usize> {
        let mut reader = self.operator.reader(path).await?;
        let mut read_size = 0;

        loop {
            // Read data and catch the size
            let result = reader.read(buf).await;
            match result {
                Ok(size) => {
                    if size == 0 {
                        break;
                    }
                    read_size = read_size.overflow_add(size);
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

    /// Stores data from the given buffer into the storage system.
    #[inline]
    async fn store(&self, path: &str, buf: &[u8]) -> StorageResult<()> {
        let mut writer = self.operator.writer(path).await?;
        writer.write_all(buf).await?;
        writer.close().await?;
        Ok(())
    }

    /// Removes the data from the storage system.
    #[inline]
    async fn remove(&self, path: &str) -> StorageResult<()> {
        self.operator.remove_all(path).await?;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use opendal::services::Fs;

    #[tokio::test]
    async fn test_fs_backend() {
        let mut builder = Fs::default();
        builder.root("/tmp/backend/");
        let operator = Operator::new(builder).unwrap().finish();
        let backend = FSBackend::new(operator);

        let path = "/tmp/backend/test.txt";
        let data = b"Hello, world!";
        backend.store(path, data).await.unwrap();

        let mut buf = vec![0; data.len()];
        backend.read(path, &mut buf).await.unwrap();
        assert_eq!(buf, data);

        backend.remove(path).await.unwrap();
    }
}
