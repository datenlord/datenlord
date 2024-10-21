use async_trait::async_trait;
use opendal::{raw::oio::ReadExt, services::Fs, ErrorKind, Operator};
use tokio::io::AsyncWriteExt;
use tracing::debug;

use std::fmt::Debug;

use crate::storage::distribute_kv_cache::local_cache::block::BLOCK_SIZE;

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
    operator: Operator,
}

impl FSBackend {
    /// Creates a new `FSBackend` instance with the given `Operator`.
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }

    /// Create a tmp backend
    pub fn default() -> Self {
        let mut builder = Fs::default();
        builder.root("/tmp/backend/");
        let operator = Operator::new(builder).unwrap().finish();
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

/// The `S3Backend` struct represents a backend storage system that uses S3.
#[derive(Debug)]
pub struct S3Backend {
    operator: Operator,
}

impl S3Backend {
    /// Creates a new `S3Backend` instance with the given `Operator`.
    /// You need to create a s3 operator and pass it to this function.
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }
}

#[async_trait]
impl Backend for S3Backend {
    /// TODO: Add rate limit for s3
    /// Reads data from the storage system into the given buffer.
    #[inline]
    async fn read(&self, path: &str, buf: &mut [u8]) -> StorageResult<usize> {
        debug!("S3Backend read path: {}", path);
        let mut reader = self.operator.reader(path).await?;
        let mut read_size = 0;

        loop {
            // Read data and catch the size
            // Calculate the remaining buffer size
            let remaining_buf = &mut buf[read_size..];
            let result = reader.read(remaining_buf).await;
            match result {
                Ok(0) => break,
                Ok(size) => {
                    // The size is not full, we should break the loop
                    read_size += size;
                    if size == BLOCK_SIZE {
                        break;
                    }
                }
                Err(e) => {
                    // If not found just return 0.
                    if e.kind() == ErrorKind::NotFound {
                        break;
                    }
                }
            }
        }

        // error!("S3Backend read size: {} and buf: {:?}", read_size, buf);

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
