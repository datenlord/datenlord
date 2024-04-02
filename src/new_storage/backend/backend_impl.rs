//! The general backend implementation with `openDAL`

use async_trait::async_trait;
use datenlord::config::{StorageParams, StorageS3Config};
use datenlord::metrics::DATENLORD_REGISTRY;
use opendal::layers::PrometheusLayer;
use opendal::raw::oio::ReadExt;
use opendal::services::{Fs, Memory, S3};
use opendal::{ErrorKind, Operator};
use prometheus::{exponential_buckets, linear_buckets};
use tokio::io::AsyncWriteExt;

use super::{Backend, StorageResult};

/// A builder to build `BackendWrapper`.
#[derive(Debug)]
pub struct BackendBuilder {
    /// The storage config
    config: StorageParams,
}

impl BackendBuilder {
    /// Create a backend builder.
    #[must_use]
    pub fn new(config: StorageParams) -> Self {
        Self { config }
    }

    /// Build the backend.
    #[allow(clippy::expect_used, clippy::unwrap_in_result)] // `.expect()` here are ensured not to panic.
    pub fn build(self) -> opendal::Result<BackendImpl> {
        let BackendBuilder { config } = self;

        let layer = PrometheusLayer::with_registry(DATENLORD_REGISTRY.clone())
            .bytes_total_buckets(
                exponential_buckets(1024.0, 2.0, 10).expect("Arguments are legal."),
            )
            .requests_duration_seconds_buckets(
                linear_buckets(0.005, 0.005, 20).expect("Arguments are legal."),
            );

        let operator = match config {
            StorageParams::S3(StorageS3Config {
                ref endpoint_url,
                ref access_key_id,
                ref secret_access_key,
                ref bucket_name,
            }) => {
                let mut builder = S3::default();

                builder
                    .endpoint(endpoint_url)
                    .access_key_id(access_key_id)
                    .secret_access_key(secret_access_key)
                    .region("auto")
                    .bucket(bucket_name);

                Operator::new(builder)?.layer(layer).finish()
            }
            StorageParams::Fs(ref root) => {
                let mut builder = Fs::default();
                builder.root(root);
                Operator::new(builder)?.layer(layer).finish()
            }
        };

        Ok(BackendImpl::new(operator))
    }
}

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
