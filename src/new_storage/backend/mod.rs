//! Backend related modules.

pub mod backend_impl;
pub mod memory_backend;

use std::fmt::Debug;

use async_trait::async_trait;
pub use backend_impl::{BackendBuilder, BackendImpl};

use super::error::StorageResult;

/// The `Backend` trait represents a backend storage system.
#[async_trait]
pub trait Backend: Debug + Send + Sync {
    /// Reads data from the storage system into the given buffer.
    async fn read(&self, path: &str, buf: &mut [u8]) -> StorageResult<usize>;
    /// Writes data from the given buffer into the storage system.
    async fn write(&self, path: &str, buf: &[u8]) -> StorageResult<()>;
    /// Removes the data from the storage system.
    async fn remove(&self, path: &str) -> StorageResult<()>;
    /// Removes data with the specified prefix (usually a directory)
    async fn remove_all(&self, prefix: &str) -> StorageResult<()>;
}

/// Test backend.
#[cfg(test)]
#[allow(clippy::unwrap_used)]
async fn test_backend(backend: impl Backend) {
    let path = "test";
    let data = b"hello world";
    backend.write(path, data).await.unwrap();
    let mut buf = vec![0_u8; data.len()];
    backend.read(path, &mut buf).await.unwrap();
    assert_eq!(buf, data);

    backend.remove(path).await.unwrap();
    let res = backend.read(path, &mut buf).await.unwrap();
    assert_eq!(res, 0);
}
