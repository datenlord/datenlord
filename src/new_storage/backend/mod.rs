//! Backend related modules.

pub mod backend_impl;
pub mod distribute_cache_backend_impl;
pub mod memory_backend;

use std::fmt::Debug;

use async_trait::async_trait;
pub use backend_impl::{BackendBuilder, BackendImpl};
pub use distribute_cache_backend_impl::{
    DistributeCacheBackendBuilder, DistributeCacheBackendImpl,
};

use super::error::StorageResult;

const BLOCK_SIZE: usize = 4 * 1024 * 1024;

/// The `Backend` trait represents a backend storage system.
#[async_trait]
pub trait Backend: Debug + Send + Sync {
    /// Reads data from the storage system into the given buffer, support read with version if current backend system supports it.
    async fn read(&self, path: &str, buf: &mut [u8], version: u64) -> StorageResult<usize>;
    /// Writes data from the given buffer into the storage system, support write with version if current backend system supports it,
    /// Current write implement might need to read the block from storage first, so this function need to sync with read function.
    async fn write(&self, path: &str, buf: &[u8], version: u64) -> StorageResult<()>;
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
    let version = 0;
    backend.write(path, data, version).await.unwrap();
    let mut buf = vec![0_u8; data.len()];
    let version = 0;
    backend.read(path, &mut buf, version).await.unwrap();
    assert_eq!(buf, data);

    backend.remove(path).await.unwrap();
    let res = backend.read(path, &mut buf, version).await.unwrap();
    assert_eq!(res, 0);
}
