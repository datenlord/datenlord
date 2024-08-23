//! The general backend implementation with `openDAL`

use std::sync::Arc;

use crate::async_fuse::util::usize_to_u64;
use crate::new_storage::utils::get_block_from_path;
use crate::storage::distribute_cache::client::DistributeCacheClient;
use crate::storage::distribute_cache::cluster::cluster_manager::ClusterManager;
use async_trait::async_trait;
use tracing::error;

use super::{Backend, StorageResult};

/// A distribute cache backend builder to build `BackendWrapper`.
#[derive(Debug)]
pub struct DistributeCacheBackendBuilder {
    /// The block size
    block_size: usize,
    /// Distribute cache client
    distribute_cache_cluster_manager: Arc<ClusterManager>,
    /// Backend operator
    backend: Arc<dyn Backend>,
}

impl DistributeCacheBackendBuilder {
    /// Create a backend builder.
    #[must_use]
    pub fn new(
        block_size: usize,
        distribute_cache_cluster_manager: Arc<ClusterManager>,
        backend: Arc<dyn Backend>,
    ) -> Self {
        Self {
            block_size,
            distribute_cache_cluster_manager,
            backend,
        }
    }

    /// Build the backend.
    #[allow(clippy::expect_used, clippy::unwrap_in_result)] // `.expect()` here are ensured not to panic.
    pub async fn build(self) -> opendal::Result<DistributeCacheBackendImpl> {
        let DistributeCacheBackendBuilder {
            block_size,
            distribute_cache_cluster_manager,
            backend,
        } = self;

        let distribute_cache_client = DistributeCacheClient::new(distribute_cache_cluster_manager);
        distribute_cache_client
            .start_watch()
            .await
            .expect("Failed to start watch task.");

        Ok(DistributeCacheBackendImpl {
            block_size,
            distribute_cache_client,
            backend,
        })
    }
}

/// The `DistributeCacheBackendImpl` struct represents a backend storage system that implements
/// the `Backend` trait.
#[derive(Debug)]
pub struct DistributeCacheBackendImpl {
    /// The block size
    block_size: usize,
    /// Distribute cache client, option
    distribute_cache_client: DistributeCacheClient,
    /// The backend
    backend: Arc<dyn Backend>,
}

impl DistributeCacheBackendImpl {
    /// Creates a new `DistributeCacheBackendImpl` instance with the given operator.
    #[must_use]
    pub fn new(
        block_size: usize,
        distribute_cache_client: DistributeCacheClient,
        backend: Arc<dyn Backend>,
    ) -> Self {
        Self {
            block_size,
            distribute_cache_client,
            backend,
        }
    }
}

#[async_trait]
impl Backend for DistributeCacheBackendImpl {
    #[inline]
    async fn read(&self, path: &str, buf: &mut [u8], version: u64) -> StorageResult<usize> {
        // Get ino and other info from current path
        let (ino, block_id) = get_block_from_path(path);
        // Try to read block from distribute cache
        match self
            .distribute_cache_client
            .read_block(ino, block_id, version, usize_to_u64(self.block_size))
            .await
        {
            Ok(block) => {
                error!(
                    "Read block from distribute cache: ino={}, block_id={}, version={} block={:?}",
                    ino, block_id, version, block,
                );
                buf.copy_from_slice(block.as_slice());
                // let elapsed = start.elapsed();
                // error!("Read block from distribute cache cost: {:?}", elapsed);
                return Ok(self.block_size);
            }
            Err(e) => {
                error!(
                    "Failed to read block from distribute cache: {:?}, will change to backend",
                    e
                );
            }
        }

        Ok(self.block_size)
    }

    #[inline]
    async fn write(&self, path: &str, buf: &[u8], version: u64) -> StorageResult<()> {
        self.backend.write(path, buf, version).await
    }

    #[inline]
    async fn remove(&self, path: &str) -> StorageResult<()> {
        self.backend.remove(path).await
    }

    /// Removes all files in the `dir`. Not supported in some kinds of backend,
    /// such as `Memory`.
    #[inline]
    async fn remove_all(&self, dir: &str) -> StorageResult<()> {
        self.backend.remove_all(dir).await
    }
}
