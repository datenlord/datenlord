use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use tokio::sync::RwLock;

use super::{
    backend::{Backend, FSBackend},
    block::{Block, MetaData, BLOCK_SIZE},
    policy::{EvictPolicy, LRUPolicy},
    StorageError, StorageResult,
};

/// `CacheManager` struct to manage cache
#[derive(Debug)]
pub struct CacheManager<K, P>
where
    K: Eq + std::hash::Hash + Clone,
    P: EvictPolicy<K>,
{
    /// EvictPolicy to manage the cache
    policy: P,
    /// Cache to store blocks
    cache: HashMap<K, Arc<RwLock<Block>>>,
}

impl<K, P> CacheManager<K, P>
where
    K: Eq + std::hash::Hash + Clone,
    P: EvictPolicy<K>,
{
    /// Create a new `CacheManager`
    #[inline]
    pub fn new(policy: P) -> Self {
        CacheManager {
            policy,
            cache: HashMap::new(),
        }
    }

    /// Insert a block into the cache
    #[inline]
    pub fn put(&mut self, key: K, block: Arc<RwLock<Block>>) {
        // If the cache is full, evict the least recently used block
        if self.cache.len() >= self.policy.size() {
            if let Some(evicted_block) = self.policy.evict() {
                self.cache.remove(&evicted_block);
            }
        }

        // Insert the block into the cache and update the metadata
        self.policy.access(&key);
        self.cache.insert(key, block);
    }

    /// Get a block from the cache
    #[inline]
    pub fn get(&self, key: &K) -> Option<Arc<RwLock<Block>>> {
        if let Some(block) = self.cache.get(key) {
            // Get the block from the cache and update the policy
            self.policy.access(key);
            Some(Arc::clone(block))
        } else {
            None
        }
    }

    /// Remove a block from the cache
    #[inline]
    pub fn remove(&mut self, key: &K) -> Option<Arc<RwLock<Block>>> {
        if let Some(block) = self.cache.remove(key) {
            // Remove the block from the cache and update the policy
            self.policy.remove(key);
            Some(block)
        } else {
            None
        }
    }
}

/// `BlockManager` struct to manage blocks
#[allow(dead_code)]
#[derive(Debug)]
pub struct BlockManager {
    /// CacheManager to manage blocks and metadata
    cache: CacheManager<MetaData, LRUPolicy<MetaData>>,
    /// Metadata to store block metadata
    metasets: Arc<RwLock<HashSet<MetaData>>>,
    /// Backend to interact with the storage, default is FSBackend
    backend: Arc<dyn Backend>,
}

impl Default for BlockManager {
    /// Create a new `BlockManager` with default `FSBackend`
    #[inline]
    fn default() -> Self {
        let backend = Arc::new(FSBackend::default());
        Self::new(backend)
    }
}

impl BlockManager {
    /// Create a new `BlockManager`
    #[inline]
    pub fn new(backend: Arc<dyn Backend>) -> Self {
        // Create a new LRUPolicy with a capacity of 1000
        // It will evict the least recently used block when the cache is full
        // TODO: Support mem limit and block size limit， current is block count limit
        let policy = LRUPolicy::new(1000);
        let cache = CacheManager::new(policy);
        let metasets = Arc::new(RwLock::new(HashSet::new()));
        BlockManager {
            cache,
            metasets,
            backend,
        }
    }

    /// Try to read the block from the cache, if not found, try to read from the storage
    /// TODO: We need to compare the meta data version with the latest version in the storage
    /// If the version is old, we need to read the block from the storage
    /// If the version is the latest, client need to fetch the latest version
    #[allow(dead_code)]
    #[allow(clippy::large_stack_arrays)] // Allow allocating a local array larger than 512000 bytes
    async fn read(&mut self, meta_data: MetaData) -> StorageResult<Option<Block>> {
        if let Some(block_ref) = self.cache.cache.get(&meta_data) {
            let block = block_ref.read().await;
            return Ok(Some(block.clone()));
        }
        // Try to read the block from current meta data
        // which means the block is not in the cache but in the storage
        // the version of the block is the latest
        let metasets_read = self.metasets.read().await;

        if metasets_read.contains(&meta_data) {
            // Try to read file from fs backend
            let relative_path = meta_data.to_id();

            let mut buf = [0; BLOCK_SIZE];
            let size = self
                .backend
                .read(&relative_path, &mut buf)
                .await
                .unwrap_or_default();

            // If the size is not equal to BLOCK_SIZE, the block is invalid
            if size != BLOCK_SIZE {
                // Remove the invalid block from fs backend
                self.backend
                    .remove(&relative_path)
                    .await
                    .unwrap_or_default();

                return Ok(None);
            }

            let block = Block::new(meta_data.clone(), buf.to_vec());

            // Write the block to the cache
            let block_ref = Arc::new(RwLock::new(block.clone()));
            self.cache.put(meta_data.clone(), block_ref);

            return Ok(Some(block));
        }

        Ok(None)
    }

    /// Write the block to the cache
    /// TODO: Now this operation only support store block to cache and fs storage
    #[allow(dead_code)]
    async fn write(&mut self, block: &Block) -> StorageResult<()> {
        let meta = block.get_meta_data();
        let relative_path = meta.to_id();

        // Write the block to the cache
        let block_ref = Arc::new(RwLock::new(block.clone()));
        self.cache.put(meta.clone(), block_ref);

        // Write the block to the storage
        self.backend
            .store(&relative_path, block.get_data().as_slice())
            .await
            .map_err(|e| StorageError::Internal(anyhow::anyhow!("Failed to store block: {}", e)))?;

        // Add the meta data to the meta sets
        let mut metasets_write = self.metasets.write().await;
        metasets_write.insert(meta.clone());

        Ok(())
    }

    /// Invalidate the block
    #[allow(dead_code)]
    async fn invalid(&mut self, meta_data: MetaData) -> StorageResult<()> {
        // Remove the block from the cache
        self.cache.remove(&meta_data);

        // Remove the block from the storage
        let relative_path = meta_data.to_id();
        self.backend.remove(&relative_path).await.map_err(|e| {
            StorageError::Internal(anyhow::anyhow!("Failed to remove block: {}", e))
        })?;

        // Remove the meta data from the meta sets
        let mut metasets_write = self.metasets.write().await;

        metasets_write.remove(&meta_data);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // TODO...
}
