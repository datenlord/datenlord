use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc, RwLock},
};

use radix_trie::Trie;
use tracing::{debug, error};

use super::{
    backend::{Backend, FSBackend},
    block::{Block, MetaData, BLOCK_SIZE},
    policy::{EvictPolicy, LRUPolicy},
    StorageResult,
};

/// CacheManager struct to manage cache
#[derive(Debug)]
pub struct CacheManager<K, P>
where
    K: Eq + std::hash::Hash + Clone,
    P: EvictPolicy<K>,
{
    policy: P,
    cache: HashMap<K, Arc<RwLock<Block>>>,
}

impl<K, P> CacheManager<K, P>
where
    K: Eq + std::hash::Hash + Clone,
    P: EvictPolicy<K>,
{
    /// Create a new CacheManager
    pub fn new(policy: P) -> Self {
        CacheManager {
            policy,
            cache: HashMap::new(),
        }
    }

    /// Insert a block into the cache
    pub fn put(&mut self, key: K, block: Arc<RwLock<Block>>) {
        // If the cache is full, evict the least recently used block
        if self.cache.len() >= self.policy.size() {
            if let Some(evicted_block) = self.policy.evict() {
                self.cache.remove(&evicted_block);
            }
        }

        // Insert the block into the cache and update the metadata
        self.cache.insert(key.clone(), block.clone());
        self.policy.access(&key);
    }

    /// Get a block from the cache
    pub fn get(&self, key: &K) -> Option<Arc<RwLock<Block>>> {
        if let Some(block) = self.cache.get(key) {
            // Get the block from the cache and update the policy
            self.policy.access(key);
            Some(block.clone())
        } else {
            None
        }
    }

    /// Remove a block from the cache
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

/// BlockManager struct to manage blocks
#[allow(dead_code)]
#[derive(Debug)]
pub struct BlockManager {
    /// CacheManager to manage blocks and metadata
    cache: Arc<RwLock<CacheManager<MetaData, LRUPolicy<MetaData>>>>,
    /// Backend to interact with the storage, default is FSBackend, we need to flush block to local fs
    backend: Arc<dyn Backend>,
}

impl BlockManager {
    /// Create a new BlockManager
    pub fn new(backend: Arc<dyn Backend>) -> Self {
        // Create a new LRUPolicy with a capacity of 2000
        // It will evict the least recently used block when the cache is full
        // TODO: Support mem limit and block size limit， current is block count limit
        let policy = LRUPolicy::new(2000);
        let cache = Arc::new(RwLock::new(CacheManager::new(policy)));
        BlockManager { cache, backend }
    }

    /// Create a new KVBlockManager with default FSBackend
    pub fn default() -> Self {
        let backend = Arc::new(FSBackend::default());
        Self::new(backend)
    }

    /// Try to read the block from the cache, if not found, try to read from the storage
    /// TODO: We need to compare the meta data version with the latest version in the storage
    /// If the version is old, we need to read the block from the storage
    /// If the version is the latest, client need to fetch the latest version
    #[allow(dead_code)]
    pub async fn read(&self, meta_data: MetaData) -> StorageResult<Option<Block>> {
        // Try to read the block from the cache
        {
            if let Some(block_ref) = self.cache.read().unwrap().get(&meta_data) {
                let block = block_ref.read().unwrap();
                return Ok(Some(block.clone()));
            }
        }

        // Try to fetch data and update local cache
        {
            // Try to read file from fs backend
            let relative_path = meta_data.to_id();
            debug!("Read block from backend: {}", relative_path);
            let mut buf = [0; BLOCK_SIZE];
            let size = self
                .backend
                .read(&relative_path, &mut buf)
                .await
                .unwrap_or_default();

            debug!("Read block size: {}", size);

            // If the size is not equal to BLOCK_SIZE, the block is invalid
            if size != BLOCK_SIZE {
                // Remove the invalid block from fs backend
                self.backend
                    .remove(&relative_path)
                    .await
                    .unwrap_or_default();
                debug!("Invalid block: {}", relative_path);
                return Ok(None);
            }

            // error!("Read block len: {:?}", buf.len());
            // error!("Read block inner content: {:?}", buf);

            let block = Block::new(meta_data.clone(), buf.to_vec());

            // error!("Read block: {:?}", block);

            // Write the block to the cache
            let block_ref = Arc::new(RwLock::new(block.clone()));
            self.cache
                .write()
                .unwrap()
                .put(meta_data.clone(), block_ref);

            // error!("Read block: {:?}", block);

            return Ok(Some(block));
        }
    }

    /// Write the block to the cache
    /// TODO: Now this operation only support store block to cache and fs storage
    #[allow(dead_code)]
    pub async fn write(&mut self, block: &Block) -> StorageResult<()> {
        let meta = block.get_meta_data();
        let relative_path = meta.to_id();

        // Write the block to the cache
        let block_ref = Arc::new(RwLock::new(block.clone()));
        self.cache.write().unwrap().put(meta.clone(), block_ref);

        // Write the block to the storage
        let _ = self
            .backend
            .store(&relative_path, block.get_data().as_slice())
            .await
            .map_err(|e| format!("Failed to write block: {}", e));

        Ok(())
    }

    /// Invalidate the block
    #[allow(dead_code)]
    pub async fn invalid(&mut self, meta_data: MetaData) -> StorageResult<()> {
        // Remove the block from the cache
        self.cache.write().unwrap().remove(&meta_data);

        // Remove the block from the storage
        let relative_path = meta_data.to_id();
        let _ = self
            .backend
            .remove(&relative_path)
            .await
            .map_err(|e| format!("Failed to remove block: {}", e));

        Ok(())
    }
}

/// KVBlockManager struct to manage blocks
/// TODO: remove disk backend cache.
#[allow(dead_code)]
#[derive(Debug)]
pub struct KVBlockManager {
    /// CacheManager to manage blocks and metadata
    cache: Arc<RwLock<CacheManager<MetaData, LRUPolicy<MetaData>>>>,
    /// Backend to interact with the storage, default is FSBackend, we need to flush block to local fs
    backend: Arc<dyn Backend>,
}

impl KVBlockManager {
    /// Create a new KVBlockManager
    pub fn new(backend: Arc<dyn Backend>) -> Self {
        // Create a new LRUPolicy with a capacity of 2000
        // It will evict the least recently used block when the cache is full
        // TODO: Support mem limit and block size limit， current is block count limit
        let policy = LRUPolicy::new(2000);
        let cache = Arc::new(RwLock::new(CacheManager::new(policy)));
        KVBlockManager { cache, backend }
    }

    /// Create a new KVBlockManager with default FSBackend
    pub fn default() -> Self {
        let backend = Arc::new(FSBackend::default());
        Self::new(backend)
    }

    /// Try to read the block from the cache, if not found, try to read from the storage
    /// TODO: We need to compare the meta data version with the latest version in the storage
    /// If the version is old, we need to read the block from the storage
    /// If the version is the latest, client need to fetch the latest version
    #[allow(dead_code)]
    pub async fn read(&self, meta_data: MetaData) -> StorageResult<Option<Block>> {
        // Try to read the block from the cache
        {
            if let Some(block_ref) = self.cache.read().unwrap().get(&meta_data) {
                let block = block_ref.read().unwrap();
                return Ok(Some(block.clone()));
            }
        }

        // Try to fetch data and update local cache
        {
            // Try to read file from fs backend
            let relative_path = meta_data.to_id();
            debug!("Read block from backend: {}", relative_path);
            let mut buf = [0; BLOCK_SIZE];
            let size = self
                .backend
                .read(&relative_path, &mut buf)
                .await
                .unwrap_or_default();

            debug!("Read block size: {}", size);

            // If the size is not equal to BLOCK_SIZE, the block is invalid
            if size != BLOCK_SIZE {
                // Remove the invalid block from fs backend
                self.backend
                    .remove(&relative_path)
                    .await
                    .unwrap_or_default();
                debug!("Invalid block: {}", relative_path);
                return Ok(None);
            }

            // error!("Read block len: {:?}", buf.len());
            // error!("Read block inner content: {:?}", buf);

            let block = Block::new(meta_data.clone(), buf.to_vec());

            // error!("Read block: {:?}", block);

            // Write the block to the cache
            let block_ref = Arc::new(RwLock::new(block.clone()));
            self.cache
                .write()
                .unwrap()
                .put(meta_data.clone(), block_ref);

            // error!("Read block: {:?}", block);

            return Ok(Some(block));
        }
    }

    /// Write the block to the cache
    /// TODO: Now this operation only support store block to cache and fs storage
    #[allow(dead_code)]
    pub async fn write(&self, block: &Block) -> StorageResult<()> {
        let meta = block.get_meta_data();
        let relative_path = meta.to_id();

        // Write the block to the cache
        let block_ref = Arc::new(RwLock::new(block.clone()));
        self.cache.write().unwrap().put(meta.clone(), block_ref);

        // Write the block to the storage
        let _ = self
            .backend
            .store(&relative_path, block.get_data().as_slice())
            .await
            .map_err(|e| format!("Failed to write block: {}", e));

        Ok(())
    }

    /// Invalidate the block
    #[allow(dead_code)]
    pub async fn invalid(&mut self, meta_data: MetaData) -> StorageResult<()> {
        // Remove the block from the cache
        self.cache.write().unwrap().remove(&meta_data);

        // Remove the block from the storage
        let relative_path = meta_data.to_id();
        let _ = self
            .backend
            .remove(&relative_path)
            .await
            .map_err(|e| format!("Failed to remove block: {}", e));

        Ok(())
    }
}

/// IndexManager struct to manage kv cache index.
#[allow(dead_code)]
#[derive(Debug)]
pub struct IndexManager {
    /// Global Radix tree to store prefix index, key is prompt, value is (block id, offset, size, address)
    index: Arc<RwLock<Trie<String, String>>>,
    /// Global block id allocator
    id_allocator: AtomicU64,
}

impl IndexManager {
    /// Create a new IndexManager
    pub fn new() -> Self {
        IndexManager {
            index: Arc::new(RwLock::new(Trie::new())),
            id_allocator: AtomicU64::new(0),
        }
    }

    /// Allocate a new block id
    pub fn allocate_id(&self) -> u64 {
        self.id_allocator
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Insert a new key-value pair into the index
    pub fn insert(&self, key: String, value: String) {
        match self.index.write() {
            Ok(mut write_lock) => {
                write_lock.insert(key, value);
            }
            Err(e) => {
                error!("Failed to get write lock: {}", e);
            }
        }
    }

    /// Remove a key-value pair from the index
    pub fn remove(&self, key: String) {
        match self.index.write() {
            Ok(mut write_lock) => {
                write_lock.remove(&key);
            }
            Err(e) => {
                error!("Failed to get write lock: {}", e);
            }
        }
    }

    /// Get the value by key from the index
    pub fn get(&self, key: &str) -> Option<String> {
        match self.index.read() {
            Ok(read_lock) => read_lock.get(key).cloned(),
            Err(e) => {
                error!("Failed to get read lock: {}", e);
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_manager() {
        let index_manager = IndexManager::new();
        index_manager.insert("test".to_owned(), "123".to_owned());
        assert_eq!(index_manager.get("test"), Some("123".to_owned()));
        index_manager.remove("test".to_owned());
        assert_eq!(index_manager.get("test"), None);
    }
}
