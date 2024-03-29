//! The `MemoryCache` implementation.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use parking_lot::RwLock;
use tracing::warn;

use super::block::{Block, BLOCK_SIZE};
use super::policy;

/// The `CacheKey` struct represents a key used for caching file data.
/// It consists of an inode number and a block ID.
#[derive(Debug, Hash, Clone, Copy, PartialEq, Eq)]
pub struct CacheKey {
    /// The inode number.
    pub ino: u64,
    /// The block ID.
    pub block_id: u64,
}

/// The `MemoryCache` struct is used to manage cache of blocks with a
/// specified capacity.
///
/// It uses an eviction policy to determine which blocks to evict when the cache
/// is full.
#[derive(Debug)]
pub struct MemoryCache<K, P>
where
    K: Eq + std::hash::Hash + Clone,
    P: policy::EvictPolicy<K>,
{
    /// The eviction policy used by the cache manager.
    policy: P,
    /// The only place to store the block
    map: HashMap<K, Arc<RwLock<Block>>>,
    /// The free list of blocks
    free_list: VecDeque<Arc<RwLock<Block>>>,
}

impl<K, P> MemoryCache<K, P>
where
    K: Eq + std::hash::Hash + Clone + std::fmt::Debug,
    P: policy::EvictPolicy<K>,
{
    /// Creates a new `CacheManager` with the specified capacity.
    ///
    /// The capacity is in blocks.
    #[inline]
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let mut free_list = VecDeque::with_capacity(capacity);
        for _ in 0..capacity {
            let block = Arc::new(RwLock::new(Block::new(vec![0; BLOCK_SIZE])));
            free_list.push_back(block);
        }
        MemoryCache {
            policy: P::new(capacity),
            map: HashMap::with_capacity(capacity),
            free_list,
        }
    }

    /// Get a free block from the cache manager's free list.
    fn get_free_block(&mut self, key: &K) -> Option<Arc<RwLock<Block>>> {
        // If the queue is empty, `pop_front` returns `None`.
        let new_block = self.free_list.pop_front()?;
        // Access the policy to update the internal state.
        self.map.insert(key.clone(), Arc::clone(&new_block));
        self.policy.access(key);
        new_block.write().pin();
        Some(new_block)
    }

    /// Create a new block from the cache manager's free list.
    /// Set the block's key to the given key.
    ///
    /// # Panic
    /// The method panics if `data` is longer than `BLOCK_SIZE`.
    #[inline]
    pub fn new_block(&mut self, key: &K, data: &[u8]) -> Option<Arc<RwLock<Block>>> {
        // Check if the key is already exist
        if let Some(block) = self.map.get(key) {
            // access, then pin,return
            self.policy.access(key);
            let block = Arc::clone(block);
            block.write().pin();
            return Some(block);
        }

        let new_block = if let Some(new_block) = self.get_free_block(key) {
            Some(new_block)
        } else {
            let evict_key = self.policy.evict();
            if evict_key.is_none() {
                warn!("The cache is full of non-evictable blocks");
                return None;
            }
            let evict_key = evict_key?;
            // It must exist in the map
            let evict_block = self.map.remove(&evict_key).unwrap_or_else(|| {
                panic!("An evicted key must be in the map.");
            });
            assert!(evict_block.read().pin_count() == 0);
            assert!(!evict_block.read().dirty());
            self.map.insert(key.clone(), Arc::clone(&evict_block));
            self.policy.access(key);
            evict_block.write().pin();
            Some(evict_block)
        };
        let new_block = new_block?;
        {
            let mut block = new_block.write();
            block
                .get_mut(0..data.len())
                .unwrap_or_else(|| {
                    panic!("Input data is longer than a block.");
                })
                .copy_from_slice(data);
        }
        Some(new_block)
    }

    /// Decrement the pin count of the block associated with the given key.
    /// If the pin count reaches 0, set the block as evictable.
    #[inline]
    pub fn unpin(&mut self, key: &K) {
        // If a block is pinned, it must exist in the map.
        let block_ref = self.map.get(key).unwrap_or_else(|| {
            panic!("A pinned block must be in the map.");
        });
        let mut block = block_ref.write();
        block.unpin();
        if block.pin_count() == 0 {
            assert!(!block.dirty());
            self.policy.set_evictable(key, true);
        }
    }

    /// Fetches the block associated with the given key from the cache.
    #[inline]
    pub fn fetch(&self, key: &K) -> Option<Arc<RwLock<Block>>> {
        let block = self.map.get(key).cloned()?;
        self.policy.access(key);
        {
            let mut block = block.write();
            block.pin();
            assert!(block.pin_count() >= 1);
        }
        Some(block)
    }

    /// Returns the number of blocks in the cache.
    #[inline]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Evicts blocks from the cache according to the eviction policy.
    #[inline]
    pub fn evict(&mut self) {
        loop {
            let key = self.policy.evict();
            if let Some(key) = key {
                let block = self.map.remove(&key).unwrap_or_else(|| {
                    panic!("An evicted key must be in the map.");
                });
                assert!(block.read().pin_count() == 0);
                assert!(!block.read().dirty());
            } else {
                break;
            }
        }
    }

    /// Removes the block associated with the given key from the cache.
    /// Returns `true` if the block was successfully removed, `false` otherwise.
    #[inline]
    pub fn remove(&mut self, key: &K) -> bool {
        // Try to remove the key from the map and immediately handle the None case
        let block_ref: Arc<RwLock<Block>> = match self.map.remove(key) {
            Some(block) => block,
            None => return false,
        };

        // Get a write lock
        let mut block = block_ref.write();

        // Check if the removal condition is satisfied
        if block.pin_count() != 0 {
            // If not satisfied, reinsert block_ref into the map and return failure
            self.map.insert(key.clone(), Arc::clone(&block_ref));
            return false;
        }

        // Ensure that the block is not dirty
        assert!(!block.dirty());

        // Clean up the block and perform subsequent operations
        block.clear();
        self.free_list.push_back(Arc::clone(&block_ref));
        self.policy.remove(key);

        true // Successfully removed
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::super::policy::LruPolicy;
    use super::*;

    fn prepare_cache(capacity: usize) -> MemoryCache<usize, LruPolicy<usize>> {
        MemoryCache::new(capacity)
    }

    #[test]
    fn basic_test() {
        let content = vec![0_u8; BLOCK_SIZE];
        let cache_size = 10;
        let mut manager = prepare_cache(cache_size);
        {
            let block_0 = manager.new_block(&0, &content).unwrap();
            let mut block_0 = block_0.write();
            block_0.copy_from_slice(&content);
            block_0.set_dirty(true);
        }

        {
            // Check if block_0's content is correct.
            let block_0 = manager.fetch(&0).unwrap();
            {
                let block_0 = block_0.read();
                assert_eq!(block_0.as_ref(), &content);
                assert_eq!(block_0.pin_count(), 2);
            }
            manager.unpin(&0);
            assert_eq!(block_0.read().pin_count(), 1);
        }

        // Create blocks [1,2,3,4,5,6,7,8,9]
        for i in 1..cache_size {
            let block = manager.new_block(&i, &content).unwrap();
            let mut block = block.write();
            block.copy_from_slice(&content);
        }

        // Now the cache is full and all blocks are pinned.
        // We can't create a new block.
        {
            assert!(manager.new_block(&cache_size, &content).is_none());
        }

        {
            manager.unpin(&1);
        }

        // This would evict block 1 and create a new block.
        assert!(manager.new_block(&cache_size, &content).is_some());
        {
            // Try get block 1
            assert!(manager.fetch(&1).is_none());
        }
    }

    #[test]
    fn test_new_same_block() {
        let content = vec![0_u8; BLOCK_SIZE];
        let mut manager = prepare_cache(10);
        let block_1 = manager.new_block(&0, &content).unwrap();
        let block_2 = manager.new_block(&0, &content).unwrap();

        assert!(Arc::ptr_eq(&block_1, &block_2));
    }

    #[test]
    fn test_evict() {
        let content = vec![0_u8; BLOCK_SIZE];
        let mut manager = prepare_cache(10);

        let _block = manager.new_block(&1, &content).unwrap();
        let _block = manager.new_block(&2, &content).unwrap();

        manager.unpin(&2);

        assert_eq!(manager.len(), 2);
        manager.evict();
        assert_eq!(manager.len(), 1);
    }

    #[test]
    fn test_remove() {
        let content = vec![0_u8; BLOCK_SIZE];
        let mut manager = prepare_cache(10);

        let block_1 = manager.new_block(&1, &content).unwrap();
        manager.remove(&1); // This will NOT remove the pinned block
        let block_2 = manager.fetch(&1).unwrap();
        assert!(Arc::ptr_eq(&block_1, &block_2));

        manager.unpin(&1);
        manager.unpin(&1);
        manager.remove(&1);

        let block_3 = manager.fetch(&1);
        assert!(block_3.is_none());
    }
}
