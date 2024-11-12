//! The LRU policy implementation.

use std::hash::Hash;

use hashlink::LinkedHashMap;
use parking_lot::Mutex;

/// The evict policy trait.
pub trait EvictPolicy<K> {
    /// Create a new policy with the given capacity.
    fn new(capacity: usize) -> Self;

    /// Create a new non-evictable entry if the key has not been seen before.
    fn access(&self, key: &K);

    /// Try to evict a evictable key by the policy.
    fn evict(&self) -> Option<K>;

    /// Toggle whether a key is evictable or non-evictable.
    fn set_evictable(&self, key: &K, evictable: bool);

    /// Decrement the size of the policy when a key is removed successfully.
    fn remove(&self, key: &K);

    /// Get the current size of the policy.
    fn size(&self) -> usize;
}

/// The evict policy based on LRU.
#[derive(Debug)]
pub struct LRUPolicy<K>
where
    K: Clone + Hash + Eq,
{
    /// The inner map
    inner: Mutex<LinkedHashMap<K, bool>>,
    /// The capacity of this policy
    capacity: usize,
}

impl<K: Clone + Hash + Eq> EvictPolicy<K> for LRUPolicy<K> {
    #[must_use]
    #[inline]
    fn new(capacity: usize) -> Self {
        LRUPolicy {
            inner: Mutex::new(LinkedHashMap::with_capacity(capacity)),
            capacity,
        }
    }

    #[inline]
    fn access(&self, key: &K) {
        {
            let mut inner = self.inner.lock();
            // Move the accessed key to the end to mark it as most recently used.
            // If the key does not exist, it inserts a new entry marked as non-evictable by
            // default (false).
            if !inner.contains_key(key) {
                // Before adding a new key, check if we reach the capacity.
                // Reach the capacity, panic
                // This should be handled by the caller, not the policy.
                assert!(!(inner.len() == self.capacity), "Capacity reached");
                inner.insert(key.clone(), false);
            } else {
                inner.to_back(key);
            }
        }
        self.set_evictable(key, false);
    }

    #[inline]
    fn evict(&self) -> Option<K> {
        let mut inner = self.inner.lock();
        let mut evict_key = None;
        // Find the first evictable key from the beginning.
        for (key, evictable) in inner.iter() {
            if *evictable {
                evict_key = Some(key.clone());
                break;
            }
        }
        let key = evict_key?;
        inner.remove(&key); // This automatically decrements the size.
        Some(key)
    }

    #[inline]
    fn set_evictable(&self, key: &K, evictable: bool) {
        let mut inner = self.inner.lock();
        if let Some(prev_evictable) = inner.get_mut(key) {
            *prev_evictable = evictable;
        } else {
            // This should not happen.
            panic!("LRUPolicy::set_evictable : Key not found");
        }
    }

    #[inline]
    fn remove(&self, key: &K) {
        let mut inner = self.inner.lock();
        let evictable = inner.get(key);
        if let Some(evictable) = evictable {
            if *evictable {
                inner.remove(key);
            }
        } else {
            // This should not happen.
            panic!("LRUPolicy::remove : Key not found");
        }
    }

    #[inline]
    fn size(&self) -> usize {
        self.inner.lock().len()
    }
}

#[cfg(test)]
#[allow(clippy::default_numeric_fallback)]
mod tests {
    use super::{EvictPolicy, LRUPolicy};

    /// Create a `LruPolicy` of `i32`, with keys `1 -> 2 -> 3`.
    fn create_lru() -> LRUPolicy<i32> {
        let cache = LRUPolicy::<i32>::new(3);

        cache.access(&1);
        cache.access(&2);
        cache.access(&3);

        cache.set_evictable(&1, true);
        cache.set_evictable(&2, true);
        cache.set_evictable(&3, true);
        assert!(cache.size() == 3);
        cache
    }

    #[test]
    #[should_panic(expected = "Capacity reached")]
    fn test_insert_full() {
        let cache = create_lru();
        cache.access(&4);
    }

    #[test]
    fn test_evict() {
        let cache = create_lru();

        let evicted = cache.evict();
        assert_eq!(evicted, Some(1));

        // 2 -> 3
        cache.access(&4);
    }

    #[test]
    fn test_touch() {
        let cache = create_lru();

        cache.access(&1);

        // 2 -> 3 -> 1
        let evicted = cache.evict();
        assert_eq!(evicted, Some(2));
    }

    #[test]
    fn test_remove() {
        let cache = create_lru();

        cache.remove(&1);
        assert_eq!(cache.size(), 2);
    }

    #[test]
    fn sample_evict_test() {
        let cache = LRUPolicy::<i32>::new(7);

        for i in 1..7 {
            cache.access(&i);
            cache.set_evictable(&i, true);
        }
        assert_eq!(cache.size(), 6);
        cache.access(&1);

        assert_eq!(cache.evict(), Some(2));
        assert_eq!(cache.evict(), Some(3));
        assert_eq!(cache.evict(), Some(4));

        // 5-> 6 -> 1
        assert_eq!(cache.size(), 3);

        // 5 -> 6 -> 1 -> 7
        cache.access(&7);

        // 5 -> 6 -> 7 -> 1
        cache.access(&1);

        // 5 -> 7 -> 1 -> 6
        cache.access(&6);

        cache.set_evictable(&5, true);
        cache.set_evictable(&6, true);
        cache.set_evictable(&1, true);

        // Since 7 is not evictable, it should be evicted.
        assert_eq!(cache.evict(), Some(5));
        assert_eq!(cache.evict(), Some(1));
        assert_eq!(cache.evict(), Some(6));
        assert_eq!(cache.evict(), None);

        // Set 7 as evictable
        cache.set_evictable(&7, true);
        assert_eq!(cache.evict(), Some(7));
    }
}
