//! The LRU policy implementation.

use std::hash::Hash;

use hashlink::LinkedHashSet;
use parking_lot::Mutex;

use super::EvictPolicy;

/// The evict policy based on LRU.
#[derive(Debug)]
pub struct LruPolicy<K> {
    /// The inner hashlink
    inner: Mutex<LinkedHashSet<K>>,
    /// The capacity of this policy
    capacity: usize,
}

impl<K: Hash + Eq> LruPolicy<K> {
    /// Create a new `LruPolicy` with `capacity`.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        LruPolicy {
            inner: Mutex::new(LinkedHashSet::with_capacity(capacity)),
            capacity,
        }
    }
}

impl<K: Clone + Hash + Eq> EvictPolicy<K> for LruPolicy<K> {
    fn touch(&self, key: &K) {
        self.inner.lock().to_back(key);
    }

    fn evict(&self) -> Option<K> {
        let mut lru = self.inner.lock();

        if lru.len() == self.capacity {
            lru.pop_front()
        } else {
            None
        }
    }

    fn try_put(&self, key: K) -> bool {
        let mut lru = self.inner.lock();
        let len = lru.len();

        if !lru.contains(&key) && len == self.capacity {
            false
        } else {
            lru.insert(key);
            true
        }
    }
}

#[cfg(test)]
#[allow(clippy::default_numeric_fallback)]
mod tests {
    use super::{EvictPolicy, LruPolicy};

    /// Create a `LruPolicy` of `i32`, with keys `1 -> 2 -> 3`.
    fn create_lru() -> LruPolicy<i32> {
        let cache = LruPolicy::<i32>::new(3);

        let mut res;
        res = cache.try_put(1);
        assert!(res);
        res = cache.try_put(2);
        assert!(res);
        res = cache.try_put(3);
        assert!(res);

        cache
    }

    #[test]
    fn test_evict() {
        let cache = create_lru();

        // 1 -> 2 -> 3
        let res = cache.try_put(4);
        assert!(!res);

        let evicted = cache.evict();
        assert_eq!(evicted, Some(1));

        // Policy is not full now.
        let evicted = cache.evict();
        assert_eq!(evicted, None);

        // 2 -> 3 -> 4
        let res = cache.try_put(4);
        assert!(res);
    }

    #[test]
    fn test_touch() {
        let cache = create_lru();

        cache.touch(&1);

        // 2 -> 3 -> 1
        let evicted = cache.evict();
        assert_eq!(evicted, Some(2));
    }

    #[test]
    fn test_touch_by_put() {
        let cache = create_lru();

        let res = cache.try_put(1);
        assert!(res);

        // 2 -> 3 -> 1
        let evicted = cache.evict();
        assert_eq!(evicted, Some(2));
    }
}
