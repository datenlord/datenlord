//! The LRU policy implementation.

use std::hash::Hash;

use hashlink::LruCache;
use parking_lot::Mutex;

use super::EvictPolicy;

/// The evict policy based on LRU.
#[derive(Debug)]
pub struct LruPolicy<K> {
    /// The inner hashlink
    inner: Mutex<LruCache<K, ()>>,
    /// The capacity of this policy
    capacity: usize,
}

impl<K: Hash + Eq> LruPolicy<K> {
    /// Create a new `LruPolicy` with `capacity`.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        LruPolicy {
            inner: Mutex::new(LruCache::new(capacity)),
            capacity,
        }
    }
}

impl<K: Hash + Eq> EvictPolicy<K> for LruPolicy<K> {
    fn put(&self, key: K) -> Option<K> {
        let mut lru = self.inner.lock();
        let len = lru.len();

        let evicted = if !lru.contains_key(&key) && len == self.capacity {
            lru.remove_lru()
        } else {
            None
        };

        lru.insert(key, ());
        evicted.map(|(k, ())| k)
    }

    fn touch(&self, key: &K) {
        let _: Option<&()> = self.inner.lock().get(key);
    }

    fn remove(&self, key: &K) {
        let _: Option<()> = self.inner.lock().remove(key);
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
        res = cache.put(1);
        assert_eq!(res, None);
        res = cache.put(2);
        assert_eq!(res, None);
        res = cache.put(3);
        assert_eq!(res, None);

        cache
    }

    #[test]
    fn test_evict() {
        let cache = create_lru();

        // 1 -> 2 -> 3
        let res = cache.put(4);
        assert_eq!(res, Some(1));
    }

    #[test]
    fn test_touch() {
        let cache = create_lru();

        cache.touch(&1);

        // 2 -> 3 -> 1
        let res = cache.put(4);
        assert_eq!(res, Some(2));
    }

    #[test]
    fn test_touch_by_put() {
        let cache = create_lru();

        let res = cache.put(1);
        assert_eq!(res, None);

        // 2 -> 3 -> 1
        let res = cache.put(4);
        assert_eq!(res, Some(2));
    }

    #[test]
    fn test_remove() {
        let cache = create_lru();

        cache.remove(&1);

        // 2 -> 3
        assert_eq!(cache.inner.lock().len(), 2);
        let res = cache.put(4);
        assert_eq!(res, None);
        let res = cache.put(5);
        assert_eq!(res, Some(2));
    }
}
