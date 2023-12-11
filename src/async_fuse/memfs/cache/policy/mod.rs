//! Evict policies for cache.

mod lru;

pub use lru::LruPolicy;

/// The evict policy trait.
/// A policy records and maintains cache keys.
pub trait EvictPolicy<K> {
    /// Put a key into the policy.
    ///
    /// If a key is to be evicted, returns it; otherwise, returns `None`.
    fn put(&self, key: K) -> Option<K>;

    /// Touch a key, as it's accessed.
    ///
    /// This may make no differences in some policy, such as FIFO.
    fn touch(&self, key: &K);

    /// Remove a key from the policy.
    fn remove(&self, key: &K);
}
