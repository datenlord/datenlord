//! Evict policies for cache.

mod lru;

pub use lru::LruPolicy;

/// The evict policy trait.
/// A policy records and maintains cache keys.
pub trait EvictPolicy<K> {
    /// Touch a key, as it's accessed.
    ///
    /// This may make no differences in some policy, such as FIFO.
    fn touch(&self, key: &K);

    /// Evict a key from the policy manually.
    ///
    /// If the policy is not full, returns None.
    fn evict(&self) -> Option<K>;

    /// Try to put a key into the policy.
    ///
    /// Returns if the putting is successful.
    fn try_put(&self, key: K) -> bool;

    /// Returns the capacity of this policy.
    fn capacity(&self) -> usize;

    /// Returns the current size of this policy.
    fn size(&self) -> usize;
}
