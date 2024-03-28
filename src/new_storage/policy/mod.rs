//! The evict policies.

mod lru;

pub use lru::LruPolicy;

/// The evict policy trait.
/// A policy records and maintains cache keys.
pub trait EvictPolicy<K> {
    /// Create a new policy with the given capacity.
    fn new(capacity: usize) -> Self;

    /// Access a key.
    /// Create a new non-evictable entry if the key has not been seen before.
    fn access(&self, key: &K);

    /// Try to evict a evictable key by the policy.
    fn evict(&self) -> Option<K>;

    /// Toggle whether a key is evictable or non-evictable.
    fn set_evictable(&self, key: &K, evictable: bool);

    /// Remove an evictable key from the policy.
    /// Decrement the size of the policy when a key is removed successfully.
    fn remove(&self, key: &K);

    /// Get the current size of the policy.
    fn size(&self) -> usize;
}
