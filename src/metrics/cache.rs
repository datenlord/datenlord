//! Metrics for file caches.

use once_cell::sync::Lazy;
use prometheus::{register_counter_vec_with_registry, CounterVec, Registry};

use super::DATENLORD_REGISTRY;

/// The file caches related metrics.
pub static CACHE_METRICS: Lazy<CacheMetrics> = Lazy::new(|| CacheMetrics::new(&DATENLORD_REGISTRY));

/// The file caches related metrics.
#[derive(Debug)]
pub struct CacheMetrics {
    /// The counters of total of cache hits. With label: `[name]`.
    cache_hit_count: CounterVec,
    /// The counters of total of cache misses. With label: `[name]`
    cache_miss_count: CounterVec,
}

impl CacheMetrics {
    /// Creates an instance of `CacheMetrics`, which will create two
    /// `CounterVec`s and register them into the specified registry.
    ///
    /// # Panics
    /// This method panics if it called multiple times on the same registry.
    #[allow(clippy::expect_used)] // We can ensure that this method won't panic if we followed the hints above
    #[allow(clippy::ignored_unit_patterns)] // Raised by `register_counter_vec_with_registry`
    fn new(registry: &Registry) -> Self {
        let cache_hit_count = register_counter_vec_with_registry!(
            "cache_hit_count",
            "The total of cache hits",
            &["name"],
            registry,
        )
        .expect("Metrics name must be unique.");

        let cache_miss_count = register_counter_vec_with_registry!(
            "cache_miss_count",
            "The total of cache misses",
            &["name"],
            registry,
        )
        .expect("Metrics name must be unique.");

        Self {
            cache_hit_count,
            cache_miss_count,
        }
    }

    /// Increase the hit count with `name`.
    pub fn cache_hit_count_inc(&self, name: &str) {
        self.cache_hit_count.with_label_values(&[name]).inc();
    }

    /// Increase the miss count with `name`.
    pub fn cache_miss_count_inc(&self, name: &str) {
        self.cache_miss_count.with_label_values(&[name]).inc();
    }
}
