//! KV related metrics.

use once_cell::sync::Lazy;
use prometheus::{
    linear_buckets, register_histogram_vec_with_registry, register_histogram_with_registry,
    Histogram, HistogramTimer, HistogramVec, Registry,
};

use super::DATENLORD_REGISTRY;

/// The KV related metrics.
pub static KV_METRICS: Lazy<KVMetrics> = Lazy::new(|| KVMetrics::new(&DATENLORD_REGISTRY));

/// The KV related metrics.
#[derive(Debug)]
pub struct KVMetrics {
    /// The latency of KV operations.
    kv_latency_seconds: HistogramVec,
    /// The latency of KV lock acquiring.
    kv_lock_latency_seconds: Histogram,
}

impl KVMetrics {
    /// Creates an instance of `KVMetrics`, which will create two
    /// `Histogram`s and register them into the specified registry.
    ///
    /// # Panics
    /// This method panics if it called multiple times on the same registry.
    #[allow(clippy::expect_used)] // We can ensure that this method won't panic if we followed the hints above
    #[allow(clippy::ignored_unit_patterns)] // Raised by `register_histogram_with_registry`
    fn new(registry: &Registry) -> Self {
        let kv_latency_seconds = register_histogram_vec_with_registry!(
            "kv_latency_seconds",
            "The latency of KV operations",
            &["type"],
            linear_buckets(0.005, 0.005, 20).expect("`count` and `width` is not zero"),
            registry,
        )
        .expect("Metrics name must be unique");

        let kv_lock_latency_seconds = register_histogram_with_registry!(
            "kv_lock_latency_seconds",
            "The latency of KV lock acquiring",
            linear_buckets(0.0, 3.0, 5).expect("`count` and `width` is not zero"),
            registry,
        )
        .expect("Metrics name must be unique");

        Self {
            kv_latency_seconds,
            kv_lock_latency_seconds,
        }
    }

    /// Starts a timer to measure KV operation latency.
    pub fn start_kv_operation_timer(&self, op_type: &str) -> HistogramTimer {
        self.kv_latency_seconds
            .with_label_values(&[op_type])
            .start_timer()
    }

    /// Starts a timer to measure KV lock acquiring latency.
    pub fn start_kv_lock_timer(&self) -> HistogramTimer {
        self.kv_lock_latency_seconds.start_timer()
    }
}
