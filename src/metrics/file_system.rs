//! Metrics for file system.

use once_cell::sync::Lazy;
use prometheus::{
    linear_buckets, register_histogram_vec_with_registry, HistogramTimer, HistogramVec, Registry,
};

use super::{LossyCast, DATENLORD_REGISTRY};

/// The file system related metrics.
pub static FILESYSTEM_METRICS: Lazy<FileSystemMetrics> =
    Lazy::new(|| FileSystemMetrics::new(&DATENLORD_REGISTRY));

/// The file system related metrics.
#[derive(Debug)]
pub struct FileSystemMetrics {
    /// The durations of fuse operations. With label: `[op]`.
    fuse_operation_duration_seconds: HistogramVec,
    /// The retry counts of KV transaction. With label: `[op]`
    kv_txn_retry_counts: HistogramVec,
}

impl FileSystemMetrics {
    /// Creates an instance of `FileSystemMetrics`, which will create two
    /// `HistogramVec`s and register them into the specified registry.
    ///
    /// # Panics
    /// This method panics if it called multiple times on the same registry.
    #[allow(clippy::expect_used)] // We can ensure that this method won't panic if we followed the hints above
    #[allow(clippy::ignored_unit_patterns)] // Raised by `register_histogram_vec_with_registry`
    fn new(registry: &Registry) -> Self {
        let fuse_operation_duration_seconds = register_histogram_vec_with_registry!(
            "fuse_operation_duration_seconds",
            "The durations of fuse operations",
            &["op"],
            linear_buckets(0.005, 0.005, 20).expect("`count` and `width` is not zero"),
            registry,
        )
        .expect("Metrics name must be unique");

        let kv_txn_retry_counts = register_histogram_vec_with_registry!(
            "kv_txn_retry_counts",
            "The retry counts of KV transaction",
            &["op"],
            linear_buckets(0.0, 3.0, 5).expect("`count` and `width` is not zero"),
            registry,
        )
        .expect("Metrics name must be unique");

        Self {
            fuse_operation_duration_seconds,
            kv_txn_retry_counts,
        }
    }

    /// Starts a timer to measure the FUSE operations' duration.
    pub fn start_storage_operation_timer(&self, op: &str) -> HistogramTimer {
        self.fuse_operation_duration_seconds
            .with_label_values(&[op])
            .start_timer()
    }

    /// Observes a value of the retry counts of KV transaction.
    pub fn observe_storage_operation_throughput<T: LossyCast<f64>>(&self, val: T, op: &str) {
        self.kv_txn_retry_counts
            .with_label_values(&[op])
            .observe(val.lossy_cast());
    }
}
