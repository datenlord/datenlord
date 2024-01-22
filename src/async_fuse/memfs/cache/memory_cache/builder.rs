//! The builder of `MemoryCache`.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use datenlord::config::SoftLimit;
use tokio::sync::mpsc;

use super::{write_back_task, MemoryCache};
use crate::async_fuse::memfs::cache::policy::EvictPolicy;
use crate::async_fuse::memfs::cache::{BlockCoordinate, Storage};

/// The default limitation of the command queue of write back task.
const DEFAULT_COMMAND_QUEUE_LIMIT: usize = 1000;
/// The default basic period of the write back task.
const DEFAULT_INTERVAL_IN_MILLISEC: u64 = 100;

/// A builder to configure and build a `MemoryCache`.
#[derive(Debug)]
pub struct MemoryCacheBuilder<P, S> {
    /// The `policy` used by the built `MemoryCache`
    policy: P,
    /// The `backend` of the built `MemoryCache`
    backend: S,
    /// The size of blocks
    block_size: usize,
    /// A flag that if the built `MemoryCache` runs in writing-through policy
    write_through: bool,
    /// The soft limit for the write back task. See [`SoftLimit`] for details.
    limit: SoftLimit,
    /// The interval of the write back task
    interval: Duration,
    /// The limitation of the message queue of the write back task
    command_queue_limit: usize,
}

impl<P, S> MemoryCacheBuilder<P, S>
where
    P: EvictPolicy<BlockCoordinate> + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    /// Create a builder.
    pub fn new(policy: P, backend: S, block_size: usize) -> Self {
        // The default soft limit is `3/5`.
        let limit = SoftLimit(
            3,
            NonZeroUsize::new(5).unwrap_or_else(|| unreachable!("5 is not zero.")),
        );
        Self {
            policy,
            backend,
            block_size,
            write_through: true,
            limit,
            interval: Duration::from_millis(DEFAULT_INTERVAL_IN_MILLISEC),
            command_queue_limit: DEFAULT_COMMAND_QUEUE_LIMIT,
        }
    }

    /// Set write policy. Write through is enabled by default.
    #[must_use]
    pub fn write_through(mut self, write_through: bool) -> Self {
        self.write_through = write_through;
        self
    }

    /// Set the soft limit for the write back task.
    ///
    /// See [`SoftLimit`] for details.
    #[must_use]
    pub fn limit(mut self, limit: SoftLimit) -> Self {
        self.limit = limit;
        self
    }

    /// Set the interval for the write back task
    #[must_use]
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Set the limitation of the message queue of the write back task.
    #[must_use]
    pub fn command_queue_limit(mut self, limit: usize) -> Self {
        self.command_queue_limit = limit;
        self
    }

    /// Builds a `MemoryCache`. Make sure that this method is called in `tokio`
    /// runtime.
    ///
    /// # Panic
    /// This method will panic if it's not called in a context of `tokio`
    /// runtime.
    pub fn build(self) -> Arc<MemoryCache<P, S>> {
        let MemoryCacheBuilder {
            policy,
            backend,
            block_size,
            write_through,
            limit,
            interval,
            command_queue_limit,
        } = self;

        let (sender, receiver) = mpsc::channel(command_queue_limit);

        let cache = Arc::new(MemoryCache::new(
            policy,
            backend,
            block_size,
            write_through,
            sender,
        ));

        let weak = Arc::downgrade(&cache);
        tokio::spawn(write_back_task::run_write_back_task(
            limit,
            interval,
            weak,
            receiver,
            command_queue_limit,
        ));

        cache
    }
}
