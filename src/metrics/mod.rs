//! Datenlord metrics.

#![allow(dead_code)]

mod cache;
mod file_system;
mod kv;
mod lock;
mod server;
mod storage;
mod utils;

use once_cell::sync::Lazy;
use prometheus::Registry;

pub use self::cache::CACHE_METRICS;
pub use self::file_system::FILESYSTEM_METRICS;
pub use self::kv::KV_METRICS;
pub use self::server::start_metrics_server;
pub use self::utils::LossyCast;

/// The global metrics registry used by `DatenLord`.
pub static DATENLORD_REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);
