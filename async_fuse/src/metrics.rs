use lazy_static::lazy_static;
use prometheus::Counter;
use prometheus::{opts, register_counter};

lazy_static! {
    /// Datenlord cache hit metrics
    pub static ref CACHE_HITS: Counter = register_counter!(opts!(
        "datenlord_cache_hits",
        "Approximate number of Cache hits since last server start"
    ))
    .unwrap();
    /// Datenlord cache miss metrics
    pub static ref CACHE_MISSES: Counter = register_counter!(opts!(
        "datenlord_cache_misses",
        "Approximate number of Cache misses since last server start"
    ))
    .unwrap();
}
