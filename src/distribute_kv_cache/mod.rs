//! Distributed cache module

/// Local in-memory cache
pub mod server_cache;

/// RPC module for cache
pub mod rpc;

/// Cluster manager
pub mod cluster;

/// Distribute cache and kvcache config
pub mod config;

/// Distribute cache and kvcache manager
pub mod manager;

/// Distribute kv cache client
pub mod kvclient;

/// Test module
pub mod tests;