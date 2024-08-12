//! Distributed cache module

/// RPC module for cache
pub mod rpc;

/// Local in-memory cache
pub mod local_cache;

/// Cluster manager
pub mod cluster;

/// Distribute cache manager
pub mod manager;

/// Distribute cache config
pub mod config;

/// Distribute cache client
pub mod client;

/// Distribute cache tests
pub mod tests;