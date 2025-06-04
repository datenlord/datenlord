//! Distributed cache module

/// Local in-memory cache
pub mod local_cache;

/// RPC module for cache
pub mod rpc;

/// Cluster manager
pub mod cluster;

/// Distribute cache and kvcache config
pub mod config;

/// Distribute cache and kvcache manager
pub mod manager;