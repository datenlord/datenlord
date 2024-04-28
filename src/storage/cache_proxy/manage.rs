use std::{fmt::Debug, sync::Arc};

use parking_lot::RwLock;

use crate::async_fuse::memfs::kv_engine::etcd_impl::EtcdKVEngine;
use crate::common::error::DatenLordResult;

use crate::async_fuse::memfs::kv_engine::{KVEngine, KVEngineType};

use super::cluster::DistributeCacheCluster;
use super::{config::Config, node::Node};

/// Cache proxy manager
///
/// This manager is used to manage the cache proxy topology.
#[derive(Debug)]
#[allow(dead_code)]
pub struct DistributeCacheManager {
    /// config
    config: Arc<Config>,
    /// RPC Server
    // rpc_server: RPCServer,
    // local_cache_manager: LocalCacheManager,
    /// The distribute cache cluster
    distribute_cache_cluster: DistributeCacheCluster,
}

impl DistributeCacheManager {
    /// Create a new cache proxy manager
    pub async fn new(config: Config) -> DatenLordResult<Self> {
        let kv_engine: Arc<EtcdKVEngine> =
            Arc::new(KVEngineType::new(config.kv_addrs.clone()).await?);

        Ok(Self {
            config: Arc::new(config),
            distribute_cache_cluster: DistributeCacheCluster::new(
                Arc::new(RwLock::new(Node::default())),
                kv_engine,
            ),
        })
    }
}
