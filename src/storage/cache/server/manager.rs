use std::{fmt::Debug, sync::Arc};

use parking_lot::RwLock;

use crate::async_fuse::memfs::kv_engine::etcd_impl::EtcdKVEngine;
use crate::common::error::DatenLordResult;

use crate::async_fuse::memfs::kv_engine::{KVEngine, KVEngineType};

use super::cluster::DistributeCacheCluster;
use super::{config::Config, node::Node};

/// Cache server manager
///
/// This manager is used to manage the cache proxy topology.
#[derive(Debug)]
#[allow(dead_code)]
pub struct DistributeCacheManager {
    /// Local config
    config: Arc<Config>,
    /// Cluster config
    cluster_config: Arc<Mutex<ClusterConfig>>,
    /// RPC Server
    rpc_server: RPCServer,
    /// Local cache manager
    local_cache_manager: LocalCacheManager,
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

    /// Watch cluster config in etcd
    pub async fn watch_cluster_config(&self) {
        // TODO: just for test.
        let client_config = ClientConfig::new(vec!["http://127.0.0.1:2379"]);
        let client = Client::connect(client_config).await.unwrap();

        let key_to_watch = "/cluster/config";
        let mut watch_request = WatchRequest::key(key_to_watch.to_string());

        let mut watch_stream = client.watch(watch_request).await.unwrap();

        while let Some(event) = watch_stream.next().await {
            match event {
                Ok(watch_response) => {
                    for event in watch_response.events() {
                        match event.event_type() {
                            kvengine::EventType::Put => {
                                println!("Updated config: {:?}", event.kv().unwrap().value_str().unwrap());
                                self.update_cluster_config(event.kv().unwrap().value_str().unwrap());
                            },
                            kvengine::EventType::Delete => {
                                println!("Config deleted");
                            },
                            _ => {}
                        }
                    }
                }
                Err(e) => println!("Error watching etcd: {:?}", e),
            }
        }

        client.shutdown().await.unwrap();
    }
}

/// Cluster config
pub struct ClusterConfig {
    /// Global cluster block size
    block_size: u64,
}

impl ClusterConfig {
    /// Create a new cluster config
    pub fn new(block_size: u64) -> Self {
        Self { block_size }
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            block_size: 4 * 1024 * 1024,
        }
    }
}
