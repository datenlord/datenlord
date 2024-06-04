use std::{fmt::Debug, sync::Arc};

use datenlord::common::task_manager::{TaskName, TASK_MANAGER};
use parking_lot::RwLock;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::async_fuse::memfs::kv_engine::etcd_impl::EtcdKVEngine;
use crate::common::error::DatenLordResult;

use crate::async_fuse::memfs::kv_engine::{KVEngine, KVEngineType, KeyType};

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
    rpc_server: Arc<RPCServer>,
    /// Local cache manager, we will use it to manage the local cache, and export manaually data for the cache
    local_cache_manager: Arc<LocalCacheManager>,
    /// The distribute cache cluster
    distribute_cache_cluster: Arc<DistributeCacheCluster>,
}

impl DistributeCacheManager {
    /// Create a new cache proxy manager
    pub async fn new(
        config: Config,
        rpc_server: Arc<RPCServer>,
        local_cache_manager: Arc<LocalCacheManager>,
    ) -> DatenLordResult<Self> {
        let _ = rpc_server;
        let kv_engine: Arc<EtcdKVEngine> =
            Arc::new(KVEngineType::new(config.kv_addrs.clone()).await?);

        Ok(Self {
            config: Arc::new(config),
            rpc_server,
            local_cache_manager,
            distribute_cache_cluster: Arc::clone(DistributeCacheCluster::new(
                Arc::new(RwLock::new(Node::default())),
                kv_engine,
            )),
        })
    }

    /// Start the distribute cache manager
    pub async fn start(&self) -> DatenLordResult<()> {
        // Start the distribute cache cluster
        self.distribute_cache_cluster.start().await?;

        // Start to run rpc server
        TASK_MANAGER.spawn(TaskName::DistributeCache, |token| {
            // TODO: support cancel token
            self.rpc_server.listen(token);
        });

        // Start to watch cluster config
        TASK_MANAGER.spawn(TaskName::DistributeCache, |token| {
            self.watch_cluster_config(token);
        });

        Ok(())
    }

    /// Watch cluster config in etcd
    async fn watch_cluster_config(&self, token: CancellationToken) {
        info!("watch_master: will watch the master node and try to update master hashring");

        let cluster_config_key = &KeyType::CacheClusterConfig;
        let mut cluster_config_events = self.kv_engine.watch(cluster_config_key).await.unwrap();
        let cluster_config_events = Arc::get_mut(&mut cluster_config_events).unwrap();

        // Watch cluster config events
        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    info!("watch_master: watch master node task is cancelled");
                    return;
                }
                event = cluster_config_events.recv() => {
                    match event {
                        Some(event) => {
                            let key = event.0;
                            let value = event.1;
                            match value {
                                Some(_) => {
                                    // Update event
                                    debug!("Receive update cluster config event with key: {:?}", key);

                                    // In this step, we just need to clean current
                                    self.local_cache_manager.clean_cache().await.unwrap();
                                }
                                None => {
                                    // Delete event
                                    info!("delete cluster config event with key: {:?}", key);
                                    // We don't need to sepicify the cluster config delete events, because the each node will contains default cluster config
                                    return Ok(());
                                }
                            }
                        }
                        None => {
                            info!("watch_master: watch master node task is finished");
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}
