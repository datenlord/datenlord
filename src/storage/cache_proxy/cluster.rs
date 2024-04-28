use std::{fmt::Debug, sync::Arc};

use parking_lot::RwLock;

use crate::async_fuse::memfs::kv_engine::KVEngineType;
use crate::common::error::DatenLordResult;

use super::{cluster_manager::ClusterManager, node::Node, ring::Ring};

/// DistributeCacheCluster
///
/// This struct is used to manage the inner topology cache.
#[allow(dead_code)]
pub struct DistributeCacheCluster {
    /// The cache proxy topology
    node: Arc<RwLock<Node>>,
    /// Proxy topology for hashring
    hashring: Arc<RwLock<Ring<Node>>>,
    /// Node list
    node_list: Arc<RwLock<Vec<Node>>>,
    /// Cluster informer
    cluster_manager: Arc<ClusterManager>,
}

impl DistributeCacheCluster {
    /// Create a new proxy topology
    pub fn new(node: Arc<RwLock<Node>>, kv_engine: Arc<KVEngineType>) -> Self {
        let node_list = Arc::new(RwLock::new(Vec::new()));
        let hashring = Arc::new(RwLock::new(Ring::default()));
        let cluster_manager = Arc::new(ClusterManager::new(kv_engine));

        Self {
            node,
            hashring,
            node_list,
            cluster_manager,
        }
    }

    /// Start the cluster manager, and update cluster info
    pub async fn run(&self) -> DatenLordResult<()> {
        loop {
            self.cluster_manager
                .run(
                    self.node.clone(),
                    self.node_list.clone(),
                    self.hashring.clone(),
                )
                .await?;
        }
    }
}

impl Debug for DistributeCacheCluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributeCacheCluster")
            .field("node", &self.node)
            .field("hashring", &self.hashring)
            .field("node_list", &self.node_list)
            .field("cluster_manager", &self.cluster_manager)
            .finish()
    }
}
