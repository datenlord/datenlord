//! The utilities of distribute cache cluster management

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use parking_lot::RwLock;
use tracing::{debug, error, info, warn};

use crate::async_fuse::memfs::kv_engine::{etcd_impl, KVEngine, SetOption};
use crate::async_fuse::memfs::kv_engine::{KVEngineType, KeyType, ValueType};
use crate::common::error::{Context, DatenLordError, DatenLordResult};

use super::node::{MasterNodeInfo, Node, NodeStatus};
use super::ring::Ring;

/// The timeout for the lock of updating the master node
const MASTER_LOCK_TIMEOUT_SEC: i64 = 30;
/// The timeout for the node register
const NODE_REGISTER_TIMEOUT_SEC: i64 = 10;

/// Node sessions
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct NodeSessions {
    /// Register session
    /// Used to store the register tasks,
    /// so we can cancel the tasks when the node is down or role changed
    register_session: Option<Arc<etcd_impl::Session>>,
    /// Master session
    /// Used to store the master tasks,
    /// Ditto
    master_session: Option<Arc<etcd_impl::Session>>,
}

impl NodeSessions {
    /// Create a new node sessions
    pub fn new() -> Self {
        Self {
            register_session: None,
            master_session: None,
        }
    }

    /// Get register session
    pub fn register_session(&self) -> Option<Arc<etcd_impl::Session>> {
        self.register_session.clone()
    }

    /// Get master session
    pub fn master_session(&self) -> Option<Arc<etcd_impl::Session>> {
        self.master_session.clone()
    }

    /// Update register session
    pub fn update_register_session(&mut self, session: Option<Arc<etcd_impl::Session>>) {
        self.register_session = session;
    }

    /// Update master session
    pub fn update_master_session(&mut self, session: Option<Arc<etcd_impl::Session>>) {
        self.master_session = session;
    }

    /// Delete register session
    pub fn delete_register_session(&mut self) {
        self.register_session = None;
    }

    /// Delete master session
    pub fn delete_master_session(&mut self) {
        self.master_session = None;
    }
}

/// ClusterManager
///
/// This struct is used to interact with etcd server and manager the cluster.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ClusterManager {
    /// inner struct
    inner: Arc<ClusterManagerInner>,
}

#[allow(dead_code)]
impl ClusterManager {
    /// Create new cluster manager
    pub fn new(kv_engine: Arc<KVEngineType>) -> Self {
        let inner = Arc::new(ClusterManagerInner::new(kv_engine));
        Self { inner }
    }

    /// Run the cluster manager as state machine
    ///
    /// We need to perpare current node info, current node list and generate hash ring info
    pub async fn run(
        &self,
        node: Arc<RwLock<Node>>,
        nodes: Arc<RwLock<Vec<Node>>>,
        ring: Arc<RwLock<Ring<Node>>>,
    ) -> DatenLordResult<()> {
        self.inner.run(node, nodes, ring).await
    }

    /// Register
    ///
    /// Register current node to etcd and keep alive
    pub async fn register(&self, node: Arc<RwLock<Node>>) -> DatenLordResult<()> {
        self.inner.register(node).await
    }

    /// Campaign
    ///
    /// Try to campaign master, will return status and master value
    pub async fn do_campaign(
        &self,
        node: Arc<RwLock<Node>>,
        ring_version: u64,
    ) -> DatenLordResult<(bool, String)> {
        self.inner.do_campaign(node, ring_version).await
    }

    /// Do slave tasks
    ///
    /// Slave node will watch the ring update and campaign master
    pub async fn do_slave_tasks(
        &self,
        node: Arc<RwLock<Node>>,
        ring: Arc<RwLock<Ring<Node>>>,
    ) -> DatenLordResult<()> {
        self.inner.do_slave_tasks(node, ring).await
    }

    /// Do master tasks
    ///
    /// Master node will watch the node list update, and update the ring
    pub async fn do_master_tasks(
        &self,
        node: Arc<RwLock<Node>>,
        nodes: Arc<RwLock<Vec<Node>>>,
        ring: Arc<RwLock<Ring<Node>>>,
    ) -> DatenLordResult<()> {
        self.inner.do_master_tasks(node, nodes, ring).await
    }
}

/// ClusterManagerInner
///
/// Inner struct of ClusterManager
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ClusterManagerInner {
    /// Etcd client
    kv_engine: Arc<KVEngineType>,
    /// Node sessions, try to keep the session alive
    node_sessions: Arc<RwLock<NodeSessions>>,
}

#[allow(dead_code)]
impl ClusterManagerInner {
    /// Create a new cluster manager
    pub fn new(kv_engine: Arc<KVEngineType>) -> Self {
        let node_sessions = Arc::new(RwLock::new(NodeSessions::new()));
        Self {
            kv_engine,
            node_sessions,
        }
    }

    /// Run the cluster manager as state machine
    ///
    /// We need to perpare current node info, current node list and generate hash ring info
    pub async fn run(
        &self,
        node: Arc<RwLock<Node>>,
        nodes: Arc<RwLock<Vec<Node>>>,
        ring: Arc<RwLock<Ring<Node>>>,
    ) -> DatenLordResult<()> {
        // 1. Init cluster manager
        info!("Cluster manager start to run");
        info!("Current node status: {:?}", node.clone().read().status());
        // Next step is to register the node
        node.write().set_status(NodeStatus::Registering);
        self.update_node_info(node.clone()).await?;
        loop {
            // 2. Register node to etcd
            info!("Current node status: {:?}", node.clone().read().status());
            while self.register(node.clone()).await.is_err() {
                error!("Failed to register node, retry in 5s");
                // Try to register node to etcd
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            // Update node status to Registering
            node.write().set_status(NodeStatus::Slave);
            self.update_node_info(node.clone()).await?;

            loop {
                // 3. Do campaign
                let (campaign_status, campaign_val) = self
                    .do_campaign(node.clone(), ring.read().version())
                    .await?;
                info!(
                    "Campaign status: {:?} val: {:?}",
                    campaign_status, campaign_val
                );

                // Check current hashring version and update local ring
                let current_master_node_info =
                    serde_json::from_str::<MasterNodeInfo>(&campaign_val)?;
                if current_master_node_info.hash_ring_version != ring.read().version() {
                    // Fetch the latest ring from etcd
                    self.load_ring(ring.clone()).await?;
                }

                // 4. Serve as normal status
                match node.clone().read().status() {
                    // Serve as slave node
                    NodeStatus::Slave => {
                        self.do_slave_tasks(node.clone(), ring.clone()).await?;
                    }
                    // Serve as master node
                    NodeStatus::Master => {
                        self.do_master_tasks(node.clone(), nodes.clone(), ring.clone())
                            .await?;
                    }
                    // Other parts can not
                    _ => {
                        // Clean up tasks
                        self.clean_sessions().await;
                        break;
                    }
                }
            }
        }
    }

    /// Update node info
    pub async fn update_node_info(&self, node: Arc<RwLock<Node>>) -> DatenLordResult<()> {
        let node = node.read();
        let key = &KeyType::CacheNode(node.ip().to_owned());
        while self
            .kv_engine
            .set(
                key,
                &ValueType::Json(serde_json::to_value(node.clone())?),
                None,
            )
            .await
            .is_err()
        {
            error!("Failed to update node info, retry in 5s");
            // Try to update node info
            tokio::time::sleep(Duration::from_secs(5)).await;
        }

        Ok(())
    }

    /// Register current node to etcd and keep alive
    pub async fn register(&self, node: Arc<RwLock<Node>>) -> DatenLordResult<()> {
        // Get current node info
        let node_dump = node.read().clone();
        info!("register: {} to etcd", node_dump.ip());

        // Try to get lease for current node
        let lease = self
            .kv_engine
            .create_lease(NODE_REGISTER_TIMEOUT_SEC)
            .await
            .with_context(|| "Failed to get lease for current node")?;

        // Try to register current node to etcd
        self.kv_engine
            .set(
                &KeyType::CacheNode(node_dump.ip().to_owned()),
                &ValueType::Json(serde_json::to_value(node_dump.clone())?),
                Some(SetOption {
                    // Set lease
                    lease: Some(lease),
                    prev_kv: false,
                }),
            )
            .await
            .with_context(|| format!("Failed to register node to etcd"))?;

        info!("register: {} to etcd success", node_dump.ip());

        // Set online status, default is slave
        node.write().set_status(NodeStatus::Slave);

        // Try keep alive current node to clsuter
        let register_session = self
            .kv_engine
            .create_session(lease, NODE_REGISTER_TIMEOUT_SEC as u64)
            .await?;
        self.node_sessions
            .write()
            .update_register_session(Some(register_session.clone()));

        Ok(())
    }

    /// Do campaign
    ///
    /// Try to campaign master, will return status and master value
    pub async fn do_campaign(
        &self,
        node: Arc<RwLock<Node>>,
        ring_version: u64,
    ) -> DatenLordResult<(bool, String)> {
        let client = self.kv_engine.clone();
        let lease = self
            .kv_engine
            .create_lease(MASTER_LOCK_TIMEOUT_SEC)
            .await
            .with_context(|| "Failed to get lease for current node")?;

        // Master key info
        let master_key = &KeyType::CacheMasterNode;
        // Create master instance
        let master_node_info = MasterNodeInfo::new(
            node.read().ip().to_owned(),
            node.read().port(),
            ring_version,
        );
        let master_node_info_json = serde_json::to_value(master_node_info)?.to_string();

        // Try to set campaign
        let txn = client.new_meta_txn().await;
        let (campaign_status, campaign_val) = txn
            .campaign(master_key, master_node_info_json, lease)
            .await?;
        // Check the leader key
        if campaign_status {
            // Serve as master node
            node.write().set_status(NodeStatus::Master);

            // Try keep alive current master to clsuter
            let master_session = self
                .kv_engine
                .create_session(lease, NODE_REGISTER_TIMEOUT_SEC as u64)
                .await?;
            self.node_sessions
                .write()
                .update_master_session(Some(master_session.clone()));
        } else {
            // Serve as slave node
            node.write().set_status(NodeStatus::Slave);
        }

        Ok((campaign_status, campaign_val))
    }

    /// Do slave tasks
    ///
    /// Slave node will watch the ring update and campaign master
    pub async fn do_slave_tasks(
        &self,
        node: Arc<RwLock<Node>>,
        ring: Arc<RwLock<Ring<Node>>>,
    ) -> DatenLordResult<()> {
        info!("do_slave_tasks: will watch the ring update and campaign master");

        // 1. Try to watch master and hashring
        // Wait for status update
        loop {
            // Check the node status
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(MASTER_LOCK_TIMEOUT_SEC as u64 / 3)) => {
                    // Check the node status
                    if node.read().status() != NodeStatus::Slave {
                        // If the node status is not slave, clean up slave tasks and return
                        // Clean up slave tasks
                        self.clean_sessions().await;

                        return Ok(());
                    }

                    // Block here, try to watch master
                    self.watch_master(ring.clone()).await?;
                }
            }
        }
    }

    /// Do master tasks
    ///
    /// 1. Master node will watch the node list update, and update the ring
    /// 2. Master will check self
    pub async fn do_master_tasks(
        &self,
        node: Arc<RwLock<Node>>,
        nodes: Arc<RwLock<Vec<Node>>>,
        ring: Arc<RwLock<Ring<Node>>>,
    ) -> DatenLordResult<()> {
        info!("do_master_tasks: will watch the node list update, and update the ring");

        // loop for node list update
        loop {
            // Keep alive master key
            // Check current status
            if node.read().status() != NodeStatus::Master {
                // If the node status is not master, clean up master tasks and return
                error!("Current node status is not master, return to endpoint");

                return Ok(());
            }

            // Do watch node list update task
            // This task will block until the master status changed
            self
                .watch_nodes(node.clone(), nodes.clone(), ring.clone())
                .await.with_context(|| "Failed to watch the node list update, and update the ring")?;
        }
    }

    /// Clean up the tasks
    pub async fn clean_sessions(&self) {
        // Clean up register tasks
        self.node_sessions.write().delete_register_session();

        // Clean up master tasks
        self.node_sessions.write().delete_master_session();
    }

    /// Try to check current session is valid
    pub async fn check_session_valid(&self, node: Arc<RwLock<Node>>) -> bool {
        // Check register session
        if let Some(register_session) = self.node_sessions.read().register_session() {
            if register_session.is_closed() {
                info!("Current session is valid");
                return false;
            }
        }

        // Check master session
        if node.read().status() == NodeStatus::Master {
            if let Some(master_session) = self.node_sessions.read().master_session() {
                if master_session.is_closed() {
                    info!("Current session is valid");
                    return false;
                }
            }
        }

        true
    }

    /// Master node will watch the node list update, and update the ring
    pub async fn watch_nodes(
        &self,
        node: Arc<RwLock<Node>>,
        nodes: Arc<RwLock<Vec<Node>>>,
        ring: Arc<RwLock<Ring<Node>>>,
    ) -> DatenLordResult<()> {
        info!("watch_nodes: will watch the node list update");

        // Get all nodes with prefix and init hash ring list
        // TODO: Block cluster and do not add any new node when watch_nodes is synced.
        let cluster_nodes = self.get_nodes().await?;
        let mut node_write = nodes.write();
        let mut ring_write = ring.write();
        info!("watch_nodes: init node list");
        for cluster_node in cluster_nodes {
            ring_write.add(&cluster_node.clone(), true);
            node_write.push(cluster_node.clone());
        }
        drop(node_write);
        drop(ring_write);

        info!("watch_nodes: init node list success");

        self.update_cluster_topo(node.clone(), ring.clone()).await?;

        // Get all nodes with prefix
        let key = &KeyType::CacheNode("".to_string());
        let mut nodes_watch_stream = self.kv_engine.watch(key).await.unwrap();

        info!(
            "watch_nodes: will watch the node list update with key: {:?}",
            key
        );

        // Wait for node list update
        loop {
            match nodes_watch_stream.next().await {
                Some(Ok(event)) => {
                    let key = event.0;
                    let value = event.1;
                    match value {
                        Some(item_value) => {
                            // Update event
                            info!("Receive update node list event with key: {:?}", key);

                            // deserialize node list to Vec<Node>
                            let updated_node = match item_value {
                                ValueType::Json(nodes_json) => {
                                    let updated_node: Node =
                                        serde_json::from_value(nodes_json.to_owned()).unwrap();
                                    Some(updated_node)
                                }
                                _ => None,
                            };

                            // Update current node list info
                            if let Some(updated_node) = updated_node {
                                // Append new node to the node list
                                nodes.write().push(updated_node.clone());

                                // Update ring
                                ring.write().add(&updated_node, true);

                                // Update cluster topo
                                self.update_cluster_topo(node.clone(), ring.clone()).await?;
                            } else {
                                error!("Failed to deserialize node list");
                            }
                        }
                        None => {
                            // Delete event
                            info!("delete node list event with key: {:?}", key);
                            // Get ip from key
                            let key = key.to_owned();
                            let key = key.replace("CacheNode", "");
                            info!("delete node list event with key: {:?}", key);

                            // Try to remove the node from the node list and updated the ring
                            if let Some(removed_node) =
                                nodes.read().iter().find(|node| node.ip() == key)
                            {
                                // Try to remove the node from the node list and get the node info
                                // And remove node from the node list
                                let _ = nodes.write().retain(|n| n.ip() != removed_node.ip());

                                info!("Remove node from node list: {:?}", removed_node);

                                // Update ring
                                let _ = ring.write().remove(removed_node.to_owned(), true);

                                // Update cluster topo
                                self.update_cluster_topo(node.clone(), ring.clone()).await?;
                            }
                        }
                    }
                }
                None => {
                    info!("111");
                    // No event
                    continue;
                }
                Some(Err(e)) => {
                    // Raise error and return to upper level, try to rewatch master again
                    error!("Failed to watch node list key: {:?}", e);
                    return Err(DatenLordError::CacheClusterErr {
                        context: vec![format!("Failed to watch node list key")],
                    });
                }
            }
        }
    }

    /// Update cluster topo
    ///
    /// Try to update cluster by current master node
    async fn update_cluster_topo(
        &self,
        node: Arc<RwLock<Node>>,
        ring: Arc<RwLock<Ring<Node>>>,
    ) -> DatenLordResult<()> {
        // Update to etcd
        let master_key = &KeyType::CacheMasterNode;

        // Create master instance
        let master_node_info = MasterNodeInfo::new(
            node.read().ip().to_owned(),
            node.read().port(),
            ring.read().version(),
        );
        let master_node_info_json = serde_json::to_value(master_node_info)?;
        let master_value = &ValueType::Json(master_node_info_json);

        // Check both session valid
        if !self.check_session_valid(node.clone()).await {
            error!("Current session is invalid, return to endpoint");
            return Err(DatenLordError::CacheClusterErr {
                context: vec![format!("Current session is invalid")],
            });
        }

        let master_sessions = self.node_sessions.read().master_session();
        match master_sessions {
            Some(session) => {
                // Update master data
                // Try to update master hashring version
                let lease = session.lease_id();
                self.kv_engine
                    .set(
                        master_key,
                        master_value,
                        Some(SetOption {
                            // Set lease
                            lease: Some(lease),
                            prev_kv: false,
                        }),
                    )
                    .await?;

                // Update hashring data
                let _ = self.save_ring(ring.clone()).await?;
            }
            None => {
                error!("Failed to renew lease for master node");
                // Change to slave node
                node.write().set_status(NodeStatus::Slave);
                return Err(DatenLordError::CacheClusterErr {
                    context: vec![format!("Failed to renew lease for master node")],
                });
            }
        };

        Ok(())
    }

    /// Try to watch the master node
    /// If the master node is down, the slave node will try to get the master lock
    /// Then current node will become the master node
    pub async fn watch_master(&self, ring: Arc<RwLock<Ring<Node>>>) -> DatenLordResult<()> {
        info!("watch_master: will watch the master node and try to update master hashring");

        // Watch with prefix
        let master_key = &KeyType::CacheMasterNode;
        let mut master_watch_stream = self.kv_engine.watch(master_key).await.unwrap();

        // Watch master key events
        // If master has changed, try to update the value
        // 1. If master ip changed, try to change the master node
        // TODO: master key is keeped by lease, so the master node will be auto deleted
        // if master node is down.
        // If the version is changed, try to update the ring
        // In current case, we just need to detect master key change and update hashring.
        // 2. If the master key is auto deleted, exit and change current status to slave.
        loop {
            match master_watch_stream.next().await {
                Some(Ok(event)) => {
                    let key = event.0;
                    let value = event.1;
                    match value {
                        Some(_) => {
                            // Update event
                            debug!("Receive update ring event with key: {:?}", key);

                            // In this step, we just need to update the ring
                            self.load_ring(ring.clone()).await?;
                        }
                        None => {
                            // Delete event
                            info!("delete master event with key: {:?}", key);
                            // Master has down, try to campaign master
                            // Return to main loop
                            return Ok(());
                        }
                    }
                }
                None => {
                    // No event
                    continue;
                }
                Some(Err(e)) => {
                    // Raise error and return to upper level, try to rewatch master again
                    error!("Failed to watch master key: {:?}", e);
                    return Err(DatenLordError::CacheClusterErr {
                        context: vec![format!("Failed to watch master key")],
                    });
                }
            }
        }
    }

    /// Save ring to etcd
    pub async fn save_ring(&self, ring: Arc<RwLock<Ring<Node>>>) -> DatenLordResult<()> {
        // Only master node can save ring to etcd
        // So we do not need to lock the ring
        let ring_key = &KeyType::CacheRing;
        let current_json_value;
        if let Ok(json_value) = serde_json::to_value(ring.read().clone()) {
            current_json_value = json_value;
        } else {
            error!("Failed to serialize ring");
            return Ok(());
        }
        let ring_value = &ValueType::Json(current_json_value);

        let key = &KeyType::CacheRing;
        debug!("Save ring to etcd: {}", key);
        self.kv_engine.set(ring_key, ring_value, None).await?;

        Ok(())
    }

    /// Load ring from etcd
    pub async fn load_ring(&self, ring: Arc<RwLock<Ring<Node>>>) -> DatenLordResult<()> {
        let key = &KeyType::CacheRing;
        debug!("Try to load ring from etcd: {}", key);

        // Get ring from etcd
        match self.kv_engine.get(key).await? {
            Some(ValueType::Json(ring_json)) => {
                let new_ring: Ring<Node> = serde_json::from_value(ring_json)?;
                ring.write().update(&new_ring);

                info!("Load ring from etcd success");
                Ok(())
            }
            _ => {
                error!("Failed to deserialize ring");
                Err(DatenLordError::CacheClusterErr {
                    context: vec![format!("Failed to deserialize ring")],
                })
            }
        }
    }

    /// Get node lists
    pub async fn get_nodes(&self) -> DatenLordResult<Vec<Node>> {
        let key = &KeyType::CacheNode("".to_string());
        debug!("Get node list from etcd: {}", key);

        // Get node list from etcd
        let nodes = self.kv_engine.range(key).await?;
        let mut node_list = Vec::new();
        for node in nodes {
            match node {
                ValueType::Json(node_json) => {
                    let node: Node = serde_json::from_value(node_json)?;
                    node_list.push(node);
                }
                _ => {
                    warn!("Failed to deserialize node");
                }
            }
        }

        Ok(node_list)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use parking_lot::RwLock;
    use tracing::info;
    use tracing_subscriber;

    use crate::{
        async_fuse::memfs::kv_engine::{DeleteOption, KVEngine, KVEngineType, KeyType},
        storage::cache_proxy::{
            cluster_manager::{ClusterManager, NODE_REGISTER_TIMEOUT_SEC},
            node::{Node, NodeStatus},
            ring::Ring,
        },
    };

    const ETCD_ADDRESS: &str = "127.0.0.1:2379";

    /// Helper function to create a new node with a given IP address
    fn create_node(ip: &str) -> Arc<RwLock<Node>> {
        let mut node = Node::default();
        node.set_ip(ip.to_string());

        let node = Arc::new(RwLock::new(node));
        node
    }

    async fn clean_up_etcd() {
        // Clean up all `CacheNode` prefix keys in etcd
        let _ = KVEngineType::new(vec![ETCD_ADDRESS.to_string()])
            .await
            .unwrap()
            .delete(
                &KeyType::CacheNode("".to_string()),
                Some(DeleteOption {
                    prev_kv: false,
                    range_end: Some(vec![0xff]),
                }),
            )
            .await;

        // Clean up all `CacheMasterNode` keys in etcd
        let _ = KVEngineType::new(vec![ETCD_ADDRESS.to_string()])
            .await
            .unwrap()
            .delete(
                &KeyType::CacheMasterNode,
                Some(DeleteOption {
                    prev_kv: false,
                    range_end: Some(vec![0xff]),
                }),
            )
            .await;
    }

    #[tokio::test]
    async fn test_single_master_election() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .init();
        let client = Arc::new(
            KVEngineType::new(vec![ETCD_ADDRESS.to_string()])
                .await
                .unwrap(),
        );

        // Clean up etcd
        clean_up_etcd().await;

        let ring_version: u64 = 1;
        let test_master_node = create_node("192.168.1.2");
        let master_cluster_manager = ClusterManager::new(client.clone());
        let test_slave_node_1 = create_node("192.168.1.3");
        let slave_1_cluster_manager = ClusterManager::new(client.clone());
        let test_slave_node_2 = create_node("192.168.1.4");
        let slave_2_cluster_manager = ClusterManager::new(client.clone());

        info!("test_single_master_election: start to test single master election");

        let (master_res, slave_1_res, slave_2_res) = tokio::join!(
            async {
                // Register node
                let _ = master_cluster_manager
                    .register(test_master_node.clone())
                    .await;
                // campaign
                master_cluster_manager
                    .do_campaign(test_master_node.clone(), ring_version)
                    .await
            },
            async {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                let _ = slave_1_cluster_manager
                    .register(test_slave_node_1.clone())
                    .await;
                // campaign
                slave_1_cluster_manager
                    .do_campaign(test_slave_node_1.clone(), ring_version)
                    .await
            },
            async {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                let _ = slave_2_cluster_manager
                    .register(test_slave_node_2.clone())
                    .await;
                // campaign
                slave_2_cluster_manager
                    .do_campaign(test_slave_node_2.clone(), ring_version)
                    .await
            }
        );

        // Check the result
        assert!(master_res.is_ok());
        assert!(slave_1_res.is_ok());
        assert!(slave_2_res.is_ok());

        // Check node role
        assert_eq!(test_master_node.read().status(), NodeStatus::Master);
        assert_eq!(test_slave_node_1.read().status(), NodeStatus::Slave);
        assert_eq!(test_slave_node_2.read().status(), NodeStatus::Slave);
    }

    #[tokio::test]
    async fn test_add_new_node() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .init();
        let client = Arc::new(
            KVEngineType::new(vec![ETCD_ADDRESS.to_string()])
                .await
                .unwrap(),
        );

        // Clean up etcd
        clean_up_etcd().await;

        let ring_version: u64 = 1;
        let test_master_node = create_node("192.168.2.2");
        let master_cluster_manager = ClusterManager::new(client.clone());
        let test_slave_node_1 = create_node("192.168.2.3");
        let slave_1_cluster_manager = ClusterManager::new(client.clone());
        let test_slave_node_2 = create_node("192.168.2.4");
        let slave_2_cluster_manager = ClusterManager::new(client.clone());

        // Join master and slave1
        let (master_res, slave_1_res) = tokio::join!(
            async {
                // Register node
                let _ = master_cluster_manager
                    .register(test_master_node.clone())
                    .await;
                // campaign
                master_cluster_manager
                    .do_campaign(test_master_node.clone(), ring_version)
                    .await
            },
            async {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                let _ = slave_1_cluster_manager
                    .register(test_slave_node_1.clone())
                    .await;
                // campaign
                slave_1_cluster_manager
                    .do_campaign(test_slave_node_1.clone(), ring_version)
                    .await
            }
        );

        // Wait for the election to finish
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        // Check the result
        assert!(master_res.is_ok());
        assert!(slave_1_res.is_ok());

        // Test add new node
        let _ = slave_2_cluster_manager
            .register(test_slave_node_2.clone())
            .await
            .unwrap();
        // campaign
        let _ = slave_2_cluster_manager
            .do_campaign(test_slave_node_2.clone(), ring_version)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        // Check node role
        assert_eq!(test_master_node.read().status(), NodeStatus::Master);
        assert_eq!(test_slave_node_1.read().status(), NodeStatus::Slave);
        assert_eq!(test_slave_node_2.read().status(), NodeStatus::Slave);
    }

    #[tokio::test]
    async fn test_remove_slave_node() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .init();
        let client = Arc::new(
            KVEngineType::new(vec![ETCD_ADDRESS.to_string()])
                .await
                .unwrap(),
        );

        // Clean up etcd
        clean_up_etcd().await;

        // Setup initial state with multiple nodes
        let test_master_node: Arc<parking_lot::lock_api::RwLock<parking_lot::RawRwLock, Node>> = create_node("192.168.3.2");
        let test_master_ring: Arc<RwLock<Ring<Node>>> = Arc::new(RwLock::new(Ring::default()));
        let test_slave_node: Arc<parking_lot::lock_api::RwLock<parking_lot::RawRwLock, Node>> = create_node("192.168.3.3");
        // let test_slave_nodes: Arc<RwLock<Vec<Node>>> = Arc::new(RwLock::new(vec![]));
        // let test_slave_ring: Arc<RwLock<Ring<Node>>> = Arc::new(RwLock::new(Ring::default()));
        // let test_master_nodes: Arc<RwLock<Vec<Node>>> = Arc::new(RwLock::new(vec![
        //     test_master_node.read().clone(),
        //     test_slave_node.read().clone(),
        // ]));
        let test_master_nodes: Arc<RwLock<Vec<Node>>> = Arc::new(RwLock::new(vec![]));

        let master_client = client.clone();
        let test_master_node_clone = test_master_node.clone();
        let test_master_nodes_clone = test_master_nodes.clone();
        let master_handle = tokio::task::spawn_blocking(move || {
            tokio::runtime::Handle::current().block_on(async move {
                let master_cluster_manager = Arc::new(ClusterManager::new(master_client));
                // Register node
                let _ = master_cluster_manager
                .register(test_master_node_clone.clone())
                .await;
                // Campaign
                let _ = master_cluster_manager
                    .do_campaign(test_master_node_clone.clone(), 1)
                    .await
                    .unwrap();
                // Run master
                let res = master_cluster_manager.do_master_tasks(
                    test_master_node_clone.clone(),
                    test_master_nodes_clone.clone(),
                    test_master_ring.clone(),
                ).await;
                info!("master_handle: {:?}", res);
            });
        });

        // Slave online
        let slave_client = client.clone();
        let test_slave_node_clone = test_slave_node.clone();
        let slave_cluster_manager = Arc::new(ClusterManager::new(slave_client));
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        // Register
        let _ = slave_cluster_manager
            .register(test_slave_node_clone.clone())
            .await;
        // Campaign
        let _ = slave_cluster_manager
            .do_campaign(test_slave_node_clone.clone(), 1)
            .await
            .unwrap();

        // Wait for the election to finish
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        assert_eq!(test_master_node.read().status(), NodeStatus::Master);
        assert_eq!(test_slave_node.read().status(), NodeStatus::Slave);

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        info!("test_remove_slave_node: update node list");
        assert_eq!(test_master_node.read().status(), NodeStatus::Master);
        assert_eq!(test_master_nodes.read().len(), 2);

        // Cancel the slave task
        drop(slave_cluster_manager);
        // Wait for slave key is deleted
        tokio::time::sleep(std::time::Duration::from_secs(2 * NODE_REGISTER_TIMEOUT_SEC as u64)).await;
        info!("test_remove_slave_node: start to test remove slave node");
        info!("Get all nodes: {:?}", test_master_nodes.read());

        assert_eq!(test_master_node.read().status(), NodeStatus::Master);
        assert_eq!(test_master_nodes.read().len(), 1);

        master_handle.abort();
    }

    #[tokio::test]
    async fn test_remove_master_node() {}
}
