//! The utilities of distribute cache cluster management

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use arc_swap::{ArcSwap, ArcSwapOption};
use clippy_utilities::OverflowArithmetic;
use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::async_fuse::memfs::kv_engine::etcd_impl::Session;
use crate::async_fuse::memfs::kv_engine::{KVEngine, SetOption};
use crate::async_fuse::memfs::kv_engine::{KVEngineType, KeyType, ValueType};
use crate::common::error::{DatenLordError, DatenLordResult};
use crate::common::task_manager::{TaskName, TASK_MANAGER};

use super::node::{MasterNodeInfo, Node, NodeStatus};
use super::ring::Ring;

/// Watch node events bounded size
const DEFAULT_WATCH_EVENTS_QUEUE_LIMIT: usize = 1000;
/// The timeout for current node's session
const SESSION_TIMEOUT_SEC: u64 = 10;

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
    pub fn new(kv_engine: Arc<KVEngineType>, node: Node) -> Self {
        let inner = Arc::new(ClusterManagerInner::new(kv_engine, node));
        Self { inner }
    }

    /// Get current node
    #[must_use]
    pub fn get_node(&self) -> Node {
        self.inner.get_node()
    }

    /// Get current hashring
    pub async fn get_ring(&self) -> DatenLordResult<Ring<Node>> {
        self.inner.get_ring(false).await
    }

    /// Get all nodes
    pub async fn get_nodes(&self) -> DatenLordResult<Vec<Node>> {
        self.inner.get_nodes(false).await
    }

    ///  Stop the cluster manager
    pub fn stop(&self) {
        // In this step, we just to clean session and force down the cluster manager
        self.inner.clean_sessions();
    }

    /// Run the cluster manager as state machine
    ///
    /// We need to perpare current node info, current node list and generate hash ring info
    pub async fn run(&self) -> DatenLordResult<()> {
        // 0. Prepare for the session
        self.inner.prepare().await?;

        // 1. Init cluster manager
        info!("Cluster manager start to run");
        let mut current_node_info = self.inner.get_node();
        // Next step is to register the node
        current_node_info.set_status(NodeStatus::Registering);
        self.inner.update_node(current_node_info);
        self.inner.update_node_in_cluster().await?;
        loop {
            let mut current_node_info = self.inner.get_node();
            // 2. Register node to etcd
            info!("Current node status: {:?}", current_node_info.status());
            while self.inner.register().await.is_err() {
                error!("Failed to register node, retry in 5s");
                // Try to register node to etcd
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            // Update node status to Registering
            current_node_info.set_status(NodeStatus::Slave);
            self.inner.update_node(current_node_info);
            self.inner.update_node_in_cluster().await?;

            loop {
                // 2. Register node to etcd
                let mut current_node_info = self.inner.get_node();
                info!("Current node status: {:?}", current_node_info.status());
                // 3. Do campaign
                let (campaign_status, campaign_val) = self.inner.do_campaign().await?;
                info!(
                    "Campaign status: {:?} val: {:?}",
                    campaign_status, campaign_val
                );
                if campaign_status {
                    // Update node status to Master
                    current_node_info.set_status(NodeStatus::Master);
                    self.inner.update_node(current_node_info);
                    self.inner.update_node_in_cluster().await?;
                }

                // 4. Serve as normal status
                let current_node_info = self.inner.get_node();
                match current_node_info.status() {
                    // Serve as slave node
                    NodeStatus::Slave => {
                        self.do_slave_tasks().await?;
                    }
                    // Serve as master node
                    NodeStatus::Master => {
                        self.do_master_tasks().await?;
                    }
                    // Other parts can not
                    NodeStatus::Initializing | NodeStatus::Registering => {
                        // Clean up tasks
                        self.inner.clean_sessions();
                        break;
                    }
                }

                // 5. Prepare for the next loop
                self.inner.prepare().await?;
            }
        }
    }

    /// Do master tasks
    ///
    /// 1. Master node will watch the node list update, and update the ring
    /// 2. Master will check self
    async fn do_master_tasks(&self) -> DatenLordResult<()> {
        info!("do_master_tasks: will watch the node list update, and update the ring");

        // loop for node list update
        loop {
            // Keep alive master key
            // Check current status
            let current_node_info = self.inner.get_node();
            if current_node_info.status() != NodeStatus::Master {
                // If the node status is not master, clean up master tasks and return
                error!("Current node status is not master, return to endpoint");

                return Ok(());
            }

            // Create a mpsc channel, we divide read and write operation into two task
            let (tx, rx) = flume::bounded::<()>(DEFAULT_WATCH_EVENTS_QUEUE_LIMIT);
            // We will try to update cluster topo in write task
            // And watch node list update in read task
            let inner = Arc::clone(&self.inner);
            TASK_MANAGER
                .spawn(TaskName::EtcdKeepAlive, |token| async move {
                    inner.write_task(rx, token).await;
                })
                .await
                .with_context(|| "Failed to spawn etcd keep alive task")?;

            // Do watch node list update task
            // This task will block until the master status changed
            self.inner
                .watch_nodes(tx)
                .await
                .with_context(|| "Failed to watch the node list update, and update the ring")?;
        }
    }

    /// Do slave tasks
    ///
    /// Slave node will watch the ring update and campaign master
    #[allow(clippy::arithmetic_side_effects, clippy::pattern_type_mismatch)] // The `select!` macro will generate code that goes against these rules.
    async fn do_slave_tasks(&self) -> DatenLordResult<()> {
        info!("do_slave_tasks: will watch the ring update and campaign master");

        // 1. Try to watch master and hashring
        // Wait for status update
        loop {
            // Check the node status
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(SESSION_TIMEOUT_SEC.overflow_div(3))) => {
                    let current_node_info = self.inner.get_node();
                    // Check the node status
                    if current_node_info.status() != NodeStatus::Slave {
                        return Ok(());
                    }

                    // Block here, try to watch master
                    // If the master is down and return ok, the slave node will try to get the master lock
                    // If watch_master failed, we will retry to watch master
                    if let Ok(()) = self.inner.watch_master().await {
                        return Ok(());
                    }
                }
            }
        }
    }
}

/// Inner struct of cluster manager
#[derive(Debug)]
#[allow(dead_code)]
pub struct ClusterManagerInner {
    /// Etcd client
    kv_engine: Arc<KVEngineType>,
    /// Node session, try to keep the session alive, we will set the session in run method
    node_session: ArcSwapOption<Session>,
    /// current node info
    node: ArcSwap<Node>,
    /// current node list, we will set the session in run method
    node_list: ArcSwap<Vec<Node>>,
    /// current hash ring, we will set the session in run method
    hash_ring: ArcSwap<Ring<Node>>,
    // TODO: Support distribute cluster config here
}

#[allow(dead_code)]
impl ClusterManagerInner {
    /// Create a new cluster manager
    pub fn new(kv_engine: Arc<KVEngineType>, node: Node) -> Self {
        let node_session = ArcSwapOption::empty();
        let node = ArcSwap::<Node>::new(Arc::new(node));
        let node_list = ArcSwap::<Vec<Node>>::new(Arc::new(Vec::new()));
        let hash_ring = ArcSwap::<Ring<Node>>::new(Arc::new(Ring::default()));

        Self {
            kv_engine,
            node_session,
            node,
            node_list,
            hash_ring,
        }
    }

    /// Prepare for the session and other data
    ///
    /// We need to perpare current node info, current node list and generate hash ring info
    pub async fn prepare(&self) -> DatenLordResult<()> {
        // Prepare session
        let session = self
            .kv_engine
            .create_session(SESSION_TIMEOUT_SEC)
            .await
            .with_context(|| format!("{} failed to create session", self.node.load().ip()))?;
        self.node_session.store(Some(session));

        // Prepare node list
        let node_list = self.load_nodes().await?;
        self.node_list.store(Arc::new(node_list.clone()));

        // Prepare hashring
        let mut current_cluster_ring = self.load_ring().await?;
        let ring_res = current_cluster_ring.batch_add(node_list.clone(), true);
        if ring_res.is_none() {
            error!("{} failed to batch add node to ring", self.node.load().ip());
            return Err(DatenLordError::CacheClusterErr {
                context: vec![format!(
                    "{} failed to batch add node to ring",
                    self.node.load().ip()
                )],
            });
        }
        self.hash_ring.store(Arc::new(current_cluster_ring));

        Ok(())
    }

    /// Update node info
    pub async fn update_node_in_cluster(&self) -> DatenLordResult<()> {
        let node = self.node.load();
        let key = &KeyType::CacheNode(node.ip().to_owned());
        let node_session = self.node_session.load();
        let Some(current_session) = node_session.as_ref().cloned() else {
            let current_node = self.node.load();
            error!(
                "Current {} session is invalid, return to endpoint",
                current_node.ip()
            );
            return Err(DatenLordError::CacheClusterErr {
                context: vec![format!("Current {} session is invalid", current_node.ip())],
            });
        };

        self.kv_engine
            .set(
                key,
                &ValueType::Json(serde_json::to_value(node.as_ref())?),
                Some(SetOption {
                    session: Some(current_session),
                    prev_kv: false,
                }),
            )
            .await
            .with_context(|| "Failed to register node to etcd".to_owned())?;

        Ok(())
    }

    /// Register current node to etcd and keep alive
    pub async fn register(&self) -> DatenLordResult<()> {
        // Get current node info
        let current_node_info = self.node.load();
        info!("register: {} to etcd", current_node_info.ip());
        let node_session = self.node_session.load();
        let Some(current_session) = node_session.as_ref().cloned() else {
            let current_node = self.node.load();
            error!(
                "Current {} session is invalid, return to endpoint",
                current_node.ip()
            );
            return Err(DatenLordError::CacheClusterErr {
                context: vec![format!("Current {} session is invalid", current_node.ip())],
            });
        };

        // Try to register current node to etcd
        self.kv_engine
            .set(
                &KeyType::CacheNode(current_node_info.ip().to_owned()),
                &ValueType::Json(serde_json::to_value(current_node_info.as_ref().clone())?),
                Some(SetOption {
                    // Set lease
                    session: Some(current_session),
                    prev_kv: false,
                }),
            )
            .await
            .with_context(|| "Failed to register node to etcd".to_owned())?;

        info!("register: {} to etcd success", current_node_info.ip());

        Ok(())
    }

    /// Do campaign
    ///
    /// Try to campaign master, will return status and master value
    pub async fn do_campaign(&self) -> DatenLordResult<(bool, String)> {
        let node = self.get_node();
        let hash_ring = self.get_ring(false).await?;

        let node_session = self.node_session.load();
        let session = if let Some(session) = node_session.as_ref().cloned() {
            if session.is_closed() {
                let current_node = self.node.load();
                error!(
                    "Current {} session is invalid, return to endpoint",
                    current_node.ip()
                );
                return Err(DatenLordError::CacheClusterErr {
                    context: vec![format!("Current {} session is invalid", current_node.ip())],
                });
            }
            session
        } else {
            let current_node = self.node.load();
            error!(
                "Current {} session is invalid, return to endpoint",
                current_node.ip()
            );
            return Err(DatenLordError::CacheClusterErr {
                context: vec![format!("Current {} session is invalid", current_node.ip())],
            });
        };

        // Master key info
        let master_key = &KeyType::CacheMasterNode;
        // Create master instance
        let master_node_info =
            MasterNodeInfo::new(node.ip().to_owned(), node.port(), hash_ring.version());
        let master_node_info_json = serde_json::to_value(master_node_info)?.to_string();

        // Try to set campaign
        let txn = self.kv_engine.new_meta_txn().await;
        let (campaign_status, campaign_val) = txn
            .campaign(master_key, master_node_info_json, session)
            .await?;

        Ok((campaign_status, campaign_val))
    }

    /// Write current cluster info to etcd
    #[allow(clippy::arithmetic_side_effects, clippy::pattern_type_mismatch)] // The `select!` macro will generate code that goes against these rules.
    async fn write_task(&self, rx: flume::Receiver<()>, token: CancellationToken) {
        // Write ticker task
        let mut interval = tokio::time::interval(Duration::from_secs(SESSION_TIMEOUT_SEC));
        let mut watch_event = false;
        loop {
            if !self.check_session_valid() {
                let current_node = self.node.load();
                error!(
                    "Current {} session is invalid, return to endpoint",
                    current_node.ip()
                );
                return;
            };

            tokio::select! {
                _ = interval.tick() => {
                    // Update cluster topo
                    if watch_event {
                        // Update cluster topo
                        match self.update_cluster_topo().await {
                            Ok(()) => {
                                info!("Update cluster topo success");
                            }
                            Err(e) => {
                                error!("Failed to update cluster topo: {:?}", e);
                            }
                        }
                        watch_event = false;
                    }
                }
                _ = rx.recv_async() => {
                    // Receive event, try to update cluster topo
                    watch_event = true;
                }
                () = token.cancelled() => {
                    // Cancelled
                    break;
                }
            }
        }
    }

    /// Clean up the tasks
    pub fn clean_sessions(&self) {
        // Clean up register tasks
        self.node_session.store(None);
    }

    /// Try to check current session is valid
    pub fn check_session_valid(&self) -> bool {
        let session = self.node_session.load();
        if let Some(session) = session.as_ref() {
            if session.is_closed() {
                return false;
            }
            return true;
        }

        false
    }

    /// Master node will watch the node list update, and update the ring
    pub async fn watch_nodes(&self, tx: flume::Sender<()>) -> DatenLordResult<()> {
        info!("watch_nodes: will watch the node list update");

        // Get all nodes with prefix and init hash ring list
        // TODO: Block cluster and do not add any new node when watch_nodes is synced.

        // Get all nodes with prefix
        let key = &KeyType::CacheNode(String::new());
        let mut nodes_watch_stream = self.kv_engine.watch(key).await?;

        // Wait for node list update
        loop {
            match nodes_watch_stream.next().await {
                Some(Ok(event)) => {
                    let key = event.0;
                    let value = event.1;
                    if value.is_some() {
                        // Update event
                        info!("Receive update node list event with key: {:?}", key);

                        tx.send_async(()).await.map_err(|e| {
                            error!("Failed to send watch event: {:?}", e);
                            DatenLordError::CacheClusterErr {
                                context: vec![format!("Failed to send watch event")],
                            }
                        })?;
                    } else {
                        // Delete event
                        info!("delete node list event with key: {:?}", key);

                        tx.send_async(()).await.map_err(|e| {
                            error!("Failed to send watch event: {:?}", e);
                            DatenLordError::CacheClusterErr {
                                context: vec![format!("Failed to send watch event")],
                            }
                        })?;
                    }
                }
                None => {
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
    async fn update_cluster_topo(&self) -> DatenLordResult<()> {
        // Check session
        let node_session = self.node_session.load();
        if let Some(session) = node_session.as_ref().cloned() {
            if session.is_closed() {
                let current_node = self.node.load();
                error!(
                    "Current {} session is invalid, return to endpoint",
                    current_node.ip()
                );
                return Err(DatenLordError::CacheClusterErr {
                    context: vec![format!("Current {} session is invalid", current_node.ip())],
                });
            }

            // 1. Update node list
            let node_list = self.get_nodes(true).await?;
            let mut hash_ring = self.get_ring(true).await?;
            // Try to replace current hashring with new node list
            // TODO: It will cause a lot of copy, we need to update it in next PR.
            let ring_res = hash_ring.batch_replace(node_list.clone(), true);
            if ring_res.is_none() {
                error!("Failed to update hash ring");
                return Err(DatenLordError::CacheClusterErr {
                    context: vec![format!("Failed to update hash ring")],
                });
            }
            self.hash_ring.store(Arc::new(hash_ring));

            // 2. Update hashring data to etcd
            self.save_ring().await?;

            // Update master info to etcd
            let master_key = &KeyType::CacheMasterNode;

            // Create master instance
            let current_node_info = self.node.load();
            let current_hash_ring = self.hash_ring.load();
            let master_node_info = MasterNodeInfo::new(
                current_node_info.ip().to_owned(),
                current_node_info.port(),
                current_hash_ring.version(),
            );
            let master_node_info_json = serde_json::to_value(master_node_info)?;
            let master_value = &ValueType::Json(master_node_info_json);

            // Update master data
            // Try to update master hashring version
            self.kv_engine
                .set(
                    master_key,
                    master_value,
                    Some(SetOption {
                        session: Some(session),
                        prev_kv: false,
                    }),
                )
                .await?;
        } else {
            let current_node = self.node.load();
            error!(
                "Current {} session is invalid, return to endpoint",
                current_node.ip()
            );
            return Err(DatenLordError::CacheClusterErr {
                context: vec![format!("Current {} session is invalid", current_node.ip())],
            });
        }

        Ok(())
    }

    /// Try to watch the master node
    /// If the master node is down, the slave node will try to get the master lock
    /// Then current node will become the master node if operation is successful
    pub async fn watch_master(&self) -> DatenLordResult<()> {
        info!("watch_master: will watch the master node and try to update master hashring");

        // Watch with prefix
        let master_key = &KeyType::CacheMasterNode;
        let mut master_watch_stream = self.kv_engine.watch(master_key).await?;

        // Check the master key is exist
        // If the master key is not exist, the slave node will be blocked in watch stream and do nothing here
        // We need to quickly return to the main loop and try to campaign master
        // 1. Fetch the master node info
        if let Some(ValueType::Json(master_json)) = self.kv_engine.get(master_key).await? {
            let master_node_info: MasterNodeInfo = serde_json::from_value(master_json)?;
            info!("master node info: {:?}", master_node_info);
        } else {
            debug!("Master is not existed, try to campaign master");
            return Ok(());
        }

        // Watch master key events
        // master key is keeped by lease, so the master node will be auto deleted
        // If master has changed, try to update the value
        // 1. If master ip changed, try to change the master node
        // if master node is down.
        // If the version is changed, try to update the ring
        // In current case, we just need to detect master key change and update hashring.
        // 2. If the master key is auto deleted, exit and change current status to slave.
        loop {
            match master_watch_stream.next().await {
                Some(Ok(event)) => {
                    let key = event.0;
                    let value = event.1;
                    if value.is_some() {
                        // Update event
                        info!("Receive update ring event with key: {:?}", key);

                        // When master update the hashring, we will try to fetch latest ring from etcd
                        // In this step, we just need to update the ring
                        // Fetch the latest ring from etcd
                        // We just ignore the update event in current step
                    } else {
                        // Delete event
                        info!("delete master event with key: {:?}", key);
                        // Master has down, try to campaign master
                        // Return to main loop
                        return Ok(());
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
    async fn save_ring(&self) -> DatenLordResult<()> {
        // Only master node can save ring to etcd
        // We need to make sure only one master can save ring to etcd
        if self.node.load().status() != NodeStatus::Master {
            error!("Current node is not master, can not save ring to etcd");
            return Err(DatenLordError::CacheClusterErr {
                context: vec![format!(
                    "Current node is not master, can not save ring to etcd"
                )],
            });
        }

        // TODO: Add a txn to update ring, update hashring is usually in low priority.
        // Try to check the ring version and update the ring to etcd
        // If failed, we need to return upper level and regenerate the hashring
        let ring_key = &KeyType::CacheRing;
        let mut txn = self.kv_engine.new_meta_txn().await;
        let latest_hash_ring = match txn.get(ring_key).await {
            Ok(Some(ValueType::Json(ring_json))) => {
                // Get current ring from etcd
                let ring: Ring<Node> = serde_json::from_value(ring_json)?;
                ring
            }
            Ok(_) => {
                // Get current local hash ring
                self.hash_ring.load().as_ref().clone()
            }
            Err(e) => {
                error!("Failed to get ring from etcd: {:?}", e);
                return Err(DatenLordError::CacheClusterErr {
                    context: vec![format!("Failed to get ring from etcd")],
                });
            }
        };

        let current_hash_ring = self.hash_ring.load();
        let ring = current_hash_ring.as_ref();
        if latest_hash_ring.version() > ring.version() {
            // If the latest ring version is greater than current ring version, we need to return
            // and regenerate the hashring
            error!("Current ring version is not the latest, return to endpoint");
            return Err(DatenLordError::CacheClusterErr {
                context: vec![format!("Current ring version is not the latest")],
            });
        }

        let current_json_value;
        if let Ok(json_value) = serde_json::to_value(ring.clone()) {
            current_json_value = json_value;
        } else {
            error!("Failed to serialize ring");
            return Ok(());
        }
        let ring_value = &ValueType::Json(current_json_value);

        txn.set(ring_key, ring_value);
        txn.commit().await?;

        Ok(())
    }

    /// Load ring from etcd
    async fn load_ring(&self) -> DatenLordResult<Ring<Node>> {
        let key = &KeyType::CacheRing;
        debug!("Try to load ring from etcd: {}", key);

        // Get ring from etcd
        match self.kv_engine.get(key).await? {
            Some(ValueType::Json(ring_json)) => {
                let new_ring: Ring<Node> = serde_json::from_value(ring_json)?;

                info!("Load ring from etcd success");
                Ok(new_ring)
            }
            None => {
                info!("Ring is not existed, return default ring");
                Ok(Ring::default())
            }
            _ => {
                error!("Failed to deserialize ring");
                Err(DatenLordError::CacheClusterErr {
                    context: vec![format!("Failed to deserialize ring")],
                })
            }
        }
    }

    /// Get node list from etcd
    async fn load_nodes(&self) -> DatenLordResult<Vec<Node>> {
        let key = &KeyType::CacheNode(String::new());
        debug!("Get node list from etcd: {}", key);

        // Get node list from etcd
        let nodes = self.kv_engine.range(key).await?;
        let mut node_list = Vec::new();
        for node in nodes {
            if let ValueType::Json(node_json) = node {
                let node: Node = serde_json::from_value(node_json)?;
                node_list.push(node);
            } else {
                warn!("Failed to deserialize node");
                return Err(DatenLordError::CacheClusterErr {
                    context: vec![format!("Failed to deserialize node")],
                });
            }
        }

        Ok(node_list)
    }

    /// Get all nodes
    ///
    /// This function is used to get all nodes from etcd
    /// If we set `must_fetch`, we will also update current node list
    pub async fn get_nodes(&self, must_fetch: bool) -> DatenLordResult<Vec<Node>> {
        // Get latest node list from etcd
        if must_fetch {
            let latest_nodes = self.load_nodes().await?;
            self.node_list.store(Arc::new(latest_nodes.clone()));
            return Ok(latest_nodes);
        }

        Ok(self.node_list.load().as_ref().clone())
    }

    /// Get current node
    pub fn get_node(&self) -> Node {
        self.node.load().as_ref().clone()
    }

    /// Update current node
    pub fn update_node(&self, node: Node) {
        self.node.store(Arc::new(node));
    }

    /// Get hashring
    ///
    /// This function is used to get current hashring and update current hashring
    /// If we set `must_fetch`, we will also update current hashring
    pub async fn get_ring(&self, must_fetch: bool) -> DatenLordResult<Ring<Node>> {
        // Get latest hashring from etcd and update current hashring
        if must_fetch {
            let latest_hash_ring = self.load_ring().await?;
            self.hash_ring.store(Arc::new(latest_hash_ring.clone()));
            return Ok(latest_hash_ring);
        }

        Ok(self.hash_ring.load().as_ref().clone())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use tracing::info;
    use tracing_subscriber;

    use crate::{
        async_fuse::memfs::kv_engine::{DeleteOption, KVEngine, KVEngineType, KeyType},
        storage::cache_proxy::{
            cluster_manager::{ClusterManager, ClusterManagerInner, SESSION_TIMEOUT_SEC},
            node::{Node, NodeStatus},
        },
    };

    const ETCD_ADDRESS: &str = "127.0.0.1:2379";

    /// Helper function to create a new node with a given IP address
    fn create_node(ip: &str) -> Node {
        let mut node = Node::default();
        node.set_ip(ip.to_owned());

        node
    }

    async fn clean_up_etcd() {
        // Clean up all `CacheNode` prefix keys in etcd
        KVEngineType::new(vec![ETCD_ADDRESS.to_owned()])
            .await
            .unwrap()
            .delete(
                &KeyType::CacheNode(String::new()),
                Some(DeleteOption {
                    prev_kv: false,
                    range_end: Some(vec![0xff]),
                }),
            )
            .await
            .unwrap();

        // Clean up all `CacheMasterNode` keys in etcd
        KVEngineType::new(vec![ETCD_ADDRESS.to_owned()])
            .await
            .unwrap()
            .delete(
                &KeyType::CacheMasterNode,
                Some(DeleteOption {
                    prev_kv: false,
                    range_end: Some(vec![0xff]),
                }),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_single_master_election() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .init();
        let client = Arc::new(
            KVEngineType::new(vec![ETCD_ADDRESS.to_owned()])
                .await
                .unwrap(),
        );

        // Clean up etcd
        clean_up_etcd().await;

        let test_master_node = create_node("192.168.1.2");
        let master_cluster_manager =
            ClusterManagerInner::new(Arc::clone(&client), test_master_node.clone());
        master_cluster_manager.prepare().await.unwrap();
        let test_slave_node_1 = create_node("192.168.1.3");
        let slave_1_cluster_manager =
            ClusterManagerInner::new(Arc::clone(&client), test_slave_node_1.clone());
        slave_1_cluster_manager.prepare().await.unwrap();
        let test_slave_node_2 = create_node("192.168.1.4");
        let slave_2_cluster_manager =
            ClusterManagerInner::new(Arc::clone(&client), test_slave_node_2.clone());
        slave_2_cluster_manager.prepare().await.unwrap();

        info!("test_single_master_election: start to test single master election");

        let (master_res, slave_1_res, slave_2_res) = tokio::join!(
            async {
                // Register node
                master_cluster_manager.register().await.unwrap();
                // campaign
                master_cluster_manager.do_campaign().await.unwrap()
            },
            async {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                slave_1_cluster_manager.register().await.unwrap();
                // campaign
                slave_1_cluster_manager.do_campaign().await.unwrap()
            },
            async {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                slave_2_cluster_manager.register().await.unwrap();
                // campaign
                slave_2_cluster_manager.do_campaign().await.unwrap()
            }
        );

        // Check the result
        assert!(master_res.0);
        assert!(!slave_1_res.0);
        assert!(!slave_2_res.0);
    }

    #[tokio::test]
    async fn test_remove_slave_node() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .init();
        let client = Arc::new(
            KVEngineType::new(vec![ETCD_ADDRESS.to_owned()])
                .await
                .unwrap(),
        );

        // Clean up etcd
        clean_up_etcd().await;

        // Setup initial state with multiple nodes
        let test_master_node = create_node("192.168.3.2");
        let test_slave_node = create_node("192.168.3.3");
        let test_slave_node_2 = create_node("192.168.3.4");

        let master_client = Arc::clone(&client);
        let test_master_node_clone = test_master_node.clone();
        let master_cluster_manager = Arc::new(ClusterManager::new(
            master_client,
            test_master_node_clone.clone(),
        ));
        let master_cluster_manager_clone = Arc::clone(&master_cluster_manager);
        let master_handle = tokio::task::spawn(async move {
            let res = master_cluster_manager_clone.run().await;
            info!("master_handle: {:?}", res);
        });

        // Wait for election
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        let slave_client = Arc::clone(&client);
        let test_slave_node_clone = test_slave_node.clone();
        let slave_cluster_manager_1 = Arc::new(ClusterManager::new(
            slave_client,
            test_slave_node_clone.clone(),
        ));
        let slave_cluster_manager_1_clone = Arc::clone(&slave_cluster_manager_1);
        let slave_handle_1 = tokio::task::spawn(async move {
            let res = slave_cluster_manager_1_clone.run().await;
            info!("slave_handle_1: {:?}", res);
        });

        let slave_client = Arc::clone(&client);
        let test_slave_node_clone = test_slave_node_2.clone();
        let slave_cluster_manager_2 = Arc::new(ClusterManager::new(
            slave_client,
            test_slave_node_clone.clone(),
        ));
        let slave_cluster_manager_2_clone = Arc::clone(&slave_cluster_manager_2);
        let slave_handle_2 = tokio::task::spawn(async move {
            let res: Result<(), crate::common::error::DatenLordError> =
                slave_cluster_manager_2_clone.run().await;
            info!("slave_handle_2: {:?}", res);
        });

        // Wait for node online
        tokio::time::sleep(std::time::Duration::from_secs(SESSION_TIMEOUT_SEC)).await;

        assert_eq!(
            Arc::clone(&master_cluster_manager).get_node().status(),
            NodeStatus::Master
        );
        assert_eq!(
            Arc::clone(&slave_cluster_manager_1).get_node().status(),
            NodeStatus::Slave
        );
        assert_eq!(
            Arc::clone(&slave_cluster_manager_2).get_node().status(),
            NodeStatus::Slave
        );
        assert_eq!(master_cluster_manager.get_nodes().await.unwrap().len(), 3);

        info!("test_remove_slave_node: update node list");
        // Cancel the slave task
        slave_handle_1.abort();
        slave_cluster_manager_1.stop();
        // Wait for slave key is deleted
        tokio::time::sleep(std::time::Duration::from_secs(2 * SESSION_TIMEOUT_SEC)).await;

        info!("test_remove_slave_node: start to test remove slave node");
        info!(
            "Get all nodes: {:?}",
            master_cluster_manager.get_nodes().await.unwrap()
        );

        assert_eq!(
            master_cluster_manager.get_node().status(),
            NodeStatus::Master
        );
        assert_eq!(master_cluster_manager.get_nodes().await.unwrap().len(), 2);

        slave_handle_2.abort();
        master_handle.abort();
    }

    #[tokio::test]
    async fn test_remove_master_node() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .init();
        let client = Arc::new(
            KVEngineType::new(vec![ETCD_ADDRESS.to_owned()])
                .await
                .unwrap(),
        );

        // Clean up etcd
        clean_up_etcd().await;

        // Setup initial state with multiple nodes
        let test_master_node = create_node("192.168.3.2");
        let test_slave_node = create_node("192.168.3.3");

        // Run master
        let master_client = Arc::clone(&client);
        let test_master_node_clone = test_master_node.clone();
        let master_cluster_manager = Arc::new(ClusterManager::new(
            master_client,
            test_master_node_clone.clone(),
        ));
        let master_cluster_manager_clone = Arc::clone(&master_cluster_manager);
        let master_handle = tokio::task::spawn(async move {
            let res = master_cluster_manager_clone.run().await;
            info!("master_handle: {:?}", res);
        });

        // Wait for the election to finish
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        let slave_client = Arc::clone(&client);
        let test_slave_node_clone = test_slave_node.clone();
        let slave_cluster_manager_1 = Arc::new(ClusterManager::new(
            slave_client,
            test_slave_node_clone.clone(),
        ));
        let slave_cluster_manager_1_clone = Arc::clone(&slave_cluster_manager_1);
        let slave_handle_1 = tokio::task::spawn(async move {
            let res = slave_cluster_manager_1_clone.run().await;
            info!("slave_handle_1: {:?}", res);
        });

        // Wait for node online
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        assert_eq!(
            Arc::clone(&master_cluster_manager).get_node().status(),
            NodeStatus::Master
        );
        assert_eq!(
            Arc::clone(&slave_cluster_manager_1).get_node().status(),
            NodeStatus::Slave
        );

        // Simulate master node removal
        info!("Simulate master node removal");
        master_cluster_manager.stop();
        master_handle.abort();
        // Wait for the system to detect the master node removal
        tokio::time::sleep(std::time::Duration::from_secs(SESSION_TIMEOUT_SEC + 1)).await;

        // Check if the slave node has become the new master
        let new_master_status = slave_cluster_manager_1.get_node().status();
        assert_eq!(new_master_status, NodeStatus::Master);

        info!("test_remove_master_node: slave node has taken over as master");
        slave_handle_1.abort();
    }
}
