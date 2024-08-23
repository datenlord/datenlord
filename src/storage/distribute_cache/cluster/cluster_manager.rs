//! The utilities of distribute cache cluster management

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use arc_swap::{ArcSwap, ArcSwapOption};
use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::async_fuse::memfs::kv_engine::etcd_impl::Session;
use crate::async_fuse::memfs::kv_engine::{KVEngine, SetOption};
use crate::async_fuse::memfs::kv_engine::{KVEngineType, KeyType, ValueType};
use crate::common::error::{DatenLordError, DatenLordResult};

use super::node::{MasterNodeInfo, Node, NodeStatus};
use super::ring::Ring;

/// The timeout for current node's session
const SESSION_TIMEOUT_SEC: u64 = 10;

/// This struct is used to interact with etcd server and manager the cluster.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ClusterManager {
    /// inner struct
    inner: Arc<ClusterManagerInner>,
}

impl ClusterManager {
    /// Create new cluster manager
    pub fn new(kv_engine: Arc<KVEngineType>, node: Node) -> Self {
        let inner = Arc::new(ClusterManagerInner::new(kv_engine, node));
        Self { inner }
    }

    /// Get current node
    #[must_use]
    pub fn get_current_node(&self) -> Node {
        self.inner.get_current_node()
    }

    /// Get current hashring
    pub async fn get_ring(&self) -> DatenLordResult<Ring<Node>> {
        self.inner.get_ring(false).await
    }

    /// Get all nodes
    pub async fn get_nodes(&self) -> DatenLordResult<Vec<Node>> {
        self.inner.get_nodes(false).await
    }

    /// Get node by hashring index
    pub async fn get_node(&self, key: String) -> DatenLordResult<Node> {
        let ring = self.get_ring().await?;
        if let Some(node) = ring.get_node(&key) {
            return Ok(node.clone());
        }

        return Err(DatenLordError::CacheClusterErr {
            context: vec![format!("Failed to get node by key: {:?}", key)],
        });
    }

    /// Watch the distribute cache nodes info,
    /// this function is used for client to watch the ring update
    pub async fn watch_ring(&self, token: CancellationToken) -> DatenLordResult<()> {
        self.inner.watch_ring(token).await
    }

    /// If current hashring version is not matched, we need to update the hashring manaually in client
    pub async fn get_ring_force(&self) -> DatenLordResult<Ring<Node>> {
        self.inner.get_ring(true).await
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
        loop {
            // 0. Prepare for the session
            self.inner.prepare().await?;

            // 1. Init cluster manager
            debug!(
                "Cluster manager start to run in node: {:?}",
                self.inner.get_current_node().ip()
            );
            let mut current_node_info = self.inner.get_current_node();
            // Next step is to register the node
            current_node_info.set_status(NodeStatus::Registering);
            self.inner.update_node(current_node_info);
            self.inner.update_node_in_cluster().await?;

            let mut current_node_info = self.inner.get_current_node();
            // 2. Register node to etcd
            debug!(
                "Current node {:?} status: {:?}",
                current_node_info.ip(),
                current_node_info.status()
            );
            while self.inner.register().await.is_err() {
                warn!("Failed to register node, retry in 5s");
                // Try to register node to etcd
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            // Update node status to Registering
            current_node_info.set_status(NodeStatus::Slave);
            self.inner.update_node(current_node_info);
            self.inner.update_node_in_cluster().await?;

            // 2. Register node to etcd
            let mut current_node_info = self.inner.get_current_node();
            debug!(
                "Current node {:?} status: {:?}",
                current_node_info.ip(),
                current_node_info.status()
            );
            // 3. Do campaign
            let (campaign_status, campaign_val) = self.inner.do_campaign().await?;
            debug!(
                "Node: {:?} Campaign status: {:?} val: {:?}",
                current_node_info.ip(),
                campaign_status,
                campaign_val
            );
            if campaign_status {
                // Update node status to Master
                current_node_info.set_status(NodeStatus::Master);
                self.inner.update_node(current_node_info);
                self.inner.update_node_in_cluster().await?;
            }

            // 4. Serve as normal status
            let current_node_info = self.inner.get_current_node();
            match current_node_info.status() {
                // Serve as slave node
                NodeStatus::Slave => match self.do_slave_tasks().await {
                    Ok(()) => {
                        debug!(
                            "Current node {:?} status: {:?} will change to campaign",
                            current_node_info.ip(),
                            current_node_info.status()
                        );
                    }
                    Err(e) => {
                        warn!("Failed to do slave tasks: {:?}", e);
                    }
                },
                // Serve as master node
                NodeStatus::Master => match self.do_master_tasks().await {
                    Ok(()) => {
                        debug!(
                            "Current node {:?} status: {:?} will change to campaign",
                            current_node_info.ip(),
                            current_node_info.status()
                        );
                    }
                    Err(e) => {
                        warn!("Failed to do master tasks: {:?}", e);
                    }
                },
                // Other parts can not
                NodeStatus::Initializing | NodeStatus::Registering => {
                    // Clean up tasks
                    self.inner.clean_sessions();
                }
            }
        }
    }

    /// Do master tasks
    ///
    /// 1. Master node will watch the node list update, and update the ring
    /// 2. Master will check self
    async fn do_master_tasks(&self) -> DatenLordResult<()> {
        let current_node_info = self.inner.get_current_node();
        debug!(
            "do_master_tasks: {:?} will watch the node list update, and update the ring",
            current_node_info.ip()
        );

        // Keep alive master key
        // Check current status
        if current_node_info.status() != NodeStatus::Master {
            // If the node status is not master, clean up master tasks and return
            warn!(
                "Current node {:?} status is not master, return to endpoint",
                current_node_info.ip()
            );

            return Ok(());
        }

        // Check current cluster hashring info, try to update it or init it
        match self.inner.update_cluster_topo().await {
            Ok(()) => {
                debug!("Update cluster topo success");
            }
            Err(e) => {
                warn!("Failed to update cluster topo: {:?}", e);
                return Err(DatenLordError::CacheClusterErr {
                    context: vec![format!("Failed to update cluster topo: {:?}", e)],
                });
            }
        }

        // Do watch node list update task
        // This task will block until the master status changed
        self.inner.watch_nodes().await.with_context(|| {
            format!(
                "Current node {:?} Failed to watch the node list update, and update the ring",
                current_node_info.ip()
            )
        })?;

        Ok(())
    }

    /// Do slave tasks
    ///
    /// Slave node will watch the ring update and campaign master
    #[allow(clippy::arithmetic_side_effects, clippy::pattern_type_mismatch)] // The `select!` macro will generate code that goes against these rules.
    async fn do_slave_tasks(&self) -> DatenLordResult<()> {
        // Try to watch master and hashring
        // Wait for status update
        let current_node_info = self.inner.get_current_node();
        debug!(
            "do_slave_tasks: Node {:?} will watch the ring update and campaign master",
            current_node_info.ip()
        );
        // Check the node status
        if current_node_info.status() != NodeStatus::Slave {
            return Ok(());
        }

        // Block here, try to watch master
        // If the master is down and return ok, the slave node will try to get the master lock
        // If watch_master failed, we will retry to watch master
        self.inner.watch_master().await?;

        Ok(())
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
    pub async fn prepare(&self) -> DatenLordResult<()> {
        // Prepare session
        let session = self
            .kv_engine
            .create_session(SESSION_TIMEOUT_SEC)
            .await
            .with_context(|| format!("{} failed to create session", self.node.load().ip()))?;
        self.node_session.store(Some(session));
        Ok(())
    }

    /// Update node info
    pub async fn update_node_in_cluster(&self) -> DatenLordResult<()> {
        let node = self.node.load();
        let key = &KeyType::CacheNode(node.ip().to_owned());
        let node_session = self.node_session.load();
        let Some(current_session) = node_session.as_ref().cloned() else {
            let current_node = self.node.load();
            warn!(
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
        debug!("register: {} to etcd", current_node_info.ip());
        let node_session = self.node_session.load();
        let Some(current_session) = node_session.as_ref().cloned() else {
            let current_node = self.node.load();
            warn!(
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

        debug!("register: {} to etcd success", current_node_info.ip());

        Ok(())
    }

    /// Do campaign
    ///
    /// Try to campaign master, will return status and master value
    pub async fn do_campaign(&self) -> DatenLordResult<(bool, String)> {
        let node = self.get_current_node();
        let hash_ring = self.get_ring(false).await?;

        let node_session = self.node_session.load();
        let session = if let Some(session) = node_session.as_ref().cloned() {
            if session.is_closed() {
                let current_node = self.node.load();
                warn!(
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
            warn!(
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
        let master_node_info_json = serde_json::to_value(master_node_info)?;
        let master_value = &ValueType::Json(master_node_info_json);
        let master_serial_value = serde_json::to_string(master_value)
            .with_context(|| format!("failed to serialize value={master_value:?} to string"))?;

        // Try to set campaign
        let txn = self.kv_engine.new_meta_txn().await;
        let (campaign_status, campaign_val) = txn
            .campaign(master_key, master_serial_value, session)
            .await?;

        Ok((campaign_status, campaign_val))
    }

    /// Clean up the tasks
    pub fn clean_sessions(&self) {
        // Clean up register tasks
        self.node_session.store(None);
    }

    /// Try to check current session is valid
    pub fn check_session_valid(&self) -> bool {
        let session = self.node_session.load_full();
        if let Some(session) = session.as_ref() {
            if session.is_closed() {
                return false;
            }
            return true;
        }

        false
    }

    /// Master node will watch the node list update, and update the ring
    pub async fn watch_nodes(&self) -> DatenLordResult<()> {
        debug!(
            "Node: {:?} watch_nodes: will watch the node list update",
            self.get_current_node().ip()
        );
        // Write ticker task
        let mut interval = tokio::time::interval(Duration::from_secs(SESSION_TIMEOUT_SEC));
        let mut watch_event = false;

        // Get all nodes with prefix and init hash ring list
        // TODO: Block cluster and do not add any new node when watch_nodes is synced.

        // Get all nodes with prefix
        let key = &KeyType::CacheNode(String::new());
        let mut nodes_watch_stream = self.kv_engine.watch(key).await?;

        // Wait for node list update
        loop {
            if !self.check_session_valid() {
                let current_node = self.node.load();
                warn!(
                    "Current {} session is invalid, return to endpoint",
                    current_node.ip()
                );
                return Err(DatenLordError::CacheClusterErr {
                    context: vec![format!("Current {} session is invalid", current_node.ip())],
                });
            };

            // Check event
            tokio::select! {
                // Write event
                // This future will write all changed events in one task
                _ = interval.tick() => {
                    if watch_event {
                        // Update cluster topo task
                        match self.update_cluster_topo().await {
                            Ok(()) => {
                                debug!("Update cluster topo success");
                            }
                            Err(e) => {
                                warn!("Failed to update cluster topo: {:?}", e);
                                return Err(DatenLordError::CacheClusterErr {
                                    context: vec![format!("Failed to update cluster topo: {:?}", e)],
                                });
                            }
                        }
                        watch_event = false;
                    }
                }
                // Read event
                stream_event = nodes_watch_stream.next() => {
                    match stream_event {
                        Some(Ok(event)) => {
                            let key = event.0;
                            let value = event.1;

                            // TODO: We want a meticulous update, we can send the event change to a channel
                            // Receive event, try to update cluster topo
                            watch_event = true;

                            if value.is_some() {
                                // Update event
                                debug!("Update node list event with key: {:?}", key);
                            } else {
                                // Delete event
                                debug!("Delete node list event with key: {:?}", key);
                            }
                        }
                        None => {
                            warn!("Failed to get event from watch stream, because of stream channel is closed");
                            return Err(DatenLordError::CacheClusterErr {
                                context: vec![format!("Failed to get event from watch stream, because of stream channel is closed")],
                            });
                        }
                        Some(Err(e)) => {
                            // Raise error and return to upper level, try to rewatch master again
                            warn!("Failed to watch node list key: {:?}", e);
                            return Err(DatenLordError::CacheClusterErr {
                                context: vec![format!("Failed to watch node list key")],
                            });
                        }
                    }
                }
            }
        }
    }

    /// Update cluster topo
    ///
    /// Try to fetch latest nodes and hashring, and update cluster by current master node
    async fn update_cluster_topo(&self) -> DatenLordResult<()> {
        // Check session
        let node_session = self.node_session.load();
        if let Some(session) = node_session.as_ref().cloned() {
            if session.is_closed() {
                let current_node = self.node.load();
                warn!(
                    "Current {} session is invalid, return to endpoint",
                    current_node.ip()
                );
                return Err(DatenLordError::CacheClusterErr {
                    context: vec![format!("Current {} session is invalid", current_node.ip())],
                });
            }

            // 1. Update node list
            let node_list = self.get_nodes(true).await?;
            // We want to update the hashring base on etcd hashring value and version
            let mut hash_ring = self.get_ring(true).await?;
            // Try to replace current hashring with new node list
            // TODO: It will cause a lot of copy, we need to update it in next PR.
            let ring_res = hash_ring.batch_replace(node_list.clone(), true);
            if ring_res.is_none() {
                warn!("Failed to update hash ring");
                return Err(DatenLordError::CacheClusterErr {
                    context: vec![format!("Failed to update hash ring")],
                });
            }
            self.hash_ring.store(Arc::new(hash_ring));

            // 2. Update hashring data and current master version to etcd with txn
            self.save_ring().await?;
        } else {
            let current_node = self.node.load();
            warn!(
                "Current {} session is invalid, return to endpoint",
                current_node.ip()
            );
            return Err(DatenLordError::CacheClusterErr {
                context: vec![format!("Current {} session is invalid", current_node.ip())],
            });
        }

        Ok(())
    }

    /// Try to watch hashring
    /// This function is used to watch the hashring update for client side and update local hashring value
    /// If the hashring is changed, we will try to update the hashring
    pub async fn watch_ring(&self, token: CancellationToken) -> DatenLordResult<()> {
        debug!("watch_ring: will watch the hashring and try to update local hashring",);

        // Watch with hashring
        let ring_key = &KeyType::CacheRing;
        let mut ring_watch_stream = self.kv_engine.watch(ring_key).await?;

        // Check the ring key is exist
        // If not existed, this function will block here and wait for cache nodes online
        loop {
            if let Ok(ring) = self.load_ring().await {
                debug!("Current ring: {:?}", ring);
                // Update local hashring cache
                self.hash_ring.store(Arc::new(ring.clone()));
                break;
            }
        }

        // Watch ring key events
        // TODO: add cancel token here
        let mut interval = tokio::time::interval(Duration::from_secs(SESSION_TIMEOUT_SEC));
        let mut watch_event = false;
        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    // Cancelled
                    break;
                }
                _ = interval.tick() => {
                    if watch_event {
                        // Fetch and update local cache
                        let ring = self.load_ring().await?;
                        self.hash_ring.store(Arc::new(ring.clone()));
                        watch_event = false;
                    }
                }
                stream_event = ring_watch_stream.next() => {
                    match stream_event {
                        Some(Ok(event)) => {
                            let key = event.0;
                            let value = event.1;
                            if value.is_some() {
                                // Update event
                                debug!(
                                    "Receive update ring event with key: {:?} value: {:?}",
                                    key, value
                                );
                                watch_event = true;

                                if value.is_some() {
                                    // Update event
                                    debug!("Update node list event with key: {:?}", key);
                                } else {
                                    // Delete event
                                    debug!("Delete node list event with key: {:?}", key);
                                }
                            } else {
                                // Delete event
                                debug!("delete ring event with key: {:?}", key);
                                // Master has down, try to campaign master
                                // Return to main loop
                                break;
                            }
                        }
                        None => {
                            warn!("Failed to get event from watch stream, because of stream channel is closed");
                            return Err(DatenLordError::CacheClusterErr {
                                context: vec![format!("Failed to get event from watch stream, because of stream channel is closed")],
                            });
                        }
                        Some(Err(e)) => {
                            // Raise error and return to upper level, try to rewatch master again
                            warn!("Failed to watch ring key: {:?}", e);
                            return Err(DatenLordError::CacheClusterErr {
                                context: vec![format!("Failed to watch ring key")],
                            });
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Try to watch the master node
    /// If the master node is down, the slave node will try to get the master lock
    /// Then current node will become the master node if operation is successful
    pub async fn watch_master(&self) -> DatenLordResult<()> {
        debug!(
            "Node: {:?} watch_master: will watch the master node and try to update master hashring",
            self.get_current_node().ip()
        );

        // Watch with prefix
        let master_key = &KeyType::CacheMasterNode;
        let mut master_watch_stream = self.kv_engine.watch(master_key).await?;

        // Check the master key is exist
        // If the master key is not exist, the slave node will be blocked in watch stream and do nothing here
        // We need to quickly return to the main loop and try to campaign master
        // 1. Fetch the master node info
        if let Some(ValueType::Json(master_json)) = self.kv_engine.get(master_key).await? {
            let master_node_info: MasterNodeInfo = serde_json::from_value(master_json)?;
            debug!("master node info: {:?}", master_node_info);
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
                        debug!(
                            "Receive update ring event with key: {:?} value: {:?}",
                            key, value
                        );

                        // When master update the hashring, we will try to fetch latest ring from etcd
                        // In this step, we just need to update the ring
                        // Fetch the latest ring from etcd
                        // We just ignore the update event in current step
                    } else {
                        // Delete event
                        debug!("delete master event with key: {:?}", key);
                        // Master has down, try to campaign master
                        // Return to main loop
                        return Ok(());
                    }
                }
                None => {
                    warn!("Failed to get event from watch stream, because of stream channel is closed");
                    return Err(DatenLordError::CacheClusterErr {
                        context: vec![format!("Failed to get event from watch stream, because of stream channel is closed")],
                    });
                }
                Some(Err(e)) => {
                    // Raise error and return to upper level, try to rewatch master again
                    warn!("Failed to watch master key: {:?}", e);
                    return Err(DatenLordError::CacheClusterErr {
                        context: vec![format!("Failed to watch master key")],
                    });
                }
            }
        }
    }

    /// Save current master node info and hashring to etcd with one txn
    async fn save_ring(&self) -> DatenLordResult<()> {
        // Only master node can save ring to etcd
        // We need to make sure current node is master and only one master can save ring to etcd
        if self.node.load().status() != NodeStatus::Master {
            warn!("Current node is not master, can not save ring to etcd");
            return Err(DatenLordError::CacheClusterErr {
                context: vec![format!(
                    "Current node is not master, can not save ring to etcd"
                )],
            });
        }

        // Add a txn to update ring, update hashring is usually in low priority.
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
                // Current hashring is not valid or not existed
                // Try to get current local hash ring as default hashring
                self.hash_ring.load().as_ref().clone()
            }
            Err(e) => {
                warn!("Failed to get ring from etcd: {:?}", e);
                return Err(DatenLordError::CacheClusterErr {
                    context: vec![format!("Failed to get ring from etcd")],
                });
            }
        };

        // Check current node is the valid master node
        let master_key = &KeyType::CacheMasterNode;
        let latest_master_node = match txn.get(master_key).await {
            Ok(Some(ValueType::Json(master_json))) => {
                // Get current master node from etcd
                let master_node: MasterNodeInfo = serde_json::from_value(master_json)?;
                master_node
            }
            Ok(_) => {
                warn!("Failed to get master node from etcd, because current master node is not existed.");
                return Err(DatenLordError::CacheClusterErr {
                    context: vec![format!("Failed to get master node from etcd, because current master node is not existed.")],
                });
            }
            Err(e) => {
                warn!("Failed to get master node from etcd: {:?}", e);
                return Err(DatenLordError::CacheClusterErr {
                    context: vec![format!("Failed to get master node from etcd")],
                });
            }
        };
        // Create master instance
        let current_node_info = self.node.load();
        if latest_master_node.ip() != current_node_info.ip()
            || latest_master_node.port() != current_node_info.port()
        {
            warn!("Current node is not the master node, return to endpoint");
            return Err(DatenLordError::CacheClusterErr {
                context: vec![format!("Current node is not the master node")],
            });
        }

        // Prepare master node info and hashring data
        let current_hash_ring = self.hash_ring.load();
        if latest_hash_ring.version() > current_hash_ring.version() {
            // If the latest ring version is greater than current ring version, we need to return
            // and regenerate the hashring
            warn!("Current ring version is not the latest, return to endpoint");
            return Err(DatenLordError::CacheClusterErr {
                context: vec![format!("Current ring version is not the latest")],
            });
        }
        let master_node_info = MasterNodeInfo::new(
            current_node_info.ip().to_owned(),
            current_node_info.port(),
            current_hash_ring.version(),
        );
        let master_node_info_json = serde_json::to_value(master_node_info)?;
        let master_value = &ValueType::Json(master_node_info_json);
        let ring = current_hash_ring.as_ref();
        let current_json_value = serde_json::to_value(ring.clone())?;
        let ring_value = &ValueType::Json(current_json_value);

        txn.set(master_key, master_value);
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

                debug!("Load ring from etcd success");
                Ok(new_ring)
            }
            None => {
                debug!("Ring is not existed, return default ring");
                Ok(Ring::default())
            }
            _ => {
                warn!("Failed to deserialize ring");
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
    pub fn get_current_node(&self) -> Node {
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

    use tracing::{debug, level_filters::LevelFilter};
    use tracing_subscriber::{
        self, filter, fmt::layer, layer::SubscriberExt, util::SubscriberInitExt, Layer,
    };

    use crate::{
        async_fuse::memfs::kv_engine::{DeleteOption, KVEngine, KVEngineType, KeyType},
        storage::distribute_cache::cluster::{
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
        let filter = filter::Targets::new().with_target(
            "datenlord::storage::cache_proxy::cluster_manager",
            LevelFilter::DEBUG,
        );
        tracing_subscriber::registry()
            .with(layer().with_filter(filter))
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

        debug!("test_single_master_election: start to test single master election");

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
        let filter = filter::Targets::new().with_target(
            "datenlord::storage::cache_proxy::cluster_manager",
            LevelFilter::DEBUG,
        );
        tracing_subscriber::registry()
            .with(layer().with_filter(filter))
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
            debug!("master_handle: {:?}", res);
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
            debug!("slave_handle_1: {:?}", res);
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
            debug!("slave_handle_2: {:?}", res);
        });

        // Wait for node online
        tokio::time::sleep(std::time::Duration::from_secs(SESSION_TIMEOUT_SEC)).await;

        assert_eq!(
            Arc::clone(&master_cluster_manager)
                .get_current_node()
                .status(),
            NodeStatus::Master
        );
        assert_eq!(
            Arc::clone(&slave_cluster_manager_1)
                .get_current_node()
                .status(),
            NodeStatus::Slave
        );
        assert_eq!(
            Arc::clone(&slave_cluster_manager_2)
                .get_current_node()
                .status(),
            NodeStatus::Slave
        );
        assert_eq!(master_cluster_manager.get_nodes().await.unwrap().len(), 3);

        debug!("test_remove_slave_node: update node list");
        // Cancel the slave task
        slave_handle_1.abort();
        slave_cluster_manager_1.stop();
        // Wait for slave key is deleted
        tokio::time::sleep(std::time::Duration::from_secs(2 * SESSION_TIMEOUT_SEC)).await;

        debug!("test_remove_slave_node: start to test remove slave node");
        debug!(
            "Get all nodes: {:?}",
            master_cluster_manager.get_nodes().await.unwrap()
        );

        assert_eq!(
            master_cluster_manager.get_current_node().status(),
            NodeStatus::Master
        );
        assert_eq!(master_cluster_manager.get_nodes().await.unwrap().len(), 2);

        slave_handle_2.abort();
        master_handle.abort();
    }

    #[tokio::test]
    async fn test_remove_master_node() {
        let filter = filter::Targets::new().with_target(
            "datenlord::storage::cache_proxy::cluster_manager",
            LevelFilter::DEBUG,
        );
        tracing_subscriber::registry()
            .with(layer().with_filter(filter))
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
            debug!("master_handle: {:?}", res);
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
            debug!("slave_handle_1: {:?}", res);
        });

        // Wait for node online
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        assert_eq!(
            Arc::clone(&master_cluster_manager)
                .get_current_node()
                .status(),
            NodeStatus::Master
        );
        assert_eq!(
            Arc::clone(&slave_cluster_manager_1)
                .get_current_node()
                .status(),
            NodeStatus::Slave
        );

        // Simulate master node removal
        debug!("Simulate master node removal");
        master_cluster_manager.stop();
        master_handle.abort();
        // Wait for the system to detect the master node removal
        tokio::time::sleep(std::time::Duration::from_secs(SESSION_TIMEOUT_SEC + 1)).await;

        // Check if the slave node has become the new master
        let new_master_status = slave_cluster_manager_1.get_current_node().status();
        assert_eq!(new_master_status, NodeStatus::Master);

        debug!("test_remove_master_node: slave node has taken over as master");
        slave_handle_1.abort();
    }
}
