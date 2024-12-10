use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::sync::Mutex;
use tracing::{debug, error, warn};

use crate::{
    common::{
        error::{DatenLordError, DatenLordResult},
        task_manager::{TaskName, TASK_MANAGER},
    },
    connect_timeout,
    distribute_kv_cache::rpc::{error::RpcError, utils::u64_to_usize},
    storage::Block,
};

use crate::distribute_kv_cache::{
    cluster::cluster_manager::ClusterManager,
    rpc::{
        client::RpcClient,
        common::ClientTimeoutOptions,
        message::{FileBlockPacket, FileBlockRequest, FileBlockResponse},
    },
};

/// Duration to check the rpc client cache
const RPC_CLIENT_CACHE_CHECK_DURATION: u64 = 60;

/// Get block path by `ino`, `mtime`, `block_id` and `block_size`
fn get_block_path_id(ino: u64, mtime: u64, block_id: u64, block_size: u64) -> String {
    format!("{}_{}_{}_{}", ino, mtime, block_id, block_size)
}

/// The distribute cache client
#[derive(Debug, Clone)]
pub struct DistributeCacheClient {
    /// The cluster manager, only used to watch hashring changes
    cluster_manager: Arc<ClusterManager>,
    /// The rpc client cache
    rpc_client_cache: Arc<Mutex<HashMap<String, RpcClient<FileBlockPacket>>>>,
}

impl DistributeCacheClient {
    /// Create a new distribute cache client
    pub fn new(cluster_manager: Arc<ClusterManager>) -> Self {
        let rpc_client_cache = Arc::new(Mutex::new(HashMap::new()));
        Self {
            cluster_manager,
            rpc_client_cache,
        }
    }

    /// Start the distribute cache client watch task
    pub async fn start_watch(&self) -> DatenLordResult<()> {
        // Start the watch task
        let cluster_manager = self.cluster_manager.clone();
        TASK_MANAGER
            .spawn(TaskName::AsyncFuse, |token| async move {
                match cluster_manager.watch_ring(token).await {
                    Ok(_) => {}
                    Err(err) => {
                        error!("Failed to watch ring: {:?}", err);
                    }
                }
            })
            .await
            .map_err(|err| DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to start watch task: {:?}", err)],
            })?;

        let rpc_client_cache = self.rpc_client_cache.clone();
        // TODO: Selectively verify that a batch of clients is valid in rpc client cache
        TASK_MANAGER
            .spawn(TaskName::AsyncFuse, |token| async move {
                let duration = Duration::from_secs(RPC_CLIENT_CACHE_CHECK_DURATION);
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(duration) => {
                            // If current rpc request is valid, we will hold this request and continue to use it,
                            // in this period, we just delete all the rpc client in the cache
                            rpc_client_cache.lock().await.clear();
                            debug!("Batch validate rpc client cache task is finished");
                        }
                        _ = token.cancelled() => {
                            warn!("Batch validate rpc client cache task is cancelled");
                            return;
                        }
                    }
                }
            })
            .await
            .map_err(|err| DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to start watch task: {:?}", err)],
            })?;

        Ok(())
    }

    /// Read the block from the distribute cache
    /// We will try to get the cache node with the block info, and create a rpc client to read this block from distribute cache
    pub async fn read_block(
        &self,
        ino: u64,
        block_id: u64,
        mtime: u64,
        block_size: u64,
    ) -> DatenLordResult<Block> {
        // Get the cache indexer id
        let block_path_id = get_block_path_id(ino, mtime, block_id, block_size);

        // Get the cache node with the block id
        let cache_node = self
            .cluster_manager
            .get_node(block_path_id)
            .await
            .map_err(|err| DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to get cache node: {:?}", err)],
            })?;
        let addr = format!("{}:{}", cache_node.ip(), cache_node.port());

        let (tx, rx) = flume::unbounded::<Result<FileBlockResponse, FileBlockRequest>>();
        // Send file block request
        let current_ring = self.cluster_manager.get_ring().await.map_err(|err| {
            DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to get ring: {:?}", err)],
            }
        })?;
        let block_request = FileBlockRequest {
            block_id: block_id,
            block_size: block_size,
            file_id: ino,
            block_version: mtime,
            hash_ring_version: current_ring.version(),
        };
        let packet = FileBlockPacket::new(&block_request, tx.clone());

        let start_time = tokio::time::Instant::now();

        {
            // Add a cache to hold the rpc client
            let mut rpc_client_cache = self.rpc_client_cache.lock().await;
            if rpc_client_cache.contains_key(&addr) {
                // If the rpc client is already in the cache, we will use it directly
                let rpc_client = rpc_client_cache.get(&addr).unwrap();
                // TODO: Test connection first, if current connection is not available, we will try to return quickly and delete this rpc client
                // But, it might cause a lot of latency, so we just check the connection and remove it when the connection is not available

                rpc_client.send_request(packet).await.map_err(|err| {
                    match err {
                        RpcError::Timeout(_) => {
                            // If the rpc client is timeout, we will remove it from the cache
                            rpc_client_cache.remove(&addr);
                        }
                        RpcError::InvalidRequest(_)
                        | RpcError::InvalidResponse(_)
                        | RpcError::InternalError(_) => {}
                    }

                    DatenLordError::DistributeCacheManagerErr {
                        context: vec![format!("Failed to send request: {:?}", err)],
                    }
                })?;
            } else {
                // If the rpc client is not in the cache, we will create a new one
                // Create rpc client from the cache node

                // Create rpc client from the cache node
                // TODO: put the rpc client into a pool, so we can use the rpc client
                //      to read the block from the cache node with keepalive
                let timeout_options = ClientTimeoutOptions {
                    read_timeout: Duration::from_secs(10),
                    write_timeout: Duration::from_secs(10),
                    task_timeout: Duration::from_secs(10),
                    keep_alive_timeout: Duration::from_secs(10),
                };
                let addr_clone = addr.clone();
                let connect_stream =
                    connect_timeout!(addr_clone, timeout_options.read_timeout).await?;
                let rpc_client =
                    RpcClient::<FileBlockPacket>::new(connect_stream, &timeout_options);
                rpc_client.start_recv();

                // Test connection first, if current connection is not available, we will try to return quickly
                rpc_client.ping().await.map_err(|err| {
                    DatenLordError::DistributeCacheManagerErr {
                        context: vec![format!("Failed to ping cache node: {:?}", err)],
                    }
                })?;

                // TODO: Change to other node?

                rpc_client.send_request(packet).await.map_err(|err| {
                    DatenLordError::DistributeCacheManagerErr {
                        context: vec![format!("Failed to send request: {:?}", err)],
                    }
                })?;

                // Add the rpc client to the cache
                rpc_client_cache.insert(addr.clone(), rpc_client);
            }
        }
        let elapsed = start_time.elapsed();
        error!("Read block from cache node: {} cost: {:?}", addr, elapsed);

        // Async read the block from the cache node
        match rx.recv_async().await {
            Ok(Ok(response)) => {
                // TODO: ignore hashring version is not match error, this case
                // will be processed in the future.

                // TODO: Update to new storage block
                return Ok(Block::from_slice(
                    u64_to_usize(response.block_size),
                    &response.data,
                ));
            }
            Ok(Err(err)) => {
                return Err(DatenLordError::DistributeCacheManagerErr {
                    context: vec![format!("Failed to read block: {:?}", err)],
                });
            }
            Err(_) => {
                return Err(DatenLordError::DistributeCacheManagerErr {
                    context: vec![format!("Failed to read block")],
                });
            }
        }
    }
}
