use std::sync::Arc;

use async_trait::async_trait;
use tokio::{select, sync::mpsc};
use tracing::{debug, error, info};

use crate::{
    async_fuse::{memfs::kv_engine::KVEngineType, util::usize_to_u64},
    common::{
        error::{DatenLordError, DatenLordResult},
        task_manager::{TaskName, TASK_MANAGER},
    },
    storage::distribute_cache::rpc::packet::Decode,
};

use super::{
    cluster::{
        cluster_manager::ClusterManager,
        node::{Node, NodeStatus},
    },
    config::DistributeCacheConfig,
    local_cache::{backend::FSBackend, block::MetaData, manager::BlockManager},
    rpc::{
        common::ServerTimeoutOptions,
        message::{FileBlockRequest, FileBlockResponse, ReqType, RespType, StatusCode},
        packet::{Encode, ReqHeader, RespHeader},
        server::{RpcServer, RpcServerConnectionHandler},
        workerpool::{Job, WorkerPool},
    },
};

/// The handler for the RPC file block request.
#[derive(Debug)]
pub struct FileBlockHandler {
    /// The request header.
    header: ReqHeader,
    /// The file block request.
    request: FileBlockRequest,
    /// The channel for sending the response.
    done_tx: mpsc::Sender<Vec<u8>>,
    /// Local cache manager
    local_cache_manager: Arc<BlockManager>,
    /// Cluster manager
    cluster_manager: Arc<ClusterManager>,
}

impl FileBlockHandler {
    /// Create a new file block handler.
    #[must_use]
    pub fn new(
        header: ReqHeader,
        request: FileBlockRequest,
        done_tx: mpsc::Sender<Vec<u8>>,
        local_cache_manager: Arc<BlockManager>,
        cluster_manager: Arc<ClusterManager>,
    ) -> Self {
        Self {
            header,
            request,
            done_tx,
            local_cache_manager,
            cluster_manager,
        }
    }
}

#[async_trait]
impl Job for FileBlockHandler {
    async fn run(&self) {
        let current_hash_ring_version = match self.cluster_manager.get_ring().await {
            Ok(ring) => ring.version(),
            Err(err) => {
                error!("Failed to get current hash ring version: {:?}", err);
                0
            }
        };

        // If the block is not found in the local cache and remote cache,
        // we need to return failed
        let mut file_block_resp = FileBlockResponse {
            file_id: self.request.file_id,
            block_id: self.request.block_id,
            block_size: self.request.block_size,
            block_version: self.request.block_version,
            hash_ring_version: current_hash_ring_version,
            status: StatusCode::NotFound,
            data: vec![],
        };

        // TODO: Check request hashring version
        // 1. Try to read block from local cache
        // 2. If not found, read from remote cache
        let meta_data = MetaData::new(
            self.request.file_id,
            self.request.block_version,
            self.request.block_id,
            self.request.block_size,
        );
        let block = self.local_cache_manager.read(meta_data).await;
        if let Ok(block) = block {
            if let Some(block) = block {
                // Check version
                if block.get_meta_data().get_version() != self.request.block_version {
                    // If the version is not matched, we need to return failed
                    file_block_resp = FileBlockResponse {
                        file_id: self.request.file_id,
                        block_id: self.request.block_id,
                        block_size: self.request.block_size,
                        block_version: self.request.block_version,
                        hash_ring_version: current_hash_ring_version,
                        status: StatusCode::VersionMismatch,
                        data: vec![],
                    };
                } else {
                    // Prepare response body
                    file_block_resp = FileBlockResponse {
                        file_id: self.request.file_id,
                        block_id: self.request.block_id,
                        block_size: self.request.block_size,
                        block_version: self.request.block_version,
                        hash_ring_version: current_hash_ring_version,
                        status: StatusCode::Success,
                        data: block.get_data(),
                    };
                }
            }
        }

        // Prepare response body
        let resp_body = file_block_resp.encode();
        // Prepare response header
        let resp_header = RespHeader {
            seq: self.header.seq,
            op: RespType::FileBlockResponse.to_u8(),
            len: usize_to_u64(resp_body.len()),
        };
        let mut resp_buffer = resp_header.encode();
        // Combine response header and body
        resp_buffer.extend_from_slice(&resp_body);

        // Send response to the done channel
        match self.done_tx.send(resp_buffer).await {
            Ok(()) => {
                debug!("Sent response to done channel");
            }
            Err(err) => {
                error!("Failed to send response to done channel: {:?}", err);
            }
        }
    }
}

/// The file block handler for the RPC server.
#[derive(Clone, Debug)]
pub struct BlockHandler {
    /// The worker pool for the RPC server.
    worker_pool: Arc<WorkerPool>,
    /// Local cache manager
    local_cache_manager: Arc<BlockManager>,
    /// Cluster manager
    cluster_manager: Arc<ClusterManager>,
}

impl BlockHandler {
    /// Create a new file block RPC server handler.
    #[must_use]
    pub fn new(
        worker_pool: Arc<WorkerPool>,
        local_cache_manager: Arc<BlockManager>,
        cluster_manager: Arc<ClusterManager>,
    ) -> Self {
        Self {
            worker_pool,
            local_cache_manager,
            cluster_manager,
        }
    }
}

#[async_trait]
impl RpcServerConnectionHandler for BlockHandler {
    async fn dispatch(
        &self,
        req_header: ReqHeader,
        req_buffer: &[u8],
        done_tx: mpsc::Sender<Vec<u8>>,
    ) {
        // Dispatch the handler for the connection
        if let Ok(req_type) = ReqType::from_u8(req_header.op) {
            if let ReqType::FileBlockRequest = req_type {
                // Try to read the request body
                // Decode the request body
                let req_body = match FileBlockRequest::decode(req_buffer) {
                    Ok(req) => req,
                    Err(err) => {
                        debug!("Failed to decode file block request: {:?}", err);
                        return;
                    }
                };

                debug!(
                    "FileBlockRpcServerHandler: Received file block request: {:?}",
                    req_body
                );

                // File block request
                // Submit the handler to the worker pool
                // When the handler is done, send the response to the done channel
                // Response need to contain the response header and body
                let handler = FileBlockHandler::new(
                    req_header,
                    req_body,
                    done_tx.clone(),
                    Arc::clone(&self.local_cache_manager),
                    Arc::clone(&self.cluster_manager),
                );
                if let Ok(()) = self
                    .worker_pool
                    .submit_job(Box::new(handler))
                    .map_err(|err| {
                        debug!("Failed to submit job: {:?}", err);
                    })
                {
                    debug!("Submitted job to worker pool");
                }
            } else {
                debug!(
                    "FileBlockRpcServerHandler: Inner request type is not matched: {:?}",
                    req_header.op
                );
            }
        }
    }
}

/// The distribute cache manager.
#[derive(Debug)]
pub struct DistributeCacheManager {
    /// Local config
    config: DistributeCacheConfig,
    /// Local cache manager, we will use it to manage the local cache, and export manaually data for the cache
    /// We will share it in rpc request
    local_cache_manager: Arc<BlockManager>,
    /// The distribute cache cluster
    /// We will serve as a standalone server for cluster manager, and read current status from it
    cluster_manager: Arc<ClusterManager>,
}

impl DistributeCacheManager {
    /// Create a new distribute cache manager.
    pub fn new(kv_engine: Arc<KVEngineType>, config: &DistributeCacheConfig) -> Self {
        // Create local cache manager
        // Create a block manager in local fs
        let backend = Arc::new(FSBackend::default());
        let local_cache_manager = Arc::new(BlockManager::new(backend));

        // Create a distribute cluster manager
        let init_current_node = Node::new(
            config.rpc_server_ip.clone(),
            config.rpc_server_port,
            0,
            NodeStatus::Initializing,
        );
        let cluster_manager = Arc::new(ClusterManager::new(kv_engine, init_current_node));

        Self {
            config: config.clone(),
            local_cache_manager,
            cluster_manager,
        }
    }

    /// Start the distribute cache manager.
    pub async fn start(&self) -> DatenLordResult<()> {
        // 1. start cluster manager
        let cluster_manager_clone = Arc::clone(&self.cluster_manager);
        TASK_MANAGER
            .spawn(TaskName::DistributeCacheManager, |token| async move {
                loop {
                    select! {
                        biased;
                        () = token.cancelled() => {
                            // Higher priority
                            debug!("Cluster manager stopped by token");
                            break;
                        }
                        res = cluster_manager_clone.run() => {
                            if let Err(err) = res {
                                error!("Cluster manager failed: {:?}", err);
                                break;
                            }
                        }
                    }
                }
            })
            .await
            .map_err(|err| DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to start cluster manager task: {:?}", err)],
            })?;

        // 2. Start the rpc server, ready to serve
        let addr = format!(
            "{}:{}",
            self.config.rpc_server_ip, self.config.rpc_server_port
        );

        let local_cache_manager_clone = Arc::clone(&self.local_cache_manager);
        let cluster_manager_clone = Arc::clone(&self.cluster_manager);
        TASK_MANAGER
            .spawn(TaskName::DistributeCacheManager, |token| async move {
                // Create rpc server
                // Default workpool for rpc server
                // Default worker for file block handler is 10, default jobs is 100
                let pool = Arc::new(WorkerPool::new(10, 100));
                let handler = BlockHandler::new(
                    Arc::clone(&pool),
                    local_cache_manager_clone,
                    cluster_manager_clone,
                );
                let server_timeout_options = ServerTimeoutOptions::default();
                // Create a new rpc server with max 100 workers and 1000 jobs
                let mut rpc_server = RpcServer::new(&server_timeout_options, 100, 1000, handler);
                match rpc_server.listen(&addr).await {
                    Ok(()) => {
                        info!("Rpc server started on: {}", addr);
                    }
                    Err(err) => {
                        error!("Failed to start rpc server: {:?}", err);
                        return;
                    }
                }

                loop {
                    select! {
                        biased;
                        () = token.cancelled() => {
                            // Higher priority
                            rpc_server.stop();
                            debug!("Rpc server stopped by token");
                            break;
                        }
                    }
                }
            })
            .await
            .map_err(|err| DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to start rpc server task: {:?}", err)],
            })?;

        Ok(())
    }
}
