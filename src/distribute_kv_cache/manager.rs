use std::{fmt, sync::Arc};

use async_trait::async_trait;
use bytes::{Buf, BufMut, BytesMut};
use clippy_utilities::Cast;
use opendal::{
    layers::{ConcurrentLimitLayer, RetryLayer},
    services::S3,
    Operator,
};
use serde::{de::DeserializeOwned, Serialize};
use tokio::{select, sync::mpsc};
use tracing::{debug, error, info};

use crate::{
    async_fuse::util::{u32_to_usize, usize_to_u64},
    common::{
        error::{DatenLordError, DatenLordResult},
        task_manager::{TaskName, TASK_MANAGER},
    },
    config::StorageS3Config,
    distribute_kv_cache::{
        rpc::{
            message::{
                KVBlockBatchPutRequest, KVBlockBatchPutResponse, KVBlockGetRequest,
                KVCacheIdAllocateRequest, KVCacheIdAllocateResponse,
                KVCacheIndexBatchInsertRequest, KVCacheIndexInsertResponse,
                KVCacheIndexMatchResponse, KVCacheIndexRemoveRequest, KVCacheIndexRemoveResponse,
            },
            packet::{self, EncodeLarge},
        },
        server_cache::block::Block,
    },
    encode_to_buf,
    fs::kv_engine::KVEngineType,
};

use super::{
    cluster::{
        cluster_manager::ClusterManager,
        node::{Node, NodeStatus},
    },
    config::DistributeCacheConfig,
    rpc::{
        common::ServerTimeoutOptions,
        message::{
            FileBlockRequest, FileBlockResponse, KVBlockGetResponse, KVCacheIndexMatchRequest,
            ReqType, RespType, StatusCode,
        },
        packet::{Encode, ReqHeader, RespHeader},
        server::{RpcServer, RpcServerConnectionHandler},
        workerpool::{Job, WorkerPool},
    },
    server_cache::{
        backend::S3Backend,
        block::MetaData,
        manager::{BlockManager, IndexManager, KVBlockManager},
    },
};

/// Default radix index key
#[allow(dead_code)]
const DEFAULT_INDEX_KEY: &str = "kvcacheindex";

/// Helper function to generate kv cache index value
fn generate_kv_cache_index_value(block_id: u64, offset: u64, size: u64, addr: &str) -> String {
    format!("{block_id}_{offset}_{size}_{addr}")
}

/// Helper function to parse kv cache index value
#[allow(clippy::indexing_slicing)]
fn parse_kv_cache_index_value(value: &str) -> DatenLordResult<(u64, u64, u64, String)> {
    let parts: Vec<&str> = value.split('_').collect();

    if parts.len() < 4 {
        return Err(DatenLordError::CacheClusterErr {
            context: vec![format!(
                "Invalid value format: expected at least 4 parts, got {}",
                parts.len()
            )],
        });
    }

    let block_id = parts[0]
        .parse()
        .map_err(|err| DatenLordError::CacheClusterErr {
            context: vec![format!("Failed to parse block id: {:?}", err)],
        })?;
    let offset = parts[1]
        .parse()
        .map_err(|err| DatenLordError::CacheClusterErr {
            context: vec![format!("Failed to parse offset: {:?}", err)],
        })?;
    let size = parts[2]
        .parse()
        .map_err(|err| DatenLordError::CacheClusterErr {
            context: vec![format!("Failed to parse size: {:?}", err)],
        })?;
    let addr = parts[3].to_owned();

    Ok((block_id, offset, size, addr))
}

/// The handler for the RPC file block request.
#[derive(Debug)]
pub struct FileBlockHandler {
    /// The request header.
    header: ReqHeader,
    /// The file block request.
    request: FileBlockRequest,
    /// The channel for sending the response.
    done_tx: mpsc::Sender<Vec<bytes::Bytes>>,
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
        done_tx: mpsc::Sender<Vec<bytes::Bytes>>,
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

        // FIXME: Check request hashring version
        let meta_data = MetaData::new(
            self.request.file_id,
            self.request.block_version,
            self.request.block_id,
            self.request.block_size,
        );
        // error!("current file block request: {:?}", self.request);
        let block = Box::pin(self.local_cache_manager.read(meta_data)).await;
        if let Ok(Some(block)) = block {
            // Check version
            if block.get_meta_data().get_version() == self.request.block_version {
                // Prepare response body
                file_block_resp = FileBlockResponse {
                    file_id: self.request.file_id,
                    block_id: self.request.block_id,
                    block_size: self.request.block_size,
                    block_version: self.request.block_version,
                    hash_ring_version: current_hash_ring_version,
                    status: StatusCode::Success,
                    // FIXME: Change response to Bytes
                    data: block.get_data().to_vec(),
                };
            } else {
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
            }
        }

        // Prepare response body
        let resp_body_buffer = BytesMut::new();
        encode_to_buf!(resp_body_buffer.clone(), &file_block_resp);
        // Prepare response header
        let resp_header = RespHeader {
            seq: self.header.seq,
            op: RespType::FileBlockResponse.into(),
            len: usize_to_u64(resp_body_buffer.len()),
        };
        let mut resp_header_buffer = BytesMut::new();
        resp_header.encode(&mut resp_header_buffer);
        // Combine response header and body
        resp_header_buffer.extend_from_slice(&resp_body_buffer);

        // Send response to the done channel
        match self.done_tx.send(vec![resp_header_buffer.freeze()]).await {
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
        req_buffer: BytesMut,
        done_tx: mpsc::Sender<Vec<bytes::Bytes>>,
    ) {
        // Dispatch the handler for the connection
        if let Ok(req_type) = ReqType::try_from(req_header.op) {
            if let ReqType::FileBlockRequest = req_type {
                // Try to read the request body
                // Decode the request body
                let reader = req_buffer.reader();
                let req_body: FileBlockRequest = match bincode::deserialize_from(reader) {
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

/// The handler for the RPC kv cache block request.
#[derive(Debug)]
pub struct KVBlockHandler {
    /// The request header.
    header: ReqHeader,
    /// The request body.
    request: bytes::Bytes,
    /// The channel for sending the response.
    done_tx: mpsc::Sender<Vec<bytes::Bytes>>,
    /// Local index manager
    cache_manager: Arc<KVBlockManager>,
}

impl KVBlockHandler {
    /// Create a new kv block handler.
    #[must_use]
    pub fn new(
        header: ReqHeader,
        request: bytes::Bytes,
        done_tx: mpsc::Sender<Vec<bytes::Bytes>>,
        cache_manager: Arc<KVBlockManager>,
    ) -> Self {
        Self {
            header,
            request,
            done_tx,
            cache_manager,
        }
    }

    /// Handle the block get request.
    async fn handle_block_get_request(
        &self,
        req_buffer: bytes::Bytes,
        resp_header_buffer: &mut BytesMut,
        resp_bytes_vec: &mut Vec<bytes::Bytes>,
    ) {
        let reader = req_buffer.reader();
        let req_body: KVBlockGetRequest = match bincode::deserialize_from(reader) {
            Ok(req) => req,
            Err(err) => {
                debug!("Failed to decode file block request: {:?}", err);
                return;
            }
        };

        let metadata = MetaData::new(req_body.kv_cache_id, 0, 0, 0);
        let block_result = self.cache_manager.read(metadata).await;
        let mut response = KVBlockGetResponse {
            kv_cache_id: req_body.kv_cache_id,
            block_size: req_body.block_size,
            status: StatusCode::InternalError,
            data: bytes::Bytes::new(),
        };

        if let Ok(Some(block)) = block_result {
            let data = block.read().map_or(bytes::Bytes::new(), |b| b.get_data());
            if usize_to_u64(data.len()) == req_body.block_size {
                response = KVBlockGetResponse {
                    kv_cache_id: req_body.kv_cache_id,
                    block_size: req_body.block_size,
                    status: StatusCode::Success,
                    data,
                };
            }
        }

        let mut resp_body_buffer = resp_header_buffer.split_off(packet::RESP_HEADER_SIZE.cast());
        let (body_len, extra_data) = response.encode_large_data(&mut resp_body_buffer);

        let resp_header = RespHeader {
            seq: self.header.seq,
            op: RespType::KVBlockGetResponse.into(),
            len: body_len,
        };
        resp_header.encode(resp_header_buffer);
        resp_header_buffer.unsplit(resp_body_buffer);

        // FIXME: This is a hack to make the response buffer work
        resp_bytes_vec.push(resp_header_buffer.clone().freeze());
        resp_bytes_vec.extend_from_slice(&extra_data);
    }

    /// Handle the block batch put request.
    async fn handle_block_batch_put_request(
        &self,
        req_buffer: bytes::Bytes,
        resp_header_buffer: &mut BytesMut,
        resp_bytes_vec: &mut Vec<bytes::Bytes>,
    ) {
        let reader = req_buffer.reader();
        let req_body: KVBlockBatchPutRequest = match bincode::deserialize_from(reader) {
            Ok(req) => req,
            Err(err) => {
                debug!("Failed to decode file block request: {:?}", err);
                return;
            }
        };

        let mut success_ids = Vec::new();
        let mut failed_ids = Vec::new();

        for block in req_body.blocks {
            let meta_data = MetaData::new(block.kv_cache_id, 0, 0, 0);
            let kv_block = Block::new(meta_data, block.data);
            match self.cache_manager.write(kv_block).await {
                Ok(()) => success_ids.push(block.kv_cache_id),
                Err(err) => {
                    error!("Failed to put block into cache: {:?}", err);
                    failed_ids.push(block.kv_cache_id);
                }
            }
        }

        let response = KVBlockBatchPutResponse {
            block_size: req_body.batch_size,
            success_batch_size: usize_to_u64(success_ids.len()),
            success_kv_cache_ids: success_ids,
            failed_batch_size: usize_to_u64(failed_ids.len()),
            failed_kv_cache_ids: failed_ids,
        };

        let resp_body_buffer = resp_header_buffer.split_off(packet::RESP_HEADER_SIZE.cast());
        encode_to_buf!(resp_body_buffer.clone(), &response);

        let resp_header = RespHeader {
            seq: self.header.seq,
            op: RespType::KVBlockBatchPutResponse.into(),
            len: usize_to_u64(resp_body_buffer.len()),
        };
        resp_header.encode(resp_header_buffer);
        resp_header_buffer.unsplit(resp_body_buffer);
        resp_bytes_vec.push(resp_header_buffer.clone().freeze());
    }
}

#[async_trait]
impl Job for KVBlockHandler {
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::wildcard_enum_match_arm)]
    async fn run(&self) {
        if let Ok(req_type) = ReqType::try_from(self.header.op) {
            let mut resp_header_buffer = BytesMut::with_capacity(40 * 1024 * 1024);
            let mut resp_bytes_vec: Vec<bytes::Bytes> = vec![];
            let req_buffer = self.request.clone();

            match req_type {
                ReqType::KVBlockGetRequest => {
                    self.handle_block_get_request(
                        req_buffer,
                        &mut resp_header_buffer,
                        &mut resp_bytes_vec,
                    )
                    .await;
                }
                ReqType::KVBlockBatchPutRequest => {
                    self.handle_block_batch_put_request(
                        req_buffer,
                        &mut resp_header_buffer,
                        &mut resp_bytes_vec,
                    )
                    .await;
                }
                _ => {
                    debug!(
                        "KVBlockHandler: Inner request type is not matched: {:?}",
                        self.header.op
                    );
                }
            }

            if let Err(err) = self.done_tx.send(resp_bytes_vec).await {
                error!("Failed to send response to done channel: {:?}", err);
            }
        }
    }
}

/// The handler for the RPC kv cache index  request.
#[derive(Debug)]
pub struct IndexHandler<K> {
    /// The request header.
    header: ReqHeader,
    /// The request body.
    request: bytes::Bytes,
    /// The channel for sending the response.
    done_tx: mpsc::Sender<Vec<bytes::Bytes>>,
    /// Local index manager
    index_manager: Arc<IndexManager<K>>,
}

impl<K> IndexHandler<K>
where
    K: num::Num + Eq + Send + Sync + Clone + fmt::Debug + 'static,
    Vec<K>: radix_trie::TrieKey + Clone,
{
    /// Create a new index handler.
    #[must_use]
    pub fn new(
        header: ReqHeader,
        request: bytes::Bytes,
        done_tx: mpsc::Sender<Vec<bytes::Bytes>>,
        index_manager: Arc<IndexManager<K>>,
    ) -> Self {
        Self {
            header,
            request,
            done_tx,
            index_manager,
        }
    }
}

#[async_trait]
impl<K> Job for IndexHandler<K>
where
    K: num::Num + Eq + Send + Sync + Clone + fmt::Debug + Serialize + DeserializeOwned + 'static,
    Vec<K>: radix_trie::TrieKey + Clone,
{
    #[allow(clippy::wildcard_enum_match_arm)]
    async fn run(&self) {
        if let Ok(req_type) = ReqType::try_from(self.header.op) {
            let mut resp_header_buffer = BytesMut::new();
            match req_type {
                ReqType::KVCacheIdAllocateRequest => {
                    self.handle_id_allocate(&mut resp_header_buffer);
                }
                ReqType::KVCacheIndexBatchInsertRequest => {
                    self.handle_index_batch_insert(&mut resp_header_buffer);
                }
                ReqType::KVCacheIndexRemoveRequest => {
                    self.handle_index_remove(&mut resp_header_buffer);
                }
                ReqType::KVCacheIndexMatchRequest => {
                    self.handle_index_match(&mut resp_header_buffer);
                }
                _ => {
                    debug!(
                        "IndexHandler: Inner request type is not matched: {:?}",
                        self.header.op
                    );
                }
            }

            if let Err(err) = self.done_tx.send(vec![resp_header_buffer.freeze()]).await {
                error!("Failed to send response to done channel: {:?}", err);
            }
        }
    }
}

impl<K> IndexHandler<K>
where
    K: num::Num + Eq + Send + Sync + Clone + fmt::Debug + Serialize + DeserializeOwned + 'static,
    Vec<K>: radix_trie::TrieKey + Clone,
{
    /// Build response
    fn build_response<T: Serialize>(&self, buf: &mut BytesMut, op_type: RespType, body: &T) {
        let body_buf = BytesMut::new();
        encode_to_buf!(body_buf.clone(), body);

        let resp_header = RespHeader {
            seq: self.header.seq,
            op: op_type.into(),
            len: usize_to_u64(body_buf.len()),
        };

        resp_header.encode(buf);
        buf.extend_from_slice(&body_buf);
    }

    /// Handle id allocate request
    fn handle_id_allocate(&self, buf: &mut BytesMut) {
        let reader = self.request.clone().reader();
        let req_body: KVCacheIdAllocateRequest = match bincode::deserialize_from(reader) {
            Ok(req) => req,
            Err(err) => {
                debug!("Failed to decode file block request: {:?}", err);
                return;
            }
        };

        let block_id = self.index_manager.allocate_id();
        let resp = KVCacheIdAllocateResponse {
            block_size: req_body.block_size,
            kv_cache_id: block_id,
        };

        self.build_response(buf, RespType::KVCacheIdAllocateResponse, &resp);
    }

    /// Handle index batch insert request
    #[allow(clippy::pattern_type_mismatch)]
    fn handle_index_batch_insert(&self, buf: &mut BytesMut) {
        let reader = self.request.clone().reader();
        let req_body: KVCacheIndexBatchInsertRequest<K> = match bincode::deserialize_from(reader) {
            Ok(req) => req,
            Err(err) => {
                debug!("Failed to decode index request: {:?}", err);
                return;
            }
        };

        let node_address = match String::from_utf8(req_body.node_address) {
            Ok(addr) => Some(addr),
            Err(err) => {
                error!("Failed to convert node address: {:?}", err);
                None
            }
        };

        let mut response = req_body.indexes.first().map_or_else(
            || KVCacheIndexInsertResponse {
                block_size: 0,
                kv_cache_id: 0,
                status: StatusCode::InternalError,
            },
            |index| KVCacheIndexInsertResponse {
                block_size: index.block_size,
                kv_cache_id: index.kv_cache_id,
                status: StatusCode::Success,
            },
        );

        for index in req_body.indexes {
            let key = index.kv_cache_key;
            let value = match &node_address {
                Some(addr) => {
                    generate_kv_cache_index_value(index.kv_cache_id, index.offset, index.size, addr)
                }
                None => {
                    response = KVCacheIndexInsertResponse {
                        block_size: index.block_size,
                        kv_cache_id: index.kv_cache_id,
                        status: StatusCode::InternalError,
                    };
                    break;
                }
            };
            self.index_manager.insert(key, value);
        }

        self.build_response(buf, RespType::KVCacheIndexInsertResponse, &response);
    }

    /// Remove index from index manager.
    fn handle_index_remove(&self, buf: &mut BytesMut) {
        let reader = self.request.clone().reader();
        let req_body: KVCacheIndexRemoveRequest<K> = match bincode::deserialize_from(reader) {
            Ok(req) => req,
            Err(err) => {
                debug!("Failed to decode index request: {:?}", err);
                return;
            }
        };

        self.index_manager.remove(&req_body.kv_cache_key);
        let resp = KVCacheIndexRemoveResponse {
            block_size: req_body.block_size,
            status: StatusCode::Success,
        };

        self.build_response(buf, RespType::KVCacheIndexRemoveResponse, &resp);
    }

    /// Match index from index manager.
    fn handle_index_match(&self, buf: &mut BytesMut) {
        let reader = self.request.clone().reader();
        let req_body: KVCacheIndexMatchRequest<K> = match bincode::deserialize_from(reader) {
            Ok(req) => req,
            Err(err) => {
                debug!("Failed to decode index request: {:?}", err);
                return;
            }
        };

        let response = match self.index_manager.get_longest_kv(&req_body.kv_cache_key) {
            Some((key, value)) => match parse_kv_cache_index_value(&value) {
                Ok((id, offset, size, addr)) => KVCacheIndexMatchResponse {
                    block_size: req_body.block_size,
                    kv_cache_key_len: usize_to_u64(key.len()),
                    kv_cache_id: id,
                    offset,
                    size,
                    status: StatusCode::Success,
                    node_address: addr.into_bytes(),
                },
                Err(_) => KVCacheIndexMatchResponse {
                    block_size: req_body.block_size,
                    ..Default::default()
                },
            },
            None => KVCacheIndexMatchResponse {
                block_size: req_body.block_size,
                status: StatusCode::NotFound,
                ..Default::default()
            },
        };

        self.build_response(buf, RespType::KVCacheIndexMatchResponse, &response);
    }
}

/// The kv cache handler for the RPC server.
#[derive(Clone, Debug)]
pub struct KVCacheHandler<K> {
    /// The worker pool for the RPC server.
    worker_pool: Arc<WorkerPool>,
    /// Local cache manager
    cache_manager: Arc<KVBlockManager>,
    /// Local index manager
    index_manager: Arc<IndexManager<K>>,
}

impl<K> KVCacheHandler<K>
where
    K: num::Num + Eq + Send + Sync + Clone + fmt::Debug + 'static,
    Vec<K>: radix_trie::TrieKey + Clone,
{
    /// Create a new file kv cache RPC server handler.
    #[must_use]
    pub fn new(
        worker_pool: Arc<WorkerPool>,
        cache_manager: Arc<KVBlockManager>,
        index_manager: Arc<IndexManager<K>>,
    ) -> Self {
        Self {
            worker_pool,
            cache_manager,
            index_manager,
        }
    }
}

#[async_trait]
impl<K> RpcServerConnectionHandler for KVCacheHandler<K>
where
    K: num::Num + Eq + Send + Sync + Clone + fmt::Debug + Serialize + DeserializeOwned + 'static,
    Vec<K>: radix_trie::TrieKey + Clone,
{
    #[allow(clippy::wildcard_enum_match_arm)]
    async fn dispatch(
        &self,
        req_header: ReqHeader,
        req_buffer: BytesMut,
        done_tx: mpsc::Sender<Vec<bytes::Bytes>>,
    ) {
        // Dispatch the handler for the connection
        if let Ok(req_type) = ReqType::try_from(req_header.op) {
            // Dispatch current kv cache request to index or block handler.
            match req_type {
                ReqType::KVCacheIdAllocateRequest
                | ReqType::KVCacheIndexBatchInsertRequest
                | ReqType::KVCacheIndexRemoveRequest
                | ReqType::KVCacheIndexMatchRequest => {
                    // Dispatch the index handler
                    let handler = IndexHandler::new(
                        req_header,
                        req_buffer.freeze(),
                        done_tx,
                        Arc::clone(&self.index_manager),
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
                }
                ReqType::KVBlockGetRequest | ReqType::KVBlockBatchPutRequest => {
                    // Dispatch the block handler
                    let handler = KVBlockHandler::new(
                        req_header,
                        req_buffer.freeze(),
                        done_tx,
                        Arc::clone(&self.cache_manager),
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
                }
                _ => {
                    debug!(
                        "KVCacheHandler: Request type is not matched: {:?}",
                        req_header.op
                    );
                }
            }
        }
    }
}

/// The distribute cache manager.
#[derive(Debug)]
pub struct DistributeCacheManager<K> {
    /// Local config
    config: DistributeCacheConfig,
    /// Local cache manager, we will use it to manage the local cache, and export manaually data for the cache
    /// We will share it in rpc request
    /// FIXME: introduce block manager trait to support different block manager.
    cache_manager: Arc<KVBlockManager>,
    /// Local index manager, we will use it to manage the local index, and export manaually data for the cache
    /// We will share it in rpc request
    index_manager: Arc<IndexManager<K>>,
    /// The distribute cache cluster
    /// We will serve as a standalone server for cluster manager, and read current status from it
    cluster_manager: Arc<ClusterManager>,
}

impl<K> DistributeCacheManager<K>
where
    K: num::Num + Eq + Send + Sync + Clone + fmt::Debug + Serialize + DeserializeOwned + 'static,
    Vec<K>: radix_trie::TrieKey + Clone,
{
    /// Create a new distribute cache manager.
    pub fn new(
        kv_engine: Arc<KVEngineType>,
        config: &DistributeCacheConfig,
        backend_config: &StorageS3Config,
    ) -> Self {
        // Create local cache manager
        // Create a block manager in local fs

        let mut builder = S3::default();
        builder
            .endpoint(&backend_config.endpoint_url)
            .access_key_id(&backend_config.access_key_id)
            .secret_access_key(&backend_config.secret_access_key)
            .bucket(&backend_config.bucket_name);

        // Init region
        if let Some(region) = backend_config.region.clone() {
            builder.region(region.as_str());
        } else {
            // Auto detect region
            builder.region("auto");
        }

        // For aws s3 issue: https://repost.aws/questions/QU_F-UC6-fSdOYzp-gZSDTvQ/receiving-s3-503-slow-down-responses
        // 3,500 PUT/COPY/POST/DELETE or 5,500 GET/HEAD requests per second per prefix in a bucket
        let valid_max_concurrent_requests =
            backend_config.max_concurrent_requests.map_or(1000, |v| v);

        let conncurrency_layer =
            ConcurrentLimitLayer::new(valid_max_concurrent_requests.to_owned());
        let retry_layer = RetryLayer::new();

        #[allow(clippy::unwrap_used)]
        let operator = Operator::new(builder)
            .unwrap()
            .layer(conncurrency_layer)
            .layer(retry_layer)
            .finish();
        let backend = Arc::new(S3Backend::new(operator));

        let cache_manager = Arc::new(KVBlockManager::new(u32_to_usize(config.capacity), backend));
        // Init empty index manager, only master node will fill this struct and use it.
        let index_manager = Arc::new(IndexManager::new());

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
            cache_manager,
            index_manager,
            cluster_manager,
        }
    }

    /// Start the distribute cache manager.
    #[allow(clippy::pattern_type_mismatch)] // for `tokio::select!`
    #[allow(clippy::never_loop)]
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

        let cache_manager_clone = Arc::clone(&self.cache_manager);
        let index_manager_clone = Arc::clone(&self.index_manager);
        TASK_MANAGER
            .spawn(TaskName::DistributeCacheManager, |token| async move {
                // Create rpc server
                // Default workpool for rpc server
                // Default worker for file block handler is 10, default jobs is 100
                let pool = Arc::new(WorkerPool::new(64, 1000));
                let handler = KVCacheHandler::new(
                    Arc::clone(&pool),
                    cache_manager_clone,
                    index_manager_clone,
                );
                let server_timeout_options = ServerTimeoutOptions::default();
                // Create a new rpc server with max 100 workers and 1000 jobs
                let mut rpc_server = RpcServer::new(&server_timeout_options, 64, 1000, handler);
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
