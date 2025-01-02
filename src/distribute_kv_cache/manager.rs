use std::{fmt, sync::Arc};

use async_trait::async_trait;
use bytes::BytesMut;
use clippy_utilities::Cast;
use opendal::{
    layers::{ConcurrentLimitLayer, RetryLayer},
    services::S3,
    Operator,
};
use tokio::{select, sync::mpsc};
use tracing::{debug, error, info};

use crate::{
    async_fuse::util::usize_to_u64,
    common::{
        error::{DatenLordError, DatenLordResult},
        task_manager::{TaskName, TASK_MANAGER},
    },
    config::StorageS3Config,
    distribute_kv_cache::{
        local_cache::block::Block,
        rpc::{
            message::{
                KVBlockBatchPutRequest, KVBlockBatchPutResponse, KVBlockGetRequest,
                KVCacheIdAllocateRequest, KVCacheIdAllocateResponse,
                KVCacheIndexBatchInsertRequest, KVCacheIndexInsertResponse,
                KVCacheIndexMatchResponse, KVCacheIndexRemoveRequest, KVCacheIndexRemoveResponse,
            },
            packet::{self, Decode},
        },
    },
    fs::kv_engine::KVEngineType,
};

use super::{
    cluster::{
        cluster_manager::ClusterManager,
        node::{Node, NodeStatus},
    },
    config::DistributeCacheConfig,
    local_cache::{
        backend::S3Backend,
        block::MetaData,
        manager::{BlockManager, IndexManager, KVBlockManager},
    },
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
};

/// Default radix index key
#[allow(dead_code)]
const DEFAULT_INDEX_KEY: &str = "kvcacheindex";

/// Helper function to generate kv cache index value
#[allow(dead_code)]
fn generate_kv_cache_index_value(block_id: u64, offset: u64, size: u64, addr: &str) -> String {
    format!("{}_{}_{}_{}", block_id, offset, size, addr)
}

/// Helper function to parse kv cache index value
#[allow(dead_code)]
fn parse_kv_cache_index_value(value: &str) -> DatenLordResult<(u64, u64, u64, String)> {
    let parts: Vec<&str> = value.split('_').collect();
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
    let addr = parts[3].to_string();

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

        // TODO: Check request hashring version
        // 1. Try to read block from local cache
        // 2. If not found, read from remote cache
        let meta_data = MetaData::new(
            self.request.file_id,
            self.request.block_version,
            self.request.block_id,
            self.request.block_size,
        );
        // error!("current file block request: {:?}", self.request);
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
                        // 20241210 TODO: Need to return data
                        // data: block.get_data(),
                        data: vec![]
                    };
                }
            }
        }

        // error!("current file block response: file_id: {}, block_id: {}, block_size: {}, block_version: {}, hash_ring_version: {}, status: {:?}",
        // file_block_resp.file_id, file_block_resp.block_id, file_block_resp.block_size, file_block_resp.block_version, file_block_resp.hash_ring_version, file_block_resp.status);

        // error!("current file block response data: {:?}", file_block_resp.data);

        // Prepare response body
        let mut resp_body_buffer = BytesMut::new();
        file_block_resp.encode(&mut resp_body_buffer);
        // Prepare response header
        let resp_header = RespHeader {
            seq: self.header.seq,
            op: RespType::FileBlockResponse.to_u8(),
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
        mut req_buffer: BytesMut,
        done_tx: mpsc::Sender<Vec<bytes::Bytes>>,
    ) {
        // Dispatch the handler for the connection
        if let Ok(req_type) = ReqType::from_u8(req_header.op) {
            if let ReqType::FileBlockRequest = req_type {
                // Try to read the request body
                // Decode the request body
                let req_body = match FileBlockRequest::decode(&mut req_buffer) {
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
}

#[async_trait]
impl Job for KVBlockHandler {
    /// KV block handler inner run.
    /// Support Block get and batch put.
    async fn run(&self) {
        // Get current request type
        if let Ok(req_type) = ReqType::from_u8(self.header.op) {
            let buffer_start = tokio::time::Instant::now();
            let mut resp_header_buffer = BytesMut::with_capacity(40*1024*1024);
            let mut resp_bytes_vec: Vec<bytes::Bytes> = vec![];
            let buffer_start_0 = buffer_start.elapsed();
            debug!("KVBlockHandler buffer_start BytesMut new: Time elapsed: {:?}", buffer_start_0);
            let req_buffer = self.request.clone();
            let req_header = &self.header;
            match req_type {
                ReqType::KVBlockGetRequest => {
                    // Try to read the request body
                    // Decode the request body
                    let start = tokio::time::Instant::now();
                    let req_body = match KVBlockGetRequest::decode_large_data(req_buffer) {
                        Ok(req) => req,
                        Err(err) => {
                            debug!("Failed to decode file block request: {:?}", err);
                            return;
                        }
                    };
                    let start_0 = start.elapsed();
                    debug!("KVBlockGetRequest decode: Time elapsed: {:?}", start_0);

                    // debug!("KVBlockGetRequest: Received request: {:?}", req_body);
                    let mut kv_block_get_resp = KVBlockGetResponse {
                        kv_cache_id: req_body.kv_cache_id,
                        block_size: req_body.block_size,
                        status: StatusCode::InternalError,
                        data: bytes::Bytes::new(),
                    };

                    let start_0_2 = start.elapsed();
                    debug!("KVBlockGetRequest decode: Time elapsed: {:?}", start_0_2);

                    // Get the block by id
                    let metadata = MetaData::new(req_body.kv_cache_id, 0, 0, 0);
                    let block = self.cache_manager.read(metadata).await;
                    if let Ok(block) = block {
                        if let Some(block) = block {
                            let data =  block.read().unwrap().get_data();
                            // let data = block.get_data();
                            if data.len() as u64 == req_body.block_size {
                                kv_block_get_resp = KVBlockGetResponse {
                                    kv_cache_id: req_body.kv_cache_id,
                                    block_size: req_body.block_size,
                                    status: StatusCode::InternalError,
                                    data: data,
                                };
                            } else {
                                kv_block_get_resp = KVBlockGetResponse {
                                    kv_cache_id: req_body.kv_cache_id,
                                    block_size: req_body.block_size,
                                    status: StatusCode::InternalError,
                                    data: bytes::Bytes::new(),
                                };
                            }
                        }
                    };
                    let start_1 = start.elapsed();
                    debug!("KVBlockGetRequest read block: Time elapsed: {:?}", start_1 - start_0_2);

                    // TODO: allocate enough buffer to store it.
                    // Prepare response body
                    // let mut resp_body_buffer = BytesMut::with_capacity(20*1024*1024);
                    // Get from header offset
                    let mut resp_body_buffer = resp_header_buffer.split_off(packet::RESP_HEADER_SIZE.cast());
                    // let mut resp_body_buffer = BytesMut::with_capacity(20*1024*1024);
                    // kv_block_get_resp.encode(&mut resp_body_buffer);
                    let (body_len, extra_data) = kv_block_get_resp.encode_large_data(&mut resp_body_buffer);
                    debug!("KVBlockGetRequest resp_body_buffer size: {:?}, capacity: {:?}", resp_body_buffer.len(), resp_body_buffer.capacity());

                    let start_2 = start.elapsed();
                    debug!("KVBlockGetRequest encode: Time elapsed: {:?}", start_2 - start_1);

                    // Prepare response header
                    let resp_header = RespHeader {
                        seq: req_header.seq,
                        op: RespType::KVBlockGetResponse.to_u8(),
                        len: body_len,
                    };
                    resp_header.encode(&mut resp_header_buffer);

                    let start_3 = start.elapsed();
                    debug!("KVBlockGetRequest encode header: Time elapsed: {:?}", start_3 - start_2);

                    // Combine response header and body
                    // resp_header_buffer.extend_from_slice(&resp_body_buffer);
                    resp_header_buffer.unsplit(resp_body_buffer);
                    resp_bytes_vec.push(resp_header_buffer.freeze());
                    resp_bytes_vec.extend_from_slice(&extra_data);

                    let start_4 = start.elapsed();
                    debug!("KVBlockGetRequest extend_from_slice: Time elapsed: {:?}", start_4 - start_3);
                }
                ReqType::KVBlockBatchPutRequest => {
                    let start = tokio::time::Instant::now();
                    // Try to read the request body
                    // Decode the request body
                    let req_body = match KVBlockBatchPutRequest::decode_large_data(req_buffer) {
                        Ok(req) => req,
                        Err(err) => {
                            debug!("Failed to decode file block request: {:?}", err);
                            return;
                        }
                    };
                    let start_0 = start.elapsed();
                    debug!("KVBlockBatchPutRequest decode: Time elapsed: {:?}", start_0);

                    // debug!("KVBlockBatchPutRequest: Received request: {:?}", req_body);
                    let mut success_ids = vec![];
                    let mut failed_ids = vec![];
                    for block in req_body.blocks.into_iter() {
                        let block_start = tokio::time::Instant::now();
                        let meta_data = MetaData::new(block.kv_cache_id, 0, 0, 0);
                        let kv_block = Block::new(meta_data, block.data);
                        let block_start_0 = block_start.elapsed();
                        debug!("KVBlockBatchPutRequest new block: Time elapsed: {:?}", block_start_0);
                        match self.cache_manager.write(kv_block).await {
                            Ok(_) => {
                                success_ids.push(block.kv_cache_id);
                                let block_start_1 = block_start.elapsed();
                                debug!("KVBlockBatchPutRequest write block: Time elapsed: {:?}", block_start_1 - block_start_0);
                            }
                            Err(err) => {
                                error!("Failed to put block into cache: {:?}", err);
                                failed_ids.push(block.kv_cache_id);
                            }
                        }
                    }
                    let start_1 = start.elapsed();
                    debug!("KVBlockBatchPutRequest Received request: Time elapsed: {:?}", start_1 - start_0);

                    debug!(
                        "KVBlockBatchPutRequest: Success ids: {:?}, Failed ids: {:?}",
                        success_ids, failed_ids
                    );

                    let kv_block_batch_put_resp = KVBlockBatchPutResponse {
                        block_size: req_body.batch_size,
                        success_batch_size: success_ids.len() as u64,
                        success_kv_cache_ids: success_ids,
                        failed_batch_size: failed_ids.len() as u64,
                        failed_kv_cache_ids: failed_ids,
                    };

                    // Prepare response body
                    // let mut resp_body_buffer = BytesMut::with_capacity(20*1024*1024);
                    let mut resp_body_buffer = resp_header_buffer.split_off(packet::RESP_HEADER_SIZE.cast());
                    kv_block_batch_put_resp.encode(&mut resp_body_buffer);
                    // Prepare response header
                    let resp_header = RespHeader {
                        seq: req_header.seq,
                        op: RespType::KVBlockBatchPutResponse.to_u8(),
                        len: usize_to_u64(resp_body_buffer.len()),
                    };
                    resp_header.encode(&mut resp_header_buffer);
                    let start_2 = start.elapsed();
                    debug!("KVBlockBatchPutRequest BytesMut new: Time elapsed: {:?} size {:?}", start_2, resp_header_buffer.len());

                    // Combine response header and body
                    // resp_header_buffer.extend_from_slice(&resp_body_buffer);
                    resp_header_buffer.unsplit(resp_body_buffer);
                    resp_bytes_vec.push(resp_header_buffer.freeze());
                    let start_3 = start.elapsed();
                    debug!("KVBlockBatchPutRequest extend_from_slice: Time elapsed: {:?}", start_3 - start_2);
                }
                _ => {
                    debug!(
                        "KVBlockHandler: Inner request type is not matched: {:?}",
                        self.header.op
                    );
                }
            }

            let start_to_vec = tokio::time::Instant::now();
            // // let resp_header_buffer_vec = resp_header_buffer.to_vec();

            // // Direct convert to vec
            // let resp_header_buffer_vec = unsafe {
            //     Vec::from_raw_parts(resp_header_buffer.as_mut_ptr(), resp_header_buffer.len(), resp_header_buffer.capacity())
            // };
            // // Avoid bytesmut drop this buffer.
            // std::mem::forget(resp_header_buffer);
            let start_to_vec_0 = start_to_vec.elapsed();
            debug!("KVBlockHandler to_vec: Time elapsed: {:?}", start_to_vec_0);

            // TODO: change to vectored data
            match self.done_tx.send(resp_bytes_vec).await {
            // match self.done_tx.send(vec![resp_header_buffer.freeze()]).await {
                Ok(()) => {
                    debug!("Sent response to done channel");
                }
                Err(err) => {
                    error!("Failed to send response to done channel: {:?}", err);
                }
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
    K: num::Num + Eq + Send + Sync + Clone + fmt::Debug + 'static,
    Vec<K>: radix_trie::TrieKey + Clone,
{
    /// Index handler inner run.
    /// Support Block id allocation and prefix index management.
    async fn run(&self) {
        // Get current request type
        if let Ok(req_type) = ReqType::from_u8(self.header.op) {
            let mut resp_header_buffer = BytesMut::new();
            let req_buffer = self.request.clone();
            let req_header = &self.header;
            match req_type {
                ReqType::KVCacheIdAllocateRequest => {
                    // Try to read the request body
                    // Decode the request body
                    let req_body = match KVCacheIdAllocateRequest::decode_large_data(req_buffer) {
                        Ok(req) => req,
                        Err(err) => {
                            debug!("Failed to decode file block request: {:?}", err);
                            return;
                        }
                    };

                    // debug!("KVCacheIdAllocateRequest: Received request: {:?}", req_body);

                    // Get next block id
                    let block_id = self.index_manager.allocate_id();
                    let kv_cache_id_allocate_resp = KVCacheIdAllocateResponse {
                        block_size: req_body.block_size,
                        kv_cache_id: block_id,
                    };

                    let mut resp_body_buffer = BytesMut::new();
                    kv_cache_id_allocate_resp.encode(&mut resp_body_buffer);
                    let resp_header = RespHeader {
                        seq: req_header.seq,
                        op: RespType::KVCacheIdAllocateResponse.to_u8(),
                        len: usize_to_u64(resp_body_buffer.len()),
                    };
                    resp_header.encode(&mut resp_header_buffer);
                    resp_header_buffer.extend_from_slice(&resp_body_buffer);
                }
                ReqType::KVCacheIndexBatchInsertRequest => {
                    // Try to read the request body
                    // Decode the request body
                    let req_body = match KVCacheIndexBatchInsertRequest::<K>::decode_large_data(req_buffer) {
                        Ok(req) => req,
                        Err(err) => {
                            debug!("Failed to decode index request: {:?}", err);
                            return;
                        }
                    };

                    debug!(
                        "KVCacheIndexInsertRequest: Received request: {:?}",
                        req_body
                    );

                    let node_address = match String::from_utf8(req_body.node_address) {
                        Ok(addr) => Some(addr),
                        Err(err) => {
                            // TODO: send error to client
                            error!("Failed to convert kv cache address to string: {:?}", err);
                            None
                        }
                    };

                    let indexes = req_body.indexes;
                    let mut kv_cache_index_insert_resp = match indexes.get(0) {
                        Some(index) => KVCacheIndexInsertResponse {
                            block_size: index.block_size,
                            kv_cache_id: index.kv_cache_id,
                            status: StatusCode::Success,
                        },
                        None => {
                            error!("Failed to get index 0 from KVCacheIndexInsertRequest");
                            KVCacheIndexInsertResponse {
                                block_size: 0,
                                kv_cache_id: 0,
                                status: StatusCode::InternalError,
                            }
                        }
                    };

                    for index in indexes {
                        let key = index.kv_cache_key;
                        debug!("KVCacheIndexInsertRequest: Insert key: {:?}", key);
                        let kv_cache_index_value = match node_address {
                            Some(ref address) => generate_kv_cache_index_value(
                                index.kv_cache_id,
                                index.offset,
                                index.size,
                                address,
                            ),
                            None => {
                                error!("Failed to get node address");
                                kv_cache_index_insert_resp = KVCacheIndexInsertResponse {
                                    block_size: index.block_size,
                                    kv_cache_id: index.kv_cache_id,
                                    status: StatusCode::InternalError,
                                };
                                break;
                            }
                        };
                        self.index_manager.insert(key, kv_cache_index_value);
                        kv_cache_index_insert_resp = KVCacheIndexInsertResponse {
                            block_size: index.block_size,
                            kv_cache_id: index.kv_cache_id,
                            status: StatusCode::Success,
                        };
                    }

                    // Prepare response body
                    let mut resp_body_buffer = BytesMut::new();
                    kv_cache_index_insert_resp.encode(&mut resp_body_buffer);
                    // Prepare response header
                    let resp_header = RespHeader {
                        seq: req_header.seq,
                        op: RespType::KVCacheIndexInsertResponse.to_u8(),
                        len: usize_to_u64(resp_body_buffer.len()),
                    };
                    resp_header.encode(&mut resp_header_buffer);
                    // Combine response header and body
                    resp_header_buffer.extend_from_slice(&resp_body_buffer);
                }
                ReqType::KVCacheIndexRemoveRequest => {
                    // Try to read the request body
                    // Decode the request body
                    let req_body = match KVCacheIndexRemoveRequest::decode_large_data(req_buffer) {
                        Ok(req) => req,
                        Err(err) => {
                            debug!("Failed to decode index request: {:?}", err);
                            return;
                        }
                    };

                    // debug!(
                    //     "KVCacheIndexRemoveRequest: Received request: {:?}",
                    //     req_body
                    // );

                    // Remove the key-value pair from the index
                    self.index_manager.remove(&req_body.kv_cache_key);

                    let kv_cache_index_remove_resp = KVCacheIndexRemoveResponse {
                        block_size: req_body.block_size,
                        status: StatusCode::Success,
                    };

                    // Prepare response body
                    let mut resp_body_buffer = BytesMut::new();
                    kv_cache_index_remove_resp.encode(&mut resp_body_buffer);
                    // Prepare response header
                    let resp_header = RespHeader {
                        seq: req_header.seq,
                        op: RespType::KVCacheIndexRemoveResponse.to_u8(),
                        len: usize_to_u64(resp_body_buffer.len()),
                    };
                    resp_header.encode(&mut resp_header_buffer);
                    // Combine response header and body
                    resp_header_buffer.extend_from_slice(&resp_body_buffer);
                }
                ReqType::KVCacheIndexMatchRequest => {
                    // Try to read the request body
                    // Decode the request body
                    let req_body = match KVCacheIndexMatchRequest::decode_large_data(req_buffer) {
                        Ok(req) => req,
                        Err(err) => {
                            debug!("Failed to decode index request: {:?}", err);
                            return;
                        }
                    };

                    debug!("KVCacheIndexMatchRequest: Received request: {:?}", req_body);

                    // Get the value by key from the index
                    let longest_kv = self.index_manager.get_longest_kv(&req_body.kv_cache_key);
                    let kv_cache_id_allocate_resp = match longest_kv {
                        Some((key, value)) => match parse_kv_cache_index_value(&value) {
                            Ok((block_id, offset, size, addr)) => {
                                debug!(
                                    "KVCacheIndexMatchRequest: Matched value: {:?}",
                                    value
                                );
                                KVCacheIndexMatchResponse {
                                    block_size: req_body.block_size,
                                    kv_cache_key_len: usize_to_u64(key.len()),
                                    kv_cache_id: block_id,
                                    offset,
                                    size,
                                    status: StatusCode::Success,
                                    node_address: addr.into_bytes(),
                                }
                            }
                            Err(err) => {
                                error!("Failed to parse kv cache index value: {:?}", err);
                                KVCacheIndexMatchResponse {
                                    block_size: req_body.block_size,
                                    kv_cache_key_len: 0,
                                    kv_cache_id: 0,
                                    offset: 0,
                                    size: 0,
                                    status: StatusCode::InternalError,
                                    node_address: vec![],
                                }
                            }
                        },
                        None => KVCacheIndexMatchResponse {
                            block_size: req_body.block_size,
                            kv_cache_key_len: 0,
                            kv_cache_id: 0,
                            offset: 0,
                            size: 0,
                            status: StatusCode::NotFound,
                            node_address: vec![],
                        },
                    };

                    // Prepare response body
                    let mut resp_body_buffer = BytesMut::new();
                    kv_cache_id_allocate_resp.encode(&mut resp_body_buffer);
                    // Prepare response header
                    let resp_header = RespHeader {
                        seq: req_header.seq,
                        op: RespType::KVCacheIndexMatchResponse.to_u8(),
                        len: usize_to_u64(resp_body_buffer.len()),
                    };
                    resp_header.encode(&mut resp_header_buffer);
                    // Combine response header and body
                    resp_header_buffer.extend_from_slice(&resp_body_buffer);
                }
                _ => {
                    debug!(
                        "IndexHandler: Inner request type is not matched: {:?}",
                        self.header.op
                    );
                }
            }

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
    K: num::Num + Eq + Send + Sync + Clone + fmt::Debug + 'static,
    Vec<K>: radix_trie::TrieKey + Clone,
{
    async fn dispatch(
        &self,
        req_header: ReqHeader,
        req_buffer: BytesMut,
        done_tx: mpsc::Sender<Vec<bytes::Bytes>>,
    ) {
        // debug!("KVCacheHandler: Received request: {:?}", req_header);
        // Dispatch the handler for the connection
        if let Ok(req_type) = ReqType::from_u8(req_header.op) {
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
    /// TODO: Support block manager trait to support different block manager.
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
    K: num::Num + Eq + Send + Sync + Clone + fmt::Debug + 'static,
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
        // let backend = Arc::new(FSBackend::default());

        let mut builder = S3::default();
        builder
            .endpoint(&backend_config.endpoint_url)
            .access_key_id(&backend_config.access_key_id)
            .secret_access_key(&backend_config.secret_access_key)
            .bucket(&backend_config.bucket_name);

        // Init region
        if let Some(region) = backend_config.region.to_owned() {
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

        let cache_manager = Arc::new(KVBlockManager::new(backend));
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use bytes::BytesMut;

    use crate::{
        async_fuse::util::usize_to_u64,
        distribute_kv_cache::{
            local_cache::{
                block::BLOCK_SIZE,
                manager::{IndexManager, KVBlockManager},
            },
            manager::KVCacheHandler,
            rpc::{
                message,
                packet::{self, Decode, Encode},
                server::RpcServerConnectionHandler,
                workerpool::WorkerPool,
            },
        },
    };

    /// Test index handler and block handler

    fn setup() -> (Arc<KVBlockManager>, Arc<IndexManager<u32>>) {
        (
            Arc::new(KVBlockManager::default()),
            Arc::new(IndexManager::new()),
        )
    }

    /// Try to request for a kv cache id from master node
    /// The first request should be 0, and the second request should be 1
    #[tokio::test]
    async fn test_kv_cache_id_alloc_handler() {
        let (cache_manager, index_manager) = setup();
        let pool = Arc::new(WorkerPool::new(64, 1000));
        let handler = KVCacheHandler::new(Arc::clone(&pool), cache_manager, index_manager);

        // Test alloc id request, the first alloc id should be 0
        let (done_tx, mut done_rx) = tokio::sync::mpsc::channel(1);
        let req_header = packet::ReqHeader {
            seq: 1,
            op: message::ReqType::KVCacheIdAllocateRequest.to_u8(),
            len: 0,
        };
        let mut req_buffer = BytesMut::new();
        let request = message::KVCacheIdAllocateRequest {
            block_size: BLOCK_SIZE as u64,
        };
        request.encode(&mut req_buffer);
        handler.dispatch(req_header, req_buffer, done_tx).await;
        let resp_buffer = done_rx.recv().await.unwrap()[0].clone();
        // TODO: decode resp header and body
        let resp_buffer = resp_buffer.to_vec();
        let resp_buffer = resp_buffer.as_slice();
        let resp_header = packet::RespHeader::decode(&mut BytesMut::from(resp_buffer)).unwrap();
        assert_eq!(
            resp_header.op,
            message::RespType::KVCacheIdAllocateResponse.to_u8()
        );
        let resp_body_buffer = resp_buffer.split_at(packet::RESP_HEADER_SIZE as usize).1;
        let resp_body_buffer = resp_body_buffer.to_vec();
        let resp_body_buffer = resp_body_buffer.as_slice();
        let resp_body = message::KVCacheIdAllocateResponse::decode(&mut BytesMut::from(resp_body_buffer)).unwrap();
        assert_eq!(resp_body.kv_cache_id, 0);

        // Test alloc id request, the second alloc id should be 1
        let (done_tx, mut done_rx) = tokio::sync::mpsc::channel(1);
        let req_header = packet::ReqHeader {
            seq: 1,
            op: message::ReqType::KVCacheIdAllocateRequest.to_u8(),
            len: 0,
        };
        let mut req_buffer = BytesMut::new();
        let request = message::KVCacheIdAllocateRequest {
            block_size: BLOCK_SIZE as u64,
        };
        request.encode(&mut req_buffer);
        handler.dispatch(req_header, req_buffer, done_tx).await;
        let resp_buffer = done_rx.recv().await.unwrap()[0].clone();
        let resp_buffer = resp_buffer.to_vec();
        let resp_buffer = resp_buffer.as_slice();
        let resp_header = packet::RespHeader::decode(&mut BytesMut::from(resp_buffer)).unwrap();
        assert_eq!(
            resp_header.op,
            message::RespType::KVCacheIdAllocateResponse.to_u8()
        );
        let resp_body_buffer = resp_buffer.split_at(packet::RESP_HEADER_SIZE as usize).1;
        let resp_body_buffer = resp_body_buffer.to_vec();
        let resp_body_buffer = resp_body_buffer.as_slice();
        let resp_body = message::KVCacheIdAllocateResponse::decode(&mut BytesMut::from(resp_body_buffer)).unwrap();
        assert_eq!(resp_body.kv_cache_id, 1);

        println!("resp_body: {:?}", resp_body);
    }

    /// Try to test current kv cache index data
    /// First, insert a kv cache index data, then match it with actual key and partial key,
    /// check if the match is success or not.
    /// Second, remove the kv cache index data, and try to match it again, the match should be failed.
    #[tokio::test]
    async fn test_kv_cache_index_handler() {
        let (cache_manager, index_manager) = setup();
        let pool = Arc::new(WorkerPool::new(64, 1000));
        let handler = KVCacheHandler::new(Arc::clone(&pool), cache_manager, index_manager);

        // Test insert index request
        let (done_tx, mut done_rx) = tokio::sync::mpsc::channel(1);
        let req_header = packet::ReqHeader {
            seq: 1,
            op: message::ReqType::KVCacheIndexBatchInsertRequest.to_u8(),
            len: 0,
        };
        let mut req_buffer = BytesMut::new();
        let key1 = vec![1_u32, 2_u32, 3_u32, 4_u32];
        let request = message::KVCacheIndexBatchInsertRequest {
            batch_size: 1,
            indexes: vec![message::KVCacheIndexInsertRequest {
                block_size: BLOCK_SIZE as u64,
                kv_cache_id: 0,
                offset: 0,
                size: 0,
                kv_cache_key_len: usize_to_u64(key1.len()),
                kv_cache_key: key1,
            }],
            node_address: "test_key".as_bytes().to_vec(),
        };
        request.encode(&mut req_buffer);
        handler.dispatch(req_header, req_buffer, done_tx).await;
        let resp_buffer = done_rx.recv().await.unwrap()[0].clone();
        let resp_buffer = resp_buffer.to_vec();
        let resp_buffer = resp_buffer.as_slice();
        let resp_header = packet::RespHeader::decode(&mut BytesMut::from(resp_buffer)).unwrap();
        assert_eq!(
            resp_header.op,
            message::RespType::KVCacheIndexInsertResponse.to_u8()
        );
        let resp_body_buffer = resp_buffer.split_at(packet::RESP_HEADER_SIZE as usize).1;
        let resp_body_buffer = resp_body_buffer.to_vec();
        let resp_body_buffer = resp_body_buffer.as_slice();
        let resp_body = message::KVCacheIndexInsertResponse::decode(&mut BytesMut::from(resp_body_buffer)).unwrap();
        assert_eq!(resp_body.status, message::StatusCode::Success);

        println!("resp_body: {:?}", resp_body);

        // Test match index request success
        let (done_tx, mut done_rx) = tokio::sync::mpsc::channel(1);
        let req_header = packet::ReqHeader {
            seq: 1,
            op: message::ReqType::KVCacheIndexMatchRequest.to_u8(),
            len: 0,
        };
        let mut req_buffer = BytesMut::new();
        let key2 = vec![1_u32, 2_u32, 3_u32, 4_u32];
        let request = message::KVCacheIndexMatchRequest {
            block_size: BLOCK_SIZE as u64,
            kv_cache_key: key2,
        };
        request.encode(&mut req_buffer);
        handler.dispatch(req_header, req_buffer, done_tx).await;
        let resp_buffer = done_rx.recv().await.unwrap()[0].clone();
        let resp_buffer = resp_buffer.to_vec();
        let resp_buffer = resp_buffer.as_slice();
        let resp_header = packet::RespHeader::decode(&mut BytesMut::from(resp_buffer)).unwrap();
        assert_eq!(
            resp_header.op,
            message::RespType::KVCacheIndexMatchResponse.to_u8()
        );
        let resp_body_buffer = resp_buffer.split_at(packet::RESP_HEADER_SIZE as usize).1;
        let resp_body_buffer = resp_body_buffer.to_vec();
        let resp_body_buffer = resp_body_buffer.as_slice();
        let resp_body = message::KVCacheIndexMatchResponse::decode(&mut BytesMut::from(resp_body_buffer)).unwrap();
        assert_eq!(resp_body.status, message::StatusCode::Success);
        assert_eq!(resp_body.kv_cache_id, 0);

        println!("resp_body: {:?}", resp_body);

        // Test partial match index request success, original key is "test_key111",
        // and we will return the longest key match with "test_key".
        let (done_tx, mut done_rx) = tokio::sync::mpsc::channel(1);
        let req_header = packet::ReqHeader {
            seq: 1,
            op: message::ReqType::KVCacheIndexMatchRequest.to_u8(),
            len: 0,
        };
        let mut req_buffer = BytesMut::new();
        let key3 = vec![1_u32, 2_u32, 3_u32, 4_u32, 5_u32, 6_u32, 7_u32, 8_u32];
        let request = message::KVCacheIndexMatchRequest {
            block_size: BLOCK_SIZE as u64,
            kv_cache_key: key3,
        };
        request.encode(&mut req_buffer);
        handler.dispatch(req_header, req_buffer, done_tx).await;
        let resp_buffer = done_rx.recv().await.unwrap()[0].clone();
        let resp_buffer = resp_buffer.to_vec();
        let resp_buffer = resp_buffer.as_slice();
        let resp_header = packet::RespHeader::decode(&mut BytesMut::from(resp_buffer)).unwrap();
        assert_eq!(
            resp_header.op,
            message::RespType::KVCacheIndexMatchResponse.to_u8()
        );
        let resp_body_buffer = resp_buffer.split_at(packet::RESP_HEADER_SIZE as usize).1;
        let resp_body_buffer = resp_body_buffer.to_vec();
        let resp_body_buffer = resp_body_buffer.as_slice();
        let resp_body = message::KVCacheIndexMatchResponse::decode(&mut BytesMut::from(resp_body_buffer)).unwrap();
        assert_eq!(resp_body.status, message::StatusCode::Success);
        assert_eq!(resp_body.kv_cache_id, 0);

        println!("resp_body: {:?}", resp_body);

        // Test match index request failed, the key is not exist
        let (done_tx, mut done_rx) = tokio::sync::mpsc::channel(1);
        let req_header = packet::ReqHeader {
            seq: 1,
            op: message::ReqType::KVCacheIndexMatchRequest.to_u8(),
            len: 0,
        };
        let mut req_buffer = BytesMut::new();
        let key4 = vec![1_u32, 3_u32];
        let request = message::KVCacheIndexMatchRequest {
            block_size: BLOCK_SIZE as u64,
            kv_cache_key: key4,
        };
        request.encode(&mut req_buffer);
        handler.dispatch(req_header, req_buffer, done_tx).await;
        let resp_buffer = done_rx.recv().await.unwrap()[0].clone();
        let resp_buffer = resp_buffer.to_vec();
        let resp_buffer = resp_buffer.as_slice();
        let resp_header = packet::RespHeader::decode(&mut BytesMut::from(resp_buffer)).unwrap();
        assert_eq!(
            resp_header.op,
            message::RespType::KVCacheIndexMatchResponse.to_u8()
        );
        let resp_body_buffer = resp_buffer.split_at(packet::RESP_HEADER_SIZE as usize).1;
        let resp_body_buffer = resp_body_buffer.to_vec();
        let resp_body_buffer = resp_body_buffer.as_slice();
        let resp_body = message::KVCacheIndexMatchResponse::decode(&mut BytesMut::from(resp_body_buffer)).unwrap();
        println!("resp_body: {:?}", resp_body);
        assert_eq!(resp_body.status, message::StatusCode::NotFound);
        assert_eq!(resp_body.kv_cache_id, 0);


        // Test remove index request
        let (done_tx, mut done_rx) = tokio::sync::mpsc::channel(1);
        let req_header = packet::ReqHeader {
            seq: 1,
            op: message::ReqType::KVCacheIndexRemoveRequest.to_u8(),
            len: 0,
        };
        let mut req_buffer = BytesMut::new();
        let key5 = vec![1_u32, 2_u32, 3_u32, 4_u32];
        let request = message::KVCacheIndexRemoveRequest {
            block_size: BLOCK_SIZE as u64,
            kv_cache_key: key5,
        };
        request.encode(&mut req_buffer);
        handler.dispatch(req_header, req_buffer, done_tx).await;
        let resp_buffer = done_rx.recv().await.unwrap()[0].clone();
        let resp_buffer = resp_buffer.to_vec();
        let resp_buffer = resp_buffer.as_slice();
        let resp_header = packet::RespHeader::decode(&mut BytesMut::from(resp_buffer)).unwrap();
        assert_eq!(
            resp_header.op,
            message::RespType::KVCacheIndexRemoveResponse.to_u8()
        );
        let resp_body_buffer = resp_buffer.split_at(packet::RESP_HEADER_SIZE as usize).1;
        let resp_body_buffer = resp_body_buffer.to_vec();
        let resp_body_buffer = resp_body_buffer.as_slice();
        let resp_body = message::KVCacheIndexRemoveResponse::decode(&mut BytesMut::from(resp_body_buffer)).unwrap();
        assert_eq!(resp_body.status, message::StatusCode::Success);

        // Test match index request failed
        let (done_tx, mut done_rx) = tokio::sync::mpsc::channel(1);
        let req_header = packet::ReqHeader {
            seq: 1,
            op: message::ReqType::KVCacheIndexMatchRequest.to_u8(),
            len: 0,
        };
        let mut req_buffer = BytesMut::new();
        let key6 = vec![1_u32, 2_u32, 3_u32, 4_u32];
        let request = message::KVCacheIndexMatchRequest {
            block_size: BLOCK_SIZE as u64,
            kv_cache_key: key6,
        };
        request.encode(&mut req_buffer);
        handler.dispatch(req_header, req_buffer, done_tx).await;
        let resp_buffer = done_rx.recv().await.unwrap()[0].clone();
        let resp_buffer = resp_buffer.to_vec();
        let resp_buffer = resp_buffer.as_slice();
        let resp_header = packet::RespHeader::decode(&mut BytesMut::from(resp_buffer)).unwrap();
        assert_eq!(
            resp_header.op,
            message::RespType::KVCacheIndexMatchResponse.to_u8()
        );
        let resp_body_buffer = resp_buffer.split_at(packet::RESP_HEADER_SIZE as usize).1;
        let resp_body_buffer = resp_body_buffer.to_vec();
        let resp_body_buffer = resp_body_buffer.as_slice();
        let resp_body = message::KVCacheIndexMatchResponse::decode(&mut BytesMut::from(resp_body_buffer)).unwrap();
        assert_eq!(resp_body.status, message::StatusCode::NotFound);
        assert_eq!(resp_body.kv_cache_id, 0);

        println!("resp_body: {:?}", resp_body);

        // Test partial match index request failed, because original key "test_key" is removed
        let (done_tx, mut done_rx) = tokio::sync::mpsc::channel(1);
        let req_header = packet::ReqHeader {
            seq: 1,
            op: message::ReqType::KVCacheIndexMatchRequest.to_u8(),
            len: 0,
        };
        let mut req_buffer = BytesMut::new();
        let key7 = vec![1_u32, 2_u32, 3_u32, 4_u32, 5_u32, 6_u32, 7_u32, 8_u32];
        let request = message::KVCacheIndexMatchRequest {
            block_size: BLOCK_SIZE as u64,
            kv_cache_key: key7,
        };
        request.encode(&mut req_buffer);
        handler.dispatch(req_header, req_buffer, done_tx).await;
        let resp_buffer = done_rx.recv().await.unwrap()[0].clone();
        let resp_buffer = resp_buffer.to_vec();
        let resp_buffer = resp_buffer.as_slice();
        let resp_header = packet::RespHeader::decode(&mut BytesMut::from(resp_buffer)).unwrap();
        assert_eq!(
            resp_header.op,
            message::RespType::KVCacheIndexMatchResponse.to_u8()
        );
        let resp_body_buffer = resp_buffer.split_at(packet::RESP_HEADER_SIZE as usize).1;
        let resp_body_buffer = resp_body_buffer.to_vec();
        let resp_body_buffer = resp_body_buffer.as_slice();
        let resp_body = message::KVCacheIndexMatchResponse::decode(&mut BytesMut::from(resp_body_buffer)).unwrap();
        assert_eq!(resp_body.status, message::StatusCode::NotFound);
        assert_eq!(resp_body.kv_cache_id, 0);

        println!("resp_body: {:?}", resp_body);
    }

    /// Try to put and get block data
    #[tokio::test]
    async fn test_kv_block_handler() {
        let (cache_manager, index_manager) = setup();
        let pool = Arc::new(WorkerPool::new(64, 1000));
        let handler = KVCacheHandler::new(Arc::clone(&pool), cache_manager, index_manager);

        // Test batch put block request
        let (done_tx, mut done_rx) = tokio::sync::mpsc::channel(1);
        let req_header = packet::ReqHeader {
            seq: 1,
            op: message::ReqType::KVBlockBatchPutRequest.to_u8(),
            len: 0,
        };
        let mut req_buffer = BytesMut::new();
        let request = message::KVBlockBatchPutRequest {
            batch_size: 1,
            blocks: vec![message::KVBlockPutRequest {
                block_size: BLOCK_SIZE as u64,
                kv_cache_id: 0,
                data: bytes::Bytes::from(vec![0u8; BLOCK_SIZE]),
            }],
        };
        request.encode(&mut req_buffer);
        handler.dispatch(req_header, req_buffer, done_tx).await;
        let resp_buffer = done_rx.recv().await.unwrap()[0].clone();
        let resp_buffer = resp_buffer.to_vec();
        let resp_buffer = resp_buffer.as_slice();
        let resp_header = packet::RespHeader::decode(&mut BytesMut::from(resp_buffer)).unwrap();
        assert_eq!(
            resp_header.op,
            message::RespType::KVBlockBatchPutResponse.to_u8()
        );
        let resp_body_buffer = resp_buffer.split_at(packet::RESP_HEADER_SIZE as usize).1;
        let resp_body_buffer = resp_body_buffer.to_vec();
        let resp_body_buffer = resp_body_buffer.as_slice();
        let resp_body = message::KVBlockBatchPutResponse::decode(&mut BytesMut::from(resp_body_buffer)).unwrap();
        assert_eq!(resp_body.block_size, 1);
        assert_eq!(resp_body.success_kv_cache_ids, vec![0]);
        assert_eq!(resp_body.success_batch_size, 1);
        assert_eq!(resp_body.failed_batch_size, 0);

        println!("resp_body: {:?}", resp_body);

        // Test get block request
        let (done_tx, mut done_rx) = tokio::sync::mpsc::channel(1);
        let req_header = packet::ReqHeader {
            seq: 1,
            op: message::ReqType::KVBlockGetRequest.to_u8(),
            len: 0,
        };
        let mut req_buffer = BytesMut::new();
        let request = message::KVBlockGetRequest {
            kv_cache_id: 0,
            block_size: BLOCK_SIZE as u64,
        };
        request.encode(&mut req_buffer);
        handler.dispatch(req_header, req_buffer, done_tx).await;
        let resp_buffer = done_rx.recv().await.unwrap()[0].clone();
        let resp_buffer = resp_buffer.to_vec();
        let resp_buffer = resp_buffer.as_slice();
        let resp_header = packet::RespHeader::decode(&mut BytesMut::from(resp_buffer)).unwrap();
        assert_eq!(
            resp_header.op,
            message::RespType::KVBlockGetResponse.to_u8()
        );
        let resp_body_buffer = resp_buffer.split_at(packet::RESP_HEADER_SIZE as usize).1;
        let resp_body_buffer = resp_body_buffer.to_vec();
        let resp_body_buffer = resp_body_buffer.as_slice();
        let resp_body = message::KVBlockGetResponse::decode(&mut BytesMut::from(resp_body_buffer)).unwrap();
        assert_eq!(resp_body.status, message::StatusCode::InternalError);
        assert_eq!(resp_body.kv_cache_id, 0);
        assert_eq!(resp_body.block_size, BLOCK_SIZE as u64);
        assert_eq!(resp_body.data.len(), BLOCK_SIZE);

        // decode with large data
        let resp_body = message::KVBlockGetResponse::decode_large_data(BytesMut::from(resp_body_buffer).freeze()).unwrap();
        assert_eq!(resp_body.status, message::StatusCode::InternalError);
        assert_eq!(resp_body.kv_cache_id, 0);
        assert_eq!(resp_body.block_size, BLOCK_SIZE as u64);
        assert_eq!(resp_body.data.len(), BLOCK_SIZE);
    }
}
