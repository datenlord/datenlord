use std::{fmt, mem};

use async_trait::async_trait;
use bytes::{Buf, BufMut, BytesMut};
use clippy_utilities::{Cast, OverflowArithmetic};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tracing::{debug, error, warn};

use super::{
    error::RpcError,
    packet::{ActualSize, DecodeLarge, Encode, EncodeLarge, RequestTask},
    utils::{get_u64_from_buf, u64_to_usize},
};
use crate::{async_fuse::util::usize_to_u64, encode_to_buf};

/// The request type of the request.
#[derive(Debug, IntoPrimitive, TryFromPrimitive, Serialize, Deserialize, Clone, Copy)]
#[repr(u8)]
pub enum ReqType {
    /// The keep alive request 0.
    KeepAliveRequest = 0,
    /// The file block request 1.
    FileBlockRequest = 1,
    /// The block allocate request 2.
    KVCacheIdAllocateRequest = 2,
    /// The kv cache index get request 3.
    KVCacheIndexMatchRequest = 3,
    /// The kv cache index batch insert request 4.
    KVCacheIndexBatchInsertRequest = 4,
    /// The kv cache index remove request 5.
    KVCacheIndexRemoveRequest = 5,
    /// The kv block get request 6.
    KVBlockGetRequest = 6,
    /// The kv block put request 7.
    KVBlockBatchPutRequest = 7,
}

/// The operation type of the response.
#[derive(Debug, IntoPrimitive, TryFromPrimitive, Serialize, Deserialize, Clone, Copy)]
#[repr(u8)]
pub enum RespType {
    /// The keep alive response.
    KeepAliveResponse = 0,
    /// The file block response.
    FileBlockResponse = 1,
    /// The block allocate response.
    KVCacheIdAllocateResponse = 2,
    /// The kv cache index get response.
    KVCacheIndexMatchResponse = 3,
    /// The kv cache index put response.
    KVCacheIndexInsertResponse = 4,
    /// The kv cache index remove response.
    KVCacheIndexRemoveResponse = 5,
    /// The kv block get response.
    KVBlockGetResponse = 6,
    /// The kv block put response.
    KVBlockBatchPutResponse = 7,
}

/// Common data structures shared between the client and server.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FileBlockRequest {
    /// The file ID.
    pub file_id: u64,
    /// The block ID.
    pub block_id: u64,
    /// The block size.
    pub block_size: u64,
    /// The block version
    pub block_version: u64,
    /// The hashring version
    /// The latest hashring version
    /// We will return current hashring version in rpc server,
    /// client will check if the hashring version is old as the latest one.
    pub hash_ring_version: u64,
}

/// The response to a file block request.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FileBlockResponse {
    /// The file ID.
    pub file_id: u64,
    /// The block ID.
    pub block_id: u64,
    /// The block size.
    pub block_size: u64,
    /// The block version
    pub block_version: u64,
    /// The hashring version
    /// The latest hashring version
    /// We will return current hashring version in rpc server,
    /// client will check if the hashring version is old as the latest one.
    pub hash_ring_version: u64,
    /// The status of the response.
    pub status: StatusCode,
    /// The data of the block.
    pub data: Vec<u8>,
}

impl EncodeLarge for FileBlockResponse {
    /// Encode the file block response into a byte buffer.
    fn encode_large_data(&self, buf: &mut BytesMut) -> (u64, Vec<bytes::Bytes>) {
        // Initialize a vector to store the byte chunks.
        let mut chunks = Vec::new();

        // Encode the fixed-size fields into the buffer.
        // These are the fields that are always a fixed size.
        buf.put_u64(self.file_id);
        buf.put_u64(self.block_id);
        buf.put_u64(self.block_size);
        buf.put_u64(self.block_version);
        buf.put_u64(self.hash_ring_version);

        // Encode the status field (status is a single byte).
        buf.put_u8(self.status.into());

        // The data is variable-size, so we need to encode it after the fixed-size fields.
        buf.put_u64(usize_to_u64(self.data.len())); // Store the length of the data first
        buf.extend_from_slice(&self.data); // Now append the actual data

        // Now we need to chunk the buffer. For example, splitting it into manageable chunks.
        let chunk_size = u64_to_usize(self.block_size);
        let total_size = usize_to_u64(buf.len());
        for chunk in buf.chunks(chunk_size) {
            chunks.push(bytes::Bytes::copy_from_slice(chunk));
        }

        // Return the total size of the encoded data and the vector of chunks.
        (total_size, chunks)
    }
}

impl DecodeLarge for FileBlockResponse {
    //// Decode the byte buffer into a file block response.
    fn decode_large_data(buf: bytes::Bytes) -> Result<Self, RpcError> {
        if buf.len() < 41 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let file_id = get_u64_from_buf(&buf, 0)?;
        let block_id = get_u64_from_buf(&buf, 8)?;
        let block_size = get_u64_from_buf(&buf, 16)?;
        let block_version = get_u64_from_buf(&buf, 24)?;
        let hash_ring_version = get_u64_from_buf(&buf, 32)?;
        let status = match buf.get(40) {
            Some(&0) => StatusCode::Success,
            Some(&1) => StatusCode::NotFound,
            Some(&2) => StatusCode::InternalError,
            Some(&3) => StatusCode::VersionMismatch,
            _ => return Err(RpcError::InternalError("Invalid status code".to_owned())),
        };
        let data = buf.get(41..).unwrap_or(&[]).to_vec();
        let data_len = usize_to_u64(data.len());
        if data_len != block_size {
            return Err(RpcError::InternalError(format!(
                "Insufficient block size {data_len}"
            )));
        }

        Ok(FileBlockResponse {
            file_id,
            block_id,
            block_size,
            block_version,
            hash_ring_version,
            status,
            data,
        })
    }
}

/// The status code of the response.
#[derive(
    Debug, IntoPrimitive, TryFromPrimitive, PartialEq, Eq, Clone, Copy, Serialize, Deserialize,
)]
#[repr(u8)]
pub enum StatusCode {
    /// The request is successful.
    Success = 0,
    /// The request is not found.
    NotFound = 1,
    /// The request is invalid.
    InternalError = 2,
    /// The request is out dated.
    VersionMismatch = 3,
}

impl Default for StatusCode {
    /// Get the default status code.
    fn default() -> Self {
        Self::Success
    }
}

/// The file block packet for client
/// This struct impl packet trait and support file request and response
#[derive(Debug, Clone)]
pub struct FileBlockRequestTask {
    /// The sequence number of the packet.
    pub seq: u64,
    /// The operation type of the packet.
    pub op: u8,
    /// The timestamp of the packet.
    pub timestamp: u64,
    /// The file block request struct.
    pub request: FileBlockRequest,
    /// The buffer of the packet, used to store response data.
    pub response: Option<FileBlockResponse>,
    /// The sender to send response back to caller.
    pub done_tx: flume::Sender<Result<FileBlockResponse, FileBlockRequest>>,
}

impl FileBlockRequestTask {
    /// Create a new file block packet.
    #[must_use]
    pub fn new(
        block_request: &FileBlockRequest,
        done_tx: flume::Sender<Result<FileBlockResponse, FileBlockRequest>>,
    ) -> Self {
        Self {
            // Will be auto set by client
            seq: 0,
            // Set operation
            op: ReqType::FileBlockRequest.into(),
            request: block_request.clone(),
            timestamp: 0,
            response: None,
            done_tx,
        }
    }
}

impl Encode for FileBlockRequestTask {
    /// Encode the file block packet into a byte buffer.
    fn encode(&self, buffer: &mut BytesMut) {
        encode_to_buf!(buffer, &self.request);
    }
}

#[async_trait]
impl RequestTask for FileBlockRequestTask {
    /// Get the sequence number of the packet.
    fn seq(&self) -> u64 {
        self.seq
    }

    /// Set the sequence number of the packet.
    fn set_seq(&mut self, seq: u64) {
        self.seq = seq;
    }

    /// Get the operation type of the packet.
    fn op(&self) -> u8 {
        self.op
    }

    /// Set the operation type of the packet.
    fn set_op(&mut self, op: u8) {
        self.op = op;
    }

    /// Get the timestamp of the packet.
    fn set_timestamp(&mut self, timestamp: u64) {
        self.timestamp = timestamp;
    }

    /// get the timestamp of the packet.
    fn get_timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Set response data to current packet
    fn set_resp_data(&mut self, data: BytesMut) -> Result<(), RpcError> {
        self.response = Some(FileBlockResponse::decode_large_data(data.freeze())?);
        Ok(())
    }

    /// Get the response data of the packet.
    async fn set_result(self, status: Result<(), RpcError>) {
        match status {
            Ok(()) => {
                if let Some(resp) = self.response {
                    match self.done_tx.send_async(Ok(resp)).await {
                        Ok(()) => {}
                        Err(err) => {
                            warn!("Failed to set result: {:?}", err);
                        }
                    }
                } else {
                    warn!("Failed to set result: response is None");
                    match self.done_tx.send_async(Err(self.request.clone())).await {
                        Ok(()) => {}
                        Err(err) => {
                            warn!("Failed to set result: {:?}", err);
                        }
                    }
                }
            }
            Err(err) => {
                warn!("Failed to set result: {:?}", err);
                match self.done_tx.send_async(Err(self.request.clone())).await {
                    Ok(()) => {}
                    Err(err) => {
                        warn!("Failed to set result: {:?}", err);
                    }
                }
            }
        }
    }

    /// Get request size to construct request header
    fn get_req_len(&self) -> u64 {
        usize_to_u64(mem::size_of_val(&self.request))
    }
}

/// The request to allocate a global kv cache id.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct KVCacheIdAllocateRequest {
    /// The kv block size.
    pub block_size: u64,
}

/// The response to allocate a global kv cache id.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct KVCacheIdAllocateResponse {
    /// The kv block size.
    pub block_size: u64,
    /// The kv cache id.
    pub kv_cache_id: u64,
}

/// The request to match kv cache index.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct KVCacheIndexMatchRequest<K> {
    /// The kv block size.
    pub block_size: u64,
    /// The kv cache key.
    pub kv_cache_key: Vec<K>,
}

/// The response to match kv cache index.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct KVCacheIndexMatchResponse {
    /// The kv block size.
    pub block_size: u64,
    /// The kv cache key length, used to identify the longest match length.
    pub kv_cache_key_len: u64,
    /// The kv cache id, match success return kv cache id, otherwise return 0.
    pub kv_cache_id: u64,
    /// The kv cache offset
    pub offset: u64,
    /// The kv cache size
    pub size: u64,
    /// The status of the response.
    pub status: StatusCode,
    /// The node address
    pub node_address: Vec<u8>,
}

/// The request to insert kv cache index.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct KVCacheIndexInsertRequest<K> {
    /// The kv block size.
    pub block_size: u64,
    /// The kv cache id, pair with kv cache key.
    pub kv_cache_id: u64,
    /// The kv cache offset
    pub offset: u64,
    /// The kv cache size
    pub size: u64,
    /// The key length
    pub kv_cache_key_len: u64,
    /// The kv cache key.
    // pub kv_cache_key: Vec<u32>,
    pub kv_cache_key: Vec<K>,
}

/// The response to insert kv cache index.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct KVCacheIndexInsertResponse {
    /// The kv block size.
    pub block_size: u64,
    /// The kv cache id.
    pub kv_cache_id: u64,
    /// The status of the response.
    pub status: StatusCode,
}

/// The request to batch insert kv cache index.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct KVCacheIndexBatchInsertRequest<K> {
    /// The batch size
    pub batch_size: u64,
    /// The kv cache index list
    pub indexes: Vec<KVCacheIndexInsertRequest<K>>,
    /// The node address
    pub node_address: Vec<u8>,
}

/// The request to remove kv cache index.
/// TODO: check both `kv_cache_id` and `kv_cache_key` to make sure current deletion is correct.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct KVCacheIndexRemoveRequest<K> {
    /// The kv block size.
    pub block_size: u64,
    /// The kv cache key.
    pub kv_cache_key: Vec<K>,
}

/// The response to remove kv cache index.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct KVCacheIndexRemoveResponse {
    /// The kv block size.
    pub block_size: u64,
    /// The status of the response.
    pub status: StatusCode,
}

/// The request to get kv block.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct KVBlockGetRequest {
    /// The kv block size.
    pub block_size: u64,
    /// The kv cache id.
    pub kv_cache_id: u64,
}

/// The response to get kv block.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct KVBlockGetResponse {
    /// The kv block size.
    pub block_size: u64,
    /// The kv cache id.
    pub kv_cache_id: u64,
    /// The status of the response.
    pub status: StatusCode,
    /// The data of the block.
    #[serde(skip_serializing, skip_deserializing)]
    pub data: bytes::Bytes,
}

impl Encode for KVBlockGetResponse {
    /// Encode the kv block get response into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        encode_to_buf!(buf, self);
    }
}

impl EncodeLarge for KVBlockGetResponse {
    /// Encode large data
    fn encode_large_data(&self, buf: &mut BytesMut) -> (u64, Vec<bytes::Bytes>) {
        let len = self.data.len() + 17;
        if buf.capacity() < 17 {
            buf.reserve(17);
        }
        buf.put_u64(self.block_size.to_le());
        buf.put_u64(self.kv_cache_id.to_le());
        buf.put_u8(self.status.into());

        // Return extra large data
        (len.cast(), vec![self.data.clone()])
    }
}

impl DecodeLarge for KVBlockGetResponse {
    /// Decode the byte buffer into a kv block get response.
    fn decode_large_data(buf: bytes::Bytes) -> Result<Self, RpcError> {
        if buf.len() < 17 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let block_size = get_u64_from_buf(&buf, 0)?;
        let kv_cache_id = get_u64_from_buf(&buf, 8)?;
        let status = StatusCode::try_from(
            buf.get(16)
                .ok_or_else(|| RpcError::InternalError("Insufficient bytes".to_owned()))?
                .to_owned(),
        )
        .map_err(|e| RpcError::InternalError(e.to_string()))?;
        let data = buf.slice(17..);

        // let data = vec![];
        // Use unsafe to avoid copy
        let data_len = usize_to_u64(data.len());
        if data_len != block_size {
            return Err(RpcError::InternalError(format!(
                "Insufficient block size {data_len}"
            )));
        }

        Ok(KVBlockGetResponse {
            block_size,
            kv_cache_id,
            status,
            data,
        })
    }
}

/// The request to put kv block.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct KVBlockPutRequest {
    /// The kv block size.
    pub block_size: u64,
    /// The kv cache id.
    pub kv_cache_id: u64,
    #[serde(skip_serializing, skip_deserializing)]
    /// The data of the block.
    pub data: bytes::Bytes,
}

impl Encode for KVBlockPutRequest {
    /// Encode the kv block put request into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        encode_to_buf!(buf, self);
    }
}

impl EncodeLarge for KVBlockPutRequest {
    fn encode_large_data(&self, buf: &mut BytesMut) -> (u64, Vec<bytes::Bytes>) {
        let len = self.data.len() + 16;
        if buf.capacity() < 16 {
            buf.reserve(16);
        }
        buf.put_u64(self.block_size.to_le());
        buf.put_u64(self.kv_cache_id.to_le());

        (len.cast(), vec![self.data.clone()])
    }
}

impl DecodeLarge for KVBlockPutRequest {
    /// Decode the byte buffer into a kv block put request.
    #[allow(clippy::as_conversions)]
    fn decode_large_data(buf: bytes::Bytes) -> Result<Self, RpcError> {
        if buf.len() < 16 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let block_size = get_u64_from_buf(&buf, 0)?;
        let kv_cache_id = get_u64_from_buf(&buf, 8)?;
        // let buf = &buf[16..block_size as usize + 16];
        // Get data from bytes slice.
        let data = buf.slice(16..u64_to_usize(block_size) + 16);
        let data_len = usize_to_u64(data.len());
        if data_len != block_size {
            return Err(RpcError::InternalError(format!(
                "Insufficient block size {data_len}"
            )));
        }

        Ok(KVBlockPutRequest {
            block_size,
            kv_cache_id,
            data,
        })
    }
}

impl ActualSize for KVBlockPutRequest {
    /// Get the actual size of the request.
    fn actual_size(&self) -> u64 {
        let data_len = usize_to_u64(self.data.len());
        let block_size_len = usize_to_u64(mem::size_of_val(&self.block_size));
        let kv_cache_id_len = usize_to_u64(mem::size_of_val(&self.kv_cache_id));
        data_len
            .overflow_add(block_size_len)
            .overflow_add(kv_cache_id_len)
    }
}

/// The request to put multiple kv blocks.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct KVBlockBatchPutRequest {
    /// The kv block batch size.
    pub batch_size: u64,
    /// A list of kv block put requests.
    pub blocks: Vec<KVBlockPutRequest>,
}

impl Encode for KVBlockBatchPutRequest {
    /// Encode the request into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.batch_size.to_le());
        for block in &self.blocks {
            block.encode(buf);
        }
    }
}

impl EncodeLarge for KVBlockBatchPutRequest {
    /// Get large data with Bytes, disable memory copy
    fn encode_large_data(&self, buf: &mut BytesMut) -> (u64, Vec<bytes::Bytes>) {
        let len = self.actual_size();
        let mut data = Vec::new();
        // (self.batch_size.to_le_bytes());
        for block in &self.blocks {
            data.extend(block.encode_large_data(buf).1);
        }
        (len, data)
    }
}

impl DecodeLarge for KVBlockBatchPutRequest {
    /// Decode the byte buffer into a kv block batch put request.
    fn decode_large_data(buf: bytes::Bytes) -> Result<Self, RpcError> {
        if buf.len() < 8 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let batch_size = get_u64_from_buf(&buf, 0)?;
        let mut blocks = Vec::new();
        let mut offset = 8;
        for _ in 0..batch_size {
            // TODO: give an end pointer.
            let sub_buf = buf.slice(offset..);
            let block = KVBlockPutRequest::decode_large_data(sub_buf)?;
            offset += 16 + block.data.len();
            blocks.push(block);
        }
        Ok(KVBlockBatchPutRequest { batch_size, blocks })
    }
}

impl ActualSize for KVBlockBatchPutRequest {
    /// Get the actual size of the request.
    fn actual_size(&self) -> u64 {
        let mut size = usize_to_u64(mem::size_of_val(&self.batch_size));
        for block in &self.blocks {
            size = size.overflow_add(block.actual_size());
        }
        size
    }
}

/// The response to put kv block.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct KVBlockBatchPutResponse {
    /// The kv block size.
    pub block_size: u64,
    /// The success batch size.
    pub success_batch_size: u64,
    /// The success kv cache ids.
    pub success_kv_cache_ids: Vec<u64>,
    /// The failed batch size.
    pub failed_batch_size: u64,
    /// The failed kv cache ids.
    pub failed_kv_cache_ids: Vec<u64>,
}

/// The kv cache small request packet type
/// In this struct, we only use bincode to directly encode and decode the request.
/// Metadata or other small request is defined here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KVCacheSmallRequest<K> {
    /// The request to allocate a global kv cache id.
    KVCacheIdAllocateRequest(KVCacheIdAllocateRequest),
    /// The request to match kv cache index.
    KVCacheIndexMatchRequest(KVCacheIndexMatchRequest<K>),
    /// The request to insert kv cache index.
    KVCacheIndexBatchInsertRequest(KVCacheIndexBatchInsertRequest<K>),
    /// The request to remove kv cache index.
    KVCacheIndexRemoveRequest(KVCacheIndexRemoveRequest<K>),
    /// The request to get kv block.
    KVBlockGetRequest(KVBlockGetRequest),
}

/// The kv cache request packet type.
/// In this struct, we need to encode and decode the large data manually.
#[derive(Debug, Clone)]
pub enum KVCacheLargeRequest {
    /// The request to put kv block.
    KVBlockBatchPutRequest(KVBlockBatchPutRequest),
}

/// TODO: change to encode large
#[allow(dead_code)]
impl KVCacheLargeRequest {
    /// Basic encode, only for test
    #[allow(clippy::pattern_type_mismatch)]
    #[allow(clippy::wildcard_enum_match_arm)]
    fn encode(&self, buf: &mut BytesMut) {
        match self {
            Self::KVBlockBatchPutRequest(request) => request.encode(buf),
        }
    }

    /// Encode and decode the large data manually.
    #[allow(clippy::pattern_type_mismatch)]
    #[allow(clippy::wildcard_enum_match_arm)]
    fn encode_large(&self, buf: &mut BytesMut) -> (u64, Vec<bytes::Bytes>) {
        match self {
            Self::KVBlockBatchPutRequest(request) => request.encode_large_data(buf),
        }
    }

    ///  Decode the large data manually.
    #[allow(clippy::unimplemented)]
    #[allow(clippy::pattern_type_mismatch)]
    #[allow(clippy::wildcard_enum_match_arm)]
    fn decode(request_type: ReqType, buf: bytes::Bytes) -> Result<Self, RpcError> {
        match request_type {
            ReqType::KVBlockBatchPutRequest => {
                let request = KVBlockBatchPutRequest::decode_large_data(buf)?;
                Ok(Self::KVBlockBatchPutRequest(request))
            }
            _ => Err(RpcError::InternalError("Invalid request type".to_owned())),
        }
    }
}

impl ActualSize for KVCacheLargeRequest {
    /// Get the actual size of the request.
    #[allow(clippy::pattern_type_mismatch)]
    fn actual_size(&self) -> u64 {
        match self {
            Self::KVBlockBatchPutRequest(request) => request.actual_size(),
        }
    }
}

/// The kv cache request packet type.
#[derive(Debug, Clone)]
pub enum KVCacheRequest<K> {
    /// Small data request, e.g. metadata request
    KVCacheSmallRequest(KVCacheSmallRequest<K>),
    /// Large data request, e.g. data request
    KVCacheLargeRequest(KVCacheLargeRequest),
}

impl<K> Encode for KVCacheRequest<K>
where
    K: num::Num + Send + Sync + Clone + fmt::Debug + Serialize + DeserializeOwned,
{
    /// Encode the kv cache response into a byte buffer.
    #[allow(clippy::needless_pass_by_value)]
    #[allow(clippy::pattern_type_mismatch)]
    fn encode(&self, buf: &mut BytesMut) {
        match self {
            Self::KVCacheSmallRequest(response) => {
                encode_to_buf!(buf, response);
            }
            Self::KVCacheLargeRequest(response) => {
                response.encode(buf);
            }
        }
    }
}

#[allow(dead_code)]
impl<K> KVCacheRequest<K>
where
    K: num::Num + Send + Sync + Clone + fmt::Debug + Serialize + DeserializeOwned,
{
    /// Decode the byte buffer into a kv cache response.
    #[allow(clippy::needless_pass_by_value)]
    fn decode(request_type: ReqType, buf: BytesMut) -> Result<Self, RpcError> {
        if buf.len() < 8 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }

        // Use decode large first, if not support, we need to use bincode to directly decode the data.
        let immutable_buf = buf.freeze();
        match KVCacheLargeRequest::decode(request_type, immutable_buf.clone()) {
            Ok(request) => Ok(Self::KVCacheLargeRequest(request)),
            Err(e) => {
                warn!("Failed to decode response with large: {}", e);
                // If decode large failed, we need to use bincode to directly decode the data.
                let reader = immutable_buf.reader();
                let small_request: KVCacheSmallRequest<K> = bincode::deserialize_from(reader)
                    .map_err(|e| RpcError::InternalError(e.to_string()))?;
                Ok(Self::KVCacheSmallRequest(small_request))
            }
        }
    }
}

impl<K> ActualSize for KVCacheRequest<K>
where
    K: num::Num + Send + Sync + Clone + fmt::Debug + Serialize + DeserializeOwned,
{
    /// Get the actual size of the request.
    #[allow(clippy::pattern_type_mismatch)]
    fn actual_size(&self) -> u64 {
        match self {
            Self::KVCacheSmallRequest(request) => usize_to_u64(mem::size_of_val(request)),
            Self::KVCacheLargeRequest(request) => request.actual_size(),
        }
    }
}

/// The kv cache small response packet type
/// In this struct, we only use bincode to directly encode and decode the response.
/// Metadata or other small response is defined here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KVCacheSmallResponse {
    /// The response to allocate a global kv cache id.
    KVCacheIdAllocateResponse(KVCacheIdAllocateResponse),
    /// The response to match kv cache index.
    KVCacheIndexMatchResponse(KVCacheIndexMatchResponse),
    /// The response to insert kv cache index.
    KVCacheIndexInsertResponse(KVCacheIndexInsertResponse),
    /// The response to remove kv cache index.
    KVCacheIndexRemoveResponse(KVCacheIndexRemoveResponse),
    /// The response to put multiple kv blocks.
    KVBlockBatchPutResponse(KVBlockBatchPutResponse),
}

/// The kv cache response packet type.
/// In this struct, we need to encode and decode the large data manually.
#[derive(Debug, Clone)]
pub enum KVCacheLargeResponse {
    /// The response to get kv block.
    KVBlockGetResponse(KVBlockGetResponse),
}

#[allow(dead_code)]
impl KVCacheLargeResponse {
    /// Basic encode, only for test
    #[allow(clippy::pattern_type_mismatch)]
    fn encode(&self, buf: &mut BytesMut) {
        match self {
            Self::KVBlockGetResponse(response) => response.encode(buf),
        }
    }

    /// Encode and decode the large data manually.
    #[allow(clippy::pattern_type_mismatch)]
    fn encode_large(&self, buf: &mut BytesMut) -> (u64, Vec<bytes::Bytes>) {
        match self {
            Self::KVBlockGetResponse(response) => response.encode_large_data(buf),
        }
    }

    ///  Decode the large data manually.
    #[allow(clippy::wildcard_enum_match_arm)]
    fn decode(response_type: RespType, buf: bytes::Bytes) -> Result<Self, RpcError> {
        match response_type {
            RespType::KVBlockGetResponse => {
                let response = KVBlockGetResponse::decode_large_data(buf)?;
                Ok(Self::KVBlockGetResponse(response))
            }
            _ => Err(RpcError::InternalError("Invalid response type".to_owned())),
        }
    }
}

/// The kv cache response packet type.
#[derive(Debug, Clone)]
pub enum KVCacheResponse {
    /// Small data response, e.g. metadata response
    KVCacheSmallResponse(KVCacheSmallResponse),
    /// Large data response, e.g. data response
    KVCacheLargeResponse(KVCacheLargeResponse),
}

impl Encode for KVCacheResponse {
    /// Encode the kv cache response into a byte buffer.
    #[allow(clippy::needless_pass_by_value)]
    #[allow(clippy::pattern_type_mismatch)]
    fn encode(&self, buf: &mut BytesMut) {
        match self {
            Self::KVCacheSmallResponse(response) => {
                encode_to_buf!(buf, response);
            }
            Self::KVCacheLargeResponse(response) => {
                response.encode(buf);
            }
        }
    }
}

impl KVCacheResponse {
    /// Decode the byte buffer into a kv cache response.
    #[allow(clippy::needless_pass_by_value)]
    fn decode(response_type: RespType, buf: BytesMut) -> Result<Self, RpcError> {
        if buf.len() < 8 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }

        // Use decode large first, if not support, we need to use bincode to directly decode the data.
        let immutable_buf = buf.freeze();
        match KVCacheLargeResponse::decode(response_type, immutable_buf.clone()) {
            Ok(response) => Ok(Self::KVCacheLargeResponse(response)),
            Err(e) => {
                warn!("Failed to decode response with large: {}", e);
                // If decode large failed, we need to use bincode to directly decode the data.
                let reader = immutable_buf.reader();
                let small_response: KVCacheSmallResponse = bincode::deserialize_from(reader)
                    .map_err(|e| RpcError::InternalError(e.to_string()))?;
                Ok(Self::KVCacheSmallResponse(small_response))
            }
        }
    }
}

/// The kv cache request packet for client
/// This struct impl packet trait and support kv cache request and response
#[derive(Clone)]
pub struct KVCacheRequestTask<K> {
    /// The sequence number of the packet.
    pub seq: u64,
    /// The operation type of the packet.
    pub op: u8,
    /// The timestamp of the packet.
    pub timestamp: u64,
    /// The file block request struct.
    pub request: KVCacheRequest<K>,
    /// The buffer of the packet, used to store response data.
    pub response: Option<KVCacheResponse>,
    /// The sender to send response back to caller.
    pub done_tx: flume::Sender<Result<KVCacheResponse, KVCacheRequest<K>>>,
}

#[allow(clippy::missing_fields_in_debug)]
impl<K> fmt::Debug for KVCacheRequestTask<K>
where
    K: num::Num + Send + Sync + Clone + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KVCacheRequestTask")
            .field("seq", &self.seq)
            .field("op", &self.op)
            .field("timestamp", &self.timestamp)
            .finish()
    }
}

impl<K> KVCacheRequestTask<K>
where
    K: num::Num + Send + Sync + Clone + fmt::Debug,
{
    /// Create a new kv cache packet.
    #[must_use]
    pub fn new(
        op: u8,
        kv_cache_request: KVCacheRequest<K>,
        done_tx: flume::Sender<Result<KVCacheResponse, KVCacheRequest<K>>>,
    ) -> Self {
        Self {
            op,
            // Will be auto set by client
            seq: 0,
            // TODO: Take owner of this request?
            request: kv_cache_request,
            timestamp: 0,
            response: None,
            done_tx,
        }
    }
}

impl<K> Encode for KVCacheRequestTask<K>
where
    K: num::Num + Send + Sync + Clone + fmt::Debug + Serialize + DeserializeOwned,
{
    /// Encode the kv cache packet into a byte buffer.
    fn encode(&self, buffer: &mut BytesMut) {
        self.request.encode(buffer);
    }
}

#[async_trait]
impl<K> RequestTask for KVCacheRequestTask<K>
where
    K: num::Num
        + Send
        + Sync
        + Clone
        + fmt::Debug
        + Send
        + Sync
        + Clone
        + Serialize
        + DeserializeOwned,
{
    /// Get the sequence number of the packet.
    fn seq(&self) -> u64 {
        self.seq
    }

    /// Set the sequence number of the packet.
    fn set_seq(&mut self, seq: u64) {
        self.seq = seq;
    }

    /// Get the operation type of the packet.
    fn op(&self) -> u8 {
        self.op
    }

    /// Set the operation type of the packet.
    fn set_op(&mut self, op: u8) {
        self.op = op;
    }

    /// Get the timestamp of the packet.
    fn set_timestamp(&mut self, timestamp: u64) {
        self.timestamp = timestamp;
    }

    /// get the timestamp of the packet.
    fn get_timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Set response data to current packet
    fn set_resp_data(&mut self, data: BytesMut) -> Result<(), RpcError> {
        let response_type = RespType::try_from(self.op)
            .map_err(|e| RpcError::InternalError(format!("Invalid response type: {e}")))?;
        match response_type {
            RespType::KVCacheIdAllocateResponse => {
                self.response = Some(KVCacheResponse::decode(
                    RespType::KVCacheIdAllocateResponse,
                    data,
                )?);
            }
            RespType::KVCacheIndexMatchResponse => {
                self.response = Some(KVCacheResponse::decode(
                    RespType::KVCacheIndexMatchResponse,
                    data,
                )?);
            }
            RespType::KVCacheIndexInsertResponse => {
                self.response = Some(KVCacheResponse::decode(
                    RespType::KVCacheIndexInsertResponse,
                    data,
                )?);
            }
            RespType::KVCacheIndexRemoveResponse => {
                self.response = Some(KVCacheResponse::decode(
                    RespType::KVCacheIndexRemoveResponse,
                    data,
                )?);
            }
            RespType::KVBlockGetResponse => {
                self.response = Some(KVCacheResponse::decode(RespType::KVBlockGetResponse, data)?);
            }
            RespType::KVBlockBatchPutResponse => {
                self.response = Some(KVCacheResponse::decode(
                    RespType::KVBlockBatchPutResponse,
                    data,
                )?);
            }
            RespType::KeepAliveResponse | RespType::FileBlockResponse => {
                return Err(RpcError::InternalError("Invalid response type".to_owned()));
            }
        }

        Ok(())
    }

    /// Get the response data of the packet.
    async fn set_result(self, status: Result<(), RpcError>) {
        match status {
            Ok(()) => {
                if let Some(resp) = self.response {
                    match self.done_tx.send_async(Ok(resp)).await {
                        Ok(()) => {
                            debug!("Set result in set_result() successfully");
                        }
                        Err(err) => {
                            warn!("Failed to set result: {:?}", err);
                        }
                    }
                } else {
                    warn!("Failed to set result: response is None");
                    match self.done_tx.send_async(Err(self.request.clone())).await {
                        Ok(()) => {}
                        Err(err) => {
                            warn!("Failed to set result: {:?}", err);
                        }
                    }
                }
            }
            Err(err) => {
                warn!("Failed to set result: {:?}", err);
                match self.done_tx.send_async(Err(self.request.clone())).await {
                    Ok(()) => {}
                    Err(err) => {
                        warn!("Failed to set result: {:?}", err);
                    }
                }
            }
        }
    }

    /// Get request size to construct request header
    fn get_req_len(&self) -> u64 {
        self.request.actual_size()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
#[allow(clippy::indexing_slicing)]
#[allow(unused_variables)]
mod test {
    use crate::distribute_kv_cache::rpc::utils::u64_to_usize;

    use super::*;

    #[test]
    fn test_file_block_request_task_encode() {
        let file_id = 123;
        let block_id = 456;
        let block_size = 789;
        let block_version = 10;
        let hash_ring_version = 20;

        let request = FileBlockRequest {
            file_id,
            block_id,
            block_size,
            block_version,
            hash_ring_version,
        };

        let (done_tx, _) = flume::unbounded();
        let packet = FileBlockRequestTask::new(&request, done_tx);

        let mut buffer = BytesMut::new();
        packet.encode(&mut buffer);

        let expected_buffer_len = u64_to_usize(packet.get_req_len());
        assert_eq!(buffer.len(), expected_buffer_len);
    }

    #[test]
    fn test_file_block_response_encode_decode_large_data() {
        let response = FileBlockResponse {
            file_id: 1,
            block_id: 2,
            block_size: 5,
            block_version: 3,
            hash_ring_version: 4,
            status: StatusCode::Success,
            data: vec![10, 20, 30, 40, 50],
        };

        let mut buf = BytesMut::new();
        let (size, chunks) = response.encode_large_data(&mut buf);
        let mut combined = BytesMut::new();
        for chunk in chunks {
            combined.extend_from_slice(&chunk);
        }
        let decoded = FileBlockResponse::decode_large_data(combined.freeze()).unwrap();
        assert_eq!(u64_to_usize(size), 41 + response.data.len());
    }

    #[test]
    fn test_kv_block_get_response_encode_decode_large_data() {
        let data = bytes::Bytes::from(vec![1, 2, 3, 4, 5, 6, 7, 8]);
        let response = KVBlockGetResponse {
            block_size: usize_to_u64(data.len()),
            kv_cache_id: 123,
            status: StatusCode::Success,
            data: data.clone(),
        };

        let mut buf = BytesMut::new();
        let (size, chunks) = response.encode_large_data(&mut buf);
        let mut combined = BytesMut::new();
        for chunk in chunks {
            combined.extend_from_slice(&chunk);
        }
        let decoded = KVBlockGetResponse::decode_large_data(combined.freeze()).unwrap();
        assert_eq!(response.block_size, decoded.block_size);
        assert_eq!(response.kv_cache_id, decoded.kv_cache_id);
        assert_eq!(response.status, decoded.status);
        assert_eq!(response.data, decoded.data);
        assert_eq!(u64_to_usize(size), 17 + data.len());
    }

    #[test]
    fn test_kv_block_put_request_encode_decode_large_data() {
        let data = bytes::Bytes::from(vec![9, 8, 7, 6, 5, 4]);
        let request = KVBlockPutRequest {
            block_size: usize_to_u64(data.len()),
            kv_cache_id: 321,
            data: data.clone(),
        };

        let mut buf = BytesMut::new();
        let (size, chunks) = request.encode_large_data(&mut buf);
        let mut combined = BytesMut::new();
        for chunk in chunks {
            combined.extend_from_slice(&chunk);
        }
        let decoded = KVBlockPutRequest::decode_large_data(combined.freeze()).unwrap();
        assert_eq!(request.block_size, decoded.block_size);
        assert_eq!(request.kv_cache_id, decoded.kv_cache_id);
        assert_eq!(request.data, decoded.data);
        assert_eq!(u64_to_usize(size), 16 + data.len());
    }
}
