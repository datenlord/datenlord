use std::{fmt, mem};

use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use clippy_utilities::OverflowArithmetic;
use tracing::warn;

use crate::async_fuse::util::usize_to_u64;

use super::{
    error::RpcError,
    packet::{ActualSize, Decode, Encode, Packet},
    utils::{get_u64_from_buf, u64_to_usize},
};

/// The request type of the request.
#[derive(Debug)]
pub enum ReqType {
    /// The keep alive request 0.
    KeepAliveRequest,
    /// The file block request 1.
    FileBlockRequest,
    /// The block allocate request 2.
    KVCacheIdAllocateRequest,
    /// The kv cache index get request 3.
    KVCacheIndexMatchRequest,
    /// The kv cache index batch insert request 4.
    KVCacheIndexBatchInsertRequest,
    /// The kv cache index remove request 5.
    KVCacheIndexRemoveRequest,
    /// The kv block get request 6.
    KVBlockGetRequest,
    /// The kv block put request 7.
    KVBlockBatchPutRequest,
}

impl ReqType {
    /// Convert u8 to `ReqType`
    pub fn from_u8(op: u8) -> Result<Self, RpcError> {
        match op {
            0 => Ok(Self::KeepAliveRequest),
            1 => Ok(Self::FileBlockRequest),
            2 => Ok(Self::KVCacheIdAllocateRequest),
            3 => Ok(Self::KVCacheIndexMatchRequest),
            4 => Ok(Self::KVCacheIndexBatchInsertRequest),
            5 => Ok(Self::KVCacheIndexRemoveRequest),
            6 => Ok(Self::KVBlockGetRequest),
            7 => Ok(Self::KVBlockBatchPutRequest),
            _ => Err(RpcError::InternalError(format!(
                "Invalid operation type: {op}"
            ))),
        }
    }

    /// Convert `ReqType` to u8
    #[must_use]
    pub fn to_u8(&self) -> u8 {
        match *self {
            Self::KeepAliveRequest => 0,
            Self::FileBlockRequest => 1,
            Self::KVCacheIdAllocateRequest => 2,
            Self::KVCacheIndexMatchRequest => 3,
            Self::KVCacheIndexBatchInsertRequest => 4,
            Self::KVCacheIndexRemoveRequest => 5,
            Self::KVBlockGetRequest => 6,
            Self::KVBlockBatchPutRequest => 7,
        }
    }
}

/// The operation type of the response.
#[derive(Debug)]
pub enum RespType {
    /// The keep alive response.
    KeepAliveResponse,
    /// The file block response.
    FileBlockResponse,
    /// The block allocate response.
    KVCacheIdAllocateResponse,
    /// The kv cache index get response.
    KVCacheIndexMatchResponse,
    /// The kv cache index put response.
    KVCacheIndexInsertResponse,
    /// The kv cache index remove response.
    KVCacheIndexRemoveResponse,
    /// The kv block get response.
    KVBlockGetResponse,
    /// The kv block put response.
    KVBlockBatchPutResponse,
}

impl RespType {
    /// Convert u8 to `RespType`
    pub fn from_u8(op: u8) -> Result<Self, RpcError> {
        match op {
            0 => Ok(Self::KeepAliveResponse),
            1 => Ok(Self::FileBlockResponse),
            2 => Ok(Self::KVCacheIdAllocateResponse),
            3 => Ok(Self::KVCacheIndexMatchResponse),
            4 => Ok(Self::KVCacheIndexInsertResponse),
            5 => Ok(Self::KVCacheIndexRemoveResponse),
            6 => Ok(Self::KVBlockGetResponse),
            7 => Ok(Self::KVBlockBatchPutResponse),
            _ => Err(RpcError::InternalError(format!(
                "Invalid operation type: {op}"
            ))),
        }
    }

    /// Convert `RespType` to u8
    #[must_use]
    pub fn to_u8(&self) -> u8 {
        match *self {
            Self::KeepAliveResponse => 0,
            Self::FileBlockResponse => 1,
            Self::KVCacheIdAllocateResponse => 2,
            Self::KVCacheIndexMatchResponse => 3,
            Self::KVCacheIndexInsertResponse => 4,
            Self::KVCacheIndexRemoveResponse => 5,
            Self::KVBlockGetResponse => 6,
            Self::KVBlockBatchPutResponse => 7,
        }
    }
}

/// Common data structures shared between the client and server.
#[derive(Debug, Default, Clone)]
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

impl Encode for FileBlockRequest {
    /// Encode the file block request into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.file_id.to_le());
        buf.put_u64(self.block_id.to_le());
        buf.put_u64(self.block_size.to_le());
        buf.put_u64(self.block_version.to_le());
        buf.put_u64(self.hash_ring_version.to_le());
    }
}

impl Decode for FileBlockRequest {
    /// Decode the byte buffer into a file block request.
    fn decode(buf: &[u8]) -> Result<Self, RpcError> {
        if buf.len() < 32 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let file_id = get_u64_from_buf(buf, 0)?;
        let block_id = get_u64_from_buf(buf, 8)?;
        let block_size = get_u64_from_buf(buf, 16)?;
        let block_version = get_u64_from_buf(buf, 24)?;
        let hash_ring_version = get_u64_from_buf(buf, 32)?;

        Ok(FileBlockRequest {
            file_id,
            block_id,
            block_size,
            block_version,
            hash_ring_version,
        })
    }
}

/// The response to a file block request.
#[derive(Debug, Default, Clone)]
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
impl Encode for FileBlockResponse {
    /// Encode the file block response into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.file_id.to_le());
        buf.put_u64(self.block_id.to_le());
        buf.put_u64(self.block_size.to_le());
        buf.put_u64(self.block_version.to_le());
        buf.put_u64(self.hash_ring_version.to_le());
        match self.status {
            StatusCode::Success => buf.put_u8(0),
            StatusCode::NotFound => buf.put_u8(1),
            StatusCode::InternalError => buf.put_u8(2),
            StatusCode::VersionMismatch => buf.put_u8(3),
        }
        buf.put_slice(&self.data);
    }
}

impl Decode for FileBlockResponse {
    //// Decode the byte buffer into a file block response.
    fn decode(buf: &[u8]) -> Result<Self, RpcError> {
        if buf.len() < 41 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let file_id = get_u64_from_buf(buf, 0)?;
        let block_id = get_u64_from_buf(buf, 8)?;
        let block_size = get_u64_from_buf(buf, 16)?;
        let block_version = get_u64_from_buf(buf, 24)?;
        let hash_ring_version = get_u64_from_buf(buf, 32)?;
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
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum StatusCode {
    /// The request is successful.
    Success,
    /// The request is not found.
    NotFound,
    /// The request is invalid.
    InternalError,
    /// The request is out dated.
    VersionMismatch,
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
pub struct FileBlockPacket {
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

impl FileBlockPacket {
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
            op: ReqType::FileBlockRequest.to_u8(),
            request: block_request.clone(),
            timestamp: 0,
            response: None,
            done_tx,
        }
    }
}

impl Encode for FileBlockPacket {
    /// Encode the file block packet into a byte buffer.
    fn encode(&self, buffer: &mut BytesMut) {
        self.request.encode(buffer);
    }
}

#[async_trait]
impl Packet for FileBlockPacket {
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
    fn set_resp_data(&mut self, data: &[u8]) -> Result<(), RpcError> {
        self.response = Some(FileBlockResponse::decode(data)?);
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
#[derive(Debug, Default, Clone)]
pub struct KVCacheIdAllocateRequest {
    /// The kv block size.
    pub block_size: u64,
}

impl Encode for KVCacheIdAllocateRequest {
    /// Encode the kv cache id allocate request into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.block_size.to_le());
    }
}

impl Decode for KVCacheIdAllocateRequest {
    /// Decode the byte buffer into a kv cache id allocate request.
    fn decode(buf: &[u8]) -> Result<Self, RpcError> {
        if buf.len() < 8 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let block_size = get_u64_from_buf(buf, 0)?;
        Ok(KVCacheIdAllocateRequest { block_size })
    }
}

impl ActualSize for KVCacheIdAllocateRequest {
    /// Get the actual size of the request.
    fn actual_size(&self) -> u64 {
        usize_to_u64(mem::size_of_val(&self.block_size))
    }
}

/// The response to allocate a global kv cache id.
#[derive(Debug, Default, Clone)]
pub struct KVCacheIdAllocateResponse {
    /// The kv block size.
    pub block_size: u64,
    /// The kv cache id.
    pub kv_cache_id: u64,
}

impl Encode for KVCacheIdAllocateResponse {
    /// Encode the kv cache id allocate response into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.block_size.to_le());
        buf.put_u64(self.kv_cache_id.to_le());
    }
}

impl Decode for KVCacheIdAllocateResponse {
    /// Decode the byte buffer into a kv cache id allocate response.
    fn decode(buf: &[u8]) -> Result<Self, RpcError> {
        if buf.len() < 16 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let block_size = get_u64_from_buf(buf, 0)?;
        let kv_cache_id = get_u64_from_buf(buf, 8)?;
        Ok(KVCacheIdAllocateResponse {
            block_size,
            kv_cache_id,
        })
    }
}

/// The request to match kv cache index.
#[derive(Debug, Default, Clone)]
pub struct KVCacheIndexMatchRequest {
    /// The kv block size.
    pub block_size: u64,
    /// The kv cache key.
    pub kv_cache_key: Vec<u8>,
}

impl Encode for KVCacheIndexMatchRequest {
    /// Encode the kv cache index match request into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.block_size.to_le());
        buf.put_slice(&self.kv_cache_key);
    }
}

impl Decode for KVCacheIndexMatchRequest {
    /// Decode the byte buffer into a kv cache index match request.
    fn decode(buf: &[u8]) -> Result<Self, RpcError> {
        if buf.len() < 8 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let block_size = get_u64_from_buf(buf, 0)?;
        let kv_cache_key = buf[8..].to_vec();
        Ok(KVCacheIndexMatchRequest {
            block_size,
            kv_cache_key,
        })
    }
}

impl ActualSize for KVCacheIndexMatchRequest {
    /// Get the actual size of the request.
    fn actual_size(&self) -> u64 {
        let kv_cache_key_len = usize_to_u64(self.kv_cache_key.len());
        let block_size_len = usize_to_u64(mem::size_of_val(&self.block_size));
        kv_cache_key_len.overflow_add(block_size_len)
    }
}

/// The response to match kv cache index.
#[derive(Debug, Default, Clone)]
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

impl Encode for KVCacheIndexMatchResponse {
    /// Encode the kv cache index match response into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.block_size.to_le());
        buf.put_u64(self.kv_cache_key_len.to_le());
        buf.put_u64(self.kv_cache_id.to_le());
        buf.put_u64(self.offset.to_le());
        buf.put_u64(self.size.to_le());
        match self.status {
            StatusCode::Success => buf.put_u8(0),
            StatusCode::NotFound => buf.put_u8(1),
            StatusCode::InternalError => buf.put_u8(2),
            // Not used here.
            StatusCode::VersionMismatch => buf.put_u8(3),
        }
        buf.put_slice(&self.node_address);
    }
}

impl Decode for KVCacheIndexMatchResponse {
    /// Decode the byte buffer into a kv cache index match response.
    fn decode(buf: &[u8]) -> Result<Self, RpcError> {
        if buf.len() < 16 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let block_size = get_u64_from_buf(buf, 0)?;
        let kv_cache_key_len = get_u64_from_buf(buf, 8)?;
        let kv_cache_id = get_u64_from_buf(buf, 16)?;
        let offset = get_u64_from_buf(buf, 24)?;
        let size = get_u64_from_buf(buf, 32)?;
        let status = match buf.get(40) {
            Some(&0) => StatusCode::Success,
            Some(&1) => StatusCode::NotFound,
            Some(&2) => StatusCode::InternalError,
            Some(&3) => StatusCode::VersionMismatch,
            _ => return Err(RpcError::InternalError("Invalid status code".to_owned())),
        };
        let node_address = buf.get(41..).unwrap_or(&[]).to_vec();
        Ok(KVCacheIndexMatchResponse {
            block_size,
            kv_cache_key_len,
            kv_cache_id,
            offset,
            size,
            status,
            node_address,
        })
    }
}

/// The request to insert kv cache index.
#[derive(Debug, Default, Clone)]
pub struct KVCacheIndexInsertRequest {
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
    pub kv_cache_key: Vec<u8>,
}

impl Encode for KVCacheIndexInsertRequest {
    /// Encode the kv cache index insert request into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.block_size.to_le());
        buf.put_u64(self.kv_cache_id.to_le());
        buf.put_u64(self.offset.to_le());
        buf.put_u64(self.size.to_le());
        buf.put_u64(self.kv_cache_key_len.to_le());
        buf.put_slice(&self.kv_cache_key);
    }
}

impl Decode for KVCacheIndexInsertRequest {
    /// Decode the byte buffer into a kv cache index insert request.
    fn decode(buf: &[u8]) -> Result<Self, RpcError> {
        if buf.len() < 16 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let block_size = get_u64_from_buf(buf, 0)?;
        let kv_cache_id = get_u64_from_buf(buf, 8)?;
        let offset = get_u64_from_buf(buf, 16)?;
        let size = get_u64_from_buf(buf, 24)?;
        let kv_cache_key_len = get_u64_from_buf(buf, 32)?;
        let kv_cache_key = buf[40..40 + u64_to_usize(kv_cache_key_len)].to_vec();
        Ok(KVCacheIndexInsertRequest {
            block_size,
            kv_cache_key,
            offset,
            size,
            kv_cache_key_len,
            kv_cache_id,
        })
    }
}

impl ActualSize for KVCacheIndexInsertRequest {
    /// Get the actual size of the request.
    fn actual_size(&self) -> u64 {
        let kv_cache_key_len = usize_to_u64(self.kv_cache_key.len());
        let block_size_len = usize_to_u64(mem::size_of_val(&self.block_size));
        let kv_cache_id_len = usize_to_u64(mem::size_of_val(&self.kv_cache_id));
        let offset_len = usize_to_u64(mem::size_of_val(&self.offset));
        let size_len = usize_to_u64(mem::size_of_val(&self.size));
        let kv_cache_key_len_len = usize_to_u64(mem::size_of_val(&self.kv_cache_key_len));
        kv_cache_key_len
            .overflow_add(block_size_len)
            .overflow_add(kv_cache_id_len)
            .overflow_add(offset_len)
            .overflow_add(size_len)
            .overflow_add(kv_cache_key_len_len)
    }
}

/// The response to insert kv cache index.
#[derive(Debug, Default, Clone)]
pub struct KVCacheIndexInsertResponse {
    /// The kv block size.
    pub block_size: u64,
    /// The kv cache id.
    pub kv_cache_id: u64,
    /// The status of the response.
    pub status: StatusCode,
}

impl Encode for KVCacheIndexInsertResponse {
    /// Encode the kv cache index insert response into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.block_size.to_le());
        buf.put_u64(self.kv_cache_id.to_le());
        match self.status {
            StatusCode::Success => buf.put_u8(0),
            StatusCode::NotFound => buf.put_u8(1),
            StatusCode::InternalError => buf.put_u8(2),
            // Not used here.
            StatusCode::VersionMismatch => buf.put_u8(3),
        }
    }
}

impl Decode for KVCacheIndexInsertResponse {
    /// Decode the byte buffer into a kv cache index insert response.
    fn decode(buf: &[u8]) -> Result<Self, RpcError> {
        if buf.len() < 17 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let block_size = get_u64_from_buf(buf, 0)?;
        let kv_cache_id = get_u64_from_buf(buf, 8)?;
        let status = match buf.get(16) {
            Some(&0) => StatusCode::Success,
            Some(&1) => StatusCode::NotFound,
            Some(&2) => StatusCode::InternalError,
            Some(&3) => StatusCode::VersionMismatch,
            _ => return Err(RpcError::InternalError("Invalid status code".to_owned())),
        };
        Ok(KVCacheIndexInsertResponse {
            block_size,
            kv_cache_id,
            status,
        })
    }
}

/// The request to batch insert kv cache index.
#[derive(Debug, Default, Clone)]
pub struct KVCacheIndexBatchInsertRequest {
    /// The batch size
    pub batch_size: u64,
    /// The kv cache index list
    pub indexes: Vec<KVCacheIndexInsertRequest>,
    /// The node address
    pub node_address: Vec<u8>,
}

impl Encode for KVCacheIndexBatchInsertRequest {
    /// Encode the kv cache index batch insert request into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.batch_size.to_le());
        for index in &self.indexes {
            index.encode(buf);
        }
        buf.put_slice(&self.node_address);
    }
}

impl Decode for KVCacheIndexBatchInsertRequest {
    /// Decode the byte buffer into a kv cache index batch insert request.
    fn decode(buf: &[u8]) -> Result<Self, RpcError> {
        if buf.len() < 8 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let batch_size = get_u64_from_buf(buf, 0)?;
        let mut indexes = Vec::new();
        let mut offset = 8;
        for _ in 0..batch_size {
            let index = KVCacheIndexInsertRequest::decode(&buf[offset..])?;
            offset += u64_to_usize(index.actual_size());
            indexes.push(index);
        }
        let node_address = buf[offset..].to_vec();
        Ok(KVCacheIndexBatchInsertRequest {
            batch_size,
            indexes,
            node_address,
        })
    }
}

impl ActualSize for KVCacheIndexBatchInsertRequest {
    /// Get the actual size of the request.
    fn actual_size(&self) -> u64 {
        let mut size = usize_to_u64(mem::size_of_val(&self.batch_size));
        for index in &self.indexes {
            size = size.overflow_add(index.actual_size());
        }
        size = size.overflow_add(usize_to_u64(self.node_address.len()));
        size
    }
}

/// The request to remove kv cache index.
/// TODO: check both kv_cache_id and kv_cache_key to make sure current deletion is correct.
#[derive(Debug, Default, Clone)]
pub struct KVCacheIndexRemoveRequest {
    /// The kv block size.
    pub block_size: u64,
    /// The kv cache key.
    pub kv_cache_key: Vec<u8>,
}

impl Encode for KVCacheIndexRemoveRequest {
    /// Encode the kv cache index remove request into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.block_size.to_le());
        buf.put_slice(&self.kv_cache_key);
    }
}

impl Decode for KVCacheIndexRemoveRequest {
    /// Decode the byte buffer into a kv cache index remove request.
    fn decode(buf: &[u8]) -> Result<Self, RpcError> {
        if buf.len() < 8 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let block_size = get_u64_from_buf(buf, 0)?;
        let kv_cache_key = buf[8..].to_vec();
        Ok(KVCacheIndexRemoveRequest {
            block_size,
            kv_cache_key,
        })
    }
}

impl ActualSize for KVCacheIndexRemoveRequest {
    /// Get the actual size of the request.
    fn actual_size(&self) -> u64 {
        let kv_cache_key_len = usize_to_u64(self.kv_cache_key.len());
        let block_size_len = usize_to_u64(mem::size_of_val(&self.block_size));
        kv_cache_key_len.overflow_add(block_size_len)
    }
}

/// The response to remove kv cache index.
#[derive(Debug, Default, Clone)]
pub struct KVCacheIndexRemoveResponse {
    /// The kv block size.
    pub block_size: u64,
    /// The status of the response.
    pub status: StatusCode,
}

impl Encode for KVCacheIndexRemoveResponse {
    /// Encode the kv cache index remove response into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.block_size.to_le());
        match self.status {
            StatusCode::Success => buf.put_u8(0),
            StatusCode::NotFound => buf.put_u8(1),
            StatusCode::InternalError => buf.put_u8(2),
            // Not used here.
            StatusCode::VersionMismatch => buf.put_u8(3),
        }
    }
}

impl Decode for KVCacheIndexRemoveResponse {
    /// Decode the byte buffer into a kv cache index remove response.
    fn decode(buf: &[u8]) -> Result<Self, RpcError> {
        if buf.len() < 9 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let block_size = get_u64_from_buf(buf, 0)?;
        let status = match buf.get(8) {
            Some(&0) => StatusCode::Success,
            Some(&1) => StatusCode::NotFound,
            Some(&2) => StatusCode::InternalError,
            Some(&3) => StatusCode::VersionMismatch,
            _ => return Err(RpcError::InternalError("Invalid status code".to_owned())),
        };
        Ok(KVCacheIndexRemoveResponse { block_size, status })
    }
}

/// The request to get kv block.
#[derive(Debug, Default, Clone)]
pub struct KVBlockGetRequest {
    /// The kv block size.
    pub block_size: u64,
    /// The kv cache id.
    pub kv_cache_id: u64,
}

impl Encode for KVBlockGetRequest {
    /// Encode the kv block get request into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.block_size.to_le());
        buf.put_u64(self.kv_cache_id.to_le());
    }
}

impl Decode for KVBlockGetRequest {
    /// Decode the byte buffer into a kv block get request.
    fn decode(buf: &[u8]) -> Result<Self, RpcError> {
        if buf.len() < 16 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let block_size = get_u64_from_buf(buf, 0)?;
        let kv_cache_id = get_u64_from_buf(buf, 8)?;
        Ok(KVBlockGetRequest {
            block_size,
            kv_cache_id,
        })
    }
}

impl ActualSize for KVBlockGetRequest {
    /// Get the actual size of the request.
    fn actual_size(&self) -> u64 {
        let block_size_len = usize_to_u64(mem::size_of_val(&self.block_size));
        let kv_cache_id_len = usize_to_u64(mem::size_of_val(&self.kv_cache_id));
        block_size_len.overflow_add(kv_cache_id_len)
    }
}

/// The response to get kv block.
#[derive(Debug, Default, Clone)]
pub struct KVBlockGetResponse {
    /// The kv block size.
    pub block_size: u64,
    /// The kv cache id.
    pub kv_cache_id: u64,
    /// The status of the response.
    pub status: StatusCode,
    /// The data of the block.
    pub data: Vec<u8>,
}

impl Encode for KVBlockGetResponse {
    /// Encode the kv block get response into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.block_size.to_le());
        buf.put_u64(self.kv_cache_id.to_le());
        match self.status {
            StatusCode::Success => buf.put_u8(0),
            StatusCode::NotFound => buf.put_u8(1),
            StatusCode::InternalError => buf.put_u8(2),
            // Not used here.
            StatusCode::VersionMismatch => buf.put_u8(3),
        }
        buf.put_slice(&self.data);
    }
}

impl Decode for KVBlockGetResponse {
    /// Decode the byte buffer into a kv block get response.
    fn decode(buf: &[u8]) -> Result<Self, RpcError> {
        if buf.len() < 17 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let block_size = get_u64_from_buf(buf, 0)?;
        let kv_cache_id = get_u64_from_buf(buf, 8)?;
        let status = match buf.get(16) {
            Some(&0) => StatusCode::Success,
            Some(&1) => StatusCode::NotFound,
            Some(&2) => StatusCode::InternalError,
            Some(&3) => StatusCode::VersionMismatch,
            _ => return Err(RpcError::InternalError("Invalid status code".to_owned())),
        };
        let data = buf.get(17..).unwrap_or(&[]).to_vec();
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
#[derive(Debug, Default, Clone)]
pub struct KVBlockPutRequest {
    /// The kv block size.
    pub block_size: u64,
    /// The kv cache id.
    pub kv_cache_id: u64,
    /// The data of the block.
    pub data: Vec<u8>,
}

impl Encode for KVBlockPutRequest {
    /// Encode the kv block put request into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.block_size.to_le());
        buf.put_u64(self.kv_cache_id.to_le());
        buf.put_slice(&self.data);
    }
}

impl Decode for KVBlockPutRequest {
    /// Decode the byte buffer into a kv block put request.
    fn decode(buf: &[u8]) -> Result<Self, RpcError> {
        if buf.len() < 16 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let block_size = get_u64_from_buf(buf, 0)?;
        let kv_cache_id = get_u64_from_buf(buf, 8)?;
        let data = buf[16..].to_vec();
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
#[derive(Debug, Default, Clone)]
pub struct KVBlockBatchPutRequest {
    /// The kv block batch size.
    pub batch_size: u64,
    /// A list of kv block put requests.
    pub blocks: Vec<KVBlockPutRequest>,
}

impl Encode for KVBlockBatchPutRequest {
    /// Encode the kv block batch put request into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.batch_size.to_le());
        for block in &self.blocks {
            block.encode(buf);
        }
    }
}

impl Decode for KVBlockBatchPutRequest {
    /// Decode the byte buffer into a kv block batch put request.
    fn decode(buf: &[u8]) -> Result<Self, RpcError> {
        if buf.len() < 8 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let batch_size = get_u64_from_buf(buf, 0)?;
        let mut blocks = Vec::new();
        let mut offset = 8;
        for _ in 0..batch_size {
            let block = KVBlockPutRequest::decode(&buf[offset..])?;
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
#[derive(Debug, Default, Clone)]
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

impl Encode for KVBlockBatchPutResponse {
    /// Encode the kv block batch put response into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.block_size.to_le());
        buf.put_u64(self.success_batch_size.to_le());
        for kv_cache_id in &self.success_kv_cache_ids {
            buf.put_u64(kv_cache_id.to_le());
        }
        buf.put_u64(self.failed_batch_size.to_le());
        for kv_cache_id in &self.failed_kv_cache_ids {
            buf.put_u64(kv_cache_id.to_le());
        }
    }
}

impl Decode for KVBlockBatchPutResponse {
    /// Decode the byte buffer into a kv block batch put response.
    fn decode(buf: &[u8]) -> Result<Self, RpcError> {
        if buf.len() < 16 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let block_size = get_u64_from_buf(buf, 0)?;
        let success_batch_size = get_u64_from_buf(buf, 8)?;
        let mut success_kv_cache_ids = Vec::new();
        let mut offset = 16;
        for _ in 0..success_batch_size {
            let kv_cache_id = get_u64_from_buf(buf, offset)?;
            success_kv_cache_ids.push(kv_cache_id);
            offset += 8;
        }
        let failed_batch_size = get_u64_from_buf(buf, offset)?;
        offset += 8;
        let mut failed_kv_cache_ids = Vec::new();
        for _ in 0..failed_batch_size {
            let kv_cache_id = get_u64_from_buf(buf, offset)?;
            failed_kv_cache_ids.push(kv_cache_id);
            offset += 8;
        }
        Ok(KVBlockBatchPutResponse {
            block_size,
            success_batch_size,
            success_kv_cache_ids,
            failed_batch_size,
            failed_kv_cache_ids,
        })
    }
}

/// The kv cache request packet type.
#[derive(Debug, Clone)]
pub enum KVCacheRequest {
    /// The request to allocate a global kv cache id.
    KVCacheIdAllocateRequest(KVCacheIdAllocateRequest),
    /// The request to match kv cache index.
    KVCacheIndexMatchRequest(KVCacheIndexMatchRequest),
    /// The request to insert kv cache index.
    KVCacheIndexBatchInsertRequest(KVCacheIndexBatchInsertRequest),
    /// The request to remove kv cache index.
    KVCacheIndexRemoveRequest(KVCacheIndexRemoveRequest),
    /// The request to get kv block.
    KVBlockGetRequest(KVBlockGetRequest),
    /// The request to put kv block.
    KVBlockBatchPutRequest(KVBlockBatchPutRequest),
}

impl Encode for KVCacheRequest {
    /// Encode the kv cache request into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        match self {
            Self::KVCacheIdAllocateRequest(request) => request.encode(buf),
            Self::KVCacheIndexMatchRequest(request) => request.encode(buf),
            Self::KVCacheIndexBatchInsertRequest(request) => request.encode(buf),
            Self::KVCacheIndexRemoveRequest(request) => request.encode(buf),
            Self::KVBlockGetRequest(request) => request.encode(buf),
            Self::KVBlockBatchPutRequest(request) => request.encode(buf),
        }
    }
}

impl ActualSize for KVCacheRequest {
    /// Get the actual size of the request.
    fn actual_size(&self) -> u64 {
        match self {
            Self::KVCacheIdAllocateRequest(request) => request.actual_size(),
            Self::KVCacheIndexMatchRequest(request) => request.actual_size(),
            Self::KVCacheIndexBatchInsertRequest(request) => request.actual_size(),
            Self::KVCacheIndexRemoveRequest(request) => request.actual_size(),
            Self::KVBlockGetRequest(request) => request.actual_size(),
            Self::KVBlockBatchPutRequest(request) => request.actual_size(),
        }
    }
}

impl KVCacheRequest {
    #[allow(unused)]
    /// Decode the byte buffer into a kv cache request.
    fn decode(request_type: ReqType, buf: &[u8]) -> Result<Self, RpcError> {
        if buf.len() < 8 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        match request_type {
            ReqType::KVCacheIdAllocateRequest => {
                let request = KVCacheIdAllocateRequest::decode(buf)?;
                Ok(Self::KVCacheIdAllocateRequest(request))
            }
            ReqType::KVCacheIndexMatchRequest => {
                let request = KVCacheIndexMatchRequest::decode(buf)?;
                Ok(Self::KVCacheIndexMatchRequest(request))
            }
            ReqType::KVCacheIndexBatchInsertRequest => {
                let request = KVCacheIndexBatchInsertRequest::decode(buf)?;
                Ok(Self::KVCacheIndexBatchInsertRequest(request))
            }
            ReqType::KVCacheIndexRemoveRequest => {
                let request = KVCacheIndexRemoveRequest::decode(buf)?;
                Ok(Self::KVCacheIndexRemoveRequest(request))
            }
            ReqType::KVBlockGetRequest => {
                let request = KVBlockGetRequest::decode(buf)?;
                Ok(Self::KVBlockGetRequest(request))
            }
            ReqType::KVBlockBatchPutRequest => {
                let request = KVBlockBatchPutRequest::decode(buf)?;
                Ok(Self::KVBlockBatchPutRequest(request))
            }
            _ => Err(RpcError::InternalError("Invalid request type".to_owned())),
        }
    }
}

/// The kv cache response packet type.
#[derive(Debug, Clone)]
pub enum KVCacheResponse {
    /// The response to allocate a global kv cache id.
    KVCacheIdAllocateResponse(KVCacheIdAllocateResponse),
    /// The response to match kv cache index.
    KVCacheIndexMatchResponse(KVCacheIndexMatchResponse),
    /// The response to insert kv cache index.
    KVCacheIndexInsertResponse(KVCacheIndexInsertResponse),
    /// The response to remove kv cache index.
    KVCacheIndexRemoveResponse(KVCacheIndexRemoveResponse),
    /// The response to get kv block.
    KVBlockGetResponse(KVBlockGetResponse),
    /// The response to put multiple kv blocks.
    KVBlockBatchPutResponse(KVBlockBatchPutResponse),
}

impl Encode for KVCacheResponse {
    /// Encode the kv cache response into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        match self {
            Self::KVCacheIdAllocateResponse(response) => response.encode(buf),
            Self::KVCacheIndexMatchResponse(response) => response.encode(buf),
            Self::KVCacheIndexInsertResponse(response) => response.encode(buf),
            Self::KVCacheIndexRemoveResponse(response) => response.encode(buf),
            Self::KVBlockGetResponse(response) => response.encode(buf),
            Self::KVBlockBatchPutResponse(response) => response.encode(buf),
        }
    }
}

impl KVCacheResponse {
    /// Decode the byte buffer into a kv cache response.
    fn decode(response_type: RespType, buf: &[u8]) -> Result<Self, RpcError> {
        if buf.len() < 8 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        match response_type {
            RespType::KVCacheIdAllocateResponse => {
                let response = KVCacheIdAllocateResponse::decode(buf)?;
                Ok(Self::KVCacheIdAllocateResponse(response))
            }
            RespType::KVCacheIndexMatchResponse => {
                let response = KVCacheIndexMatchResponse::decode(buf)?;
                Ok(Self::KVCacheIndexMatchResponse(response))
            }
            RespType::KVCacheIndexInsertResponse => {
                let response = KVCacheIndexInsertResponse::decode(buf)?;
                Ok(Self::KVCacheIndexInsertResponse(response))
            }
            RespType::KVCacheIndexRemoveResponse => {
                let response = KVCacheIndexRemoveResponse::decode(buf)?;
                Ok(Self::KVCacheIndexRemoveResponse(response))
            }
            RespType::KVBlockGetResponse => {
                let response = KVBlockGetResponse::decode(buf)?;
                Ok(Self::KVBlockGetResponse(response))
            }
            RespType::KVBlockBatchPutResponse => {
                let response = KVBlockBatchPutResponse::decode(buf)?;
                Ok(Self::KVBlockBatchPutResponse(response))
            }
            _ => Err(RpcError::InternalError("Invalid response type".to_owned())),
        }
    }
}

/// The kv cache request packet for client
/// This struct impl packet trait and support kv cache request and response
#[derive(Clone)]
pub struct KVCachePacket {
    /// The sequence number of the packet.
    pub seq: u64,
    /// The operation type of the packet.
    pub op: u8,
    /// The timestamp of the packet.
    pub timestamp: u64,
    /// The file block request struct.
    pub request: KVCacheRequest,
    /// The buffer of the packet, used to store response data.
    pub response: Option<KVCacheResponse>,
    /// The sender to send response back to caller.
    pub done_tx: flume::Sender<Result<KVCacheResponse, KVCacheRequest>>,
}

impl fmt::Debug for KVCachePacket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KVCachePacket")
            .field("seq", &self.seq)
            .field("op", &self.op)
            .field("timestamp", &self.timestamp)
            .finish()
    }
}

impl KVCachePacket {
    /// Create a new kv cache packet.
    #[must_use]
    pub fn new(
        op: u8,
        kv_cache_request: KVCacheRequest,
        done_tx: flume::Sender<Result<KVCacheResponse, KVCacheRequest>>,
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

impl Encode for KVCachePacket {
    /// Encode the kv cache packet into a byte buffer.
    fn encode(&self, buffer: &mut BytesMut) {
        self.request.encode(buffer);
    }
}

#[async_trait]
impl Packet for KVCachePacket {
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
    fn set_resp_data(&mut self, data: &[u8]) -> Result<(), RpcError> {
        let response_type = RespType::from_u8(self.op)?;
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
            _ => {
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
        self.request.actual_size()
    }
}

#[cfg(test)]
mod test {
    use crate::distribute_kv_cache::rpc::utils::u64_to_usize;

    use super::*;

    #[test]
    fn test_file_block_request_encode_decode() {
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

        let mut buffer = BytesMut::new();
        request.encode(&mut buffer);

        let decoded_request = FileBlockRequest::decode(&buffer).unwrap();

        assert_eq!(decoded_request.file_id, file_id);
        assert_eq!(decoded_request.block_id, block_id);
        assert_eq!(decoded_request.block_size, block_size);
        assert_eq!(decoded_request.block_version, block_version);
        assert_eq!(decoded_request.hash_ring_version, hash_ring_version);
    }

    #[test]
    fn test_file_block_response_encode_decode() {
        let file_id = 123;
        let block_id = 456;
        let block_size = 789;
        let block_version = 10;
        let hash_ring_version = 20;
        let status = StatusCode::Success;
        let data = vec![1, 2, 3, 4, 5];

        let response = FileBlockResponse {
            file_id,
            block_id,
            block_size,
            block_version,
            hash_ring_version,
            status,
            data: data.clone(),
        };

        let mut buffer = BytesMut::new();
        response.encode(&mut buffer);

        let decoded_response = FileBlockResponse::decode(&buffer).unwrap();

        assert_eq!(decoded_response.file_id, file_id);
        assert_eq!(decoded_response.block_id, block_id);
        assert_eq!(decoded_response.block_size, block_size);
        assert_eq!(decoded_response.block_version, block_version);
        assert_eq!(decoded_response.hash_ring_version, hash_ring_version);
        assert_eq!(decoded_response.status, status);
        assert_eq!(decoded_response.data, data);
    }

    #[test]
    fn test_file_block_packet_encode() {
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
        let packet = FileBlockPacket::new(&request, done_tx);

        let mut buffer = BytesMut::new();
        packet.encode(&mut buffer);

        let expected_buffer_len = u64_to_usize(packet.get_req_len());
        assert_eq!(buffer.len(), expected_buffer_len);
    }
}
