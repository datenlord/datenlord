use std::mem;

use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use tracing::warn;

use crate::async_fuse::util::usize_to_u64;

use super::{
    error::RpcError,
    packet::{Decode, Encode, Packet},
    utils::get_u64_from_buf,
};

/// The request type of the request.
#[derive(Debug)]
pub enum ReqType {
    /// The keep alive request.
    KeepAliveRequest,
    /// The file block request.
    FileBlockRequest,
    /// The block allocate request.
    KVCacheIdAllocateRequest,
    /// The kv cache index get request.
    KVCacheIndexMatchRequest,
    /// The kv cache index put request.
    KVCacheIndexInsertRequest,
    /// The kv cache index remove request.
    KVCacheIndexRemoveRequest,
    /// The kv block get request.
    KVBlockGetRequest,
    /// The kv block put request.
    KVBlockPutRequest,
}

impl ReqType {
    /// Convert u8 to `ReqType`
    pub fn from_u8(op: u8) -> Result<Self, RpcError> {
        match op {
            0 => Ok(Self::KeepAliveRequest),
            1 => Ok(Self::FileBlockRequest),
            2 => Ok(Self::KVCacheIdAllocateRequest),
            3 => Ok(Self::KVCacheIndexMatchRequest),
            4 => Ok(Self::KVCacheIndexInsertRequest),
            5 => Ok(Self::KVCacheIndexRemoveRequest),
            6 => Ok(Self::KVBlockGetRequest),
            7 => Ok(Self::KVBlockPutRequest),
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
            Self::KVCacheIndexInsertRequest => 4,
            Self::KVCacheIndexRemoveRequest => 5,
            Self::KVBlockGetRequest => 6,
            Self::KVBlockPutRequest => 7,
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
    KVBlockPutResponse,
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
            7 => Ok(Self::KVBlockPutResponse),
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
            Self::KVBlockPutResponse => 7,
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
    block_size: u64,
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

/// The response to allocate a global kv cache id.
#[derive(Debug, Default, Clone)]
pub struct KVCacheIdAllocateResponse {
    /// The kv block size.
    block_size: u64,
    /// The kv cache id.
    kv_cache_id: u64,
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
    block_size: u64,
    /// The kv cache key.
    kv_cache_key: Vec<u8>,
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

/// The response to match kv cache index.
#[derive(Debug, Default, Clone)]
pub struct KVCacheIndexMatchResponse {
    /// The kv block size.
    block_size: u64,
    /// The kv cache id, match success return kv cache id, otherwise return 0.
    /// TODO: support partial match
    kv_cache_id: u64,
}

impl Encode for KVCacheIndexMatchResponse {
    /// Encode the kv cache index match response into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.block_size.to_le());
        buf.put_u64(self.kv_cache_id.to_le());
    }
}

impl Decode for KVCacheIndexMatchResponse {
    /// Decode the byte buffer into a kv cache index match response.
    fn decode(buf: &[u8]) -> Result<Self, RpcError> {
        if buf.len() < 16 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let block_size = get_u64_from_buf(buf, 0)?;
        let kv_cache_id = get_u64_from_buf(buf, 8)?;
        Ok(KVCacheIndexMatchResponse {
            block_size,
            kv_cache_id,
        })
    }
}

/// The request to insert kv cache index.
#[derive(Debug, Default, Clone)]
pub struct KVCacheIndexInsertRequest {
    /// The kv block size.
    block_size: u64,
    /// The kv cache id, pair with kv cache key.
    kv_cache_id: u64,
    /// The kv cache key.
    kv_cache_key: Vec<u8>,
}

impl Encode for KVCacheIndexInsertRequest {
    /// Encode the kv cache index insert request into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.block_size.to_le());
        buf.put_slice(&self.kv_cache_key);
        buf.put_u64(self.kv_cache_id.to_le());
    }
}

impl Decode for KVCacheIndexInsertRequest {
    /// Decode the byte buffer into a kv cache index insert request.
    fn decode(buf: &[u8]) -> Result<Self, RpcError> {
        if buf.len() < 16 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let block_size = get_u64_from_buf(buf, 0)?;
        let kv_cache_key = buf[8..].to_vec();
        let kv_cache_id = get_u64_from_buf(buf, 8 + kv_cache_key.len())?;
        Ok(KVCacheIndexInsertRequest {
            block_size,
            kv_cache_key,
            kv_cache_id,
        })
    }
}

/// The response to insert kv cache index.
#[derive(Debug, Default, Clone)]
pub struct KVCacheIndexInsertResponse {
    /// The kv block size.
    block_size: u64,
    /// The kv cache id.
    kv_cache_id: u64,
    /// The status of the response.
    status: StatusCode,
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

/// The request to remove kv cache index.
#[derive(Debug, Default, Clone)]
pub struct KVCacheIndexRemoveRequest {
    /// The kv block size.
    block_size: u64,
    /// The kv cache key.
    kv_cache_key: Vec<u8>,
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

/// The response to remove kv cache index.
#[derive(Debug, Default, Clone)]
pub struct KVCacheIndexRemoveResponse {
    /// The kv block size.
    block_size: u64,
    /// The status of the response.
    status: StatusCode,
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
    block_size: u64,
    /// The kv cache id.
    kv_cache_id: u64,
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

/// The response to get kv block.
#[derive(Debug, Default, Clone)]
pub struct KVBlockGetResponse {
    /// The kv block size.
    block_size: u64,
    /// The kv cache id.
    kv_cache_id: u64,
    /// The status of the response.
    status: StatusCode,
    /// The data of the block.
    data: Vec<u8>,
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
    block_size: u64,
    /// The kv cache id.
    kv_cache_id: u64,
    /// The data of the block.
    data: Vec<u8>,
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

/// The response to put kv block.
#[derive(Debug, Default, Clone)]
pub struct KVBlockPutResponse {
    /// The kv block size.
    block_size: u64,
    /// The kv cache id.
    kv_cache_id: u64,
    /// The status of the response.
    status: StatusCode,
}

impl Encode for KVBlockPutResponse {
    /// Encode the kv block put response into a byte buffer.
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

impl Decode for KVBlockPutResponse {
    /// Decode the byte buffer into a kv block put response.
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
        Ok(KVBlockPutResponse {
            block_size,
            kv_cache_id,
            status,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::storage::distribute_kv_cache::rpc::utils::u64_to_usize;

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
