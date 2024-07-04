use bytes::{BufMut, BytesMut};

use crate::async_fuse::util::usize_to_u64;

use super::{
    error::RpcError,
    packet::{Decode, Encode},
    utils::get_u64_from_buf,
};

/// The request type of the request.
#[derive(Debug)]
pub enum ReqType {
    /// The keep alive request.
    KeepAliveRequest,
    /// The file block request.
    FileBlockRequest,
}

impl ReqType {
    /// Convert u8 to `ReqType`
    pub fn from_u8(op: u8) -> Result<Self, RpcError<String>> {
        match op {
            0 => Ok(Self::KeepAliveRequest),
            1 => Ok(Self::FileBlockRequest),
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
}

impl RespType {
    /// Convert u8 to `RespType`
    pub fn from_u8(op: u8) -> Result<Self, RpcError<String>> {
        match op {
            0 => Ok(Self::KeepAliveResponse),
            1 => Ok(Self::FileBlockResponse),
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
        }
    }
}

/// Common data structures shared between the client and server.
#[derive(Debug, Default)]
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
    fn encode(&self) -> Vec<u8> {
        encode_file_block_request(self)
    }
}

impl Decode for FileBlockRequest {
    /// Decode the byte buffer into a file block request.
    fn decode(buf: &[u8]) -> Result<Self, RpcError<String>> {
        decode_file_block_request(buf)
    }
}

/// Decode the file block request from the buffer.
pub fn decode_file_block_request(buf: &[u8]) -> Result<FileBlockRequest, RpcError<String>> {
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

/// Encode the file block request into a buffer.
#[must_use]
pub fn encode_file_block_request(req: &FileBlockRequest) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put_u64(req.file_id.to_be());
    buf.put_u64(req.block_id.to_be());
    buf.put_u64(req.block_size.to_be());
    buf.put_u64(req.block_version.to_be());
    buf.put_u64(req.hash_ring_version.to_be());
    buf.to_vec()
}

/// The response to a file block request.
#[derive(Debug, Default)]
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
    fn encode(&self) -> Vec<u8> {
        encode_file_block_response(self)
    }
}

impl Decode for FileBlockResponse {
    //// Decode the byte buffer into a file block response.
    fn decode(buf: &[u8]) -> Result<Self, RpcError<String>> {
        decode_file_block_response(buf)
    }
}

/// Decode the file block response from the buffer.
pub fn decode_file_block_response(buf: &[u8]) -> Result<FileBlockResponse, RpcError<String>> {
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

/// Encode the file block response into a buffer.
#[must_use]
pub fn encode_file_block_response(resp: &FileBlockResponse) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put_u64(resp.file_id.to_be());
    buf.put_u64(resp.block_id.to_be());
    buf.put_u64(resp.block_size.to_be());
    buf.put_u64(resp.block_version.to_be());
    buf.put_u64(resp.hash_ring_version.to_be());
    match resp.status {
        StatusCode::Success => buf.put_u8(0),
        StatusCode::NotFound => buf.put_u8(1),
        StatusCode::InternalError => buf.put_u8(2),
        StatusCode::VersionMismatch => buf.put_u8(3),
    }
    buf.put_slice(&resp.data);

    buf.to_vec()
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
