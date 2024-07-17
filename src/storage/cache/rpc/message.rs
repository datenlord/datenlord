use std::mem;

use bytes::{BufMut, BytesMut};
use tracing::error;

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
}

impl ReqType {
    /// Convert u8 to `ReqType`
    pub fn from_u8(op: u8) -> Result<Self, RpcError> {
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
    pub fn from_u8(op: u8) -> Result<Self, RpcError> {
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
#[derive(Debug, Default, Clone)]
pub struct FileBlockRequest {
    /// The file ID.
    pub file_id: u64,
    /// The block ID.
    pub block_id: u64,
    /// The block size.
    pub block_size: u64,
}

impl Encode for FileBlockRequest {
    /// Encode the file block request into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.file_id.to_le());
        buf.put_u64(self.block_id.to_le());
        buf.put_u64(self.block_size.to_le());
    }
}

impl Decode for FileBlockRequest {
    /// Decode the byte buffer into a file block request.
    fn decode(buf: &[u8]) -> Result<Self, RpcError> {
        if buf.len() < 24 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let file_id = get_u64_from_buf(buf, 0)?;
        let block_id = get_u64_from_buf(buf, 8)?;
        let block_size = get_u64_from_buf(buf, 16)?;

        Ok(FileBlockRequest {
            file_id,
            block_id,
            block_size,
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
        match self.status {
            StatusCode::Success => buf.put_u8(0),
            StatusCode::NotFound => buf.put_u8(1),
            StatusCode::InternalError => buf.put_u8(2),
            StatusCode::VersionMismatch => buf.put_u8(3),
        }
        buf.put_u64(self.block_size.to_le());
        buf.put_slice(&self.data);
    }
}

impl Decode for FileBlockResponse {
    //// Decode the byte buffer into a file block response.
    fn decode(buf: &[u8]) -> Result<Self, RpcError> {
        if buf.len() < 17 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let file_id = get_u64_from_buf(buf, 0)?;
        let block_id = get_u64_from_buf(buf, 8)?;
        let status = match buf.get(16) {
            Some(&0) => StatusCode::Success,
            Some(&1) => StatusCode::NotFound,
            Some(&2) => StatusCode::InternalError,
            Some(&3) => StatusCode::VersionMismatch,
            _ => return Err(RpcError::InternalError("Invalid status code".to_owned())),
        };
        let block_size = get_u64_from_buf(buf, 17)?;
        let data = buf.get(25..).unwrap_or(&[]).to_vec();
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
    pub sender: flume::Sender<Result<FileBlockResponse, FileBlockRequest>>,
}

impl FileBlockPacket {
    /// Create a new file block packet.
    pub fn new(
        block_request: &FileBlockRequest,
        sender: flume::Sender<Result<FileBlockResponse, FileBlockRequest>>,
    ) -> Self {
        Self {
            // Will be auto set by client
            seq: 0,
            // Set operation
            op: ReqType::FileBlockRequest.to_u8(),
            request: block_request.clone(),
            timestamp: 0,
            response: None,
            sender,
        }
    }
}

impl Encode for FileBlockPacket {
    /// Encode the file block packet into a byte buffer.
    fn encode(&self, buffer: &mut BytesMut) {
        self.request.encode(buffer);
    }
}

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
    fn set_result(&self, status: Result<(), RpcError>) {
        match status {
            Ok(()) => {
                match self.response {
                    Some(ref resp) => {
                        let file_block_resp = FileBlockResponse {
                            file_id: resp.file_id,
                            block_id: resp.block_id,
                            block_size: resp.block_size,
                            status: StatusCode::Success,
                            data: resp.data.clone(),
                        };
                        match self.sender.send(Ok(file_block_resp)) {
                            Ok(()) => {}
                            Err(err) => {
                                error!("Failed to set result: {:?}", err);
                            }
                        }
                    }
                    None => {
                        error!("Failed to set result: response is None");
                        match self.sender.send(Err(self.request.clone())) {
                            Ok(()) => {}
                            Err(err) => {
                                error!("Failed to set result: {:?}", err);
                            }
                        }
                    }
                }
            }
            Err(err) => {
                error!("Failed to set result: {:?}", err);
                self.sender.send(Err(self.request.clone())).unwrap();
            }
        }
    }

    /// Get request size to construct request header
    fn get_req_len(&self) -> u64 {
        usize_to_u64(mem::size_of_val(&self.request))
    }
}