use std::mem;

use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use tracing::error;

use crate::async_fuse::util::usize_to_u64;

use super::{
    error::RpcError,
    packet::{Decode, Encode, Packet},
    utils::get_u64_from_buf,
};

/// The request type of the request.
#[derive(Debug, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
pub enum ReqType {
    /// The keep alive request.
    KeepAliveRequest = 0,
    /// The file block request.
    FileBlockRequest = 1,
}

/// The operation type of the response.
#[derive(Debug, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
pub enum RespType {
    /// The keep alive response.
    KeepAliveResponse = 0,
    /// The file block response.
    FileBlockResponse = 1,
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
        buf.put_u8(self.status.into());
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
            Some(&status) => match StatusCode::try_from(status) {
                Ok(status) => status,
                Err(_) => return Err(RpcError::InternalError("Invalid status code".to_owned())),
            },
            None => return Err(RpcError::InternalError("Invalid status code".to_owned())),
        };
        let data = buf.get(41..).unwrap_or(&[]).to_vec();
        let data_len = usize_to_u64(data.len());
        if data_len != block_size {
            return Err(RpcError::InternalError(format!(
                "Insufficient block size: {block_size}, data len is : {data_len}"
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
#[derive(Debug, PartialEq, Eq, Clone, Copy, IntoPrimitive, TryFromPrimitive)]
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
        block_request: FileBlockRequest,
        done_tx: flume::Sender<Result<FileBlockResponse, FileBlockRequest>>,
    ) -> Self {
        Self {
            // Will be auto set by client
            seq: 0,
            // Set operation
            op: ReqType::FileBlockRequest.into(),
            request: block_request,
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
                            error!("Failed to set result: {:?}", err);
                        }
                    }
                } else {
                    error!("Failed to set result: response is None");
                    match self.done_tx.send_async(Err(self.request.clone())).await {
                        Ok(()) => {}
                        Err(err) => {
                            error!("Failed to set result: {:?}", err);
                        }
                    }
                }
            }
            Err(err) => {
                error!("Failed to set result: {:?}", err);
                match self.done_tx.send_async(Err(self.request.clone())).await {
                    Ok(()) => {}
                    Err(err) => {
                        error!("Failed to set result: {:?}", err);
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
#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod test {
    use crate::storage::cache::rpc::utils::u64_to_usize;

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
        let block_size = 5;
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
        let block_size = 5;
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
        let packet = FileBlockPacket::new(request, done_tx);

        let mut buffer = BytesMut::new();
        packet.encode(&mut buffer);

        let expected_buffer_len = u64_to_usize(packet.get_req_len());
        assert_eq!(buffer.len(), expected_buffer_len);
    }
}
