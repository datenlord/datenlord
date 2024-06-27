use super::{
    error::RpcError,
    packet::{Decode, Encode, Packet, PacketStatus},
};

/// Impl the keep alive request for Packet trait
#[derive(Debug, Clone)]
pub struct KeepAlivePacket {
    /// The sequence number of the request.
    pub seq: u64,
    /// The operation type of the request.
    pub op: u8,
    /// The status of the request.
    pub status: PacketStatus,
}

impl Default for KeepAlivePacket {
    fn default() -> Self {
        Self::new()
    }
}

impl KeepAlivePacket {
    /// Create a new keep alive packet.
    #[must_use] pub fn new() -> Self {
        Self {
            seq: 0,
            op: 0,
            status: PacketStatus::Pending,
        }
    }
}

impl Packet for KeepAlivePacket {
    /// Get the packet seq number
    fn seq(&self) -> u64 {
        self.seq
    }

    /// Set the packet seq number
    fn set_seq(&mut self, seq: u64) {
        self.seq = seq;
    }

    /// Get packet type
    fn op(&self) -> u8 {
        self.op
    }

    /// Set packet type
    fn set_op(&mut self, op: u8) {
        self.op = op;
    }

    /// Set the raw request data
    fn set_req_data(&mut self, _data: &[u8]) -> Result<(), RpcError<String>> {
        Ok(())
    }

    /// Get the raw request data
    fn get_req_data(&self) -> Result<Vec<u8>, RpcError<String>> {
        Ok(Vec::new())
    }

    /// Set the raw response data
    fn set_resp_data(&mut self, _data: &[u8]) -> Result<(), RpcError<String>> {
        Ok(())
    }

    /// Get the raw response data
    fn get_resp_data(&self) -> Result<Vec<u8>, RpcError<String>> {
        Ok(Vec::new())
    }

    /// Get the packet status
    fn status(&self) -> PacketStatus {
        self.status
    }

    /// Set the packet status
    fn set_status(&mut self, status: PacketStatus) {
        self.status = status;
    }
}

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
    #[must_use] pub fn to_u8(&self) -> u8 {
        match self {
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
    #[must_use] pub fn to_u8(&self) -> u8 {
        match self {
            Self::KeepAliveResponse => 0,
            Self::FileBlockResponse => 1,
        }
    }
}

/// Common data structures shared between the client and server.
#[derive(Debug, Default)]
pub struct FileBlockRequest {
    /// The sequence number of the request.
    pub seq: u64,
    /// The file ID.
    pub file_id: u64,
    /// The block ID.
    pub block_id: u64,
    /// The block size.
    pub block_size: u64,
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
        return Err(RpcError::InternalError("Insufficient bytes".to_string()));
    }
    let seq = u64::from_be_bytes(buf[0..8].try_into().unwrap());
    let file_id = u64::from_be_bytes(buf[8..16].try_into().unwrap());
    let block_id = u64::from_be_bytes(buf[16..24].try_into().unwrap());
    let block_size = u64::from_be_bytes(buf[24..32].try_into().unwrap());

    Ok(FileBlockRequest {
        seq,
        file_id,
        block_id,
        block_size,
    })
}

/// Encode the file block request into a buffer.
#[must_use] pub fn encode_file_block_request(req: &FileBlockRequest) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend(req.seq.to_be_bytes());
    bytes.extend(req.file_id.to_be_bytes());
    bytes.extend(req.block_id.to_be_bytes());
    bytes.extend(req.block_size.to_be_bytes());
    bytes
}

/// The response to a file block request.
#[derive(Debug, Default)]
pub struct FileBlockResponse {
    /// The sequence number of the response.
    pub seq: u64,
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
    if buf.len() < 32 {
        return Err(RpcError::InternalError("Insufficient bytes".to_string()));
    }
    let seq = u64::from_be_bytes(
        buf[0..8]
            .try_into()
            .map_err(|_| RpcError::InternalError("Failed to convert bytes".to_string()))?,
    );
    let file_id = u64::from_be_bytes(
        buf[8..16]
            .try_into()
            .map_err(|_| RpcError::InternalError("Failed to convert bytes".to_string()))?,
    );
    let block_id = u64::from_be_bytes(
        buf[16..24]
            .try_into()
            .map_err(|_| RpcError::InternalError("Failed to convert bytes".to_string()))?,
    );
    let status = match buf[24] {
        0 => StatusCode::Success,
        1 => StatusCode::NotFound,
        2 => StatusCode::InternalError,
        3 => StatusCode::VersionMismatch,
        _ => return Err(RpcError::InternalError("Invalid status code".to_string())),
    };
    let block_size = u64::from_be_bytes(
        buf[25..33]
            .try_into()
            .map_err(|_| RpcError::InternalError("Failed to convert bytes".to_string()))?,
    );
    let data = buf[33..].to_vec();

    Ok(FileBlockResponse {
        seq,
        file_id,
        block_id,
        block_size,
        status,
        data,
    })
}

/// Encode the file block response into a buffer.
#[must_use] pub fn encode_file_block_response(resp: &FileBlockResponse) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend(resp.seq.to_be_bytes());
    bytes.extend(resp.file_id.to_be_bytes());
    bytes.extend(resp.block_id.to_be_bytes());
    match resp.status {
        StatusCode::Success => bytes.push(0),
        StatusCode::NotFound => bytes.push(1),
        StatusCode::InternalError => bytes.push(2),
        StatusCode::VersionMismatch => bytes.push(3),
    }
    bytes.extend(resp.block_size.to_be_bytes());
    bytes.extend(&resp.data);
    bytes
}

/// The status code of the response.
#[derive(Debug)]
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
