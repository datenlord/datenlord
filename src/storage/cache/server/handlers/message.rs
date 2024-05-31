use serde::{Serialize, Deserialize};
use std::fmt;

#[derive(Debug, Serialize, Deserialize)]
pub enum StatusCode {
    Ok,
    Error,
    NotFound,
    Unauthorized,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileBlockRequest {
    /// The file ID.
    pub file_id: u64,
    /// The block ID.
    pub block_id: u64,
    /// The block size.
    pub block_size: u64,
    /// Block version
    pub version: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileBlockResponse {
    /// The file ID.
    pub file_id: u64,
    /// The block ID.
    pub block_id: u64,
    /// The block size.
    pub block_size: u64,
    /// Block version
    pub version: u64,
    /// The status of the response.
    pub status: StatusCode,
    /// The data of the block.
    pub data: Vec<u8>,
}

impl fmt::Display for StatusCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StatusCode::Ok => write!(f, "OK"),
            StatusCode::Error => write!(f, "Error"),
            StatusCode::NotFound => write!(f, "Not Found"),
            StatusCode::Unauthorized => write!(f, "Unauthorized"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_deserialize() {
        let request = FileBlockRequest {
            file_id: 1,
            block_id: 42,
            block_size: 1024,
            version: 1,
        };

        let response = FileBlockResponse {
            file_id: 1,
            block_id: 42,
            block_size: 1024,
            version: 1,
            status: StatusCode::Ok,
            data: vec![1, 2, 3, 4, 5],
        };

        let serialized_request = serde_json::to_string(&request).unwrap();
        let serialized_response = serde_json::to_string(&response).unwrap();

        println!("Serialized request: {}", serialized_request);
        println!("Serialized response: {}", serialized_response);

        let deserialized_request: FileBlockRequest = serde_json::from_str(&serialized_request).unwrap();
        let deserialized_response: FileBlockResponse = serde_json::from_str(&serialized_response).unwrap();

        println!("Deserialized request: {:?}", deserialized_request);
        println!("Deserialized response: {:?}", deserialized_response);
    }
}