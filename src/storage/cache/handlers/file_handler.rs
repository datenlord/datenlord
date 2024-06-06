use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::BytesMut;
use file_async_rpc::{
    client::RpcClient,
    common::TimeoutOptions,
    connect_timeout,
    error::RpcError,
    message::ReqType,
    packet::{Encode, Packet, PacketStatus, ReqHeader, RespHeader},
    server::{FileBlockRpcServerHandler, RpcServer, RpcServerConnectionHandler},
    workerpool::{Job, WorkerPool},
};
use serde::Deserialize;
use tokio::{net::TcpStream, sync::mpsc, time::Instant};
use tonic::async_trait;
use tracing::{debug, error, info};

use crate::storage::cache::server::handlers::MAX_PACKET_SIZE;

use super::message::FileBlockRequest;


/// File block handler
#[derive(Debug, Clone)]
pub struct FileBlockPacket {
    /// Sequence number
    pub seq: u64,
    /// Operation code
    pub op: u8,
    /// Packet status
    pub status: PacketStatus,
    /// Buffer
    pub buffer: BytesMut,
}

/// FileBlockPacket for client to control the req/resp data
impl FileBlockPacket {
    pub fn new(op: u8) -> Self {
        Self {
            seq: 0,
            op,
            status: PacketStatus::Pending,
            buffer: BytesMut::with_capacity(MAX_PACKET_SIZE),
        }
    }
}

impl Packet for FileBlockPacket {
    fn seq(&self) -> u64 {
        self.seq
    }

    fn set_seq(&mut self, seq: u64) {
        self.seq = seq;
    }

    fn op(&self) -> u8 {
        self.op
    }

    fn set_op(&mut self, op: u8) {
        self.op = op;
    }

    fn set_req_data(&mut self, data: &[u8]) -> Result<(), RpcError<String>> {
        // Try to set the request data
        self.buffer.extend_from_slice(data);
        self.status = PacketStatus::Request;
        Ok(())
    }

    fn get_req_data(&self) -> Result<Vec<u8>, RpcError<String>> {
        // Try to serialize the request data
        debug!("Getting request data");
        Ok(self.buffer.to_vec())
    }

    fn set_resp_data(&mut self, data: &[u8]) -> Result<(), RpcError<String>> {
        // Try to set the response data
        debug!("Setting response data");
        self.buffer.extend_from_slice(data);
        self.status = PacketStatus::Response;
        Ok(())
    }

    fn get_resp_data(&self) -> Result<Vec<u8>, RpcError<String>> {
        // Try to get the response data
        debug!("Getting response data");
        Ok(self.buffer.to_vec())
    }

    fn status(&self) -> PacketStatus {
        self.status
    }

    fn set_status(&mut self, status: PacketStatus) {
        self.status = status;
    }
}

/// File block handler
pub struct FileBlockHandler {
    done_tx: mpsc::Sender<Vec<u8>>,
    file_block_request: FileBlockRequest,
    local_cache_manager: Arc<LocalCacheManager>,
}

/// File block handler
impl FileBlockHandler {
    pub fn new(done_tx: mpsc::Sender<Vec<u8>>, file_block_request: FileBlockRequest, local_cache_manager: Arc<LocalCacheManager>) -> Self {
        Self { done_tx, file_block_request, local_cache_manager }
    }
}

#[async_trait]
impl Job for FileBlockHandler {
    async fn run(&self) {
        let block_id = self.file_block_request.block_id;
        let block_size = self.file_block_request.block_size;
        let block_version = self.file_block_request.version;

        // Retrieve block from local
        match self.local_cache_manager.read(block_id, block_size, block_version).await {
            Ok(data) => {
                // Create a response packet
                let resp_body_packet =
                    FileBlockPacket::new(ReqType::FileBlockRequest as u8).get_resp_data().unwrap();
                let resp_header = RespHeader {
                    seq: 0,
                    op: ReqType::FileBlockRequest as u8,
                    len: resp_body_packet.len() as u64,
                };

                let mut resp_packet = resp_header.encode();
                resp_packet.extend(resp_body_packet);

                self.done_tx.send(resp_packet).await.unwrap();
            }
            Err(err) => {
                error!("Failed to read block from local cache: {:?}", err);
            }
        }
    }
}

/// File block server handler
#[derive(Clone)]
pub struct FileBlockServerHandler {
    worker_pool: Arc<WorkerPool>,
    local_cache_manager: Arc<LocalCacheManager>,
}

impl FileBlockServerHandler {
    pub fn new(worker_pool: Arc<WorkerPool>, local_cache_manager: Arc<LocalCacheManager>) -> Self {
        Self { worker_pool, local_cache_manager }
    }
}

#[async_trait]
impl RpcServerConnectionHandler for FileBlockServerHandler {
    async fn dispatch(
        &self,
        req_header: ReqHeader,
        req_buffer: &[u8],
        done_tx: mpsc::Sender<Vec<u8>>,
    ) {
        // Dispatch the handler for the connection
        if let Ok(req_type) = ReqType::from_u8(req_header.op) {
            match req_type {
                ReqType::FileBlockRequest => {
                    // Try to read the request body
                    // Decode the request body
                    // File block request
                    let file_block_request = FileBlockRequest::decode(req_buffer).unwrap();
                    let handler = FileBlockHandler::new(done_tx.clone(), file_block_request, Arc::clone(local_cache_manager));
                    // Submit the handler to the worker pool
                    // When the handler is done, send the response to the done channel
                    // Response need to contain the response header and body
                    if let Ok(_) = self
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
                        "FileBlockRpcServerHandler: Inner request type is not matched: {:?}",
                        req_header.op
                    );
                }
            }
        }
    }
}
