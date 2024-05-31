use std::{sync::Arc, time::Duration};

use bytes::BytesMut;
use file_async_rpc::{client::RpcClient, common::TimeoutOptions, connect_timeout, error::RpcError, message::ReqType, packet::{Encode, Packet, PacketStatus, ReqHeader, RespHeader}, server::{FileBlockRpcServerHandler, RpcServer, RpcServerConnectionHandler}, workerpool::{Job, WorkerPool}};
use tokio::{net::TcpStream, sync::mpsc, time::Instant};
use tonic::async_trait;
use tracing::{debug, error, info};

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

impl FileBlockPacket {
    pub fn new(op: u8) -> Self {
        Self { seq:0, op, status:PacketStatus::Pending, buffer: BytesMut::with_capacity(MAX_PACKET_SIZE)}
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
        debug!("Setting request data");

        Ok(())
    }

    fn get_req_data(&self) -> Result<Vec<u8>, RpcError<String>> {
        // Try to get the request data
        debug!("Getting request data");

        // Return a 4MB vec
        Ok(vec![0u8; MAX_PACKET_SIZE])
    }

    fn set_resp_data(&mut self, _data: &[u8]) -> Result<(), RpcError<String>> {
        // Try to set the response data
        debug!("Setting response data");

        Ok(())
    }

    fn get_resp_data(&self) -> Result<Vec<u8>, RpcError<String>> {
        // Try to get the response data
        debug!("Getting response data");

        // Return a 4MB vec
        Ok(vec![0u8; MAX_PACKET_SIZE])
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
}

/// File block handler
impl FileBlockHandler {
    pub fn new( done_tx: mpsc::Sender<Vec<u8>>) -> Self {
        Self { done_tx }
    }
}

#[async_trait]
impl Job for FileBlockHandler {
    async fn run(&self) {
        self.done_tx.send(vec![0u8; 4]).await.unwrap();
    }
}

/// File block server handler
#[derive(Clone)]
pub struct FileBlockServerHandler {
    worker_pool: Arc<WorkerPool>,
}

impl FileBlockServerHandler {
    pub fn new(worker_pool: Arc<WorkerPool>) -> Self {
        Self { worker_pool }
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
                    // Submit the handler to the worker pool
                    // When the handler is done, send the response to the done channel
                    // Response need to contain the response header and body
                    let handler = FileBlockHandler::new(done_tx.clone());
                    if let Ok(_) = self
                        .worker_pool
                        .submit_job(Box::new(handler))
                        .map_err(|err| {
                            debug!("Failed to submit job: {:?}", err);
                        })
                    {
                        debug!("Submitted job to worker pool");
                    }

                    // Create a response packet
                    let resp_body_packet = FileBlockPacket::new(req_header.op).get_resp_data().unwrap();
                    let resp_header = RespHeader{
                        seq: req_header.seq,
                        op: req_header.op,
                        len: resp_body_packet.len() as u64,
                    };

                    let mut resp_packet = resp_header.encode();
                    resp_packet.extend(resp_body_packet);

                    done_tx.send(resp_packet).await.unwrap();
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
