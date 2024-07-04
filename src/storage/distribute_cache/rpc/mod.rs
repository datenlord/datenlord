use std::sync::Once;

use tracing::level_filters::LevelFilter;
use tracing_subscriber::{
    filter, fmt::layer, layer::SubscriberExt, util::SubscriberInitExt, Layer,
};

/// This module contains the RPC server and client for the cache service.

/// The client module contains the client implementation for the cache service.
pub mod client;

/// The common module contains the shared structures and functions for the cache service.
pub mod common;

/// The error module contains the error types for the cache service.
pub mod error;

/// The message module contains the data structures shared between the client and server.
pub mod message;

/// The server module contains the server implementation for the cache service.
pub mod server;

/// The workerpool module contains the worker pool implementation for the cache service.
pub mod workerpool;

/// The packet module contains the packet encoding and decoding functions for the cache service.
pub mod packet;

/// The utils module contains the utility functions for the cache service.
#[macro_use]
pub mod utils;

/// Use for unit test to setup tracing
#[allow(dead_code)]
static INIT: Once = Once::new();

/// Set up once for tracing
#[allow(dead_code)]
fn setup() {
    // init tracing once
    INIT.call_once(|| {
        // Set the tracing log level to debug
        let filter =
            filter::Targets::new().with_target("datenlord::storage::cache", LevelFilter::DEBUG);
        tracing_subscriber::registry()
            .with(layer().with_filter(filter))
            .init();
    });
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
#[allow(clippy::indexing_slicing)]
mod tests {
    use std::net::TcpStream;
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use bytes::{BufMut, BytesMut};
    use client::RpcClient;
    use common::{ClientTimeoutOptions, ServerTimeoutOptions};
    use error::RpcError;
    use message::{
        decode_file_block_request, FileBlockRequest, FileBlockResponse, ReqType, RespType,
        StatusCode,
    };
    use packet::{Decode, Encode, Packet, ReqHeader, RespHeader};
    use server::{RpcServer, RpcServerConnectionHandler};
    use tokio::sync::mpsc;
    use tokio::time;
    use tracing::{debug, error};
    use utils::u64_to_usize;
    use workerpool::{Job, WorkerPool};

    use crate::async_fuse::util::usize_to_u64;
    use crate::storage::distribute_cache::rpc::packet::PacketStatus;

    use super::*;

    /// The handler for the RPC file block request.
    #[derive(Debug)]
    pub struct FileBlockHandler {
        /// The request header.
        header: ReqHeader,
        /// The file block request.
        request: FileBlockRequest,
        /// The channel for sending the response.
        done_tx: mpsc::Sender<Vec<u8>>,
    }

    impl FileBlockHandler {
        /// Create a new file block handler.
        #[must_use]
        pub fn new(
            header: ReqHeader,
            request: FileBlockRequest,
            done_tx: mpsc::Sender<Vec<u8>>,
        ) -> Self {
            Self {
                header,
                request,
                done_tx,
            }
        }
    }

    #[async_trait]
    impl Job for FileBlockHandler {
        async fn run(&self) {
            // Mock: serve block request and send response
            let size = self.request.block_size;

            // Prepare response body
            // Mock: response body is all zeros
            let file_block_resp = FileBlockResponse {
                file_id: self.request.file_id,
                block_id: self.request.block_id,
                block_size: size,
                block_version: self.request.block_version,
                hash_ring_version: self.request.hash_ring_version,
                status: StatusCode::Success,
                data: vec![0_u8; u64_to_usize(size)],
            };
            let resp_body = file_block_resp.encode();
            // Prepare response header
            let resp_header = RespHeader {
                seq: self.header.seq,
                op: RespType::FileBlockResponse.to_u8(),
                len: usize_to_u64(resp_body.len()),
            };
            let mut resp_buffer = resp_header.encode();
            // Combine response header and body
            resp_buffer.extend_from_slice(&resp_body);

            // Send response to the done channel
            match self.done_tx.send(resp_buffer).await {
                Ok(()) => {
                    debug!("Sent response to done channel");
                }
                Err(err) => {
                    error!("Failed to send response to done channel: {:?}", err);
                }
            }
        }
    }

    /// The file block handler for the RPC server.
    #[derive(Clone, Debug)]
    pub struct FileBlockRpcServerHandler {
        /// The worker pool for the RPC server.
        worker_pool: Arc<WorkerPool>,
    }

    impl FileBlockRpcServerHandler {
        /// Create a new file block RPC server handler.
        #[must_use]
        pub fn new(worker_pool: Arc<WorkerPool>) -> Self {
            Self { worker_pool }
        }
    }

    #[async_trait]
    impl RpcServerConnectionHandler for FileBlockRpcServerHandler {
        async fn dispatch(
            &self,
            req_header: ReqHeader,
            req_buffer: &[u8],
            done_tx: mpsc::Sender<Vec<u8>>,
        ) {
            // Dispatch the handler for the connection
            if let Ok(req_type) = ReqType::from_u8(req_header.op) {
                if let ReqType::FileBlockRequest = req_type {
                    // Try to read the request body
                    // Decode the request body
                    let req_body = match decode_file_block_request(req_buffer) {
                        Ok(req) => req,
                        Err(err) => {
                            debug!("Failed to decode file block request: {:?}", err);
                            return;
                        }
                    };

                    debug!(
                        "FileBlockRpcServerHandler: Received file block request: {:?}",
                        req_body
                    );

                    // File block request
                    // Submit the handler to the worker pool
                    // When the handler is done, send the response to the done channel
                    // Response need to contain the response header and body
                    let handler = FileBlockHandler::new(req_header, req_body, done_tx.clone());
                    if let Ok(()) = self
                        .worker_pool
                        .submit_job(Box::new(handler))
                        .map_err(|err| {
                            debug!("Failed to submit job: {:?}", err);
                        })
                    {
                        debug!("Submitted job to worker pool");
                    }
                } else {
                    debug!(
                        "FileBlockRpcServerHandler: Inner request type is not matched: {:?}",
                        req_header.op
                    );
                }
            }
        }
    }

    #[derive(Debug, Clone)]
    pub struct TestFilePacket {
        pub seq: u64,
        pub op: u8,
        pub status: PacketStatus,
        pub buffer: BytesMut,
    }

    impl TestFilePacket {
        pub fn new(block_request: &FileBlockRequest) -> Self {
            let block_request_buf = block_request.encode();
            let mut buffer = BytesMut::new();
            buffer.extend_from_slice(block_request_buf.as_slice());
            Self {
                // Will be auto set by client
                seq: 0,
                // Set operation
                op: ReqType::FileBlockRequest.to_u8(),
                status: PacketStatus::Pending,
                buffer,
            }
        }
    }

    impl Packet for TestFilePacket {
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

        fn set_req_data(&mut self, _data: &[u8]) -> Result<(), RpcError<String>> {
            // Try to set the request data
            Ok(())
        }

        fn get_req_data(&self) -> Result<Vec<u8>, RpcError<String>> {
            // Try to get the request data
            // check current buffer is req
            let req = FileBlockRequest::decode(&self.buffer)?;

            // Return a 4MB vec
            Ok(req.encode())
        }

        fn set_resp_data(&mut self, data: &[u8]) -> Result<(), RpcError<String>> {
            // Try to set the response data
            self.buffer.clear();
            self.buffer.put(data);
            // Make sure the padding data is invisible

            Ok(())
        }

        fn get_resp_data(&self) -> Result<Vec<u8>, RpcError<String>> {
            // Try to get the response data, check current data is ready
            let resp = FileBlockResponse::decode(&self.buffer)?;
            // Return a 4MB vec
            let resp_vec = resp.encode();
            Ok(resp_vec)
        }

        fn status(&self) -> PacketStatus {
            self.status
        }

        fn set_status(&mut self, status: PacketStatus) {
            self.status = status;
        }
    }

    /// Check if the port is in use
    fn is_port_in_use(addr: &str) -> bool {
        if let Ok(stream) = TcpStream::connect(addr) {
            // Port is in use
            drop(stream);
            true
        } else {
            // Port is not in use
            false
        }
    }

    #[tokio::test]
    async fn test_send_and_recv_packet() {
        setup();
        // Setup server
        let addr = "127.0.0.1:2788";
        let pool = Arc::new(WorkerPool::new(4, 100));
        let handler = FileBlockRpcServerHandler::new(Arc::clone(&pool));
        let mut server = RpcServer::new(&ServerTimeoutOptions::default(), 4, 100, handler);
        server.listen(addr).await.unwrap();
        time::sleep(Duration::from_secs(1)).await;
        assert!(is_port_in_use(addr));
        time::sleep(Duration::from_secs(1)).await;

        // Create a client
        let timeout_options = ClientTimeoutOptions {
            read_timeout: Duration::from_secs(100),
            write_timeout: Duration::from_secs(100),
            idle_timeout: Duration::from_secs(100),
            keep_alive_timeout: Duration::from_secs(20),
        };
        let connect_stream = connect_timeout!(addr, timeout_options.read_timeout)
            .await
            .unwrap();
        let rpc_client = RpcClient::<TestFilePacket>::new(connect_stream, &timeout_options);
        rpc_client.start_recv();

        // Send ping
        rpc_client.ping().await.unwrap();

        // Send file block request
        let block_request = FileBlockRequest {
            block_id: 0,
            block_size: 4096,
            file_id: 0,
            block_version: 0,
            hash_ring_version: 0,
        };
        let mut packet = TestFilePacket::new(&block_request);
        rpc_client.send_request(&mut packet).await.unwrap();

        loop {
            match rpc_client.recv_response(&mut packet) {
                Ok(()) => {
                    let resp = FileBlockResponse::decode(&packet.get_resp_data().unwrap()).unwrap();

                    assert_eq!(resp.file_id, 0);
                    assert_eq!(resp.block_id, 0);
                    assert_eq!(resp.block_size, 4096);
                    assert_eq!(resp.status, StatusCode::Success);
                    assert_eq!(resp.data.len(), 4096);
                    break;
                }
                Err(err) => {
                    error!("Failed to receive response: {:?}", err);
                    time::sleep(Duration::from_secs(1)).await;
                }
            }
        }

        server.stop();
    }
}
