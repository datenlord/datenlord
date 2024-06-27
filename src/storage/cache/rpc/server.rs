use std::{
    cell::UnsafeCell,
    fmt::{self, Debug},
    mem::transmute,
    sync::Arc,
};

use async_trait::async_trait;
use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net,
    sync::mpsc,
    task,
};

use crate::{read_exact_timeout, write_all_timeout};

use super::{
    common::ServerTimeoutOptions,
    error::RpcError,
    message::{
        decode_file_block_request, FileBlockRequest, FileBlockResponse, ReqType, RespType,
        StatusCode,
    },
    packet::{Decode, Encode, ReqHeader, RespHeader, REQ_HEADER_SIZE},
    workerpool::{Job, WorkerPool},
};

use tracing::{debug, error, info};

/// The handler for the RPC file block request.
#[derive(Debug)]
pub struct FileBlockHandler {
    request: FileBlockRequest,
    done_tx: mpsc::Sender<Vec<u8>>,
}

impl FileBlockHandler {
    /// Create a new file block handler.
    pub fn new(request: FileBlockRequest, done_tx: mpsc::Sender<Vec<u8>>) -> Self {
        Self { request, done_tx }
    }
}

#[async_trait]
impl Job for FileBlockHandler {
    async fn run(&self) {
        debug!("RpcServerHandler::run");
        // Mock: serve block request and send response
        let size = self.request.block_size;

        // Prepare response body
        // Mock: response body is all zeros
        let file_block_resp = FileBlockResponse {
            seq: self.request.seq,
            file_id: self.request.file_id,
            block_id: self.request.block_id,
            block_size: size,
            status: StatusCode::Success,
            data: vec![0u8; size as usize],
        };
        let resp_body = file_block_resp.encode();
        // Prepare response header
        let resp_header = RespHeader {
            seq: self.request.seq,
            op: RespType::FileBlockResponse.to_u8(),
            len: resp_body.len() as u64,
        };
        let mut resp_buffer = resp_header.encode();
        // Combine response header and body
        resp_buffer.extend_from_slice(&resp_body);

        // Send response to the done channel
        self.done_tx.send(resp_buffer).await.unwrap();
    }
}

/// The handler for the RPC keep-alive request.
#[derive(Debug)]
pub struct KeepAliveHandler {}

impl KeepAliveHandler {
    /// Create a new keep-alive handler.
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl Job for KeepAliveHandler {
    /// Create a run function for the keep-alive handler.
    async fn run(&self) {
        // TODO: serve request and send response
        debug!("RpcServerHandler::run");
    }
}

/// Define trait for implementing the RPC server connection handler.
#[async_trait]
pub trait RpcServerConnectionHandler {
    /// Dispatch the handler for the connection.
    async fn dispatch(
        &self,
        req_header: ReqHeader,
        req_buffer: &[u8],
        done_tx: mpsc::Sender<Vec<u8>>,
    );
}

/// The file block handler for the RPC server.
#[derive(Clone, Debug)]
pub struct FileBlockRpcServerHandler {
    worker_pool: Arc<WorkerPool>,
}

impl FileBlockRpcServerHandler {
    /// Create a new file block RPC server handler.
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
            match req_type {
                ReqType::FileBlockRequest => {
                    // Try to read the request body
                    // Decode the request body
                    let req_body = decode_file_block_request(&req_buffer)
                        .expect("Failed to decode file block request");

                    // File block request
                    // Submit the handler to the worker pool
                    // When the handler is done, send the response to the done channel
                    // Response need to contain the response header and body
                    let handler = FileBlockHandler::new(req_body, done_tx.clone());
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
/// The connection for the RPC server.
#[derive(Clone, Debug)]
pub struct RpcServerConnection<T>
where
    T: RpcServerConnectionHandler + Send + Sync + 'static,
{
    /// The inner connection for the RPC server.
    inner: Arc<RpcServerConnectionInner<T>>,
}

///
#[derive(Debug)]
pub struct RpcServerConnectionInner<T>
where
    T: RpcServerConnectionHandler + Send + Sync + 'static,
{
    /// The TCP stream for the connection.
    stream: UnsafeCell<net::TcpStream>,
    /// The worker pool for the connection.
    /// TODO:
    #[allow(dead_code)]
    worker_pool: Arc<WorkerPool>,
    /// Options for the timeout of the connection
    timeout_options: ServerTimeoutOptions,
    /// The handler for the connection
    dispatch_handler: T,
    /// Response buffer, try to reuse same buffer to reduce memory allocation
    /// In case of the response is too large, we need to consider the buffer size
    /// Init size is 4MB
    /// Besides, we want to reduce memory copy, and read/write the response to the same data buffer
    req_buf: UnsafeCell<BytesMut>,
}

/// Current implementation is safe because the stream is only accessed by one thread
unsafe impl<T> Send for RpcServerConnectionInner<T> where
    T: RpcServerConnectionHandler + Send + Sync + 'static
{
}

/// Current implementation is safe because the stream is only accessed by one thread
unsafe impl<T> Sync for RpcServerConnectionInner<T> where
    T: RpcServerConnectionHandler + Send + Sync + 'static
{
}

impl<T> RpcServerConnectionInner<T>
where
    T: RpcServerConnectionHandler + Send + Sync + 'static,
{
    /// Create a new RPC server connection.
    pub fn new(
        stream: net::TcpStream,
        worker_pool: Arc<WorkerPool>,
        timeout_options: ServerTimeoutOptions,
        dispatch_handler: T,
    ) -> Self {
        Self {
            stream: UnsafeCell::new(stream),
            worker_pool,
            timeout_options,
            dispatch_handler,
            req_buf: UnsafeCell::new(BytesMut::with_capacity(8 * 1024 * 1024)),
        }
    }

    /// Recv request header from the stream
    pub async fn recv_header(&self) -> Result<ReqHeader, RpcError<String>> {
        // Try to read to buffer
        match self.recv_len(REQ_HEADER_SIZE).await {
            Ok(_) => {}
            Err(err) => {
                debug!("Failed to receive request header: {:?}", err);
                return Err(err);
            }
        }

        let buffer: &mut BytesMut = unsafe { transmute(self.req_buf.get()) };
        let req_header = ReqHeader::decode(&buffer)?;
        debug!("Received request header: {:?}", req_header);

        Ok(req_header)
    }

    /// Recv request body from the stream
    pub async fn recv_len(&self, len: u64) -> Result<(), RpcError<String>> {
        let mut req_buffer: &mut BytesMut = unsafe { transmute(self.req_buf.get()) };
        req_buffer.resize(len as usize, 0);
        let reader = self.get_stream_mut();
        match read_exact_timeout!(reader, &mut req_buffer, self.timeout_options.read_timeout).await
        {
            Ok(size) => {
                debug!("Received request body: {:?}", size);
                return Ok(());
            }
            Err(err) => {
                debug!("Failed to receive request: {:?}", err);
                return Err(RpcError::InternalError(err.to_string()));
            }
        }
    }

    /// Send response to the stream
    /// The response is a byte array, contains the response header and body.
    pub async fn send_response(&self, resp: &[u8]) -> Result<(), RpcError<String>> {
        let writer = self.get_stream_mut();
        match write_all_timeout!(writer, resp, self.timeout_options.write_timeout).await {
            Ok(_) => {
                debug!("Sent response: {:?}", resp.len());
                return Ok(());
            }
            Err(err) => {
                debug!("Failed to send response: {:?}", err);
                return Err(RpcError::InternalError(err.to_string()));
            }
        }
    }

    /// Get stream with mutable reference
    #[inline(always)]
    fn get_stream_mut(&self) -> &mut net::TcpStream {
        // Current implementation is safe because the stream is only accessed by one thread
        unsafe { transmute(self.stream.get()) }
    }
}

impl<T> RpcServerConnection<T>
where
    T: RpcServerConnectionHandler + Send + Sync + 'static,
{
    /// Create a new RPC server connection.
    pub fn new(
        stream: net::TcpStream,
        worker_pool: Arc<WorkerPool>,
        timeout_options: ServerTimeoutOptions,
        dispatch_handler: T,
    ) -> Self {
        let inner = Arc::new(RpcServerConnectionInner::new(
            stream,
            worker_pool,
            timeout_options,
            dispatch_handler,
        ));
        Self { inner }
    }

    /// Dispatch the handler for the connection.
    async fn dispatch(&self, req_header: ReqHeader, done_tx: mpsc::Sender<Vec<u8>>) {
        // Dispatch the handler for the connection
        let seq = req_header.seq;
        let body_len = req_header.len;
        if let Ok(req_type) = ReqType::from_u8(req_header.op) {
            debug!(
                "Dispatch request with header type: {:?}, seq: {:?}, body_len: {:?}",
                req_type, seq, body_len
            );
            match req_type {
                ReqType::KeepAliveRequest => {
                    // Keep-alive request
                    // Directly send keepalive response to client, do not need to submit to worker pool.
                    let _ = KeepAliveHandler::new();
                    // In current implementation, we just send keepalive header to the client stream
                    let resp_header = RespHeader {
                        seq,
                        op: RespType::KeepAliveResponse.to_u8(),
                        len: 0,
                    };
                    let resp_buffer = RespHeader::encode(&resp_header);
                    if let Ok(res) = self.inner.send_response(&resp_buffer).await {
                        debug!("Sent keepalive response: {:?}", res);
                    } else {
                        error!("Failed to send keepalive response");
                    }
                }
                _ => {
                    // Try to read the request body
                    match self.inner.recv_len(body_len).await {
                        Ok(_) => {}
                        Err(err) => {
                            error!("Failed to receive request body: {:?}", err);
                            return;
                        }
                    };
                    debug!(
                        "Dispatched handler for the connection, seq: {:?}",
                        req_header.seq
                    );
                    let req_buffer: &mut BytesMut = unsafe { transmute(self.inner.req_buf.get()) };
                    self.inner
                        .dispatch_handler
                        .dispatch(req_header, req_buffer, done_tx)
                        .await;
                }
            }
        } else {
            debug!("Inner request type is not matched: {:?}", req_header.op);
        }
    }

    /// Keep the connection and get the handler for the connection.
    pub async fn run(&self) {
        // Dispatch the handler for the connection
        debug!("RpcServerConnection::run");

        // TODO: copy done_tx to the worker pool
        let (done_tx, mut done_rx) = mpsc::channel::<Vec<u8>>(10000);

        // Send response to the stream from the worker pool
        // Worker pool will handle the response sending
        let inner_conn = self.inner.clone();
        tokio::spawn(async move {
            debug!("Start to send response to the stream from client");
            loop {
                match done_rx.recv().await {
                    Some(resp_buffer) => {
                        debug!(
                            "Recv buffer from done_tx channel, try to send response to the stream"
                        );
                        // Send response to the stream
                        if let Ok(res) = inner_conn.send_response(&resp_buffer).await {
                            debug!("Sent file block response: {:?}", res);
                        } else {
                            error!("Failed to send file block response");
                        }
                    }
                    None => {
                        info!("done_rx channel is closed, stop sending response to the stream");
                        break;
                    }
                }
            }
        });

        let start_time = std::time::Instant::now();
        loop {
            // Receive the request header
            let req_header = match self.inner.recv_header().await {
                Ok(header) => {
                    debug!("Received request header: {:?}", header);
                    header
                }
                Err(err) => {
                    debug!("Failed to receive request header: {:?}", err);
                    return;
                }
            };

            if req_header.seq == 1000 {
                let end_time = std::time::Instant::now();
                info!("Total time: {:?}", end_time - start_time);
            }
            // Dispatch the handler for the connection
            self.dispatch(req_header, done_tx.clone()).await;
        }
    }
}

/// The worker factory for the RPC connection.
#[derive(Clone, Debug)]
pub struct RpcConnWorkerFactory<T>
where
    T: RpcServerConnectionHandler + Send + Sync + 'static,
{
    /// Global worker pool for the RPC connection, shared by all connections.
    worker_pool: Arc<WorkerPool>,
    /// The handler for the connection
    dispatch_handler: T,
}

impl<T> RpcConnWorkerFactory<T>
where
    T: RpcServerConnectionHandler + Send + Sync + Clone + 'static,
{
    /// Create a new worker factory for the RPC connection.
    pub fn new(max_workers: usize, max_jobs: usize, dispatch_handler: T) -> Self {
        Self {
            worker_pool: Arc::new(WorkerPool::new(max_workers, max_jobs)),
            dispatch_handler,
        }
    }

    /// Serve the connection.
    pub fn serve(&self, conn: RpcServerConnection<T>) {
        // Run the connection
        tokio::spawn(async move {
            conn.run().await;
        });
    }
}

/// The RPC server definition.
pub struct RpcServer<T>
where
    T: RpcServerConnectionHandler + Send + Sync + Clone + 'static,
{
    /// Options for the timeout of the server connection
    timeout_options: ServerTimeoutOptions,
    /// Main worker for the server
    main_worker: Option<task::JoinHandle<()>>,
    /// The worker factory for the RPC connection
    rpc_conn_worker_factory: RpcConnWorkerFactory<T>,
}

impl Debug for RpcServer<FileBlockRpcServerHandler> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RpcServer")
            .field("timeout_options", &self.timeout_options)
            .field("main_worker", &self.main_worker)
            .finish()
    }
}

impl<T> RpcServer<T>
where
    T: RpcServerConnectionHandler + Send + Sync + Clone + 'static,
{
    /// Create a new RPC server.
    pub fn new(
        timeout_options: ServerTimeoutOptions,
        max_workers: usize,
        max_jobs: usize,
        dispatch_handler: T,
    ) -> Self {
        Self {
            timeout_options: timeout_options.clone(),
            main_worker: None,
            rpc_conn_worker_factory: RpcConnWorkerFactory::<T>::new(
                max_workers,
                max_jobs,
                dispatch_handler,
            ),
        }
    }

    /// Start the RPC server.
    pub async fn listen(&mut self, addr: &str) -> Result<(), RpcError<String>> {
        // Start the server
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|err| RpcError::InternalError(err.to_string()))?;
        info!("listening on {:?}", addr.to_string());

        // Accept incoming connections
        let timeout_options = self.timeout_options.clone();
        let factory = self.rpc_conn_worker_factory.clone();
        let handle = tokio::task::spawn(async move {
            loop {
                let conn_timeout_options = timeout_options.clone();
                match listener.accept().await {
                    Ok((stream, _)) => {
                        debug!("Accepted connection from {:?}", stream.peer_addr().unwrap());
                        factory.serve(RpcServerConnection::<T>::new(
                            stream,
                            factory.worker_pool.clone(),
                            conn_timeout_options,
                            factory.dispatch_handler.clone(),
                        ));
                    }
                    Err(err) => {
                        debug!("Failed to accept connection: {:?}", err);
                        continue;
                    }
                }
            }
        });

        self.main_worker = Some(handle);
        Ok(())
    }

    /// Stop the RPC server.
    pub async fn stop(&mut self) {
        // TODO: Gracefully stop the server?
        if let Some(handle) = self.main_worker.take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::time;

    use super::*;
    use std::net::TcpStream;
    use std::time::Duration;

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
    async fn test_rpc_server() {
        let addr = "127.0.0.1:2788";
        let pool = Arc::new(WorkerPool::new(4, 100));
        let handler = FileBlockRpcServerHandler::new(pool.clone());
        let mut server = RpcServer::new(ServerTimeoutOptions::default(), 4, 100, handler);
        server.listen(addr).await.unwrap();
        time::sleep(Duration::from_secs(1)).await;
        assert!(is_port_in_use(addr));
        server.stop().await;
        time::sleep(Duration::from_secs(1)).await;
        assert!(!is_port_in_use(addr));
    }

    // #[tokio::test]
    // async fn test_recv_header_success() {
    //     // Setup the mock TcpStream using Tokio's utilities or external mocking libraries
    //     let mock_stream = mock_tcp_stream_with_header_data().await;  // Assuming this function is implemented to mock header data

    //     let worker_pool = Arc::new(WorkerPool::new(1, 1));
    //     let handler = FileBlockRpcServerHandler::new(worker_pool.clone());
    //     let connection_inner = RpcServerConnectionInner::new(mock_stream, worker_pool, ServerTimeoutOptions::default(), handler);

    //     let result = connection_inner.recv_header().await;
    //     assert!(result.is_ok());
    //     let header = result.unwrap();
    //     assert_eq!(header.seq, expected_seq);
    //     assert_eq!(header.op, expected_op);
    // }

    // #[tokio::test]
    // async fn test_recv_len_success() {
    //     let mock_stream = mock_tcp_stream_with_specific_length_data(1024).await; // Mock function setup

    //     let worker_pool = Arc::new(WorkerPool::new(1, 1));
    //     let handler = FileBlockRpcServerHandler::new(worker_pool.clone());
    //     let connection_inner = RpcServerConnectionInner::new(mock_stream, worker_pool, ServerTimeoutAllocations::default(), handler);

    //     let result = connection_inner.recv_len(1024).await;
    //     assert!(result.is_ok());
    // }

    // #[tokio::test]
    // async fn test_dispatch_file_block_request() {
    //     let (done_tx, _) = mpsc::channel::<Vec<u8>>(1);
    //     let worker_pool = Arc::new(WorkerPool::new(1, 1));
    //     let handler = FileBlockRpcServerHandler::new(worker_pool.clone());
    //     let stream = tokio::net::TcpStream::connect("localhost:12345").await.unwrap(); // This should be a mock
    //     let connection = RpcServerConnection::new(stream, worker + pool, ServerTimeoutOptions::default(), handler);

    //     let req_header = ReqHeader {
    //         seq: 1,
    //         op: ReqType::FileBlockRequest.to_u8(),
    //         len: 1024,
    //     };

    //     // Setup mocking for expected behavior, perhaps using a mock library or fixtures
    //     connection.dispatch(req_with_header, done_tx).await;

    //     // Assertions to validate correct handler was used
    // }

    // #[tokio::test]
    // async fn test_server_connection_run() {
    //     let worker_pool = Arc::new(WorkerPool::new(1, 1));
    //     let handler = FileBlockRpcServerHandler::new(worker_pool.clone());
    //     let stream = mock_tcp_stream().await; // Needs proper setup for continuous requests

    //     let server_connection = RpcServerConnection::new(stream, worker_pool, ServerTimeoutOptions::default(), handler);
    //     server_connection.run().await;

    //     // Assertions to check the flow of handling and mock verifications
    // }

    // #[tokio::test]
    // async fn test_rpc_server_listen() {
    //     let mut server = setup_rpc_server().await;  // Assumes a setup function
    //     let result = server.listen("127.0.0.1:8080").await;
    //     assert!(result.is_ok());

    //     // Additional assertions to check that the server is listening and can accept connections
    // }

    // #[tokio::test]
    // async fn test_rpc_server_stop() {
    //     let mut server = setup_rpc_server().await; // Setup server with necessary handlers and worker pools
    //     server.listen("127.0.0.1:8080").await.unwrap();
    //     server.stop().await;

    //     // Verify that the server has stopped accepting new connections and all handlers are shutdown
    // }
}
