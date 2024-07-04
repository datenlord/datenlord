use std::{
    cell::UnsafeCell,
    fmt::{self, Debug},
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
    message::{ReqType, RespType},
    packet::{Decode, Encode, ReqHeader, RespHeader, REQ_HEADER_SIZE},
    utils::u64_to_usize,
    workerpool::WorkerPool,
};

use tracing::{debug, error, info};

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
    pub async fn recv_header(&self) -> Result<ReqHeader, RpcError> {
        // Try to read to buffer
        match self.recv_len(REQ_HEADER_SIZE).await {
            Ok(()) => {}
            Err(err) => {
                debug!("Failed to receive request header: {:?}", err);
                return Err(err);
            }
        }

        let buffer: &mut BytesMut = unsafe { &mut *self.req_buf.get() };
        let req_header = ReqHeader::decode(buffer)?;
        debug!("Received request header: {:?}", req_header);

        Ok(req_header)
    }

    /// Recv request body from the stream
    pub async fn recv_len(&self, len: u64) -> Result<(), RpcError> {
        let mut req_buffer: &mut BytesMut = unsafe { &mut *self.req_buf.get() };
        req_buffer.resize(u64_to_usize(len), 0);
        let reader = self.get_stream_mut();
        match read_exact_timeout!(reader, &mut req_buffer, self.timeout_options.read_timeout).await
        {
            Ok(size) => {
                debug!("Received request body: {:?}", size);
                Ok(())
            }
            Err(err) => {
                debug!("Failed to receive request: {:?}", err);
                Err(RpcError::InternalError(err.to_string()))
            }
        }
    }

    /// Send response to the stream
    /// The response is a byte array, contains the response header and body.
    pub async fn send_response(&self, resp: &[u8]) -> Result<(), RpcError> {
        let writer = self.get_stream_mut();
        match write_all_timeout!(writer, resp, self.timeout_options.write_timeout).await {
            Ok(()) => {
                debug!("Sent response successfully: {:?}", resp.len());
                Ok(())
            }
            Err(err) => {
                debug!("Failed to send response: {:?}", err);
                Err(RpcError::InternalError(err.to_string()))
            }
        }
    }

    /// Get stream with mutable reference
    #[allow(clippy::mut_from_ref)]
    fn get_stream_mut(&self) -> &mut net::TcpStream {
        // Current implementation is safe because the stream is only accessed by one thread
        unsafe { &mut *self.stream.get() }
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
            if let ReqType::KeepAliveRequest = req_type {
                // Keep-alive request
                // Directly send keepalive response to client, do not need to submit to worker pool.
                // In current implementation, we just send keepalive header to the client stream
                let resp_header = RespHeader {
                    seq,
                    op: RespType::KeepAliveResponse.to_u8(),
                    len: 0,
                };
                // Only one process will access this buffer, so we can use this buffer directly.
                let resp_buffer: &mut BytesMut = unsafe { &mut *self.inner.req_buf.get() };
                RespHeader::encode(&resp_header, resp_buffer);
                if let Ok(res) = self.inner.send_response(resp_buffer).await {
                    debug!("Sent keepalive response: {:?}", res);
                } else {
                    error!("Failed to send keepalive response");
                }
            } else {
                // Try to read the request body
                match self.inner.recv_len(body_len).await {
                    Ok(()) => {}
                    Err(err) => {
                        error!("Failed to receive request body: {:?}", err);
                        return;
                    }
                };
                debug!(
                    "Dispatched handler for the connection, seq: {:?}",
                    req_header.seq
                );
                let req_buffer: &mut BytesMut = unsafe { &mut *self.inner.req_buf.get() };
                self.inner
                    .dispatch_handler
                    .dispatch(req_header, req_buffer, done_tx)
                    .await;
            }
        } else {
            debug!("Inner request type is not matched: {:?}", req_header.op);
        }
    }

    /// Keep the connection and get the handler for the connection.
    pub async fn run(&self) {
        // Dispatch the handler for the connection
        debug!("RpcServerConnection::Received a new TcpStream.");

        // TODO: copy done_tx to the worker pool
        let (done_tx, mut done_rx) = mpsc::channel::<Vec<u8>>(10000);

        // Send response to the stream from the worker pool
        // Worker pool will handle the response sending
        let inner_conn = Arc::clone(&self.inner);
        tokio::spawn(async move {
            debug!("Start to send response to the stream from client");
            loop {
                if let Some(resp_buffer) = done_rx.recv().await {
                    debug!("Recv buffer from done_tx channel, try to send response to the stream");
                    // Send response to the stream
                    if let Ok(res) = inner_conn.send_response(&resp_buffer).await {
                        debug!("Sent file block response successfully: {:?}", res);
                    } else {
                        error!("Failed to send file block response");
                    }
                } else {
                    info!("done_rx channel is closed, stop sending response to the stream");
                    break;
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
    T: RpcServerConnectionHandler + Send + Sync + Debug + Clone + 'static,
{
    /// Create a new worker factory for the RPC connection.
    pub fn new(max_workers: usize, max_jobs: usize, dispatch_handler: T) -> Self {
        Self {
            worker_pool: Arc::new(WorkerPool::new(max_workers, max_jobs)),
            dispatch_handler,
        }
    }

    /// Get dispatch handler for the connection.
    pub fn get_dispatch_handler(&self) -> T {
        self.dispatch_handler.clone()
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
    T: RpcServerConnectionHandler + Send + Sync + Debug + Clone + 'static,
{
    /// Options for the timeout of the server connection
    timeout_options: ServerTimeoutOptions,
    /// Main worker for the server
    main_worker: Option<task::JoinHandle<()>>,
    /// The worker factory for the RPC connection
    rpc_conn_worker_factory: RpcConnWorkerFactory<T>,
}

impl<T> Debug for RpcServer<T>
where
    T: RpcServerConnectionHandler + Send + Sync + Debug + Clone + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RpcServer")
            .field("timeout_options", &self.timeout_options)
            .field("main_worker", &self.main_worker)
            .finish_non_exhaustive()
    }
}

impl<T> RpcServer<T>
where
    T: RpcServerConnectionHandler + Send + Sync + Debug + Clone + 'static,
{
    /// Create a new RPC server.
    pub fn new(
        timeout_options: &ServerTimeoutOptions,
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
    pub async fn listen(&mut self, addr: &str) -> Result<(), RpcError> {
        // Start the server
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|err| RpcError::InternalError(err.to_string()))?;
        info!("listening on {:?}", addr.to_owned());

        // Accept incoming connections
        let timeout_options = self.timeout_options.clone();
        let factory = self.rpc_conn_worker_factory.clone();
        // TODO: move to task manager
        let handle = tokio::task::spawn(async move {
            loop {
                let conn_timeout_options = timeout_options.clone();
                match listener.accept().await {
                    Ok((stream, _)) => {
                        match stream.peer_addr() {
                            Ok(addr) => {
                                debug!("Accepted connection from {:?}", addr);
                            }
                            Err(err) => {
                                debug!("Failed to get peer address: {:?}", err);
                                break;
                            }
                        }
                        factory.serve(RpcServerConnection::<T>::new(
                            stream,
                            Arc::clone(&factory.worker_pool),
                            conn_timeout_options,
                            factory.get_dispatch_handler(),
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
    pub fn stop(&mut self) {
        // TODO: Gracefully stop the server?
        if let Some(handle) = self.main_worker.take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use tokio::time;

    use super::*;
    use std::time::Duration;

    /// Check if the port is in use
    async fn is_port_in_use(addr: &str) -> bool {
        if let Ok(stream) = tokio::net::TcpStream::connect(addr).await {
            // Port is in use
            drop(stream);
            true
        } else {
            // Port is not in use
            false
        }
    }

    /// Test handler for the RPC server connection.
    #[derive(Clone, Debug)]
    struct TestHandler;

    impl TestHandler {
        /// Create a new test handler.
        pub fn new() -> Self {
            Self {}
        }
    }

    #[async_trait]
    impl RpcServerConnectionHandler for TestHandler {
        async fn dispatch(
            &self,
            _req_header: ReqHeader,
            _req_buffer: &[u8],
            _done_tx: mpsc::Sender<Vec<u8>>,
        ) {
            // Dispatch the handler for the connection
            debug!("Dispatched handler for the connection");
        }
    }

    #[tokio::test]
    async fn test_rpc_server() {
        let addr = "127.0.0.1:2888";
        let handler = TestHandler::new();
        let mut server = RpcServer::new(&ServerTimeoutOptions::default(), 4, 100, handler);
        server.listen(addr).await.unwrap();
        time::sleep(Duration::from_secs(1)).await;
        assert!(is_port_in_use(addr).await);
        server.stop();
        time::sleep(Duration::from_secs(1)).await;
        assert!(!is_port_in_use(addr).await);
    }
}
