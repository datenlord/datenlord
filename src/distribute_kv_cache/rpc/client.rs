use std::{
    cell::UnsafeCell,
    fmt::Debug,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use bytes::BytesMut;
use futures::{stream::FuturesUnordered, Future, StreamExt};
use once_cell::sync::Lazy;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::Instant,
};
use tracing::debug;

use crate::{distribute_kv_cache::rpc::utils, read_exact_timeout, write_all_timeout};

use super::{
    common::ClientTimeoutOptions,
    error::RpcError,
    message::{ReqType, RespType},
    packet::{Decode, Encode, PacketsKeeper, ReqHeader, RequestTask, RespHeader, REQ_HEADER_SIZE},
    utils::u64_to_usize,
};

/// The huge body length for the response,
/// when the response is too large, we need to consider to take user buffer.
const HUGE_BODY_LEN: u64 = 1024 * 1024;

/// The client ID generator for the connection.
static CLIENT_ID_GEN: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(1));

/// TODO: combine `RpcClientConnectionInner` and `RpcClientConnection`
struct RpcClientConnectionInner<P>
where
    P: RequestTask + Clone + Send + Sync + 'static,
{
    /// The TCP stream for the connection.
    stream: UnsafeCell<TcpStream>,
    /// Options for the timeout of the connection
    timeout_options: ClientTimeoutOptions,
    /// Stream auto increment sequence number, used to mark the request and response
    seq: AtomicU64,
    /// The instant of the connection, used to calculate the keep alive timeout
    clock_instant: Instant,
    /// Received keep alive timestamp, will mark the valid keep alive response.
    /// If the keep alive response is not received in right mestamp, the connection will be closed,
    /// If we receive other response, we will update the received_keepalive_timestamp too, just treat it as a keep alive response.
    received_keepalive_timestamp: AtomicU64,
    /// Send packet task
    packets_keeper: PacketsKeeper<P>,
    /// Response buffer, try to reuse same buffer to reduce memory allocation
    /// In case of the response is too large, we need to consider the buffer size
    /// Init size is 4MB
    /// Besides, we want to reduce memory copy, and read/write the response to the same data buffer
    resp_buf: UnsafeCell<BytesMut>,
    /// Request encode buffer
    req_buf: UnsafeCell<BytesMut>,
    /// The Client ID for the connection
    client_id: u64,
}

// TODO: Add markable id for this client
impl<P> Debug for RpcClientConnectionInner<P>
where
    P: RequestTask + Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcClientConnectionInner")
            .field("client id", &self.client_id)
            .finish_non_exhaustive()
    }
}

impl<P> RpcClientConnectionInner<P>
where
    P: RequestTask + Clone + Send + Sync + 'static,
{
    /// Create a new connection.
    pub fn new(stream: TcpStream, timeout_options: &ClientTimeoutOptions, client_id: u64) -> Self {
        Self {
            stream: UnsafeCell::new(stream),
            timeout_options: timeout_options.clone(),
            seq: AtomicU64::new(0),
            clock_instant: Instant::now(),
            received_keepalive_timestamp: AtomicU64::new(0),
            packets_keeper: PacketsKeeper::new(timeout_options.clone().task_timeout.as_secs()),
            // Init buffer with 16MB
            resp_buf: UnsafeCell::new(BytesMut::with_capacity(16 * 1024 * 1024)),
            req_buf: UnsafeCell::new(BytesMut::with_capacity(16 * 1024 * 1024)),
            client_id,
        }
    }

    /// Recv request header from the stream
    pub async fn recv_header(&self) -> Result<RespHeader, RpcError> {
        // Try to read to buffer
        match self.recv_len(REQ_HEADER_SIZE).await {
            Ok(()) => {}
            Err(err) => {
                debug!("Failed to receive request header: {:?}", err);
                return Err(err);
            }
        }

        let buffer: &mut BytesMut = unsafe { &mut *self.resp_buf.get() };
        let req_header = RespHeader::decode(buffer)?;
        debug!("Received response header: {:?}", req_header);

        Ok(req_header)
    }

    /// Receive request body from the server.
    pub async fn recv_len(&self, len: u64) -> Result<(), RpcError> {
        let req_buffer: &mut BytesMut = unsafe { &mut *self.resp_buf.get() };

        self.recv_len_pass_in(len, req_buffer).await
    }

    /// Receive request body from the server, try to read huge len data with given buffer.
    pub async fn recv_len_pass_in(
        &self,
        len: u64,
        req_buffer: &mut BytesMut,
    ) -> Result<(), RpcError> {
        utils::ensure_buffer_len(req_buffer, u64_to_usize(len));

        let reader = self.get_stream_mut();
        match read_exact_timeout!(reader, req_buffer, self.timeout_options.read_timeout).await {
            Ok(size) => {
                debug!("{:?} Received response body len: {:?}", self, size);
                Ok(())
            }
            Err(err) => {
                debug!("{:?} Failed to receive response header: {:?}", self, err);
                Err(RpcError::InternalError(err.to_string()))
            }
        }
    }

    /// Send a request to the server.
    /// We need to make sure
    /// Current send packet need to be in one task, so if we need to send ping and packet
    /// we need to make sure only one operation is running, like select.
    pub async fn send_data(
        &self,
        req_header: &dyn Encode,
        req_body: Option<&dyn Encode>,
    ) -> Result<(), RpcError> {
        let buf = unsafe { &mut *self.req_buf.get() };
        buf.clear();

        // encode just need to append to buffer, do not clear buffer
        req_header.encode(buf);
        // Append the body to the buffer
        if let Some(body) = req_body {
            // encode just need to append to buffer, do not clear buffer
            body.encode(buf);
        }

        // FIXME: If data is very large, we need to send it in multiple times
        // and do not copy the data
        debug!("{:?} Sent data with length: {:?}", self, buf.len());
        let writer = self.get_stream_mut();
        // Copy from user space to tcp stream
        match write_all_timeout!(writer, buf, self.timeout_options.write_timeout).await {
            Ok(()) => Ok(()),
            Err(err) => {
                debug!("{:?} Failed to send data: {:?}", self, err);
                Err(RpcError::InternalError(err.to_string()))
            }
        }
    }

    /// Get the next sequence number.
    pub fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, std::sync::atomic::Ordering::AcqRel)
    }

    /// Send keep alive message to the server
    pub async fn ping(&self) -> Result<(), RpcError> {
        // Send keep alive message
        let current_seq = self.next_seq();
        let current_timestamp = self.clock_instant.elapsed().as_secs();
        // Check keepalive is valid
        let received_keepalive_timestamp = self
            .received_keepalive_timestamp
            .load(std::sync::atomic::Ordering::Acquire);
        if current_timestamp - received_keepalive_timestamp
            > self.timeout_options.keep_alive_timeout.as_secs()
        {
            debug!("{:?} keep alive timeout, close the connection", self);
            return Err(RpcError::InternalError("Keep alive timeout".to_owned()));
        }

        // Set to packet task
        let keep_alive_msg = ReqHeader {
            seq: current_seq,
            op: ReqType::KeepAliveRequest.into(),
            len: 0,
        };

        if let Ok(()) = self.send_data(&keep_alive_msg, None).await {
            debug!("{:?} Success to sent keep alive message", self);
            self.received_keepalive_timestamp
                .store(current_timestamp, std::sync::atomic::Ordering::Release);
            Ok(())
        } else {
            debug!("{:?} Failed to send keep alive message", self);
            Err(RpcError::InternalError(
                "Failed to send keep alive message".to_owned(),
            ))
        }
    }

    /// Send packet by the client.
    pub async fn send_packet(&self, mut req_packet: P) -> Result<(), RpcError> {
        let current_seq = self.next_seq();
        req_packet.set_seq(current_seq);
        debug!("{:?} Try to send request: {:?}", self, current_seq);

        // Send may be used in different threads, so we need to create local buffer to store the request data
        let req_len = req_packet.get_req_len(); // Try to get request buffer
        let req_header = ReqHeader {
            seq: req_packet.seq(),
            op: req_packet.op(),
            len: req_len,
        };

        debug!("{:?} Try to send request: {:?}", self, req_header);

        // concate req_header and req_buffer
        if let Ok(()) = self.send_data(&req_header, Some(&req_packet)).await {
            // We have set a copy to keeper and manage the status for the packets keeper
            // Set to packet task with clone
            self.packets_keeper.add_task(req_packet)?;
            debug!("{:?} Sent request success: {:?}", self, current_seq);

            Ok(())
        } else {
            debug!("{:?} Failed to send request: {:?}", self, current_seq);
            Err(RpcError::InternalError("Failed to send request".to_owned()))
        }
    }

    /// Receive loop for the client.
    #[allow(clippy::type_complexity)]
    pub async fn recv_loop(&self) {
        // Check and clean keeper timeout unused task
        // The timeout data will be cleaned by the get function
        // We don't need to clean the timeout data radically
        let mut pending: FuturesUnordered<
            Pin<Box<dyn Future<Output = Result<RespHeader, RpcError>> + Send>>,
        > = FuturesUnordered::new();
        loop {
            debug!("{:?} recv_loop Start to receive response", self);

            let tick_fut = async {
                let mut tickers = Box::pin(tokio::time::interval(
                    self.timeout_options.task_timeout.div_f32(2.0),
                ));
                tickers.tick().await;
                Err::<RespHeader, RpcError>(RpcError::Timeout(
                    "Current tasks is timeout".to_owned(),
                ))
            };
            pending.push(Box::pin(tick_fut));
            pending.push(Box::pin(self.recv_header()));

            // let selector = ReceiveHeaderFuture::new(self, &mut tickers, &mut recv_header_f);
            match pending.next().await {
                Some(Ok(header)) => {
                    // Try to receive the response from the server
                    let header_seq = header.seq;
                    debug!("{:?} Received keep alive response or other response.", self);
                    let current_timestamp = self.clock_instant.elapsed().as_secs();
                    self.received_keepalive_timestamp
                        .store(current_timestamp, std::sync::atomic::Ordering::Release);
                    // Update the received keep alive seq
                    if let Ok(resp_type) = RespType::try_from(header.op) {
                        if let RespType::KeepAliveResponse = resp_type {
                            // Keep alive response, receive_header will handle this op.
                            debug!("{:?} Received keep alive response: {:?}", self, header);
                        } else {
                            debug!("{:?} Received response header: {:?}", self, header);
                            // Try to read to buffer
                            if header.len <= HUGE_BODY_LEN {
                                debug!("{:?} Try to receive response body with size {:?} in client buffer", self, header.len);
                                match self.recv_len(header.len).await {
                                    Ok(()) => {}
                                    Err(err) => {
                                        debug!(
                                            "{:?} Failed to receive request header: {:?}",
                                            self, err
                                        );
                                        break;
                                    }
                                }

                                // Take the packet task and recv the response
                                let resp_buffer: &mut BytesMut =
                                    unsafe { &mut *self.resp_buf.get() };
                                // Fix: add a retry here in case of the task is not ready or failed
                                match self
                                    .packets_keeper
                                    .take_task(header_seq, resp_buffer.clone())
                                    .await
                                {
                                    Ok(()) => {
                                        debug!("{:?} Received response: {:?}", self, header_seq);
                                    }
                                    Err(err) => {
                                        debug!("{:?} Failed to update task: {:?}", self, err);
                                    }
                                }
                            } else {
                                debug!("{:?} Try to receive huge response body with size {:?} in user buffer", self, header.len);
                                let mut resp_buffer =
                                    BytesMut::with_capacity(u64_to_usize(header.len));
                                match self.recv_len_pass_in(header.len, &mut resp_buffer).await {
                                    Ok(()) => {}
                                    Err(err) => {
                                        debug!(
                                            "{:?} Failed to receive request header: {:?}",
                                            self, err
                                        );
                                        break;
                                    }
                                }

                                // Fix: add a retry here in case of the task is not ready or failed
                                // TODO: take a look about take_task is slow
                                match self.packets_keeper.take_task(header_seq, resp_buffer).await {
                                    Ok(()) => {
                                        debug!("{:?} Received response: {:?}", self, header_seq);
                                    }
                                    Err(err) => {
                                        debug!("{:?} Failed to update task: {:?}", self, err);
                                    }
                                }
                            }
                        }
                    } else {
                        debug!("{:?} Invalid response type: {:?}", self, header.op);
                        break;
                    }
                }
                Some(Err(err)) => {
                    debug!("{:?} recv_loop Failed to receive response: {:?}", self, err);
                }
                None => {
                    debug!("{:?} recv_loop No response received", self);
                }
            }
        }
    }

    /// Get stream with mutable reference
    #[allow(clippy::mut_from_ref)]
    fn get_stream_mut(&self) -> &mut TcpStream {
        // Current implementation is safe because the stream is only accessed by one thread
        unsafe { &mut *self.stream.get() }
    }
}

unsafe impl<P> Send for RpcClientConnectionInner<P> where
    P: RequestTask + Clone + Send + Sync + 'static
{
}

unsafe impl<P> Sync for RpcClientConnectionInner<P> where
    P: RequestTask + Clone + Send + Sync + 'static
{
}

/// The RPC client definition.
#[derive(Debug)]
pub struct RpcClient<P>
where
    P: RequestTask + Clone + Send + Sync + 'static,
{
    /// The timeout options for the client.
    #[allow(dead_code)]
    timeout_options: ClientTimeoutOptions,
    /// Inner connection for the client.
    inner_connection: Arc<RpcClientConnectionInner<P>>,
    /// Client ID for the connection
    #[allow(dead_code)]
    client_id: AtomicU64,
}

impl<P> RpcClient<P>
where
    P: RequestTask + Clone + Send + Sync + 'static,
{
    /// Create a new RPC client.
    ///
    /// We don't manage the stream is dead or clean
    /// The client will be closed if the keep alive message is not received in 100 times
    /// When the stream is broken, the client will be closed, and we need to recreate a new client
    pub fn new(stream: TcpStream, timeout_options: &ClientTimeoutOptions) -> Self {
        let client_id_val = CLIENT_ID_GEN.fetch_add(1, Ordering::Relaxed);
        let client_id = AtomicU64::new(client_id_val);
        let inner_connection = RpcClientConnectionInner::new(
            stream,
            timeout_options,
            client_id.load(std::sync::atomic::Ordering::Acquire),
        );

        Self {
            timeout_options: timeout_options.clone(),
            inner_connection: Arc::new(inner_connection),
            client_id,
        }
    }

    /// Start client receiver
    pub fn start_recv(&self) {
        // Create a receiver loop
        let inner_connection_clone = Arc::clone(&self.inner_connection);
        tokio::spawn(async move {
            inner_connection_clone.recv_loop().await;
        });
    }

    /// Send a request to the server.
    /// Try to send data to channel, if the channel is full, return an error.
    /// Contains the rezquest header and body.
    /// WARN: this function does not support concurrent call
    pub async fn send_request(&self, req: P) -> Result<(), RpcError> {
        self.inner_connection
            .send_packet(req)
            .await
            .map_err(|err| RpcError::InternalError(err.to_string()))
    }

    /// Manually send ping by the send request
    /// The inner can not start two loop, because the stream is unsafe
    /// we need to start the loop in the client or higher level manually
    ///
    /// # Safety
    /// The stream is unsafe, we need to make sure the stream is not used by other threads,
    /// you can only call this function inside the client loop.
    pub async unsafe fn ping(&self) -> Result<(), RpcError> {
        self.inner_connection.ping().await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
#[allow(clippy::indexing_slicing)]
mod tests {
    use async_trait::async_trait;
    use bytes::BufMut;
    use tokio::net::TcpListener;

    use crate::{
        async_fuse::util::usize_to_u64, distribute_kv_cache::rpc::utils::get_u64_from_buf,
    };

    use super::*;
    use std::{mem, time::Duration};

    /// Request struct for test
    #[derive(Debug, Default, Clone)]
    pub struct TestRequest {
        pub id: u64,
    }

    impl Encode for TestRequest {
        fn encode(&self, buf: &mut BytesMut) {
            buf.put_u64(self.id.to_le());
        }
    }

    impl Decode for TestRequest {
        fn decode(buf: &mut BytesMut) -> Result<Self, RpcError> {
            if buf.len() < 8 {
                return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
            }
            let id = get_u64_from_buf(buf, 0)?;
            Ok(TestRequest { id })
        }
    }

    #[derive(Debug, Clone)]
    pub struct TestPacket {
        pub seq: u64,
        pub op: u8,
        pub request: TestRequest,
        pub timestamp: u64,
    }

    impl Encode for TestPacket {
        fn encode(&self, buf: &mut BytesMut) {
            self.request.encode(buf);
        }
    }

    #[async_trait]
    impl RequestTask for TestPacket {
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

        fn set_timestamp(&mut self, timestamp: u64) {
            self.timestamp = timestamp;
        }

        fn get_timestamp(&self) -> u64 {
            self.timestamp
        }

        fn set_resp_data(&mut self, _data: BytesMut) -> Result<(), RpcError> {
            Ok(())
        }

        async fn set_result(self, _status: Result<(), RpcError>) {}

        fn get_req_len(&self) -> u64 {
            usize_to_u64(mem::size_of_val(&self.request))
        }
    }

    // Helper function to setup a mock server
    async fn setup_mock_server(addr: String, response: Vec<u8>) -> String {
        let listener = TcpListener::bind(addr).await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                socket.write_all(&response).await.unwrap(); // Send a predefined response
                socket.flush().await.unwrap();
            }
        });
        addr
    }

    #[tokio::test]
    async fn test_new_connection() {
        // setup();
        let local_addr = setup_mock_server("127.0.0.1:50050".to_owned(), vec![]).await;
        let stream = TcpStream::connect(local_addr).await.unwrap();
        let timeout_options = ClientTimeoutOptions {
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            task_timeout: Duration::from_secs(60),
            keep_alive_timeout: Duration::from_secs(30),
        };
        let client_id = 123;
        let connection =
            RpcClientConnectionInner::<TestPacket>::new(stream, &timeout_options, client_id);
        assert_eq!(connection.client_id, client_id);
    }

    #[tokio::test]
    async fn test_recv_header() {
        // setup();
        let mut buf = BytesMut::new();
        RespHeader {
            seq: 1,
            op: RespType::KeepAliveResponse.into(),
            len: 0,
        }
        .encode(&mut buf);
        let addr = setup_mock_server("127.0.0.1:50052".to_owned(), buf.to_vec()).await;
        let stream = TcpStream::connect(addr).await.unwrap();
        let timeout_options = ClientTimeoutOptions {
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            task_timeout: Duration::from_secs(60),
            keep_alive_timeout: Duration::from_secs(30),
        };
        let connection = RpcClientConnectionInner::<TestPacket>::new(stream, &timeout_options, 123);
        let header = connection.recv_header().await.unwrap();
        assert_eq!(header.seq, 1);
        assert_eq!(header.op, Into::<u8>::into(RespType::KeepAliveResponse));
    }

    #[tokio::test]
    async fn test_recv_len() {
        // setup();
        // Create a 10 len vector
        let response = vec![0; 10];
        let addr = setup_mock_server("127.0.0.1:50053".to_owned(), response).await;
        let stream = TcpStream::connect(addr).await.unwrap();
        let timeout_options = ClientTimeoutOptions {
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            task_timeout: Duration::from_secs(60),
            keep_alive_timeout: Duration::from_secs(30),
        };
        let connection = RpcClientConnectionInner::<TestPacket>::new(stream, &timeout_options, 123);
        connection.recv_len(10).await.unwrap();
    }

    struct TestData;
    impl Encode for TestData {
        fn encode(&self, buf: &mut BytesMut) {
            let data = b"Hello, world!";
            buf.extend_from_slice(data);
        }
    }

    #[tokio::test]
    async fn test_send_data() {
        // setup();
        let addr = setup_mock_server("127.0.0.1:50054".to_owned(), vec![]).await;
        let stream = TcpStream::connect(addr).await.unwrap();
        let timeout_options = ClientTimeoutOptions {
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            task_timeout: Duration::from_secs(60),
            keep_alive_timeout: Duration::from_secs(30),
        };
        let connection = RpcClientConnectionInner::<TestPacket>::new(stream, &timeout_options, 123);
        let req_header = ReqHeader {
            seq: 1,
            op: ReqType::KeepAliveRequest.into(),
            len: 0,
        };

        connection
            .send_data(&req_header, Some(&TestData {}))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_next_seq() {
        // setup();
        let addr = setup_mock_server("127.0.0.1:50055".to_owned(), vec![]).await;
        let stream = TcpStream::connect(addr).await.unwrap();
        let timeout_options = ClientTimeoutOptions {
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            task_timeout: Duration::from_secs(60),
            keep_alive_timeout: Duration::from_secs(30),
        };
        let client_id = 123;
        let connection =
            RpcClientConnectionInner::<TestPacket>::new(stream, &timeout_options, client_id);
        let seq1 = connection.next_seq();
        let seq2 = connection.next_seq();
        assert_eq!(seq1 + 1, seq2);
    }

    #[tokio::test]
    async fn test_ping() {
        // setup();
        let mut buf = BytesMut::new();
        RespHeader {
            seq: 1,
            op: RespType::KeepAliveResponse.into(),
            len: 0,
        }
        .encode(&mut buf);
        let addr = setup_mock_server("127.0.0.1:50056".to_owned(), buf.to_vec()).await;
        let stream = TcpStream::connect(addr).await.unwrap();
        let timeout_options = ClientTimeoutOptions {
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            task_timeout: Duration::from_secs(60),
            keep_alive_timeout: Duration::from_secs(30),
        };
        let connection = RpcClientConnectionInner::<TestPacket>::new(stream, &timeout_options, 123);
        connection.ping().await.unwrap();
    }
}
