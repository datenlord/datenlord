use std::{
    cell::UnsafeCell,
    fmt::Debug,
    sync::{atomic::AtomicU64, Arc},
    u8,
};

use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tracing::debug;

use crate::{read_exact_timeout, write_all_timeout};

use super::{
    common::ClientTimeoutOptions,
    error::RpcError,
    message::{ReqType, RespType},
    packet::{Decode, Encode, Packet, PacketsKeeper, ReqHeader, RespHeader, REQ_HEADER_SIZE},
};

/// TODO: combine `RpcClientConnectionInner` and `RpcClientConnection`
struct RpcClientConnectionInner<P>
where
    P: Packet + Clone + Send + Sync + 'static,
{
    /// The TCP stream for the connection.
    stream: UnsafeCell<TcpStream>,
    /// Options for the timeout of the connection
    timeout_options: ClientTimeoutOptions,
    /// Stream auto increment sequence number, used to mark the request and response
    seq: AtomicU64,
    /// Received keep alive seq number, will mark the valid keep alive response.
    /// If the keep alive response is not received in right mestamp, the connection will be closed,
    /// If we receive other response, we will update the received_keepalive_timestamp too, just treat it as a keep alive response.
    received_keepalive_timestamp: AtomicU64,
    /// Send packet task
    packets_keeper: UnsafeCell<PacketsKeeper<P>>,
    /// Response buffer, try to reuse same buffer to reduce memory allocation
    /// In case of the response is too large, we need to consider the buffer size
    /// Init size is 4MB
    /// Besides, we want to reduce memory copy, and read/write the response to the same data buffer
    resp_buf: UnsafeCell<BytesMut>,
    /// The Client ID for the connection
    client_id: u64,
}

// TODO: Add markable id for this client
impl<P> Debug for RpcClientConnectionInner<P>
where
    P: Packet + Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcClientConnectionInner")
            .field("client id", &self.client_id)
            .finish()
    }
}

impl<P> RpcClientConnectionInner<P>
where
    P: Packet + Clone + Send + Sync + 'static,
{
    pub fn new(stream: TcpStream, timeout_options: ClientTimeoutOptions, client_id: u64) -> Self {
        Self {
            stream: UnsafeCell::new(stream),
            timeout_options: timeout_options.clone(),
            seq: AtomicU64::new(0),
            received_keepalive_timestamp: AtomicU64::new(0),
            packets_keeper: UnsafeCell::new(PacketsKeeper::new(
                timeout_options.clone().idle_timeout.as_secs(),
            )),
            resp_buf: UnsafeCell::new(BytesMut::with_capacity(8 * 1024 * 1024)),
            client_id,
        }
    }

    /// Recv request header from the stream
    pub async fn recv_header(&self) -> Result<RespHeader, RpcError<String>> {
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
    pub async fn recv_len(&self, len: u64) -> Result<(), RpcError<String>> {
        let mut req_buffer: &mut BytesMut = unsafe { &mut *self.resp_buf.get() };
        req_buffer.resize(len as usize, 0);
        let reader = self.get_stream_mut();
        match read_exact_timeout!(reader, &mut req_buffer, self.timeout_options.read_timeout).await
        {
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
    pub async fn send_data(&self, data: &[u8]) -> Result<(), RpcError<String>> {
        let writer = self.get_stream_mut();
        match write_all_timeout!(writer, data, self.timeout_options.write_timeout).await {
            Ok(()) => {
                debug!("{:?} Sent data with length: {:?}", self, data.len());
                Ok(())
            }
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

    pub async fn ping(&self) -> Result<(), RpcError<String>> {
        // Send keep alive message
        let current_seq = self.next_seq();
        let current_timestamp = tokio::time::Instant::now().elapsed().as_secs();
        // Check keepalive is valid
        let received_keepalive_timestamp = self
            .received_keepalive_timestamp
            .load(std::sync::atomic::Ordering::Acquire);
        if current_timestamp - received_keepalive_timestamp
            > self.timeout_options.keep_alive_timeout.as_secs()
        {
            debug!("{:?} keep alive timeout, close the connection", self);
            return Err(RpcError::InternalError("Keep alive timeout".to_string()));
        }

        // Set to packet task
        let keep_alive_msg = ReqHeader {
            seq: current_seq,
            op: ReqType::KeepAliveRequest.to_u8(),
            len: 0,
        }
        .encode();

        if let Ok(()) = self.send_data(&keep_alive_msg).await {
            debug!("{:?} Success to sent keep alive message", self);
            Ok(())
        } else {
            debug!("{:?} Failed to send keep alive message", self);
            Err(RpcError::InternalError(
                "Failed to send keep alive message".to_string(),
            ))
        }
    }

    /// Send packet by the client.
    pub async fn send_packet(&self, req_packet: &mut P) -> Result<(), RpcError<String>> {
        let current_seq = self.next_seq();
        req_packet.set_seq(current_seq);
        debug!("{:?} Try to send request: {:?}", self, current_seq);

        // Send may be used in different threads, so we need to create local buffer to store the request data
        if let Ok(req_buffer) = req_packet.get_req_data() {
            let mut req_header = ReqHeader {
                seq: req_packet.seq(),
                op: req_packet.op(),
                len: req_buffer.len() as u64,
            }
            .encode();

            // concate req_header and req_buffer
            req_header.extend_from_slice(&req_buffer);

            if let Ok(()) = self.send_data(&req_header).await {
                debug!("{:?} Sent request success: {:?}", self, req_packet.seq());
                // We have set a copy to keeper and manage the status for the packets keeper
                // Set to packet task with clone
                self.get_packets_keeper_mut().add_task(req_packet.clone());

                Ok(())
            } else {
                debug!("{:?} Failed to send request: {:?}", self, req_packet.seq());
                Err(RpcError::InternalError(
                    "Failed to send request".to_string(),
                ))
            }
        } else {
            debug!(
                "{:?} Failed to serialize request: {:?}",
                self,
                req_packet.seq()
            );
            Err(RpcError::InternalError(
                "Failed to serialize request".to_string(),
            ))
        }
    }

    /// Receive packet by the client
    pub async fn recv_packet(&self, resp_packet: &mut P) -> Option<P> {
        // Try to receive the response from innner keeper
        let packets_keeper: &mut PacketsKeeper<P> = self.get_packets_keeper_mut();
        packets_keeper.consume_task(resp_packet.seq())
    }

    /// Receive loop for the client.
    pub async fn recv_loop(&self) {
        // Check and clean keeper timeout unused task
        // The timeout data will be cleaned by the get function
        // We don't need to clean the timeout data radically
        let mut tickers = tokio::time::interval(self.timeout_options.idle_timeout * 100);
        loop {
            // Clean timeout tasks, will block here
            tokio::select! {
                _ = tickers.tick() => {
                    let packets_keeper: &mut PacketsKeeper<P> = self.get_packets_keeper_mut();
                    packets_keeper.clean_timeout_tasks();
                }
            }

            // Try to receive the response from the server
            debug!("{:?} Waiting for response...", self);
            let resp_header = self.recv_header().await;
            match resp_header {
                Ok(header) => {
                    let header_seq = header.seq;
                    debug!("{:?} Received keep alive response or other response.", self);
                    let current_timestamp = tokio::time::Instant::now().elapsed().as_secs();
                    self.received_keepalive_timestamp
                        .store(current_timestamp, std::sync::atomic::Ordering::Release);
                    // Update the received keep alive seq
                    if let Ok(resp_type) = RespType::from_u8(header.op) {
                        match resp_type {
                            RespType::KeepAliveResponse => {
                                continue;
                            }
                            _ => {
                                debug!("{:?} Received response header: {:?}", self, header);
                                // Try to read to buffer
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
                                let packets_keeper: &mut PacketsKeeper<P> =
                                    self.get_packets_keeper_mut();
                                if let Some(body) = packets_keeper.get_task_mut(header_seq) {
                                    // Try to fill the packet with the response
                                    debug!(
                                        "{:?} Find response body in current client: {:?}",
                                        self,
                                        body.seq()
                                    );
                                    let resp_buffer: &mut BytesMut =
                                        unsafe { &mut *self.resp_buf.get() };

                                    // Update status data
                                    // Try to set result code in `set_resp_data`
                                    body.set_resp_data(resp_buffer).unwrap();
                                } else {
                                    debug!("{:?} Failed to get packet task", self);
                                    break;
                                }
                            }
                        }
                    } else {
                        debug!("{:?} Invalid response type: {:?}", self, header.op);
                        break;
                    }
                }
                Err(err) => {
                    debug!("{:?} Failed to receive request header: {:?}", self, err);
                    break;
                }
            }
        }
    }

    /// Get stream with mutable reference
    #[inline(always)]
    fn get_stream_mut(&self) -> &mut TcpStream {
        // Current implementation is safe because the stream is only accessed by one thread
        unsafe { &mut *self.stream.get() }
    }

    /// Get packet task with mutable reference
    #[inline(always)]
    fn get_packets_keeper_mut(&self) -> &mut PacketsKeeper<P> {
        // Current implementation is safe because the packet task is only accessed by one thread
        unsafe { &mut *self.packets_keeper.get() }
    }
}

unsafe impl<P> Send for RpcClientConnectionInner<P> where P: Packet + Clone + Send + Sync + 'static {}

unsafe impl<P> Sync for RpcClientConnectionInner<P> where P: Packet + Clone + Send + Sync + 'static {}

/// The RPC client definition.
#[derive(Debug)]
pub struct RpcClient<P>
where
    P: Packet + Clone + Send + Sync + 'static,
{
    /// The timeout options for the client.
    /// TODO:
    #[allow(dead_code)]
    timeout_options: ClientTimeoutOptions,
    /// Inner connection for the client.
    inner_connection: Arc<RpcClientConnectionInner<P>>,
    /// Client ID for the connection
    /// TODO:
    #[allow(dead_code)]
    client_id: AtomicU64,
}

impl<P> RpcClient<P>
where
    P: Packet + Clone + Send + Sync + 'static,
{
    /// Create a new RPC client.
    ///
    /// We don't manage the stream is dead or clean
    /// The client will be closed if the keep alive message is not received in 100 times
    /// When the stream is broken, the client will be closed, and we need to recreate a new client
    pub async fn new(stream: TcpStream, timeout_options: ClientTimeoutOptions) -> Self {
        let client_id = AtomicU64::new(0);
        let inner_connection = RpcClientConnectionInner::new(
            stream,
            timeout_options.clone(),
            client_id.load(std::sync::atomic::Ordering::Acquire),
        );

        Self {
            timeout_options,
            inner_connection: Arc::new(inner_connection),
            client_id,
        }
    }

    /// Start client receiver
    pub async fn start_recv(&self) {
        // Create a receiver loop
        let inner_connection_clone = self.inner_connection.clone();
        tokio::spawn(async move {
            inner_connection_clone.recv_loop().await;
        });
    }

    /// Send a request to the server.
    /// Try to send data to channel, if the channel is full, return an error.
    /// Contains the request header and body.
    pub async fn send_request(&self, req: &mut P) -> Result<(), RpcError<String>> {
        self.inner_connection
            .send_packet(req)
            .await
            .map_err(|err| RpcError::InternalError(err.to_string()))
    }

    /// Get the response from the server.
    pub async fn recv_response(&self, resp: &mut P) -> Option<P> {
        // Try to receive the response from innner keeper
        self.inner_connection.recv_packet(resp).await
    }

    /// Manually send ping by the send request
    /// The inner can not start two loop, because the stream is unsafe
    /// we need to start the loop in the client or higher level manually
    pub async fn ping(&self) -> Result<(), RpcError<String>> {
        self.inner_connection.ping().await
    }
}

#[cfg(test)]
mod tests {
    use tokio::net::TcpListener;

    use crate::connect_timeout;
    use crate::storage::cache::rpc::packet::PacketStatus;

    use super::*;
    use std::time::Duration;

    #[derive(Debug, Clone)]
    pub struct TestPacket {
        pub seq: u64,
        pub op: u8,
        pub status: PacketStatus,
    }

    impl Packet for TestPacket {
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
            Ok(())
        }

        fn get_req_data(&self) -> Result<Vec<u8>, RpcError<String>> {
            Ok(Vec::new())
        }

        fn set_resp_data(&mut self, _data: &[u8]) -> Result<(), RpcError<String>> {
            Ok(())
        }

        fn get_resp_data(&self) -> Result<Vec<u8>, RpcError<String>> {
            Ok(Vec::new())
        }

        fn status(&self) -> PacketStatus {
            self.status
        }

        fn set_status(&mut self, status: PacketStatus) {
            self.status = status;
        }
    }

    #[tokio::test]
    async fn test_rpc_client() {
        // Set the tracing log level to debug
        tracing::subscriber::set_global_default(
            tracing_subscriber::FmtSubscriber::builder()
                .with_max_level(tracing::Level::DEBUG)
                .finish(),
        )
        .expect("Failed to set tracing subscriber");

        let addr = "127.0.0.1:2789";
        let timeout_options = ClientTimeoutOptions {
            read_timeout: Duration::from_secs(20),
            write_timeout: Duration::from_secs(20),
            idle_timeout: Duration::from_secs(20),
            keep_alive_timeout: Duration::from_secs(20),
        };

        // Create a fake server, will directly return the request
        tokio::spawn(async move {
            let listener = TcpListener::bind(addr).await.unwrap();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let (mut reader, mut writer) = stream.into_split();
                let mut buffer = vec![0_u8; 1024];
                let size = reader.read(&mut buffer).await.unwrap();
                debug!("Received request: {:?}", &buffer[..size]);
                // Create a response header
                let resp_header = RespHeader {
                    seq: 0,
                    op: RespType::KeepAliveResponse.to_u8(),
                    len: 0,
                };
                let resp_header = resp_header.encode();
                writer.write_all(&resp_header).await.unwrap();
                debug!("Sent response: {:?}", resp_header);
            }
        });

        // Wait for the server to start
        tokio::time::sleep(Duration::from_secs(2)).await;

        let connect_stream = connect_timeout!(addr, timeout_options.read_timeout)
            .await
            .unwrap();

        let rpc_client = RpcClient::<TestPacket>::new(connect_stream, timeout_options).await;

        // Wait for the server to start
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Current implementation will send keep alive message every 20/3 seconds
        let test_packet = &mut TestPacket {
            seq: 0,
            op: ReqType::KeepAliveRequest.to_u8(),
            status: PacketStatus::Pending,
        };
        let resp = rpc_client.recv_response(test_packet).await;
        assert!(resp.is_none());
    }

    // Helper function to setup a mock server
    async fn setup_mock_server(response: Vec<u8>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                let _ = socket.write_all(&response).await; // Send a predefined response
                let _ = socket.flush().await;
            }
        });
        addr
    }

    #[tokio::test]
    async fn test_new_connection() {
        let stream = TcpStream::connect("127.0.0.1:8080").await.unwrap();
        let timeout_options = ClientTimeoutOptions {
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(60),
            keep_alive_timeout: Duration::from_secs(30),
        };
        let client_id = 123;
        let connection =
            RpcClientConnectionInner::<TestPacket>::new(stream, timeout_options, client_id);
        assert_eq!(connection.client_id, client_id);
    }

    #[tokio::test]
    async fn test_recv_header() {
        let response = RespHeader {
            seq: 1,
            op: RespType::KeepAliveResponse.to_u8(),
            len: 0,
        }
        .encode();
        let addr = setup_mock_server(response).await;
        let stream = TcpStream::connect(addr).await.unwrap();
        let timeout_options = ClientTimeoutOptions {
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(60),
            keep_alive_timeout: Duration::from_secs(30),
        };
        let connection = RpcClientConnectionInner::<TestPacket>::new(stream, timeout_options, 123);
        let header = connection.recv_header().await.unwrap();
        assert_eq!(header.seq, 1);
        assert_eq!(header.op, RespType::KeepAliveResponse.to_u8());
    }

    #[tokio::test]
    async fn test_send_data() {
        let addr = setup_mock_server(vec![]).await;
        let stream = TcpStream::connect(addr).await.unwrap();
        let timeout_options = ClientTimeoutOptions {
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(60),
            keep_alive_timeout: Duration::from_secs(30),
        };
        let connection = RpcClientConnectionInner::<TestPacket>::new(stream, timeout_options, 123);
        let data = b"Hello, world!";
        connection.send_data(data).await.unwrap();
    }

    #[tokio::test]
    async fn test_next_seq() {
        let stream = TcpStream::connect("127.0.0.1:8080").await.unwrap();
        let timeout_options = ClientTimeoutOptions {
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(60),
            keep_alive_timeout: Duration::from_secs(30),
        };
        let client_id = 123;
        let connection =
            RpcClientConnectionInner::<TestPacket>::new(stream, timeout_options, client_id);
        let seq1 = connection.next_seq();
        let seq2 = connection.next_seq();
        assert_eq!(seq1 + 1, seq2);
    }

    #[tokio::test]
    async fn test_ping() {
        let response = RespHeader {
            seq: 1,
            op: RespType::KeepAliveResponse.to_u8(),
            len: 0,
        }
        .encode();
        let addr = setup_mock_server(response).await;
        let stream = TcpStream::connect(addr).await.unwrap();
        let timeout_options = ClientTimeoutOptions {
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(60),
            keep_alive_timeout: Duration::from_secs(30),
        };
        let connection = RpcClientConnectionInner::<TestPacket>::new(stream, timeout_options, 123);
        connection.ping().await.unwrap();
    }
}
