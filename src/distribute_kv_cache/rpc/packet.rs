use std::{collections::HashMap, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use tokio::{sync::Mutex, time::Instant};
use tracing::debug;

use super::{
    error::RpcError,
    utils::{get_u64_from_buf, get_u8_from_buf, u64_to_usize},
};

/// The size of the request header.
pub const REQ_HEADER_SIZE: u64 = 17;
/// The size of the response header.
pub const RESP_HEADER_SIZE: u64 = 17;

/// The Encode trait is used to encode a message structure into a byte buffer.
pub trait Encode: Sync {
    /// Encode the message into a byte buffer, this operation will append to buffer
    /// If you need to encode from start, you should clear the buffer first
    fn encode(&self, buf: &mut BytesMut);

    /// Get large data with Bytes, disable memory copy
    fn encode_large_data(&self, _buf: &mut BytesMut) -> (u64, Vec<bytes::Bytes>) {
        // Default is empty
        (0, vec![])
    }
}

/// The Decode trait is used to message a byte buffer into a data structure.
#[allow(clippy::unimplemented)]
pub trait Decode {
    /// Decode the byte buffer into a data structure
    fn decode(_buf: &mut BytesMut) -> Result<Self, RpcError>
    where
        Self: Sized,
    {
        unimplemented!("Not implemented for this type")
    }

    /// Decode the large data into a data structure
    fn decode_large_data(_buf: bytes::Bytes) -> Result<Self, RpcError>
    where
        Self: Sized,
    {
        unimplemented!("Not implemented for this type")
    }
}

/// The `ActualSize` trait is used to get the actual size of the data structure.
pub trait ActualSize {
    /// Get the actual size of the data structure
    fn actual_size(&self) -> u64;
}

/// The message module contains the data structures shared between the client and server.
/// Assume the cluster only contains one version server, so we won't consider the compatibility
#[derive(Debug)]
pub struct ReqHeader {
    /// The sequence number of the request.
    pub seq: u64,
    /// The operation type of the request.
    /// 0: keepalive
    /// other is defined by the user
    pub op: u8,
    /// The length of the request.
    pub len: u64,
}

impl Encode for ReqHeader {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.seq.to_le());
        buf.put_u8(self.op.to_le());
        buf.put_u64(self.len.to_le());
    }
}

impl Decode for ReqHeader {
    fn decode(buf: &mut BytesMut) -> Result<Self, RpcError> {
        if buf.len() < u64_to_usize(REQ_HEADER_SIZE) {
            return Err(RpcError::InvalidRequest(
                "Invalid request header".to_owned(),
            ));
        }

        let seq = get_u64_from_buf(buf, 0)?;
        let op = get_u8_from_buf(buf, 8)?;
        let len = get_u64_from_buf(buf, 9)?;

        Ok(ReqHeader { seq, op, len })
    }
}

/// The message module contains the data structures shared between the client and server.
/// Assume the cluster only contains one version server, so we won't consider the compatibility
#[derive(Debug, PartialEq, Clone, Copy)]
pub struct RespHeader {
    /// The sequence number of the response.
    pub seq: u64,
    /// The operation type of the response.
    /// 0: keepalive
    /// other is defined by the user
    pub op: u8,
    /// The length of the response.
    pub len: u64,
}

impl Encode for RespHeader {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.seq.to_le());
        buf.put_u8(self.op);
        buf.put_u64(self.len.to_le());
    }
}

impl Decode for RespHeader {
    fn decode(buf: &mut BytesMut) -> Result<Self, RpcError> {
        if buf.len() < u64_to_usize(RESP_HEADER_SIZE) {
            return Err(RpcError::InvalidResponse(
                "Invalid response header".to_owned(),
            ));
        }

        let seq = get_u64_from_buf(buf, 0)?;
        let op = get_u8_from_buf(buf, 8)?;
        let len = get_u64_from_buf(buf, 9)?;

        Ok(RespHeader { seq, op, len })
    }
}

/// Define the Packet trait, for once send or receive packets
///
/// Client will create a packet and serialize it to a byte array,
/// send it to the server. And create a temp response packet in client.
///
/// Server will receive the packet, deserialize it to a packet struct,
/// and process the request, then create a response packet and serialize.
///
/// Client will receive the response packet and deserialize it to a packet struct.
/// and check the status of the packet and the response.
#[async_trait]
pub trait Packet: Sync + Send + Clone + Debug + Encode {
    /// Get the packet seq number
    fn seq(&self) -> u64;
    /// Set the packet seq number
    fn set_seq(&mut self, seq: u64);

    /// Get packet type
    fn op(&self) -> u8;
    /// Set packet type
    fn set_op(&mut self, op: u8);

    /// Set timestamp
    fn set_timestamp(&mut self, timestamp: u64);
    /// Get timestamp
    fn get_timestamp(&self) -> u64;

    /// Get request buf length
    /// This function is used to get the length of the request body size.
    fn get_req_len(&self) -> u64;

    /// Serialize response data to bytes
    /// We will get the serialized response data by accessing the property of the Packet
    fn set_resp_data(&mut self, data: BytesMut) -> Result<(), RpcError>;

    /// Set the packet result, and we will send the response buffer to caller, caller and directly decode this buffer
    /// The buffer is different in req and resp, so we can not hold one buffer in packet
    async fn set_result(self, status: Result<(), RpcError>);
}
/// The `PacketsInner` struct is used to store the current tasks.
#[derive(Debug)]
struct PacketsInner<P>
where
    P: Packet + Send + Sync,
{
    /// current tasks, marked by the seq number
    packets: HashMap<u64, P>,
}

impl<P: Packet + Send + Sync> PacketsInner<P> {
    /// Create a new `PacketsInner`
    #[must_use]
    pub fn new() -> Self {
        PacketsInner {
            packets: HashMap::new(),
        }
    }

    /// Add a task to the packets
    pub fn add_task(&mut self, seq: u64, packet: P) {
        self.packets.insert(seq, packet);
    }

    /// Get a task from the packets and remove it
    pub fn remove_task(&mut self, seq: u64) -> Option<P> {
        // remove packet and return it
        self.packets.remove(&seq)
    }

    /// Clean pending tasks as timeout
    /// In first step, we will try to mark the task as timeout if it is pending and timeout
    /// In 10 * timeout time, we will force to remove the task if the task is not consumed
    pub async fn clean_timeout_tasks(&mut self, current_timestamp: u64, timeout: u64) {
        let mut last_timeout_packets = Vec::new();
        for (seq, packet) in &mut self.packets {
            // Check if the task is timeout
            let timestamp = packet.get_timestamp();
            if current_timestamp - timestamp > timeout {
                // Set the task as timeout
                last_timeout_packets.push(*seq);
            }
        }

        // clean the timeout packets
        for seq in last_timeout_packets {
            if let Some(packet) = self.packets.remove(&seq) {
                debug!("Task {} is timeout", seq);
                packet
                    .set_result(Err(RpcError::Timeout(format!("Task {seq} is timeout"))))
                    .await;
            } else {
                debug!("Task {} is timeout, but not found in the packets", seq);
                continue;
            }
        }
    }

    /// Purge the outdated tasks when connection is timeout
    pub async fn purge_outdated_tasks(&mut self) {
        // Take ownership and pass it to the caller
        for (seq, packet) in self.packets.drain() {
            packet
                .set_result(Err(RpcError::Timeout(format!("Task {seq} is timeout"))))
                .await;
        }

        self.packets.clear();
    }
}

/// The `PacketsKeeper` struct is used to store the current and previous tasks.
/// It will be modified by single task, so we don't need to share it.
#[derive(Debug)]
pub struct PacketsKeeper<P>
where
    P: Packet + Send + Sync,
{
    /// current tasks, marked by the seq number
    inner: Arc<Mutex<PacketsInner<P>>>,
    /// buffer sender
    buffer_packets_sender: flume::Sender<P>,
    /// buffer receiver
    buffer_packets_receiver: flume::Receiver<P>,
    /// The maximum number of tasks that can be stored in the previous_tasks
    /// We will mark the task as timeout if it is in the previous_tasks and the previous_tasks is full
    timeout: u64,
    /// current timestamp
    current_time: Instant,
}

impl<P: Packet + Send + Sync> PacketsKeeper<P> {
    /// Create a new `PacketsKeeper`
    #[must_use]
    pub fn new(timeout: u64) -> Self {
        let (buffer_packets_sender, buffer_packets_receiver) = flume::bounded::<P>(1000);
        let packets_inner = Arc::new(Mutex::new(PacketsInner::new()));
        let current_time = tokio::time::Instant::now();

        PacketsKeeper {
            inner: packets_inner,
            buffer_packets_sender,
            buffer_packets_receiver,
            timeout,
            current_time,
        }
    }

    /// Add a task to the packets
    pub fn add_task(&self, mut packet: P) -> Result<(), RpcError> {
        // TODO: use a global atomic ticker(updated by check_loop) or read current time?
        let timestamp = self.current_time.elapsed().as_secs();
        packet.set_timestamp(timestamp);
        self.buffer_packets_sender.send(packet).map_err(|e| {
            RpcError::InternalError(format!("Failed to send packet to buffer: {e:?}"))
        })?;

        Ok(())
    }

    /// Clean pending tasks as timeout
    pub async fn clean_timeout_tasks(&self) {
        {
            let mut packets_inner = self.inner.lock().await;
            while let Ok(packet) = self.buffer_packets_receiver.try_recv() {
                let seq = packet.seq();
                packets_inner.add_task(seq, packet);
            }

            let current_timestamp = self.current_time.elapsed().as_secs();
            packets_inner
                .clean_timeout_tasks(current_timestamp, self.timeout)
                .await;
        }
    }

    /// Purge the outdated tasks when connection is timeout
    pub async fn purge_outdated_tasks(&self) {
        let mut packets_inner = self.inner.lock().await;
        while let Ok(packet) = self.buffer_packets_receiver.try_recv() {
            let seq = packet.seq();
            packets_inner.add_task(seq, packet);
        }

        packets_inner.purge_outdated_tasks().await;
    }

    /// Get a task from the packets, and update data in the task
    pub async fn take_task(&self, seq: u64, resp_buffer: BytesMut) -> Result<(), RpcError> {
        // Try to sync from buffer
        {
            let mut packets_inner = self.inner.lock().await;
            while let Ok(packet) = self.buffer_packets_receiver.try_recv() {
                debug!(
                    "take_task self.buffer_packets_receiver.try_recv(): {:?}",
                    packet
                );
                let seq = packet.seq();
                packets_inner.add_task(seq, packet);
            }

            if let Some(mut packet) = packets_inner.remove_task(seq) {
                let seq = packet.seq();
                // Update status data
                // Try to set result code in `set_resp_data`

                // forget this buffer for raw decode!!! 20241210
                let set_result = packet.set_resp_data(resp_buffer);
                match set_result {
                    Ok(()) => {
                        debug!("{:?} Success to set response data, seq: {:?}", self, seq);
                        packet.set_result(Ok(())).await;
                    }
                    Err(err) => {
                        debug!(
                            "{:?} Failed to set response data: {:?} with error {:?}",
                            self, seq, err
                        );
                        packet
                            .set_result(Err(RpcError::InternalError(format!(
                                "Failed to set response data: {seq:?} with error {err:?}"
                            ))))
                            .await;
                    }
                }
                packets_inner.remove_task(seq);

                return Ok(());
            }
        }

        Err(RpcError::InvalidRequest(format!("can not find seq: {seq}")))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
#[allow(dead_code)]
mod tests {
    use std::{mem, thread::sleep, time};

    use crate::async_fuse::util::usize_to_u64;

    use super::*;

    #[derive(Debug, Clone)]
    struct TestRequest {
        pub mock: u64,
    }

    impl Encode for TestRequest {
        fn encode(&self, _buf: &mut BytesMut) {}
    }

    #[derive(Debug, Clone)]
    struct TestPacket {
        pub seq: u64,
        pub op: u8,
        pub timestamp: u64,
        pub request: TestRequest,
        pub buf: BytesMut,
        pub sender: flume::Sender<Result<(), RpcError>>,
    }

    impl Encode for TestPacket {
        fn encode(&self, buf: &mut BytesMut) {
            self.request.encode(buf);
        }
    }

    #[async_trait]
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

        fn set_timestamp(&mut self, timestamp: u64) {
            self.timestamp = timestamp;
        }

        fn get_timestamp(&self) -> u64 {
            self.timestamp
        }

        fn get_req_len(&self) -> u64 {
            usize_to_u64(mem::size_of_val(&self.request))
        }

        fn set_resp_data(&mut self, _data: BytesMut) -> Result<(), RpcError> {
            Ok(())
        }

        async fn set_result(self, status: Result<(), RpcError>) {
            self.sender.send(status).unwrap();
        }
    }

    #[test]
    fn test_req_header_encode_and_decode() {
        let header = ReqHeader {
            seq: 1,
            op: 2,
            len: 3,
        };
        let mut buf = BytesMut::new();
        header.encode(&mut buf);
        assert_eq!(buf.len(), u64_to_usize(REQ_HEADER_SIZE));

        let header_decoded = ReqHeader::decode(&mut buf).unwrap();
        assert_eq!(header_decoded.seq, 1);
        assert_eq!(header_decoded.op, 2);
        assert_eq!(header_decoded.len, 3);
    }

    #[test]
    fn test_resp_header_encode_and_decode() {
        let header = RespHeader {
            seq: 1,
            op: 2,
            len: 3,
        };
        let mut buf = BytesMut::new();
        header.encode(&mut buf);
        assert_eq!(buf.len(), u64_to_usize(RESP_HEADER_SIZE));

        let header_decoded = RespHeader::decode(&mut buf).unwrap();
        assert_eq!(header_decoded.seq, 1);
        assert_eq!(header_decoded.op, 2);
        assert_eq!(header_decoded.len, 3);
    }

    #[tokio::test]
    async fn test_packets_keeper() {
        let packets_keeper = PacketsKeeper::<TestPacket>::new(1);

        // You can control what is need to be send.
        let (tx, rx) = flume::unbounded::<Result<(), RpcError>>();

        // Success
        let packet = TestPacket {
            seq: 1,
            op: 2,
            timestamp: 0,
            buf: BytesMut::new(),
            sender: tx.clone(),
            request: TestRequest { mock: 0 },
        };
        packets_keeper.add_task(packet.clone()).unwrap();
        packets_keeper
            .take_task(packet.seq(), BytesMut::new())
            .await
            .unwrap();
        match rx.recv() {
            Ok(Ok(())) => {
                debug!("Success to get response");
            }
            _ => panic!("Failed to get response"),
        }

        // Mark timeout
        let packet_2 = TestPacket {
            seq: 2,
            op: 2,
            timestamp: 0,
            buf: BytesMut::new(),
            sender: tx.clone(),
            request: TestRequest { mock: 0 },
        };
        packets_keeper.add_task(packet_2.clone()).unwrap();
        sleep(time::Duration::from_secs(2));
        packets_keeper.clean_timeout_tasks().await;
        match rx.recv() {
            Ok(res) => {
                debug!("Task is timeout {:?}", res);
                assert!(res.is_err());
            }
            _ => panic!("Failed to get response"),
        }

        // Purge the timeout task
        let packet_3 = TestPacket {
            seq: 3,
            op: 2,
            timestamp: 0,
            buf: BytesMut::new(),
            sender: tx.clone(),
            request: TestRequest { mock: 0 },
        };
        packets_keeper.add_task(packet_3.clone()).unwrap();
        packets_keeper.purge_outdated_tasks().await;
        match rx.recv() {
            Ok(res) => {
                debug!("Task is timeout {:?}", res);
                assert!(res.is_err());
            }
            _ => panic!("Failed to get response"),
        }
    }
}
