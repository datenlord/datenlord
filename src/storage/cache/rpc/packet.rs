use std::{collections::HashMap, fmt::Debug};

use tracing::debug;

use super::error::RpcError;

/// The size of the request header.
pub const REQ_HEADER_SIZE: u64 = 17;
/// The size of the response header.
pub const RESP_HEADER_SIZE: u64 = 17;

/// The Encode trait is used to encode a message structure into a byte buffer.
pub trait Encode {
    /// Encode the message into a byte buffer.
    fn encode(&self) -> Vec<u8>;
}

/// The Decode trait is used to message a byte buffer into a data structure.
pub trait Decode {
    /// Decode the byte buffer into a data structure.
    fn decode(buf: &[u8]) -> Result<Self, RpcError<String>>
    where
        Self: Sized;
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
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(REQ_HEADER_SIZE as usize);
        buf.extend_from_slice(&self.seq.to_be_bytes());
        buf.push(self.op);
        buf.extend_from_slice(&self.len.to_be_bytes());
        buf
    }
}

impl Decode for ReqHeader {
    fn decode(buf: &[u8]) -> Result<Self, RpcError<String>> {
        if buf.len() < REQ_HEADER_SIZE as usize {
            return Err(RpcError::InvalidRequest(
                "Invalid request header".to_string(),
            ));
        }

        let seq = u64::from_be_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]);
        let op = buf[8];
        let len = u64::from_be_bytes([
            buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15], buf[16],
        ]);

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
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(RESP_HEADER_SIZE as usize);
        buf.extend_from_slice(&self.seq.to_be_bytes());
        buf.push(self.op);
        buf.extend_from_slice(&self.len.to_be_bytes());
        buf
    }
}

impl Decode for RespHeader {
    fn decode(buf: &[u8]) -> Result<Self, RpcError<String>> {
        if buf.len() < RESP_HEADER_SIZE as usize {
            return Err(RpcError::InvalidResponse(
                "Invalid response header".to_string(),
            ));
        }

        let seq = u64::from_be_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]);
        let op = buf[8];
        let len = u64::from_be_bytes([
            buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15], buf[16],
        ]);

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
pub trait Packet: Sync + Send + Clone + Debug {
    /// Get the packet seq number
    fn seq(&self) -> u64;
    /// Set the packet seq number
    fn set_seq(&mut self, seq: u64);

    /// Get packet type
    fn op(&self) -> u8;
    /// Set packet type
    fn set_op(&mut self, op: u8);

    /// Serialize request data to bytes
    fn set_req_data(&mut self, data: &[u8]) -> Result<(), RpcError<String>>;

    /// Get the serialized request data
    fn get_req_data(&self) -> Result<Vec<u8>, RpcError<String>>;

    /// Serialize response data to bytes
    fn set_resp_data(&mut self, data: &[u8]) -> Result<(), RpcError<String>>;

    /// Deserialize response data from bytes
    fn get_resp_data(&self) -> Result<Vec<u8>, RpcError<String>>;

    /// Get the packet status
    fn status(&self) -> PacketStatus;

    /// Set the packet status
    fn set_status(&mut self, status: PacketStatus);
}

/// The PacketStatus enum is used to represent the status of a packet.
#[derive(Debug, Clone, Copy)]
pub enum PacketStatus {
    /// The packet is pending.
    Pending,
    /// The packet is successful.
    Success,
    /// The packet is failed.
    Failed,
    /// The packet is timeout.
    Timeout,
}

impl PacketStatus {
    /// Convert the PacketStatus to u8
    pub fn to_u8(&self) -> u8 {
        match self {
            PacketStatus::Pending => 0,
            PacketStatus::Success => 1,
            PacketStatus::Failed => 2,
            PacketStatus::Timeout => 3,
        }
    }

    /// Convert u8 to PacketStatus
    pub fn from_u8(status: u8) -> Self {
        match status {
            0 => PacketStatus::Pending,
            1 => PacketStatus::Success,
            2 => PacketStatus::Failed,
            3 => PacketStatus::Timeout,
            _ => PacketStatus::Failed,
        }
    }
}

/// The PacketsKeeper struct is used to store the current and previous tasks.
/// It will be modified by single task, so we don't need to share it.
#[derive(Debug)]
pub struct PacketsKeeper<P>
where
    P: Packet + Send + Sync,
{
    /// current tasks, marked by the seq number
    packets: HashMap<u64, P>,
    /// Delay to kepp the task from the previous_tasks
    /// Each vec item is in one second, if the task is timeout, we will remove it from the previous_tasks
    /// If the task is full, we will remove the task from the previous_tasks and set timeout
    /// Typically, the delay task id is growing, so we will use the VecDeque to store the tasks
    // delay_tasks: VecDeque<HashMap<u64, P>>,
    /// Search by the start seq number and reduce search time
    // delay_start: VecDeque<u64>,
    /// timestamp of seq number, marked by the seq number
    timestamp: HashMap<u64, u64>,
    /// The maximum number of tasks that can be stored in the previous_tasks
    /// We will mark the task as timeout if it is in the previous_tasks and the previous_tasks is full
    timeout: u64,
    // TODO: add init timeout here, to avoid the systemcall
}

impl<P: Packet + Send + Sync> PacketsKeeper<P> {
    /// Create a new PacketsKeeper
    pub fn new(timeout: u64) -> Self {
        let keeper = PacketsKeeper {
            packets: HashMap::new(),
            timestamp: HashMap::new(),
            timeout,
        };

        keeper
    }

    /// Add a task to the packets
    pub fn add_task(&mut self, packet: P) {
        let seq = packet.seq();
        self.packets.insert(seq, packet);
        // Get current timestamp
        // TODO: use a global atomic ticker(updated by check_loop) or read current time?
        let timestamp = tokio::time::Instant::now().elapsed().as_secs();
        self.timestamp.insert(seq, timestamp);
    }

    /// Clean pending tasks as timeout
    pub fn clean_timeout_tasks(&mut self) {
        let mut timeout_packets = Vec::new();
        let current_timestamp = tokio::time::Instant::now().elapsed().as_secs();
        for (seq, packet) in self.packets.iter_mut() {
            match packet.status() {
                PacketStatus::Pending => {
                    // Check if the task is timeout
                    if let Some(timestamp) = self.timestamp.get(seq) {
                        if current_timestamp - timestamp > self.timeout {
                            // Set the task as timeout
                            debug!("Task {} is timeout", seq);
                            packet.set_status(PacketStatus::Timeout);
                            timeout_packets.push(*seq);
                        }
                    }
                }
                _ => {}
            }
        }

        // Remove the timeout packets
        for seq in timeout_packets {
            self.packets.remove(&seq);
            self.timestamp.remove(&seq);
        }
    }

    /// Get a task from the packets and remove it
    pub fn consume_task(&mut self, seq: u64) -> Option<P> {
        if let Some(packet) = self.packets.remove(&seq) {
            self.timestamp.remove(&seq);
            return Some(packet);
        }

        None
    }

    /// Get a task from the packets
    pub fn get_task_mut(&mut self, seq: u64) -> Option<&mut P> {
        if let Some(packet) = self.packets.get_mut(&seq) {
            match packet.status() {
                // TODO: Only used for check status, we will not modify the status here
                PacketStatus::Success | PacketStatus::Failed | PacketStatus::Timeout => {
                    return Some(packet);
                }
                PacketStatus::Pending => {
                    // Check if the task is timeout
                    if let Some(timestamp) = self.timestamp.get(&seq) {
                        let current_timestamp = tokio::time::Instant::now().elapsed().as_secs();
                        if current_timestamp - timestamp > self.timeout {
                            // Set the task as timeout
                            debug!("Task {} is timeout", seq);
                            packet.set_status(PacketStatus::Timeout);
                            return None;
                        }

                        return Some(packet);
                    } else {
                        return None;
                    }
                }
            }
        }

        None
    }
}
