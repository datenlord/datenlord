use std::sync::{Arc, Mutex};
use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc;
use tokio_util::bytes::BytesMut;
use std::error::Error;
use bytes::Bytes;

#[derive(Debug, Clone)]
struct Node {
    id: u64,
    address: String,
}

#[derive(Debug, Clone)]
struct HashRing {
    nodes: Vec<Node>,
}

impl HashRing {
    fn get_node(&self, key: &str) -> Option<&Node> {
        self.nodes.get(0)
    }
}

#[derive(Debug)]
struct FileInfo {
    file_id: u64,
}

#[derive(Debug)]
struct BlockInfo {
    block_id: u64,
    file_id: u64,
}

enum CacheNodeError {
    RpcError(String),
    NodeNotFound,
    IOError(std::io::Error),
}

struct CacheNodeClient {
    inner: Arc<Mutex<CacheNodeClientInner>>,
}

struct CacheNodeClientInner {
    hashring: HashRing,
    node_list: VecDeque<Node>,
}

impl CacheNodeClient {
    pub fn new(hashring: HashRing, nodes: VecDeque<Node>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(CacheNodeClientInner {
                hashring,
                node_list: nodes,
            }))
        }
    }

    pub async fn read_file(&self, file: FileInfo) -> Result<Vec<u8>, CacheNodeError> {
        let data = self.read_block(BlockInfo { block_id: 1, file_id: file.file_id }).await?;
        Ok(data)
    }

    pub async fn read_block(&self, block: BlockInfo) -> Result<Vec<u8>, CacheNodeError> {
        let inner = self.inner.lock().unwrap();
        let node_key = format!("{}-{}", block.file_id, block.block_id);
        let node = inner.hashring.get_node(&node_key).ok_or(CacheNodeError::NodeNotFound)?;

        let result = simulate_rpc_call(&node.address, &block).await;
        match result {
            Ok(data) => Ok(data),
            Err(e) => Err(CacheNodeError::RpcError(e)),
        }
    }
}

async fn simulate_rpc_call(address: &str, block: &BlockInfo) -> Result<Vec<u8>, String> {
    println!("Calling RPC on {} for block {:?}", address, block);
    Ok(vec![0; 1024])
}

/// TimeoutOptions
pub struct ClientConfig {
    /// client timeout options
    timeout_options: TimeoutOptions
}
/// DistributeCacheClient
///
/// This client is used to read/write the distribute cache.
#[derive(Debug)]
#[allow(dead_code)]
pub struct DistributeCacheClient {
    /// Local config
    config: Arc<ClientConfig>,
    /// RPC Client
    rpc_client: Arc<RPCClient>,
}

impl DistributeCacheClient {
    /// Create a new cache proxy client
    pub async fn new(config: ClientConfig, rpc_client: Arc<RPCServer>) -> Self {
        Self {
            config: Arc::new(config),
            rpc_client,
        }
    }

    pub async fn keep_alive(&self) -> Result<(), CacheNodeError> {
        // Tickers to keep alive the connection
        let mut tickers = tokio::time::interval(self.timeout_options.idle_timeout / 3);
        loop {
            tokio::select! {
                _ = tickers.tick() => {
                    // Send keep alive message
                    let keep_alive_msg = ReqHeader {
                        seq: self.keep_alive_seq.load(std::sync::atomic::Ordering::Relaxed) as u64,
                        req_type: ReqType::KeepAliveRequest,
                        len: 0,
                    }.encode();

                    if let Ok(_) =  self.send_data(&keep_alive_msg).await {
                        debug!("Sent keep alive message");
                    } else {
                        debug!("Failed to send keep alive message");
                        break;
                    }
                }
                req_result = request_channel_rx.recv() => {
                    match req_result {
                        Some(req) => {
                            if let Ok(_) = self.send_data(&req).await {
                                debug!("Sent request: {:?}", req);
                            } else {
                                debug!("Failed to send request: {:?}", req);
                                break;
                            }
                        }
                        None => {
                            // The request channel is closed and no remaining requests
                            debug!("Request channel closed.");
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Read file from the distribute cache
    pub async fn read_file(&self, file: FileInfo) -> Result<Bytes, CacheNodeError> {
        let file_range = HashMap<u64, BytesMut>();
    }
}