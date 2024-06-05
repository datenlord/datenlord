use std::sync::{Arc, Mutex};
use std::collections::VecDeque;
use tokio::sync::mpsc;
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

        // 模拟RPC调用
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

#[tokio::main]
async fn main() {
    let hashring = HashRing {
        nodes: vec![Node { id: 1, address: "127.0.0.1:8080".to_string() }],
    };
    let nodes = VecDeque::from(vec![Node { id: 1, address: "127.0.0.1:8080".to_string() }]);

    let client = CacheNodeClient::new(hashring, nodes);
    match client.read_file(FileInfo { file_id: 123 }).await {
        Ok(data) => println!("Read data: {:?}", data),
        Err(e) => println!("Error: {:?}", e),
    }
}
