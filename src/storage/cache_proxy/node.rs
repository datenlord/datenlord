use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use super::ring::NodeType;

/// Physical node struct
///
/// physical node is the node in the slot mapping
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    /// We assume that ip is unique in our system
    /// TODO: Use hash set to judge the uniqueness?
    /// The ip of the node
    ip: String,
    /// The port of the node
    port: u16,
    /// The weight of the node
    weight: u32,
}

impl NodeType for Node {}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.ip == other.ip && self.port == other.port && self.weight == other.weight
    }
}

impl Eq for Node {}

impl std::hash::Hash for Node {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.ip.hash(state);
        self.port.hash(state);
        self.weight.hash(state);
    }
}

impl Node {
    /// Create a new node
    pub fn new(ip: String, port: u16, weight: u32) -> Self {
        Self { ip, port, weight }
    }

    /// Get the ip of the node
    pub fn ip(&self) -> &str {
        &self.ip
    }

    /// Get the port of the node
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Get the weight of the node
    pub fn weight(&self) -> u32 {
        self.weight
    }
}

/// Node list
///
/// Node list is used to manage the physical nodes
#[derive(Debug)]
pub struct NodeList {
    inner: Arc<Mutex<Vec<Node>>>,
}

impl NodeList {
    /// Create a new node list
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Add a node to the list
    pub fn add(&self, node: Node) {
        self.inner.lock().unwrap().push(node);
    }

    /// Get the node list
    pub fn list(&self) -> Vec<Node> {
        self.inner.lock().unwrap().clone()
    }

    /// Remove a node from the list by ip
    pub fn remove(&self, ip: &str) {
        let mut list = self.inner.lock().unwrap();
        list.retain(|node| node.ip() != ip);
    }

    /// Get the node by ip
    pub fn get(&self, ip: &str) -> Option<Node> {
        let list = match self.inner.lock() {
            Ok(lock) => lock,
            Err(_) => return None,
        };
        list.iter().find(|node| node.ip() == ip).cloned()
    }
}
