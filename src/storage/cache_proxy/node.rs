use std::sync::{Arc, Mutex};

/// Physical node struct
/// 
/// physical node is the node in the slot mapping
#[derive(Debug, Clone)]
pub struct Node {
    /// The id of the node
    id: u64,
    /// The ip of the node
    ip: String,
    /// The port of the node
    port: u16,
    /// The weight of the node
    weight: u32,
}

impl Node {
    /// Create a new node
    pub fn new(id: u64, ip: String, port: u16, weight: u32) -> Self {
        Self {
            id,
            ip,
            port,
            weight,
        }
    }

    /// Get the id of the node
    pub fn id(&self) -> u64 {
        self.id
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

    /// Remove a node from the list
    pub fn remove(&self, id: u64) {
        let mut list = self.inner.lock().unwrap();
        list.retain(|node| node.id() != id);
    }

    /// Get the node by id
    pub fn get(&self, id: u64) -> Option<Node> {
        let list = self.inner.lock().unwrap();
        list.iter().find(|node| node.id() == id).cloned()
    }
}

/// HashRing
///
/// This struct is used to manage the hashring.
pub struct HashRing {
    inner: Arc<Mutex<Ring<Node>>>,
}

impl Debug for HashRing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HashRing inner: {:?}", self.inner)
    }
}

impl HashRing {
    /// Create a new hashring
    pub fn new(slots: Vec<Node>) -> Self {
        // Init hashring with slots
        let mut ring = Ring::default();
        ring.batch_add(slots, false);

        Self {
            inner: Arc::new(Mutex::new(ring)),
        }
    }

    /// Get the version of the hashring
    pub fn version(&self) -> u64 {
        // Lock the hashring
        let inner = self.inner.lock().unwrap();

        // Get the version of the hashring
        inner.version()
    }

    /// Get the node by key
    pub fn get_node(&self, key: &str) -> Option<Node> {
        // Lock the hashring
        let inner = self.inner.lock().unwrap();

        // Get the slot by key
        inner.get_node(&key)
    }

    /// Get the nodes by key
    pub fn get_nodes(&self, key: &str, n: usize) -> Option<Vec<Node>> {
        // Lock the hashring
        let inner = self.inner.lock().unwrap();

        // Get the slots by key
        inner.get_nodes(&key, n)
    }

    /// Add a node to the hashring
    pub fn add_node(&self, node: Node) {
        // Lock the hashring
        let mut inner = self.inner.lock().unwrap();

        // Add the slot to the hashring
        inner.add(&node.clone(), false);
    }

    /// Remove a node from the hashring
    pub fn remove_node(&self, node: Node) {
        // Lock the hashring
        let mut inner = self.inner.lock().unwrap();

        // Remove the slot from the hashring
        inner.remove(&node.clone(), false);
    }

    /// Batch add nodes to the hashring
    pub fn batch_add_nodes(&self, nodes: Vec<Node>) {
        // Lock the hashring
        let mut inner = self.inner.lock().unwrap();

        // Batch add the slots to the hashring
        inner.batch_add(nodes, false);
    }

    /// Batch remove nodes from the hashring
    pub fn batch_remove_nodes(&self, nodes: Vec<Node>) {
        // Lock the hashring
        let mut inner = self.inner.lock().unwrap();

        // Batch remove the slots from the hashring
        inner.batch_remove(&nodes.clone(), false);
    }
}
