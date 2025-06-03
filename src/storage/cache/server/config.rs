/// default block size is 4M
pub const DEFAULT_BLOCK_SIZE: u64 = 4 * 1024 * 1024;

/// Config for the distribute cache
#[derive(Debug, Clone)]
pub struct Config {
    /// Key-value store addresses
    pub kv_addrs: Vec<String>,
}

impl Config {
    /// Create a new config
    pub fn new() -> Self {
        Self {
            kv_addrs: Vec::new(),
        }
    }
}

/// Cluster config
pub struct ClusterConfig {
    /// Global cluster block size
    block_size: u64,
}

impl ClusterConfig {
    /// Create a new cluster config
    pub fn new(block_size: u64) -> Self {
        Self { block_size }
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            block_size: DEFAULT_BLOCK_SIZE,
        }
    }
}
