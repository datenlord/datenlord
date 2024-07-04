/// Config for the distribute cache
#[derive(Debug, Clone)]
pub struct DistributeCacheConfig {
    /// Key-value store addresses
    pub kv_addrs: Vec<String>,
    /// RPC server ip
    /// For example, 127.0.0.1:50001
    pub rpc_server_ip: String,
    /// RPC server port
    pub rpc_server_port: u16,
}

impl DistributeCacheConfig {
    /// Create a new config
    #[must_use]
    pub fn new(kv_addrs: Vec<String>, rpc_server_ip: String, rpc_server_port: u16) -> Self {
        Self {
            kv_addrs,
            rpc_server_ip,
            rpc_server_port,
        }
    }
}
