/// Config for the distribute cache
#[derive(Debug, Clone)]
pub struct DistributeCacheConfig {
    /// RPC server ip
    /// For example, 127.0.0.1:50001
    pub rpc_server_ip: String,
    /// RPC server port
    pub rpc_server_port: u16,
    /// Capacity of the cache in in Byte
    pub capacity: u32,
}

impl DistributeCacheConfig {
    /// Create a new config
    #[must_use]
    pub fn new(rpc_server_ip: String, rpc_server_port: u16, capacity: u32) -> Self {
        Self {
            rpc_server_ip,
            rpc_server_port,
            capacity,
        }
    }
}
