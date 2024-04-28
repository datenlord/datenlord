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
