/// Config for the distribute cache
#[derive(Debug, Clone)]
pub struct Config {
    /// Key-value store addresses
    pub kv_addrs: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

impl Config {
    /// Create a new config
    #[must_use]
    pub fn new() -> Self {
        Self {
            kv_addrs: Vec::new(),
        }
    }
}
