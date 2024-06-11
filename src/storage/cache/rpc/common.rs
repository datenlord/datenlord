use std::time::Duration;

/// Options for the timeout of the connection
#[derive(Debug, Clone)]
pub struct ServerTimeoutOptions {
    /// The timeout for reading data from the connection
    pub read_timeout: Duration,
    /// The timeout for writing data to the connection
    pub write_timeout: Duration,
    /// The timeout for the idle connection
    pub idle_timeout: Duration,
}

impl Default for ServerTimeoutOptions {
    fn default() -> Self {
        Self {
            read_timeout: Duration::from_secs(20),
            write_timeout: Duration::from_secs(20),
            idle_timeout: Duration::from_secs(120),
        }
    }
}


/// Options for the timeout of the connection
#[derive(Debug, Clone)]
pub struct ClientTimeoutOptions {
    /// The timeout for reading data from the connection
    pub read_timeout: Duration,
    /// The timeout for writing data to the connection
    pub write_timeout: Duration,
    /// The timeout for the idle connection
    pub idle_timeout: Duration,
    /// The timeout for keep-alive connection
    pub keep_alive_timeout: Duration,
}

impl Default for ClientTimeoutOptions {
    fn default() -> Self {
        Self {
            read_timeout: Duration::from_secs(20),
            write_timeout: Duration::from_secs(20),
            idle_timeout: Duration::from_secs(120),
            keep_alive_timeout: Duration::from_secs(60),
        }
    }
}
