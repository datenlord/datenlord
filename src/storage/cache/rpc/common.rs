use std::time::Duration;

/// Default tcp request memory buffer for the connection
pub const DEFAULT_TCP_REQUEST_BUFFER_SIZE: usize = 1024 * 1024 * 8;
/// Default tcp request memory buffer for the connection
pub const DEFAULT_TCP_RESPONSE_BUFFER_SIZE: usize = 1024 * 1024 * 8;

/// Options for the timeout of the connection
#[derive(Debug, Clone)]
pub struct ServerTimeoutOptions {
    /// The timeout for reading data from the connection
    pub read_timeout: Duration,
    /// The timeout for writing data to the connection
    pub write_timeout: Duration,
    /// The timeout for recving packet task
    pub task_timeout: Duration,
}

impl Default for ServerTimeoutOptions {
    fn default() -> Self {
        Self {
            read_timeout: Duration::from_secs(20),
            write_timeout: Duration::from_secs(20),
            task_timeout: Duration::from_secs(120),
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
    /// The timeout for recving packet task
    pub task_timeout: Duration,
    /// The timeout for keep-alive connection
    pub keep_alive_timeout: Duration,
}

impl Default for ClientTimeoutOptions {
    fn default() -> Self {
        Self {
            read_timeout: Duration::from_secs(20),
            write_timeout: Duration::from_secs(20),
            task_timeout: Duration::from_secs(120),
            keep_alive_timeout: Duration::from_secs(60),
        }
    }
}
