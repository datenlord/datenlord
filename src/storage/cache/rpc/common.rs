use std::time::Duration;

/// Options for the timeout of the connection
#[derive(Debug, Clone)]
pub struct TimeoutOptions {
    /// The timeout for reading data from the connection
    pub read_timeout: Duration,
    /// The timeout for writing data to the connection
    pub write_timeout: Duration,
    /// The timeout for the idle connection
    pub idle_timeout: Duration,
}

impl Default for TimeoutOptions {
    fn default() -> Self {
        Self {
            read_timeout: Duration::from_secs(20),
            write_timeout: Duration::from_secs(20),
            idle_timeout: Duration::from_secs(120),
        }
    }
}
