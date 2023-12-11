/// Configuration module. This module is used to parse configuration from command line arguments
mod config;
/// Inner configuration module. This module is used to store the parsed configuration
/// and will be used to initialize the server
mod inner;

pub use config::Config;
pub use inner::{InnerConfig, Role as NodeRole, StorageConfig, StorageParams, StorageS3Config};
