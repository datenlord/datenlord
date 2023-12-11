use clap::Parser;

#[derive(Debug, Parser)]
#[clap(author, version, about, long_about = None)]
/// A config
pub struct Config {
    #[clap(long = "role", value_name = "VALUE")]
    /// Role: Controller, Node, AsyncFuse, Scheduler
    pub role: String,
    #[clap(long = "node-name", value_name = "VALUE")]
    /// Node name
    pub node_name: String,
    #[clap(long = "node-ip", value_name = "VALUE")]
    /// Node ip
    pub node_ip: String,
    #[clap(long = "mount-path", value_name = "VALUE")]
    /// Set the mount point of FUSE
    pub mount_path: String,
    #[clap(long = "kv-addrs", value_name = "VALUE")]
    /// Set the kv server addresses
    pub kv_addr: String,
    #[clap(long = "server-port", value_name = "VALUE", default_value_t = 8800)]
    /// Set service port number
    pub server_port: u16,
    #[clap(flatten)]
    /// Storage related config
    pub storage: StorageConfig,
    #[clap(flatten)]
    /// CSI related config
    pub csi_config: CSIConfig,
}

#[derive(Debug, Parser)]
/// Storage config
pub struct StorageConfig {
    #[clap(long = "storage-type", value_name = "VALUE", default_value = "none")]
    /// Storage type: S3,None
    pub storage_type: String,
    #[clap(
        long = "storage-cache-capacity",
        value_name = "VALUE",
        default_value_t = 1073741824
    )]
    /// Set memory cache capacity, default is 1GB
    pub cache_capacity: usize,
    #[clap(flatten)]
    /// S3 storage config
    pub s3_storage_config: S3StorageConfig,
}

/// S3 storage config
#[derive(Debug, Parser)]
pub struct S3StorageConfig {
    #[clap(
        long = "storage-s3-endpoint-url",
        value_name = "VALUE",
        default_value_t
    )]
    /// The endpoint url of s3 storage
    pub endpoint_url: String,
    #[clap(
        long = "storage-s3-access-key-id",
        value_name = "VALUE",
        default_value_t
    )]
    /// The access key id of s3 storage
    pub access_key_id: String,
    #[clap(
        long = "storage-s3-secret-access-key",
        value_name = "VALUE",
        default_value_t
    )]
    /// The secret access key of s3 storage
    pub secret_access_key: String,
    #[clap(long = "storage-s3-bucket", value_name = "VALUE", default_value_t)]
    /// The bucket name of s3 storage
    pub bucket_name: String,
}

/// CSI related config
#[derive(Debug, Clone, Parser)]
pub struct CSIConfig {
    #[clap(long = "csi-endpoint", value_name = "VALUE", default_value_t)]
    /// Set the socket end point of CSI service
    pub endpoint: String,
    #[clap(long = "csi-driver-name", value_name = "VALUE", default_value_t)]
    /// The driver name of csi server
    pub driver_name: String,
    #[clap(long = "csi-node-id", value_name = "VALUE", default_value_t)]
    /// The node id of csi server
    pub node_id: String,
    #[clap(long = "csi-worker-port", value_name = "VALUE", default_value_t)]
    /// The worker port of csi server
    pub worker_port: u16,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::net::IpAddr;
    use std::str::FromStr;

    use super::*;
    use crate::config::inner::{InnerConfig, Role, StorageParams as InnerStorageParams};

    #[test]
    #[allow(clippy::indexing_slicing)]
    fn test_basic_config() {
        // Set the args
        let args = vec![
            "datenlord",
            "--role",
            "node",
            "--node-name",
            "node1",
            "--node-ip",
            "127.0.0.1",
            "--mount-path",
            "/tmp/datenlord_data_dir",
            "--storage-cache-capacity",
            "1024",
            "--kv-addrs",
            "127.0.0.1:7890,127.0.0.1:7891",
            "--csi-endpoint",
            "unix:///tmp/node.sock ",
            "--csi-driver-name",
            "io.datenlord.csi.plugin",
            "--csi-node-id",
            "node1",
            "--storage-s3-endpoint-url",
            "http://127.0.0.1:9000",
            "--storage-s3-access-key-id",
            "test_access_key",
            "--storage-s3-secret-access-key",
            "test_secret_key",
            "--storage-s3-bucket",
            "test_bucket",
            "--csi-worker-port",
            "9001",
            "--server-port",
            "8800",
        ];
        let config = Config::parse_from(args);
        assert_eq!(config.role, "node");
        assert_eq!(config.node_name, "node1");
        assert_eq!(config.node_ip.as_str(), "127.0.0.1");
        assert_eq!(config.mount_path.as_str(), "/tmp/datenlord_data_dir");
        assert_eq!(config.kv_addr.as_str(), "127.0.0.1:7890,127.0.0.1:7891");
        assert_eq!(config.server_port, 8800);

        // Following are the default values
        assert_eq!(config.storage.storage_type, "none");

        // Cache capacity
        assert_eq!(config.storage.cache_capacity, 1024);

        // Cast to InnerConfig
        let inner_config: InnerConfig = config.try_into().unwrap();
        assert_eq!(inner_config.role, Role::Node);
        assert_eq!(inner_config.node_name, "node1");
        assert_eq!(inner_config.node_ip, IpAddr::from_str("127.0.0.1").unwrap());
        assert_eq!(inner_config.mount_path.as_str(), "/tmp/datenlord_data_dir");
        assert_eq!(inner_config.server_port, 8800);

        let kv_addrs = inner_config.kv_addrs;
        assert_eq!(kv_addrs.len(), 2);
        assert_eq!(kv_addrs[0], "127.0.0.1:7890");
        assert_eq!(kv_addrs[1], "127.0.0.1:7891");

        let storage_config = inner_config.storage;
        assert_eq!(storage_config.cache_capacity, 1024);
        match storage_config.params {
            InnerStorageParams::None(_) => {}
            InnerStorageParams::S3(_) => panic!("storage params should be None"),
        }

        let csi_config = inner_config.csi_config;
        assert_eq!(csi_config.endpoint, "unix:///tmp/node.sock ");
        assert_eq!(csi_config.driver_name, "io.datenlord.csi.plugin");
        assert_eq!(csi_config.node_id, "node1");
        assert_eq!(csi_config.worker_port, 9001);
    }

    #[test]
    fn test_s3_config() {
        // Set the args
        let args = vec![
            "datenlord",
            "--role",
            "node",
            "--node-name",
            "node1",
            "--node-ip",
            "127.0.0.1",
            "--mount-path",
            "/tmp/datenlord_data_dir",
            "--server-port",
            "8800",
            "--csi-endpoint",
            "http://127.0.0.1:9000",
            "--storage-type",
            "S3",
            "--storage-cache-capacity",
            "1024",
            "--storage-s3-endpoint-url",
            "http://127.0.0.1:9000",
            "--storage-s3-access-key-id",
            "test_access_key",
            "--storage-s3-secret-access-key",
            "test_secret_key",
            "--storage-s3-bucket",
            "test_bucket",
            "--kv-addrs",
            "127.0.0.1:7890,127.0.0.1:7891",
        ];
        let config: InnerConfig = Config::parse_from(args).try_into().unwrap();
        let storage_config = config.storage;
        assert_eq!(storage_config.cache_capacity, 1024);
        match storage_config.params {
            InnerStorageParams::S3(s3_config) => {
                assert_eq!(s3_config.endpoint_url, "http://127.0.0.1:9000");
                assert_eq!(s3_config.access_key_id, "test_access_key");
                assert_eq!(s3_config.secret_access_key, "test_secret_key");
                assert_eq!(s3_config.bucket_name, "test_bucket");
            }
            InnerStorageParams::None(_) => panic!("storage params should be S3"),
        }
    }

    #[test]
    #[allow(clippy::indexing_slicing)]
    fn test_csi_controller_config() {
        // Set the args
        let args = vec![
            "datenlord",
            "--role",
            "controller",
            "--csi-endpoint",
            "unix:///tmp/controller.sock",
            "--csi-driver-name",
            "io.datenlord.csi.plugin",
            "--csi-worker-port",
            "9001",
            "--node-name",
            "localhost",
            "--node-ip",
            "127.0.0.1",
            "--mount-path",
            "/tmp/datenlord_data_dir",
            "--kv-addrs",
            "127.0.0.1:7890",
        ];
        let config: InnerConfig = Config::parse_from(args).try_into().unwrap();
        assert_eq!(config.role, Role::Controller);
        assert_eq!(config.node_name, "localhost");
        assert_eq!(config.node_ip, IpAddr::from_str("127.0.0.1").unwrap());
        assert_eq!(config.mount_path.as_str(), "/tmp/datenlord_data_dir");
        assert_eq!(config.server_port, 8800);

        let kv_addrs = config.kv_addrs;
        assert_eq!(kv_addrs.len(), 1);
        assert_eq!(kv_addrs[0], "127.0.0.1:7890");

        let csi_config = config.csi_config;
        assert_eq!(csi_config.endpoint, "unix:///tmp/controller.sock");
        assert_eq!(csi_config.driver_name, "io.datenlord.csi.plugin");
        assert_eq!(csi_config.worker_port, 9001);
    }
}
