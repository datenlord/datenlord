use clap::Parser;

#[derive(Debug, Parser)]
#[clap(author, version, about, long_about = None)]
/// A config
pub struct Config {
    #[clap(long, value_name = "VALUE")]
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
    #[clap(long = "kv-server-list", value_name = "VALUE", value_delimiter = ',')]
    /// A list of kv servers, separated by commas
    pub kv_server_list: Vec<String>,
    #[clap(long = "server-port", value_name = "VALUE", default_value_t = 8800)]
    /// Set service port number
    pub server_port: u16,
    #[clap(
        long = "scheduler-extender-port",
        value_name = "VALUE",
        default_value_t = 12345
    )]
    /// Set the port of the scheduler extender
    pub scheduler_extender_port: u16,
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
    #[clap(long = "storage-type", value_name = "VALUE", default_value = "fs")]
    /// Storage type: s3, fs
    pub storage_type: String,
    /// The size of blocks, default is 512 KiB.
    #[clap(
        long = "storage-block-size",
        value_name = "VALUE",
        default_value_t = 0x8_0000
    )]
    pub block_size: usize,
    #[clap(flatten)]
    /// The memory cache config
    pub memory_cache_config: MemoryCacheConfig,
    #[clap(flatten)]
    /// S3 storage config
    pub s3_storage_config: S3StorageConfig,
    #[clap(
        long = "storage-fs-root",
        value_name = "VALUE",
        default_value = "/tmp/datenlord_backend"
    )]
    /// The root of FS backend
    pub fs_storage_root: String,
}

/// Memory cache config
#[derive(Debug, Parser)]
pub struct MemoryCacheConfig {
    /// The capacity of memory cache in bytes, default is 8 GiB.
    #[clap(
        long = "storage-mem-cache-capacity",
        value_name = "VALUE",
        default_value_t = 0x2_0000_0000
    )]
    pub capacity: usize,
    /// The limitation of the message queue of the write back task, default is
    /// 1000
    #[clap(
        long = "storage-mem-cache-command-limit",
        value_name = "VALUE",
        default_value_t = 1000
    )]
    pub command_queue_limit: usize,
    /// A flag whether the cache runs in write-back policy, default is false
    #[clap(long = "storage-mem-cache-write-back")]
    pub write_back: bool,
    /// A soft limit of evict policy for write back task.
    ///
    /// It's a fraction with the form of `a,b`, which means that
    /// the soft limit is `a/b` of the capacity.
    ///
    /// **Note**: `b` cannot be set to 0.
    #[clap(long = "storage-mem-cache-soft-limit", default_value = "3,5")]
    pub soft_limit: String,
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
    #[clap(long = "csi-worker-port", value_name = "VALUE", default_value_t)]
    /// The worker port of csi server
    pub worker_port: u16,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::net::IpAddr;
    use std::num::NonZeroUsize;
    use std::str::FromStr;

    use super::*;
    use crate::config::inner::{InnerConfig, Role, StorageParams as InnerStorageParams};
    use crate::config::SoftLimit;

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
            "--kv-server-list",
            "127.0.0.1:7890,127.0.0.1:7891",
            "--csi-endpoint",
            "unix:///tmp/node.sock ",
            "--csi-driver-name",
            "io.datenlord.csi.plugin",
            "--csi-worker-port",
            "9001",
            "--storage-fs-root",
            "/tmp/datenlord_backend",
        ];
        let config = Config::parse_from(args);
        assert_eq!(config.role, "node");
        assert_eq!(config.node_name, "node1");
        assert_eq!(config.node_ip.as_str(), "127.0.0.1");
        assert_eq!(config.mount_path.as_str(), "/tmp/datenlord_data_dir");

        let kv_addrs = &config.kv_server_list;
        assert_eq!(kv_addrs.len(), 2);
        assert_eq!(kv_addrs[0], "127.0.0.1:7890");
        assert_eq!(kv_addrs[1], "127.0.0.1:7891");
        assert_eq!(config.server_port, 8800);

        // Following are the default values
        assert_eq!(config.storage.storage_type, "fs");

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
        match storage_config.params {
            InnerStorageParams::Fs(root) => assert_eq!(root, "/tmp/datenlord_backend"),
            InnerStorageParams::S3(_) => panic!("storage params should be Fs"),
        }
        assert_eq!(storage_config.block_size, 0x8_0000);

        let memory_cache_config = storage_config.memory_cache_config;
        assert_eq!(memory_cache_config.capacity, 0x2_0000_0000);
        assert_eq!(memory_cache_config.command_queue_limit, 1000);
        assert!(!memory_cache_config.write_back);
        assert_eq!(
            memory_cache_config.soft_limit,
            SoftLimit(3, NonZeroUsize::new(5).unwrap())
        );

        let csi_config = inner_config.csi_config;
        assert_eq!(csi_config.endpoint, "unix:///tmp/node.sock ");
        assert_eq!(csi_config.driver_name, "io.datenlord.csi.plugin");
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
            "--storage-s3-endpoint-url",
            "http://127.0.0.1:9000",
            "--storage-s3-access-key-id",
            "test_access_key",
            "--storage-s3-secret-access-key",
            "test_secret_key",
            "--storage-s3-bucket",
            "test_bucket",
            "--kv-server-list",
            "127.0.0.1:7890,127.0.0.1:7891",
        ];
        let config: InnerConfig = Config::parse_from(args).try_into().unwrap();
        let storage_config = config.storage;
        match storage_config.params {
            InnerStorageParams::S3(s3_config) => {
                assert_eq!(s3_config.endpoint_url, "http://127.0.0.1:9000");
                assert_eq!(s3_config.access_key_id, "test_access_key");
                assert_eq!(s3_config.secret_access_key, "test_secret_key");
                assert_eq!(s3_config.bucket_name, "test_bucket");
            }
            InnerStorageParams::Fs(_) => panic!("storage params should be S3"),
        }
    }

    #[test]
    fn test_memory_cache_config() {
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
            "--kv-server-list",
            "127.0.0.1:7890,127.0.0.1:7891",
            "--csi-endpoint",
            "unix:///tmp/node.sock ",
            "--csi-driver-name",
            "io.datenlord.csi.plugin",
            "--csi-worker-port",
            "9001",
            "--storage-mem-cache-capacity",
            "10240",
            "--storage-mem-cache-command-limit",
            "2000",
            "--storage-mem-cache-write-back",
            "--storage-mem-cache-soft-limit",
            "1,2",
        ];

        let config = Config::parse_from(args);

        let memory_cache_config = &config.storage.memory_cache_config;

        assert_eq!(memory_cache_config.capacity, 10240);
        assert_eq!(memory_cache_config.command_queue_limit, 2000);
        assert!(memory_cache_config.write_back);
        assert_eq!(memory_cache_config.soft_limit, "1,2");

        let config: InnerConfig = config.try_into().unwrap();
        let memory_cache_config = &config.storage.memory_cache_config;

        let soft_limit = SoftLimit(1, NonZeroUsize::new(2).unwrap());
        assert_eq!(memory_cache_config.capacity, 10240);
        assert_eq!(memory_cache_config.command_queue_limit, 2000);
        assert!(memory_cache_config.write_back);
        assert_eq!(memory_cache_config.soft_limit, soft_limit);
    }

    #[test]
    #[allow(clippy::assertions_on_result_states)]
    fn test_invalid_soft_limit() {
        let build_args = |soft_limit: &'static str| {
            vec![
                "datenlord",
                "--role",
                "node",
                "--node-name",
                "node1",
                "--node-ip",
                "127.0.0.1",
                "--mount-path",
                "/tmp/datenlord_data_dir",
                "--kv-server-list",
                "127.0.0.1:7890,127.0.0.1:7891",
                "--csi-endpoint",
                "unix:///tmp/node.sock ",
                "--csi-driver-name",
                "io.datenlord.csi.plugin",
                "--storage-mem-cache-soft-limit",
                soft_limit,
                "--csi-worker-port",
                "9001",
            ]
        };

        let config: Result<InnerConfig, _> = Config::parse_from(build_args("a,1")).try_into();
        assert!(config.is_err());

        let config: Result<InnerConfig, _> = Config::parse_from(build_args("1,a")).try_into();
        assert!(config.is_err());

        let config: Result<InnerConfig, _> = Config::parse_from(build_args("1,0")).try_into();
        assert!(config.is_err());
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
            "--kv-server-list",
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

    #[test]
    #[allow(clippy::assertions_on_result_states)]
    fn test_config_parsing() {
        // Test wrong config key
        let wrong_args = vec!["datenlord", "--it-is-wrong", "value"];
        let config = Config::try_parse_from(wrong_args);
        assert!(config.is_err());

        // Test lack of config value
        let wrong_args = vec!["datenlord", "--role"];
        let config = Config::try_parse_from(wrong_args);
        assert!(config.is_err());
    }
}
