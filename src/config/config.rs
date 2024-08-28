use clap::Parser;
use serde::{Deserialize, Serialize};
use serfig::collectors::{from_env, from_file, from_self};
use serfig::parsers::Toml;

use crate::common::error::DatenLordError;

#[derive(Debug, Clone, Serialize, Deserialize, Parser, Default)]
#[clap(author, version, about, long_about = None)]
#[serde(default)]
/// A config
pub struct Config {
    /// Config file path, now only support toml file
    #[clap(long = "config-file", value_name = "VALUE")]
    #[serde(skip)]
    pub config_file: Option<String>,
    #[clap(long, value_name = "VALUE")]
    /// Role: Controller, Node, AsyncFuse, Scheduler
    pub role: Option<String>,
    #[clap(long = "node-name", value_name = "VALUE")]
    /// Node name
    pub node_name: Option<String>,
    #[clap(long = "node-ip", value_name = "VALUE")]
    /// Node ip
    pub node_ip: Option<String>,
    #[clap(long = "log-level", value_name = "VALUE", default_value = "debug")]
    /// Log level
    pub log_level: String,
    #[clap(long = "mount-path", value_name = "VALUE")]
    /// Set the mount point of FUSE
    pub mount_path: Option<String>,
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

#[derive(Debug, Clone, Serialize, Deserialize, Parser, Default)]
#[serde(default)]
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
#[derive(Debug, Clone, Serialize, Deserialize, Parser, Default)]
#[serde(default)]
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
    /// The interval of a write back cycle (in ms).
    #[clap(long = "storage-mem-cache-write-back-interval", default_value_t = 200)]
    pub write_back_interval: u64,
    /// The max count of pending dirty blocks.
    #[clap(
        long = "storage-mem-cache-write-back-dirty-limit",
        default_value_t = 10
    )]
    pub write_back_dirty_limit: usize,
}

/// S3 storage config
#[derive(Debug, Clone, Serialize, Deserialize, Parser, Default)]
#[serde(default)]
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
    /// The region of s3 storage
    #[clap(long = "storage-s3-region", value_name = "VALUE")]
    pub region: Option<String>,
    /// S3 Max concurrent requests
    #[clap(long = "storage-s3-max-concurrent-requests", value_name = "VALUE")]
    pub max_concurrent_requests: Option<usize>,
}

/// CSI related config
#[derive(Debug, Clone, Serialize, Deserialize, Parser, Default)]
#[serde(default)]
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

impl Config {
    /// Load config from command line, config file and environment variables
    ///
    /// Priority: command line > config file > environment variables
    #[inline]
    pub fn load_from_args(arg_conf: Self) -> Result<Self, DatenLordError> {
        let mut builder: serfig::Builder<Self> = serfig::Builder::default();

        // Load from config file
        if let Some(config_file) = arg_conf
            .config_file
            .clone()
            .or_else(|| std::env::var("CONFIG_FILE").ok())
        {
            builder = builder.collect(from_file(Toml, &config_file));
        }

        // Load from environment
        builder = builder.collect(from_env());

        // Load from arguments
        builder = builder.collect(from_self(arg_conf));

        // Build the config with multi sources
        let conf = builder.build()?;

        // Check valid
        if conf.check_valid() {
            Ok(conf)
        } else {
            Err(DatenLordError::ArgumentInvalid {
                context: vec!["Current command line arguments are not valid, please check role/node_name/node_ip/mount_path is correct.".to_owned()],
            })
        }
    }

    /// Check the config values are valid
    #[inline]
    #[must_use]
    pub fn check_valid(&self) -> bool {
        // Check role/node_name/node_ip/mount_path
        let checked_values = [&self.role, &self.node_name, &self.node_ip, &self.mount_path];

        let valid = checked_values.iter().all(|x| x.is_some());

        valid
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::net::IpAddr;
    use std::num::NonZeroUsize;
    use std::str::FromStr;
    use std::time::Duration;

    use std::env;
    use std::io::Write;
    use tempfile::NamedTempFile;

    use tracing::level_filters::LevelFilter;

    use super::*;
    use crate::config::inner::{InnerConfig, Role, StorageParams as InnerStorageParams};
    use crate::config::SoftLimit;

    #[test]
    fn test_load_config_from_file() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let temp_file_path = temp_file.path().to_owned();
        writeln!(
            temp_file,
            r#"
            role = "node"
            node_name = "node1"
            node_ip = "127.0.0.1"
            server_port = 8800
            mount_path = "/tmp/datenlord_data_dir"
            kv_server_list = [
              "127.0.0.1:2379"
            ]
        "#
        )
        .unwrap();

        let args = vec![
            "datenlord",
            "--config-file",
            temp_file_path.to_str().unwrap(),
        ];
        let arg_conf = Config::parse_from(args);
        let conf = Config::load_from_args(arg_conf).unwrap();

        assert_eq!(conf.role, Some("node".to_owned()));
    }

    #[test]
    fn test_load_config_from_env() {
        let args = vec![
            ("CONFIG_FILE", "config.toml"),
            ("ROLE", "node"),
            ("NODE_NAME", "node222"),
            ("NODE_IP", "127.0.0.1"),
            ("SERVER_PORT", "8800"),
            ("MOUNT_PATH", "/tmp/datenlord_data_dir"),
            ("KV_SERVER_LIST", "127.0.0.1:7890,127.0.0.1:7891"),
        ];

        for &(k, v) in &args {
            env::set_var(k, v);
        }

        let args = vec!["datenlord"];
        let arg_conf = Config::parse_from(args);
        let conf = Config::load_from_args(arg_conf).unwrap();

        assert_eq!(conf.role, Some("node".to_owned()));
        assert_eq!(conf.node_ip, Some("127.0.0.1".to_owned()));
        assert_eq!(conf.mount_path, Some("/tmp/datenlord_data_dir".to_owned()));
        assert_eq!(conf.server_port, 8800);
        let kv_addrs = &conf.kv_server_list;
        assert_eq!(kv_addrs.len(), 2);
        assert_eq!(kv_addrs.get(0), Some(&"127.0.0.1:7890".to_owned()));
        assert_eq!(kv_addrs.get(1), Some(&"127.0.0.1:7891".to_owned()));
    }

    #[test]
    fn test_load_config_from_file_with_priority() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let temp_file_path = temp_file.path().to_owned();
        writeln!(
            temp_file,
            r#"
            role = "node"
            node_name = "node1"
            node_ip = "127.0.0.1"
            server_port = 8800
            mount_path = "/tmp/datenlord_data_dir"
            kv_server_list = [
              "127.0.0.1:2379"
            ]
        "#
        )
        .unwrap();

        let args = vec![
            ("CONFIG_FILE", "config.toml"),
            ("ROLE", "node"),
            ("NODE_NAME", "node1"),
            ("NODE_IP", "127.0.0.1"),
            ("SERVER_PORT", "8801"),
            ("MOUNT_PATH", "/tmp/datenlord_data_dir"),
            ("KV_SERVER_LIST", "127.0.0.1:7890,127.0.0.1:7891"),
        ];

        for &(k, v) in &args {
            env::set_var(k, v);
        }

        // Set the args
        let args = vec![
            "datenlord",
            "--config-file",
            temp_file_path.to_str().unwrap(),
            "--node-name",
            "node2",
        ];
        let arg_conf = Config::parse_from(args);
        let conf = Config::load_from_args(arg_conf).unwrap();

        assert_eq!(conf.role, Some("node".to_owned()));
        assert_eq!(conf.node_name, Some("node2".to_owned()));
    }

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
            "--log-level",
            "info",
        ];
        let config = Config::parse_from(args);
        assert_eq!(config.role, Some("node".to_owned()));
        assert_eq!(config.node_name, Some("node1".to_owned()));
        assert_eq!(config.node_ip, Some("127.0.0.1".to_owned()));
        assert_eq!(
            config.mount_path,
            Some("/tmp/datenlord_data_dir".to_owned())
        );
        assert_eq!(config.log_level, "info");

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
        assert_eq!(
            memory_cache_config.write_back_interval,
            Duration::from_millis(200)
        );
        assert_eq!(memory_cache_config.write_back_dirty_limit, 10);

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
            "--storage-mem-cache-write-back-interval",
            "1000",
            "--storage-mem-cache-write-back-dirty-limit",
            "100",
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

        assert_eq!(
            memory_cache_config.write_back_interval,
            Duration::from_millis(1000)
        );
        assert_eq!(memory_cache_config.write_back_dirty_limit, 100);
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
            "--log-level",
            "info",
        ];
        let config: InnerConfig = Config::parse_from(args).try_into().unwrap();
        assert_eq!(config.role, Role::Controller);
        assert_eq!(config.node_name, "localhost");
        assert_eq!(config.node_ip, IpAddr::from_str("127.0.0.1").unwrap());
        assert_eq!(config.mount_path.as_str(), "/tmp/datenlord_data_dir");
        assert_eq!(config.server_port, 8800);
        assert_eq!(config.log_level, LevelFilter::INFO);

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
