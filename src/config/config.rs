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
    /// Mount path
    pub mount_path: String,
    #[clap(flatten)]
    /// Storage related config
    pub storage: StorageConfig,
}

#[derive(Debug, Parser)]
/// Storage config
pub struct StorageConfig {
    #[clap(long = "storage-type", value_name = "VALUE", default_value = "None")]
    /// Storage type: S3,None
    pub storage_type: String,
    #[clap(
        long = "storage-cache-capacity",
        value_name = "VALUE",
        default_value = "104857600"
    )]
    /// Cache capacity, default 100MB
    pub cache_capacity: usize,
    #[clap(flatten)]
    /// S3 storage config
    pub s3_storage_config: S3StorageConfig,
}

#[derive(Debug, Parser)]
pub struct S3StorageConfig {
    #[clap(long = "endpoint-url", value_name = "VALUE", default_value_t)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::inner::StorageParams as InnerStorageParams;
    use crate::config::inner::{InnerConfig, Role};
    use std::net::IpAddr;
    use std::str::FromStr;

    #[test]
    fn test_default_config() {
        // Set the args
        let args = vec![
            "datenlord",
            "--role",
            "Node",
            "--node-name",
            "node1",
            "--node-ip",
            "127.0.0.1",
            "--mount-path",
            "/tmp/datenlord_data_dir",
        ];
        let config = Config::parse_from(args);
        assert_eq!(config.role, "Node");
        assert_eq!(config.node_name, "node1");
        assert_eq!(config.node_ip.as_str(), "127.0.0.1");
        assert_eq!(config.mount_path.as_str(), "/tmp/datenlord_data_dir");

        // Following are the default values
        assert_eq!(config.storage.storage_type, "None");
        assert_eq!(config.storage.cache_capacity, 104857600);

        // Cast to InnerConfig
        let inner_config: InnerConfig = config.try_into().unwrap();
        assert_eq!(inner_config.role, Role::Node);
        assert_eq!(inner_config.node_name, "node1");
        assert_eq!(inner_config.node_ip, IpAddr::from_str("127.0.0.1").unwrap());
        assert_eq!(inner_config.mount_path.as_str(), "/tmp/datenlord_data_dir");

        let storage_config = inner_config.storage;
        assert_eq!(storage_config.cache_capacity, 104857600);
        match storage_config.params {
            InnerStorageParams::None => {}
            _ => panic!("storage params should be None"),
        }
    }

    #[test]
    fn test_s3_config() {
        // Set the args
        let args = vec![
            "datenlord",
            "--role",
            "Node",
            "--node-name",
            "node1",
            "--node-ip",
            "127.0.0.1",
            "--mount-path",
            "/tmp/datenlord_data_dir",
            "--storage-type",
            "S3",
            "--storage-cache-capacity",
            "1024",
            "--endpoint-url",
            "http://127.0.0.1:9000",
            "--storage-s3-access-key-id",
            "test_access_key",
            "--storage-s3-secret-access-key",
            "test_secret_key",
            "--storage-s3-bucket",
            "test_bucket",
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
            _ => panic!("storage params should be S3"),
        }
    }
}
