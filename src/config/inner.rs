use std::net::IpAddr;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::common::error::DatenLordError;
use crate::config::config::{
    CSIConfig, Config as SuperConfig, S3StorageConfig as SuperS3StorageConfig,
    StorageConfig as SuperStorageConfig,
};

/// The role of the node
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Run as controller
    Controller,
    /// Run as node
    Node,
    /// Run as scheduler extender
    SchedulerExtender,
    /// Run async fuse
    AsyncFuse,
}

impl FromStr for Role {
    type Err = DatenLordError;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "controller" => Ok(Role::Controller),
            "node" => Ok(Role::Node),
            "scheduler" => Ok(Role::SchedulerExtender),
            "asyncFuse" => Ok(Role::AsyncFuse),
            _ => Err(DatenLordError::ArgumentInvalid {
                context: vec![format!("role {} is not supported", s)],
            }),
        }
    }
}

/// Inner config struct
/// This struct is used to store the parsed config
/// and will be used to initialize the server
#[derive(Clone, Debug)]
pub struct InnerConfig {
    /// Role
    pub role: Role,
    /// Node name
    pub node_name: String,
    /// Node ip
    pub node_ip: IpAddr,
    /// Mount path
    pub mount_path: String,
    /// kv server addresses
    pub kv_addrs: Vec<String>,
    /// Service port number
    pub server_port: u16,
    /// Storage related config
    pub storage: StorageConfig,
    /// CSI related config
    pub csi_config: CSIConfig,
}

impl TryFrom<SuperConfig> for InnerConfig {
    type Error = DatenLordError;

    #[inline]
    fn try_from(value: SuperConfig) -> Result<Self, Self::Error> {
        let role = Role::from_str(value.role.as_str())?;
        let node_name = value.node_name;
        let server_port = value.server_port;
        let node_ip = IpAddr::from_str(value.node_ip.as_str()).map_err(|e| {
            DatenLordError::ArgumentInvalid {
                context: vec![format!("node ip {} is invalid: {}", value.node_ip, e)],
            }
        })?;
        let mount_path = value.mount_path;
        let storage = value.storage.try_into()?;
        let kv_addrs: Vec<String> = value
            .kv_addr
            .split(',')
            .map(std::string::ToString::to_string)
            .collect();
        if kv_addrs.is_empty() {
            return Err(DatenLordError::ArgumentInvalid {
                context: vec!["kv server addresses is empty".to_owned()],
            });
        }
        Ok(InnerConfig {
            role,
            node_name,
            server_port,
            node_ip,
            mount_path,
            storage,
            kv_addrs,
            csi_config: value.csi_config,
        })
    }
}

/// Storage config
/// Cache related config, currently only support cache capacity
/// Storage backend related config, currently only support S3
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Cache capacity
    pub cache_capacity: usize,
    /// Storage params
    pub params: StorageParams,
}

impl TryFrom<SuperStorageConfig> for StorageConfig {
    type Error = DatenLordError;

    #[inline]
    fn try_from(value: SuperStorageConfig) -> Result<Self, Self::Error> {
        let cache_capacity = value.cache_capacity;
        let params = match value.storage_type.as_str() {
            "S3" => StorageParams::S3(value.s3_storage_config.try_into()?),
            "none" => StorageParams::None(value.s3_storage_config.try_into()?),
            _ => {
                return Err(DatenLordError::ArgumentInvalid {
                    context: vec![format!(
                        "storage type {} is not supported",
                        value.storage_type
                    )],
                })
            }
        };
        Ok(StorageConfig {
            cache_capacity,
            params,
        })
    }
}

/// Storage backend related config
/// S3 : `endpoint_url`, `access_key_id`, `secret_access_key`, `bucket_name`
/// None : currently it equal to a fake s3 storage, we will refactor it soon
/// TODO(xiaguan) add more storage types
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StorageParams {
    /// S3 storage params
    S3(StorageS3Config),
    /// None storage : currently it equal to a fake s3 storage
    None(StorageS3Config),
}

/// S3 config struct
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageS3Config {
    /// Endpoint url
    pub endpoint_url: String,
    /// Access key id
    pub access_key_id: String,
    /// Secret access key
    pub secret_access_key: String,
    /// Bucket name
    pub bucket_name: String,
}

impl TryFrom<SuperS3StorageConfig> for StorageS3Config {
    type Error = DatenLordError;

    #[inline]
    fn try_from(value: SuperS3StorageConfig) -> Result<Self, Self::Error> {
        Ok(StorageS3Config {
            endpoint_url: value.endpoint_url,
            access_key_id: value.access_key_id,
            secret_access_key: value.secret_access_key,
            bucket_name: value.bucket_name,
        })
    }
}
