use crate::common::error::DatenLordError;
use crate::config::config::Config as SuperConfig;
use crate::config::config::S3StorageConfig as SuperS3StorageConfig;
use crate::config::config::StorageConfig as SuperStorageConfig;
use std::net::IpAddr;
use std::str::FromStr;

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

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Controller" => Ok(Role::Controller),
            "Node" => Ok(Role::Node),
            "Scheduler" => Ok(Role::SchedulerExtender),
            "AsyncFuse" => Ok(Role::AsyncFuse),
            _ => Err(DatenLordError::ArgumentInvalid {
                context: vec![format!("role {} is not supported", s)],
            }),
        }
    }
}

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
    /// Storage related config
    pub storage: StorageConfig,
}

impl TryFrom<SuperConfig> for InnerConfig {
    type Error = DatenLordError;

    fn try_from(value: SuperConfig) -> Result<Self, Self::Error> {
        let role = Role::from_str(value.role.as_str())?;
        let node_name = value.node_name;
        let node_ip = IpAddr::from_str(value.node_ip.as_str()).map_err(|e| {
            DatenLordError::ArgumentInvalid {
                context: vec![format!("node ip {} is invalid: {}", value.node_ip, e)],
            }
        })?;
        let mount_path = value.mount_path;
        let storage = value.storage.try_into()?;
        Ok(InnerConfig {
            role,
            node_name,
            node_ip,
            mount_path,
            storage,
        })
    }
}

#[derive(Clone, Debug)]
pub struct StorageConfig {
    /// Cache capacity
    pub cache_capacity: usize,
    /// Storage params
    pub params: StorageParams,
}

impl TryFrom<SuperStorageConfig> for StorageConfig {
    type Error = DatenLordError;

    fn try_from(value: SuperStorageConfig) -> Result<Self, Self::Error> {
        let cache_capacity = value.cache_capacity;
        let params = match value.storage_type.as_str() {
            "S3" => StorageParams::S3(value.s3_storage_config.try_into()?),
            "None" => StorageParams::None,
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

/// TODO(xiaguan) add more storage types
#[derive(Clone, Debug)]
pub enum StorageParams {
    S3(StorageS3Config),
    /// Currently it equal to memory only, we will refactor it soon
    None,
}

#[derive(Clone, Debug)]
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

    fn try_from(value: SuperS3StorageConfig) -> Result<Self, Self::Error> {
        // Check if any of the required fields are missing
        if value.endpoint_url.is_empty()
            || value.access_key_id.is_empty()
            || value.secret_access_key.is_empty()
            || value.bucket_name.is_empty()
        {
            return Err(DatenLordError::ArgumentInvalid {
                context: vec![format!("S3 storage config is invalid: {:?}", value)],
            });
        }
        Ok(StorageS3Config {
            endpoint_url: value.endpoint_url,
            access_key_id: value.access_key_id,
            secret_access_key: value.secret_access_key,
            bucket_name: value.bucket_name,
        })
    }
}
