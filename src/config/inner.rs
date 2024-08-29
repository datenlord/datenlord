use std::net::IpAddr;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::level_filters::LevelFilter as Level;

use crate::common::error::DatenLordError;
use crate::config::config::{
    CSIConfig as SupperCSIConfig, Config as SuperConfig,
    MemoryCacheConfig as SuperMemoryCacheConfig, S3StorageConfig as SuperS3StorageConfig,
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
    /// Run as sdk
    SDK,
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
            "sdk" => Ok(Role::SDK),
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
    /// Log level
    pub log_level: Level,
    /// kv server addresses
    pub kv_addrs: Vec<String>,
    /// Service port number
    pub server_port: u16,
    /// Set the port of the scheduler extender
    pub scheduler_extender_port: u16,
    /// Storage related config
    pub storage: StorageConfig,
    /// CSI related config
    pub csi_config: CSIConfig,
}

impl TryFrom<SuperConfig> for InnerConfig {
    type Error = DatenLordError;

    #[inline]
    fn try_from(value: SuperConfig) -> Result<Self, Self::Error> {
        let Some(role_str) = value.role else {
            return Err(DatenLordError::ArgumentInvalid {
                context: vec!["role is empty".to_owned()],
            });
        };
        let log_level = Level::from_str(value.log_level.as_str()).map_err(|e| {
            DatenLordError::ArgumentInvalid {
                context: vec![format!("log level {} is invalid: {}", value.log_level, e)],
            }
        })?;
        let role = Role::from_str(&role_str)?;
        let Some(node_name) = value.node_name else {
            return Err(DatenLordError::ArgumentInvalid {
                context: vec!["node name is empty".to_owned()],
            });
        };
        let server_port = value.server_port;
        let scheduler_extender_port = value.scheduler_extender_port;
        let Some(node_ip_str) = value.node_ip else {
            return Err(DatenLordError::ArgumentInvalid {
                context: vec!["node ip is empty".to_owned()],
            });
        };
        let node_ip = IpAddr::from_str(node_ip_str.as_str()).map_err(|e| {
            DatenLordError::ArgumentInvalid {
                context: vec![format!(
                    "node ip {} is invalid: {}",
                    node_ip_str.as_str(),
                    e
                )],
            }
        })?;
        let Some(mount_path) = value.mount_path else {
            return Err(DatenLordError::ArgumentInvalid {
                context: vec!["mount path is empty".to_owned()],
            });
        };
        let storage = value.storage.try_into()?;
        let kv_addrs: Vec<String> = value.kv_server_list;
        if kv_addrs.is_empty() {
            return Err(DatenLordError::ArgumentInvalid {
                context: vec!["kv server addresses is empty".to_owned()],
            });
        }
        let csi_config = value.csi_config.try_into()?;
        Ok(InnerConfig {
            role,
            node_name,
            node_ip,
            mount_path,
            log_level,
            kv_addrs,
            server_port,
            scheduler_extender_port,
            storage,
            csi_config,
        })
    }
}

/// Storage related config
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageConfig {
    /// The size of blocks, default is 512 KiB.
    pub block_size: usize,
    /// Cache capacity
    pub memory_cache_config: MemoryCacheConfig,
    /// Storage params
    pub params: StorageParams,
}

impl TryFrom<SuperStorageConfig> for StorageConfig {
    type Error = DatenLordError;

    #[inline]
    fn try_from(value: SuperStorageConfig) -> Result<Self, Self::Error> {
        let memory_cache_config = value.memory_cache_config.try_into()?;
        let params = match value.storage_type.to_lowercase().as_str() {
            "s3" => StorageParams::S3(value.s3_storage_config.try_into()?),
            "fs" => StorageParams::Fs(value.fs_storage_root),
            _ => {
                return Err(DatenLordError::ArgumentInvalid {
                    context: vec![format!(
                        "storage type {} is not supported",
                        value.storage_type
                    )],
                })
            }
        };
        let block_size = value.block_size;
        Ok(StorageConfig {
            block_size,
            memory_cache_config,
            params,
        })
    }
}

/// Memory cache config
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MemoryCacheConfig {
    /// The capacity of memory cache in bytes, default is 8 GiB.
    pub capacity: usize,
    /// The limitation of the message queue of the write back task, default is
    /// `1000`
    pub command_queue_limit: usize,
    /// A flag whether the cache runs in write-back policy, default is `false`
    pub write_back: bool,
    /// A soft limit of evict policy for write back task.
    ///
    /// It's a fraction with the form of `a,b`, which means that
    /// the soft limit is `a/b` of the capacity.
    pub soft_limit: SoftLimit,
    /// he interval of a write back cycle.
    pub write_back_interval: Duration,
    /// The max count of pending dirty blocks.
    pub write_back_dirty_limit: usize,
}

/// A type to represent the soft limit of cache.
///
/// It's represented as a fraction, for example,
/// a soft limit of `SoftLimit(3, 5)` means the soft limit is
/// `3/5` if the policy's capacity.
///
/// Because the second component of this struct is used as a denominator,
/// it cannot be zero, therefore, we use `NonZeroUsize` here.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SoftLimit(pub usize, pub NonZeroUsize);

impl FromStr for SoftLimit {
    type Err = DatenLordError;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((a, b)) = s.split_once(',') {
            let a = a.trim().parse::<usize>().map_err(|e|
                DatenLordError::ArgumentInvalid { context: vec![
                    e.to_string(),
                    "The first component of `--storage-mem-cache-soft-limit` must be a valid value of `usize`.".to_owned(),
                    ] }
            )?;
            let b = b.trim().parse::<NonZeroUsize>().map_err(|e|
                DatenLordError::ArgumentInvalid { context: vec![
                    e.to_string(),
                    "The second component of `--storage-mem-cache-soft-limit` must be a valid value of `usize` and not zero.".to_owned(),
                    ] }
            )?;

            Ok(SoftLimit(a, b))
        } else {
            Err(DatenLordError::ArgumentInvalid {
                context: vec![
                    "Argument `--storage-mem-cache-soft-limit` must be in form of `a,b`."
                        .to_owned(),
                ],
            })
        }
    }
}

impl TryFrom<SuperMemoryCacheConfig> for MemoryCacheConfig {
    type Error = DatenLordError;

    #[inline]
    fn try_from(value: SuperMemoryCacheConfig) -> Result<Self, Self::Error> {
        let SuperMemoryCacheConfig {
            capacity,
            command_queue_limit,
            write_back,
            soft_limit,
            write_back_interval,
            write_back_dirty_limit,
        } = value;

        Ok(Self {
            capacity,
            command_queue_limit,
            write_back,
            soft_limit: soft_limit.parse()?,
            write_back_interval: Duration::from_millis(write_back_interval),
            write_back_dirty_limit,
        })
    }
}

/// Storage backend related config
///
/// - `S3` : `endpoint_url`, `access_key_id`, `secret_access_key`, `bucket_name`
/// - `Fs` : A local filesystem based backend, with argument `backend_root`.
/// TODO(xiaguan) add more storage types
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StorageParams {
    /// S3 storage params
    S3(StorageS3Config),
    /// Fs storage
    Fs(String),
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
    /// Region
    pub region: Option<String>,
    /// Max concurrent requests
    pub max_concurrent_requests: Option<usize>,
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
            region: value.region,
            max_concurrent_requests: value.max_concurrent_requests,
        })
    }
}

/// CSI config struct
#[derive(Clone, Debug)]
pub struct CSIConfig {
    /// CSI endpoint
    pub endpoint: String,
    /// CSI driver name
    pub driver_name: String,
    /// CSI worker port
    pub worker_port: u16,
}

impl TryFrom<SupperCSIConfig> for CSIConfig {
    type Error = DatenLordError;

    #[inline]
    fn try_from(value: SupperCSIConfig) -> Result<Self, Self::Error> {
        let endpoint = value.endpoint;
        let driver_name = value.driver_name;
        let worker_port = value.worker_port;
        Ok(CSIConfig {
            endpoint,
            driver_name,
            worker_port,
        })
    }
}
