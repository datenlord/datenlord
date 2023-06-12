use crate::async_fuse::memfs::kv_engine::{
    self, KVEngine, KVEngineType, KeyType, LockKeyType, ValueType,
};
use crate::common::error::{Context, DatenLordResult};
use log::debug;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

/// Register current node to etcd.
/// The registered information contains IP.
pub async fn register_node_id(
    kv_engine: &Arc<KVEngineType>,
    node_id: &str,
    node_ipaddr: &str,
    port: &str,
) -> DatenLordResult<()> {
    kv_engine
        .set(
            &KeyType::NodeIpPort(node_id.to_owned()),
            &ValueType::String(format!("{node_ipaddr}:{port}")),
            None,
        )
        .await
        .with_context(|| {
            format!(
                "Fail to register node {node_id} to etcd, node_ipaddr:{node_ipaddr}, port:{port}",
            )
        })?;

    Ok(())
}

/// Get ip address and port of a node
pub async fn get_node_ip_and_port(
    kv_engine: &Arc<KVEngineType>,
    node_id: &str,
) -> DatenLordResult<String> {
    let ip_and_port = kv_engine
        .get(&KeyType::NodeIpPort(node_id.to_owned()))
        .await?;
    if let Some(value) = ip_and_port {
        let ip_and_port = value.into_string();
        debug!("node {} ip and port is {}", node_id, ip_and_port);
        Ok(ip_and_port)
    } else {
        debug!("node {} missing ip and port information", node_id);
        panic!("Ip and port is not registered for Node {node_id}")
    }
}

/// Register volume information, add the volume to `node_id` list mapping
pub async fn register_volume(
    kv_engine: &Arc<KVEngineType>,
    node_id: &str,
    volume_info: &str,
) -> anyhow::Result<()> {
    let _lock_key = kv_engine
        .lock(&LockKeyType::VolumeInfoLock, Duration::from_secs(10), None)
        .await?;

    let _volume_info_key = volume_info.to_owned();
    let volume_node_list = kv_engine
        .get(&KeyType::VolumeInfo(volume_info.to_owned()))
        .await?
        .map(kv_engine::ValueType::into_raw);

    /*
    let new_volume_node_list = if let Some(node_list) = volume_node_list {
        let mut node_set: HashSet<String> = bincode::deserialize(node_list.as_slice())
            .unwrap_or_else(|e| {
                panic!(
                    "fail to deserialize node list for volume {:?}, error: {}",
                    volume_info, e
                );
            });
        if !node_set.contains(node_id) {
            node_set.insert(node_id.to_owned());
        }

        node_set
    } else {
        let mut hash = HashSet::new();
        hash.insert(node_id.to_owned());
        hash
    };
    */

    let new_volume_node_list = volume_node_list.map_or_else(
        || {
            let mut hash = HashSet::new();
            hash.insert(node_id.to_owned());
            hash
        },
        |node_list| {
            let mut node_set: HashSet<String> = bincode::deserialize(node_list.as_slice())
                .unwrap_or_else(|e| {
                    panic!("fail to deserialize node list for volume {volume_info:?}, error: {e}");
                });
            if !node_set.contains(node_id) {
                node_set.insert(node_id.to_owned());
            }

            node_set
        },
    );

    let volume_node_list_bin = bincode::serialize(&new_volume_node_list).unwrap_or_else(|e| {
        panic!("fail to serialize node list for volume {volume_info:?}, error: {e}")
    });

    kv_engine
        .set(
            &KeyType::VolumeInfo(volume_info.to_owned()),
            &ValueType::Raw(volume_node_list_bin.clone()),
            None,
        )
        .await?;

    kv_engine
        .unlock(&LockKeyType::VolumeInfoLock)
        .await
        .with_context(|| "unlock fail while register volume")?;

    Ok(())
}

/// Get node list related to a volume, execluding the input `node_ide` as its the local node id.
/// This function is used to sync metadata, the inode information.
pub async fn get_volume_nodes(
    kv_engine: &Arc<KVEngineType>,
    node_id: &str,
    volume_info: &str,
) -> anyhow::Result<HashSet<String>> {
    let volume_info_key = volume_info.to_owned();
    let volume_node_list: Option<Vec<u8>> = kv_engine
        .get(&KeyType::VolumeInfo(volume_info_key.clone()))
        .await?
        .map(kv_engine::ValueType::into_raw);

    let new_volume_node_list = if let Some(node_list) = volume_node_list {
        let mut node_set: HashSet<String> = bincode::deserialize(node_list.as_slice())
            .unwrap_or_else(|e| {
                panic!("fail to deserialize node list for volume {volume_info:?}, error: {e}");
            });

        debug!("node set when get volume related node, {:?}", node_set);

        if node_set.contains(node_id) {
            node_set.remove(node_id);
        }

        node_set
    } else {
        debug!("node set is empty");
        HashSet::new()
    };

    Ok(new_volume_node_list)
}

/// Modify node list of a file
async fn modify_file_node_list<F: Fn(Option<Vec<u8>>) -> HashSet<String>>(
    kv_engine: &Arc<KVEngineType>,
    file_name: &[u8],
    fun: F,
) -> anyhow::Result<()>
where
    F: Send,
{
    let file_lock_key = file_name.to_vec();

    // FIXME : lock key should be a string
    let _lock_key = kv_engine
        .lock(
            &LockKeyType::FileNodeListLock(file_lock_key.clone()),
            Duration::from_secs(10),
            None,
        )
        .await
        .with_context(|| "lock fail update file node list")?;

    let node_list_key = file_name.to_vec();
    let node_list_key_clone = node_list_key.clone();

    let node_list: Option<Vec<u8>> = kv_engine
        .get(&KeyType::FileNodeList(node_list_key))
        .await?
        .map(kv_engine::ValueType::into_raw);

    let new_node_list = fun(node_list);

    let node_list_bin = bincode::serialize(&new_node_list).unwrap_or_else(|e| {
        panic!("fail to serialize node list for file {file_name:?}, error: {e}")
    });

    let _node_list_key_clone_2 = node_list_key_clone.clone();
    kv_engine
        .set(
            &KeyType::FileNodeList(node_list_key_clone),
            &ValueType::Raw(node_list_bin.clone()),
            None,
        )
        .await?;

    kv_engine
        .unlock(&LockKeyType::FileNodeListLock(file_lock_key))
        .await
        .with_context(|| "unlock fail while update file node list")?;

    Ok(())
}

/// Add a node to node list of a file
pub async fn add_node_to_file_list(
    kv_engine: &Arc<KVEngineType>,
    node_id: &str,
    file_name: &[u8],
) -> anyhow::Result<()> {
    let add_node_fun = |node_list: Option<Vec<u8>>| -> HashSet<String> {
        node_list.map_or_else(
            || {
                let mut node_set = HashSet::<String>::new();
                node_set.insert(node_id.to_owned());
                node_set
            },
            |list| {
                let mut node_set: HashSet<String> = bincode::deserialize(list.as_slice())
                    .unwrap_or_else(|e| {
                        panic!("fail to deserialize node list for file {file_name:?}, error: {e}");
                    });

                if !node_set.contains(node_id) {
                    node_set.insert(node_id.to_owned());
                }

                node_set
            },
        )
    };

    modify_file_node_list(kv_engine, file_name, add_node_fun).await
}

/// Remove a node to node list of a file
pub async fn remove_node_from_file_list(
    kv_engine: &Arc<KVEngineType>,
    node_id: &str,
    file_name: &[u8],
) -> anyhow::Result<()> {
    let remove_node_fun = |node_list: Option<Vec<u8>>| -> HashSet<String> {
        match node_list {
            Some(list) => {
                let mut node_set: HashSet<String> = bincode::deserialize(list.as_slice())
                    .unwrap_or_else(|e| {
                        panic!("fail to deserialize node list for file {file_name:?}, error: {e}");
                    });

                if node_set.contains(node_id) {
                    node_set.remove(node_id);
                }

                node_set
            }
            None => HashSet::<String>::new(),
        }
    };

    modify_file_node_list(kv_engine, file_name, remove_node_fun).await
}
