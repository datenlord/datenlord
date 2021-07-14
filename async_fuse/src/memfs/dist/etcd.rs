use common::error::Context;
use common::etcd_delegate::EtcdDelegate;
use log::debug;
use std::collections::HashSet;
use std::env;
use std::sync::Arc;

/// Ip address environment var name
const ENV_IP_ADDRESS: &str = "pod_ip_address";
/// ETCD node id lock
const ETCD_NODE_ID_LOCK: &str = "datenlord_node_id_lock";
/// ETCD node id counter key
const ETCD_NODE_ID_COUNTER_KEY: &str = "datenlord_node_id_counter";
/// ETCD node ip and port info
const ETCD_NODE_IP_PORT_PREFIX: &str = "datenlord_node_ip_info_";
/// ETCD volume information lock
const ETCD_VOLUME_INFO_LOCK: &str = "datenlord_volume_info_lock";
/// ETCD volume information prefix
const ETCD_VOLUME_INFO_PREFIX: &str = "datenlord_volume_info_";
/// ETCD file node list lock prefix
const ETCD_FILE_NODE_LIST_LOCK_PREFIX: &str = "datenlord_file_node_list_lock_";
/// ETCD file node list prefix
const ETCD_FILE_NODE_LIST_PREFIX: &str = "datenlord_file_node_list_";

/// Register current node to etcd and get a dedicated node id.
/// The registered information contains IP.
pub(crate) async fn register_node_id(
    etcd_client: &EtcdDelegate,
    port: &str,
) -> anyhow::Result<(u64, String)> {
    let lock_key = etcd_client
        .lock(ETCD_NODE_ID_LOCK.as_bytes(), 10)
        .await
        .with_context(|| "lock fail while register node_id")?;

    let node_id: Option<Vec<u8>> = etcd_client
        .get_at_most_one_value(ETCD_NODE_ID_COUNTER_KEY)
        .await
        .with_context(|| format!("get {} from etcd fail", ETCD_NODE_ID_COUNTER_KEY))?;

    let node_id = match node_id {
        Some(current_id_str) => {
            let current_id = std::str::from_utf8(current_id_str.as_slice())?;
            let (new_id, overflow) = current_id
                .parse::<u64>()
                .unwrap_or_else(|_| {
                    panic!("{} can't be parsed as node id (integer)", current_id);
                })
                .overflowing_add(1);
            if overflow {
                panic!("node_id({}) is too large, it overflows", current_id);
            } else {
                new_id
            }
        }
        None => 0,
    };

    let node_id_string = node_id.to_string();

    etcd_client
        .write_or_update_kv(ETCD_NODE_ID_COUNTER_KEY, &node_id_string)
        .await
        .with_context(|| {
            format!(
                "update {} to value {} failed",
                ETCD_NODE_ID_COUNTER_KEY, node_id_string
            )
        })?;

    etcd_client
        .unlock(lock_key)
        .await
        .with_context(|| "unlock fail while register node_id")?;

    let ip = env::var(ENV_IP_ADDRESS).unwrap_or_else(|_| {
        panic!(
            "pod Ip address should be assigned via environment variable: {}!",
            ENV_IP_ADDRESS
        );
    });

    etcd_client
        .write_or_update_kv(
            format!("{}{}", ETCD_NODE_IP_PORT_PREFIX, node_id_string).as_str(),
            &format!("{}:{}", ip, port),
        )
        .await
        .with_context(|| {
            format!(
                "Update Node Ip address failed, node_id:{}, ip: {}, port: {}",
                node_id_string, ip, port
            )
        })?;

    Ok((node_id, ip))
}

pub(crate) async fn get_node_ip_and_port(
    etcd_client: Arc<EtcdDelegate>,
    node_id: u64,
) -> anyhow::Result<String> {
    let node_id_string = node_id.to_string();

    let ip_and_port = etcd_client
        .get_at_most_one_value(format!("{}{}", ETCD_NODE_IP_PORT_PREFIX, node_id_string))
        .await
        .with_context(|| {
            format!(
                "Fail to get Node Ip address and port information, node_id:{}",
                node_id_string
            )
        })?;

    match ip_and_port {
        Some(ip_and_port) => {
            debug!("node {} ip and port is {}", node_id, ip_and_port);
            Ok(ip_and_port)
        }
        None => {
            debug!("node {} missing ip and port information", node_id);
            Err(anyhow::Error::msg(format!(
                "Ip and port is not registered for Node {}",
                node_id_string
            )))
        }
    }
}

/// Register volume information, add the volume to `node_id` list mapping
pub(crate) async fn register_volume(
    etcd_client: &EtcdDelegate,
    node_id: u64,
    volume_info: &str,
) -> anyhow::Result<()> {
    let lock_key = etcd_client
        .lock(ETCD_VOLUME_INFO_LOCK.as_bytes(), 10)
        .await
        .with_context(|| "lock fail while register volume")?;

    let volume_info_key = format!("{}{}", ETCD_VOLUME_INFO_PREFIX, volume_info);
    let volume_node_list: Option<Vec<u8>> = etcd_client
        .get_at_most_one_value(volume_info_key.as_str())
        .await
        .with_context(|| format!("get {} from etcd fail", ETCD_NODE_ID_COUNTER_KEY))?;

    let new_volume_node_list = match volume_node_list {
        Some(node_list) => {
            let mut node_set: HashSet<u64> = bincode::deserialize(node_list.as_slice())
                .unwrap_or_else(|e| {
                    panic!(
                        "fail to deserialize node list for volume {:?}, error: {}",
                        volume_info, e
                    );
                });
            if !node_set.contains(&node_id) {
                node_set.insert(node_id);
            }

            node_set
        }
        None => {
            let mut hash = HashSet::new();
            hash.insert(node_id);
            hash
        }
    };

    let volume_node_list_bin = bincode::serialize(&new_volume_node_list).unwrap_or_else(|e| {
        panic!(
            "fail to serialize node list for volume {:?}, error: {}",
            volume_info, e
        )
    });

    etcd_client
        .write_or_update_kv(volume_info_key.as_str(), &volume_node_list_bin)
        .await
        .with_context(|| {
            format!(
                "Update Volume to Node Id mapping failed, volume:{}, node id: {}",
                volume_info, node_id
            )
        })?;

    etcd_client
        .unlock(lock_key)
        .await
        .with_context(|| "unlock fail while register volume")?;
    Ok(())
}

/// Get node list related to a volume, execluding the input `node_ide` as its the local node id.
/// This function is used to sync metadata, the inode information.
pub(crate) async fn get_volume_nodes(
    etcd_client: Arc<EtcdDelegate>,
    node_id: u64,
    volume_info: &str,
) -> anyhow::Result<HashSet<u64>> {
    let volume_info_key = format!("{}{}", ETCD_VOLUME_INFO_PREFIX, volume_info);
    let volume_node_list: Option<Vec<u8>> = etcd_client
        .get_at_most_one_value(volume_info_key.as_str())
        .await
        .with_context(|| format!("get {} from etcd fail", ETCD_NODE_ID_COUNTER_KEY))?;

    let new_volume_node_list = match volume_node_list {
        Some(node_list) => {
            let mut node_set: HashSet<u64> = bincode::deserialize(node_list.as_slice())
                .unwrap_or_else(|e| {
                    panic!(
                        "fail to deserialize node list for volume {:?}, error: {}",
                        volume_info, e
                    );
                });

            debug!("node set when get volume related node, {:?}", node_set);

            if node_set.contains(&node_id) {
                node_set.remove(&node_id);
            }

            node_set
        }
        None => {
            debug!("node set is empty");
            HashSet::new()
        }
    };

    Ok(new_volume_node_list)
}

async fn modify_file_node_list<F: Fn(Option<Vec<u8>>) -> HashSet<u64>>(
    etcd_client: &EtcdDelegate,
    file_name: &[u8],
    fun: F,
) -> anyhow::Result<()> {
    let mut file_lock_key = ETCD_FILE_NODE_LIST_LOCK_PREFIX.as_bytes().to_vec();
    file_lock_key.extend_from_slice(file_name);

    let lock_key = etcd_client
        .lock(file_lock_key.as_slice(), 10)
        .await
        .with_context(|| "lock fail update file node list")?;

    let mut node_list_key = ETCD_FILE_NODE_LIST_PREFIX.as_bytes().to_vec();
    node_list_key.extend_from_slice(file_name);
    let node_list_key_clone = node_list_key.clone();

    let node_list: Option<Vec<u8>> = etcd_client
        .get_at_most_one_value(node_list_key)
        .await
        .with_context(|| format!("get {} from etcd fail", ETCD_NODE_ID_COUNTER_KEY))?;

    let new_node_list = fun(node_list);

    let node_list_bin = bincode::serialize(&new_node_list).unwrap_or_else(|e| {
        panic!(
            "fail to serialize node list for file {:?}, error: {}",
            file_name, e
        )
    });

    let node_list_key_clone_2 = node_list_key_clone.clone();
    etcd_client
        .write_or_update_kv(node_list_key_clone, &node_list_bin)
        .await
        .with_context(|| {
            format!(
                "update {:?} to value {:?} failed",
                node_list_key_clone_2, node_list_bin
            )
        })?;

    etcd_client
        .unlock(lock_key)
        .await
        .with_context(|| "unlock fail while update file node list")?;

    Ok(())
}

pub(crate) async fn add_node_to_file_list(
    etcd_client: &EtcdDelegate,
    node_id: u64,
    file_name: &[u8],
) -> anyhow::Result<()> {
    let add_node_fun = |node_list: Option<Vec<u8>>| -> HashSet<u64> {
        match node_list {
            Some(list) => {
                let mut node_set: HashSet<u64> = bincode::deserialize(list.as_slice())
                    .unwrap_or_else(|e| {
                        panic!(
                            "fail to deserialize node list for file {:?}, error: {}",
                            file_name, e
                        );
                    });

                if !node_set.contains(&node_id) {
                    node_set.insert(node_id);
                }

                node_set
            }
            None => {
                let mut node_set = HashSet::<u64>::new();
                node_set.insert(node_id);
                node_set
            }
        }
    };

    modify_file_node_list(etcd_client, file_name, add_node_fun).await
}

pub(crate) async fn remove_node_from_file_list(
    etcd_client: &EtcdDelegate,
    node_id: u64,
    file_name: &[u8],
) -> anyhow::Result<()> {
    let remove_node_fun = |node_list: Option<Vec<u8>>| -> HashSet<u64> {
        match node_list {
            Some(list) => {
                let mut node_set: HashSet<u64> = bincode::deserialize(list.as_slice())
                    .unwrap_or_else(|e| {
                        panic!(
                            "fail to deserialize node list for file {:?}, error: {}",
                            file_name, e
                        );
                    });

                if node_set.contains(&node_id) {
                    node_set.remove(&node_id);
                }

                node_set
            }
            None => HashSet::<u64>::new(),
        }
    };

    modify_file_node_list(etcd_client, file_name, remove_node_fun).await
}
