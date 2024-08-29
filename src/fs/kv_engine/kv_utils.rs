use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use crate::common::error::{Context, DatenLordResult};
use crate::fs::fs_util::INum;
use crate::fs::kv_engine::{self, KVEngine, KVEngineType, KeyType, LockKeyType, ValueType};

/// The kv lock 's timeout
const LOCK_TIME_OUT_SECS: u64 = 10;

/// Modify node list of a file
async fn modify_file_node_list<F: Fn(Option<Vec<u8>>) -> HashSet<String>>(
    kv_engine: &Arc<KVEngineType>,
    file_ino: INum,
    fun: F,
) -> DatenLordResult<()>
where
    F: Send,
{
    let lock_key = kv_engine
        .lock(
            &LockKeyType::FileNodeListLock(file_ino),
            Duration::from_secs(LOCK_TIME_OUT_SECS),
        )
        .await
        .with_context(|| "lock fail update file node list")?;

    let node_list: Option<Vec<u8>> = kv_engine
        .get(&KeyType::FileNodeList(file_ino))
        .await
        .with_context(|| format!("fail to get node list for file {file_ino:?}",))?
        .map(kv_engine::ValueType::into_raw);

    let new_node_list = fun(node_list);

    let node_list_bin = bincode::serialize(&new_node_list).unwrap_or_else(|e| {
        panic!("fail to serialize node list for file {file_ino:?}, error: {e}")
    });

    kv_engine
        .set(
            &KeyType::FileNodeList(file_ino),
            &ValueType::Raw(node_list_bin.clone()),
            None,
        )
        .await
        .with_context(|| format!("fail to set node list for file {file_ino:?}"))?;

    kv_engine
        .unlock(lock_key)
        .await
        .with_context(|| "unlock fail while update file node list")?;

    Ok(())
}

/// Add a node to node list of a file
pub async fn add_node_to_file_list(
    kv_engine: &Arc<KVEngineType>,
    node_id: &str,
    file_ino: INum,
) -> DatenLordResult<()> {
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
                        panic!("fail to deserialize node list for file {file_ino:?}, error: {e}");
                    });

                if !node_set.contains(node_id) {
                    node_set.insert(node_id.to_owned());
                }

                node_set
            },
        )
    };

    modify_file_node_list(kv_engine, file_ino, add_node_fun).await
}

/// Remove a node to node list of a file
pub async fn remove_node_from_file_list(
    kv_engine: &Arc<KVEngineType>,
    node_id: &str,
    file_ino: INum,
) -> DatenLordResult<()> {
    let remove_node_fun = |node_list: Option<Vec<u8>>| -> HashSet<String> {
        match node_list {
            Some(list) => {
                let mut node_set: HashSet<String> = bincode::deserialize(list.as_slice())
                    .unwrap_or_else(|e| {
                        panic!("fail to deserialize node list for file {file_ino:?}, error: {e}");
                    });

                if node_set.contains(node_id) {
                    node_set.remove(node_id);
                }

                node_set
            }
            None => HashSet::<String>::new(),
        }
    };

    modify_file_node_list(kv_engine, file_ino, remove_node_fun).await
}
