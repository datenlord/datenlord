//! distributed rwlock implementation
use super::{
    error::DatenLordResult,
    etcd_delegate::{EtcdDelegate, KvVersion},
};
use clippy_utilities::OverflowArithmetic;
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, time::Duration};

/// Distributed rwlock
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum DistRwLockType {
    /// read lock
    RLock,
    /// write lock
    WLock,
}

/// Serialize to lock value
#[derive(Serialize, Deserialize, Debug, Clone)]
struct RwLockValue {
    /// type of lock
    locktype: DistRwLockType,
    /// maybe node ip or node id, to verify the ownership of lock
    tags_of_nodes: HashSet<String>,
}

/// Renew the lease for continue use the lock key.
///
/// - `version` - Previous version of the lock need to be updated
#[inline]
async fn renew_lease(
    etcd: &EtcdDelegate,
    key: &str,
    value: RwLockValue,
    fail_ctx_info: &str,
    version: KvVersion,
    timeout: Duration,
) -> DatenLordResult<bool> {
    match etcd
        .write_or_update_kv_with_version(key, &value, version, Some(timeout))
        .await
    {
        Ok(res) => {
            if res.is_some() {
                Ok(false)
            } else {
                log::debug!("dist rw lock by renew {}", key);
                Ok(true)
            }
        }
        Err(err) => {
            log::warn!("renew_lease failed,fail_ctxinfo:{fail_ctx_info},err:{err}");
            Err(err)
        }
    }
}

/// update lock info
#[inline]
async fn update_lock(
    etcd: &EtcdDelegate,
    key: &str,
    value: RwLockValue,
    fail_ctx_info: &str,
    version: KvVersion,
) -> DatenLordResult<bool> {
    match etcd
        .write_or_update_kv_with_version(key, &value, version, None)
        .await
    {
        Ok(res) => {
            if res.is_some() {
                Ok(false)
            } else {
                Ok(true)
            }
        }
        Err(err) => {
            log::warn!("update_lock failed,fail_ctxinfo:{fail_ctx_info},err:{err}");
            Err(err)
        }
    }
}

#[inline]
/// Helper function to wait for a key to be deleted to get the lock.
async fn wait_release(etcd: &EtcdDelegate, key: &str, fail_ctx_info: &str) -> DatenLordResult<()> {
    match etcd.wait_key_delete(key).await {
        Ok(_) => Ok(()),
        Err(err) => {
            log::warn!("wait_release failed,fail_ctxinfo:{fail_ctx_info},err:{err}");
            Err(err)
        }
    }
}

#[inline]
#[allow(dead_code)]
/// Helper function to remove a lock kv
async fn remove_key(etcd: &EtcdDelegate, key: &str) -> DatenLordResult<()> {
    match etcd.delete_one_value::<RwLockValue>(key).await {
        Ok(_) => Ok(()),
        Err(err) => Err(err),
    }
}

/// Inner lock function of try lock and lock function
/// - if try lock failed, return current lock type
/// - else return None
#[allow(clippy::else_if_without_else)] // conflict with redundant else
#[allow(dead_code)]
async fn rw_lock_impl(
    etcd: &EtcdDelegate,
    name: &str,
    locktype: DistRwLockType,
    timeout: Duration,
    tag_of_local_node: &str, // mark node tag
    try_or_block: bool,
) -> DatenLordResult<Option<DistRwLockType>> {
    // todo1 fairness
    // todo2 timeout of different read lock
    let mut failtime = 0_i32;
    //  It's ok because we only care for the last read lock to be release.
    loop {
        // fix: we need a version to make sure the update is safe.
        let res = etcd
            .write_new_kv_no_panic_return_with_version(
                name,
                &RwLockValue {
                    locktype: locktype.clone(),
                    tags_of_nodes: {
                        let mut s = HashSet::new();
                        s.insert(tag_of_local_node.to_owned());
                        s
                    },
                },
                Some(timeout),
            )
            .await?;

        // lock exists
        if let Some((mut res, version)) = res {
            if res.locktype == DistRwLockType::RLock && locktype == DistRwLockType::RLock {
                // remote: r | current: r
                // check if the node exist
                // 1. if node exists in the set, renew the lease (already hold)
                // 2. if node doesn't exist, add it into the set and renew the lease
                res.tags_of_nodes.insert(tag_of_local_node.to_owned());
                // must todo:: when there's conflict, we should offer the version and use the transaction.
                //  make sure the operated data is the version we got.
                if !renew_lease(etcd, name, res, "renew read lock", version, timeout).await? {
                    failtime = failtime.overflow_add(1_i32);
                    log::debug!("etcd renew_lease txn failed on node {tag_of_local_node}, will retry, fail time: {failtime}");
                    continue;
                }

                return Ok(None);
            } else if res.locktype == DistRwLockType::WLock && locktype == DistRwLockType::WLock {
                // remote: w | current: w
                // 1. if same node, renew the lease
                if res.tags_of_nodes.contains(tag_of_local_node) {
                    if !renew_lease(etcd, name, res, "renew write lock", version, timeout).await? {
                        failtime = failtime.overflow_add(1_i32);
                        log::debug!("etcd renew_lease txn failed on node {tag_of_local_node}, will retry, fail time: {failtime}");
                        continue;
                    }
                    return Ok(None);
                }
                // 2. try lock, return current lock
                else if try_or_block {
                    return Ok(Some(res.locktype));
                }
                // 3. else, wait release
                wait_release(etcd, name, "need write lock, wait for write lock").await?;
            }
            // remote: r | current: w
            // remote: w | current: r
            else if try_or_block {
                return Ok(Some(res.locktype));
            } else {
                wait_release(etcd, name, "different lock type, wait for release").await?;
            }
        }
        // lock successfully
        else {
            log::debug!("dist rw lock directly {}", name);
            return Ok(None);
        }
    }
}

// todo
//  lock timeout log

/// Lock a rwlock
///  if txn failed, will retry.
///  if kv error occured, will return the error directly
#[inline]
#[allow(dead_code)]
pub async fn rw_lock(
    etcd: &EtcdDelegate,
    name: &str,
    locktype: DistRwLockType,
    timeout: Duration,
    tag_of_local_node: &str, // mark node tag
) -> DatenLordResult<()> {
    rw_lock_impl(etcd, name, locktype, timeout, tag_of_local_node, false)
        .await
        .map(|_| ())
}

/// Try to lock a rwlock
///  if txn failed, will retry.
///  if kv error occured, will return the error directly
/// - if try lock failed, return current lock type
/// - else return None
#[inline]
#[allow(dead_code)]
pub async fn rw_try_lock(
    etcd: &EtcdDelegate,
    name: &str,
    locktype: DistRwLockType,
    timeout: Duration,
    tag_of_local_node: &str, // mark node tag
) -> DatenLordResult<Option<DistRwLockType>> {
    rw_lock_impl(etcd, name, locktype, timeout, tag_of_local_node, true).await
}

/// Unlock rwlock
///  if txn failed, will retry.
///  if kv error occured, will return the error directly
/// - if unlock failed, return false
/// - else return true
#[inline]
#[allow(dead_code)]
pub async fn rw_unlock(
    etcd: &EtcdDelegate,
    name: &str,
    tag_of_local_node: &str, // mark node tag
) -> DatenLordResult<bool> {
    let mut failtime = 0_i32;
    loop {
        let res = etcd
            .get_at_most_one_value_with_version::<RwLockValue, &str>(name)
            .await;
        match res {
            Ok(res) => {
                if let Some((mut lockinfo, version)) = res {
                    if lockinfo.tags_of_nodes.remove(tag_of_local_node) {
                        // These two operations must be atomic (use transaction to make sure the version)
                        //  if the transaction failed, retry will be needed.
                        if lockinfo.tags_of_nodes.is_empty() {
                            // remove the key
                            return remove_key(etcd, name).await.map(|_| true);
                        }
                        // update the value
                        if update_lock(
                            etcd,
                            name,
                            lockinfo,
                            "update lock nodes when unlock failed",
                            version,
                        )
                        .await?
                        {
                            return Ok(true);
                        }

                        failtime = failtime.overflow_add(1_i32);
                        log::debug!("update_lock txn failed, fail time: {failtime}, retry unlock");
                        continue;
                    }
                    log::debug!("try unlock, but this node doesn't hold the lock, lock key:{name}");
                    return Ok(false);
                }
                log::debug!("try unlock, but this node does'nt hold the lock, lock key:{name}");
                return Ok(false);
            }
            Err(err) => {
                log::warn!("etcd error when unlock, err:{err}");
                return Err(err);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    const ETCD_ADDRESS: &str = "localhost:2379";

    fn timestamp() -> Duration {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|err| panic!("Time went backwards, err:{err}"))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_etcd() {
        let mut server = mock_etcd::MockEtcdServer::default();
        server.start();
        let addrs = vec![ETCD_ADDRESS.to_owned()];

        let etcd = EtcdDelegate::new(addrs).await;
        match etcd {
            Ok(etcd) => {
                let testk = "test_k";
                if let Err(err) = etcd.write_or_update_kv(testk, &testk.to_owned()).await {
                    panic!("write etcd kv failed with err: {err}");
                }
                let deleted = etcd
                    .delete_exact_one_value_return_with_version::<String>(testk)
                    .await;
                match deleted {
                    Ok((res, _)) => {
                        assert_eq!(&res, testk, "delete test key failed");
                    }
                    Err(err) => {
                        panic!("delete test key failed, err:{err}");
                    }
                }
            }
            Err(err) => {
                panic!("failed to connect etcd, err:{err}");
            }
        }
    }

    #[allow(clippy::unwrap_used, clippy::too_many_lines)]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_rwlock() {
        let mut server = mock_etcd::MockEtcdServer::default();
        server.start();

        let addrs = vec![ETCD_ADDRESS.to_owned()];
        let etcd_ = EtcdDelegate::new(addrs).await;
        match etcd_ {
            Ok(etcd) => {
                {
                    // There's no conflict between different lock.
                    // So there's no blocking.
                    let begin = timestamp();
                    rw_lock(
                        &etcd,
                        "lock",
                        DistRwLockType::RLock,
                        Duration::from_secs(1),
                        "node_tag",
                    )
                    .await
                    .unwrap();
                    rw_lock(
                        &etcd,
                        "lock2",
                        DistRwLockType::RLock,
                        Duration::from_secs(1),
                        "node_tag",
                    )
                    .await
                    .unwrap();
                    let end = timestamp();
                    assert!(
                        (end - begin).as_millis() < 100,
                        "time cost {} ms, should be shorter than 100ms",
                        (end - begin).as_millis()
                    );
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
                {
                    // It's non-block for same node to get same rlock
                    let begin = timestamp();
                    rw_lock(
                        &etcd,
                        "lock",
                        DistRwLockType::RLock,
                        Duration::from_secs(1),
                        "node_tag",
                    )
                    .await
                    .unwrap();
                    rw_lock(
                        &etcd,
                        "lock",
                        DistRwLockType::RLock,
                        Duration::from_secs(1),
                        "node_tag",
                    )
                    .await
                    .unwrap();
                    let end = timestamp();
                    assert!(
                        (end - begin).as_millis() < 100,
                        "time cost {} ms, should be shorter than 100ms",
                        (end - begin).as_millis()
                    );
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
                {
                    // It's non-block for different nodes to get same rlock
                    let begin = timestamp();
                    rw_lock(
                        &etcd,
                        "lock",
                        DistRwLockType::RLock,
                        Duration::from_secs(1),
                        "node_tag",
                    )
                    .await
                    .unwrap();
                    rw_lock(
                        &etcd,
                        "lock",
                        DistRwLockType::RLock,
                        Duration::from_secs(1),
                        "node_tag2",
                    )
                    .await
                    .unwrap();
                    let end = timestamp();
                    assert!(
                        (end - begin).as_millis() < 100,
                        "time cost {} ms, should be shorter than 100ms",
                        (end - begin).as_millis()
                    );
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
                {
                    // There's conflict between different lock type
                    let begin = timestamp();
                    rw_lock(
                        &etcd,
                        "lock",
                        DistRwLockType::WLock,
                        Duration::from_secs(1),
                        "node_tag",
                    )
                    .await
                    .unwrap();
                    rw_lock(
                        &etcd,
                        "lock",
                        DistRwLockType::RLock,
                        Duration::from_secs(1),
                        "node_tag",
                    )
                    .await
                    .unwrap();
                    let end = timestamp();

                    assert!(
                        (end - begin).as_millis() > 900,
                        "time cost {} ms, should be longer than 900ms",
                        (end - begin).as_millis()
                    );
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
                {
                    // Different node get different type lock
                    // There's conflict between different lock type
                    // So there's blocking.
                    let begin = timestamp();
                    rw_lock(
                        &etcd,
                        "lock",
                        DistRwLockType::WLock,
                        Duration::from_secs(1),
                        "node_tag1",
                    )
                    .await
                    .unwrap();
                    rw_lock(
                        &etcd,
                        "lock",
                        DistRwLockType::RLock,
                        Duration::from_secs(1),
                        "node_tag",
                    )
                    .await
                    .unwrap();
                    let end = timestamp();

                    assert!(
                        (end - begin).as_millis() > 900,
                        "time cost {} ms, should be longer than 900ms",
                        (end - begin).as_millis()
                    );
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
                {
                    // Same node get write lock
                    // It's non-block for same node to get same wlock
                    let begin = timestamp();
                    rw_lock(
                        &etcd,
                        "lock",
                        DistRwLockType::WLock,
                        Duration::from_secs(1),
                        "node_tag",
                    )
                    .await
                    .unwrap();
                    rw_unlock(&etcd, "lock", "node_tag").await.unwrap();
                    rw_lock(
                        &etcd,
                        "lock",
                        DistRwLockType::WLock,
                        Duration::from_secs(1),
                        "node_tag",
                    )
                    .await
                    .unwrap();
                    let end = timestamp();
                    assert!(
                        (end - begin).as_millis() < 300,
                        "time cost {} ms, should be shorter than 300ms",
                        (end - begin).as_millis()
                    );
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
                {
                    // It's ok for the same node to unlock twice
                    let begin = timestamp();
                    rw_lock(
                        &etcd,
                        "lock",
                        DistRwLockType::WLock,
                        Duration::from_secs(1),
                        "node_tag",
                    )
                    .await
                    .unwrap();
                    rw_unlock(&etcd, "lock", "node_tag").await.unwrap();
                    rw_unlock(&etcd, "lock", "node_tag").await.unwrap();
                    let end = timestamp();
                    assert!(
                        (end - begin).as_millis() < 300,
                        "time cost {} ms, should be shorter than 300ms",
                        (end - begin).as_millis()
                    );
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
                {
                    // unlock by other node is not ok
                    rw_lock(
                        &etcd,
                        "lock",
                        DistRwLockType::WLock,
                        Duration::from_secs(1),
                        "node_tag",
                    )
                    .await
                    .unwrap();

                    assert!(
                        !rw_unlock(&etcd, "lock", "node_tag1").await.unwrap(),
                        "It's not ok for other node to unlock"
                    );
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
                {
                    // try write lock
                    assert!(
                        rw_try_lock(
                            &etcd,
                            "first_try_wlock",
                            DistRwLockType::WLock,
                            Duration::from_secs(3),
                            "node_tag",
                        )
                        .await
                        .unwrap()
                        .is_none(),
                        "first try write lock should be ok"
                    );

                    assert!(
                        rw_try_lock(
                            &etcd,
                            "first_try_wlock",
                            DistRwLockType::WLock,
                            Duration::from_secs(3),
                            "node_tag1",
                        )
                        .await
                        .unwrap()
                        .is_some(),
                        "second try write lock should be not ok"
                    );

                    assert!(
                        rw_try_lock(
                            &etcd,
                            "first_try_wlock",
                            DistRwLockType::RLock,
                            Duration::from_secs(3),
                            "node_tag1",
                        )
                        .await
                        .unwrap()
                        .is_some(),
                        "second try read lock should be not ok"
                    );

                    // first try read lock
                    assert!(
                        rw_try_lock(
                            &etcd,
                            "fisrt_try_read_lock",
                            DistRwLockType::RLock,
                            Duration::from_secs(3),
                            "node_tag",
                        )
                        .await
                        .unwrap()
                        .is_none(),
                        "first try read lock should be ok"
                    );

                    assert!(
                        rw_try_lock(
                            &etcd,
                            "fisrt_try_read_lock",
                            DistRwLockType::RLock,
                            Duration::from_secs(3),
                            "node_tag1",
                        )
                        .await
                        .unwrap()
                        .is_none(),
                        "second try read lock should be ok"
                    );

                    assert!(
                        rw_try_lock(
                            &etcd,
                            "fisrt_try_read_lock",
                            DistRwLockType::WLock,
                            Duration::from_secs(3),
                            "node_tag1",
                        )
                        .await
                        .unwrap()
                        .is_some(),
                        "second try write lock should be not ok"
                    );
                }
                // todo: add test about etcd crash
            }
            Err(err) => {
                panic!("failed to connect etcd, err:{err}");
            }
        }
    }
}
