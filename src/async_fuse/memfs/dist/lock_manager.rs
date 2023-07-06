//! Wrap the basic distribute lock
//!  Offers lock function that auto renew the lease of lock
//!  Note that the lock is reentrant, which means that the same distribute node can lock the same lock multiple times

use crate::async_fuse::fuse::file_system::FsAsyncResultSender;
use crate::async_fuse::memfs::kv_engine::{
    KVEngine, KVEngineLeaseKeeper, KVEngineLeaseKeeperType, KVEngineType, LockKeyType, UnlockToken,
};
use crate::common::error::Context;
use crate::common::error::DatenLordResult;
use futures::{stream::FuturesUnordered, StreamExt};
use parking_lot::RwLock;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::task::JoinHandle;
use tokio::{select, sync};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_RENEW_TIMEOUT: Duration = Duration::from_secs(12);
const DEFAULT_TRYLOCK_TIMEOUT: Duration = Duration::from_secs(1);

/// The manager to lock,unlock distribute rwlock and renew the lease of using lock
#[allow(missing_debug_implementations)]
#[derive(Debug)]
pub struct DistLockManager {
    locked: RwLock<DistLockManagerLocked>,
    kv_engine: Arc<KVEngineType>,
    register_renew_tx: sync::mpsc::Sender<LockDescription>,
}

#[derive(Debug)]
struct DistLockManagerLocked {
    /// lockname - (cancel sender, unlock token)
    timeout_renews: HashMap<Vec<u8>, (sync::oneshot::Sender<()>, UnlockToken)>,
    /// System end signal
    system_end_tx: Option<sync::oneshot::Sender<()>>,
}

struct LockDescription {
    lease_keeper: KVEngineLeaseKeeperType,
    renew_timeout: Duration,
    cancel_rx: sync::oneshot::Receiver<()>,
}

impl DistLockManager {
    /// - return true if renew lease success or will return false
    #[inline]
    async fn do_renew_lock_lease(
        lock_disc: &mut LockDescription,
        fs_async_sender: &FsAsyncResultSender,
    ) -> bool {
        if let Err(err) = lock_disc.lease_keeper.lease_keep_alive().await {
            fs_async_sender
                .send(Err(err).with_context(|| {
                    format!("lock manager: renew lock lease failed at `do_renew_lock_lease`")
                }))
                .await
                .unwrap_or_else(|e|{
                    panic!(
                        "lock manager: failed to send error to fs main loop at `do_renew_lock_lease`,err:{e}"
                    );
                });
            return false;
        }
        true
    }

    // select code is auto generated
    #[allow(clippy::integer_arithmetic)]
    async fn renew_lock_future(
        mut lock_disc: LockDescription,
        fs_async_sender: &FsAsyncResultSender,
    ) -> Option<LockDescription> {
        select! {
            _ = tokio::time::sleep(lock_disc.renew_timeout) => {
                // timeout, renew lock
                if Self::do_renew_lock_lease(&mut lock_disc, fs_async_sender).await {
                    // renew success, continue renew
                    Some(lock_disc)
                } else {
                    // renew failed, don't continue
                    log::warn!("failed to renew lock lease, won't continue renewing");
                    None
                }
            },
            _ = &mut lock_disc.cancel_rx => {
                // cancel lock, don't continue
                None
            }
        }
    }
    fn spawn_renew_lock_task(
        mut new_lock_rx: sync::mpsc::Receiver<LockDescription>,
        mut system_end_rx: sync::oneshot::Receiver<()>,
        fs_async_sender: FsAsyncResultSender,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut futures = FuturesUnordered::new();

            loop {
                select! {
                    // 1. new lock renew job
                    new_lock= new_lock_rx.recv() => {
                        if let Some(new_lock)=new_lock{
                            futures.push(Self::renew_lock_future(new_lock, &fs_async_sender));
                        }
                    },
                    // 2. poll one renew job
                    continue_renew_lock= futures.next() => {
                        if let Some(Some(continue_renew_lock))=continue_renew_lock{
                            futures.push(Self::renew_lock_future(continue_renew_lock, &fs_async_sender));
                        }
                    },
                    // 3. system end
                    _ = &mut system_end_rx => {
                        break;
                    }
                }
            }
        })
    }

    /// new `DistRwLockManager`
    pub fn new(
        kv_engine: Arc<KVEngineType>,
        fs_async_sender: FsAsyncResultSender,
    ) -> (Arc<DistLockManager>, JoinHandle<()>) {
        //init a main loop to renew the lock lease
        let (register_renew_tx, register_renew_rx) =
            tokio::sync::mpsc::channel::<LockDescription>(10);
        let (system_end_tx, system_end_rx) = tokio::sync::oneshot::channel::<()>();
        let renew_task =
            Self::spawn_renew_lock_task(register_renew_rx, system_end_rx, fs_async_sender);
        let man: Arc<DistLockManager> = Arc::new(DistLockManager {
            locked: RwLock::new(DistLockManagerLocked {
                timeout_renews: HashMap::new(),
                system_end_tx: Some(system_end_tx),
            }),
            kv_engine,
            register_renew_tx,
        });

        (man, renew_task)
    }

    /// try lock dist lock
    /// - return true if lock success
    #[inline]
    pub async fn try_lock(&self, key: &LockKeyType) -> DatenLordResult<bool> {
        self.inner_lock(key, true).await
    }

    #[inline]
    /// lock dist lock
    pub async fn lock(&self, key: &LockKeyType) -> DatenLordResult<()> {
        self.inner_lock(key, false).await.map(|_| ())
    }
    /// jnner lock function for lock and try lock
    /// todo: fix bug, when wating for the lock, lease should be updated.
    async fn inner_lock(&self, key: &LockKeyType, try_lock: bool) -> DatenLordResult<bool> {
        log::debug!("`LockManager::lock`, key: {key:?}");
        let key_vec: Vec<u8> = key.get_key();

        // already locked, only need the system to renew the lease
        if self.locked.read().timeout_renews.contains_key(&key_vec) {
            return Ok(true);
        }
        let lease_info = self.kv_engine.lease_grant(DEFAULT_TIMEOUT).await?;
        // lock
        let lockinfo = if try_lock {
            match tokio::time::timeout(
                DEFAULT_TRYLOCK_TIMEOUT,
                self.kv_engine.lock(&key, &lease_info),
            )
            .await
            {
                Ok(res) => match res {
                    Ok(lockinfo) => lockinfo,
                    Err(e) => {
                        return Err(e).with_context(|| {
                            format!("lock manager lock operation: kv_engine lock failed")
                        })
                    }
                },
                Err(_) => {
                    self.kv_engine.lease_revoke(lease_info.lease_id).await?;
                    return Ok(false);
                }
            }
        } else {
            self.kv_engine
                .lock(&key, &lease_info)
                .await
                .with_context(|| format!("lock manager lock operation: kv_engine lock failed"))?
        };

        let lease_keeper: KVEngineLeaseKeeperType = self
            .kv_engine
            .alloc_lease_keeper(lease_info.lease_id)
            .await
            .with_context(|| {
                format!("lock manager lock operation: kv_engine alloc_lease_keeper failed")
            })?;

        // cancel signal
        let (cancel_tx, cancel_rx) = sync::oneshot::channel::<()>();

        self.locked
            .write()
            .timeout_renews
            .insert(key_vec, (cancel_tx, lockinfo.unlock_token));
        self.register_renew_tx
            .send(LockDescription {
                lease_keeper,
                renew_timeout: DEFAULT_RENEW_TIMEOUT,
                cancel_rx,
            })
            .await
            .unwrap_or_else(|e| {
                panic!("lock manager: register renew rx is dropped, which is impossible, err:{e}")
            });
        Ok(true)
    }
    /// Unlock dist lock
    /// - if locked before return true
    /// - if not locked before return false
    pub async fn unlock(&self, key: &LockKeyType) -> DatenLordResult<bool> {
        log::debug!("`LockManager::unlock`, key: {key:?}");
        let key: Vec<u8> = key.get_key();
        let take_lock = self.locked.write().timeout_renews.remove_entry(&key);
        if let Some((key, (cancel_sender, unlock_token))) = take_lock {
            cancel_sender
                .send(())
                .unwrap_or_else(|_| panic!("lock manager cancel receiver dropped, which should be there until this function is called"));
            self.kv_engine
                .unlock(unlock_token)
                .await
                .with_context(|| format!("lock manager: etcd unlock failed, lock key: {key:?}"))?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
    /// Stop the async task
    pub async fn system_end(&self) {
        let tx =
            self.locked.write().system_end_tx.take().unwrap_or_else(|| {
                panic!("lock manager: system end tx is taken, which is impossible")
            });
        tx.send(()).unwrap_or_else(|_| {
            panic!("lock manager: system end rx is dropped, which is impossible")
        });
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod test {
    use std::sync::Arc;

    use clippy_utilities::OverflowArithmetic;

    use super::*;
    use crate::{
        async_fuse::{
            fuse::file_system::new_fs_async_result_chan,
            memfs::kv_engine::{EtcdKVEngine, LockKeyType},
        },
        common,
    };

    const ETCD_ADDRESS: &str = "localhost:2379";

    #[tokio::test(flavor = "multi_thread")]
    async fn test_lockmanager() {
        common::util::setup_test_log_debug();
        for i in 0_i32..6_i32 {
            log::debug!("test_lockmanager_once for the {} time", i.overflow_add(1));
            test_lockmanager_once().await;
        }
    }

    // #[tokio::test(flavor = "multi_thread")]
    #[allow(clippy::unwrap_used, clippy::too_many_lines)]
    async fn test_lockmanager_once() {
        common::util::setup_test_log_debug();

        let mut server = mock_etcd::MockEtcdServer::default();
        server.start();

        let addrs = vec![ETCD_ADDRESS.to_owned()];
        let kv_engine = Arc::new(EtcdKVEngine::new_for_local_test(addrs).await.unwrap());

        let (tx, _rx) = new_fs_async_result_chan();
        let (lock_manager, _sub_task) = DistLockManager::new(Arc::clone(&kv_engine), tx.clone());

        let lock_key1 = "test_lockmanager1";

        lock_manager
            .unlock(&LockKeyType::TestOnly(lock_key1.to_owned()))
            .await
            .unwrap_or_else(|err| panic!("unlock failed, err:{err}"));

        lock_manager
            .lock(&LockKeyType::TestOnly(lock_key1.to_owned()))
            .await
            .unwrap_or_else(|err| panic!("lock failed, err:{err}"));

        {
            // One lock manager can lock the same key multiple times
            lock_manager
                .lock(&LockKeyType::TestOnly(lock_key1.to_owned()))
                .await
                .unwrap_or_else(|err| panic!("One lock manager should be able to lock the same key multiple times, err:{err}", ));

            {
                let kv_engine = Arc::clone(&kv_engine);
                let tx = tx.clone();
                let (lock_manager2, _sub_task) = DistLockManager::new(kv_engine, tx);

                let trylock_res = lock_manager2
                    .try_lock(&LockKeyType::TestOnly(lock_key1.to_owned()))
                    .await
                    .unwrap_or_else(|err| panic!("unexpected error when try lock: {err}",));
                assert_eq!(trylock_res,false,"This lock should be hold by another lock manager, so try lock should return false");

                // block on lock until timeout
                let timeout = tokio::time::timeout(
                    DEFAULT_TIMEOUT + Duration::from_secs(2), // A little longer for lease timeout to expose the bug if lock is not renewed
                    lock_manager2.lock(&LockKeyType::TestOnly(lock_key1.to_owned())),
                )
                .await;
                assert!(timeout.is_err(),"It should block on lock until timeout, because it has been locked by another lock manager, and always will be renewed before lease time");
            }

            assert!(
                lock_manager
                    .unlock(&LockKeyType::TestOnly(lock_key1.to_owned()))
                    .await
                    .unwrap(),
                "Locked before, it should return true when unlock",
            );
            // mock is a little slow, so wait a little longer, but the time is smaller than lease time
            tokio::time::sleep(Duration::from_secs(1)).await;
            log::debug!("{lock_key1} unlocked");
            {
                let kv_engine = Arc::clone(&kv_engine);
                let tx = tx.clone();

                let (lock_manager2, _sub_task) = DistLockManager::new(kv_engine, tx);

                // test trylock
                log::debug!("test trylock after unlock");
                let trylock_res = lock_manager2
                    .try_lock(&LockKeyType::TestOnly(lock_key1.to_owned()))
                    .await
                    .unwrap_or_else(|err| panic!("unexpected error when try lock: {err}",));
                assert!(
                    trylock_res,
                    "Another lock manager just freed the lock, so try lock should return true"
                );

                // unlock the lock from trylock
                assert!(
                    lock_manager2
                        .unlock(&LockKeyType::TestOnly(lock_key1.to_owned()))
                        .await
                        .unwrap(),
                    "Locked before, it should return true when unlock",
                );

                // block on lock until timeout
                let timeout = tokio::time::timeout(
                    Duration::from_millis(2000), // lock should be successful in time because it is unlocked before
                    lock_manager.lock(&LockKeyType::TestOnly(lock_key1.to_owned())),
                )
                .await;
                assert!(
                    timeout.is_ok(),
                    "The lock {lock_key1} should lock in time, because it has been unlocked by another lock manager"
                );
            }
        }
    }
}
