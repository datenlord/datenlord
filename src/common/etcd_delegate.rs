//! The etcd client implementation

use super::error::{Context, DatenLordError, DatenLordResult};
use super::util;
use clippy_utilities::OverflowArithmetic;
use core::fmt;
use core::fmt::Debug;
use core::time::Duration;
use etcd_client::{EtcdLeaseGrantRequest, EventType, KeyRange, TxnCmp, TxnOpResponse};
use log::debug;
use serde::de::DeserializeOwned;
use serde::Serialize;

/// version of kv for transaction verify
pub type KvVersion = usize;

/// The client to communicate with etcd
#[allow(missing_debug_implementations)] // etcd_client::Client doesn't impl Debug
#[derive(Clone)]
pub struct EtcdDelegate {
    /// The inner etcd client
    etcd_rs_client: etcd_client::Client,
    /// Etcd end point address
    end_point: Vec<String>,
}

impl Debug for EtcdDelegate {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EtcdDelegate")
            .field("endpoint", &self.end_point)
            .finish()
    }
}

impl EtcdDelegate {
    /// Build etcd client
    #[inline]
    pub async fn new(etcd_address_vec: Vec<String>) -> DatenLordResult<Self> {
        let end_point = etcd_address_vec.clone();
        let etcd_rs_client = etcd_client::Client::connect(etcd_client::ClientConfig::new(
            etcd_address_vec.clone(),
            None,
            // TODO: cache size should come from parameter
            64,
            true,
        ))
        .await
        .with_context(|| {
            format!("failed to build etcd client to addresses={etcd_address_vec:?}")
        })?;
        Ok(Self {
            etcd_rs_client,
            end_point,
        })
    }

    /// Lock a name with time out
    #[inline]
    pub async fn lock(&self, name: &[u8], timeout: u64) -> DatenLordResult<Vec<u8>> {
        let lease_id = self
            .etcd_rs_client
            .lease()
            .grant(etcd_client::EtcdLeaseGrantRequest::new(
                Duration::from_secs(timeout),
            ))
            .await
            .with_context(|| {
                format!("failed to get LeaseGrantResponse from etcd, the timeout={timeout}")
            })?
            .id();

        let key = self
            .etcd_rs_client
            .lock()
            .lock(etcd_client::EtcdLockRequest::new(name, lease_id))
            .await
            .with_context(|| {
                format!("failed to get LockResponse from etcd, the lease id={lease_id}")
            })?
            .take_key();

        Ok(key)
    }

    /// Unlock with the key, which comes from the lock operation
    #[inline]
    pub async fn unlock<T: Into<Vec<u8>> + Clone + Send>(&self, key: T) -> DatenLordResult<()> {
        let key_clone = key.clone();
        self.etcd_rs_client
            .lock()
            .unlock(etcd_client::EtcdUnlockRequest::new(key_clone))
            .await
            .with_context(|| {
                format!(
                    "failed to get UnlockResponse from etcd, the lease key={:?}",
                    key.into()
                )
            })?;

        Ok(())
    }

    /// Get one key-value pair from etcd
    async fn get_one_kv_async<T: DeserializeOwned, K: Into<Vec<u8>> + Debug + Clone + Send>(
        &self,
        key: K,
    ) -> DatenLordResult<Option<(T, KvVersion)>> {
        let key_clone = key.clone();
        let req = etcd_client::EtcdRangeRequest::new(etcd_client::KeyRange::key(key));
        let mut resp = self.etcd_rs_client.kv().range(req).await.with_context(|| {
            format!(
                "failed to get RangeResponse of one key-value pair from etcd, the key={key_clone:?}"
            )
        })?;

        let kvs = resp.take_kvs();

        match kvs.get(0) {
            Some(kv) => Ok(Some((
                util::decode_from_bytes::<T>(kv.value())?,
                kv.version(),
            ))),
            None => Ok(None),
        }
    }

    /// offer an version check
    /// - `expire`: auto delete after expire
    async fn write_to_etcd_with_version<
        T: DeserializeOwned + Serialize + Clone + Debug + Send + Sync,
        K: Into<Vec<u8>> + Debug + Clone + Send,
    >(
        &self,
        key: K,
        value: &T,
        prev_version: KvVersion,
        expire: Option<Duration>,
    ) -> DatenLordResult<Option<(T, KvVersion)>> {
        let bin_value = bincode::serialize(value)
            .with_context(|| format!("failed to encode {value:?} to binary"))?;
        let mut put_request = etcd_client::EtcdPutRequest::new(key.clone(), bin_value);
        if let Some(dur) = expire {
            put_request.set_lease(
                self.etcd_rs_client
                    .lease()
                    .grant(EtcdLeaseGrantRequest::new(dur))
                    .await
                    .with_context(|| {
                        format!(
                            "failed to get LeaseGrantResponse from etcd, the timeout={}",
                            dur.as_secs()
                        )
                    })?
                    .id(),
            );
        };
        let txn_req = etcd_client::EtcdTxnRequest::new()
            // etcd：chack key exist in txn
            //  https://etcd.io/docs/v3.4/learning/api/
            //  https://github.com/etcd-io/etcd/issues/7115
            //  https://github.com/etcd-io/etcd/issues/6740
            //key does not exist when create revision is 0, check the links above
            .when_version(
                etcd_client::KeyRange::key(key.clone()),
                TxnCmp::Equal,
                prev_version,
            )
            //key does not exist, insert kv
            .and_then(put_request)
            //key exists, return old value
            .or_else(etcd_client::EtcdRangeRequest::new(
                etcd_client::KeyRange::key(key.clone()),
            ));
        let txn_res = self
            .etcd_rs_client
            .kv()
            .txn(txn_req)
            .await
            .with_context(|| {
                format!("failed to get PutResponse from etcd for key={key:?}, value={value:?}",)
            })?;

        if txn_res.is_success() {
            //key does not exist, insert kv
            Ok(None)
        } else {
            let mut resp_ = txn_res.get_responses();
            assert_eq!(resp_.len(), 1, "txn response length should be 1");
            match resp_.pop().unwrap_or_else(|| {
                panic!("txn response length should be 1 and pop should not fail")
            }) {
                TxnOpResponse::Range(mut resp) => {
                    let kvs = resp.take_kvs();
                    let kv_first = kvs.get(0).unwrap_or_else(|| panic!("kv res must be sz>0"));
                    //key exists
                    let decoded_value: T = util::decode_from_bytes(kv_first.value())?;
                    Ok(Some((decoded_value, kv_first.version())))
                }
                TxnOpResponse::Put(_) | TxnOpResponse::Delete(_) | TxnOpResponse::Txn(_) => {
                    panic!("txn response should be RangeResponse");
                }
                _ => {
                    panic!("new op unconsidered");
                }
            }
        }
    }

    /// Write a key value pair to etcd
    /// - `expire`: auto delete after expire
    async fn write_to_etcd<
        T: DeserializeOwned + Serialize + Clone + Debug + Send + Sync,
        K: Into<Vec<u8>> + Debug + Clone + Send,
    >(
        &self,
        key: K,
        value: &T,
        expire: Option<Duration>,
    ) -> DatenLordResult<Option<(T, KvVersion)>> {
        let key_clone = key.clone();
        let bin_value = bincode::serialize(value)
            .with_context(|| format!("failed to encode {value:?} to binary"))?;
        let mut req = etcd_client::EtcdPutRequest::new(key, bin_value);
        req.set_prev_kv(true); // Return previous value
        if let Some(expire) = expire {
            req.set_lease(
                self.etcd_rs_client
                    .lease()
                    .grant(EtcdLeaseGrantRequest::new(expire))
                    .await
                    .with_context(|| {
                        format!(
                            "failed to get LeaseGrantResponse from etcd, the timeout={} s",
                            expire.as_secs()
                        )
                    })?
                    .id(),
            );
        }

        let mut resp = self.etcd_rs_client.kv().put(req).await.with_context(|| {
            format!("failed to get PutResponse from etcd for key={key_clone:?}, value={value:?}")
        })?;
        if let Some(pre_kv) = resp.take_prev_kv() {
            let decoded_value: T = util::decode_from_bytes(pre_kv.value())?;
            Ok(Some((decoded_value, pre_kv.version())))
        } else {
            Ok(None)
        }
    }

    /// - if key exist, don't insert, return the old value
    /// - if key not exist, insert, return None
    /// - `expire`: auto delete after expire
    async fn write_to_etcd_if_none<
        T: DeserializeOwned + Serialize + Clone + Debug + Send + Sync,
        K: Into<Vec<u8>> + Debug + Clone + Send,
    >(
        &self,
        key: K,
        value: &T,
        expire: Option<Duration>,
    ) -> DatenLordResult<Option<(T, KvVersion)>> {
        let bin_value = bincode::serialize(value)
            .with_context(|| format!("failed to encode {value:?} to binary"))?;
        let mut put_request = etcd_client::EtcdPutRequest::new(key.clone(), bin_value);
        if let Some(dur) = expire {
            put_request.set_lease(
                self.etcd_rs_client
                    .lease()
                    .grant(EtcdLeaseGrantRequest::new(dur))
                    .await
                    .with_context(|| {
                        format!(
                            "failed to get LeaseGrantResponse from etcd, the timeout={}",
                            dur.as_secs()
                        )
                    })?
                    .id(),
            );
        };
        let txn_req = etcd_client::EtcdTxnRequest::new()
            // etcd：chack key exist in txn
            //  https://github.com/etcd-io/etcd/issues/7115
            //  https://github.com/etcd-io/etcd/issues/6740
            //key does not exist when create revision is 0, check the links above
            .when_version(etcd_client::KeyRange::key(key.clone()), TxnCmp::Equal, 0)
            //key does not exist, insert kv
            .and_then(put_request)
            //key exists, return old value
            .or_else(etcd_client::EtcdRangeRequest::new(
                etcd_client::KeyRange::key(key.clone()),
            ));
        let txn_res = self
            .etcd_rs_client
            .kv()
            .txn(txn_req)
            .await
            .with_context(|| {
                format!("failed to get PutResponse from etcd for key={key:?}, value={value:?}",)
            })?;

        if txn_res.is_success() {
            //key does not exist, insert kv
            Ok(None)
        } else {
            let mut resp_ = txn_res.get_responses();
            assert_eq!(resp_.len(), 1, "txn response length should be 1");
            match resp_.pop().unwrap_or_else(|| {
                panic!("txn response length should be 1 and pop should not fail")
            }) {
                TxnOpResponse::Range(mut resp) => {
                    let kvs = resp.take_kvs();
                    let kv_first = kvs.get(0).unwrap_or_else(|| panic!("kv res must be sz>0"));
                    //key exists
                    let decoded_value: T = util::decode_from_bytes(kv_first.value())?;
                    Ok(Some((decoded_value, kv_first.version())))
                }
                TxnOpResponse::Put(_) | TxnOpResponse::Delete(_) | TxnOpResponse::Txn(_) => {
                    panic!("txn response should be RangeResponse");
                }
                _ => {
                    panic!("new op unconsidered");
                }
            }
        }
    }

    /// Delete a key value pair or nothing from etcd
    async fn delete_from_etcd<T: DeserializeOwned + Clone + Debug + Send + Sync>(
        &self,
        key: &str,
    ) -> DatenLordResult<Vec<(T, KvVersion)>> {
        let mut req = etcd_client::EtcdDeleteRequest::new(etcd_client::KeyRange::key(key));
        req.set_prev_kv(true);
        let mut resp = self
            .etcd_rs_client
            .kv()
            .delete(req)
            .await
            .with_context(|| format!("failed to get DeleteResponse from etcd for key={key:?}"))?;

        if resp.has_prev_kvs() {
            let deleted_value_list = resp.take_prev_kvs();

            let mut result_vec = Vec::with_capacity(deleted_value_list.len());
            for kv in deleted_value_list {
                let decoded_value: T = util::decode_from_bytes(kv.value())?;
                result_vec.push((decoded_value, kv.version()));
            }
            Ok(result_vec)
        } else {
            Ok(Vec::new())
        }
    }

    /// Get key-value list from etcd
    #[inline]
    pub async fn get_list<T: DeserializeOwned>(&self, prefix: &str) -> DatenLordResult<Vec<T>> {
        let req = etcd_client::EtcdRangeRequest::new(etcd_client::KeyRange::prefix(prefix));
        let mut resp =
            self.etcd_rs_client.kv().range(req).await.with_context(|| {
                format!("failed to get RangeResponse from etcd for key={prefix:?}")
            })?;
        let mut result_vec = Vec::with_capacity(resp.count());
        for kv in resp.take_kvs() {
            let decoded_value: T = util::decode_from_bytes(kv.value())?;
            result_vec.push(decoded_value);
        }
        Ok(result_vec)
    }

    /// Get zero or one key-value pair from etcd with version
    #[inline]
    pub async fn get_at_most_one_value_with_version<
        T: DeserializeOwned,
        K: Into<Vec<u8>> + Debug + Clone + Send,
    >(
        &self,
        key: K,
    ) -> DatenLordResult<Option<(T, KvVersion)>> {
        let value = self.get_one_kv_async(key).await?;
        Ok(value)
    }

    /// Get zero or one key-value pair from etcd without version
    #[inline]
    pub async fn get_at_most_one_value<
        T: DeserializeOwned,
        K: Into<Vec<u8>> + Debug + Clone + Send,
    >(
        &self,
        key: K,
    ) -> DatenLordResult<Option<T>> {
        let value = self.get_one_kv_async(key).await?;
        Ok(value.map(|(v, _)| v))
    }

    /// Update a existing key value pair to etcd
    /// # Panics
    ///
    /// Will panic if failed to get previous value
    #[inline]
    pub async fn update_existing_kv_and_return_with_version<
        T: DeserializeOwned + Serialize + Clone + Debug + Send + Sync,
    >(
        &self,
        key: &str,
        value: &T,
    ) -> DatenLordResult<(T, KvVersion)> {
        let write_res = self.write_to_etcd(key, value, None).await?;
        if let Some(pre_value) = write_res {
            Ok(pre_value)
        } else {
            panic!("failed to replace previous value, return nothing");
        }
    }

    /// Update a existing key value pair to etcd
    /// # Panics
    ///
    /// Will panic if failed to get previous value
    #[inline]
    pub async fn update_existing_kv<
        T: DeserializeOwned + Serialize + Clone + Debug + Send + Sync,
    >(
        &self,
        key: &str,
        value: &T,
    ) -> DatenLordResult<T> {
        self.update_existing_kv_and_return_with_version(key, value)
            .await
            .map(|(v, _)| v)
    }

    /// Write a new key value pair to etcd
    /// # Panics
    ///
    /// Will panic if key has already existed in etcd
    #[inline]
    pub async fn write_new_kv<T: DeserializeOwned + Serialize + Clone + Debug + Send + Sync>(
        &self,
        key: &str,
        value: &T,
    ) -> DatenLordResult<()> {
        let write_res = self.write_to_etcd(key, value, None).await?;
        if let Some(pre_value) = write_res {
            panic!(
                "failed to write new key vaule pair, the key={key} exists in etcd, \
                    the previous value={pre_value:?}"
            );
        } else {
            Ok(())
        }
    }

    /// return exist value if key exists
    /// return none if key does not exist
    /// - `expire`: auto delete after expire
    #[inline]
    pub async fn write_new_kv_no_panic<
        T: DeserializeOwned + Serialize + Clone + Debug + Send + Sync,
    >(
        &self,
        key: &str,
        value: &T,
        expire: Option<Duration>,
    ) -> DatenLordResult<Option<T>> {
        self.write_to_etcd_if_none(key, value, expire)
            .await
            .map(|v| v.map(|(v, _)| v))
    }

    /// return exist value if key exists
    /// return none if key does not exist
    /// - `expire`: auto delete after expire
    #[inline]
    pub async fn write_new_kv_no_panic_return_with_version<
        T: DeserializeOwned + Serialize + Clone + Debug + Send + Sync,
    >(
        &self,
        key: &str,
        value: &T,
        expire: Option<Duration>,
    ) -> DatenLordResult<Option<(T, KvVersion)>> {
        self.write_to_etcd_if_none(key, value, expire).await
    }

    /// Write key value pair to etcd, if key exists, update it
    #[inline]
    pub async fn write_or_update_kv<
        T: DeserializeOwned + Serialize + Clone + Debug + Send + Sync,
        K: Into<Vec<u8>> + Debug + Clone + Send,
    >(
        &self,
        key: K,
        value: &T,
    ) -> DatenLordResult<()> {
        let key_clone = key.clone();
        let write_res = self.write_to_etcd(key, value, None).await?;
        if let Some(pre_value) = write_res {
            debug!(
                "key={:?} exists in etcd, the previous value={:?}, update it",
                key_clone, pre_value
            );
        }
        Ok(())
    }

    /// Write key value pair with timeout lease to etcd, if key exists, update it
    /// - `expire`: auto delete after expire
    #[inline]
    pub async fn write_or_update_kv_with_timeout<
        T: DeserializeOwned + Serialize + Clone + Debug + Send + Sync,
        K: Into<Vec<u8>> + Debug + Clone + Send,
    >(
        &self,
        key: K,
        value: &T,
        expire: Duration,
    ) -> DatenLordResult<()> {
        let key_clone = key.clone();
        let write_res = self.write_to_etcd(key, value, Some(expire)).await?;
        if let Some(pre_value) = write_res {
            debug!(
                "key={:?} exists in etcd, the previous value={:?}, update it",
                key_clone, pre_value
            );
        }
        Ok(())
    }

    /// Write or update key only when matching the previous version.
    /// - `expire`: auto delete after expire
    #[inline]
    pub async fn write_or_update_kv_with_version<
        T: DeserializeOwned + Serialize + Clone + Debug + Send + Sync,
        K: Into<Vec<u8>> + Debug + Clone + Send,
    >(
        &self,
        key: K,
        value: &T,
        version: KvVersion,
        expire: Option<Duration>,
    ) -> DatenLordResult<Option<(T, KvVersion)>> {
        self.write_to_etcd_with_version(key, value, version, expire)
            .await
    }

    /// Delete an existing key value pair from etcd
    /// # Panics
    ///
    /// Will panic if it doesn't delete one value from etcd
    #[inline]
    pub async fn delete_exact_one_value_return_with_version<
        T: DeserializeOwned + Clone + Debug + Send + Sync,
    >(
        &self,
        key: &str,
    ) -> DatenLordResult<(T, KvVersion)> {
        let mut deleted_value_vec = self.delete_from_etcd(key).await?;
        debug_assert_eq!(
            deleted_value_vec.len(),
            1,
            "delete {} key value pairs for a single key={}, should delete exactly one",
            deleted_value_vec.len(),
            key,
        );
        let Some(deleted_kv) = deleted_value_vec.pop() else {
            panic!("failed to get the exactly one deleted key value pair")
        };
        Ok(deleted_kv)
    }

    /// Delete an existing key value pair from etcd
    /// # Panics
    ///
    /// Will panic if it doesn't delete one value from etcd
    #[inline]
    pub async fn delete_exact_one_value<T: DeserializeOwned + Clone + Debug + Send + Sync>(
        &self,
        key: &str,
    ) -> DatenLordResult<T> {
        self.delete_exact_one_value_return_with_version(key)
            .await
            .map(|(v, _)| v)
    }

    /// Delete an key value pair from etcd
    /// # Panics
    ///
    /// Will panic if it deletes more than one value from etcd
    #[inline]
    pub async fn delete_one_value_return_with_version<
        T: DeserializeOwned + Clone + Debug + Send + Sync,
    >(
        &self,
        key: &str,
    ) -> DatenLordResult<Option<(T, KvVersion)>> {
        let mut deleted_value_vec = self.delete_from_etcd(key).await?;
        debug_assert!(
            deleted_value_vec.len() <= 1,
            "delete {} key value pairs for a single key={}, shouldn't delete more than one",
            deleted_value_vec.len(),
            key,
        );
        Ok(deleted_value_vec.pop())
    }

    /// Delete an key value pair from etcd
    /// # Panics
    ///
    /// Will panic if it deletes more than one value from etcd
    #[inline]
    pub async fn delete_one_value<T: DeserializeOwned + Clone + Debug + Send + Sync>(
        &self,
        key: &str,
    ) -> DatenLordResult<Option<T>> {
        self.delete_one_value_return_with_version(key)
            .await
            .map(|opt| opt.map(|(v, _)| v))
    }

    /// Delete all key value pairs from etcd
    #[allow(dead_code)]
    #[inline]
    pub async fn delete_all(&self) -> DatenLordResult<()> {
        let mut req = etcd_client::EtcdDeleteRequest::new(etcd_client::KeyRange::all());
        req.set_prev_kv(false);
        self.etcd_rs_client
            .kv()
            .delete(req)
            .await
            .add_context("failed to delete all data from etcd")?;
        Ok(())
    }

    /// Get the inner etcd client's clone
    #[must_use]
    #[inline]
    pub fn get_inner_client_clone(&self) -> etcd_client::Client {
        self.etcd_rs_client.clone()
    }

    /// Wait until a key is deleted.
    /// This function will return when watch closed or there's etcd error.
    /// Timeout
    #[inline]
    #[allow(clippy::integer_arithmetic, clippy::arithmetic_side_effects)] // for the auto generate code from tokio select!
    pub async fn wait_key_delete(&self, name: &str) -> DatenLordResult<()> {
        let receiver_opt = self.etcd_rs_client.watch(KeyRange::key(name)).await;
        let mut receiver = match receiver_opt {
            Ok(receiver) => receiver,
            Err(e) => {
                return Err(DatenLordError::EtcdClientErr {
                    source: e,
                    context: vec!["wait_key_delete receive watch failed".to_owned()],
                });
            }
        };

        let (stop_wait_log_task_chan_tx, mut stop_wait_log_task_chan_rx) =
            tokio::sync::oneshot::channel::<()>();
        let name_clone = name.to_owned();
        let wait_log_task = tokio::spawn(async move {
            let mut sec_cnt: i32 = 0;
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        sec_cnt=sec_cnt.overflow_add(1_i32);
                        log::debug!("wait for dist lock {name_clone} for {sec_cnt} seconds, if this goes too long, there might be dead lock");
                    }
                    _ = &mut stop_wait_log_task_chan_rx => {
                        break;
                    }
                }
            }
        });

        // When we call this function,
        //  there might be no key in it or someone just insert one into it.
        // We might first receive a insert event.
        // We need a loop to read until there's delete event.
        loop {
            match receiver.recv().await {
                Ok(mut ok) => {
                    let events = ok.take_events();
                    for e in &events {
                        if let EventType::Delete = e.event_type() {
                            stop_wait_log_task_chan_tx.send(()).unwrap_or_else(|_| {
                                panic!("send failed, stop_wait_log_task_chan_rx was dropped?")
                            });
                            wait_log_task.await.unwrap_or_else(|_| {
                                panic!("join wait log task failed");
                            });

                            return Ok(());
                        }
                    }
                }
                Err(err) => {
                    let mut context = vec![];
                    let errinfo = "etcd watch failed when wait for a key to be deleted";
                    debug!("{}", errinfo.to_owned());
                    context.push(errinfo.to_owned());

                    stop_wait_log_task_chan_tx.send(()).unwrap_or_else(|_| {
                        panic!("send failed, stop_wait_log_task_chan_rx was dropped?")
                    });
                    wait_log_task.await.unwrap_or_else(|_| {
                        panic!("join wait log task failed");
                    });
                    return Err(DatenLordError::EtcdClientErr {
                        source: err,
                        context,
                    });
                }
            }
        }
    }
}
