//! The etcd client implementation

use super::error::{Context, DatenLordError, DatenLordResult};
use super::util;
use clippy_utilities::OverflowArithmetic;
use core::fmt;
use core::fmt::Debug;
use core::time::Duration;
use etcd_client::TxnOpResponse::Put;
use etcd_client::{
    Compare, CompareOp, DeleteOptions, EventType, GetOptions, LockOptions, PutOptions, Txn, TxnOp,
    TxnOpResponse,
};
use log::debug;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::vec;

/// version of kv for transaction verify
pub type KvVersion = i64;

/// Convert u64 seceond to i64
fn conv_u64_sec_2_i64(sec: u64) -> i64 {
    sec.try_into()
        .unwrap_or_else(|e| panic!("ttl timeout_sec should < MAX_I64, err:{e}"))
}
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
        let etcd_rs_client = etcd_client::Client::connect(etcd_address_vec.clone(), None)
            .await
            .with_context(|| {
                format!("failed to connect to etcd, the etcd address={etcd_address_vec:?}")
            })?;
        Ok(Self {
            etcd_rs_client,
            end_point,
        })
    }

    /// Lock a name with time out
    #[inline]
    pub async fn lock(&self, name: &[u8], timeout_sec: u64) -> DatenLordResult<Vec<u8>> {
        let timeout_sec = if timeout_sec == 0 { 1 } else { timeout_sec };
        let mut etcd_rs_client = self.etcd_rs_client.clone();
        let lease_id = etcd_rs_client
            .lease_grant(conv_u64_sec_2_i64(timeout_sec), None)
            .await
            .with_context(|| "failed to get lease id from etcd when lock".to_owned())?
            .id();

        let options = LockOptions::new().with_lease(lease_id);

        let key = etcd_rs_client
            .lock(name, Some(options))
            .await
            .with_context(|| format!("failed to get lock from etcd, the lock name={name:?}"))?
            .key()
            .to_vec();

        Ok(key)
    }

    /// Unlock with the key, which comes from the lock operation
    #[inline]
    pub async fn unlock<T: Into<Vec<u8>> + Clone + Send>(&self, key: T) -> DatenLordResult<()> {
        let mut etcd_rs_client = self.etcd_rs_client.clone();
        etcd_rs_client
            .unlock(key)
            .await
            .with_context(|| "failed to unlock from etcd".to_owned())?;
        Ok(())
    }

    /// Get one key-value pair from etcd
    async fn get_one_kv_async<T: DeserializeOwned, K: Into<Vec<u8>> + Debug + Clone + Send>(
        &self,
        key: K,
    ) -> DatenLordResult<Option<(T, KvVersion)>> {
        let resp = self
            .etcd_rs_client
            .kv_client()
            .get(key, None)
            .await
            .with_context(|| "failed to get one key-value pair from etcd".to_owned())?;
        let kvs = resp.kvs();

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
        let mut etcd_rs_client = self.etcd_rs_client.clone();
        let bin_value = bincode::serialize(value)
            .with_context(|| format!("failed to encode {value:?} to binary"))?;
        let put_options = match expire {
            Some(expire) => PutOptions::new().with_prev_key().with_lease(
                etcd_rs_client
                    .lease_grant(conv_u64_sec_2_i64(expire.as_secs()), None)
                    .await
                    .with_context(|| {
                        "failed to get lease id at `write_to_etcd_with_version`".to_owned()
                    })?
                    .id(),
            ),
            None => PutOptions::new().with_prev_key(),
        };

        let txn = Txn::new()
            .when(vec![Compare::version(
                key.clone(),
                CompareOp::Equal,
                prev_version,
            )])
            .and_then(vec![TxnOp::put(key.clone(), bin_value, Some(put_options))])
            .or_else(vec![TxnOp::get(key.clone(), None)]);

        let txn_res = etcd_rs_client.txn(txn).await.with_context(|| {
            "failed to execute txn operation at `write_to_etcd_with_version`".to_owned()
        })?;

        if txn_res.succeeded() {
            //key does not exist, insert kv
            Ok(None)
        } else {
            let mut resp_ = txn_res.op_responses();
            // let mut resp_ = txn_res.get_responses();
            assert_eq!(resp_.len(), 1, "txn response length should be 1");
            match resp_.pop().unwrap_or_else(|| {
                panic!("txn response length should be 1 and pop should not fail")
            }) {
                TxnOpResponse::Get(resp) => {
                    let kvs = resp.kvs();
                    let kv_first = kvs.get(0).unwrap_or_else(|| panic!("kv res must be sz>0"));
                    //key exists
                    let decoded_value: T = util::decode_from_bytes(kv_first.value())?;
                    Ok(Some((decoded_value, kv_first.version())))
                }
                Put(_) | TxnOpResponse::Delete(_) | TxnOpResponse::Txn(_) => {
                    panic!("txn response should be RangeResponse");
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
        let mut etcd_rs_client = self.etcd_rs_client.clone();
        let bin_value = bincode::serialize(value)
            .with_context(|| format!("failed to encode {value:?} to binary"))?;
        // let mut req = etcd_client::EtcdPutRequest::new(key, bin_value);
        let put_options = match expire {
            Some(expire) => PutOptions::new().with_prev_key().with_lease(
                etcd_rs_client
                    .lease_grant(conv_u64_sec_2_i64(expire.as_secs()), None)
                    .await?
                    .id(),
            ),
            None => PutOptions::new().with_prev_key(),
        };
        let resp = self
            .etcd_rs_client
            .kv_client()
            .put(key.clone(), bin_value, Some(put_options))
            .await
            .with_context(|| "failed to put at `write_to_etcd`".to_owned())?;
        if let Some(prev_kv) = resp.prev_key() {
            let decoded_value = util::decode_from_bytes(prev_kv.value())?;
            Ok(Some((decoded_value, prev_kv.version())))
        } else {
            Ok(None)
        }
    }

    /// Write a key value pair to etcd when there's no key
    // - if key exist, don't insert, return the old value
    // - if key not exist, insert, return None
    // - `expire`: auto delete after expire
    async fn write_to_etcd_if_none<
        T: DeserializeOwned + Serialize + Clone + Debug + Send + Sync,
        K: Into<Vec<u8>> + Debug + Clone + Send,
    >(
        &self,
        key: K,
        value: &T,
        expire: Option<Duration>,
    ) -> DatenLordResult<Option<(T, KvVersion)>> {
        let mut etcd_rs_client = self.etcd_rs_client.clone();
        let bin_value = bincode::serialize(value)
            .with_context(|| format!("failed to encode {value:?} to binary"))?;
        // let mut req = etcd_client::EtcdPutRequest::new(key, bin_value);
        let put_options = match expire {
            Some(expire) => PutOptions::new().with_prev_key().with_lease(
                etcd_rs_client
                    .lease_grant(conv_u64_sec_2_i64(expire.as_secs()), None)
                    .await
                    .with_context(|| "failed to grant lease at `write_to_etcd_if_none`".to_owned())?
                    .id(),
            ),
            None => PutOptions::new().with_prev_key(),
        };

        // etcd：chack key exist in txn
        //  https://github.com/etcd-io/etcd/issues/7115
        //  https://github.com/etcd-io/etcd/issues/6740
        let txn = Txn::new()
            .when(vec![Compare::version(key.clone(), CompareOp::Equal, 0)])
            .and_then(vec![TxnOp::put(key.clone(), bin_value, Some(put_options))])
            .or_else(vec![TxnOp::get(key.clone(), None)]);

        let txn_res = etcd_rs_client
            .txn(txn)
            .await
            .with_context(|| "failed to do txn at `write_to_etcd_if_none`".to_owned())?;

        if txn_res.succeeded() {
            //key does not exist, insert kv
            Ok(None)
        } else {
            let mut resp_ = txn_res.op_responses();
            assert_eq!(resp_.len(), 1, "txn response length should be 1");
            match resp_.pop().unwrap_or_else(|| {
                panic!("txn response length should be 1 and pop should not fail")
            }) {
                TxnOpResponse::Get(resp) => {
                    let kvs = resp.kvs();
                    let kv_first = kvs.get(0).unwrap_or_else(|| panic!("kv res must be sz>0"));
                    //key exists
                    let decoded_value: T = util::decode_from_bytes(kv_first.value())?;
                    Ok(Some((decoded_value, kv_first.version())))
                }
                Put(_) | TxnOpResponse::Delete(_) | TxnOpResponse::Txn(_) => {
                    panic!("txn response should be RangeResponse");
                }
            }
        }
    }

    /// Delete a key value pair or nothing from etcd
    async fn delete_from_etcd<T: DeserializeOwned + Clone + Debug + Send + Sync>(
        &self,
        key: &str,
    ) -> DatenLordResult<Vec<(T, KvVersion)>> {
        let resp = self
            .etcd_rs_client
            .kv_client()
            .delete(key, Some(DeleteOptions::new().with_prev_key()))
            .await
            .with_context(|| "failed to delete at `delete_from_etcd`".to_owned())?;

        if resp.prev_kvs().is_empty() {
            Ok(Vec::new())
        } else {
            let deleted_value_list = resp.prev_kvs();
            let mut result_vec = Vec::with_capacity(deleted_value_list.len());
            for kv in deleted_value_list {
                let decoded_value: T = util::decode_from_bytes(kv.value())?;
                result_vec.push((decoded_value, kv.version()));
            }
            Ok(result_vec)
        }
    }

    /// Get key-value list from etcd
    #[inline]
    pub async fn get_list<T: DeserializeOwned>(&self, prefix: &str) -> DatenLordResult<Vec<T>> {
        let resp = self
            .etcd_rs_client
            .kv_client()
            .get(prefix, Some(GetOptions::new().with_prefix()))
            .await
            .with_context(|| "failed to get at `get_list`".to_owned())?;
        let mut result_vec = Vec::with_capacity(
            resp.count()
                .try_into()
                .unwrap_or_else(|e| panic!("resp.count() should be > 0, err:{e}")),
        );
        for kv in resp.kvs() {
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
        let value = self.get_one_kv_async(key).await.with_context(|| {
            "failed to `get_one_kv_async` at `get_at_most_one_value_with_version`".to_owned()
        })?;
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
        let value = self.get_one_kv_async(key).await.with_context(|| {
            "failed to `get_one_kv_async` at `get_at_most_one_value`".to_owned()
        })?;
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
        let write_res = self
            .write_to_etcd(key, value, None)
            .await
            .with_context(|| {
                "failed to `write_to_etcd` at `update_existing_kv_and_return_with_version`"
                    .to_owned()
            })?;
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
            .with_context(|| {
                "failed to `update_existing_kv_and_return_with_version` at `update_existing_kv`"
                    .to_owned()
            })
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
        let write_res = self
            .write_to_etcd(key, value, None)
            .await
            .with_context(|| "failed to `write_to_etcd` at `write_new_kv`".to_owned())?;
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
            .with_context(|| {
                "failed to `write_to_etcd_if_none` at `write_new_kv_no_panic`".to_owned()
            })
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
        self.write_to_etcd_if_none(key, value, expire)
            .await
            .with_context(|| {
                "failed to `write_to_etcd_if_none` at `write_new_kv_no_panic_return_with_version`"
                    .to_owned()
            })
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
        let write_res = self
            .write_to_etcd(key, value, None)
            .await
            .with_context(|| "failed to `write_to_etcd` at `write_or_update_kv`".to_owned())?;
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
        let write_res = self
            .write_to_etcd(key, value, Some(expire))
            .await
            .with_context(|| {
                "failed to `write_to_etcd` at `write_or_update_kv_with_timeout`".to_owned()
            })?;
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
            .with_context(|| {
                "failed to `write_to_etcd_with_version` at `write_or_update_kv_with_version`"
                    .to_owned()
            })
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
        let mut deleted_value_vec = self.delete_from_etcd(key).await.with_context(|| {
            "failed to `delete_from_etcd` at `delete_exact_one_value_return_with_version`"
                .to_owned()
        })?;
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
            .with_context(|| {
                "failed to `delete_exact_one_value_return_with_version` at `delete_exact_one_value`"
                    .to_owned()
            })
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
        let mut deleted_value_vec = self.delete_from_etcd(key).await.with_context(|| {
            "failed to `delete_from_etcd` at `delete_one_value_return_with_version`".to_owned()
        })?;
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
            .with_context(|| {
                "failed to `delete_one_value_return_with_version` at `delete_one_value`".to_owned()
            })
            .map(|opt| opt.map(|(v, _)| v))
    }

    /// Delete all key value pairs from etcd
    #[allow(dead_code)]
    #[inline]
    pub async fn delete_all(&self) -> DatenLordResult<()> {
        self.etcd_rs_client
            .kv_client()
            .delete("", Some(DeleteOptions::new().with_all_keys()))
            .await
            .with_context(|| "failed to `delete_all` at `delete_all`".to_owned())?;
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
        let mut etcd_rs_client = self.etcd_rs_client.clone();
        // let reciver_opt = self.etcd_rs_client.watch()
        let res = etcd_rs_client.watch(name, None).await;
        let (mut watcher, mut stream) =
            res.with_context(|| format!("failed to `watch` at `wait_key_delete` for key={name}"))?;

        let (stop_wait_log_task_chan_tx, mut stop_wait_log_task_chan_rx) =
            tokio::sync::oneshot::channel::<()>();
        let wait_log_task = {
            let name_clone = name.to_owned();
            tokio::spawn(async move {
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
            })
        };

        // // When we call this function,
        // //  there might be no key in it or someone just insert one into it.
        // // We might first receive a insert event.
        // // We need a loop to read until there's delete event.
        loop {
            match stream.message().await {
                Ok(ok) => {
                    if let Some(response) = ok {
                        if response.events().is_empty() {
                            debug!("receive empty response, watch closed?");
                            continue;
                        }
                        for event in response.events() {
                            if let Some(kv) = event.kv() {
                                let watched_key = kv.key_str().unwrap_or_else(|e| {
                                    panic!("Key here supposed to be str, err:{e}");
                                });
                                if watched_key == name {
                                    if let EventType::Delete = event.event_type() {
                                        stop_wait_log_task_chan_tx.send(()).unwrap_or_else(|_| {
                                            panic!("send failed, stop_wait_log_task_chan_rx was dropped?")
                                        });
                                        wait_log_task.await.unwrap_or_else(|_| {
                                            panic!("join wait log task failed");
                                        });

                                        watcher.cancel().await.with_context(||{
                                            format!("failed to `cancel watch` at `wait_key_delete` for key={name}")
                                        })?;
                                        return Ok(());
                                    }
                                }
                            }
                        }
                    } else {
                        panic!("receive empty response, watch closed?");
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
                    watcher.cancel().await.with_context(|| {
                        format!("failed to `cancel watch` at `wait_key_delete` for key={name}")
                    })?;

                    return Err(DatenLordError::EtcdClientErr {
                        source: err,
                        context,
                    });
                }
            }
        }
    }
}
