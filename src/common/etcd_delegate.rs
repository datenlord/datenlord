//! The etcd client implementation

use super::error::{Context, DatenLordResult};
use super::util;
use log::debug;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt;
use std::fmt::Debug;
use std::time::Duration;
use etcd_client::{TxnCmp, TxnOpResponse};

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
            format!(
                "failed to build etcd client to addresses={:?}",
                etcd_address_vec,
            )
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
                format!(
                    "failed to get LeaseGrantResponse from etcd, the timeout={}",
                    timeout
                )
            })?
            .id();

        let key = self
            .etcd_rs_client
            .lock()
            .lock(etcd_client::EtcdLockRequest::new(name, lease_id))
            .await
            .with_context(|| {
                format!(
                    "failed to get LockResponse from etcd, the lease id={}",
                    lease_id
                )
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
    ) -> DatenLordResult<Option<T>> {
        let key_clone = key.clone();
        let req = etcd_client::EtcdRangeRequest::new(etcd_client::KeyRange::key(key));
        let mut resp = self.etcd_rs_client.kv().range(req).await.with_context(|| {
            format!(
                "failed to get RangeResponse of one key-value pair from etcd, the key={:?}",
                key_clone
            )
        })?;

        let kvs = resp.take_kvs();
        match kvs.get(0) {
            Some(kv) => Ok(Some(util::decode_from_bytes::<T>(kv.value())?)),
            None => Ok(None),
        }
    }

    /// Write a key value pair to etcd
    async fn write_to_etcd<
        T: DeserializeOwned + Serialize + Clone + Debug + Send + Sync,
        K: Into<Vec<u8>> + Debug + Clone + Send,
    >(
        &self,
        key: K,
        value: &T,
    ) -> DatenLordResult<Option<T>> {
        let key_clone = key.clone();
        let bin_value = bincode::serialize(value)
            .with_context(|| format!("failed to encode {:?} to binary", value))?;
        let mut req = etcd_client::EtcdPutRequest::new(key, bin_value);
        req.set_prev_kv(true); // Return previous value
        let mut resp = self.etcd_rs_client.kv().put(req).await.with_context(|| {
            format!(
                "failed to get PutResponse from etcd for key={:?}, value={:?}",
                key_clone, value,
            )
        })?;
        if let Some(pre_kv) = resp.take_prev_kv() {
            let decoded_value: T = util::decode_from_bytes(pre_kv.value())?;
            Ok(Some(decoded_value))
        } else {
            Ok(None)
        }
    }


    /// if key exist, don't insert, return the old value
    /// if key not exist, insert, return None
    async fn write_to_etcd_if_none<
        T: DeserializeOwned + Serialize + Clone + Debug + Send + Sync,
        K: Into<Vec<u8>> + Debug + Clone + Send,
    >(
        &self,
        key: K,
        value: &T,
    ) -> DatenLordResult<Option<T>> {

        let bin_value = bincode::serialize(value)
            .with_context(|| format!("failed to encode {:?} to binary", value))?;

        let txn_req=etcd_client::EtcdTxnRequest::new()
            // etcd：chack key exist in txn
            //  https://github.com/etcd-io/etcd/issues/7115
            //  https://github.com/etcd-io/etcd/issues/6740
            //key does not exist when create revision is 0, check the links above
            .when_create_revision(etcd_client::KeyRange::key(key.clone()), TxnCmp::Equal,0)
            //key does not exist, insert kv
            .and_then(etcd_client::EtcdPutRequest::new(key.clone(), bin_value))
            //key exists, return old value
            .or_else(etcd_client::EtcdRangeRequest::new(etcd_client::KeyRange::key(key)));
        let txn_res=self.etcd_rs_client.kv().txn(txn_req).await.with_context(|| {
            format!(
                "failed to get PutResponse from etcd for key={:?}, value={:?}",
                key, value,
            )
        })?;

        if txn_res.is_success(){
            //key does not exist, insert kv
            Ok(None)
        }else{
            let resp=txn_res.get_responses();
            assert_eq!(resp.len(), 1, "txn response length should be 1");
            let resp=&resp[0];
            match resp {
                TxnOpResponse::Range(_) => {
                    //key exists
                    let decoded_value: T = util::decode_from_bytes(pre_kv.value())?;
                    Ok(Some(decoded_value))
                }
                _ => {
                    panic!("txn response should be RangeResponse");
                }
            }
        }
    }

    /// Delete a key value pair or nothing from etcd
    async fn delete_from_etcd<T: DeserializeOwned + Clone + Debug + Send + Sync>(
        &self,
        key: &str,
    ) -> DatenLordResult<Vec<T>> {
        let mut req = etcd_client::EtcdDeleteRequest::new(etcd_client::KeyRange::key(key));
        req.set_prev_kv(true);
        let mut resp = self
            .etcd_rs_client
            .kv()
            .delete(req)
            .await
            .with_context(
                || format!("failed to get DeleteResponse from etcd for key={:?}", key,),
            )?;

        if resp.has_prev_kvs() {
            let deleted_value_list = resp.take_prev_kvs();

            let mut result_vec = Vec::with_capacity(deleted_value_list.len());
            for kv in deleted_value_list {
                let decoded_value: T = util::decode_from_bytes(kv.value())?;
                result_vec.push(decoded_value);
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
        let mut resp = self.etcd_rs_client.kv().range(req).await.with_context(|| {
            format!("failed to get RangeResponse from etcd for key={:?}", prefix,)
        })?;
        let mut result_vec = Vec::with_capacity(resp.count());
        for kv in resp.take_kvs() {
            let decoded_value: T = util::decode_from_bytes(kv.value())?;
            result_vec.push(decoded_value);
        }
        Ok(result_vec)
    }

    /// Get zero or one key-value pair from etcd
    #[inline]
    pub async fn get_at_most_one_value<
        T: DeserializeOwned,
        K: Into<Vec<u8>> + Debug + Clone + Send,
    >(
        &self,
        key: K,
    ) -> DatenLordResult<Option<T>> {
        let value = self.get_one_kv_async(key).await?;
        Ok(value)
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
        let write_res = self.write_to_etcd(key, value).await?;
        if let Some(pre_value) = write_res {
            Ok(pre_value)
        } else {
            panic!("failed to replace previous value, return nothing");
        }
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
        let write_res = self.write_to_etcd(key, value).await?;
        if let Some(pre_value) = write_res {
            panic!(
                "failed to write new key vaule pair, the key={} exists in etcd, \
                    the previous value={:?}",
                key, pre_value,
            );
        } else {
            Ok(())
        }
    }

    /// return exist value if key exists
    /// return none if key does not exist
    #[inline]
    pub async fn write_new_kv_no_panic<T: DeserializeOwned + Serialize + Clone + Debug + Send + Sync>(
        &self,
        key: &str,
        value: &T,
    ) -> DatenLordResult<Option<T>> {

        self.write_to_etcd_if_none(key, value).await
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
        let write_res = self.write_to_etcd(key, value).await?;
        if let Some(pre_value) = write_res {
            debug!(
                "key={:?} exists in etcd, the previous value={:?}, update it",
                key_clone, pre_value
            );
        }
        Ok(())
    }

    /// Delete an existing key value pair from etcd
    /// # Panics
    ///
    /// Will panic if failed to get key from etcd
    #[inline]
    pub async fn delete_exact_one_value<T: DeserializeOwned + Clone + Debug + Send + Sync>(
        &self,
        key: &str,
    ) -> DatenLordResult<T> {
        let mut deleted_value_vec = self.delete_from_etcd(key).await?;
        debug_assert_eq!(
            deleted_value_vec.len(),
            1,
            "delete {} key value pairs for a single key={}, should delete exactly one",
            deleted_value_vec.len(),
            key,
        );
        let deleted_kv = if let Some(kv) = deleted_value_vec.pop() {
            kv
        } else {
            panic!("failed to get the exactly one deleted key value pair")
        };
        Ok(deleted_kv)
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
}
