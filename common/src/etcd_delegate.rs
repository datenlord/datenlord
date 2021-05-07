//! The etcd client implementation

use async_compat::Compat;
use log::debug;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use std::time::Duration;

use super::error::{Context, DatenLordResult};
use super::util;

/// The client to communicate with etcd
#[derive(Clone)]
pub struct EtcdDelegate {
    /// The inner etcd client
    etcd_rs_client: etcd_client::Client,
}

impl EtcdDelegate {
    /// Build etcd client
    pub fn new(etcd_address_vec: Vec<String>) -> DatenLordResult<Self> {
        let etcd_rs_client = smol::block_on(Compat::new(async move {
            etcd_client::Client::connect(etcd_client::ClientConfig {
                endpoints: etcd_address_vec.clone(),
                auth: None,
                cache_enable: true,
                // TODO: cache size should come from parameter
                cache_size: 64,
            })
            .await
            .with_context(|| {
                format!(
                    "failed to build etcd client to addresses={:?}",
                    etcd_address_vec,
                )
            })
        }))?;
        Ok(Self { etcd_rs_client })
    }

    /// Lock a name with time out
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
    pub async fn unlock<T: Into<Vec<u8>> + Clone>(&self, key: T) -> DatenLordResult<()> {
        self.etcd_rs_client
            .lock()
            .unlock(etcd_client::EtcdUnlockRequest::new(key.clone()))
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
    async fn get_one_kv_async<T: DeserializeOwned>(&self, key: &str) -> DatenLordResult<Option<T>> {
        let req = etcd_client::EtcdRangeRequest::new(etcd_client::KeyRange::key(key));
        let mut resp = self.etcd_rs_client.kv().range(req).await.with_context(|| {
            format!(
                "failed to get RangeResponse of one key-value pair from etcd, the key={:?}",
                key
            )
        })?;

        let kvs = resp.take_kvs();
        match kvs.get(0) {
            Some(kv) => Ok(Some(util::decode_from_bytes::<T>(kv.value())?)),
            None => Ok(None),
        }
    }

    /// Write a key value pair to etcd
    async fn write_to_etcd<T: DeserializeOwned + Serialize + Clone + Debug + Send + Sync>(
        &self,
        key: &str,
        value: &T,
    ) -> DatenLordResult<Option<T>> {
        let bin_value = bincode::serialize(value)
            .with_context(|| format!("failed to encode {:?} to binary", value))?;
        let mut req = etcd_client::EtcdPutRequest::new(key, bin_value);
        req.set_prev_kv(true); // Return previous value
        let mut resp = self.etcd_rs_client.kv().put(req).await.with_context(|| {
            format!(
                "failed to get PutResponse from etcd for key={:?}, value={:?}",
                key, value,
            )
        })?;
        if let Some(pre_kv) = resp.take_prev_kv() {
            let decoded_value: T = util::decode_from_bytes(pre_kv.value())?;
            Ok(Some(decoded_value))
        } else {
            Ok(None)
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
    pub async fn get_at_most_one_value<T: DeserializeOwned>(
        &self,
        key: &str,
    ) -> DatenLordResult<Option<T>> {
        let value = self.get_one_kv_async(key).await?;
        Ok(value)
    }

    /// Update a existing key value pair to etcd
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

    /// Write key value pair to etcd, if key exists, update it
    pub async fn write_or_update_kv<
        T: DeserializeOwned + Serialize + Clone + Debug + Send + Sync,
    >(
        &self,
        key: &str,
        value: &T,
    ) -> DatenLordResult<()> {
        let write_res = self.write_to_etcd(key, value).await?;
        if let Some(pre_value) = write_res {
            debug!(
                "key={} exists in etcd, the previous value={:?}, update it",
                key, pre_value
            );
        }
        Ok(())
    }

    /// Delete an existing key value pair from etcd
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
