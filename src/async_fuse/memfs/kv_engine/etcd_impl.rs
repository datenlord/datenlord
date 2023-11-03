use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Duration;

use async_trait::async_trait;
use etcd_client::{
    Compare, CompareOp, DeleteOptions, GetOptions, LockOptions, PutOptions, Txn, TxnOp,
};

use super::{
    check_ttl, conv_u64_sec_2_i64, fmt, DeleteOption, KVEngine, KeyRange, KeyType, KvVersion,
    LockKeyType, MetaTxn, SetOption, ValueType,
};
use crate::common::error::{Context, DatenLordResult};

#[derive(Clone)]
/// Wrap the etcd client to support the `KVEngine` trait.
pub struct EtcdKVEngine {
    /// The etcd client.
    client: etcd_client::Client,
}

impl Debug for EtcdKVEngine {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EtcdKVEngine").finish()
    }
}

impl EtcdKVEngine {
    #[allow(dead_code)]
    /// For local test, we need to create a new etcd kv engine locally.
    async fn new_for_local_test(etcd_address_vec: Vec<String>) -> DatenLordResult<Self> {
        let client = etcd_client::Client::connect(etcd_address_vec.clone(), None)
            .await
            .with_context(|| {
                format!("failed to connect to etcd, the etcd address={etcd_address_vec:?}")
            })?;
        Ok(EtcdKVEngine { client })
    }
}

#[async_trait]
impl KVEngine for EtcdKVEngine {
    #[must_use]
    async fn new(end_points: Vec<String>) -> DatenLordResult<Self> {
        let client = etcd_client::Client::connect(end_points, None).await?;
        Ok(Self { client })
    }

    async fn new_meta_txn(&self) -> Box<dyn MetaTxn + Send> {
        Box::new(EtcdTxn::new(self.client.clone()))
    }

    async fn lease_grant(&self, ttl: i64) -> DatenLordResult<i64> {
        let mut client = self.client.clone();
        let timeout_sec = check_ttl(ttl)
            .with_context(|| "timeout_sec should be >=1s, please fix the call".to_owned())?;
        Ok(client
            .lease_grant(timeout_sec, None)
            .await
            .with_context(|| "failed to get lease at `MetaTxn::lock`".to_owned())?
            .id())
    }

    async fn range(&self, key_range: KeyRange) -> DatenLordResult<Vec<(Vec<u8>, Vec<u8>)>> {
        // check that with_all_keys and with_prefix are not set at the same time
        debug_assert!(
            !(key_range.with_all_keys && key_range.with_prefix),
            "with_all_keys and with_prefix are not set at the same time"
        );
        let option = if key_range.with_all_keys {
            Some(GetOptions::new().with_all_keys())
        } else if key_range.with_prefix {
            Some(GetOptions::new().with_prefix())
        } else {
            Some(GetOptions::new().with_range(key_range.range_end))
        };
        let resp = self
            .client
            .kv_client()
            .get(key_range.key, option)
            .await
            .with_context(|| "failed to get range at `KVEngine::range`".to_owned())?;
        let kvs = resp.kvs();
        let result: Vec<_> = kvs
            .iter()
            .map(|kv| (kv.key().to_vec(), kv.value().to_vec()))
            .collect();
        Ok(result)
    }

    /// Distribute lock - lock
    /// - `timeout_sec` should be >=1s
    /// - `timeout_sec` should be >=1s
    async fn lock(&self, key: &LockKeyType, timeout_sec: Duration) -> DatenLordResult<Vec<u8>> {
        let mut client = self.client.clone();
        let timeout_sec = check_ttl(conv_u64_sec_2_i64(timeout_sec.as_secs()))
            .with_context(|| "timeout_sec should be >=1s, please fix the call".to_owned())?;

        let lease_id = client
            .lease_grant(timeout_sec, None)
            .await
            .with_context(|| "failed to get lease at `MetaTxn::lock`".to_owned())?
            .id();

        let resp = client
            .lock(key.get_key(), Some(LockOptions::new().with_lease(lease_id)))
            .await
            .with_context(|| "failed to lock at `MetaTxn::lock`".to_owned())?;

        Ok(resp.key().to_vec())
    }

    /// Distribute lock - unlock
    async fn unlock(&self, key: Vec<u8>) -> DatenLordResult<()> {
        let mut client = self.client.clone();
        client
            .unlock(key)
            .await
            .with_context(|| "failed to unlock at `MetaTxn::unlock`".to_owned())?;

        Ok(())
    }

    /// Get the value by the key.
    async fn get(&self, key: &KeyType) -> DatenLordResult<Option<ValueType>> {
        let mut client = self.client.clone();
        let resp = client
            .get(key.get_key(), None)
            .await
            .with_context(|| format!("failed to get at `MetaTxn::get`, key={key:?}"))?;

        let kvs = resp.kvs();
        match kvs.get(0) {
            Some(kv) => Ok(Some(serde_json::from_slice::<ValueType>(kv.value()).with_context(||{
                "failed to deserialize value from bytes, KVEngine's value supposed to be `ValueType`".to_owned()
            })?)),
            None => Ok(None),
        }
    }

    /// Set the value by the key.
    async fn set(
        &self,
        key: &KeyType,
        value: &ValueType,
        option: Option<SetOption>,
    ) -> DatenLordResult<Option<ValueType>> {
        let option = match option {
            Some(option) => {
                let mut set_option = PutOptions::new();
                if option.prev_kv {
                    set_option = set_option.with_prev_key();
                }
                if let Some(lease) = option.lease {
                    set_option = set_option.with_lease(lease);
                }
                Some(set_option)
            }
            None => None,
        };
        let serial_value = serde_json::to_vec(value)
            .with_context(|| format!("failed to serialize value={value:?} to bytes"))?;
        let mut client = self.client.clone();
        let mut resp = client
            .put(key.get_key(), serial_value, option)
            .await
            .with_context(|| "failed to put at `MetaTxn::set`".to_owned())?;
        if let Some(pre_kv) = resp.take_prev_key() {
            let decoded_value: ValueType = serde_json::from_slice(pre_kv.value())?;
            Ok(Some(decoded_value))
        } else {
            Ok(None)
        }
    }

    /// Delete the kv pair by the key.
    async fn delete(
        &self,
        key: &KeyType,
        option: Option<DeleteOption>,
    ) -> DatenLordResult<Option<ValueType>> {
        let option = match option {
            Some(option) => {
                let mut delete_option = DeleteOptions::new();
                if option.prev_kv {
                    delete_option = delete_option.with_prev_key();
                }
                if let Some(range_end) = option.range_end {
                    delete_option = delete_option.with_range(range_end);
                }
                Some(delete_option)
            }
            None => None,
        };
        let resp = self
            .client
            .kv_client()
            .delete(key.get_key(), option)
            .await
            .with_context(|| format!("failed to get DeleteResponse from etcd for key={key:?}"))?;
        if let Some(pre_kv) = resp.prev_kvs().first() {
            let decoded_value: ValueType = serde_json::from_slice(pre_kv.value())?;
            Ok(Some(decoded_value))
        } else {
            Ok(None)
        }
    }
}

/// The `etcd`'s transaction impl.
/// The txn won't do anything until commit is called.
/// Write operations are buffered until commit is called.
struct EtcdTxn {
    /// The etcd client.
    client: etcd_client::Client,
    /// The key is the key in bytes, the value is the version of the key.
    version_map: HashMap<Vec<u8>, KvVersion>,
    /// Store the write operations in the buffer.
    buffer: HashMap<Vec<u8>, Option<Vec<u8>>>,
}

impl EtcdTxn {
    /// Create a new etcd transaction.
    fn new(client: etcd_client::Client) -> Self {
        EtcdTxn {
            client,
            version_map: HashMap::new(),
            buffer: HashMap::new(),
        }
    }
}

#[async_trait]
impl MetaTxn for EtcdTxn {
    async fn get(&mut self, key: &KeyType) -> DatenLordResult<Option<ValueType>> {
        // first check if the key is in buffer (write op)
        let key = key.get_key();
        assert!(
            self.buffer.get(&key).is_none(),
            "get the key after write in the same transaction"
        );
        assert!(
            self.version_map.get(&key).is_none(),
            "get the key twice in the same transaction"
        );
        // Fetch the value from `etcd`
        let resp = self
            .client
            .get(key.clone(), None)
            .await
            .with_context(|| "failed to get at `MetaTxn::get`".to_owned())?;
        let kvs = resp.kvs();
        // we don't expect to have multiple values for one key
        assert!(kvs.len() <= 1, "multiple values for one key");
        if let Some(kv) = kvs.get(0) {
            let value = kv.value();
            // update the version_map
            self.version_map.insert(key.clone(), kv.version());
            Ok(Some(serde_json::from_slice(value)?))
        } else {
            // update the version_map
            self.version_map.insert(key, 0);
            Ok(None)
        }
    }

    fn set(&mut self, key: &KeyType, value: &ValueType) {
        let key = key.get_key();
        // Because the ValueType derives the serde::Serialize
        // This unwrap will not panic.
        let value = serde_json::to_vec(value)
            .unwrap_or_else(|value| panic!("failed to serialize value to json,value = {value:?}"));
        self.buffer.insert(key, Some(value));
    }

    fn delete(&mut self, key: &KeyType) {
        let key = key.get_key();
        self.buffer.insert(key, None);
    }

    async fn commit(&mut self) -> DatenLordResult<bool> {
        if self.version_map.is_empty() && self.buffer.is_empty() {
            return Ok(true);
        }

        let resp = self
            .client
            .txn(
                Txn::new()
                    .when(
                        self.version_map
                            .iter()
                            .map(|(key, version)| {
                                Compare::version(key.clone(), CompareOp::Equal, *version)
                            })
                            .collect::<Vec<Compare>>(),
                    )
                    .and_then(
                        self.buffer
                            .iter()
                            .map(|(key, value)| {
                                if let Some(ref value) = *value {
                                    TxnOp::put(key.clone(), value.clone(), None)
                                } else {
                                    TxnOp::delete(key.clone(), None)
                                }
                            })
                            .collect::<Vec<TxnOp>>(),
                    ),
            )
            .await
            .with_context(|| "failed to do txn operation at `MetaTxn::commit`".to_owned())?;
        Ok(resp.succeeded())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod test {

    use std::time::Instant;

    use super::*;
    use crate::common::error::DatenLordError;
    use crate::retry_txn;

    const ETCD_ADDRESS: &str = "localhost:2379";

    #[tokio::test]
    async fn test_lock_unlock() {
        let test_key = 1224;
        let client = EtcdKVEngine::new_for_local_test(vec![ETCD_ADDRESS.to_owned()])
            .await
            .unwrap();
        let key: LockKeyType = LockKeyType::FileNodeListLock(test_key);
        let lock_key = client.lock(&key, Duration::from_secs(9999)).await.unwrap();
        // start a new thread to lock the same key
        // to check that lock the same key will be blocked
        // the first lock will be unlock after 2 seconds
        // if the second lock the same key ,it will be blocked until the first lock
        // unlock
        let lock_time = Duration::from_secs(2);
        let time_now = Instant::now();
        let handle = tokio::spawn(async move {
            let client2 = EtcdKVEngine::new_for_local_test(vec![ETCD_ADDRESS.to_owned()])
                .await
                .unwrap();
            // the time it takes to lock the same key should be greater than 5 seconds
            // check the time duration
            let key = LockKeyType::FileNodeListLock(test_key);
            let lock_key = client2.lock(&key, Duration::from_secs(9999)).await.unwrap();
            let time_duration = Instant::now().duration_since(time_now).as_secs();
            assert_eq!(time_duration, 2, "lock the same key should be blocked",);
            assert!(time_duration >= lock_time.as_secs());
            client2.unlock(lock_key).await.unwrap();
        });
        // sleep 5 second to make sure the second lock is blocked
        tokio::time::sleep(lock_time).await;
        client.unlock(lock_key).await.unwrap();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_connect_local() {
        let client = EtcdKVEngine::new_for_local_test(vec![ETCD_ADDRESS.to_owned()])
            .await
            .unwrap();
        // insert a key , and then get it , and then delete it, and then get it again
        let key = KeyType::String("test_key".to_owned());
        let value = ValueType::INum(123);
        client.set(&key, &value, None).await.unwrap();
        let get_value = client.get(&key).await.unwrap().unwrap();
        assert_eq!(get_value, value);
        client.delete(&key, None).await.unwrap();
        let get_value = client.get(&key).await.unwrap();
        assert!(get_value.is_none());
    }

    #[tokio::test]
    async fn test_easy_commit_fail() {
        // Generate three transactions
        // The first one will set two keys and commit
        // And the second one read two keys
        // And the third one will set two keys and commit
        // What we expect is that the second one will fail
        // Between it's read ,the third one will set the same key
        let client = EtcdKVEngine::new_for_local_test(vec![ETCD_ADDRESS.to_owned()])
            .await
            .unwrap();
        let mut first_txn = client.new_meta_txn().await;
        let key1 = KeyType::String(String::from("test_commit key1"));
        let value1 = ValueType::INum(12);
        let key2 = KeyType::String(String::from("test_commit key2"));
        let value2 = ValueType::INum(13);
        first_txn.set(&key1, &value1);
        first_txn.set(&key2, &value2);
        first_txn.commit().await.unwrap();
        drop(client);
        // use two thread to do the second and third txn
        // and use channel to control the order
        let (first_step_tx, mut first_step_rx) = tokio::sync::mpsc::channel(1);
        let (second_step_tx, mut second_step_rx) = tokio::sync::mpsc::channel(1);
        let second_handle = tokio::spawn(async move {
            let result = retry_txn!(1, {
                let client = EtcdKVEngine::new_for_local_test(vec![ETCD_ADDRESS.to_owned()])
                    .await
                    .unwrap();
                let mut second_txn = client.new_meta_txn().await;
                let key1 = KeyType::String(String::from("test_commit key1"));
                let value1 = second_txn.get(&key1).await.unwrap();
                assert!(value1.is_some());
                if let Some(ValueType::INum(num)) = value1 {
                    assert_eq!(num, 12);
                } else {
                    panic!("wrong value type");
                }
                // let the third txn start
                first_step_tx.send(()).await.unwrap();
                // wait for the third txn to set the key
                second_step_rx.recv().await.unwrap();
                let key2 = KeyType::String(String::from("test_commit key2"));
                let value2 = second_txn.get(&key2).await.unwrap();
                assert!(value2.is_some());
                if let Some(ValueType::INum(num)) = value2 {
                    assert_eq!(num, 13);
                } else {
                    panic!("wrong value type");
                }
                (second_txn.commit().await, ())
            });
            assert!(result.is_err());
            // check if the err is TransactionRetryLimitExceededErr
            if let Err(DatenLordError::TransactionRetryLimitExceededErr { .. }) = result {
            } else {
                panic!("wrong error type");
            }
        });
        let third_handle = tokio::spawn(async move {
            let client = EtcdKVEngine::new_for_local_test(vec![ETCD_ADDRESS.to_owned()])
                .await
                .unwrap();
            let mut third_txn = client.new_meta_txn().await;
            // wait for the second read first key and send the signal
            first_step_rx.recv().await.unwrap();
            let key1 = KeyType::String(String::from("test_commit key1"));
            let value1 = ValueType::INum(14);
            third_txn.set(&key1, &value1);
            third_txn.commit().await.unwrap();
            // send the signal to the second txn
            second_step_tx.send(()).await.unwrap();
        });
        second_handle.await.unwrap();
        third_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_txn_retry() {
        let result = retry_txn!(3, {
            let client = EtcdKVEngine::new_for_local_test(vec![ETCD_ADDRESS.to_owned()])
                .await
                .unwrap();
            let mut txn = client.new_meta_txn().await;
            let key = KeyType::String(String::from("/"));
            let _ = txn.get(&key).await.unwrap();
            (txn.commit().await, ())
        });
        result.unwrap();
    }
}
