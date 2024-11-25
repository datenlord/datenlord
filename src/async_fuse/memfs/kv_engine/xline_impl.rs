use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Duration;

use async_trait::async_trait;
use datenlord::metrics::KV_METRICS;
use anyhow::Context;
use xline_client::{
    types::{kv::{
        Compare, CompareResult, DeleteRangeRequest, PutRequest, RangeRequest,
        TxnOp, TxnRequest, 
    }, lock::{LockRequest, UnlockRequest},
    lease::LeaseGrantRequest
    },
    Client, ClientOptions,
};


use super::{
    check_ttl, conv_u64_sec_2_i64, fmt, DeleteOption, KVEngine, KeyType, KvVersion, LockKeyType,
    MetaTxn, SetOption, ValueType,
};
//use crate::common::error::{Context
use crate::common::error::DatenLordResult;

#[derive(Clone)]
/// Wrap the etcd client to support the `KVEngine` trait.
pub struct XlineKVEngine {
    /// The xline client.
    client: Client,
}


impl Debug for XlineKVEngine {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EtcdKVEngine").finish()
    }
}

impl XlineKVEngine {
    #[allow(dead_code)]
    /// For local test, we need to create a new xline kv engine locally.
    async fn new_for_local_test(curp_members_vec: Vec<String>) -> DatenLordResult<Self> {
        let client = xline_client::Client::connect(curp_members_vec.clone(), ClientOptions::default())
            .await
            .with_context(|| {
                format!("failed to connect to xline, the curp members={curp_members_vec:?}")
            })?;
        Ok(XlineKVEngine { client })
    }


    /// Get all key/value pairs with the given prefix.
    async fn range_raw_key(&self, prefix: impl Into<Vec<u8>>) -> DatenLordResult<Vec<ValueType>> {
        let client = self.client.kv_client();
        let resp = client
            .range(RangeRequest::new(prefix))
            .await
            .with_context(|| "failed to get from xline engine".to_owned())?;
        let kvs = resp.kvs;
        let mut result = Vec::new();
        for kv in kvs {
            let value = serde_json::from_slice::<ValueType>(&kv.value).with_context(|| {
                "failed to deserialize value from bytes, KVEngine's value supposed to be `ValueType`".to_owned()
            })?;
            result.push(value);
        }
        Ok(result)
    }
}

#[async_trait]
impl KVEngine for XlineKVEngine {
    async fn new(end_points: Vec<String>) -> DatenLordResult<Self> {
        let client = xline_client::Client::connect(end_points, ClientOptions::default())
        .await
        .with_context(|| "failed to get from xline engine".to_owned())?;
        Ok(Self { client })
    }

    async fn new_meta_txn(&self) -> Box<dyn MetaTxn + Send> {
        Box::new(XlineTxn::new(self.client.clone()))
    }


    //lease grant

    async fn lease_grant(&self, ttl: i64) -> DatenLordResult<i64> {
        let client = self.client.lease_client();
        let timeout_sec = check_ttl(ttl)
            .with_context(|| "timeout_sec should be >=1s, please fix the call".to_owned())?;
        Ok(client
            .grant(LeaseGrantRequest::new(timeout_sec))
            .await
            .with_context(|| "failed to get lease at `MetaTxn::lock`".to_owned())?
            .id
            )
    }


    /// Distribute lock - lock
    /// - `timeout_sec` should be >=1s
    /// - `timeout_sec` should be >=1s
    

    async fn lock(&self, key: &LockKeyType, timeout_sec: Duration) -> DatenLordResult<Vec<u8>> {
        let _timer = KV_METRICS.start_kv_lock_timer();
        let client = &self.client;
        let timeout_sec = check_ttl(conv_u64_sec_2_i64(timeout_sec.as_secs()))
            .with_context(|| "timeout_sec should be >=1s, please fix the call".to_owned())?;

        let lease_id = client
            .lease_client()
            .grant(LeaseGrantRequest::new(timeout_sec))
            .await
            .with_context(|| "failed to get lease at `MetaTxn::lock`".to_owned())?
            .id;

        let resp = client
            .lock_client()
            .lock(
                LockRequest::new(key.to_string_key()).with_lease(lease_id)
            )
            .await
            .with_context(|| "failed to lock at `MetaTxn::lock`".to_owned())?;

        Ok(resp.key.to_vec())
    }

    //unlock
    async fn unlock(&self, key: Vec<u8>) -> DatenLordResult<()> {
        let client = self.client.lock_client();
        client
            .unlock(UnlockRequest::new(key))
            .await
            .with_context(|| "failed to unlock at `MetaTxn::unlock`".to_owned())?;

        Ok(())
    }


    /// Get the value by the key.
    async fn get(&self, key: &KeyType) -> DatenLordResult<Option<ValueType>> {
        let _timer = KV_METRICS.start_kv_operation_timer("get");
        let client = self.client.kv_client();
        let resp = client
            .range(RangeRequest::new(key.to_string_key()))
            .await
            .with_context(|| format!("failed to get from etcd engine, key={key:?}"))?;

        let kvs = resp.kvs.first();
        match kvs {
            Some(kv) => Ok(Some(serde_json::from_slice::<ValueType>(&kv.value).with_context(||{
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
        _option: Option<SetOption>,
    ) -> DatenLordResult<Option<ValueType>> {
        let _timer = KV_METRICS.start_kv_operation_timer("set");
        let serial_value = serde_json::to_vec(value)
        .with_context(|| format!("failed to serialize value={value:?} to bytes"))?;
        let mut set_request = PutRequest::new(key.to_string_key(), serial_value);
        let _option = match _option {
            Some(_option) => {
                if _option.prev_kv {
                    set_request = set_request.with_prev_kv(_option.prev_kv);
                }
                if let Some(lease) = _option.lease {
                    set_request = set_request.with_lease(lease);
                }
                Some(&set_request)
            }
            None => None,
        };

        let client = self.client.kv_client();
        let resp = client
            .put(set_request)
            .await
            .with_context(|| "failed to put at `MetaTxn::set`".to_owned())?;
        if let Some(pre_kv) = resp.prev_kv {
            let decoded_value: ValueType = serde_json::from_slice(&pre_kv.value)?;
            Ok(Some(decoded_value))
        } else {
            Ok(None)
        }
    }

    /// Delete the kv pair by the key.
    async fn delete(
        &self,
        key: &KeyType,
        _option: Option<DeleteOption>,
    ) -> DatenLordResult<Option<ValueType>> {
        let _timer = KV_METRICS.start_kv_operation_timer("delete");
        let mut delete_req = DeleteRangeRequest::new(key.to_string_key());
        let _option = match _option {
            Some(_option) => {
                //let mut delete_req = DeleteRangeRequest::new(key.to_string_key());
                if _option.prev_kv {
                    //with_prev_key => with_prev_kv(true)
                    delete_req = delete_req.with_prev_kv(true);
                }
                if let Some(range_end) = _option.range_end {
                    delete_req = delete_req.with_range_end(range_end);
                }
    
                Some(&delete_req)
            }
            None => None,
        };

        //.delete(DeleteRangeRequest::new("key1").with_prev_kv(true))
        let resp = self
            .client
            .kv_client()
            .delete(delete_req)
            .await
            .with_context(|| format!("failed to get DeleteResponse from etcd for key={key:?}"))?;
        if let Some(pre_kv) = resp.prev_kvs.first() {
            let decoded_value: ValueType = serde_json::from_slice(&pre_kv.value)?;
            Ok(Some(decoded_value))
        } else {
            Ok(None)
        }
    }
    /// range
    async fn range(&self, prefix: &KeyType) -> DatenLordResult<Vec<ValueType>> {
        let _timer = KV_METRICS.start_kv_operation_timer("range");
        let result = self.range_raw_key(prefix.to_string_key()).await?;
        Ok(result)
    }
    

}


/// The `xline`'s transaction impl.
/// The txn won't do anything until commit is called.
/// Write operations are buffered until commit is called.
struct XlineTxn {
    /// The etcd client.
    client: Client,
    /// The key is the key in bytes, the value is the version of the key.
    version_map: HashMap<Vec<u8>, KvVersion>,
    /// Store the write operations in the buffer.
    buffer: HashMap<Vec<u8>, Option<Vec<u8>>>,
}

impl XlineTxn {
    /// Create a new xline transaction.
    fn new(client: Client) -> Self {
        XlineTxn {
            client,
            version_map: HashMap::new(),
            buffer: HashMap::new(),
        }
    }
}


#[async_trait]
impl MetaTxn for XlineTxn {
    async fn get(&mut self, key_arg: &KeyType) -> DatenLordResult<Option<ValueType>> {
        let _timer = KV_METRICS.start_kv_operation_timer("get");

        // first check if the key is in buffer (write op)
        let key = key_arg.to_string_key().into_bytes();
        assert!(
            self.buffer.get(&key).is_none(),
            "get the key={key_arg:?} after write in the same transaction"
        );
        assert!(
            self.version_map.get(&key).is_none(),
            "get the key={key_arg:?} twice in the same transaction"
        );
        // Fetch the value from `etcd`
        let resp = self
            .client
            .kv_client()
            .range(RangeRequest::new(key.clone()))
            .await
            .with_context(|| "failed to get at `MetaTxn::get`".to_owned())?;
        let kvs = resp.kvs;
        // we don't expect to have multiple values for one key
        assert!(kvs.len() <= 1, "multiple values for one key");
        if let Some(kv) = kvs.get(0) {
            let value = kv.value.clone();
            // update the version_map
            self.version_map.insert(key.clone(), kv.version);
            Ok(Some(serde_json::from_slice(&value)?))
        } else {
            // update the version_map
            self.version_map.insert(key, 0);
            Ok(None)
        }
    }

    fn set(&mut self, key: &KeyType, value: &ValueType) {
        let key = key.to_string_key().into_bytes();
        // Because the ValueType derives the serde::Serialize
        // This unwrap will not panic.
        let value = serde_json::to_vec(value)
            .unwrap_or_else(|value| panic!("failed to serialize value to json,value = {value:?}"));
        // Set same key twice in the same transaction is not allowed.
        debug_assert!(
            self.buffer.get(&key).is_none(),
            "set the key={key:?} twice in the same transaction"
        );
        self.buffer.insert(key, Some(value));
    }

    fn delete(&mut self, key: &KeyType) {
        let key = key.to_string_key().into_bytes();
        self.buffer.insert(key, None);
    }

    async fn commit(&mut self) -> DatenLordResult<bool> {
        let _timer = KV_METRICS.start_kv_operation_timer("txn");

        if self.version_map.is_empty() && self.buffer.is_empty() {
            return Ok(true);
        }
       
        let resp = self
            .client
            .kv_client()
            .txn(
                TxnRequest::new()
                    .when(
                        self.version_map
                            .iter()
                            .map(|(key, version)| {
                                Compare::version(key.clone(), CompareResult::Equal, *version)
                            })
                            .collect::<Vec<Compare>>(),
                    )
                    .and_then(
                        self.buffer
                            .iter()
                            .map(|(key, value)| {
                                if let Some(ref value) = *value {
                                    TxnOp::put(PutRequest::new(key.clone(), value.clone()))         //key.clone(), value.clone(), None)
                                } else {
                                    TxnOp::delete(DeleteRangeRequest::new(key.clone()))    //key.clone(), None)
                                }
                            })
                            .collect::<Vec<TxnOp>>(),
                    ),
            )
            .await
            .with_context(|| "failed to do txn operation at `MetaTxn::commit`".to_owned())?;
        Ok(resp.succeeded)
        }
}