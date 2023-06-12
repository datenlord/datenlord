use super::{s3_node::S3Node, s3_wrapper::S3BackEnd, INum, S3MetaData};
use crate::common::error::{Context, DatenLordResult};
use async_trait::async_trait;
use core::fmt::Debug;
use etcd_client::TxnCmp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;
use std::{fmt, time::Duration};

/// The `KVEngineType` is used to provide support for metadata.
/// We use this alias to avoid tem
pub type KVEngineType = EtcdKVEngine;

use std::sync::Arc;

use super::serial::{SerialDirEntry, SerialFileAttr, SerialNode};

/// The `ValueType` is used to provide support for metadata.
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum ValueType {
    /// SerialNode
    Node(SerialNode),
    /// SerialDirEntry
    DirEntry(SerialDirEntry),
    /// INum
    INum(INum),
    /// FileAttr
    Attr(SerialFileAttr),
    /// Next id allocate range begin
    NextIdAllocateRangeBegin(u64),
}

impl ValueType {
    #[allow(dead_code, clippy::wildcard_enum_match_arm)] // Allow wildcard because there should be only one enum branch matches one specific type.
    /// Turn the `ValueType` into `SerialNode` then into `S3Node`.
    /// # Panics
    /// Panics if `ValueType` is not `ValueType::Node`.
    pub async fn into_s3_node<S: S3BackEnd + Send + Sync + 'static>(
        self,
        meta: &S3MetaData<S>,
    ) -> S3Node<S> {
        match self {
            ValueType::Node(node) => S3Node::from_serial_node(node, meta).await,
            _ => {
                panic!("expect ValueType::Node but get {self:?}");
            }
        }
    }

    /// Turn the `ValueType` into `NextIdAllocateRangeBegin`.
    /// # Panics
    /// Panics if `ValueType` is not `ValueType::NextIdAllocateRangeBegin`.
    #[allow(clippy::wildcard_enum_match_arm)] // Allow wildcard because there should be only one enum branch matches one specific type.
    #[must_use]
    pub fn into_next_id_allocate_range_begin(self) -> u64 {
        match self {
            ValueType::NextIdAllocateRangeBegin(begin) => begin,
            _ => panic!("expect ValueType::NextIdAllocateRangeBegin but get {self:?}"),
        }
    }

    /// Turn the `ValueType` into `INum`.
    /// # Panics
    /// Panics if `ValueType` is not `ValueType::INum`.
    #[allow(clippy::wildcard_enum_match_arm)] // Allow wildcard because there should be only one enum branch matches one specific type.
    #[must_use]
    pub fn into_inum(self) -> INum {
        match self {
            ValueType::INum(inum) => inum,
            _ => panic!("expect ValueType::INum but get {self:?}"),
        }
    }
}

/// The `KeyType` is used to locate the value in the distributed K/V storage.
/// Every key is prefixed with a string to indicate the type of the value.
/// If you want to add a new type of value, you need to add a new variant to the enum.
/// And you need to add a new match arm to the `get_key` function , make sure the key is unique.
#[allow(dead_code)]
#[derive(Debug, Eq, PartialEq)]
pub enum KeyType {
    /// INum -> SerailNode
    INum2Node(INum),
    /// INum -> DirEntry
    INum2DirEntry(INum),
    /// Path -> Inum
    Path2INum(String),
    /// INum -> SerialFileAttr
    INum2Attr(INum),
    /// IdAllocator value key
    IdAllocatorValue {
        /// the prefix of the key for different id type
        unique_id: u8,
    },
}

/// Lock key type the memfs used.
#[derive(Debug, Eq, PartialEq)]
pub enum LockKeyType {
    /// IdAllocator lock key
    IdAllocatorLock {
        /// the prefix of the key for different id type
        unique_id: u8,
    },
}

impl Display for KeyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            KeyType::INum2Node(ref i) => write!(f, "INum2Node{{i: {i}}}"),
            KeyType::INum2DirEntry(ref i) => write!(f, "INum2DirEntry{{i: {i}}}"),
            KeyType::Path2INum(ref p) => write!(f, "Path2INum{{p: {p}}}"),
            KeyType::INum2Attr(ref i) => write!(f, "INum2Attr{{i: {i}}}"),
            KeyType::IdAllocatorValue { unique_id } => {
                write!(f, "IdAllocatorValue{{unique_id: {unique_id}}}")
            }
        }
    }
}

impl Display for LockKeyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            LockKeyType::IdAllocatorLock { unique_id } => {
                write!(f, "IdAllocatorLock{{unique_id: {unique_id}}}")
            }
        }
    }
}

/// Get key serialized data with prefix and key.
#[inline]
fn serialize_key<K: ?Sized + Serialize>(key_prefix: u16, key: &K) -> Vec<u8> {
    let mut v = vec![];
    bincode::serialize_into(&mut v, &key_prefix).unwrap_or_else(|e| {
        panic!("serialize key prefix failed, err:{e}");
    });
    assert_eq!(v.len(), 2);
    bincode::serialize_into(&mut v, key).unwrap_or_else(|e| {
        panic!("serialize key failed, err:{e}");
    });

    v
}

impl KeyType {
    /// Get the key in bytes.
    #[must_use]
    pub fn get_key(&self) -> Vec<u8> {
        match *self {
            KeyType::INum2Node(ref i) => serialize_key(0, i),
            KeyType::INum2DirEntry(ref i) => serialize_key(1, i),
            KeyType::Path2INum(ref p) => serialize_key(2, p),
            KeyType::INum2Attr(ref i) => serialize_key(3, i),
            KeyType::IdAllocatorValue { unique_id } => serialize_key(4, &unique_id),
        }
    }
}

impl LockKeyType {
    /// Get the key in vec bytes.
    fn get_key(&self) -> Vec<u8> {
        match *self {
            LockKeyType::IdAllocatorLock { unique_id } => serialize_key(100, &unique_id),
        }
    }
}

/// The Txn is used to provide support for metadata.
#[async_trait]
pub trait MetaTxn {
    /// Get the value by the key.
    /// Notice : do not get the same key twice in one transaction.
    #[must_use]
    async fn get(&mut self, key: &KeyType) -> DatenLordResult<Option<ValueType>>;
    /// Set the value by the key.
    fn set(&mut self, key: &KeyType, value: &ValueType);
    /// Delete the value by the key.
    fn delete(&mut self, key: &KeyType);
    /// Commit the transaction.
    /// Only when commit is called, the write operations will be executed.
    /// If the commit is successful, return true, else return false.
    async fn commit(&mut self) -> DatenLordResult<bool>;
}

/// To support different K/V storage engines, we need to a trait to abstract the K/V storage engine.
#[async_trait]
pub trait KVEngine: Send + Sync + Debug {
    /// create a new KVEngine.
    fn new(etcd_client: etcd_client::Client) -> Self;
    /// Create a new transaction.
    async fn new_meta_txn(&self) -> Box<dyn MetaTxn + Send>;
    /// Distribute lock - lock
    async fn lock(&self, key: &LockKeyType, timeout: Duration) -> DatenLordResult<()>;
    /// Distribute lock - unlock
    async fn unlock(&self, key: &LockKeyType) -> DatenLordResult<()>;
    /// Get the value by the key.
    async fn get(&self, key: &KeyType) -> DatenLordResult<Option<ValueType>>;
    /// Set the value by the key.
    async fn set(&self, key: &KeyType, value: &ValueType) -> DatenLordResult<Option<ValueType>>;
    /// Delete the kv pair by the key.
    async fn delete(&self, key: &KeyType) -> DatenLordResult<Option<ValueType>>;
}

/// The `etcd`'s transaction impl.
/// The txn won't do anything until commit is called.
/// Write operations are buffered until commit is called.
struct EtcdTxn {
    /// The etcd client.
    client: etcd_client::Client,
    /// The key is the key in bytes, the value is the version of the key.
    version_map: HashMap<Vec<u8>, usize>,
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
        let req = etcd_client::EtcdGetRequest::new(key.clone());
        let mut resp = self
            .client
            .kv()
            .get(req)
            .await
            .with_context(|| format!("failed to get GetResponse from etcd, key={key:?}"))?;
        let kvs = resp.take_kvs();
        // we don't expect to have multiple values for one key
        assert!(kvs.len() <= 1, "multiple values for one key");
        if let Some(kv) = kvs.get(0) {
            let value = kv.value();
            // update the version_map
            self.version_map.insert(key.clone(), kv.version());
            Ok(Some(serde_json::from_slice(value)?))
        } else {
            // update the version_map
            self.version_map.insert(key.clone(), 0);
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
        let mut req: etcd_client::EtcdTxnRequest = etcd_client::EtcdTxnRequest::new();
        // add the version check
        for (key, value) in &self.version_map {
            let version = *value;
            req = req.when_version(
                etcd_client::KeyRange::key(key.clone()),
                TxnCmp::Equal,
                version,
            );
        }
        // add the write operations
        for (key, value) in &self.buffer {
            if let Some(ref value) = *value {
                let put_req = etcd_client::EtcdPutRequest::new(key.clone(), value.clone());
                req = req.and_then(put_req);
            } else {
                let delete_req =
                    etcd_client::EtcdDeleteRequest::new(etcd_client::KeyRange::key(key.clone()));
                req = req.and_then(delete_req);
            }
        }
        let resp = self
            .client
            .kv()
            .txn(req)
            .await
            .with_context(|| "failed to get TxnResponse from etcd".to_owned())?;
        Ok(resp.is_success())
    }
}

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
        let client = etcd_client::Client::connect(etcd_client::ClientConfig::new(
            etcd_address_vec.clone(),
            None,
            64,
            true,
        ))
        .await
        .with_context(|| {
            format!("failed to build etcd client to addresses={etcd_address_vec:?}")
        })?;
        Ok(EtcdKVEngine { client })
    }

    #[allow(dead_code)]
    #[must_use]
    /// Create a new etcd kv engine.
    pub fn new_kv_engine(etcd_client: etcd_client::Client) -> Arc<Self> {
        Arc::new(EtcdKVEngine {
            client: etcd_client,
        })
    }
}

/// break `retry_txn` result
pub const RETRY_TXN_BREAK: DatenLordResult<bool> = Ok(true);

#[allow(unused_macros, clippy::crate_in_macro_def)]
#[macro_export]
/// the logic should return a tuple (txn commit/cancel result, res)
/// For example: `Ok((txn.commit().await,...))`
/// For example: `Ok((BREAK_RETRY_TXN,...))`
macro_rules! retry_txn {
    ($retry_num : expr ,$logic: block) => {{
        use crate::common::error::DatenLordError;

        let mut result = Err(DatenLordError::TransactionRetryLimitExceededErr {
            context: vec!["Transaction retry failed due to exceeding the retry limit".to_owned()],
        });
        let mut attempts: u32 = 0;

        while attempts < $retry_num {
            attempts = attempts.wrapping_add(1);
            let (commit_res, res) = { $logic };
            match commit_res {
                Ok(commit_res) => {
                    if commit_res {
                        result = Ok(res);
                        break;
                    }
                }
                Err(err) => {
                    result = Err(err);
                    break;
                }
            }
        }
        result
    }};
}

#[async_trait]
impl KVEngine for EtcdKVEngine {
    #[must_use]
    fn new(etcd_client: etcd_client::Client) -> Self {
        EtcdKVEngine {
            client: etcd_client,
        }
    }
    async fn new_meta_txn(&self) -> Box<dyn MetaTxn + Send> {
        Box::new(EtcdTxn::new(self.client.clone()))
    }
    /// Distribute lock - lock
    async fn lock(&self, key: &LockKeyType, timeout: Duration) -> DatenLordResult<()> {
        let lease_id = self
            .client
            .lease()
            .grant(etcd_client::EtcdLeaseGrantRequest::new(timeout))
            .await
            .with_context(|| {
                format!(
                    "failed to get LeaseGrantResponse from etcd, the timeout = {} ms",
                    timeout.as_millis()
                )
            })?
            .id();

        let _ = self
            .client
            .lock()
            .lock(etcd_client::EtcdLockRequest::new(key.get_key(), lease_id))
            .await
            .with_context(|| {
                format!(
                    "failed to get LockResponse from etcd, the lease id={lease_id}, key = {key}"
                )
            })?;

        Ok(())
    }
    /// Distribute lock - unlock
    async fn unlock(&self, key: &LockKeyType) -> DatenLordResult<()> {
        self.client
            .lock()
            .unlock(etcd_client::EtcdUnlockRequest::new(key.get_key()))
            .await
            .with_context(|| {
                format!("failed to get UnlockResponse from etcd, the lease key={key:?}",)
            })?;

        Ok(())
    }

    /// Get the value by the key.
    async fn get(&self, key: &KeyType) -> DatenLordResult<Option<ValueType>> {
        let req = etcd_client::EtcdRangeRequest::new(etcd_client::KeyRange::key(key.get_key()));
        let mut resp = self.client.kv().range(req).await.with_context(|| {
            format!("failed to get RangeResponse of one key-value pair from etcd, the key={key:?}")
        })?;

        let kvs = resp.take_kvs();
        match kvs.get(0) {
            Some(kv) => Ok(Some(serde_json::from_slice::<ValueType>(kv.value()).with_context(||{
                "failed to deserialize value from bytes, KVEngine's value supposed to be `ValueType`".to_owned()
            })?)),
            None => Ok(None),
        }
    }
    /// Set the value by the key.
    async fn set(&self, key: &KeyType, value: &ValueType) -> DatenLordResult<Option<ValueType>> {
        let serial_value = serde_json::to_vec(value)
            .with_context(|| format!("failed to serialize value={value:?} to bytes"))?;
        let mut req = etcd_client::EtcdPutRequest::new(key.get_key(), serial_value);
        req.set_prev_kv(true); // Return previous value
        let mut resp = self
            .client
            .kv()
            .put(req)
            .await
            .with_context(|| format!("failed to get PutResponse from etcd for key={key:?}"))?;
        if let Some(pre_kv) = resp.take_prev_kv() {
            let decoded_value: ValueType = serde_json::from_slice(pre_kv.value())?;
            Ok(Some(decoded_value))
        } else {
            Ok(None)
        }
    }

    /// Delete the kv pair by the key.
    async fn delete(&self, key: &KeyType) -> DatenLordResult<Option<ValueType>> {
        let mut req =
            etcd_client::EtcdDeleteRequest::new(etcd_client::KeyRange::key(key.get_key()));
        req.set_prev_kv(true); // Return previous value
        let mut resp =
            self.client.kv().delete(req).await.with_context(|| {
                format!("failed to get DeleteResponse from etcd for key={key:?}")
            })?;
        if let Some(pre_kv) = resp.take_prev_kvs().into_iter().next() {
            let decoded_value: ValueType = serde_json::from_slice(pre_kv.value())?;
            Ok(Some(decoded_value))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod test {

    use super::*;
    use crate::common::error::DatenLordError;

    const ETCD_ADDRESS: &str = "localhost:2379";

    #[tokio::test]
    async fn test_key_serial() {
        let key = KeyType::INum2Attr(1).get_key();
        assert_eq!(key.len(), 10);
        let key = KeyType::IdAllocatorValue { unique_id: 0 }.get_key();
        assert_eq!(key.len(), 3);
    }

    #[tokio::test]
    async fn test_connect_local() {
        let client = EtcdKVEngine::new_for_local_test(vec![ETCD_ADDRESS.to_owned()])
            .await
            .unwrap();
        // insert a key , and then get it , and then delete it, and then get it again
        let key = KeyType::Path2INum("test_key".to_owned());
        let value = ValueType::INum(123);
        client.set(&key, &value).await.unwrap();
        let get_value = client.get(&key).await.unwrap().unwrap();
        assert_eq!(get_value, value);
        client.delete(&key).await.unwrap();
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
        let key1 = KeyType::Path2INum(String::from("/"));
        let value1 = ValueType::INum(12);
        let key2 = KeyType::Path2INum(String::from("/a"));
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
                let key1 = KeyType::Path2INum(String::from("/"));
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
                let key2 = KeyType::Path2INum(String::from("/a"));
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
            let key1 = KeyType::Path2INum(String::from("/"));
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
            let key = KeyType::Path2INum(String::from("/"));
            let _ = txn.get(&key).await.unwrap();
            (txn.commit().await, ())
        });
        result.unwrap();
    }
}
