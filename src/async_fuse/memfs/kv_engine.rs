use super::INum;
use async_trait::async_trait;
use core::fmt::Debug;
use datenlord::common::error::{Context, DatenLordResult};
use etcd_client::TxnCmp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

use std::sync::Arc;

use super::serial::{SerialDirEntry, SerialFileAttr, SerialNode};

/// The `ValueType` is used to provide support for metadata.
#[derive(Serialize, Deserialize, Debug)]
pub enum ValueType {
    /// SerialNode
    Node(SerialNode),
    /// SerailDirEntry
    DirEntry(SerialDirEntry),
    /// INum
    INum(INum),
    ///
    Attr(SerialFileAttr),
}

/// The `KeyType` is used to locate the value in the distributed K/V storage.
/// Every key is prefixed with a string to indicate the type of the value.
/// If you want to add a new type of value, you need to add a new variant to the enum.
/// And you need to add a new match arm to the `get_key` function , make sure the key is unique.
#[allow(dead_code)]
pub enum KeyType {
    /// INum -> SerailNode
    INum2Node(INum),
    /// INum -> DirEntry
    INum2DirEntry(INum),
    /// Path -> Inum
    Path2INum(String),
    /// INum -> SerailFileAttr
    INum2Attr(INum),
}

impl KeyType {
    /// Get the key in bytes.
    fn get_key(&self) -> Vec<u8> {
        match *self {
            KeyType::INum2Node(ref i) => format!("I{i}").into_bytes(),
            KeyType::INum2DirEntry(ref i) => format!("D{i}").into_bytes(),
            KeyType::Path2INum(ref p) => format!("P{p}").into_bytes(),
            KeyType::INum2Attr(ref i) => format!("A{i}").into_bytes(),
        }
    }
}

/// The Txn is used to provide support for metadata.
#[async_trait]
pub trait MetaTxn {
    /// Get the value by the key.
    /// Notice : do not get the same key twice in one transaction.
    async fn get(&mut self, key: &KeyType) -> DatenLordResult<Option<ValueType>>;
    /// Set the value by the key.
    async fn set(&mut self, key: &KeyType, value: &ValueType) -> DatenLordResult<()>;
    /// Delete the value by the key.
    async fn delete(&mut self, key: &KeyType) -> DatenLordResult<()>;
    /// Commit the transaction.
    /// Only when commit is called, the write operations will be executed.
    /// If the commit is successful, return true, else return false.
    async fn commit(&mut self) -> DatenLordResult<bool>;
}

/// To support different K/V storage engines, we need to a trait to abstract the K/V storage engine.
#[async_trait]
pub trait KVEngine: Send + Sync + Debug {
    /// Create a new transaction.
    async fn new_meta_txn(&self) -> Box<dyn MetaTxn + Send>;
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

    async fn set(&mut self, key: &KeyType, value: &ValueType) -> DatenLordResult<()> {
        let key = key.get_key();
        let value = serde_json::to_vec(value)?;
        self.buffer.insert(key, Some(value));
        Ok(())
    }

    async fn delete(&mut self, key: &KeyType) -> DatenLordResult<()> {
        let key = key.get_key();
        self.buffer.insert(key, None);
        Ok(())
    }

    async fn commit(&mut self) -> DatenLordResult<bool> {
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
    /// Create a new etcd kv engine.
    pub fn new_kv_engine(etcd_client: etcd_client::Client) -> Arc<dyn KVEngine> {
        Arc::new(EtcdKVEngine {
            client: etcd_client,
        })
    }
}

#[allow(unused_macros)]
/// add comment about why use macro here not closure
macro_rules! retry_txn {
    ($retry_num : expr ,$logic: block) => {{
        // result is Option<DatenLordResult<()>>
        let mut result: DatenLordResult<()> =
            Err(DatenLordError::TransactionRetryLimitExceededErr {
                context: vec![
                    "Transaction retry failed due to exceeding the retry limit".to_owned()
                ],
            });
        let mut attempts: u32 = 0;

        while attempts < $retry_num {
            match { $logic } {
                Ok(value) => {
                    if value {
                        result = Ok(());
                        break;
                    }
                }
                Err(err) => {
                    result = Err(err);
                    break;
                }
            }
            attempts = attempts.wrapping_add(1);
        }
        result
    }};
}

#[async_trait]
impl KVEngine for EtcdKVEngine {
    async fn new_meta_txn(&self) -> Box<dyn MetaTxn + Send> {
        Box::new(EtcdTxn::new(self.client.clone()))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod test {

    use super::*;

    const ETCD_ADDRESS: &str = "localhost:2379";

    #[tokio::test]
    async fn test_connect_local() {
        let client = EtcdKVEngine::new_for_local_test(vec![ETCD_ADDRESS.to_owned()])
            .await
            .unwrap();
        let mut txn = client.new_meta_txn().await;
        let key = KeyType::Path2INum(String::from("/"));
        let value = ValueType::INum(12);
        txn.set(&key, &value).await.unwrap();
        txn.commit().await.unwrap();
        let mut txn = client.new_meta_txn().await;
        let value = txn.get(&key).await.unwrap();
        if let Some(ValueType::INum(num)) = value {
            assert_eq!(num, 12);
        } else {
            panic!("wrong value type {value:?}");
        }
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
        first_txn.set(&key1, &value1).await.unwrap();
        first_txn.set(&key2, &value2).await.unwrap();
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
                second_txn.commit().await
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
            third_txn.set(&key1, &value1).await.unwrap();
            third_txn.commit().await.unwrap();
            // send the signal to the second txn
            second_step_tx.send(()).await.unwrap();
        });
        second_handle.await.unwrap();
        third_handle.await.unwrap();
    }

    use datenlord::common::error::DatenLordError;

    #[tokio::test]
    async fn test_txn_retry() {
        let result = retry_txn!(3, {
            let client = EtcdKVEngine::new_for_local_test(vec![ETCD_ADDRESS.to_owned()])
                .await
                .unwrap();
            let mut txn = client.new_meta_txn().await;
            let key = KeyType::Path2INum(String::from("/"));
            let _ = txn.get(&key).await.unwrap();
            txn.commit().await
        });
        result.unwrap();
    }
}
