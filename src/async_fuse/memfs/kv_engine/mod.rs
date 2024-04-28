use core::fmt::Debug;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::common::async_fuse_error::KVEngineError;
use crate::common::error::{DatenLordError, DatenLordResult};

/// The `KVEngineType` is used to provide support for metadata.
/// We use this alias to avoid generic type.
pub type KVEngineType = etcd_impl::EtcdKVEngine;

/// The etcd implementation of `KVEngine` and `MetaTxn`
pub mod etcd_impl;
/// The `kv_utils` is used to provide some common functions for `KVEngine`
pub mod kv_utils;

/// The key type api
pub mod key_type;
/// The value type api
pub mod value_type;

pub use key_type::{KeyType, LockKeyType};
pub use value_type::ValueType;
/// The Txn is used to provide support for metadata.
#[async_trait]
pub trait MetaTxn: Send {
    /// Get the value by the key.
    /// Notice : do not get the same key twice in one transaction.
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

/// The option of 'set' operation
/// Currently support 'lease' and `prev_kv`
/// `lease` is used to set the lease of the key
/// `prev_kv` is used to return the previous key-value pair
#[allow(dead_code)]
#[derive(Debug, Eq, PartialEq)]
pub struct SetOption {
    /// The lease of the key
    pub(crate) lease: Option<i64>,
    /// Whether to return the previous key-value pair
    pub(crate) prev_kv: bool,
}

impl SetOption {
    #[allow(dead_code)]
    #[must_use]
    /// Create a new `SetOption`
    /// Default lease is None, `prev_kv` is false
    fn new() -> Self {
        Self {
            lease: None,
            prev_kv: false,
        }
    }

    #[allow(dead_code)]
    #[must_use]
    /// Set the lease of the key
    fn with_lease(mut self, lease: i64) -> Self {
        self.lease = Some(lease);
        self
    }

    #[allow(dead_code)]
    #[must_use]
    /// Set whether to return the previous key-value pair
    fn with_prev_kv(mut self) -> Self {
        self.prev_kv = true;
        self
    }
}

/// The option of 'delete' operation
/// Currently support `prev_kv` and `range_end`
/// `prev_kv` is used to return the previous key-value pair
/// `range_end` is used to delete all keys in the range [key, `range_end`)
#[allow(dead_code)]
#[derive(Debug, Eq, PartialEq)]
pub struct DeleteOption {
    /// Whether to return the previous key-value pair
    pub(crate) prev_kv: bool,
    /// The range end of the delete operation
    pub(crate) range_end: Option<Vec<u8>>,
}

impl DeleteOption {
    #[allow(dead_code)]
    #[must_use]
    /// Create a new `DeleteOption`
    fn new() -> Self {
        Self {
            prev_kv: false,
            range_end: None,
        }
    }

    #[allow(dead_code)]
    #[must_use]
    /// Set whether to return the previous key-value pair
    fn with_prev_kv(mut self) -> Self {
        self.prev_kv = true;
        self
    }

    #[allow(dead_code)]
    #[must_use]
    /// Set the range end of the delete operation
    fn with_range_end(mut self, range_end: Vec<u8>) -> Self {
        self.range_end = Some(range_end);
        self
    }

    /// Delete all keys
    #[allow(dead_code)]
    #[must_use]
    /// Set the range end of the delete operation
    fn with_all_keys(mut self) -> Self {
        self.range_end = Some(vec![0xff]);
        self
    }
}

/// `KeyRange` is an abstraction for describing etcd key of various types.
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
#[allow(dead_code)]
/// Key range builder.
pub struct KeyRange {
    /// The starting key of the range.
    pub(crate) key: Vec<u8>,

    /// The ending key of the range.
    pub(crate) range_end: Vec<u8>,

    /// A flag that, when set to true, causes the `build` method to treat `key`
    /// as a prefix.
    pub(crate) with_prefix: bool,

    /// A flag that, when set to true, causes the `build` method to include all
    /// keys in the range.
    pub(crate) with_all_keys: bool,
}

impl KeyRange {
    /// Creates a new `KeyRange` builder.
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        KeyRange {
            key: Vec::new(),
            range_end: Vec::new(),
            with_prefix: false,
            with_all_keys: false,
        }
    }

    /// Sets key.
    #[inline]
    pub fn with_key(&mut self, key: impl Into<Vec<u8>>) {
        self.key = key.into();
    }

    /// Specifies the range end.
    /// `end_key` must be lexicographically greater than start key.
    #[inline]
    pub fn with_range(&mut self, end_key: impl Into<Vec<u8>>) {
        self.range_end = end_key.into();
        self.with_prefix = false;
        self.with_all_keys = false;
    }

    /// Sets all keys prefixed with key.
    #[inline]
    pub fn with_prefix(&mut self) {
        self.with_prefix = true;
        self.with_all_keys = false;
    }

    /// Sets all keys.
    #[inline]
    pub fn with_all_keys(&mut self) {
        self.with_all_keys = true;
        self.with_prefix = false;
    }
}

/// To support different K/V storage engines, we need to a trait to abstract the
/// K/V storage engine.
#[async_trait]
pub trait KVEngine: Send + Sync + Debug + Sized {
    /// The session type for keep alive
    type Session: Send + Sync;

    /// create a new KVEngine.
    async fn new(end_points: Vec<String>) -> DatenLordResult<Self>;
    /// Create a new transaction.
    async fn new_meta_txn(&self) -> Box<dyn MetaTxn + Send>;
    /// Distribute lock - lock
    async fn lock(&self, key: &LockKeyType, timeout: Duration) -> DatenLordResult<Vec<u8>>;
    /// Distribute lock - unlock
    async fn unlock(&self, key: Vec<u8>) -> DatenLordResult<()>;
    /// Get the value by the key.
    async fn get(&self, key: &KeyType) -> DatenLordResult<Option<ValueType>>;
    /// Set the value by the key.
    async fn set(
        &self,
        key: &KeyType,
        value: &ValueType,
        option: Option<SetOption>,
    ) -> DatenLordResult<Option<ValueType>>;
    /// Delete the kv pair by the key.
    async fn delete(
        &self,
        key: &KeyType,
        option: Option<DeleteOption>,
    ) -> DatenLordResult<Option<ValueType>>;

    /// Lease grant
    async fn lease_grant(&self, ttl: i64) -> DatenLordResult<i64>;

    /// Campaign leader
    async fn campaign(&self, key: &KeyType, val: &ValueType, lease_id: i64) -> bool;

    /// Lease keep alive
    async fn create_keep_alive_session(&self, lease_id: i64, ttl: i64) -> Arc<Self::Session>;

    /// Range get, return all key-value pairs start with prefix
    async fn range(&self, prefix: &KeyType) -> DatenLordResult<Vec<ValueType>>;

    /// Watch the key, return a receiver to receive the value
    async fn watch(
        &self,
        key: &KeyType,
    ) -> DatenLordResult<Arc<mpsc::Receiver<(String, Option<ValueType>)>>>;
}

/// The version of the key.
type KvVersion = i64;

/// Convert u64 seceond to i64
fn conv_u64_sec_2_i64(sec: u64) -> i64 {
    sec.try_into()
        .unwrap_or_else(|e| panic!("ttl timeout_sec should < MAX_I64, err:{e}"))
}

/// Fix ttl, ttl should be > 0
fn check_ttl(sec: i64) -> DatenLordResult<i64> {
    if sec <= 0 {
        Err(DatenLordError::KVEngineErr {
            source: KVEngineError::WrongTimeoutArg,
            context: vec!["Timeout arg for kv should be >= 1 second".to_owned()],
        })
    } else {
        Ok(sec)
    }
}

/// break `retry_txn` result
pub const RETRY_TXN_BREAK: DatenLordResult<bool> = Ok(true);

#[macro_export]
/// the logic should return a tuple (txn commit/cancel result, res)
/// For example: `Ok((txn.commit().await,...))`
/// For example: `Ok((BREAK_RETRY_TXN,...))`
///
/// Returns the result and transaction retry times.
macro_rules! retry_txn {
    ($retry_num:expr, $logic:block) => {{
        use $crate::common::error::DatenLordError;

        let mut result = Err(DatenLordError::TransactionRetryLimitExceededErr {
            context: vec!["Transaction retry failed due to exceeding the retry limit".to_owned()],
        });
        let mut attempts: u32 = 0;

        while attempts < $retry_num {
            let (commit_res, res) = { $logic };
            match commit_res {
                Ok(commit_success) => {
                    if commit_success {
                        result = Ok(res);
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
        (result, attempts)
    }};
}
