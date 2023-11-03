use core::fmt::Debug;
use std::fmt;
use std::fmt::Display;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::s3_node::S3Node;
use super::s3_wrapper::S3BackEnd;
use super::{INum, S3MetaData};
use crate::async_fuse::memfs::dist::id_alloc::IdType;
use crate::common::async_fuse_error::KVEngineError;
use crate::common::error::{DatenLordError, DatenLordResult};

/// The `KVEngineType` is used to provide support for metadata.
/// We use this alias to avoid tem
pub type KVEngineType = etcd_impl::EtcdKVEngine;

use super::serial::{SerialDirEntry, SerialFileAttr, SerialNode};

/// The etcd implementation of `KVEngine` and `MetaTxn`
pub mod etcd_impl;
/// The `kv_utils` is used to provide some common functions for `KVEngine`
pub mod kv_utils;

/// The `ValueType` is used to provide support for metadata.
///
/// The variants `DirEntry`, `INum` and `Attr` are not used currently,
/// but preserved for the future.
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
    /// Raw value
    Raw(Vec<u8>),
    /// String value
    String(String),
}

impl ValueType {
    #[allow(dead_code, clippy::wildcard_enum_match_arm)] // Allow wildcard because there should be only one enum branch matches one specific type.
    /// Turn the `ValueType` into `SerialNode` then into `S3Node`.
    /// # Panics
    /// Panics if `ValueType` is not `ValueType::Node`.
    pub async fn into_s3_node<S: S3BackEnd + Send + Sync + 'static>(
        self,
        meta: &S3MetaData<S>,
    ) -> DatenLordResult<S3Node<S>> {
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

    /// Turn the `ValueType` into `String`
    /// # Panics
    /// Panics if `ValueType` is not `ValueType::String`.
    #[allow(clippy::wildcard_enum_match_arm)] // Allow wildcard because there should be only one enum branch matches one specific type.
    #[must_use]
    pub fn into_string(self) -> String {
        match self {
            ValueType::String(string) => string,
            _ => panic!("expect ValueType::String but get {self:?}"),
        }
    }

    /// Turn the `ValueType` into `Raw`
    /// # Panics
    /// Panics if `ValueType` is not `ValueType::Raw`.
    #[allow(clippy::wildcard_enum_match_arm)] // Allow wildcard because there should be only one enum branch matches one specific type.
    #[must_use]
    pub fn into_raw(self) -> Vec<u8> {
        match self {
            ValueType::Raw(raw) => raw,
            _ => panic!("expect ValueType::Raw but get {self:?}"),
        }
    }
}

/// The `KeyType` is used to locate the value in the distributed K/V storage.
/// Every key is prefixed with a string to indicate the type of the value.
/// If you want to add a new type of value, you need to add a new variant to the
/// enum. And you need to add a new match arm to the `get_key` function , make
/// sure the key is unique.
#[derive(Debug, Eq, PartialEq)]
pub enum KeyType {
    /// INum -> SerailNode
    INum2Node(INum),
    /// INum -> DirEntry
    INum2DirEntry(INum),
    /// INum -> SerialFileAttr
    INum2Attr(INum),
    /// IdAllocator value key
    IdAllocatorValue(IdType),
    /// Node ip and port info : node_id -> "{node_ipaddr}:{port}"
    /// The corresponding value type is ValueType::String
    NodeIpPort(String),
    /// Volume information
    /// The corresponding value type is ValueType::RawData
    VolumeInfo(String),
    /// Node list
    /// The corresponding value type is ValueType::RawData
    FileNodeList(INum),
    /// Just a string key for testing the KVEngine.
    #[cfg(test)]
    String(String),
}

// ::<KeyType>::get() -> ValueType
/// Lock key type the memfs used.
#[derive(Debug, Eq, PartialEq)]
#[allow(variant_size_differences)]
pub enum LockKeyType {
    /// IdAllocator lock key
    IdAllocatorLock(IdType),
    /// ETCD volume information lock
    VolumeInfoLock,
    /// ETCD file node list lock
    FileNodeListLock(INum),
}

impl Display for KeyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            KeyType::INum2Node(ref i) => write!(f, "INum2Node{{i: {i}}}"),
            KeyType::INum2DirEntry(ref i) => write!(f, "INum2DirEntry{{i: {i}}}"),
            #[cfg(test)]
            KeyType::String(ref s) => write!(f, "String{{s: {s}}}"),
            KeyType::INum2Attr(ref i) => write!(f, "INum2Attr{{i: {i}}}"),
            KeyType::IdAllocatorValue(ref id_type) => {
                write!(f, "IdAllocatorValue{{unique_id: {id_type:?}}}")
            }
            KeyType::NodeIpPort(ref s) => write!(f, "NodeIpPort{{s: {s}}}"),
            KeyType::VolumeInfo(ref s) => write!(f, "VolumeInfo{{s: {s}}}"),
            KeyType::FileNodeList(ref s) => write!(f, "FileNodeList{{s: {s:?}}}"),
        }
    }
}

impl Display for LockKeyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            LockKeyType::IdAllocatorLock(ref id_type) => {
                write!(f, "IdAllocatorLock{{unique_id: {id_type:?}}}")
            }
            LockKeyType::VolumeInfoLock => {
                write!(f, "LockKeyType::VolumeInfoLock ")
            }
            LockKeyType::FileNodeListLock(ref file_name) => {
                write!(f, "LockKeyType::FileNodeList {{file_name: {file_name:?}}}")
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
            #[cfg(test)]
            KeyType::String(ref s) => serialize_key(2, s),
            KeyType::INum2Attr(ref i) => serialize_key(3, i),
            KeyType::IdAllocatorValue(ref id_type) => serialize_key(4, &id_type.to_unique_id()),
            KeyType::NodeIpPort(ref s) => serialize_key(6, s),
            KeyType::VolumeInfo(ref s) => serialize_key(8, s),
            KeyType::FileNodeList(ref s) => serialize_key(10, s),
        }
    }
}

impl LockKeyType {
    /// Get the key in vec bytes.
    fn get_key(&self) -> Vec<u8> {
        match *self {
            LockKeyType::IdAllocatorLock(ref id_type) => {
                serialize_key(100, &id_type.to_unique_id())
            }
            LockKeyType::VolumeInfoLock => serialize_key(101, &0_i32),
            LockKeyType::FileNodeListLock(ref file_name) => serialize_key(102, file_name),
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

    /// Range query
    async fn range(&self, key_range: KeyRange) -> DatenLordResult<Vec<(Vec<u8>, Vec<u8>)>>;
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

#[allow(unused_macros, clippy::crate_in_macro_def)]
#[macro_export]
/// the logic should return a tuple (txn commit/cancel result, res)
/// For example: `Ok((txn.commit().await,...))`
/// For example: `Ok((BREAK_RETRY_TXN,...))`
macro_rules! retry_txn {
    ($retry_num:expr, $logic:block) => {{
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
