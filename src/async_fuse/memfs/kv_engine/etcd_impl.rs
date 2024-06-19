use std::fmt::Debug;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;
use std::time::Duration;
use std::vec;
use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use async_trait::async_trait;
use datenlord::common::task_manager::{TaskName, TASK_MANAGER};
use datenlord::metrics::KV_METRICS;
use etcd_client::{
    Compare, CompareOp, DeleteOptions, GetOptions, LockOptions, PutOptions, Txn, TxnOp,
    TxnOpResponse,
};
use futures::{task, Stream, StreamExt};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use super::{
    check_ttl, conv_u64_sec_2_i64, fmt, DeleteOption, KVEngine, KeyType, KvVersion, LockKeyType,
    MetaTxn, SetOption, ValueType,
};
use crate::common::error::DatenLordResult;

/// Revoke the lease
macro_rules! revoke_lease {
    ($client:expr, $lease_id:expr) => {
        match $client.lease_revoke($lease_id).await {
            Ok(_) => {
                info!("lease revoke success, lease_id={}", $lease_id);
            }
            Err(e) => {
                warn!("failed to revoke lease, error={:?}", e);
            }
        }
    };
}

/// The keepalive session struct
#[derive(Clone)]
pub struct Session {
    /// close_tx is the sender to close the keep alive session
    close_tx: mpsc::Sender<()>,
    /// The session inner
    inner: Arc<SessionInner>,
}

impl Debug for Session {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl Session {
    /// Create a new session
    pub async fn new(
        client: etcd_client::Client,
        ttl: u64,
        lease_id: i64,
    ) -> DatenLordResult<Arc<Self>> {
        let closed = AtomicBool::new(false);
        let inner = Arc::new(SessionInner {
            closed,
            lease_id,
            ttl,
            client,
        });
        let (close_tx, close_rx) = mpsc::channel(1);

        let inner_clone = Arc::clone(&inner);
        TASK_MANAGER
            .spawn(TaskName::EtcdKeepAlive, |token| async move {
                inner_clone.keepalive(close_rx, token).await;
            })
            .await
            .with_context(|| "failed to spawn keep alive session task")?;

        let session = Session { close_tx, inner };
        Ok(Arc::new(session))
    }

    /// Get the lease id
    #[must_use]
    pub fn lease_id(&self) -> i64 {
        self.inner.lease_id
    }

    /// Get the ttl
    #[must_use]
    pub fn ttl(&self) -> u64 {
        self.inner.ttl
    }

    /// Get the `is_closed` flag
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Try to close session
    pub fn close(&self) {
        match self.close_tx.try_send(()) {
            Ok(o) => {
                info!("close the keep alive session, {o:?}");
            }
            Err(e) => {
                error!("failed to close the keep alive session, error={e:?}");
            }
        }
        self.inner.close();
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        // Set the close flag
        info!(
            "drop the keep alive session, lease_id={lease_id}, ttl={ttl}",
            lease_id = self.lease_id(),
            ttl = self.ttl()
        );
        self.close();
    }
}

/// The keepalive session inner struct
pub struct SessionInner {
    /// The closed flag
    closed: AtomicBool,
    /// Current lease id
    lease_id: i64,
    /// The ttl of the lease
    ttl: u64,
    /// The etcd client
    #[cfg_attr(not(debug_assertions), skip)]
    client: etcd_client::Client,
}

impl Debug for SessionInner {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Session")
            .field("lease_id", &self.lease_id)
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl SessionInner {
    /// Get the closed flag
    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Set close flag
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    /// Keep alive the lease
    #[allow(clippy::pattern_type_mismatch)] // Raised by `tokio::select`
    pub async fn keepalive(
        self: Arc<Self>,
        mut close_rx: mpsc::Receiver<()>,
        token: CancellationToken,
    ) {
        let mut interval = tokio::time::interval(Duration::from_secs(self.ttl.div_ceil(3)));

        // Try to clone a client, if the keeper failed, we need to revoke the lease
        let mut client = self.client.clone();
        let lease_id = self.lease_id;
        let (mut keeper, mut lease_keep_alive_stream) =
            match client.lease_keep_alive(lease_id).await {
                Ok((keeper, lease_keep_alive_stream)) => (keeper, lease_keep_alive_stream),
                Err(e) => {
                    error!("failed to keep alive lease, error={e:?}");

                    // revoke the lease
                    revoke_lease!(client, lease_id);
                    self.close();
                    return;
                }
            };

        // Start to keep alive the lease
        loop {
            if self.is_closed() {
                debug!("lease keep alive stream closed by is_closed flag");

                // revoke the lease
                revoke_lease!(client, lease_id);
                return;
            }

            tokio::select! {
                _ = interval.tick() => {
                    // Try to send a keep alive request
                    match keeper.keep_alive().await {
                        Ok(o) => {
                            debug!("keep alive lease, lease_id={lease_id}, {o:?}");
                        }
                        Err(e) => {
                            error!("failed to keep alive lease, error={e:?}");
                            break;
                        }
                    }

                    // Try to parse
                    match lease_keep_alive_stream.message().await {
                        Ok(Some(lease_alive_response)) => {
                            if lease_alive_response.ttl() == 0 {
                                error!("lease keep alive stream closed, because ttl is 0");
                                // revoke the lease
                                revoke_lease!(client, lease_id);
                                break;
                            }
                            continue
                        }
                        Ok(None) => {
                            error!("lease keep alive stream closed, because the sender is closed");
                            // revoke the lease
                            revoke_lease!(client, lease_id);
                            break;
                        }
                        Err(e) => {
                            error!("failed to keep alive lease, error={e:?}");
                            break;
                        }
                    }
                }
                _ = close_rx.recv() => {
                    info!("close the keep alive session, lease_id={lease_id}");
                    // revoke the lease
                    revoke_lease!(client, lease_id);
                    break;
                }
                () = token.cancelled() => {
                    debug!("lease keep alive stream closed by token canceled");
                    // revoke the lease
                    revoke_lease!(client, lease_id);
                    break;
                }
            }
        }

        // Make sure the lease is revoked
        self.close();
    }
}

/// The kvengine watch stream for etcd
/// Try to receice message from etcd, and return key and value for each event,
/// Wrap the etcd watch stream to support the `KVEngine` trait.
#[derive(Debug)]
pub struct KVEngineWatchStream {
    /// The etcd client, used to cancel the watch stream
    watcher: etcd_client::Watcher,
    /// The etcd watch stream
    watch_stream: etcd_client::WatchStream,
}

impl KVEngineWatchStream {
    /// Create a new watch stream
    #[must_use]
    pub fn new(watcher: etcd_client::Watcher, watch_stream: etcd_client::WatchStream) -> Self {
        Self {
            watcher,
            watch_stream,
        }
    }

    /// Cancel the watch stream, it will take over the ownership of the watch stream
    #[inline]
    pub async fn cancel(mut self) -> DatenLordResult<()> {
        match self.watcher.cancel().await {
            Ok(o) => {
                debug!("etcd watcher task canceled, {o:?}");
                Ok(())
            }
            Err(e) => {
                error!("failed to cancel etcd watcher, error={e:?}");
                Err(crate::common::error::DatenLordError::EtcdClientErr {
                    source: e,
                    context: vec!["failed to cancel etcd watcher".to_owned()],
                })
            }
        }
    }
}

// This impl is for WatchStream from etcd_client, does testing for the watch stream.
impl Stream for KVEngineWatchStream {
    type Item = DatenLordResult<(String, Option<ValueType>)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Option<Self::Item>> {
        self.get_mut()
            .watch_stream
            .poll_next_unpin(cx)
            .map(|t| match t {
                Some(Ok(resp)) => {
                    let mut result = None;
                    for event in resp.events() {
                        // Get event data
                        if let Some(kv) = event.kv() {
                            // Get key and value
                            let item_key = match kv.key_str() {
                                Ok(key) => key.to_owned(),
                                Err(e) => {
                                    error!("failed to get key from etcd watch event, error={e:?}");
                                    continue;
                                }
                            };
                            match event.event_type() {
                                etcd_client::EventType::Put => {
                                    info!("put event, key={item_key}");
                                    let item_value = match serde_json::from_slice(kv.value()) {
                                        Ok(value) => value,
                                        Err(e) => {
                                            error!("failed to deserialize value from etcd watch event, error={e:?}");
                                            continue;
                                        }
                                    };
                                    result = Some(Ok((item_key, Some(item_value))));
                                }
                                etcd_client::EventType::Delete => {
                                    info!("delete event, key={item_key}");
                                    result = Some(Ok((item_key, None)));
                                }
                            }
                        }
                    }
                    result
                },
                // TODO: Cancel watch when meet an error.
                Some(Err(e)) => Some(Err(From::from(e))),
                None => None,
            })
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
        let client = etcd_client::Client::connect(etcd_address_vec.clone(), None)
            .await
            .with_context(|| {
                format!("failed to connect to etcd, the etcd address={etcd_address_vec:?}")
            })?;
        Ok(EtcdKVEngine { client })
    }

    /// Get all key/value pairs with the given prefix.
    async fn range_raw_key(&self, prefix: impl Into<Vec<u8>>) -> DatenLordResult<Vec<ValueType>> {
        let mut client = self.client.clone();
        let option = Some(GetOptions::new().with_prefix());
        let resp = client
            .get(prefix, option)
            .await
            .with_context(|| "failed to get from etcd engine".to_owned())?;
        let kvs = resp.kvs();
        let mut result = Vec::new();
        for kv in kvs {
            let value = serde_json::from_slice::<ValueType>(kv.value()).with_context(|| {
                "failed to deserialize value from bytes, KVEngine's value supposed to be `ValueType`".to_owned()
            })?;
            result.push(value);
        }
        Ok(result)
    }
}

#[async_trait]
impl KVEngine for EtcdKVEngine {
    type Session = Session;
    type KVEngineWatchStream = KVEngineWatchStream;

    async fn new(end_points: Vec<String>) -> DatenLordResult<Self> {
        let client = etcd_client::Client::connect(end_points, None).await?;
        Ok(Self { client })
    }

    async fn new_meta_txn(&self) -> Box<dyn MetaTxn<Session = Self::Session> + Send> {
        Box::new(EtcdTxn::new(self.client.clone()))
    }

    /// Create a lease with keepalive
    async fn create_session(&self, ttl: u64) -> DatenLordResult<Arc<Session>> {
        let _timer = KV_METRICS.start_kv_operation_timer("create_session");
        let mut client = self.client.clone();
        let resp = client
            .lease_grant(conv_u64_sec_2_i64(ttl), None)
            .await
            .with_context(|| "failed to get lease at `KVEngine::create_session`".to_owned())?;

        let session = Session::new(client.clone(), ttl, resp.id()).await;

        return session;
    }

    /// Distribute lock - lock
    /// - `timeout_sec` should be >=1s
    /// - `timeout_sec` should be >=1s
    async fn lock(&self, key: &LockKeyType, timeout_sec: Duration) -> DatenLordResult<Vec<u8>> {
        let _timer = KV_METRICS.start_kv_lock_timer();
        let mut client = self.client.clone();
        let timeout_sec = check_ttl(conv_u64_sec_2_i64(timeout_sec.as_secs()))
            .with_context(|| "timeout_sec should be >=1s, please fix the call".to_owned())?;

        let lease_id = client
            .lease_grant(timeout_sec, None)
            .await
            .with_context(|| "failed to get lease at `MetaTxn::lock`".to_owned())?
            .id();

        let resp = client
            .lock(
                key.to_string_key(),
                Some(LockOptions::new().with_lease(lease_id)),
            )
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
        let _timer = KV_METRICS.start_kv_operation_timer("get");
        let mut client = self.client.clone();
        let resp = client
            .get(key.to_string_key(), None)
            .await
            .with_context(|| format!("failed to get from etcd engine, key={key:?}"))?;

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
        let _timer = KV_METRICS.start_kv_operation_timer("set");
        let option = match option {
            Some(option) => {
                let mut set_option = PutOptions::new();
                if option.prev_kv {
                    set_option = set_option.with_prev_key();
                }
                if let Some(session) = option.session {
                    if session.is_closed() {
                        return Err(crate::common::error::DatenLordError::EtcdClientErr {
                            source: etcd_client::Error::LeaseKeepAliveError(
                                "session is invalid".to_owned(),
                            ),
                            context: vec![],
                        });
                    }
                    set_option = set_option.with_lease(session.lease_id());
                }
                Some(set_option)
            }
            None => None,
        };
        let serial_value = serde_json::to_vec(value)
            .with_context(|| format!("failed to serialize value={value:?} to bytes"))?;
        let mut client = self.client.clone();
        let mut resp = client
            .put(key.to_string_key(), serial_value, option)
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
        let _timer = KV_METRICS.start_kv_operation_timer("delete");
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
            .delete(key.to_string_key(), option)
            .await
            .with_context(|| format!("failed to get DeleteResponse from etcd for key={key:?}"))?;
        if let Some(pre_kv) = resp.prev_kvs().first() {
            let decoded_value: ValueType = serde_json::from_slice(pre_kv.value())?;
            Ok(Some(decoded_value))
        } else {
            Ok(None)
        }
    }

    /// Range get, return all key-value pairs start with prefix
    async fn range(&self, prefix: &KeyType) -> DatenLordResult<Vec<ValueType>> {
        let _timer = KV_METRICS.start_kv_operation_timer("range");
        let result = self.range_raw_key(prefix.to_string_key()).await?;
        Ok(result)
    }

    /// Watch the key, return a receiver to receive the value
    async fn watch(&self, prefix: &KeyType) -> DatenLordResult<KVEngineWatchStream> {
        let mut client = self.client.clone();
        // Try to watch the key prefix
        let opt = etcd_client::WatchOptions::new().with_prefix();
        let (watcher, watch_stream) = client
            .watch(prefix.to_string_key(), Some(opt.clone()))
            .await
            .with_context(|| "Failed to create watcher".to_owned())?;

        return Ok(KVEngineWatchStream::new(watcher, watch_stream));
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
    type Session = Session;

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

    /// Try to campaign the master with simple txn
    /// Old(reference to etcd client v3):
    /// Create a key with prefix, and compare key (sorted by revision) with the key created by the same session.
    /// If current key is the smallest, then the session is the master, and campaign success.
    ///
    /// New(For datenlord cache scenario):
    /// A small batch of nodes in cluster, we just need a simple txn to get the master key.
    /// The thundering herd is not the main problem, which will cause a bunch of etcd raft logs.
    /// Use String as val can be easily compared.
    async fn campaign(
        &self,
        key: &KeyType,
        val: String,
        session: Arc<Session>,
    ) -> DatenLordResult<(bool, String)> {
        if session.is_closed() {
            return Err(crate::common::error::DatenLordError::EtcdClientErr {
                source: etcd_client::Error::LeaseKeepAliveError("session is invalid".to_owned()),
                context: vec![],
            });
        }

        let mut client = self.client.clone();

        // Try to get the key, if key is existed
        // We need to return the data from etcd,
        // If the key is not existed, we need to set the key.
        let txn = Txn::new()
            // Check the key is existed and key is not equal to current value, which means the key is ready to be campaign.
            .when(vec![
                Compare::create_revision(key.to_string_key(), CompareOp::NotEqual, 0),
                Compare::value(key.to_string_key(), CompareOp::NotEqual, val.clone()),
            ])
            // Campaign failed
            .and_then(vec![TxnOp::get(
                key.to_string_key(),
                Some(GetOptions::new().with_serializable()),
            )])
            // Campaign succeed
            .or_else(vec![TxnOp::put(
                key.to_string_key(),
                val.clone(),
                Some(PutOptions::new().with_lease(session.lease_id())),
            )]);

        let (campaign_status, responses) = match client.txn(txn).await {
            Ok(resp) => {
                // Check the txn branch is `then` or `else`
                // If succeeded, means the campaign is failed and can not set current data to etcd
                // If failed, means the campaign is success, and put the current data to etcd
                (!resp.succeeded(), resp.op_responses())
            }
            Err(e) => {
                error!("failed to campaign, error={e:?}");
                return Err(crate::common::error::DatenLordError::EtcdClientErr {
                    source: e,
                    context: vec!["failed to do txn operation at `MetaTxn::campaign`".to_owned()],
                });
            }
        };

        // Campaign success, return the current data
        if campaign_status {
            return Ok((true, val.clone()));
        }

        // If the campaign is failed, we need to check the response
        if let Some(response) = responses.first() {
            match *response {
                TxnOpResponse::Put(_) => {
                    // we can directly return current data
                    return Ok((true, val.clone()));
                }
                TxnOpResponse::Get(ref resp) => {
                    debug!("failed to campaign, return the existing key");
                    if let Some(kv) = resp.kvs().first() {
                        let item_value = match kv.value_str() {
                            Ok(value) => value.to_owned(),
                            Err(err) => {
                                return Err(crate::common::error::DatenLordError::EtcdClientErr {
                                    source: err,
                                    context: vec![
                                        "failed to get key from etcd txn get event".to_owned()
                                    ],
                                });
                            }
                        };

                        return Ok((false, item_value));
                    }
                    return Err(crate::common::error::DatenLordError::EtcdClientErr {
                        source: etcd_client::Error::InvalidArgs(
                            "failed to get kvs from etcd txn get event".to_owned(),
                        ),
                        context: vec![],
                    });
                }
                TxnOpResponse::Delete(_) | TxnOpResponse::Txn(_) => {
                    return Err(crate::common::error::DatenLordError::EtcdClientErr {
                        source: etcd_client::Error::InvalidArgs(
                            "failed to campaign, the responses op is not match".to_owned(),
                        ),
                        context: vec![],
                    });
                }
            }
        }

        return Err(crate::common::error::DatenLordError::EtcdClientErr {
            source: etcd_client::Error::InvalidArgs(
                "failed to campaign, the responses length is not match".to_owned(),
            ),
            context: vec![],
        });
    }

    async fn commit(&mut self) -> DatenLordResult<bool> {
        let _timer = KV_METRICS.start_kv_operation_timer("txn");

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
    use crate::async_fuse::memfs::direntry::{DirEntry, FileType};
    use crate::common::error::DatenLordError;
    use crate::retry_txn;

    const ETCD_ADDRESS: &str = "localhost:2379";

    #[tokio::test]
    async fn test_lock_unlock() {
        let test_key = 1224;
        let client = EtcdKVEngine::new_for_local_test(vec![ETCD_ADDRESS.to_owned()])
            .await
            .unwrap();
        let key = LockKeyType::FileNodeListLock(test_key);
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
        let value = ValueType::String("test_connect_local_key".to_owned());
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
        let value1 = ValueType::String("value1".to_owned());
        let key2 = KeyType::String(String::from("test_commit key2"));
        let value2 = ValueType::String("value2".to_owned());
        first_txn.set(&key1, &value1);
        first_txn.set(&key2, &value2);
        first_txn.commit().await.unwrap();
        drop(client);
        // use two thread to do the second and third txn
        // and use channel to control the order
        let (first_step_tx, mut first_step_rx) = tokio::sync::mpsc::channel(1);
        let (second_step_tx, mut second_step_rx) = tokio::sync::mpsc::channel(1);
        let second_handle = tokio::spawn(async move {
            let (result, retry) = retry_txn!(1, {
                let client = EtcdKVEngine::new_for_local_test(vec![ETCD_ADDRESS.to_owned()])
                    .await
                    .unwrap();
                let mut second_txn = client.new_meta_txn().await;
                let key1 = KeyType::String(String::from("test_commit key1"));
                let result1 = second_txn.get(&key1).await.unwrap();
                assert!(result1.is_some());
                if let Some(ValueType::String(value1)) = result1 {
                    assert_eq!(value1, "value1");
                } else {
                    panic!("wrong value type");
                }
                // let the third txn start
                first_step_tx.send(()).await.unwrap();
                // wait for the third txn to set the key
                second_step_rx.recv().await.unwrap();
                let key2 = KeyType::String(String::from("test_commit key2"));
                let result2 = second_txn.get(&key2).await.unwrap();
                assert!(result2.is_some());
                if let Some(ValueType::String(value2)) = result2 {
                    assert_eq!(value2, "value2");
                } else {
                    panic!("wrong value type");
                }
                (second_txn.commit().await, ())
            });
            assert!(matches!(
                result,
                Err(DatenLordError::TransactionRetryLimitExceededErr { .. })
            ));
            assert_eq!(retry, 1);
        });
        let third_handle = tokio::spawn(async move {
            let client = EtcdKVEngine::new_for_local_test(vec![ETCD_ADDRESS.to_owned()])
                .await
                .unwrap();
            let mut third_txn = client.new_meta_txn().await;
            // wait for the second read first key and send the signal
            first_step_rx.recv().await.unwrap();
            let key1 = KeyType::String(String::from("test_commit key1"));
            let value1 = ValueType::String("value3".to_owned());
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
        let (result, retry) = retry_txn!(3, {
            let client = EtcdKVEngine::new_for_local_test(vec![ETCD_ADDRESS.to_owned()])
                .await
                .unwrap();
            let mut txn = client.new_meta_txn().await;
            let key = KeyType::String(String::from("/"));
            let _: Option<ValueType> = txn.get(&key).await.unwrap();
            (txn.commit().await, ())
        });
        result.unwrap();
        assert_eq!(retry, 0);
    }

    #[tokio::test]
    async fn test_range_get() {
        let client = EtcdKVEngine::new_for_local_test(vec![ETCD_ADDRESS.to_owned()])
            .await
            .unwrap();
        // Avoid conflict with other tests
        let parent_id_1 = 1024 * 1024 * 1024;
        let parent_id_2 = 1024 * 1024 * 1024 + 1;
        let child_names = vec!["__1_child_a", "_1_child_b", "_child_c"];

        for child_name in &child_names {
            let key = KeyType::DirEntryKey((parent_id_1, (*child_name).to_owned()));
            let value =
                ValueType::DirEntry(DirEntry::new(1, (*child_name).to_owned(), FileType::Dir));
            client.set(&key, &value, None).await.unwrap();
        }

        for child_name in &child_names {
            let key = KeyType::DirEntryKey((parent_id_2, (*child_name).to_owned()));
            let value =
                ValueType::DirEntry(DirEntry::new(2, (*child_name).to_owned(), FileType::Dir));
            client.set(&key, &value, None).await.unwrap();
        }

        // Range get
        let key = KeyType::DirEntryKey((parent_id_1, String::new()));
        let result = client.range(&key).await.unwrap();
        assert_eq!(result.len(), 3);
        for value in result {
            let dir_entry = value.into_dir_entry();
            assert_eq!(dir_entry.ino(), 1);
            assert!(child_names.contains(&dir_entry.name()));
            assert_eq!(dir_entry.file_type(), FileType::Dir);
        }

        let key = KeyType::DirEntryKey((parent_id_2, String::new()));
        let result = client.range(&key).await.unwrap();
        assert_eq!(result.len(), 3);
        for value in result {
            let dir_entry = value.into_dir_entry();
            assert_eq!(dir_entry.ino(), 2);
            assert!(child_names.contains(&dir_entry.name()));
            assert_eq!(dir_entry.file_type(), FileType::Dir);
        }
    }

    #[tokio::test]
    async fn test_watch() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .init();
        let client = EtcdKVEngine::new_for_local_test(vec![ETCD_ADDRESS.to_owned()])
            .await
            .unwrap();
        // Create a new key
        let key = KeyType::String("test_watch".to_owned());
        let mut watch_stream = client.watch(&key).await.unwrap();
        for _ in 0_i32..5_i32 {
            let client_clone = client.clone();
            tokio::spawn(async move {
                let key = KeyType::String("test_watch".to_owned());
                let value = ValueType::String("test_watch_value".to_owned());
                // Wait for watch message is ready()
                tokio::time::sleep(Duration::from_secs(1)).await;
                client_clone.set(&key, &value, None).await.unwrap();
            });
            let value = ValueType::String("test_watch_value".to_owned());
            let watch_result = watch_stream.next().await.unwrap().unwrap();
            assert_eq!(watch_result.0, key.to_string_key());
            assert_eq!(watch_result.1.unwrap(), value);

            // Update a key
            let client_clone = client.clone();
            tokio::spawn(async move {
                let key = KeyType::String("test_watch".to_owned());
                let value = ValueType::String("test_watch_value2".to_owned());
                tokio::time::sleep(Duration::from_secs(1)).await;
                client_clone.set(&key, &value, None).await.unwrap();
            });
            let key = KeyType::String("test_watch".to_owned());
            let value = ValueType::String("test_watch_value2".to_owned());
            let watch_result = watch_stream.next().await.unwrap().unwrap();
            assert_eq!(watch_result.0, key.to_string_key());
            assert_eq!(watch_result.1.unwrap(), value);

            // Delete a key
            let client_clone = client.clone();
            tokio::spawn(async move {
                let key = KeyType::String("test_watch".to_owned());
                tokio::time::sleep(Duration::from_secs(1)).await;
                client_clone.delete(&key, None).await.unwrap();
            });
            let watch_result = watch_stream.next().await.unwrap().unwrap();
            assert!(watch_result.1.is_none());
        }

        // Close stream
        watch_stream.cancel().await.unwrap();
    }

    #[tokio::test]
    async fn test_create_session() {
        let client = EtcdKVEngine::new_for_local_test(vec![ETCD_ADDRESS.to_owned()])
            .await
            .unwrap();
        // Test keep alive
        let session = client.create_session(5).await.unwrap();
        // Set a temp key for this lease
        let key = KeyType::String("test_create_session".to_owned());
        let value = ValueType::String("test_create_session_value".to_owned());
        let session_opt = Arc::clone(&session);
        client
            .set(
                &key,
                &value,
                Some(SetOption::new().with_session(session_opt)),
            )
            .await
            .unwrap();

        assert_eq!(client.get(&key).await.unwrap().unwrap(), value);

        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(client.get(&key).await.unwrap().unwrap(), value);

        // Test cancel keepalive
        drop(session);
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert!(client.get(&key).await.unwrap().is_none());
    }

    /// Utils function to get the lease id from etcd by key
    async fn get_lease_id_from_key(
        etcd_address_vec: Vec<String>,
        key: &KeyType,
    ) -> DatenLordResult<i64> {
        let mut client = etcd_client::Client::connect(etcd_address_vec.clone(), None)
            .await
            .with_context(|| {
                format!("failed to connect to etcd, the etcd address={etcd_address_vec:?}")
            })?;

        let resp = client
            .get(key.to_string_key(), None)
            .await
            .with_context(|| format!("failed to get from etcd engine, key={key:?}"))?;

        let kvs = resp.kvs();
        match kvs.get(0) {
            Some(kv) => Ok(kv.lease()),
            None => Ok(0),
        }
    }

    #[tokio::test]
    async fn test_campaign() {
        let client = EtcdKVEngine::new_for_local_test(vec![ETCD_ADDRESS.to_owned()])
            .await
            .unwrap();
        let txn = client.new_meta_txn().await;

        let key = KeyType::String("test_campaign".to_owned());
        let val = "test_campaign_val".to_owned();
        let val2 = "test_campaign_val2".to_owned();

        let campaign_session_1 = client.create_session(5).await.unwrap();
        // Campaign the key
        let (campaign_status, campaign_val) = txn
            .campaign(&key, val.clone(), Arc::clone(&campaign_session_1))
            .await
            .unwrap();
        assert!(campaign_status);
        assert_eq!(campaign_val, val);
        let result_lease_id = get_lease_id_from_key(vec![ETCD_ADDRESS.to_owned()], &key)
            .await
            .unwrap();
        assert_eq!(result_lease_id, campaign_session_1.lease_id());

        let campaign_session_2 = client.create_session(5).await.unwrap();
        // Recampaign the key
        let (campaign_status, campaign_val) = txn
            .campaign(&key, val.clone(), Arc::clone(&campaign_session_2))
            .await
            .unwrap();
        assert!(campaign_status);
        assert_eq!(campaign_val, val);
        // Check the lease id from etcd
        let result_lease_id = get_lease_id_from_key(vec![ETCD_ADDRESS.to_owned()], &key)
            .await
            .unwrap();
        assert_ne!(result_lease_id, campaign_session_1.lease_id());
        assert_eq!(result_lease_id, campaign_session_2.lease_id());

        let campaign_session_3 = client.create_session(5).await.unwrap();
        // Campaign the key with different value, campaign failed
        let (campaign_status, campaign_val) = txn
            .campaign(&key, val2.clone(), Arc::clone(&campaign_session_3))
            .await
            .unwrap();
        assert!(!campaign_status);
        assert_eq!(campaign_val, val);
        let result_lease_id = get_lease_id_from_key(vec![ETCD_ADDRESS.to_owned()], &key)
            .await
            .unwrap();
        assert_ne!(result_lease_id, campaign_session_1.lease_id());
        assert_ne!(result_lease_id, campaign_session_3.lease_id());
        assert_eq!(result_lease_id, campaign_session_2.lease_id());
    }
}
