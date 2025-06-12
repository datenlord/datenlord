use core::fmt;
use std::{collections::HashMap, sync::Arc, time::Duration};

use clippy_utilities::Cast;
use radix_trie::Trie;
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use crate::{
    async_fuse::util::usize_to_u64,
    common::{
        error::{DatenLordError, DatenLordResult},
        task_manager::{TaskName, TASK_MANAGER},
    },
    connect_timeout,
    distribute_kv_cache::rpc::message::{
        self, KVCacheLargeRequest, KVCacheLargeResponse, KVCacheSmallRequest, KVCacheSmallResponse,
    },
};

use super::{
    cluster::cluster_manager::ClusterManager,
    rpc::{
        client::RpcClient,
        common::ClientTimeoutOptions,
        message::{
            KVBlockBatchPutRequest, KVBlockGetRequest, KVBlockPutRequest, KVCacheIdAllocateRequest,
            KVCacheIndexBatchInsertRequest, KVCacheIndexInsertRequest, KVCacheIndexMatchRequest,
            KVCacheIndexRemoveRequest, KVCacheRequest, KVCacheRequestTask, KVCacheResponse,
            ReqType,
        },
        utils::u64_to_usize,
    },
};

/// Duration to check the rpc client cache
const RPC_CLIENT_CACHE_CHECK_DURATION: u64 = 60;

/// Unused kv block id
const UNUSED_KV_BLOCK_ID: u64 = 0;

/// `KVBlock` instance
///
/// |--------`KVBlock`-----| => single kv block data
/// |cache1|cache2|cache3| => contains kv cache data = cache1, cache2, cache3
/// |0-----|1-----|2-----| => prefix = 0, 1, 2
///
/// TODO: Hold the kv cache and fetch from local cache
#[derive(Debug, Clone)]
pub struct KVBlock {
    /// The block id
    pub block_id: u64,
    /// The block size
    pub block_size: u64,
    /// The block data, contains one or more kv cache data
    pub data: Vec<u8>,
}

/// `KVCache` instance metadata
///
/// Set current kv cache metadata info to single kv block
#[derive(Debug, Clone)]
pub struct KVCacheMeta<K>
where
    K: num::Num + Eq + Send + Sync + Clone + fmt::Debug + 'static,
{
    /// The kv block id
    pub block_id: u64,
    /// The kv cache offset
    pub offset: u64,
    /// The kv cache size
    pub size: u64,
    /// The kv cache prefix
    pub prefix: Vec<K>,
}

impl<K> Default for KVCacheMeta<K>
where
    K: num::Num + Eq + Send + Sync + Clone + fmt::Debug + 'static,
{
    fn default() -> Self {
        Self {
            block_id: UNUSED_KV_BLOCK_ID,
            offset: 0,
            size: 0,
            prefix: Vec::new(),
        }
    }
}

/// Local block cache
#[derive(Debug, Clone)]
struct LocalBlockCache<K>
where
    K: num::Num + Eq + Send + Sync + Clone + fmt::Debug + 'static,
{
    /// The block data
    block_cache: KVBlock,
    /// The radix tree for block metas, support partial match
    /// Use token ids as the key, and the kv cache meta as the value
    /// u32 size is enough for the token id in tokenized.
    block_metas_tree: Trie<Vec<K>, KVCacheMeta<K>>,
}

impl<K> LocalBlockCache<K>
where
    K: num::Num + Eq + Send + Sync + Clone + fmt::Debug + 'static,
    Vec<K>: radix_trie::TrieKey + Clone,
{
    /// Create a new local block cache
    pub fn new(block_size: u64) -> Self {
        Self {
            block_cache: KVBlock {
                block_id: UNUSED_KV_BLOCK_ID,
                block_size,
                data: Vec::with_capacity(u64_to_usize(block_size)),
            },
            block_metas_tree: Trie::new(),
        }
    }

    /// Insert a kv cache to the local block cache
    #[allow(clippy::needless_pass_by_value)]
    pub fn insert(&mut self, kv_cache_meta: KVCacheMeta<K>, data: &[u8]) -> DatenLordResult<()> {
        // Check current block id is equal to the kv cache block id
        if self.block_cache.block_id == UNUSED_KV_BLOCK_ID {
            return Err(DatenLordError::DistributeCacheManagerErr {
                context: vec![format!(
                    "Failed to insert kv cache to the block cache, block id is invalid with UNUSED_KV_BLOCK_ID"
                )],
            });
        }

        // Check current block size is enough
        if self.block_cache.data.len() + data.len() > self.block_cache.data.capacity() {
            return Err(DatenLordError::DistributeCacheManagerErr {
                context: vec![format!(
                    "Failed to insert kv cache to the block cache, block size is not enough"
                )],
            });
        }

        // 1. Insert the kv cache data to the block cache
        self.block_cache.data.extend_from_slice(data);

        // 2. Insert the kv cache meta to the block metas tree
        self.block_metas_tree
            .insert(kv_cache_meta.prefix.clone(), kv_cache_meta.clone());

        Ok(())
    }

    /// Get the block id
    pub fn get_block_id(&self) -> u64 {
        self.block_cache.block_id
    }

    /// Get next offset
    pub fn get_next_offset(&self) -> u64 {
        usize_to_u64(self.block_cache.data.len())
    }

    /// Try to match local cache prefix.
    #[allow(clippy::needless_pass_by_value)] // Used by pyo3.
    pub fn match_prefix(&self, prefix: Vec<K>) -> Option<KVCacheMeta<K>> {
        // Find the kv cache meta with the prefix
        match self.block_metas_tree.get_ancestor_key(&prefix) {
            Some(ancestor_key) => {
                let kv_cache_meta = self.block_metas_tree.get_ancestor_value(ancestor_key)?;

                Some(kv_cache_meta.clone())
            }
            None => None,
        }
    }

    /// Try to get the block data from the local block cache
    ///
    /// TODO: Local cache may cover the remote block metas tree, consider to set a threshold to drop the query
    #[allow(clippy::needless_pass_by_value)] // Used by pyo3.
    #[allow(clippy::indexing_slicing)] // We have checked the index is valid.
    pub fn try_load(&self, kv_cache_meta: KVCacheMeta<K>) -> Option<Vec<u8>> {
        let offset = kv_cache_meta.offset.cast::<usize>();
        let size = kv_cache_meta.size.cast::<usize>();

        // Check the kv cache meta offset and size is valid
        if offset + size > self.block_cache.data.len() {
            return None;
        }

        let mut data = Vec::new();
        data.extend_from_slice(&self.block_cache.data[offset..offset + size]);

        Some(data)
    }

    /// Clear the local block cache with new block id
    pub fn clear(&mut self, new_block_id: u64) {
        self.block_cache.block_id = new_block_id;
        self.block_cache.data.clear();

        // Clear the block metas tree
        self.block_metas_tree = Trie::new();
    }

    /// Get `KVBlock`
    pub fn get_kv_block(&self) -> KVBlock {
        let mut current_block = self.block_cache.clone();
        // Return a copy of the block data
        current_block
            .data
            .resize(u64_to_usize(self.block_cache.block_size), 0);

        current_block
    }

    /// Get `KVCacheMeta`
    pub fn get_kv_cache_metas(&self) -> Vec<&KVCacheMeta<K>> {
        self.block_metas_tree.get_values()
    }
}

/// The distribute cache client
#[derive(Debug, Clone)]
pub struct DistributeKVCacheClient<K>
where
    K: num::Num + Eq + Send + Sync + Clone + fmt::Debug + Serialize + DeserializeOwned + 'static,
{
    /// The distribute cache inner
    inner: DistributeKVCacheClientInner<K>,
    /// Single block cache, used to collect the block data from the infer side.
    block_cache: Arc<Mutex<LocalBlockCache<K>>>,
}

impl<K> DistributeKVCacheClient<K>
where
    K: num::Num + Eq + Send + Sync + Clone + fmt::Debug + Serialize + DeserializeOwned + 'static,
    Vec<K>: radix_trie::TrieKey + Clone,
{
    /// Create a new distribute cache client
    #[must_use]
    pub fn new(cluster_manager: Arc<ClusterManager>, block_size: u64) -> Self {
        let inner = DistributeKVCacheClientInner::new(cluster_manager, block_size);
        Self {
            inner,
            block_cache: Arc::new(Mutex::new(LocalBlockCache::new(block_size))),
        }
    }

    /// Start the distribute cache client watch task
    pub async fn start_watch(&self) -> DatenLordResult<()> {
        // Start the watch task
        self.inner.start_watch().await
    }

    #[allow(unreachable_code)]
    #[allow(unused_variables)]
    /// Try to match the prefix and get the block from the distribute cache
    pub async fn match_prefix(&self, prefix: Vec<K>) -> DatenLordResult<Vec<K>> {
        // 1. check current node has the local block cache
        // If ok, get the block from the local block cache
        // If not, get the block from the distribute cache
        // Optimize: Try to compare partial prefix with the local block cache and remote block cache
        let local_cache_lock = self.block_cache.lock().await;
        let local_kv_cache_meta = if let Some(kv_cache_meta) =
            local_cache_lock.match_prefix(prefix.clone())
        {
            debug!(
                "Matched local kv cache meta from local cache: {:?}",
                kv_cache_meta
            );
            kv_cache_meta
        } else {
            debug!("Failed to match local kv cache meta from local cache, try to get from the distribute cache");
            // Empty meta
            KVCacheMeta::default()
        };
        let local_prefix_len = local_kv_cache_meta.prefix.len().cast::<u64>();

        // 2. Match prefix to get the block id and target node
        let (kv_block_meta, node_address) = match self.inner.match_prefix(prefix.clone()).await {
            Ok((kv_block_meta, node_address)) => {
                debug!(
                    "Matched remote kv block meta: {:?} node_address: {:?}",
                    kv_block_meta, node_address
                );
                (kv_block_meta, node_address)
            }
            Err(err) => {
                error!("Failed to match prefix: {:?} with error: {:?}", prefix, err);
                // Return a empty data
                (KVCacheMeta::default(), String::new())
            }
        };

        let remote_prefix_len = kv_block_meta.prefix.len().cast::<u64>();
        info!(
            "Matched remote kv block meta: {:?} node_address: {:?}",
            kv_block_meta, node_address
        );

        // If local prefix length is less than remote prefix length, we will get the block from the local cache
        if local_prefix_len >= remote_prefix_len {
            return Ok(local_kv_cache_meta.prefix);
        }

        // Return the prefix
        Ok(kv_block_meta.prefix)
    }

    #[allow(unreachable_code)]
    #[allow(unused_variables)]
    /// Try to load the block from the distribute cache, return sub string with the prefix
    pub async fn try_load(&self, prefix: Vec<K>) -> DatenLordResult<(Vec<K>, bytes::Bytes)> {
        // 1. check current node has the local block cache
        // If ok, get the block from the local block cache
        // If not, get the block from the distribute cache
        // Optimize: Try to compare partial prefix with the local block cache and remote block cache
        let local_cache_lock = self.block_cache.lock().await;
        let local_kv_cache_meta = if let Some(kv_cache_meta) =
            local_cache_lock.match_prefix(prefix.clone())
        {
            debug!(
                "Matched local kv cache meta from local cache: {:?}",
                kv_cache_meta
            );
            kv_cache_meta
        } else {
            debug!("Failed to match local kv cache meta from local cache, try to get from the distribute cache");
            // Empty meta
            KVCacheMeta::default()
        };
        let local_prefix_len = local_kv_cache_meta.prefix.len().cast::<u64>();

        // 2. Match prefix to get the block id and target node
        let (kv_block_meta, node_address) = match self.inner.match_prefix(prefix.clone()).await {
            Ok((kv_block_meta, node_address)) => {
                debug!(
                    "Matched remote kv block meta: {:?} node_address: {:?}",
                    kv_block_meta, node_address
                );
                (kv_block_meta, node_address)
            }
            Err(err) => {
                error!("Failed to match prefix: {:?} with error: {:?}", prefix, err);
                // Return a empty data
                (KVCacheMeta::default(), String::new())
            }
        };

        let remote_prefix_len = kv_block_meta.prefix.len().cast::<u64>();
        info!(
            "Matched remote kv block meta: {:?} node_address: {:?}",
            kv_block_meta, node_address
        );

        // If local prefix length is less than remote prefix length, we will get the block from the local cache
        if local_prefix_len >= remote_prefix_len {
            // Get the block from local cache
            if let Some(data) = local_cache_lock.try_load(local_kv_cache_meta.clone()) {
                debug!(
                    "Get block from local cache: {:?} data len {:?}",
                    local_kv_cache_meta,
                    data.len()
                );
                // TODO: use bytes instead of Vec<u8>
                return Ok((local_kv_cache_meta.prefix, bytes::Bytes::from(data)));
            }

            debug!("Failed to get block from local cache, try to get from the distribute cache");
        }

        // Empty address from the distribute cache
        if node_address.is_empty() {
            error!("Failed to get block, node address is empty");
            return Ok((Vec::new(), bytes::Bytes::new()));
        }

        // 3. Get the block from the distribute cache
        // TODO: update string key with u64
        let block_data = match self
            .inner
            .get_block(node_address.clone(), kv_block_meta.block_id)
            .await
        {
            Ok(data) => data,
            Err(err) => {
                // Inform master to delete this key
                self.inner.remove_index(prefix).await?;

                error!(
                    "Failed to get block: {:?} node_address: {:?} kv_block_meta: {:?}",
                    err, node_address, kv_block_meta
                );
                return Ok((Vec::new(), bytes::Bytes::new()));
            }
        };

        // 4. Copy the block data to the data with kv cache meta offset and size
        let offset = u64_to_usize(kv_block_meta.offset);
        let size = u64_to_usize(kv_block_meta.size);
        debug!(
            "Get block from remote cache: {:?} offset: {:?} size: {:?}",
            kv_block_meta, offset, size
        );

        Ok((
            kv_block_meta.prefix,
            block_data.slice(offset..offset + size),
        ))
    }

    /// Insert a block to the distribute cache
    #[allow(clippy::too_many_lines)]
    pub async fn insert(&self, prefix: Vec<K>, data: Vec<u8>) -> DatenLordResult<()> {
        // Check current size is valid
        if data.len().cast::<u64>() > self.inner.block_size {
            return Err(DatenLordError::DistributeCacheManagerErr {
                context: vec![format!(
                    "Failed to insert kv cache to the block cache, data size is too large"
                )],
            });
        }

        // 1. fill current block cache
        let mut block_cache = self.block_cache.lock().await;
        let mut current_block_id = block_cache.get_block_id();
        debug!(
            "Insert kv cache to the block cache with block id: {:?}",
            current_block_id
        );

        while current_block_id == UNUSED_KV_BLOCK_ID {
            let new_block_id = self.inner.alloc_block_id().await?;
            debug!("Alloc new block id: {:?}", new_block_id);
            block_cache.clear(new_block_id);
            current_block_id = new_block_id;
        }

        let next_offset = block_cache.get_next_offset();
        let kv_cache_meta = KVCacheMeta {
            block_id: current_block_id,
            offset: next_offset,
            size: data.len().cast(),
            prefix: prefix.clone(),
        };

        match block_cache.insert(kv_cache_meta, &data) {
            Ok(()) => {
                debug!(
                    "Insert kv cache to the block cache with block id: {:?} successfully",
                    current_block_id
                );
            }
            Err(err) => {
                debug!("Failed to insert kv cache to the block cache: {:?}, try to allocate new block cache", err);
                // 2. current block cache is full, we will insert this block to the distribute cache
                // and clear the block cache
                let kv_block = block_cache.get_kv_block();
                let kv_cache_metas = block_cache.get_kv_cache_metas();

                // Insert the block to the distribute cache
                let node = self
                    .inner
                    .cluster_manager
                    .get_node_remote(kv_block.block_id.to_string())
                    .await?;

                let addr = format!("{}:{}", node.ip(), node.port());
                self.inner.put_blocks(addr.clone(), vec![kv_block]).await?;

                // If ok, insert the indexes to the distribute cache
                self.inner.insert_indexes(kv_cache_metas, addr).await?;

                // If ok, clear the block cache
                let current_block_id = self.inner.alloc_block_id().await?;
                block_cache.clear(current_block_id);

                // Try to insert the kv cache meta again
                let next_offset = block_cache.get_next_offset();
                let kv_cache_meta = KVCacheMeta {
                    block_id: current_block_id,
                    offset: next_offset,
                    size: data.len().cast(),
                    prefix: prefix.clone(),
                };
                block_cache.insert(kv_cache_meta, &data)?;
            }
        }

        Ok(())
    }
}

/// The distribute cache client
#[allow(clippy::type_complexity)]
#[derive(Debug, Clone)]
pub struct DistributeKVCacheClientInner<K>
where
    K: num::Num + Eq + Send + Sync + Clone + fmt::Debug + Serialize + DeserializeOwned + 'static,
{
    /// The cluster manager, only used to watch hashring changes
    cluster_manager: Arc<ClusterManager>,
    /// The rpc client cache
    /// TODO: Change to trait
    rpc_client_cache: Arc<Mutex<HashMap<String, Arc<RpcClient<KVCacheRequestTask<K>>>>>>,
    /// Block size
    block_size: u64,
}

impl<K> DistributeKVCacheClientInner<K>
where
    K: num::Num + Eq + Send + Sync + Clone + fmt::Debug + Serialize + DeserializeOwned + 'static,
{
    /// Create a new distribute cache client
    #[must_use]
    pub fn new(cluster_manager: Arc<ClusterManager>, block_size: u64) -> Self {
        let rpc_client_cache = Arc::new(Mutex::new(HashMap::new()));
        Self {
            cluster_manager,
            rpc_client_cache,
            block_size,
        }
    }

    /// Start the distribute cache client watch task
    #[allow(clippy::ignored_unit_patterns)]
    pub async fn start_watch(&self) -> DatenLordResult<()> {
        // Start the watch task
        let cluster_manager = Arc::clone(&self.cluster_manager);
        TASK_MANAGER
            .spawn(TaskName::AsyncFuse, |token| async move {
                match cluster_manager.watch_ring(token).await {
                    Ok(()) => {}
                    Err(err) => {
                        error!("Failed to watch ring: {:?}", err);
                    }
                }
            })
            .await
            .map_err(|err| DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to start watch task: {:?}", err)],
            })?;

        // Wait for the ring to be stable
        let cluster_manager = Arc::clone(&self.cluster_manager);
        loop {
            match cluster_manager.get_ring_force().await {
                Ok(current_ring) => {
                    if !current_ring.is_empty() && current_ring.version() != 0 {
                        info!("Current ring is stable: {:?}", current_ring);
                        break;
                    }
                }
                Err(err) => {
                    warn!("Failed to get ring: {:?}", err);
                }
            }
        }

        let rpc_client_cache = Arc::clone(&self.rpc_client_cache);
        // TODO: Selectively verify that a batch of clients is valid in rpc client cache
        TASK_MANAGER
            .spawn(TaskName::AsyncFuse, |token| async move {
                let duration = Duration::from_secs(RPC_CLIENT_CACHE_CHECK_DURATION);
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(duration) => {
                            // If current rpc request is valid, we will hold this request and continue to use it,
                            // in this period, we just delete all the rpc client in the cache
                            rpc_client_cache.lock().await.clear();
                            debug!("Batch validate rpc client cache task is finished");
                        }
                        _ = token.cancelled() => {
                            warn!("Batch validate rpc client cache task is cancelled");
                            return;
                        }
                    }
                }
            })
            .await
            .map_err(|err| DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to start watch task: {:?}", err)],
            })?;

        Ok(())
    }

    /// Allocate block id from the distribute cache
    #[allow(clippy::wildcard_enum_match_arm)]
    async fn alloc_block_id(&self) -> DatenLordResult<u64> {
        // Get the cache node with the block id
        let master_node = self.cluster_manager.get_master_node().await?;
        let addr = format!("{}:{}", master_node.ip(), master_node.port());
        debug!("Alloc block id from the master node: {:?}", addr);

        let (tx, rx) = flume::unbounded::<Result<KVCacheResponse, KVCacheRequest<K>>>();
        let kv_cache_id_allocate_request = KVCacheRequest::KVCacheSmallRequest(
            KVCacheSmallRequest::KVCacheIdAllocateRequest(KVCacheIdAllocateRequest {
                block_size: self.block_size,
            }),
        );
        let packet = KVCacheRequestTask::new(
            ReqType::KVCacheIdAllocateRequest.into(),
            kv_cache_id_allocate_request,
            tx.clone(),
        );
        let rpc_client: Arc<RpcClient<KVCacheRequestTask<K>>> =
            self.get_client(addr.clone()).await?;
        rpc_client.send_request(packet).await.map_err(|err| {
            DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to send request: {:?}", err)],
            }
        })?;

        match rx.recv_async().await {
            Ok(Ok(response)) => match response {
                KVCacheResponse::KVCacheSmallResponse(
                    KVCacheSmallResponse::KVCacheIdAllocateResponse(response),
                ) => Ok(response.kv_cache_id),
                _ => Err(DatenLordError::DistributeCacheManagerErr {
                    context: vec![format!("Failed to read block: {:?}", response)],
                }),
            },
            Ok(Err(err)) => Err(DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to read block: {:?}", err)],
            }),
            Err(_) => Err(DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to read block")],
            }),
        }
    }

    /// Match the prefix and get block id from the distribute cache
    /// return the kv cache meta info and remote node address
    ///
    //. If not find, will return a empty string
    #[allow(clippy::wildcard_enum_match_arm)]
    async fn match_prefix(&self, prefix: Vec<K>) -> DatenLordResult<(KVCacheMeta<K>, String)> {
        let mut raw_prefix = prefix.clone();
        // Get the cache node with the block id
        let master_node = self.cluster_manager.get_master_node().await?;
        let addr = format!("{}:{}", master_node.ip(), master_node.port());

        let (tx, rx) = flume::unbounded::<Result<KVCacheResponse, KVCacheRequest<K>>>();
        let kv_cache_index_match_request = KVCacheRequest::KVCacheSmallRequest(
            KVCacheSmallRequest::KVCacheIndexMatchRequest(KVCacheIndexMatchRequest {
                block_size: self.block_size,
                kv_cache_key: prefix,
            }),
        );
        let packet = KVCacheRequestTask::new(
            ReqType::KVCacheIndexMatchRequest.into(),
            kv_cache_index_match_request,
            tx.clone(),
        );
        let rpc_client = self.get_client(addr.clone()).await?;
        rpc_client.send_request(packet).await.map_err(|err| {
            DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to send request: {:?}", err)],
            }
        })?;

        match rx.recv_async().await {
            Ok(Ok(response)) => match response {
                KVCacheResponse::KVCacheSmallResponse(
                    KVCacheSmallResponse::KVCacheIndexMatchResponse(response),
                ) => {
                    // Check the kv cache index is existed, means the prefix is not found and partial key is not matched
                    if message::StatusCode::NotFound == response.status {
                        return Err(DatenLordError::DistributeCacheManagerErr {
                            context: vec![format!(
                                "Failed to match prefix: {:?}, this index is not found",
                                raw_prefix
                            )],
                        });
                    }

                    let node_address = String::from_utf8(response.node_address).map_err(|err| {
                        DatenLordError::DistributeCacheManagerErr {
                            context: vec![format!("Failed to parse node address: {:?}", err)],
                        }
                    })?;
                    info!("Matched kv cache meta address: {:?}", node_address);
                    raw_prefix.truncate(u64_to_usize(response.kv_cache_key_len));
                    Ok((
                        KVCacheMeta {
                            block_id: response.kv_cache_id,
                            offset: response.offset,
                            size: response.size,
                            // Get sub string from the raw prefix
                            prefix: raw_prefix,
                        },
                        node_address,
                    ))
                }
                _ => Err(DatenLordError::DistributeCacheManagerErr {
                    context: vec![format!("Failed to read block: {:?}", response)],
                }),
            },
            Ok(Err(err)) => Err(DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to read block: {:?}", err)],
            }),
            Err(_) => Err(DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to read block")],
            }),
        }
    }

    /// Insert a index to the distribute cache
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::wildcard_enum_match_arm)]
    async fn insert_indexes(
        &self,
        kv_cache_meta_list: Vec<&KVCacheMeta<K>>,
        node_address: String,
    ) -> DatenLordResult<()> {
        // Get the cache node with the block id
        let master_node = self.cluster_manager.get_master_node().await?;
        let addr = format!("{}:{}", master_node.ip(), master_node.port());

        // Generate Vec<KVCacheIndexInsertRequest>
        let mut kv_cache_index_insert_requests = Vec::new();
        for item in kv_cache_meta_list {
            let kv_cache_key = item.prefix.clone();
            kv_cache_index_insert_requests.push(KVCacheIndexInsertRequest {
                block_size: self.block_size,
                kv_cache_id: item.block_id,
                offset: item.offset,
                size: item.size,
                kv_cache_key_len: usize_to_u64(kv_cache_key.len()),
                kv_cache_key,
            });
        }

        let (tx, rx) = flume::unbounded::<Result<KVCacheResponse, KVCacheRequest<K>>>();
        let kv_cache_index_batch_insert_request = KVCacheRequest::KVCacheSmallRequest(
            KVCacheSmallRequest::KVCacheIndexBatchInsertRequest(KVCacheIndexBatchInsertRequest {
                batch_size: usize_to_u64(kv_cache_index_insert_requests.len()),
                indexes: kv_cache_index_insert_requests,
                node_address: node_address.into_bytes(),
            }),
        );
        debug!(
            "Insert indexes to the distribute cache with kv_cache_index_batch_insert_request: {:?}",
            kv_cache_index_batch_insert_request,
        );
        let packet = KVCacheRequestTask::new(
            ReqType::KVCacheIndexBatchInsertRequest.into(),
            kv_cache_index_batch_insert_request,
            tx.clone(),
        );
        let rpc_client = self.get_client(addr.clone()).await?;
        rpc_client.send_request(packet).await.map_err(|err| {
            DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to send request: {:?}", err)],
            }
        })?;

        match rx.recv_async().await {
            Ok(Ok(response)) => match response {
                KVCacheResponse::KVCacheSmallResponse(
                    KVCacheSmallResponse::KVCacheIndexInsertResponse(_),
                ) => Ok(()),
                _ => Err(DatenLordError::DistributeCacheManagerErr {
                    context: vec![format!("Failed to read block: {:?}", response)],
                }),
            },
            Ok(Err(err)) => Err(DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to read block: {:?}", err)],
            }),
            Err(_) => Err(DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to read block")],
            }),
        }
    }

    /// Remove a index from the distribute cache
    #[allow(clippy::wildcard_enum_match_arm)]
    async fn remove_index(&self, prefix: Vec<K>) -> DatenLordResult<()> {
        // Get the cache node with the block id
        let master_node = self.cluster_manager.get_master_node().await?;
        let addr = format!("{}:{}", master_node.ip(), master_node.port());

        let (tx, rx) = flume::unbounded::<Result<KVCacheResponse, KVCacheRequest<K>>>();
        let kv_cache_index_remove_request = KVCacheRequest::KVCacheSmallRequest(
            KVCacheSmallRequest::KVCacheIndexRemoveRequest(KVCacheIndexRemoveRequest {
                block_size: self.block_size,
                kv_cache_key: prefix,
            }),
        );
        let packet = KVCacheRequestTask::new(
            ReqType::KVCacheIndexRemoveRequest.into(),
            kv_cache_index_remove_request,
            tx.clone(),
        );
        let rpc_client = self.get_client(addr.clone()).await?;
        rpc_client.send_request(packet).await.map_err(|err| {
            DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to send request: {:?}", err)],
            }
        })?;

        match rx.recv_async().await {
            Ok(Ok(response)) => match response {
                KVCacheResponse::KVCacheSmallResponse(
                    KVCacheSmallResponse::KVCacheIndexRemoveResponse(_),
                ) => Ok(()),
                _ => Err(DatenLordError::DistributeCacheManagerErr {
                    context: vec![format!("Failed to read block: {:?}", response)],
                }),
            },
            Ok(Err(err)) => Err(DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to read block: {:?}", err)],
            }),
            Err(_) => Err(DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to read block")],
            }),
        }
    }

    /// Get the kv block from the distribute cache node
    #[allow(clippy::wildcard_enum_match_arm)]
    async fn get_block(&self, addr: String, kv_cache_id: u64) -> DatenLordResult<bytes::Bytes> {
        let (tx, rx) = flume::unbounded::<Result<KVCacheResponse, KVCacheRequest<K>>>();
        let kv_cache_request = KVCacheRequest::KVCacheSmallRequest(
            KVCacheSmallRequest::KVBlockGetRequest(KVBlockGetRequest {
                block_size: self.block_size,
                kv_cache_id,
            }),
        );
        let packet = KVCacheRequestTask::new(
            ReqType::KVBlockGetRequest.into(),
            kv_cache_request,
            tx.clone(),
        );
        let rpc_client = self.get_client(addr.clone()).await?;
        rpc_client.send_request(packet).await.map_err(|err| {
            DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to send request: {:?}", err)],
            }
        })?;

        match rx.recv_async().await {
            Ok(Ok(response)) => match response {
                // TODO: fix this get operation.
                KVCacheResponse::KVCacheLargeResponse(
                    KVCacheLargeResponse::KVBlockGetResponse(response),
                ) => {
                    debug!("Get block from remote cache");
                    // Return bytes here.
                    // return Ok(vec![]);
                    Ok(response.data)
                }
                _ => Err(DatenLordError::DistributeCacheManagerErr {
                    context: vec![format!("Failed to read block: {:?}", response)],
                }),
            },
            Ok(Err(err)) => Err(DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to read block: {:?}", err)],
            }),
            Err(_) => Err(DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to read block")],
            }),
        }
    }

    /// Batch put the kv block to the distribute cache node with same addr
    #[allow(clippy::wildcard_enum_match_arm)]
    async fn put_blocks(&self, addr: String, kv_blocks: Vec<KVBlock>) -> DatenLordResult<()> {
        // Create a Vec<KVBlockPutRequest> for batch put
        let mut kv_block_put_requests = Vec::new();
        for item in kv_blocks {
            kv_block_put_requests.push(KVBlockPutRequest {
                block_size: self.block_size,
                kv_cache_id: item.block_id,
                data: bytes::Bytes::from(item.data),
            });
        }

        let (tx, rx) = flume::unbounded::<Result<KVCacheResponse, KVCacheRequest<K>>>();
        let kv_cache_batch_put_request = KVCacheRequest::KVCacheLargeRequest(
            KVCacheLargeRequest::KVBlockBatchPutRequest(KVBlockBatchPutRequest {
                batch_size: usize_to_u64(kv_block_put_requests.len()),
                blocks: kv_block_put_requests,
            }),
        );
        let packet = KVCacheRequestTask::new(
            ReqType::KVBlockBatchPutRequest.into(),
            kv_cache_batch_put_request,
            tx.clone(),
        );

        let rpc_client = self.get_client(addr.clone()).await?;
        rpc_client.send_request(packet).await.map_err(|err| {
            DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to send request: {:?}", err)],
            }
        })?;

        match rx.recv_async().await {
            Ok(Ok(response)) => {
                match response {
                    KVCacheResponse::KVCacheSmallResponse(
                        KVCacheSmallResponse::KVBlockBatchPutResponse(response),
                    ) => {
                        // TODO: show block result here.
                        debug!("Batch put blocks: {:?}", response);
                        Ok(())
                    }
                    _ => Err(DatenLordError::DistributeCacheManagerErr {
                        context: vec![format!("Failed to read block: {:?}", response)],
                    }),
                }
            }
            Ok(Err(err)) => Err(DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to read block: {:?}", err)],
            }),
            Err(_) => Err(DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to read block")],
            }),
        }
    }

    /// Get a client from the cache manager
    async fn get_client(
        &self,
        addr: String,
    ) -> DatenLordResult<Arc<RpcClient<KVCacheRequestTask<K>>>> {
        let mut rpc_client_cache = self.rpc_client_cache.lock().await;
        if rpc_client_cache.contains_key(&addr) {
            let rpc_client =
                rpc_client_cache
                    .get(&addr)
                    .ok_or(DatenLordError::DistributeCacheManagerErr {
                        context: vec![format!("Failed to get rpc client")],
                    })?;

            // TODO: add client closed check here and remove it.
            Ok(rpc_client.to_owned())
        } else {
            let timeout_options = ClientTimeoutOptions {
                read_timeout: Duration::from_secs(10),
                write_timeout: Duration::from_secs(10),
                task_timeout: Duration::from_secs(10),
                keep_alive_timeout: Duration::from_secs(10),
            };
            let addr_clone = addr.clone();
            let connect_stream = connect_timeout!(addr_clone, timeout_options.read_timeout).await?;
            let rpc_client =
                RpcClient::<KVCacheRequestTask<K>>::new(connect_stream, &timeout_options);
            rpc_client.start_recv();

            // TODO: add ping into a loop.
            unsafe {
                rpc_client.ping().await.map_err(|err| {
                    DatenLordError::DistributeCacheManagerErr {
                        context: vec![format!("Failed to ping cache node: {:?}", err)],
                    }
                })?;
            }

            let client = Arc::new(rpc_client);
            rpc_client_cache.insert(addr.clone(), Arc::clone(&client));

            Ok(client)
        }
    }
}
