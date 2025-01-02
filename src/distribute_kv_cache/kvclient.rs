use core::fmt;
use std::{collections::HashMap, sync::Arc, time::Duration};

use clippy_utilities::Cast;
use radix_trie::Trie;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use crate::{
    async_fuse::util::usize_to_u64,
    common::{
        error::{DatenLordError, DatenLordResult},
        task_manager::{TaskName, TASK_MANAGER},
    },
    connect_timeout,
    distribute_kv_cache::rpc::message,
};

use super::{
    cluster::cluster_manager::ClusterManager,
    rpc::{
        client::RpcClient,
        common::ClientTimeoutOptions,
        message::{
            KVBlockBatchPutRequest, KVBlockGetRequest, KVBlockPutRequest, KVCacheIdAllocateRequest,
            KVCacheIndexBatchInsertRequest, KVCacheIndexInsertRequest, KVCacheIndexMatchRequest,
            KVCacheIndexRemoveRequest, KVCachePacket, KVCacheRequest, KVCacheResponse, ReqType,
        },
        utils::u64_to_usize,
    },
};

/// Duration to check the rpc client cache
const RPC_CLIENT_CACHE_CHECK_DURATION: u64 = 60;

/// Unused kv block id
const UNUSED_KV_BLOCK_ID: u64 = 0;

/// KVBlock instance
///
/// |--------KVBlock-----| => single kv block data
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

/// KVCache instance metadata
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
    /// The block metas
    block_metas: Vec<KVCacheMeta<K>>,
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
                data: Vec::with_capacity(block_size as usize),
            },
            block_metas: Vec::new(),
            block_metas_tree: Trie::new(),
        }
    }

    /// Insert a kv cache to the local block cache
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

        // 3. Insert the kv cache meta to the block metas
        self.block_metas.push(kv_cache_meta);

        Ok(())
    }

    /// Get the block id
    pub fn get_block_id(&self) -> u64 {
        self.block_cache.block_id
    }

    /// Get next offset
    pub fn get_next_offset(&self) -> u64 {
        self.block_cache.data.len() as u64
    }

    /// Try to match local cache prefix.
    pub fn match_prefix(&self, prefix: Vec<K>) -> Option<KVCacheMeta<K>> {
        // Find the kv cache meta with the prefix
        match self.block_metas_tree.get_ancestor_key(&prefix) {
            Some(ancestor_key) => {
                let kv_cache_meta = self.block_metas_tree.get_ancestor_value(ancestor_key)?;
                return Some(kv_cache_meta.clone());
            }
            None => None,
        }
    }

    /// Try to get the block data from the local block cache
    ///
    /// TODO: Local cache may cover the remote block metas tree, consider to set a threshold to drop the query
    pub fn try_load(&self, kv_cache_meta: KVCacheMeta<K>) -> Option<Vec<u8>> {
        let offset = kv_cache_meta.offset.cast::<usize>();
        let size = kv_cache_meta.size.cast::<usize>();

        // Check the kv cache meta offset and size is valid
        if offset + size > self.block_cache.data.len() {
            return None;
        }

        let mut data = Vec::new();
        data.extend_from_slice(&self.block_cache.data[offset..offset + size]);

        return Some(data);
    }

    /// Clear the local block cache with new block id
    pub fn clear(&mut self, new_block_id: u64) {
        self.block_cache.block_id = new_block_id;
        self.block_cache.data.clear();
        self.block_metas.clear();

        // Clear the block metas tree
        self.block_metas_tree = Trie::new();
    }

    /// Get KVBlock
    pub fn get_kv_block(&self) -> KVBlock {
        let mut current_block = self.block_cache.clone();
        // Return a copy of the block data
        current_block
            .data
            .resize(u64_to_usize(self.block_cache.block_size), 0);

        current_block
    }

    /// Get KVCacheMeta
    pub fn get_kv_cache_metas(&self) -> Vec<KVCacheMeta<K>> {
        self.block_metas.clone()
    }
}

/// The distribute cache client
#[derive(Debug, Clone)]
pub struct DistributeKVCacheClient<K>
where
    K: num::Num + Eq + Send + Sync + Clone + fmt::Debug + 'static,
{
    /// The distribute cache inner
    inner: DistributeKVCacheClientInner<K>,
    /// Single block cache, used to collect the block data from the infer side.
    block_cache: Arc<Mutex<LocalBlockCache<K>>>,
}

impl<K> DistributeKVCacheClient<K>
where
    K: num::Num + Eq + Send + Sync + Clone + fmt::Debug + 'static,
    Vec<K>: radix_trie::TrieKey + Clone,
{
    /// Create a new distribute cache client
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
        let start = tokio::time::Instant::now();
        let local_cache_lock = self.block_cache.lock().await;
        let local_kv_cache_meta = match local_cache_lock.match_prefix(prefix.clone()) {
            Some(kv_cache_meta) => {
                debug!(
                    "Matched local kv cache meta from local cache: {:?}",
                    kv_cache_meta
                );
                kv_cache_meta
            }
            None => {
                debug!("Failed to match local kv cache meta from local cache, try to get from the distribute cache");
                // Empty meta
                KVCacheMeta::default()
            }
        };
        let local_prefix_len = local_kv_cache_meta.prefix.len().cast::<u64>();
        let start_1 = start.elapsed();
        debug!("local_cache_lock.match_prefix(prefix.clone()) check Time cost: {:?}", start_1);

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
                error!(
                    "Failed to match prefix: {:?} with error: {:?}",
                    prefix, err
                );
                // Return a empty data
                (KVCacheMeta::default(), String::new())
            }
        };
        let start_2 = start.elapsed();
        debug!("self.inner.match_prefix(prefix.clone()).await check Time cost: {:?}", start_2 - start_1);

        let remote_prefix_len = kv_block_meta.prefix.len().cast::<u64>();
        // println!(
        //     "Matched remote kv block meta: {:?} node_address: {:?}",
        //     kv_block_meta, node_address
        // );
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
        let start = tokio::time::Instant::now();
        let local_cache_lock = self.block_cache.lock().await;
        let local_kv_cache_meta = match local_cache_lock.match_prefix(prefix.clone()) {
            Some(kv_cache_meta) => {
                debug!(
                    "Matched local kv cache meta from local cache: {:?}",
                    kv_cache_meta
                );
                kv_cache_meta
            }
            None => {
                debug!("Failed to match local kv cache meta from local cache, try to get from the distribute cache");
                // Empty meta
                KVCacheMeta::default()
            }
        };
        let local_prefix_len = local_kv_cache_meta.prefix.len().cast::<u64>();
        let start_1 = start.elapsed();
        debug!("local_cache_lock.match_prefix(prefix.clone()) check Time cost: {:?}", start_1);

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
                error!(
                    "Failed to match prefix: {:?} with error: {:?}",
                    prefix, err
                );
                // Return a empty data
                (KVCacheMeta::default(), String::new())
            }
        };
        let start_2 = start.elapsed();
        debug!("self.inner.match_prefix(prefix.clone()).await check Time cost: {:?}", start_2 - start_1);

        let remote_prefix_len = kv_block_meta.prefix.len().cast::<u64>();
        // println!(
        //     "Matched remote kv block meta: {:?} node_address: {:?}",
        //     kv_block_meta, node_address
        // );
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
            } else {
                debug!(
                    "Failed to get block from local cache, try to get from the distribute cache"
                );
            }
        }

        // Empty address from the distribute cache
        if node_address.len() == 0 {
            error!("Failed to get block, node address is empty");
            return Ok((Vec::new(), bytes::Bytes::new()));
        }
        let start_3 = start.elapsed();
        debug!("local_cache_lock.try_load(local_kv_cache_meta.clone()) check Time cost: {:?}", start_3 - start_2);


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
        let start_4 = start.elapsed();
        debug!("self.inner.get_block(node_address.clone(), kv_block_meta.block_id) check Time cost: {:?}", start_4 - start_3);

        // return Ok((Vec::new(), Vec::new()));
        // return Ok((String::new(), block_data));

        // 4. Copy the block data to the data with kv cache meta offset and size
        let offset = kv_block_meta.offset as usize;
        let size = kv_block_meta.size as usize;
        debug!(
            "Get block from remote cache: {:?} offset: {:?} size: {:?}",
            kv_block_meta, offset, size
        );
        // TODO: Check range or update with bytesmut
        // let mut data = Vec::with_capacity(size);
        // data.extend_from_slice(&block_data[offset..offset + size]);

        Ok((kv_block_meta.prefix, block_data.slice(offset..offset + size)))
    }

    /// Insert a block to the distribute cache
    pub async fn insert(&self, prefix: Vec<K>, data: Vec<u8>) -> DatenLordResult<()> {
        let start = tokio::time::Instant::now();
        // Check current size is valid
        if data.len().cast::<u64>() > self.inner.block_size {
            return Err(DatenLordError::DistributeCacheManagerErr {
                context: vec![format!(
                    "Failed to insert kv cache to the block cache, data size is too large"
                )],
            });
        }

        let start_1 = start.elapsed();
        debug!("self.inner.block_size check Time cost: {:?}", start_1);

        // 1. fill current block cache
        let mut block_cache = self.block_cache.lock().await;
        let mut current_block_id = block_cache.get_block_id();
        debug!(
            "Insert kv cache to the block cache with block id: {:?}",
            current_block_id
        );
        let start_2 = start.elapsed();
        debug!("block_cache.get_block_id() check Time cost: {:?}", start_2 - start_1);

        while current_block_id == UNUSED_KV_BLOCK_ID {
            let new_block_id = self.inner.alloc_block_id().await?;
            debug!("Alloc new block id: {:?}", new_block_id);
            block_cache.clear(new_block_id);
            current_block_id = new_block_id;
        }
        let start_3 = start.elapsed();
        debug!("block_cache.clear(new_block_id) check Time cost: {:?}", start_3 - start_2);

        let next_offset = block_cache.get_next_offset();
        let kv_cache_meta = KVCacheMeta {
            block_id: current_block_id,
            offset: next_offset,
            size: data.len().cast(),
            prefix: prefix.clone(),
        };
        let start_4 = start.elapsed();
        debug!("block_cache.get_next_offset() check Time cost: {:?}", start_4 - start_3);

        match block_cache.insert(kv_cache_meta, &data) {
            Ok(()) => {
                debug!(
                    "Insert kv cache to the block cache with block id: {:?} successfully",
                    current_block_id
                );
                let start_5 = start.elapsed();
                debug!("block_cache.insert(kv_cache_meta, &data) check Time cost: {:?}", start_5 - start_4);
            }
            Err(err) => {
                debug!("Failed to insert kv cache to the block cache: {:?}, try to allocate new block cache", err);
                // 2. current block cache is full, we will insert this block to the distribute cache
                // and clear the block cache
                let kv_block = block_cache.get_kv_block();
                let kv_cache_metas = block_cache.get_kv_cache_metas();

                let start_6 = start.elapsed();
                debug!("block_cache.get_kv_block() check Time cost: {:?}", start_6 - start_4);

                // Insert the block to the distribute cache
                let node = self
                    .inner
                    .cluster_manager
                    .get_node(kv_block.block_id.to_string())
                    .await?;

                let start_6_2 = start.elapsed();
                debug!("self.inner.cluster_manager.get_node(kv_block.block_id.to_string()) check Time cost: {:?}", start_6_2 - start_6);

                let addr = format!("{}:{}", node.ip(), node.port());
                self.inner.put_blocks(addr.clone(), vec![kv_block]).await?;

                let start_7 = start.elapsed();
                debug!("self.inner.put_blocks(addr.clone(), vec![kv_block]) check Time cost: {:?}", start_7 - start_6_2);

                // If ok, insert the indexes to the distribute cache
                self.inner.insert_indexes(kv_cache_metas, addr).await?;

                let start_8 = start.elapsed();
                debug!("self.inner.insert_indexes(kv_cache_metas, addr).await? check Time cost: {:?}", start_8 - start_7);

                // If ok, clear the block cache
                let current_block_id = self.inner.alloc_block_id().await?;
                block_cache.clear(current_block_id);

                let start_9 = start.elapsed();
                debug!("block_cache.clear(current_block_id) check Time cost: {:?}", start_9 - start_8);

                // Try to insert the kv cache meta again
                let next_offset = block_cache.get_next_offset();
                let kv_cache_meta = KVCacheMeta {
                    block_id: current_block_id,
                    offset: next_offset,
                    size: data.len().cast(),
                    prefix: prefix.clone(),
                };
                block_cache.insert(kv_cache_meta, &data)?;

                let start_10 = start.elapsed();
                debug!("block_cache.insert(kv_cache_meta, &data) check Time cost: {:?}", start_10 - start_9);
            }
        }

        Ok(())
    }
}

/// The distribute cache client
#[derive(Debug, Clone)]
pub struct DistributeKVCacheClientInner<K>
where
    K: num::Num + Eq + Send + Sync + Clone + fmt::Debug + 'static,
{
    /// The cluster manager, only used to watch hashring changes
    cluster_manager: Arc<ClusterManager>,
    /// The rpc client cache
    /// TODO: Change to trait
    rpc_client_cache: Arc<Mutex<HashMap<String, Arc<RpcClient<KVCachePacket<K>>>>>>,
    /// Block size
    block_size: u64,
}

impl<K> DistributeKVCacheClientInner<K>
where
    K: num::Num + Eq + Send + Sync + Clone + fmt::Debug + 'static,
{
    /// Create a new distribute cache client
    pub fn new(cluster_manager: Arc<ClusterManager>, block_size: u64) -> Self {
        let rpc_client_cache = Arc::new(Mutex::new(HashMap::new()));
        Self {
            cluster_manager,
            rpc_client_cache,
            block_size,
        }
    }

    /// Start the distribute cache client watch task
    pub async fn start_watch(&self) -> DatenLordResult<()> {
        // Start the watch task
        let cluster_manager = self.cluster_manager.clone();
        TASK_MANAGER
            .spawn(TaskName::AsyncFuse, |token| async move {
                match cluster_manager.watch_ring(token).await {
                    Ok(_) => {}
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
        let cluster_manager = self.cluster_manager.clone();
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

        let rpc_client_cache = self.rpc_client_cache.clone();
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
    async fn alloc_block_id(&self) -> DatenLordResult<u64> {
        // Get the cache node with the block id
        let master_node = self.cluster_manager.get_master_node().await?;
        let addr = format!("{}:{}", master_node.ip(), master_node.port());
        debug!("Alloc block id from the master node: {:?}", addr);

        let (tx, rx) = flume::unbounded::<Result<KVCacheResponse, KVCacheRequest<K>>>();
        let kv_cache_id_allocate_request =
            KVCacheRequest::KVCacheIdAllocateRequest(KVCacheIdAllocateRequest {
                block_size: self.block_size,
            });
        let packet = KVCachePacket::new(
            ReqType::KVCacheIdAllocateRequest.to_u8(),
            kv_cache_id_allocate_request,
            tx.clone(),
        );
        let rpc_client: Arc<RpcClient<KVCachePacket<K>>> = self.get_client(addr.clone()).await?;
        rpc_client.send_request(packet).await.map_err(|err| {
            DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to send request: {:?}", err)],
            }
        })?;

        match rx.recv_async().await {
            Ok(Ok(response)) => match response {
                KVCacheResponse::KVCacheIdAllocateResponse(response) => {
                    return Ok(response.kv_cache_id);
                }
                _ => {
                    return Err(DatenLordError::DistributeCacheManagerErr {
                        context: vec![format!("Failed to read block: {:?}", response)],
                    });
                }
            },
            Ok(Err(err)) => {
                return Err(DatenLordError::DistributeCacheManagerErr {
                    context: vec![format!("Failed to read block: {:?}", err)],
                });
            }
            Err(_) => {
                return Err(DatenLordError::DistributeCacheManagerErr {
                    context: vec![format!("Failed to read block")],
                });
            }
        }
    }

    /// Match the prefix and get block id from the distribute cache
    /// return the kv cache meta info and remote node address
    ///
    //. If not find, will return a empty string
    async fn match_prefix(&self, prefix: Vec<K>) -> DatenLordResult<(KVCacheMeta<K>, String)> {
        let mut raw_prefix = prefix.clone();
        // Get the cache node with the block id
        let master_node = self.cluster_manager.get_master_node().await?;
        let addr = format!("{}:{}", master_node.ip(), master_node.port());

        let (tx, rx) = flume::unbounded::<Result<KVCacheResponse, KVCacheRequest<K>>>();
        let kv_cache_index_match_request =
            KVCacheRequest::KVCacheIndexMatchRequest(KVCacheIndexMatchRequest {
                block_size: self.block_size,
                kv_cache_key: prefix,
            });
        let packet = KVCachePacket::new(
            ReqType::KVCacheIndexMatchRequest.to_u8(),
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
                KVCacheResponse::KVCacheIndexMatchResponse(response) => {
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
                    return Ok((
                        KVCacheMeta {
                            block_id: response.kv_cache_id,
                            offset: response.offset,
                            size: response.size,
                            // Get sub string from the raw prefix
                            prefix: raw_prefix,
                        },
                        node_address,
                    ));
                }
                _ => {
                    return Err(DatenLordError::DistributeCacheManagerErr {
                        context: vec![format!("Failed to read block: {:?}", response)],
                    });
                }
            },
            Ok(Err(err)) => {
                return Err(DatenLordError::DistributeCacheManagerErr {
                    context: vec![format!("Failed to read block: {:?}", err)],
                });
            }
            Err(_) => {
                return Err(DatenLordError::DistributeCacheManagerErr {
                    context: vec![format!("Failed to read block")],
                });
            }
        }
    }

    /// Insert a index to the distribute cache
    async fn insert_indexes(
        &self,
        kv_cache_meta_list: Vec<KVCacheMeta<K>>,
        node_address: String,
    ) -> DatenLordResult<()> {
        // Get the cache node with the block id
        let master_node = self.cluster_manager.get_master_node().await?;
        let addr = format!("{}:{}", master_node.ip(), master_node.port());

        // Generate Vec<KVCacheIndexInsertRequest>
        let mut kv_cache_index_insert_requests = Vec::new();
        for item in kv_cache_meta_list.into_iter() {
            let kv_cache_key = item.prefix;
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
        let kv_cache_index_batch_insert_request =
            KVCacheRequest::KVCacheIndexBatchInsertRequest(KVCacheIndexBatchInsertRequest {
                batch_size: kv_cache_index_insert_requests.len() as u64,
                indexes: kv_cache_index_insert_requests,
                node_address: node_address.into_bytes(),
            });
        debug!(
            "Insert indexes to the distribute cache with kv_cache_index_batch_insert_request: {:?}",
            kv_cache_index_batch_insert_request,
        );
        let packet = KVCachePacket::new(
            ReqType::KVCacheIndexBatchInsertRequest.to_u8(),
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
                KVCacheResponse::KVCacheIndexInsertResponse(_) => {
                    return Ok(());
                }
                _ => {
                    return Err(DatenLordError::DistributeCacheManagerErr {
                        context: vec![format!("Failed to read block: {:?}", response)],
                    });
                }
            },
            Ok(Err(err)) => {
                return Err(DatenLordError::DistributeCacheManagerErr {
                    context: vec![format!("Failed to read block: {:?}", err)],
                });
            }
            Err(_) => {
                return Err(DatenLordError::DistributeCacheManagerErr {
                    context: vec![format!("Failed to read block")],
                });
            }
        }
    }

    /// Remove a index from the distribute cache
    async fn remove_index(&self, prefix: Vec<K>) -> DatenLordResult<()> {
        // Get the cache node with the block id
        let master_node = self.cluster_manager.get_master_node().await?;
        let addr = format!("{}:{}", master_node.ip(), master_node.port());

        let (tx, rx) = flume::unbounded::<Result<KVCacheResponse, KVCacheRequest<K>>>();
        let kv_cache_index_remove_request =
            KVCacheRequest::KVCacheIndexRemoveRequest(KVCacheIndexRemoveRequest {
                block_size: self.block_size,
                kv_cache_key: prefix,
            });
        let packet = KVCachePacket::new(
            ReqType::KVCacheIndexRemoveRequest.to_u8(),
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
                KVCacheResponse::KVCacheIndexRemoveResponse(_) => {
                    return Ok(());
                }
                _ => {
                    return Err(DatenLordError::DistributeCacheManagerErr {
                        context: vec![format!("Failed to read block: {:?}", response)],
                    });
                }
            },
            Ok(Err(err)) => {
                return Err(DatenLordError::DistributeCacheManagerErr {
                    context: vec![format!("Failed to read block: {:?}", err)],
                });
            }
            Err(_) => {
                return Err(DatenLordError::DistributeCacheManagerErr {
                    context: vec![format!("Failed to read block")],
                });
            }
        }
    }

    /// Get the kv block from the distribute cache node
    async fn get_block(&self, addr: String, kv_cache_id: u64) -> DatenLordResult<bytes::Bytes> {
        let (tx, rx) = flume::unbounded::<Result<KVCacheResponse, KVCacheRequest<K>>>();
        let kv_cache_request = KVCacheRequest::KVBlockGetRequest(KVBlockGetRequest {
            block_size: self.block_size,
            kv_cache_id,
        });
        let packet = KVCachePacket::new(
            ReqType::KVBlockGetRequest.to_u8(),
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
                KVCacheResponse::KVBlockGetResponse(response) => {
                    debug!("Get block from remote cache");
                    // Return bytes here.
                    return Ok(response.data);
                    // return Ok(vec![]);
                }
                _ => {
                    return Err(DatenLordError::DistributeCacheManagerErr {
                        context: vec![format!("Failed to read block: {:?}", response)],
                    });
                }
            },
            Ok(Err(err)) => {
                return Err(DatenLordError::DistributeCacheManagerErr {
                    context: vec![format!("Failed to read block: {:?}", err)],
                });
            }
            Err(_) => {
                return Err(DatenLordError::DistributeCacheManagerErr {
                    context: vec![format!("Failed to read block")],
                });
            }
        }
    }

    /// Batch put the kv block to the distribute cache node with same addr
    async fn put_blocks(&self, addr: String, kv_blocks: Vec<KVBlock>) -> DatenLordResult<()> {
        let start = tokio::time::Instant::now();
        // Create a Vec<KVBlockPutRequest> for batch put
        let mut kv_block_put_requests = Vec::new();
        for item in kv_blocks.into_iter() {
            kv_block_put_requests.push(KVBlockPutRequest {
                block_size: self.block_size,
                kv_cache_id: item.block_id,
                data: bytes::Bytes::from(item.data),
            });
        }
        let start_1 = start.elapsed();
        debug!("kv_blocks.into_iter() check Time cost: {:?}", start_1);

        let (tx, rx) = flume::unbounded::<Result<KVCacheResponse, KVCacheRequest<K>>>();
        let kv_cache_batch_put_request =
            KVCacheRequest::KVBlockBatchPutRequest(KVBlockBatchPutRequest {
                batch_size: kv_block_put_requests.len() as u64,
                blocks: kv_block_put_requests,
            });
        let packet = KVCachePacket::new(
            ReqType::KVBlockBatchPutRequest.to_u8(),
            kv_cache_batch_put_request,
            tx.clone(),
        );
        let start_2 = start.elapsed();
        debug!("KVCachePacket::new check Time cost: {:?}", start_2 - start_1);

        let rpc_client = self.get_client(addr.clone()).await?;
        rpc_client.send_request(packet).await.map_err(|err| {
            DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to send request: {:?}", err)],
            }
        })?;

        let start_3 = start.elapsed();
        debug!("rpc_client.send_request(packet) check Time cost: {:?}", start_3 - start_2);

        match rx.recv_async().await {
            Ok(Ok(response)) => {
                match response {
                    KVCacheResponse::KVBlockBatchPutResponse(response) => {
                        let start_4 = start.elapsed();
                        debug!("KVCacheResponse::KVBlockBatchPutResponse(response) check Time cost: {:?}", start_4 - start_3);
                        // TODO: show block result here.
                        debug!("Batch put blocks: {:?}", response);
                        return Ok(());
                    }
                    _ => {
                        return Err(DatenLordError::DistributeCacheManagerErr {
                            context: vec![format!("Failed to read block: {:?}", response)],
                        });
                    }
                }
            }
            Ok(Err(err)) => {
                return Err(DatenLordError::DistributeCacheManagerErr {
                    context: vec![format!("Failed to read block: {:?}", err)],
                });
            }
            Err(_) => {
                return Err(DatenLordError::DistributeCacheManagerErr {
                    context: vec![format!("Failed to read block")],
                });
            }
        }
    }

    async fn get_client(&self, addr: String) -> DatenLordResult<Arc<RpcClient<KVCachePacket<K>>>> {
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
            let rpc_client = RpcClient::<KVCachePacket<K>>::new(connect_stream, &timeout_options);
            rpc_client.start_recv();

            // TODO: add ping into a loop.
            rpc_client
                .ping()
                .await
                .map_err(|err| DatenLordError::DistributeCacheManagerErr {
                    context: vec![format!("Failed to ping cache node: {:?}", err)],
                })?;

            let client = Arc::new(rpc_client);
            rpc_client_cache.insert(addr.clone(), Arc::clone(&client));

            Ok(client)
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use crate::{
        distribute_kv_cache::{
            cluster::{cluster_manager::ClusterManager, node::Node},
            kvclient::DistributeKVCacheClientInner,
            local_cache::manager::{IndexManager, KVBlockManager},
            manager::KVCacheHandler,
            rpc::{common::ServerTimeoutOptions, server::RpcServer, workerpool::WorkerPool},
        },
        fs::kv_engine::{etcd_impl::EtcdKVEngine, KVEngine},
    };

    use super::{KVCacheMeta, LocalBlockCache};

    /// Test local block cache operations
    /// 1. Insert kv cache to the local block cache
    /// 2. Get the block id
    /// 3. Get the next offset
    /// 4. Clear the local block cache with new block id
    /// 5. Get KVBlock
    /// 6. Get KVCacheMeta
    #[tokio::test]
    async fn test_local_block_cache() {
        // Create a new local block cache with 64 block size
        let mut local_block_cache = LocalBlockCache::new(64);

        let fetched_block_id = 1;
        local_block_cache.clear(fetched_block_id);

        let data = vec![1u8; 10];
        let prefix1 = vec![1_u32, 2_u32, 3_u32];
        let offset = local_block_cache.get_next_offset();
        let kv_cache_meta = KVCacheMeta {
            block_id: fetched_block_id,
            offset: offset,
            size: data.len() as u64,
            prefix: prefix1.clone(),
        };
        let insert_result = local_block_cache.insert(kv_cache_meta, &data).is_ok();
        assert!(insert_result);

        let data = vec![2u8; 30];
        let prefix2 = vec![1_u32, 2_u32, 4_u32];
        let offset = local_block_cache.get_next_offset();
        let kv_cache_meta = KVCacheMeta {
            block_id: fetched_block_id,
            offset: offset,
            size: data.len() as u64,
            prefix: prefix2.clone(),
        };
        let insert_result = local_block_cache.insert(kv_cache_meta, &data).is_ok();
        assert!(insert_result);

        // Current local block cache is full
        let data = vec![3u8; 30];
        let prefix3 = vec![1_u32, 2_u32, 5_u32];
        let offset = local_block_cache.get_next_offset();
        let kv_cache_meta = KVCacheMeta {
            block_id: fetched_block_id,
            offset: offset,
            size: data.len() as u64,
            prefix: prefix3,
        };
        let insert_result = local_block_cache.insert(kv_cache_meta, &data).is_ok();
        assert!(!insert_result);

        let current_kv_block = local_block_cache.get_kv_block();
        assert_eq!(current_kv_block.block_id, fetched_block_id);
        assert_eq!(current_kv_block.data.len(), 40);
        assert_eq!(current_kv_block.data[0], 1u8);
        assert_eq!(current_kv_block.data[10], 2u8);

        let current_kv_cache_metas = local_block_cache.get_kv_cache_metas();
        assert_eq!(current_kv_cache_metas.len(), 2);
        assert_eq!(current_kv_cache_metas[0].block_id, fetched_block_id);
        assert_eq!(current_kv_cache_metas[0].offset, 0);
        assert_eq!(current_kv_cache_metas[0].size, 10);
        assert_eq!(current_kv_cache_metas[0].prefix, prefix1);
        assert_eq!(current_kv_cache_metas[1].block_id, fetched_block_id);
        assert_eq!(current_kv_cache_metas[1].offset, 10);
        assert_eq!(current_kv_cache_metas[1].size, 30);
        assert_eq!(current_kv_cache_metas[1].prefix, prefix2);

        // Clean the local block cache
        let new_block_id = 2;
        local_block_cache.clear(new_block_id);
        assert_eq!(local_block_cache.get_block_id(), new_block_id);
        assert_eq!(local_block_cache.get_next_offset(), 0);
        assert_eq!(local_block_cache.get_kv_block().block_id, new_block_id);
        assert_eq!(local_block_cache.get_kv_cache_metas().len(), 0);
    }

    /// Test get client can return an available client.
    #[tokio::test]
    async fn test_get_client() {
        // Start a mocked rpc server
        let ip = "127.0.0.1";
        let port = 2889;
        let addr = format!("{}:{}", ip, port);
        let cache_manager = Arc::new(KVBlockManager::default());
        let index_manager = Arc::new(IndexManager::<u32>::new());
        let pool = Arc::new(WorkerPool::new(5, 5));
        let handler = KVCacheHandler::new(Arc::clone(&pool), cache_manager, index_manager);
        let mut server = RpcServer::new(&ServerTimeoutOptions::default(), 5, 5, handler);
        server.listen(&addr).await.unwrap();

        let etcd_endpoint = "localhost:2379";
        let client = EtcdKVEngine::new(vec![etcd_endpoint.to_owned()])
            .await
            .unwrap();
        let client = Arc::new(client);
        let node = Node::default();
        let distribute_kvcache_client_inner =
            DistributeKVCacheClientInner::<u32>::new(Arc::new(ClusterManager::new(client, node)), 64);

        let res = distribute_kvcache_client_inner.get_client(addr).await;
        assert!(res.is_ok());
    }
}
