

pub struct DistributeCacheClient {
    pub cluster: Arc<Cluster>,
    pub rpc_client: RpcClient,
    pub cache: Arc<LocalCache>,
    pub config: DistributeCacheConfig,
    pub manager: DistributeCacheManager,
}