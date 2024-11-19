use std::sync::Arc;

use datenlord::{common::task_manager::{TaskName, TASK_MANAGER}, distribute_kv_cache::{cluster::{cluster_manager::ClusterManager, node::{Node, NodeStatus}}, local_cache::manager::{IndexManager, KVBlockManager}, manager::KVCacheHandler, rpc::{common::ServerTimeoutOptions, server::RpcServer, workerpool::WorkerPool}}, fs::kv_engine::{etcd_impl::EtcdKVEngine, KVEngine}, metrics};
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    TASK_MANAGER
        .spawn(TaskName::Metrics, metrics::start_metrics_server)
        .await?;

    let ip = "127.0.0.1";
    let port = 2789;
    let addr = format!("{}:{}", ip, port);
    let etcd_endpoint = "localhost:2379";
    let client = EtcdKVEngine::new(vec![etcd_endpoint.to_owned()])
    .await
    .unwrap();
    let client = Arc::new(client);

    let node = Node::new(ip.to_owned(), port, 1, NodeStatus::Initializing);
    let cluster_manager = Arc::new(ClusterManager::new(client, node));
    let cluster_manager_clone = Arc::clone(&cluster_manager);
    match cluster_manager_clone.run().await {
        Ok(()) => {},
        Err(err) => {
            panic!("Failed to run cluster manager: {:?}", err);
        }
    }

    let cache_manager = Arc::new(KVBlockManager::default());
    let index_manager = Arc::new(IndexManager::new());

    let pool = Arc::new(WorkerPool::new(5, 5));
    let handler = KVCacheHandler::new(Arc::clone(&pool), cache_manager, index_manager);
    let mut server = RpcServer::new(&ServerTimeoutOptions::default(), 5, 5, handler);
    match server.listen(&addr).await {
        Ok(()) => {},
        Err(err) => {
            panic!("Failed to start kv cache server: {:?}", err);
        },
    }

    info!("KV cache server stopped");
    Ok(())
}