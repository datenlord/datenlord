use std::{str::FromStr, sync::Arc};

use clap::Parser;
use datenlord::{common::{error::{DatenLordError, DatenLordResult}, logger::{init_logger, LogRole}, task_manager::{TaskName, TASK_MANAGER}}, distribute_kv_cache::{cluster::{cluster_manager::ClusterManager, node::{Node, NodeStatus}}, local_cache::manager::{IndexManager, KVBlockManager}, manager::KVCacheHandler, rpc::{common::ServerTimeoutOptions, server::RpcServer, workerpool::WorkerPool}}, fs::kv_engine::{etcd_impl::EtcdKVEngine, KVEngine}, metrics};
use tracing::{info, level_filters::LevelFilter};

#[derive(Debug, Parser)]
#[clap(author,version,about,long_about=None)]
pub struct KVCacheServerConfig {
    /// Log level
    #[clap(short = 'l', long = "log-level", value_name = "LEVEL", default_value = "info")]
    log_level: String,
    /// IP
    #[clap(short = 'i', long = "ip", value_name = "IP", default_value = "127.0.0.1")]
    ip: String,
    /// Port
    #[clap(short = 'p', long = "port", value_name = "PORT", default_value = "2789")]
    port: u16,
    /// ETCD endpoint
    #[clap(short = 'e', long = "etcd-endpoint", value_name = "ENDPOINT", default_value = "localhost:2379")]
    etcd_endpoint: String,
}

#[tokio::main]
async fn main() -> DatenLordResult<()> {
    TASK_MANAGER
        .spawn(TaskName::Metrics, metrics::start_metrics_server)
        .await
        .map_err(|e| {
            DatenLordError::DistributeCacheManagerErr {
                context: vec![format!("Failed to start metrics server: {:?}", e)],
            }
        })?;

    let config = KVCacheServerConfig::parse();
    init_logger(
        LogRole::Cache,
        LevelFilter::from_str(config.log_level.as_str()).map_err(|e| {
            DatenLordError::ArgumentInvalid {
                context: vec![format!("log level {} is invalid: {}", config.log_level, e)],
            }
        })?,
    );

    let ip = config.ip;
    let port = config.port;
    let addr = format!("{}:{}", ip, port);
    let etcd_endpoint = config.etcd_endpoint;
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