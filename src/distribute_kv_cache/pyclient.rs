use std::{str::FromStr, sync::Arc};

use clap::Parser;
use clippy_utilities::Cast;
use datenlord::{
    common::{
        error::{DatenLordError, DatenLordResult},
        logger::{init_logger, LogRole},
    },
    distribute_kv_cache::{
        cluster::{
            cluster_manager::ClusterManager,
            node::Node,
        }, kvclient::DistributeKVCacheClient,
    },
    fs::kv_engine::{etcd_impl::EtcdKVEngine, KVEngine, KVEngineType},
    // metrics,
};
use tracing::{debug, error, info, level_filters::LevelFilter};

#[derive(Debug, Parser)]
#[clap(author,version,about,long_about=None)]
pub struct KVCacheClientConfig {
    /// Log level
    #[clap(
        short = 'l',
        long = "log-level",
        value_name = "LEVEL",
        default_value = "error"
    )]
    log_level: String,
    /// Block size, default is 16MB
    #[clap(
        short = 'b',
        long = "block_size",
        value_name = "BLOCK_SIZE",
        default_value = "16777216"
    )]
    block_size: u64,
    /// ETCD endpoint
    #[clap(
        short = 'e',
        long = "etcd-endpoint",
        value_name = "ENDPOINT",
        default_value = "localhost:2379"
    )]
    etcd_endpoint: String,
    /// Op type
    #[clap(
        short = 'o',
        long = "op-type",
        value_name = "OP_TYPE",
        default_value = "read"
    )]
    op_type: String,
}

#[tokio::main]
async fn main() -> DatenLordResult<()> {
    let config = KVCacheClientConfig::parse();
    init_logger(
        LogRole::SDK,
        LevelFilter::from_str(config.log_level.as_str()).map_err(|e| {
            DatenLordError::ArgumentInvalid {
                context: vec![format!("log level {} is invalid: {}", config.log_level, e)],
            }
        })?,
    );

    let kv_engine = match KVEngineType::new(vec![config.etcd_endpoint.clone()]).await {
        Ok(kv_engine) => kv_engine,
        Err(e) => {
            panic!("Failed to create KVEngine: {:?}", e);
        }
    };

    let kv_engine: Arc<EtcdKVEngine> = Arc::new(kv_engine);
    let node = Node::default();
    let cluster_manager = Arc::new(ClusterManager::new(kv_engine, node));

    let kvcacheclient = Arc::new(DistributeKVCacheClient::new(cluster_manager, config.block_size));
    let kvcacheclient_clone = Arc::clone(&kvcacheclient);
    match kvcacheclient_clone.start_watch().await {
        Ok(()) => {
            info!("DistributeKVCacheClient start_watch ok");
        }
        Err(e) => {
            panic!("start_watch failed: {:?}", e);
        }
    }

    let start = tokio::time::Instant::now();
    for _ in 0..100 {
        match config.op_type.as_str() {
            "read" => {
                let key = "key".to_string();
                let (prefix, value) = kvcacheclient.try_load(key.clone()).await.unwrap();
                debug!("Get key: {} value len: {:?}", prefix, value.len());
            }
            "write" => {
                let key = "key".to_string();
                let value  = vec![0_u8; config.block_size.cast()];
                kvcacheclient.insert(key.clone(), value).await.unwrap();
            }
            _ => {
                println!("Invalid op type: {}", config.op_type);
            }
        }
    }


    let end = start.elapsed();
    info!("Total time: {:?}", end);
    info!("Throughput: {:?} MB/s", ((config.block_size * 100) as f64) / 1024.0 / 1024.0 / (end.as_secs() as f64));

    // task_manager::wait_for_shutdown(&TASK_MANAGER)?.await;
    info!("KV cache server stopped");
    Ok(())
}
