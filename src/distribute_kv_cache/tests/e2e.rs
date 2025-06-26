#[cfg(test)]
#[allow(clippy::unwrap_used)]
#[allow(clippy::indexing_slicing)]
#[allow(unused_imports)]
#[allow(clippy::unreadable_literal)]
#[allow(clippy::decimal_literal_representation)]
#[allow(clippy::unseparated_literal_suffix)]
#[allow(clippy::default_numeric_fallback)]
#[allow(clippy::assertions_on_result_states)]
mod tests {
    use std::sync::Arc;

    use crate::{
        distribute_kv_cache::{
            cluster::{
                cluster_manager::ClusterManager,
                node::{Node, NodeStatus},
            },
            kvclient::DistributeKVCacheClient,
            manager::KVCacheHandler,
            rpc::{common::ServerTimeoutOptions, server::RpcServer, workerpool::WorkerPool},
            server_cache::manager::{IndexManager, KVBlockManager},
        },
        fs::kv_engine::{etcd_impl::EtcdKVEngine, DeleteOption, KVEngine, KVEngineType, KeyType},
    };
    use std::sync::Once;

    use tracing::level_filters::LevelFilter;
    use tracing_subscriber::{
        filter, fmt::layer, layer::SubscriberExt, util::SubscriberInitExt, Layer,
    };

    const ETCD_ADDRESS: &str = "127.0.0.1:2379";
    static INIT: Once = Once::new();

    fn setup() {
        INIT.call_once(|| {
            let filter = filter::Targets::new().with_target(
                "datenlord::storage::distribute_kv_cache",
                LevelFilter::DEBUG,
            );
            tracing_subscriber::registry()
                .with(layer().with_filter(filter))
                .init();
        });
    }

    async fn clean_up_etcd() {
        KVEngineType::new(vec![ETCD_ADDRESS.to_owned()])
            .await
            .unwrap()
            .delete(
                &KeyType::CacheNode(String::new()),
                Some(DeleteOption {
                    prev_kv: false,
                    range_end: Some(vec![0xff]),
                }),
            )
            .await
            .unwrap();

        KVEngineType::new(vec![ETCD_ADDRESS.to_owned()])
            .await
            .unwrap()
            .delete(
                &KeyType::CacheMasterNode,
                Some(DeleteOption {
                    prev_kv: false,
                    range_end: Some(vec![0xff]),
                }),
            )
            .await
            .unwrap();
    }

    async fn start_node(ip: &str, port: u16, node_id: u32) -> Arc<ClusterManager> {
        let addr = format!("{ip}:{port}");
        let client = EtcdKVEngine::new(vec![ETCD_ADDRESS.to_owned()])
            .await
            .unwrap();
        let client = Arc::new(client);

        let node = Node::new(ip.to_owned(), port, node_id, NodeStatus::Initializing);
        let cluster_manager = Arc::new(ClusterManager::new(client, node));
        let cluster_manager_clone = Arc::clone(&cluster_manager);
        tokio::spawn(async move {
            cluster_manager_clone.run().await.unwrap();
        });

        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        cluster_manager
    }

    #[tokio::test]
    async fn test_distribute_kv_cache_e2e() {
        setup();
        clean_up_etcd().await;

        // Start three nodes
        let node1 = start_node("127.0.0.1", 2789, 1).await;
        let node2 = start_node("127.0.0.1", 2790, 2).await;
        let node3 = start_node("127.0.0.1", 2791, 3).await;

        // Verify master node is online
        assert!(node1.get_master_node().await.is_ok());

        // Simulate node2 going offline
        node2.set_status(NodeStatus::Offline).await.unwrap();

        // Setup the kv cache client
        let block_size = 16777216;
        let kvcacheclient = DistributeKVCacheClient::new(node1.clone(), block_size);
        kvcacheclient.start_watch().await.unwrap();

        // Test insert and read operations
        let prefix = [1, 2, 3, 1];
        let data = vec![1u8; 16777216];
        kvcacheclient.insert(prefix.to_vec(), data.clone()).await.unwrap();

        let (matched_prefix, buf) = kvcacheclient.try_load(prefix.to_vec()).await.unwrap();
        assert_eq!(matched_prefix, vec![1, 2, 3, 1]);
        assert_eq!(buf, data);

        // Bring node2 back online
        node2.set_status(NodeStatus::Online).await.unwrap();

        // Additional read/write tests can be added here
    }
}