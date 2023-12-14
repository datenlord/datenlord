//! FUSE async implementation

use std::sync::Arc;

use clippy_utilities::OverflowArithmetic;

use self::memfs::cache::policy::LruPolicy;
use self::memfs::cache::{BackendBuilder, BlockCoordinate, MemoryCacheBuilder, StorageManager};
use self::memfs::kv_engine::KVEngineType;
use crate::async_fuse::fuse::session;
use crate::AsyncFuseArgs;

pub mod fuse;
pub mod memfs;
/// Datenlord metrics
// Caused by prometheus macros
#[allow(clippy::ignored_unit_patterns)]
pub mod metrics;
pub mod proactor;
pub mod util;

/// Start async-fuse
pub async fn start_async_fuse(
    kv_engine: Arc<KVEngineType>,
    args: &AsyncFuseArgs,
) -> anyhow::Result<()> {
    metrics::start_metrics_server();

    memfs::kv_engine::kv_utils::register_node_id(
        &kv_engine,
        &args.node_id,
        &args.ip_address.to_string(),
        args.server_port,
    )
    .await?;

    let storage_config = &args.storage_config;

    let volume_info = serde_json::to_string(storage_config)?;
    memfs::kv_engine::kv_utils::register_volume(&kv_engine, &args.node_id, &volume_info).await?;

    let mount_point = std::path::Path::new(&args.mount_dir);
    let global_cache_capacity = args.storage_config.memory_cache_config.capacity;
    let storage = {
        let storage_param = &storage_config.params;
        let memory_cache_config = &storage_config.memory_cache_config;

        let block_size = storage_config.block_size;
        let capacity_in_blocks = memory_cache_config.capacity.overflow_div(block_size);

        let backend = BackendBuilder::new(storage_param.clone(), block_size).build()?;
        let lru_policy = LruPolicy::<BlockCoordinate>::new(capacity_in_blocks);
        let memory_cache = MemoryCacheBuilder::new(lru_policy, backend, block_size)
            .command_queue_limit(memory_cache_config.command_queue_limit)
            .limit(memory_cache_config.soft_limit)
            .write_through(!memory_cache_config.write_back)
            .build();
        StorageManager::new(memory_cache, block_size)
    };

    let fs: memfs::MemFs<memfs::S3MetaData> = memfs::MemFs::new(
        &args.mount_dir,
        global_cache_capacity,
        kv_engine,
        &args.node_id,
        storage_config,
        storage,
    )
    .await?;

    let ss = session::new_session_of_memfs(mount_point, fs).await?;
    ss.run().await?;

    Ok(())
}

#[cfg(test)]
mod test {
    mod integration_tests;
    mod test_util;

    use std::{fs, io};

    use futures::StreamExt;
    use tracing::debug;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_iter() -> io::Result<()> {
        let dir = tokio::task::spawn_blocking(|| fs::read_dir(".")).await??;
        let mut dir = futures::stream::iter(dir);
        while let Some(entry) = dir.next().await {
            let path = entry?.path();
            if path.is_file() {
                debug!("read file: {:?}", path);
                let buf = tokio::fs::read(path).await?;
                let output_length = 16;
                if buf.len() > output_length {
                    debug!(
                        "first {} bytes: {:?}",
                        output_length,
                        &buf.get(..output_length)
                    );
                } else {
                    debug!("total bytes: {:?}", buf);
                }
            } else {
                debug!("skip directory: {:?}", path);
            }
        }
        Ok(())
    }
}
