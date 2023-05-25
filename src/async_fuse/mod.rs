//! FUSE async implementation

use self::fuse::file_system::FsController;
use crate::async_fuse::fuse::session;
use crate::{common::etcd_delegate::EtcdDelegate, AsyncFuseArgs, VolumeType};
use memfs::s3_wrapper::{DoNothingImpl, S3BackEndImpl};
// use std::sync::Arc;

pub mod fuse;
pub mod memfs;
/// Datenlord metrics
pub mod metrics;
pub mod proactor;
pub mod util;

use memfs::kv_engine::EtcdKVEngine;

/// Start async-fuse
pub async fn start_async_fuse(
    etcd_delegate: EtcdDelegate,
    args: &AsyncFuseArgs,
) -> anyhow::Result<()> {
    metrics::start_metrics_server();

    memfs::dist::etcd::register_node_id(
        &etcd_delegate,
        &args.node_id,
        &args.ip_address.to_string(),
        &args.server_port,
    )
    .await?;
    memfs::dist::etcd::register_volume(&etcd_delegate, &args.node_id, &args.volume_info).await?;
    let mount_point = std::path::Path::new(&args.mount_dir);
    match args.volume_type {
        VolumeType::Local => {
            let (fs, fs_controller): (memfs::MemFs<memfs::DefaultMetaData>, FsController) =
                memfs::MemFs::new(
                    &args.mount_dir,
                    args.cache_capacity,
                    &args.ip_address.to_string(),
                    &args.server_port,
                    etcd_delegate,
                    &args.node_id,
                    &args.volume_info,
                )
                .await?;

            let ss = session::new_session_of_memfs(mount_point, fs, fs_controller).await?;
            ss.run().await?;
        }
        VolumeType::S3 => {
            let (fs, fs_controller): (
                memfs::MemFs<memfs::S3MetaData<S3BackEndImpl, EtcdKVEngine>>,
                FsController,
            ) = memfs::MemFs::new(
                &args.volume_info,
                args.cache_capacity,
                &args.ip_address.to_string(),
                &args.server_port,
                etcd_delegate,
                &args.node_id,
                &args.volume_info,
            )
            .await?;

            let ss = session::new_session_of_memfs(mount_point, fs, fs_controller).await?;
            ss.run().await?;
        }
        VolumeType::None => {
            let (fs, fs_controller): (
                memfs::MemFs<memfs::S3MetaData<DoNothingImpl, EtcdKVEngine>>,
                FsController,
            ) = memfs::MemFs::new(
                &args.volume_info,
                args.cache_capacity,
                &args.ip_address.to_string(),
                &args.server_port,
                etcd_delegate,
                &args.node_id,
                &args.volume_info,
            )
            .await?;

            let ss = session::new_session_of_memfs(mount_point, fs, fs_controller).await?;
            ss.run().await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    mod integration_tests;
    mod test_util;

    use log::debug;
    use std::fs;
    use std::io;

    use futures::StreamExt;

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
