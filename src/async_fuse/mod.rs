//! FUSE async implementation

use crate::{common::etcd_delegate::EtcdDelegate, AsyncFuseArgs, VolumeType};
use fuse::session::Session;
use memfs::s3_wrapper::{DoNothingImpl, S3BackEndImpl};

pub mod fuse;
pub mod memfs;
/// Datenlord metrics
pub mod metrics;
pub mod proactor;
pub mod util;

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
            let fs: memfs::MemFs<memfs::DefaultMetaData> = memfs::MemFs::new(
                &args.mount_dir,
                args.cache_capacity,
                &args.ip_address.to_string(),
                &args.server_port,
                etcd_delegate,
                &args.node_id,
                &args.volume_info,
            )
            .await?;
            let ss = Session::new(mount_point, fs).await?;
            ss.run().await?;
        }
        VolumeType::S3 => {
            let fs: memfs::MemFs<memfs::S3MetaData<S3BackEndImpl>> = memfs::MemFs::new(
                &args.volume_info,
                args.cache_capacity,
                &args.ip_address.to_string(),
                &args.server_port,
                etcd_delegate,
                &args.node_id,
                &args.volume_info,
            )
            .await?;
            let ss = Session::new(mount_point, fs).await?;
            ss.run().await?;
        }
        VolumeType::None => {
            let fs: memfs::MemFs<memfs::S3MetaData<DoNothingImpl>> = memfs::MemFs::new(
                &args.volume_info,
                args.cache_capacity,
                &args.ip_address.to_string(),
                &args.server_port,
                etcd_delegate,
                &args.node_id,
                &args.volume_info,
            )
            .await?;
            let ss = Session::new(mount_point, fs).await?;
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
