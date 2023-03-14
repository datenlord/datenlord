use crate::async_fuse::fuse::protocol::INum;
use crate::async_fuse::memfs::RenameParam;

use super::super::dir::DirEntry;
use super::super::fs_util::FileAttr;
use super::super::serial;
use super::etcd;
use super::request::{self, Index};
use super::response;
use super::tcp;
use crate::common::etcd_delegate::EtcdDelegate;
use log::debug;
use nix::sys::stat::SFlag;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use tokio::net::TcpStream;

/// Send message to all other nodes
async fn send_to_others<F, T>(
    etcd_client: Arc<EtcdDelegate>,
    node_id: &str,
    volume_info: &str,
    data: &[u8],
    filter: F,
    default: anyhow::Result<T>,
) -> anyhow::Result<T>
where
    F: Fn(&[u8]) -> (bool, anyhow::Result<T>) + Send,
    T: Send,
{
    let mut result: anyhow::Result<T> = default;
    if let Ok(nodes) = etcd::get_volume_nodes(
        Arc::<EtcdDelegate>::clone(&etcd_client),
        node_id,
        volume_info,
    )
    .await
    {
        for other_id in nodes {
            if let Ok(ref ip_and_port) =
                etcd::get_node_ip_and_port(Arc::<EtcdDelegate>::clone(&etcd_client), &other_id)
                    .await
            {
                let mut one_result = Vec::new();
                let mut stream = TcpStream::connect(ip_and_port)
                    .await
                    .unwrap_or_else(|e| panic!("fail connect to {ip_and_port}, error: {e}"));
                tcp::write_message(&mut stream, data).await?;
                tcp::read_message(&mut stream, &mut one_result).await?;

                let parsed = filter(one_result.as_slice());

                if parsed.0 {
                    return parsed.1;
                }
                result = parsed.1;
            }
        }
    }
    result
}

/// Load dir data from remote
pub async fn load_dir(
    etcd_client: Arc<EtcdDelegate>,
    node_id: &str,
    volume_info: &str,
    path: &str,
) -> anyhow::Result<Option<BTreeMap<String, DirEntry>>> {
    debug!("load_dir {}", path);
    let load_dir_filter =
        |data: &[u8]| -> (bool, anyhow::Result<Option<BTreeMap<String, DirEntry>>>) {
            let de = response::deserialize_load_dir(data);
            if de.is_some() {
                (true, Ok(de))
            } else {
                (false, Ok(de))
            }
        };
    let default: Option<BTreeMap<String, DirEntry>> = None;
    send_to_others(
        etcd_client,
        node_id,
        volume_info,
        &request::load_dir(path),
        load_dir_filter,
        Ok(default),
    )
    .await
}

/// Update dir data to remote
pub async fn update_dir(
    etcd_client: Arc<EtcdDelegate>,
    node_id: &str,
    volume_info: &str,
    parent: &str,
    child: &str,
    child_attr: &FileAttr,
    target_path: Option<&Path>,
) -> anyhow::Result<()> {
    debug!("update_dir parent {} child {}", parent, child);
    let do_nothing = |_: &[u8]| -> (bool, anyhow::Result<()>) { (false, Ok(())) };

    send_to_others(
        etcd_client,
        node_id,
        volume_info,
        &request::update_dir(parent, child, child_attr, target_path),
        do_nothing,
        Ok(()),
    )
    .await
}

#[allow(dead_code)]
/// Remove dir entry from remote
pub async fn remove_dir_entry(
    etcd_client: Arc<EtcdDelegate>,
    node_id: &str,
    volume_info: &str,
    parent: &str,
    child: &str,
) -> anyhow::Result<()> {
    debug!("remove_dir_entry parent {} child {}", parent, child);
    let do_nothing = |_: &[u8]| -> (bool, anyhow::Result<()>) { (false, Ok(())) };

    send_to_others(
        etcd_client,
        node_id,
        volume_info,
        &request::remove_dir_entry(parent, child),
        do_nothing,
        Ok(()),
    )
    .await
}

/// Get attr from remote
pub async fn get_attr(
    etcd_client: Arc<EtcdDelegate>,
    node_id: &str,
    volume_info: &str,
    path: &str,
) -> anyhow::Result<Option<FileAttr>> {
    debug!("get_attr {}", path);
    let get_attr_filter = |data: &[u8]| -> (bool, anyhow::Result<Option<FileAttr>>) {
        let de = response::deserialize_get_attr(data);
        if de.is_some() {
            (true, Ok(de))
        } else {
            (false, Ok(de))
        }
    };
    let default: Option<FileAttr> = None;
    send_to_others(
        etcd_client,
        node_id,
        volume_info,
        &request::get_file_attr(path),
        get_attr_filter,
        Ok(default),
    )
    .await
}

/// Push attr to remote
pub async fn push_attr(
    etcd_client: Arc<EtcdDelegate>,
    node_id: &str,
    volume_info: &str,
    path: &str,
    attr: &FileAttr,
) -> anyhow::Result<()> {
    debug!("push_attr {}", path);
    let do_nothing = |_: &[u8]| -> (bool, anyhow::Result<()>) { (false, Ok(())) };

    send_to_others(
        etcd_client,
        node_id,
        volume_info,
        &request::push_file_attr(path, serial::file_attr_to_serial(attr)),
        do_nothing,
        Ok(()),
    )
    .await
}

/// Invalidate file cache to remote
pub async fn invalidate(
    etcd_client: Arc<EtcdDelegate>,
    node_id: &str,
    volume_info: &str,
    path: &str,
    start: usize,
    end: usize,
) -> anyhow::Result<()> {
    debug!("invalidate {} start {} end {}", path, start, end);
    let do_nothing = |_: &[u8]| -> (bool, anyhow::Result<()>) { (false, Ok(())) };

    let invalid_req = request::invalidate(path.as_bytes().to_vec(), vec![Index::Range(start, end)]);

    send_to_others(
        etcd_client,
        node_id,
        volume_info,
        &invalid_req,
        do_nothing,
        Ok(()),
    )
    .await
}

/// Read data from remote
pub async fn read_data(
    etcd_client: Arc<EtcdDelegate>,
    node_id: &str,
    volume_info: &str,
    path: &str,
    start: usize,
    end: usize,
) -> anyhow::Result<Option<Vec<u8>>> {
    debug!("read_data {} start {} end {}", path, start, end);
    let check = request::check_available(path.as_bytes().to_vec(), vec![Index::Range(start, end)]);
    let read_data = request::read(path.as_bytes().to_vec(), vec![Index::Range(start, end)]);
    if let Ok(nodes) = etcd::get_volume_nodes(
        Arc::<EtcdDelegate>::clone(&etcd_client),
        node_id,
        volume_info,
    )
    .await
    {
        for other_id in nodes {
            if let Ok(ref ip_and_port) =
                etcd::get_node_ip_and_port(Arc::<EtcdDelegate>::clone(&etcd_client), &other_id)
                    .await
            {
                {
                    let mut result = Vec::new();
                    let mut stream = TcpStream::connect(ip_and_port)
                        .await
                        .unwrap_or_else(|e| panic!("fail connect to {ip_and_port}, error: {e}"));

                    tcp::write_message(&mut stream, &check).await?;
                    tcp::read_message(&mut stream, &mut result).await?;
                    if response::deserialize_check_available(&result).is_none() {
                        continue;
                    }
                }
                {
                    let mut result = Vec::new();
                    // FIXME: Should reuse the connection
                    let mut stream = TcpStream::connect(ip_and_port)
                        .await
                        .unwrap_or_else(|e| panic!("fail connect to {ip_and_port}, error: {e}"));

                    tcp::write_message(&mut stream, &read_data).await?;
                    tcp::read_message(&mut stream, &mut result).await?;
                    return Ok(Some(result));
                }
            }
        }
    }

    Ok(None)
}

/// Rename file request to remote
pub async fn rename(
    etcd_client: Arc<EtcdDelegate>,
    node_id: &str,
    volume_info: &str,
    rename_param: RenameParam,
) -> anyhow::Result<()> {
    debug!("rename {:?}", rename_param);
    let do_nothing = |_: &[u8]| -> (bool, anyhow::Result<()>) { (false, Ok(())) };

    send_to_others(
        etcd_client,
        node_id,
        volume_info,
        &request::rename(rename_param),
        do_nothing,
        Ok(()),
    )
    .await
}

/// Remove file request to remote
pub async fn remove(
    etcd_client: Arc<EtcdDelegate>,
    node_id: &str,
    volume_info: &str,
    parent: INum,
    child_name: &str,
    child_type: SFlag,
) -> anyhow::Result<()> {
    debug!(
        "remove parent {:?} child_name {:?} child_type {:?}",
        parent, child_name, child_type
    );
    let do_nothing = |_: &[u8]| -> (bool, anyhow::Result<()>) { (false, Ok(())) };

    send_to_others(
        etcd_client,
        node_id,
        volume_info,
        &request::remove(parent, child_name, child_type),
        do_nothing,
        Ok(()),
    )
    .await
}
