use super::super::dir::DirEntry;
use super::super::fs_util::FileAttr;
use super::etcd;
use super::request::{self, Index};
use super::response;
use super::tcp;
use super::types;
use common::etcd_delegate::EtcdDelegate;
use log::debug;
use std::collections::BTreeMap;
use std::net::TcpStream;
use std::sync::Arc;

pub(crate) async fn send_to_others<F, T>(
    etcd_client: Arc<EtcdDelegate>,
    node_id: u64,
    volume_info: &str,
    data: &[u8],
    filter: F,
    default: anyhow::Result<T>,
) -> anyhow::Result<T>
where
    F: Fn(&[u8]) -> (bool, anyhow::Result<T>),
{
    let mut result: anyhow::Result<T> = default;
    if let Ok(nodes) = etcd::get_volume_nodes(etcd_client.clone(), node_id, volume_info).await {
        for other_id in nodes {
            if let Ok(ref ip_and_port) =
                etcd::get_node_ip_and_port(etcd_client.clone(), other_id).await
            {
                let mut one_result = Vec::new();
                let mut stream = TcpStream::connect(ip_and_port)
                    .unwrap_or_else(|e| panic!("fail connect to {}, error: {}", ip_and_port, e));
                tcp::write_message(&mut stream, data)?;
                tcp::read_message(&mut stream, &mut one_result)?;

                let parsed = filter(one_result.as_slice());

                if parsed.0 {
                    return parsed.1;
                } else {
                    result = parsed.1;
                }
            }
        }
    }
    return result;
}

pub(crate) async fn load_dir(
    etcd_client: Arc<EtcdDelegate>,
    node_id: u64,
    volume_info: &str,
    path: &str,
) -> anyhow::Result<Option<BTreeMap<String, DirEntry>>> {
    debug!("load_dir {}", path);
    let load_dir_filter =
        |data: &[u8]| -> (bool, anyhow::Result<Option<BTreeMap<String, DirEntry>>>) {
            let de = response::deserialize_load_dir(data);
            if let Some(_) = de {
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

pub(crate) async fn update_dir(
    etcd_client: Arc<EtcdDelegate>,
    node_id: u64,
    volume_info: &str,
    parent: &str,
    child: &str,
    entry: &DirEntry,
) -> anyhow::Result<()> {
    debug!("update_dir parent {} child {}", parent, child);
    let do_nothing = |_: &[u8]| -> (bool, anyhow::Result<()>) { (false, Ok(())) };
    let default = ();

    send_to_others(
        etcd_client,
        node_id,
        volume_info,
        &request::update_dir(parent, child, entry),
        do_nothing,
        Ok(default),
    )
    .await
}

pub(crate) async fn remove_dir_entry(
    etcd_client: Arc<EtcdDelegate>,
    node_id: u64,
    volume_info: &str,
    parent: &str,
    child: &str,
) -> anyhow::Result<()> {
    debug!("remove_dir_entry parent {} child {}", parent, child);
    let do_nothing = |_: &[u8]| -> (bool, anyhow::Result<()>) { (false, Ok(())) };
    let default = ();

    send_to_others(
        etcd_client,
        node_id,
        volume_info,
        &request::remove_dir_entry(parent, child),
        do_nothing,
        Ok(default),
    )
    .await
}

pub(crate) async fn get_attr(
    etcd_client: Arc<EtcdDelegate>,
    node_id: u64,
    volume_info: &str,
    path: &str,
) -> anyhow::Result<Option<FileAttr>> {
    debug!("get_attr {}", path);
    let get_attr_filter = |data: &[u8]| -> (bool, anyhow::Result<Option<FileAttr>>) {
        let de = response::deserialize_get_attr(data);
        if let Some(_) = de {
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

pub(crate) async fn push_attr(
    etcd_client: Arc<EtcdDelegate>,
    node_id: u64,
    volume_info: &str,
    path: &str,
    attr: &FileAttr,
) -> anyhow::Result<()> {
    debug!("push_attr {}", path);
    let do_nothing = |_: &[u8]| -> (bool, anyhow::Result<()>) { (false, Ok(())) };
    let default = ();

    send_to_others(
        etcd_client,
        node_id,
        volume_info,
        &request::push_file_attr(path, types::file_attr_to_serial(attr)),
        do_nothing,
        Ok(default),
    )
    .await
}

/// end is included
pub(crate) async fn invalidate(
    etcd_client: Arc<EtcdDelegate>,
    node_id: u64,
    volume_info: &str,
    path: &str,
    start: usize,
    end: usize,
) -> anyhow::Result<()> {
    debug!("invalidate {} start {} end {}", path, start, end);
    let do_nothing = |_: &[u8]| -> (bool, anyhow::Result<()>) { (false, Ok(())) };
    let default = ();

    let invalid_req = request::invalidate(path.as_bytes().to_vec(), vec![Index::Range(start, end)]);

    send_to_others(
        etcd_client,
        node_id,
        volume_info,
        &invalid_req,
        do_nothing,
        Ok(default),
    )
    .await
}

/// end is included
pub(crate) async fn read_data(
    etcd_client: Arc<EtcdDelegate>,
    node_id: u64,
    volume_info: &str,
    path: &str,
    start: usize,
    end: usize,
) -> anyhow::Result<Option<Vec<u8>>> {
    debug!("read_data {} start {} end {}", path, start, end);
    let check = request::check_available(path.as_bytes().to_vec(), vec![Index::Range(start, end)]);
    let read_data = request::read(path.as_bytes().to_vec(), vec![Index::Range(start, end)]);
    if let Ok(nodes) = etcd::get_volume_nodes(etcd_client.clone(), node_id, volume_info).await {
        for other_id in nodes {
            if let Ok(ref ip_and_port) =
                etcd::get_node_ip_and_port(etcd_client.clone(), other_id).await
            {
                {
                    let mut result = Vec::new();
                    let mut stream = TcpStream::connect(ip_and_port).unwrap_or_else(|e| {
                        panic!("fail connect to {}, error: {}", ip_and_port, e)
                    });

                    tcp::write_message(&mut stream, &check)?;
                    tcp::read_message(&mut stream, &mut result)?;
                    if let None = response::deserialize_check_available(&result) {
                        continue;
                    }
                }
                {
                    let mut result = Vec::new();
                    // FIXME: Should reuse the connection
                    let mut stream = TcpStream::connect(ip_and_port).unwrap_or_else(|e| {
                        panic!("fail connect to {}, error: {}", ip_and_port, e)
                    });

                    tcp::write_message(&mut stream, &read_data)?;
                    tcp::read_message(&mut stream, &mut result)?;
                    return Ok(Some(result));
                }
            }
        }
    }

    Ok(None)
}

pub(crate) async fn get_ino_num(
    etcd_client: Arc<EtcdDelegate>,
    node_id: u64,
    volume_info: &str,
    default: u32,
) -> anyhow::Result<u32> {
    debug!("get_ino_num");
    let get_inum = request::get_ino_num();
    let mut cur = default;
    if let Ok(nodes) = etcd::get_volume_nodes(etcd_client.clone(), node_id, volume_info).await {
        for other_id in nodes {
            if let Ok(ref ip_and_port) =
                etcd::get_node_ip_and_port(etcd_client.clone(), other_id).await
            {
                let mut stream = TcpStream::connect(ip_and_port)
                    .unwrap_or_else(|e| panic!("fail connect to {}, error: {}", ip_and_port, e));

                tcp::write_message(&mut stream, &get_inum)?;
                let inum = tcp::read_u32(&mut stream)?;

                if inum > cur {
                    cur = inum;
                }
            }
        }
    }

    Ok(cur)
}
