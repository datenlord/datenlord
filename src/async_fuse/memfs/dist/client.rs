use std::sync::Arc;

use log::debug;
use tokio::net::TcpStream;

use super::request::{self, Index};
use super::{response, tcp};
use crate::async_fuse::fuse::protocol::INum;
use crate::async_fuse::memfs::kv_engine::kv_utils::{get_node_ip_and_port, get_volume_nodes};
use crate::async_fuse::memfs::kv_engine::KVEngineType;

/// Send message to all other nodes
async fn send_to_others<F, T>(
    kv_engine: &Arc<KVEngineType>,
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
    if let Ok(nodes) = get_volume_nodes(kv_engine, node_id, volume_info).await {
        for other_id in nodes {
            if let Ok(ref ip_and_port) = get_node_ip_and_port(kv_engine, &other_id).await {
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

/// Invalidate file cache to remote
pub async fn invalidate(
    kv_engine: &Arc<KVEngineType>,
    node_id: &str,
    volume_info: &str,
    ino: INum,
    start: usize,
    end: usize,
) -> anyhow::Result<()> {
    debug!("invalidate {} start {} end {}", ino, start, end);
    let do_nothing = |_: &[u8]| -> (bool, anyhow::Result<()>) { (false, Ok(())) };

    let invalid_req = request::invalidate(ino, vec![Index::Range(start, end)]);

    send_to_others(
        kv_engine,
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
    kv_engine: &Arc<KVEngineType>,
    node_id: &str,
    volume_info: &str,
    ino: INum,
    start: usize,
    end: usize,
) -> anyhow::Result<Option<Vec<u8>>> {
    debug!("read_data {} start {} end {}", ino, start, end);
    let check = request::check_available(ino, vec![Index::Range(start, end)]);
    let read_data = request::read(ino, vec![Index::Range(start, end)]);
    if let Ok(nodes) = get_volume_nodes(kv_engine, node_id, volume_info).await {
        for other_id in nodes {
            if let Ok(ref ip_and_port) = get_node_ip_and_port(kv_engine, &other_id).await {
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
