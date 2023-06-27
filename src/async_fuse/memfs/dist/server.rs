//! This is the server for the cache, which is used to accpet the request

use super::super::cache::GlobalCache;
use super::request::{self, DistRequest, OpArgs};
use super::response;
use super::tcp;
use std::fmt::{self, Debug};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

/// Distributed cache server
pub struct CacheServer {
    /// Ip address
    ip: String,
    /// Port number
    port: String,
    /// Server thread handler
    listener_join_handler: JoinHandle<()>,
}

impl Debug for CacheServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CacheServer")
            .field("ip", &self.ip)
            .field("port", &self.port)
            .finish()
    }
}

impl Drop for CacheServer {
    fn drop(&mut self) {
        self.listener_join_handler.abort();
    }
}

impl CacheServer {
    /// New a `CacheServer `
    pub(crate) fn new(ip: String, port: String, cache: Arc<GlobalCache>) -> Self {
        let ip_copy = ip.clone();
        let port_copy = port.clone();

        let listener_join_handler = tokio::spawn(listen(ip_copy, port_copy, cache));
        Self {
            ip,
            port,
            listener_join_handler,
        }
    }
}

/// async listen routine
async fn listen(ip: String, port: String, cache: Arc<GlobalCache>) {
    let listener = tokio::net::TcpListener::bind(format!("{ip}:{port}"))
        .await
        .unwrap_or_else(|e| {
            panic!("Fail to bind tcp listener to {ip}:{port}, error is {e}");
        });
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let cache_clone = Arc::<GlobalCache>::clone(&cache);

                tokio::spawn(async move {
                    let mut local_stream = stream;
                    match dispatch(&mut local_stream, cache_clone).await {
                        Ok(_) => {}
                        Err(e) => panic!("process cache request error: {e}"),
                    }
                });
            }
            Err(e) => panic!("Fail to create incoming tcp stream, error is {e}"),
        }
    }
}

/// Dispatch request
async fn dispatch(stream: &mut TcpStream, cache: Arc<GlobalCache>) -> anyhow::Result<bool> {
    let mut buf = Vec::new();
    if let Err(e) = tcp::read_message(stream, &mut buf).await {
        panic!("fail to read distributed cache request from tcp stream, {e}");
    }

    let request = request::deserialize_cache(buf.as_slice());

    match request {
        DistRequest::Invalidate(args) => {
            invalidate(stream, &cache, args).await?;
            Ok(true)
        }

        DistRequest::CheckAvailable(args) => {
            check_available(stream, &cache, args).await?;
            Ok(true)
        }

        DistRequest::Read(args) => {
            read(stream, &cache, args).await?;
            Ok(true)
        }
    }
}

/// Handle `Invalidate` request
async fn invalidate(
    stream: &mut TcpStream,
    cache: &Arc<GlobalCache>,
    args: OpArgs,
) -> anyhow::Result<()> {
    cache.invalidate(args.file_ino, args.index);
    tcp::write_message(stream, response::invalidate().as_slice()).await?;
    Ok(())
}

/// Handle `CheckAvailable` request
async fn check_available(
    stream: &mut TcpStream,
    cache: &Arc<GlobalCache>,
    args: OpArgs,
) -> anyhow::Result<()> {
    let available = cache.check_available(args.file_ino, args.index);
    if available.1 {
        tcp::write_message(
            stream,
            response::check_available(&Some(available.0)).as_slice(),
        )
        .await?;
    } else {
        tcp::write_message(stream, response::check_available(&None).as_slice()).await?;
    }
    Ok(())
}

/// Handle `Read` request
async fn read(
    stream: &mut TcpStream,
    cache: &Arc<GlobalCache>,
    args: OpArgs,
) -> anyhow::Result<()> {
    let data = cache.read(args.file_ino, args.index);
    tcp::write_message_vector(stream, data).await?;
    Ok(())
}
