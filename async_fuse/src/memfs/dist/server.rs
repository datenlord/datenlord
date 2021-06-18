//! This is the server for the cache, which is used to accpet the request

use super::super::cache::GlobalCache;
use super::super::node::Node;
use super::super::s3_metadata::S3MetaData;
use super::request::{self, DistRequest, OpArgs, RemoveDirEntryArgs, UpdateDirArgs};
use super::response;
use super::tcp;
use super::types::{self, SerialFileAttr};
use std::fmt::{self, Debug};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use log::debug;

pub struct CacheServer {
    ip: String,
    port: String,
    th: Option<JoinHandle<bool>>,
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
        let mut connect =
            TcpStream::connect(format!("{}:{}", self.ip, self.port)).unwrap_or_else(|e| {
                panic!(
                    "Connect to local service {}:{} failed, error: {}",
                    self.ip, self.port, e
                )
            });
        if let Err(e) = tcp::write_message(&mut connect, request::turnoff().as_slice()) {
            panic!("Fail to send turn off request, {}", e);
        }
        self.th
            .take()
            .unwrap_or_else(|| panic!("Th in Cache server is None"))
            .join()
            .unwrap_or_else(|e| {
                panic!(
                    "join failed while waiting for cache server termination, {:?}",
                    e
                )
            });
    }
}

impl CacheServer {
    pub(crate) fn new(
        ip: String,
        port: String,
        cache: Arc<GlobalCache>,
        meta: Arc<S3MetaData>,
    ) -> Self {
        let ip_copy = ip.clone();
        let port_copy = port.clone();

        let th = thread::spawn(move || {
            let listener =
                TcpListener::bind(format!("{}:{}", ip_copy, port_copy)).unwrap_or_else(|e| {
                    panic!(
                        "Fail to bind tcp listener to {}:{}, error is {}",
                        ip_copy, port_copy, e
                    )
                });
            for stream in listener.incoming() {
                let stream = stream.unwrap_or_else(|e| {
                    panic!("Fail to create incoming tcp stream, error is {}", e)
                });
                let cache_clone = cache.clone();
                let meta_clone = meta.clone();

                smol::spawn(async move {
                    let mut local_stream = stream;
                    match dispatch(&mut local_stream, cache_clone, meta_clone).await {
                        Ok(is_continue) => {
                            if !is_continue {
                                return;
                            }
                        }
                        Err(e) => panic!("process cache request error: {}", e),
                    }
                })
                .detach();
            }

            return true;
        });

        Self {
            ip,
            port,
            th: Some(th),
        }
    }
}

async fn dispatch(
    stream: &mut TcpStream,
    cache: Arc<GlobalCache>,
    meta: Arc<S3MetaData>,
) -> anyhow::Result<bool> {
    let mut buf = Vec::new();
    if let Err(e) = tcp::read_message(stream, &mut buf) {
        panic!(
            "fail to read distributed cache request from tcp stream, {}",
            e
        );
    }

    let request = request::deserialize_cache(buf.as_slice());

    match request {
        DistRequest::TurnOff => {
            turnoff(stream)?;
            return Ok(false);
        }
        DistRequest::Invalidate(args) => {
            invalidate(stream, cache, args)?;
            return Ok(true);
        }

        DistRequest::CheckAvailable(args) => {
            check_available(stream, cache, args)?;
            return Ok(true);
        }

        DistRequest::Read(args) => {
            read(stream, cache, args)?;
            return Ok(true);
        }
        DistRequest::LoadDir(path) => {
            load_dir(stream, meta, &path).await?;
            return Ok(true);
        }
        DistRequest::UpdateDir(args) => {
            update_dir(stream, meta, args).await?;
            return Ok(true);
        }
        DistRequest::RemoveDirEntry(args) => {
            remove_dir_entry(stream, meta, args).await?;
            return Ok(true);
        }
        DistRequest::GetFileAttr(path) => {
            get_attr(stream, meta, &path).await?;
            return Ok(true);
        }
        DistRequest::PushFileAttr((path, attr)) => {
            push_attr(stream, meta, &path, &attr).await?;
            return Ok(true);
        }
    }
}

fn turnoff(stream: &mut TcpStream) -> anyhow::Result<()> {
    tcp::write_message(stream, response::turnoff().as_slice())?;
    Ok(())
}

fn invalidate(stream: &mut TcpStream, cache: Arc<GlobalCache>, args: OpArgs) -> anyhow::Result<()> {
    cache.invlidate(args.file_name.as_slice(), args.index);
    tcp::write_message(stream, response::invalidate().as_slice())?;
    Ok(())
}

fn check_available(
    stream: &mut TcpStream,
    cache: Arc<GlobalCache>,
    args: OpArgs,
) -> anyhow::Result<()> {
    let available = cache.check_available(args.file_name.as_slice(), args.index);
    if available.1 {
        tcp::write_message(
            stream,
            response::check_available(Some(available.0)).as_slice(),
        )?;
    } else {
        tcp::write_message(stream, response::check_available(None).as_slice())?;
    }
    Ok(())
}

fn read(stream: &mut TcpStream, cache: Arc<GlobalCache>, args: OpArgs) -> anyhow::Result<()> {
    let data = cache.read(args.file_name.as_slice(), args.index);
    tcp::write_message_vector(stream, data)?;
    Ok(())
}

async fn load_dir(stream: &mut TcpStream, meta: Arc<S3MetaData>, path: &str) -> anyhow::Result<()> {
    let path2inum = meta.path2inum.read().await;

    match path2inum.get(path) {
        None => tcp::write_message(stream, response::load_dir_none().as_slice())?,
        Some(inum) => match meta.cache.read().await.get(inum) {
            None => tcp::write_message(stream, response::load_dir_none().as_slice())?,
            Some(ref node) => {
                tcp::write_message(stream, response::load_dir(node.get_dir_data()).as_slice())?
            }
        },
    };

    Ok(())
}

async fn update_dir(
    stream: &mut TcpStream,
    meta: Arc<S3MetaData>,
    args: UpdateDirArgs,
) -> anyhow::Result<()> {
    let path2inum = meta.path2inum.read().await;
    if let Some(parent_inum) = path2inum.get(&args.parent_path) {
        if let Some(parent_node) = meta.cache.write().await.get_mut(parent_inum) {
            parent_node
                .get_dir_data_mut()
                .insert(args.child_name, types::serial_to_dir_entry(&args.entry));
        }
    }
    tcp::write_message(stream, &response::update_dir())?;
    Ok(())
}

async fn remove_dir_entry(
    stream: &mut TcpStream,
    meta: Arc<S3MetaData>,
    args: RemoveDirEntryArgs,
) -> anyhow::Result<()> {
    let path2inum = meta.path2inum.read().await;
    if let Some(parent_inum) = path2inum.get(&args.parent_path) {
        if let Some(parent_node) = meta.cache.write().await.get_mut(parent_inum) {
            parent_node.get_dir_data_mut().remove(&args.child_name);
        }
    }
    tcp::write_message(stream, &response::update_dir())?;
    Ok(())
}

async fn get_attr(stream: &mut TcpStream, meta: Arc<S3MetaData>, path: &str) -> anyhow::Result<()> {
    let path2inum = meta.path2inum.read().await;
    if let Some(inum) = path2inum.get(path) {
        let mut cache = meta.cache.write().await;
        if let Some(node) = cache.get_mut(inum) {
            let attr = node.get_attr();
            debug!("Success get attr for path {} .", path);
            tcp::write_message(stream, &response::get_attr(&attr))?;
            return Ok(());
        } else {
            debug!("inum {} is not find in meta.cache, inode collection {:?}.", inum, cache.keys());
        }
    } else {
        debug!("path {} is not find in path2inum, path2inum keys {:?}.", path, path2inum.keys());
    }

    tcp::write_message(stream, &response::get_attr_none())?;
    Ok(())
}

async fn push_attr(
    stream: &mut TcpStream,
    meta: Arc<S3MetaData>,
    path: &str,
    attr: &SerialFileAttr,
) -> anyhow::Result<()> {
    let path2inum = meta.path2inum.read().await;
    if let Some(inum) = path2inum.get(path) {
        if let Some(node) = meta.cache.write().await.get_mut(inum) {
            // Keep iNum
            let old_attr = node.get_attr();
            let mut new_attr = types::serial_to_file_attr(attr);
            new_attr.ino = old_attr.ino;

            node._set_attr(new_attr, false).await;
        }
    }

    tcp::write_message(stream, &response::push_attr())?;
    Ok(())
}
