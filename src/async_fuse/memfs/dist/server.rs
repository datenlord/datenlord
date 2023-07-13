//! This is the server for the cache, which is used to accpet the request

use super::super::cache::GlobalCache;
use super::super::dir::DirEntry;
use super::super::node::Node;
use super::super::s3_metadata::S3MetaData;
use super::super::s3_node::S3Node;
use super::super::serial::{self, SerialFileAttr};
use super::request::{self, DistRequest, OpArgs, RemoveArgs, RemoveDirEntryArgs, UpdateDirArgs};
use super::response;
use super::tcp;
use crate::async_fuse::memfs::s3_wrapper::S3BackEnd;
use crate::async_fuse::memfs::RenameParam;
use log::debug;
use parking_lot::RwLock;
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
    pub(crate) fn new<S: S3BackEnd + Send + Sync + 'static>(
        ip: String,
        port: String,
        cache: Arc<GlobalCache>,
        meta: Arc<S3MetaData<S>>,
    ) -> Self {
        let ip_copy = ip.clone();
        let port_copy = port.clone();

        let listener_join_handler = tokio::spawn(listen(ip_copy, port_copy, cache, meta));
        Self {
            ip,
            port,
            listener_join_handler,
        }
    }
}

/// async listen routine
async fn listen<S: S3BackEnd + Send + Sync + 'static>(
    ip: String,
    port: String,
    cache: Arc<GlobalCache>,
    meta: Arc<S3MetaData<S>>,
) {
    let listener = tokio::net::TcpListener::bind(format!("{ip}:{port}"))
        .await
        .unwrap_or_else(|e| {
            panic!("Fail to bind tcp listener to {ip}:{port}, error is {e}");
        });
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let cache_clone = Arc::<GlobalCache>::clone(&cache);
                let meta_clone = Arc::<S3MetaData<S>>::clone(&meta);

                tokio::spawn(async move {
                    let mut local_stream = stream;
                    match dispatch(&mut local_stream, cache_clone, meta_clone).await {
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
async fn dispatch<S: S3BackEnd + Send + Sync + 'static>(
    stream: &mut TcpStream,
    cache: Arc<GlobalCache>,
    meta: Arc<S3MetaData<S>>,
) -> anyhow::Result<bool> {
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
        DistRequest::LoadDir(path) => {
            load_dir(stream, meta, &path).await?;
            Ok(true)
        }
        DistRequest::UpdateDir(args) => {
            update_dir(stream, meta, args).await?;
            Ok(true)
        }
        DistRequest::RemoveDirEntry(args) => {
            remove_dir_entry(stream, meta, args).await?;
            Ok(true)
        }
        DistRequest::GetFileAttr(path) => {
            get_attr(stream, meta, &path).await?;
            Ok(true)
        }
        DistRequest::PushFileAttr((path, attr)) => {
            push_attr(stream, meta, &path, &attr).await?;
            Ok(true)
        }
        DistRequest::Rename(args) => {
            rename(stream, meta, args).await?;
            Ok(true)
        }
        DistRequest::Remove(args) => {
            remove(stream, meta, args).await?;
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
    cache.invalidate(args.file_name.as_slice(), args.index);
    tcp::write_message(stream, response::invalidate().as_slice()).await?;
    Ok(())
}

/// Handle `CheckAvailable` request
async fn check_available(
    stream: &mut TcpStream,
    cache: &Arc<GlobalCache>,
    args: OpArgs,
) -> anyhow::Result<()> {
    let available = cache.check_available(args.file_name.as_slice(), args.index);
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
    let data = cache.read(args.file_name.as_slice(), args.index);
    tcp::write_message_vector(stream, data).await?;
    Ok(())
}

/// Handle `LoadDir` request
async fn load_dir<S: S3BackEnd + Send + Sync + 'static>(
    stream: &mut TcpStream,
    meta: Arc<S3MetaData<S>>,
    path: &str,
) -> anyhow::Result<()> {
    let inum_opt = {
        let path2inum = meta.path2inum.read().await;
        path2inum.get(path).copied()
    };

    match inum_opt {
        None => tcp::write_message(stream, response::load_dir_none().as_slice()).await?,
        Some(inum) => match meta.cache.read().await.get(&inum) {
            None => tcp::write_message(stream, response::load_dir_none().as_slice()).await?,
            Some(node) => {
                tcp::write_message(stream, response::load_dir(node.get_dir_data()).as_slice())
                    .await?
            }
        },
    };

    Ok(())
}

/// Handle `UpdateDir` request
async fn update_dir<S: S3BackEnd + Send + Sync + 'static>(
    stream: &mut TcpStream,
    meta: Arc<S3MetaData<S>>,
    args: UpdateDirArgs,
) -> anyhow::Result<()> {
    debug!("receive update_dir request {:?}", args);
    let mut cache = meta.cache.write().await;
    let mut path2inum = meta.path2inum.write().await;
    if let Some(parent_inum) = path2inum.get(&args.parent_path) {
        if let Some(parent_node) = cache.get_mut(parent_inum) {
            let child_attr_serial = args.child_attr;
            let child_attr = Arc::new(RwLock::new(serial::serial_to_file_attr(&child_attr_serial)));
            let child_node = S3Node::new_child_node_of_parent(
                parent_node,
                &args.child_name,
                Arc::clone(&child_attr),
                args.target_path,
                &meta.data_cache,
            );

            let child_ino = child_node.get_ino();
            // let child_type = child_node.get_type();
            let entry = DirEntry::new(args.child_name.clone(), child_attr);
            // Add to parent node
            parent_node
                .get_dir_data_mut()
                .insert(args.child_name.clone(), entry);
            // Add child to cache
            path2inum.insert(child_node.full_path().to_owned(), child_ino);
            cache.insert(child_ino, child_node);
        }
    }
    tcp::write_message(stream, &response::update_dir()).await?;
    Ok(())
}

/// Handle `RemoveDirEntry` request
async fn remove_dir_entry<S: S3BackEnd + Send + Sync + 'static>(
    stream: &mut TcpStream,
    meta: Arc<S3MetaData<S>>,
    args: RemoveDirEntryArgs,
) -> anyhow::Result<()> {
    let parent_inum_opt = {
        let path2inum = meta.path2inum.read().await;
        path2inum.get(&args.parent_path).copied()
    };
    if let Some(parent_inum) = parent_inum_opt {
        if let Some(parent_node) = meta.cache.write().await.get_mut(&parent_inum) {
            parent_node.get_dir_data_mut().remove(&args.child_name);
        }
    }
    tcp::write_message(stream, &response::update_dir()).await?;
    Ok(())
}

/// Handle `GetAttr` request
async fn get_attr<S: S3BackEnd + Send + Sync + 'static>(
    stream: &mut TcpStream,
    meta: Arc<S3MetaData<S>>,
    path: &str,
) -> anyhow::Result<()> {
    let inum_opt = {
        let path2inum = meta.path2inum.read().await;
        path2inum.get(path).copied()
    };
    if let Some(inum) = inum_opt {
        let cache = meta.cache.read().await;
        if let Some(node) = cache.get(&inum) {
            let attr = node.get_attr();
            debug!("Success get attr for path {} .", path);
            tcp::write_message(stream, &response::get_attr(&attr)).await?;
            return Ok(());
        }
        debug!(
            "inum {} is not find in meta.cache, inode collection {:?}.",
            inum,
            cache.keys()
        );
    } else {
        debug!("path {} is not find in path2inum.", path,);
    }

    tcp::write_message(stream, &response::get_attr_none()).await?;
    Ok(())
}

/// Handle `PushAttr` request
async fn push_attr<S: S3BackEnd + Send + Sync + 'static>(
    stream: &mut TcpStream,
    meta: Arc<S3MetaData<S>>,
    path: &str,
    attr: &SerialFileAttr,
) -> anyhow::Result<()> {
    let inum_opt = {
        let path2inum = meta.path2inum.read().await;
        path2inum.get(path).copied()
    };
    if let Some(inum) = inum_opt {
        if let Some(node) = meta.cache.write().await.get_mut(&inum) {
            // Keep iNum
            let old_attr = node.get_attr();
            let mut new_attr = serial::serial_to_file_attr(attr);
            new_attr.ino = old_attr.ino;

            node._set_attr(new_attr, false);
        }
    }

    tcp::write_message(stream, &response::push_attr()).await?;
    Ok(())
}

/// Handle `Rename` request
async fn rename<S: S3BackEnd + Send + Sync + 'static>(
    stream: &mut TcpStream,
    meta: Arc<S3MetaData<S>>,
    args: RenameParam,
) -> anyhow::Result<()> {
    meta.rename_local(&args, true).await;
    tcp::write_message(stream, &response::rename()).await?;
    Ok(())
}

/// Handle `Remove` request
async fn remove<S: S3BackEnd + Send + Sync + 'static>(
    stream: &mut TcpStream,
    meta: Arc<S3MetaData<S>>,
    args: RemoveArgs,
) -> anyhow::Result<()> {
    debug!("receive remove request {:?}", args);
    if let Err(e) = meta
        .remove_node_local(
            args.parent,
            &args.child_name,
            serial::serial_to_entry_type(&args.child_type),
            true,
        )
        .await
    {
        panic!(
            "failed to remove child {:?} from parent {:?} locally, error is {:?}",
            args.parent, args.child_name, e,
        );
    }
    tcp::write_message(stream, &response::remove()).await?;
    Ok(())
}
