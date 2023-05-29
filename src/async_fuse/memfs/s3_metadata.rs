use super::cache::GlobalCache;
use super::cache::IoMemBlock;
use super::dir::DirEntry;
use super::dist::client as dist_client;
use super::dist::etcd;
use super::dist::server::CacheServer;
use super::fs_util::{self, FileAttr};
use super::inode::InodeState;
use super::kv_engine::{KVEngine, KeyType, ValueType};
use super::metadata::MetaData;
use super::node::Node;
use super::persist::PersistDirContent;
use super::persist::PersistHandle;
use super::persist::PersistTask;
use super::s3_node::S3NodeWrap;
use super::s3_node::{self, S3Node};
use super::s3_wrapper::S3BackEnd;
use super::serial;
use super::RenameParam;
use super::SetAttrParam;
use crate::async_fuse::fuse::file_system::FsAsyncResultSender;
#[cfg(feature = "abi-7-18")]
use crate::async_fuse::fuse::fuse_reply::FuseDeleteNotification;
use crate::async_fuse::fuse::fuse_reply::ReplyDirectory;
use crate::async_fuse::fuse::fuse_reply::StatFsParam;
use crate::async_fuse::fuse::protocol::{FuseAttr, INum, FUSE_ROOT_ID};
use crate::async_fuse::util;
use crate::common::error::{DatenLordError, DatenLordResult};
use crate::common::etcd_delegate::EtcdDelegate;
use anyhow::Context;
use async_trait::async_trait;
use clippy_utilities::{Cast, OverflowArithmetic};
use itertools::Itertools;
use log::debug;
use log::warn;
use nix::errno::Errno;
use nix::fcntl::OFlag;
use nix::sys::stat::SFlag;
use parking_lot::RwLock as SyncRwLock;
use std::collections::BTreeMap;
use std::os::unix::io::RawFd;
use std::os::unix::prelude::OsStringExt;
use std::path::Path;
use std::sync::{atomic::AtomicU32, Arc};
use std::time::Duration;
use tokio::sync::{Mutex, RwLock, RwLockWriteGuard};
use tokio::task::JoinHandle; // conflict with tokio RwLock

/// The time-to-live seconds of FUSE attributes
const MY_TTL_SEC: u64 = 3600; // TODO: should be a long value, say 1 hour
/// The generation ID of FUSE attributes
const MY_GENERATION: u64 = 1; // TODO: find a proper way to set generation
/// S3 information string delimiter
const S3_INFO_DELIMITER: char = ';';

/// File system in-memory meta-data
#[derive(Debug)]
#[allow(dead_code)]
pub struct S3MetaData<S: S3BackEnd + Send + Sync + 'static, K: KVEngine + 'static> {
    /// S3 backend
    pub(crate) s3_backend: Arc<S>,
    /// Etcd client
    pub(crate) etcd_client: Arc<EtcdDelegate>,
    /// The cache to hold opened directories and files
    pub(crate) cache: RwLock<BTreeMap<INum, S3Node<S>>>,
    /// Global data cache
    pub(crate) data_cache: Arc<GlobalCache>,
    /// inode related, inum alloc and recycle
    pub(crate) inode_state: InodeState,
    /// Current available fd, it'll increase after using
    pub(crate) cur_fd: AtomicU32,
    /// Current service id
    pub(crate) node_id: Arc<str>,
    /// Volume Info
    pub(crate) volume_info: Arc<str>,
    /// Full path and node mapping
    pub(crate) path2inum: RwLock<BTreeMap<String, INum>>,
    /// Fuse fd
    fuse_fd: Mutex<RawFd>,
    /// Persist handle
    persist_handle: PersistHandle,
    /// KV engine
    pub(crate) kv_engine: Arc<K>,
}

/// Parse S3 info
fn parse_s3_info(info: &str) -> (&str, &str, &str, &str) {
    info.split(S3_INFO_DELIMITER)
        .next_tuple()
        .unwrap_or_else(|| panic!("parse s3 information failed. s3_info: {info}"))
}

#[async_trait]
impl<S: S3BackEnd + Sync + Send + 'static, K: KVEngine + 'static> MetaData for S3MetaData<S, K> {
    type N = S3Node<S>;

    async fn release(&self, ino: u64, fh: u64, _flags: u32, _lock_owner: u64, flush: bool) {
        {
            // TODO: handle lock_owner
            let mut cache = self.cache().write().await;
            let inode = cache.get_mut(&ino).unwrap_or_else(|| {
                panic!(
                    "relese() found fs is inconsistent, \
                     the inode ino={ino} is not in cache"
                );
            });
            inode.close(ino, fh, flush).await;
            debug!(
                "release() successfully closed the file handler={} of ino={} and name={:?}",
                fh,
                ino,
                inode.get_name(),
            );
        }
        self.try_delete_node(ino).await;
    }

    async fn readdir(
        &self,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) -> nix::Result<usize> {
        let mut readdir_helper = |data: &BTreeMap<String, DirEntry>| -> usize {
            let mut num_child_entries = 0;
            for (i, (child_name, child_entry)) in data.iter().enumerate().skip(offset.cast()) {
                let child_ino = child_entry.ino();
                reply.add(
                    child_ino,
                    offset.overflow_add(i.cast()).overflow_add(1), // i + 1 means the index of the next entry
                    child_entry.entry_type(),
                    child_name,
                );
                num_child_entries = num_child_entries.overflow_add(1);
                debug!(
                    "readdir() found one child of ino={}, name={:?}, offset={}, and entry={:?} \
                        under the directory of ino={}",
                    child_ino,
                    child_name,
                    offset.overflow_add(i.cast()).overflow_add(1),
                    child_entry,
                    ino,
                );
            }
            num_child_entries
        };

        let mut cache = self.cache().write().await;
        let inode = cache.get_mut(&ino).unwrap_or_else(|| {
            panic!(
                "relese() found fs is inconsistent, \
                 the inode ino={ino} is not in cache"
            );
        });
        if inode.need_load_dir_data() {
            let load_res = inode.load_data(0_usize, 0_usize).await;
            if let Err(e) = load_res {
                debug!(
                    "readdir() failed to load the data for directory of ino={} and name={:?}, \
                        the error is: {}",
                    ino,
                    inode.get_name(),
                    e
                );
                return reply.error(e).await;
            }
        }
        let num_child_entries = inode.read_dir(&mut readdir_helper);
        debug!(
            "readdir() successfully read {} entries \
                under the directory of ino={} and name={:?}",
            num_child_entries,
            ino,
            inode.get_name(),
        );
        reply.ok().await
    }

    async fn opendir(&self, ino: u64, flags: u32) -> DatenLordResult<RawFd> {
        let cache = self.cache().read().await;
        let node = cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "opendir() found fs is inconsistent, \
                    the i-node of ino={ino} should be in cache",
            );
        });
        let o_flags = fs_util::parse_oflag(flags);
        node.dup_fd(o_flags).await
    }

    async fn readlink(&self, ino: u64) -> Vec<u8> {
        let cache = self.cache().read().await;
        let node = cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "readlink() found fs is inconsistent, \
                    the i-node of ino={ino} should be in cache",
            );
        });
        node.get_symlink_target().as_os_str().to_owned().into_vec()
    }

    async fn statfs(&self, ino: u64) -> DatenLordResult<StatFsParam> {
        let cache = self.cache().read().await;
        let node = cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "statfs() found fs is inconsistent, \
                    the i-node of ino={ino} should be in cache",
            );
        });
        node.statefs().await
    }

    async fn flush(&self, ino: u64, fh: u64) {
        let mut cache = self.cache().write().await;
        let node = cache.get_mut(&ino).unwrap_or_else(|| {
            panic!(
                "flush() found fs is inconsistent, \
                    the i-node of ino={ino} should be in cache",
            );
        });
        node.flush(ino, fh).await;
    }

    async fn releasedir(&self, ino: u64, fh: u64) {
        {
            let cache = self.cache().read().await;
            let node = cache.get(&ino).unwrap_or_else(|| {
                panic!(
                    "releasedir() found fs is inconsistent, \
                    the i-node of ino={ino} should be in cache",
                );
            });
            node.closedir(ino, fh).await;
        }
        self.try_delete_node(ino).await;
    }

    async fn read_helper(
        &self,
        ino: INum,
        _fh: u64,
        offset: i64,
        size: u32,
    ) -> DatenLordResult<Vec<IoMemBlock>> {
        let mut cache = self.cache().write().await;
        let inode = cache.get_mut(&ino).unwrap_or_else(|| {
            panic!(
                "read() found fs is inconsistent, \
                 the inode ino={ino} is not in cache"
            );
        });

        let size: u64 =
            if offset.cast::<u64>().overflow_add(size.cast::<u64>()) > inode.get_attr().size {
                inode.get_attr().size.overflow_sub(offset.cast::<u64>())
            } else {
                size.cast()
            };

        // let node_type = node.get_type();
        // let file_data = if SFlag::S_IFREG == node_type {
        if inode.need_load_file_data(offset.cast(), size.cast()).await {
            let load_res = inode.load_data(offset.cast(), size.cast()).await;
            if let Err(e) = load_res {
                debug!(
                    "read() failed to load file data of ino={} and name={:?}, the error is: {}",
                    ino,
                    inode.get_name(),
                    e,
                );
                return Err(e);
            }
        }
        return Ok(inode.get_file_data(offset.cast(), size.cast()).await);
    }

    async fn open(&self, ino: u64, flags: u32) -> DatenLordResult<RawFd> {
        let cache = self.cache().read().await;
        let node = cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "open() found fs is inconsistent, \
                    the i-node of ino={ino} should be in cache",
            );
        });
        let o_flags = fs_util::parse_oflag(flags);
        // TODO: handle open flags
        // <https://pubs.opengroup.org/onlinepubs/9699919799/functions/open.html>
        // let open_res = if let SFlag::S_IFLNK = node.get_type() {
        //     node.open_symlink_target(o_flags).await.add_context(format!(
        //         "open() failed to open symlink target={:?} with flags={}",
        //         node.get_symlink_target(),
        //         flags,
        //     ))
        // } else {
        node.dup_fd(o_flags).await
    }

    async fn getattr(&self, ino: u64) -> DatenLordResult<(Duration, FuseAttr)> {
        let cache = self.cache().read().await;
        let inode = cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "getattr() found fs is inconsistent, \
                 the inode ino={ino} is not in cache"
            );
        });
        let attr = inode.get_attr();
        debug!(
            "getattr() cache hit when searching the attribute of ino={} and name={:?}",
            ino,
            inode.get_name(),
        );
        let ttl = Duration::new(MY_TTL_SEC, 0);
        let fuse_attr = fs_util::convert_to_fuse_attr(attr);
        Ok((ttl, fuse_attr))
    }

    async fn forget(&self, ino: u64, nlookup: u64) {
        let current_count: i64;
        {
            let cache = self.cache().read().await;
            let inode = cache.get(&ino).unwrap_or_else(|| {
                panic!(
                    "forget() found fs is inconsistent, \
                     the inode ino={ino} is not in cache"
                );
            });

            let previous_count = inode.dec_lookup_count_by(nlookup);
            current_count = inode.get_lookup_count();
            debug_assert!(current_count >= 0);
            debug_assert_eq!(
                previous_count.overflow_sub(current_count),
                nlookup.cast::<i64>()
            ); // assert no race forget
            debug!(
                "forget() successfully reduced lookup count of ino={} and name={:?} from {} to {}",
                ino,
                inode.get_name(),
                previous_count,
                current_count,
            );
        }
        self.try_delete_node(ino).await;
    }

    async fn setattr_helper(
        &self,
        ino: u64,
        param: SetAttrParam,
    ) -> DatenLordResult<(Duration, FuseAttr)> {
        let ttl = Duration::new(MY_TTL_SEC, 0);
        let mut cache = self.cache().write().await;
        let inode = cache.get_mut(&ino).unwrap_or_else(|| {
            panic!(
                "setattr() found fs is inconsistent, \
                    the i-node of ino={ino} should be in cache",
            );
        });

        match inode.setattr_precheck(param).await {
            Ok((attr_changed, file_attr)) => {
                if attr_changed {
                    inode.set_attr(file_attr);
                    debug!(
                        "setattr() successfully set the attribute of ino={} and name={:?}, the set attr={:?}",
                        ino, inode.get_name(), file_attr,
                    );
                } else {
                    warn!(
                        "setattr() did not change any attribute of ino={} and name={:?}",
                        ino,
                        inode.get_name(),
                    );
                }
                Ok((ttl, fs_util::convert_to_fuse_attr(file_attr)))
            }
            Err(e) => {
                debug!(
                    "setattr() failed to set the attribute of ino={} and name={:?}, the error is: {}",
                    ino,
                    inode.get_name(),
                    e,
                );
                Err(e)
            }
        }
    }

    async fn unlink(&self, parent: INum, name: &str) -> DatenLordResult<()> {
        let entry_type = {
            let cache = self.cache().read().await;
            let parent_node = cache.get(&parent).unwrap_or_else(|| {
                panic!(
                    "unlink() found fs is inconsistent, \
                        parent of ino={parent} should be in cache before remove its child",
                );
            });
            let child_entry = parent_node.get_entry(name).unwrap_or_else(|| {
                panic!(
                    "unlink() found fs is inconsistent, \
                        the child entry name={name:?} to remove is not under parent of ino={parent}",
                );
            });
            let entry_type = child_entry.entry_type();
            debug_assert_ne!(
                SFlag::S_IFDIR,
                entry_type,
                "unlink() should not remove sub-directory name={name:?} under parent ino={parent}",
            );
            entry_type
        };

        self.remove_node_helper(parent, name, entry_type).await
    }

    async fn new(
        s3_info: &str,
        capacity: usize,
        ip: &str,
        port: &str,
        etcd_client: EtcdDelegate,
        node_id: &str,
        volume_info: &str,
        fs_async_sender: FsAsyncResultSender,
    ) -> (Arc<Self>, Option<CacheServer>, Vec<JoinHandle<()>>) {
        let (bucket_name, endpoint, access_key, secret_key) = parse_s3_info(s3_info);
        let s3_backend = Arc::new(
            match S::new_backend(bucket_name, endpoint, access_key, secret_key).await {
                Ok(s) => s,
                Err(e) => panic!("{e:?}"),
            },
        );
        let mut async_tasks = vec![];
        let (persist_handle, persist_join_handle) =
            PersistTask::spawn(Arc::clone(&s3_backend), fs_async_sender);
        async_tasks.push(persist_join_handle);
        let etcd_arc = Arc::new(etcd_client);
        let data_cache = Arc::new(GlobalCache::new_dist_with_bz_and_capacity(
            10_485_760, // 10 * 1024 * 1024
            capacity,
            Arc::<EtcdDelegate>::clone(&etcd_arc),
            node_id,
        ));

        let kv_engine = Arc::new(K::new(etcd_arc.get_inner_client_clone()));

        let meta = Arc::new(Self {
            s3_backend: Arc::clone(&s3_backend),
            cache: RwLock::new(BTreeMap::new()),
            etcd_client: etcd_arc,
            data_cache: Arc::<GlobalCache>::clone(&data_cache),
            inode_state: InodeState::new(),
            cur_fd: AtomicU32::new(4),
            node_id: Arc::<str>::from(node_id.to_owned()),
            volume_info: Arc::<str>::from(volume_info.to_owned()),
            path2inum: RwLock::new(BTreeMap::new()),
            fuse_fd: Mutex::new(-1_i32),
            persist_handle,
            kv_engine,
        });

        let server = CacheServer::new(
            ip.to_owned(),
            port.to_owned(),
            data_cache,
            Arc::<Self>::clone(&meta),
        );

        let root_inode = S3Node::open_root_node(FUSE_ROOT_ID, "/", s3_backend, Arc::clone(&meta))
            .await
            .context("failed to open FUSE root node")
            .unwrap_or_else(|e| {
                panic!("{}", e);
            });

        let full_path = root_inode.full_path().to_owned();
        meta.cache.write().await.insert(FUSE_ROOT_ID, root_inode);
        meta.path2inum.write().await.insert(full_path, FUSE_ROOT_ID);

        (meta, Some(server), async_tasks)
    }

    /// Get metadata cache
    fn cache(&self) -> &RwLock<BTreeMap<INum, Self::N>> {
        &self.cache
    }

    /// Set fuse fd into `MetaData`
    async fn set_fuse_fd(&self, fuse_fd: RawFd) {
        *self.fuse_fd.lock().await = fuse_fd;
    }

    /// Try to delete node that is marked as deferred deletion
    async fn try_delete_node(&self, ino: INum) -> bool {
        let mut cache = self.cache.write().await;
        let node = cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "try_delete_node() found fs is inconsistent, \
                    the i-node of ino={ino} is not in cache",
            );
        });

        if node.get_open_count() == 0 && node.get_lookup_count() == 0 {
            debug!(
                "try_delete_node() deleted i-node of ino={} and name={:?}",
                ino,
                node.get_name(),
            );
            if let Some(node) = cache.remove(&ino) {
                if let SFlag::S_IFREG = node.get_type() {
                    self.data_cache
                        .remove_file_cache(node.get_full_path().as_bytes())
                        .await;
                }
            }
            true
        } else {
            debug!(
                "try_delete_node() cannot deleted i-node of ino={} and name={:?},\
                     open_count={}, lookup_count={}",
                ino,
                node.get_name(),
                node.get_open_count(),
                node.get_lookup_count()
            );
            false
        }
    }

    /// Helper function to create node
    #[allow(clippy::too_many_lines)]
    async fn create_node_helper(
        &self,
        parent: INum,
        node_name: &str,
        mode: u32,
        node_type: SFlag,
        target_path: Option<&Path>,
    ) -> DatenLordResult<(Duration, FuseAttr, u64)> {
        // pre-check
        let (parent_full_path, full_path, new_node_attr, fuse_attr) = {
            let mut cache = self.cache.write().await;
            let parent_node = Self::create_node_pre_check(parent, node_name, &mut cache)
                .context("create_node_helper() failed to pre check")?;
            let full_path = format!("{}{}", parent_node.full_path(), node_name);
            // check cluster conflict creating by trying to alloc ino
            let (inum, is_new) = self.inode_get_inum_by_fullpath(full_path.as_str()).await;
            if !is_new {
                // inum created by others or exist in remote cache
                //todo delete parent node cache to update parent dir content
                return Err(DatenLordError::from(anyhow::anyhow!("create_node_helper() failed to create node, ino alloc failed, \
                    the node of name={:?} already exists under parent directory of ino={} and name={:?}",
                    node_name, parent, parent_node.get_name())));
            }

            let parent_name = parent_node.get_name().to_owned();
            // all checks are passed, ready to create new node
            let m_flags = fs_util::parse_mode(mode);
            let new_node = match node_type {
                SFlag::S_IFDIR => {
                    debug!(
                    "create_node_helper() about to create a sub-directory with name={:?} and mode={:?} \
                        under parent directory of ino={} and name={:?}",
                    node_name, m_flags, parent, parent_name,
                );
                    parent_node
                    .create_child_dir(inum,node_name, m_flags)
                    .await
                    .context(format!(
                        "create_node_helper() failed to create directory with name={node_name:?} and mode={m_flags:?} \
                            under parent directory of ino={parent} and name={parent_name:?}",
                    ))?
                }
                SFlag::S_IFREG => {
                    let o_flags = OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_RDWR;
                    debug!(
                        "helper_create_node() about to \
                        create a file with name={:?}, oflags={:?}, mode={:?} \
                        under parent directory of ino={} and name={:?}",
                        node_name, o_flags, m_flags, parent, parent_name,
                    );
                    parent_node
                        .create_child_file(
                            inum,
                            node_name,
                            o_flags,
                            m_flags,
                            Arc::<GlobalCache>::clone(&self.data_cache),
                        )
                        .await
                        .context(format!(
                        "create_node_helper() failed to create file with name={node_name:?} and mode={m_flags:?} \
                            under parent directory of ino={parent} and name={parent_name:?}",
                    ))?
                }
                SFlag::S_IFLNK => {
                    debug!(
                        "create_node_helper() about to \
                        create a symlink with name={:?} to target path={:?} \
                        under parent directory of ino={} and name={:?}",
                        node_name, target_path, parent, parent_name
                    );
                    parent_node
                    .create_child_symlink(
                        inum,
                        node_name,
                        target_path.unwrap_or_else(|| panic!(
                            "create_node_helper() failed to \
                                get target path when create symlink with name={:?} \
                                under parent directory of ino={} and name={:?}",
                            node_name, parent, parent_node.get_name(),
                        )).to_owned(),
                    )
                    .await
                    .context(format!(
                        "create_node_helper() failed to create symlink with name={node_name:?} to target path={target_path:?} \
                            under parent directory of ino={parent} and name={parent_name:?}",
                    ))?
                }
                _ => {
                    panic!(
                    "create_node_helper() found unsupported i-node type={node_type:?} with name={node_name:?} to create \
                        under parent directory of ino={parent} and name={parent_name:?}",
                );
                }
            };
            let new_ino = new_node.get_ino();
            let new_node_attr = new_node.get_attr();
            let new_node_full_path = new_node.full_path().to_owned();
            cache.insert(new_ino, new_node);
            self.path2inum
                .write()
                .await
                .insert(new_node_full_path, new_ino);
            let fuse_attr = fs_util::convert_to_fuse_attr(new_node_attr);
            debug!(
                "create_node_helper() successfully created the new child name={:?} \
                of ino={} and type={:?} under parent ino={} and name={:?}",
                node_name, new_ino, node_type, parent, parent_name,
            );
            let pnode = cache.get(&parent).unwrap_or_else(|| {
                panic!("failed to get parent inode {parent:?}, parent name {parent_name:?}")
            });
            let parent_full_path = pnode.full_path().to_owned();

            (parent_full_path, full_path, new_node_attr, fuse_attr)
        };
        {
            self.sync_attr_remote(&parent_full_path).await;
            self.sync_dir_remote(&parent_full_path, node_name, &new_node_attr, target_path)
                .await;
            let cache = self.cache.read().await;
            let pnode = cache.get(&parent).unwrap_or_else(|| {
                panic!(
                    "failed to get parent inode {parent:?}, parent fullpath {parent_full_path:?}"
                )
            });
            // After sync to other，we should do async persist
            self.persist_handle.mark_dirty(
                parent,
                PersistDirContent::new_from_cache(
                    parent_full_path,
                    pnode.get_dir_data(),
                    serial::file_attr_to_serial(&pnode.get_attr()),
                ),
            );
        }
        // inode is cached, so we should remove the path mark
        // We dont need to sync for the unmark
        let etcd_client = Arc::clone(&self.etcd_client);
        let volume = Arc::clone(&self.volume_info);
        tokio::spawn(async move {
            let vol = volume;
            etcd::unmark_fullpath_with_ino_in_etcd(etcd_client, &vol, full_path).await;
        });

        let ttl = Duration::new(MY_TTL_SEC, 0);
        Ok((ttl, fuse_attr, MY_GENERATION))
    }

    /// Helper function to remove node
    async fn remove_node_helper(
        &self,
        parent: INum,
        node_name: &str,
        node_type: SFlag,
    ) -> DatenLordResult<()> {
        debug!(
            "remove_node_helper() about to remove parent ino={:?}, \
            child_name={:?}, child_type={:?}",
            parent, node_name, node_type
        );
        self.remove_node_local(parent, node_name, node_type, false)
            .await?;
        self.remove_remote(parent, node_name, node_type).await;
        self.load_parent_from_cache_and_mark_dirty(parent).await;
        Ok(())
    }

    /// Helper function to lookup
    #[allow(clippy::too_many_lines)]
    async fn lookup_helper(
        &self,
        parent: INum,
        child_name: &str,
    ) -> DatenLordResult<(Duration, FuseAttr, u64)> {
        let pre_check_res = self.lookup_pre_check(parent, child_name).await;
        let (ino, child_type, child_attr) = match pre_check_res {
            Ok((ino, child_type, child_attr)) => (ino, child_type, child_attr),
            Err(e) => {
                debug!("lookup() failed to pre-check, the error is: {}", e);
                return Err(e);
            }
        };

        let ttl = Duration::new(MY_TTL_SEC, 0);
        {
            // cache hit
            let cache = self.cache.read().await;
            if let Some(node) = cache.get(&ino) {
                debug!(
                    "lookup_helper() cache hit when searching i-node of \
                        ino={} and name={:?} under parent ino={}",
                    ino, child_name, parent,
                );
                let attr = node.lookup_attr();
                let fuse_attr = fs_util::convert_to_fuse_attr(attr);
                debug!(
                    "lookup_helper() successfully found in cache the i-node of \
                        ino={} name={:?} under parent ino={}, the attr={:?}",
                    ino, child_name, parent, &attr,
                );
                return Ok((ttl, fuse_attr, MY_GENERATION));
            }
        }
        {
            // cache miss
            debug!(
                "lookup_helper() cache missed when searching parent ino={} \
                    and i-node of ino={} and name={:?}",
                parent, ino, child_name,
            );
            let (mut child_node, parent_name) = {
                let cache = self.cache.read().await;
                let parent_node = cache.get(&parent).unwrap_or_else(|| {
                    panic!(
                        "lookup_helper() found fs is inconsistent, \
                        parent i-node of ino={parent} should be in cache",
                    );
                });
                let parent_name = parent_node.get_name().to_owned();
                let child_node = match child_type {
                    SFlag::S_IFDIR => parent_node
                        .open_child_dir(child_name, child_attr)
                        .await
                        .context(format!(
                            "lookup_helper() failed to open sub-directory name={child_name:?} \
                            under parent directory of ino={parent} and name={parent_name:?}",
                        ))?,
                    SFlag::S_IFREG => {
                        let oflags = OFlag::O_RDWR;
                        parent_node
                            .open_child_file(
                                child_name,
                                child_attr,
                                oflags,
                                Arc::<GlobalCache>::clone(&self.data_cache),
                            )
                            .await
                            .context(format!(
                            "lookup_helper() failed to open child file name={child_name:?} with flags={oflags:?} \
                                under parent directory of ino={parent} and name={parent_name:?}",
                        ))?
                    }
                    SFlag::S_IFLNK => parent_node
                        .load_child_symlink(child_name, child_attr)
                        .await
                        .context(format!(
                            "lookup_helper() failed to read child symlink name={child_name:?} \
                                under parent directory of ino={parent} and name={parent_name:?}",
                        ))?,
                    _ => panic!("lookup_helper() found unsupported file type={child_type:?}",),
                };
                (child_node, parent_name)
            };

            debug_assert_eq!(ino, child_node.get_ino());
            child_node.set_ino(ino);
            let child_ino = ino;
            let attr = child_node.lookup_attr();
            let full_path = child_node.full_path().to_owned();
            {
                let mut cache = self.cache.write().await;
                cache.insert(child_ino, child_node);
            }

            self.path2inum.write().await.insert(full_path, child_ino);

            let fuse_attr = fs_util::convert_to_fuse_attr(attr);
            debug!(
                "lookup_helper() successfully found the i-node of ino={} and name={:?} \
                under parent of ino={} and name={:?}",
                child_ino, child_name, parent, parent_name,
            );
            Ok((ttl, fuse_attr, MY_GENERATION))
        }
    }

    /// Rename helper to exchange on disk
    async fn rename_exchange_helper(&self, param: RenameParam) -> DatenLordResult<()> {
        let old = param.old_parent;
        let new = param.new_parent;
        self.rename_exchange_local(&param).await?;
        self.rename_remote(param).await;
        self.load_parent_from_cache_and_mark_dirty(old).await;
        self.load_parent_from_cache_and_mark_dirty(new).await;
        Ok(())
    }

    /// Rename helper to move on disk, it may replace destination entry
    async fn rename_may_replace_helper(&self, param: RenameParam) -> DatenLordResult<()> {
        let old = param.old_parent;
        let new = param.new_parent;
        self.rename_may_replace_local(&param, false).await?;
        self.rename_remote(param).await;
        self.load_parent_from_cache_and_mark_dirty(old).await;
        self.load_parent_from_cache_and_mark_dirty(new).await;
        Ok(())
    }

    /// Helper function of fsync
    async fn fsync_helper(
        &self,
        ino: u64,
        fh: u64,
        _datasync: bool,
        // reply: ReplyEmpty,
    ) -> DatenLordResult<()> {
        let mut cache = self.cache().write().await;
        let inode = cache.get_mut(&ino).unwrap_or_else(|| {
            panic!(
                "fsync_helper() found fs is inconsistent, \
                 the inode ino={ino} is not in cache"
            );
        });

        inode.flush(ino, fh).await;

        Ok(())
    }

    /// Helper function to write data
    async fn write_helper(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        data: Vec<u8>,
        flags: u32,
    ) -> DatenLordResult<usize> {
        let data_len = data.len();
        let (result, full_path, parent_ino) = {
            let mut cache = self.cache().write().await;
            let inode = cache.get_mut(&ino).unwrap_or_else(|| {
                panic!(
                    "write() found fs is inconsistent, \
                     the inode ino={ino} is not in cache"
                );
            });
            let parent_ino = inode.get_parent_ino();

            debug!(
                "write_helper() about to write {} byte data to file of ino={} \
                and name {:?} at offset={}",
                data.len(),
                ino,
                inode.get_name(),
                offset
            );
            let o_flags = fs_util::parse_oflag(flags);
            let write_to_disk = true;
            let res = inode
                .write_file(fh, offset, data, o_flags, write_to_disk)
                .await;
            (res, inode.get_full_path().to_owned(), parent_ino)
        };
        self.invalidate_remote(&full_path, offset, data_len).await;
        self.sync_attr_remote(&full_path).await;
        // update dir
        let cache = self.cache().write().await;
        let parent = cache.get(&parent_ino).unwrap_or_else(|| {
            panic!(
                "write() found fs is inconsistent, \
                        the inode ino={ino} is not in cache"
            );
        });
        self.persist_handle.mark_dirty(
            parent.get_ino(),
            PersistDirContent::new_from_cache(
                parent.full_path().to_owned(),
                parent.get_dir_data(),
                serial::file_attr_to_serial(&parent.get_attr()),
            ),
        );
        result
    }
    /// Stop all async tasks
    fn stop_all_async_tasks(&self) {
        self.persist_handle.system_end();
    }
}

impl<S: S3BackEnd + Send + Sync + 'static, K: KVEngine + 'static> S3MetaData<S, K> {
    #[allow(dead_code)]
    #[allow(clippy::unwrap_used)]
    /// Get a node from kv engine by inum
    pub async fn get_node_from_kv_engine(&self, inum: INum) -> Option<S3NodeWrap<S, K>> {
        let inum_key = KeyType::INum2Node(inum).get_key();
        let raw_data = self.kv_engine.get(&inum_key).await.unwrap_or_else(|e| {
            panic!(
                "get_node_from_kv_engine() failed to get node of ino={inum} from kv engine, \
                        error={e:?}"
            );
        });
        let raw_data = raw_data.as_ref()?;
        // deserialize node
        Some(S3NodeWrap::new(
            serde_json::from_slice::<ValueType>(raw_data)
                .unwrap_or_else(|e| {
                    panic!(
                        "get_node_from_kv_engine() failed to deserialize node of ino={inum} from \
                        kv engine, error={e:?}"
                    );
                })
                .into_s3_node(self)
                .await,
            self,
        ))
    }

    #[allow(dead_code)]
    /// Set node to kv engine use inum
    pub async fn set_node_to_kv_engine(&self, inum: INum, node: S3Node<S>) {
        let inum_key = KeyType::INum2Node(inum).get_key();
        let node_value = serde_json::to_vec::<ValueType>(&ValueType::Node(node.into_serial_node()))
            .unwrap_or_else(|e| {
                panic!(
                    "set_node_to_kv_engine() failed to serialize node of ino={inum} to kv engine, \
                        error={e:?}"
                );
            });
        self.kv_engine
            .set(&inum_key, &node_value)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "set_node_to_kv_engine() failed to set node of ino={inum} to kv engine, \
                        error={e:?}"
                );
            });
    }

    /// Remove node from kv engine use inum
    pub async fn remove_node_from_kv_engine(&self, inum: INum) {
        let inum_key = KeyType::INum2Node(inum).get_key();
        self.kv_engine.delete(&inum_key).await.unwrap_or_else(|e| {
            panic!(
                "remove_node_from_kv_engine() failed to remove node of ino={inum} from kv engine, \
                        error={e:?}"
            );
        });
    }

    #[allow(dead_code)]
    #[allow(clippy::unwrap_used)]
    /// Get inum from kv engine use full path
    pub async fn get_inum_from_kv_engine(&self, full_path: &str) -> Option<INum> {
        let full_path_key = KeyType::Path2INum(full_path.to_owned()).get_key();
        let raw_data = self
            .kv_engine
            .get(&full_path_key)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "get_inum_from_kv_engine() failed to get inum of full_path={full_path} from kv engine, \
                    error={e:?}"
                );
            });
        let raw_data = raw_data.as_ref()?;
        // deserialize inum
        Some(
            serde_json::from_slice::<ValueType>(raw_data).unwrap_or_else(|e| {
                panic!(
                    "get_inum_from_kv_engine() failed to deserialize inum of full_path={full_path} from kv engine, \
                        error={e:?}"
                );
            }).into_inum()
        )
    }

    #[allow(dead_code)]
    /// Set inum to kv engine use full path
    async fn set_inum_to_kv_engine(&self, full_path: &str, inum: INum) {
        let full_path_key = KeyType::Path2INum(full_path.to_owned()).get_key();
        let inum_value = serde_json::to_vec::<ValueType>(&ValueType::INum(inum)).unwrap_or_else(
            |e| {
                panic!(
                    "set_inum_to_kv_engine() failed to serialize inum of full_path={full_path} to kv engine, \
                        error={e:?}"
                );
            },
        );
        self.kv_engine
            .set(&full_path_key, &inum_value)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "set_inum_to_kv_engine() failed to set inum of full_path={full_path} to kv engine, \
                        error={e:?}"
                );
            });
    }

    /// Get parent content from cache and do the persist operation.
    /// This function will try to get the lock, so make sure there's no deadlock.
    async fn load_parent_from_cache_and_mark_dirty(&self, parent: INum) {
        let cache = self.cache.read().await;
        let p = cache.get(&parent).unwrap_or_else(|| {
            panic!(
                "load_parent_from_cache_and_mark_dirty() found fs is inconsistent, \
                    parent of ino={parent} should be in cache",
            );
        });
        self.persist_handle.mark_dirty(
            parent,
            PersistDirContent::new_from_cache(
                p.full_path().to_owned(),
                p.get_dir_data(),
                serial::file_attr_to_serial(&p.get_attr()),
            ),
        );
    }

    /// The pre-check before create node
    #[allow(single_use_lifetimes)]
    fn create_node_pre_check<'b>(
        parent: INum,
        node_name: &str,
        cache: &'b mut RwLockWriteGuard<BTreeMap<INum, <Self as MetaData>::N>>,
    ) -> DatenLordResult<&'b mut <Self as MetaData>::N> {
        let parent_node = cache.get_mut(&parent).unwrap_or_else(|| {
            panic!(
                "create_node_pre_check() found fs is inconsistent, \
                    parent of ino={parent} should be in cache before create it new child",
            );
        });
        if let Some(occupied) = parent_node.get_entry(node_name) {
            debug!(
                "create_node_pre_check() found the directory of ino={} and name={:?} \
                    already exists a child with name={:?} and ino={}",
                parent,
                parent_node.get_name(),
                node_name,
                occupied.ino(),
            );
            return util::build_error_result_from_errno(
                Errno::EEXIST,
                format!(
                    "create_node_pre_check() found the directory of ino={} and name={:?} \
                        already exists a child with name={:?} and ino={}",
                    parent,
                    parent_node.get_name(),
                    node_name,
                    occupied.ino(),
                ),
            );
        }
        Ok(parent_node)
    }

    /// Helper function to pre-check if node can be deferred deleted.
    fn deferred_delete_pre_check(inode: &S3Node<S>) -> (bool, INum, String) {
        // pre-check whether deferred delete or not
        debug_assert!(inode.get_lookup_count() >= 0); // lookup count cannot be negative
        debug_assert!(inode.get_open_count() >= 0); // open count cannot be negative
        (
            inode.get_lookup_count() > 0 || inode.get_open_count() > 0,
            inode.get_parent_ino(),
            inode.get_name().to_owned(),
        )
    }

    /// Helper function to delete or deferred delete node
    async fn may_deferred_delete_node_helper(
        &self,
        ino: INum,
        from_remote: bool,
    ) -> DatenLordResult<()> {
        // remove entry from parent i-node
        let mut cache = self.cache.write().await;
        let inode = cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "may_deferred_delete_node_helper() failed to \
                         find the i-node of ino={ino} to remove",
            );
        });
        let (deferred_deletion, parent_ino, node_name) = Self::deferred_delete_pre_check(inode);
        let parent_node = cache.get_mut(&parent_ino).unwrap_or_else(|| {
            panic!(
                "may_deferred_delete_node_helper() failed to \
                     find the parent of ino={parent_ino} for i-node of ino={ino}",
            );
        });
        let deleted_entry = parent_node.unlink_entry(&node_name).await.context(format!(
            "may_deferred_delete_node_helper() failed to remove entry name={node_name:?} \
                 and ino={ino} from parent directory ino={parent_ino}",
        ))?;
        debug!(
            "may_deferred_delete_node_helper() successfully remove entry name={:?} \
                 ino={} from parent directory ino={}",
            node_name, ino, parent_ino
        );
        debug_assert_eq!(node_name, deleted_entry.entry_name());
        debug_assert_eq!(deleted_entry.ino(), ino);

        if deferred_deletion {
            // Deferred deletion
            let inode = cache.get(&ino).unwrap_or_else(|| {
                panic!(
                    "impossible case, may_deferred_delete_node_helper() \
                     i-node of ino={ino} is not in cache",
                );
            });
            debug!(
                "may_deferred_delete_node_helper() defered removed \
                    the i-node name={:?} of ino={} under parent ino={}, \
                    open count={}, lookup count={}",
                inode.get_name(),
                ino,
                parent_ino,
                inode.get_open_count(),
                inode.get_lookup_count(),
            );
            inode.mark_deferred_deletion();
            // Notify kernel to drop cache
            if from_remote && inode.get_lookup_count() > 0 {
                let fuse_fd = *self.fuse_fd.lock().await;
                // fuse_fd must be set
                assert!(fuse_fd > 0_i32);
                #[cfg(feature = "abi-7-18")]
                {
                    let fuse_delete_notification = FuseDeleteNotification::new(fuse_fd);
                    fuse_delete_notification
                        .notify(parent_ino, ino, inode.get_name().to_owned())
                        .await?;
                }
            }
        } else {
            // immediate deletion
            let inode = cache.remove(&ino).unwrap_or_else(|| {
                panic!(
                    "impossible case, may_deferred_delete_node_helper() \
                     i-node of ino={ino} is not in cache",
                )
            });
            debug!(
                "may_deferred_delete_node_helper() immediately removed \
                    the i-node name={:?} of ino={} under parent ino={}, \
                    open count={}, lookup count={}",
                inode.get_name(),
                ino,
                parent_ino,
                inode.get_open_count(),
                inode.get_lookup_count(),
            );
        }
        Ok(())
    }

    /// Lookup helper function to pre-check
    async fn lookup_pre_check(
        &self,
        parent: INum,
        name: &str,
    ) -> DatenLordResult<(INum, SFlag, Arc<SyncRwLock<FileAttr>>)> {
        // lookup child ino and type first
        let cache = self.cache.read().await;
        let parent_node = cache.get(&parent).unwrap_or_else(|| {
            panic!(
                "lookup_helper() found fs is inconsistent, \
                        the parent i-node of ino={parent} should be in cache",
            );
        });
        if let Some(child_entry) = parent_node.get_entry(name) {
            let ino = child_entry.ino();
            let child_type = child_entry.entry_type();
            Ok((ino, child_type, Arc::clone(child_entry.file_attr_arc_ref())))
        } else {
            debug!(
                "lookup_helper() failed to find the file name={:?} \
                    under parent directory of ino={} and name={:?}",
                name,
                parent,
                parent_node.get_name(),
            );
            // lookup() didn't find anything, this is normal
            util::build_error_result_from_errno(
                Errno::ENOENT,
                format!(
                    "lookup_helper() failed to find the file name={:?} \
                        under parent directory of ino={} and name={:?}",
                    name,
                    parent,
                    parent_node.get_name(),
                ),
            )
        }
    }

    /// Rename helper function to pre-check
    async fn rename_pre_check(
        &self,
        old_parent: INum,
        old_name: &str,
        new_parent: INum,
        new_name: &str,
        no_replace: bool,
    ) -> DatenLordResult<(RawFd, INum, RawFd, Option<INum>)> {
        let cache = self.cache.read().await;
        let old_parent_node = cache.get(&old_parent).unwrap_or_else(|| {
            panic!(
                "rename() found fs is inconsistent, \
                    the parent i-node of ino={old_parent} should be in cache",
            );
        });
        let old_parent_fd = old_parent_node.get_fd();
        let old_entry_ino = match old_parent_node.get_entry(old_name) {
            None => {
                debug!(
                    "rename() failed to find child entry of name={:?} under parent directory ino={} and name={:?}",
                    old_name, old_parent, old_parent_node.get_name(),
                );
                return util::build_error_result_from_errno(
                    Errno::ENOENT,
                    format!(
                        "rename_pre_check() failed to find child entry of name={:?} \
                            under parent directory ino={} and name={:?}",
                        old_name,
                        old_parent,
                        old_parent_node.get_name(),
                    ),
                );
            }
            Some(old_entry) => {
                debug_assert_eq!(&old_name, &old_entry.entry_name());
                assert!(cache.get(&old_entry.ino()).is_some(), "rename() found fs is inconsistent, the i-node of ino={} and name={:?} \
                            under parent directory of ino={} and name={:?} to rename should be in cache",
                        old_entry.ino(), old_name, old_parent, old_parent_node.get_name(),);
                old_entry.ino()
            }
        };

        let new_parent_node = cache.get(&new_parent).unwrap_or_else(|| {
            panic!(
                "rename() found fs is inconsistent, \
                    the new parent i-node of ino={new_parent} should be in cache",
            );
        });
        let new_parent_fd = new_parent_node.get_fd();
        let new_entry_ino = if let Some(new_entry) = new_parent_node.get_entry(new_name) {
            debug_assert_eq!(&new_name, &new_entry.entry_name());
            let new_ino = new_entry.ino();
            assert!(cache.get(&new_ino).is_some(), "rename() found fs is inconsistent, the i-node of ino={} and name={:?} \
                        under parent directory of ino={} and name={:?} to replace should be in cache",
                    new_ino, new_name, new_parent, new_parent_node.get_name(),);
            if no_replace {
                debug!(
                    "rename() found i-node of ino={} and name={:?} under new parent ino={} and name={:?}, \
                        but RENAME_NOREPLACE is specified",
                    new_ino, new_name, new_parent, new_parent_node.get_name(),
                );
                return util::build_error_result_from_errno(
                    Errno::EEXIST, // RENAME_NOREPLACE
                    format!(
                        "rename() found i-node of ino={} and name={:?} under new parent ino={} and name={:?}, \
                            but RENAME_NOREPLACE is specified",
                        new_ino, new_name, new_parent, new_parent_node.get_name(),
                    ),
                );
            }
            debug!(
                "rename() found the new parent directory of ino={} and name={:?} already has a child with name={:?}",
                new_parent, new_parent_node.get_name(), new_name,
            );
            Some(new_ino)
        } else {
            None
        };
        Ok((old_parent_fd, old_entry_ino, new_parent_fd, new_entry_ino))
    }

    /// Rename in cache helper
    async fn rename_in_cache_helper(
        &self,
        old_parent: INum,
        old_name: &str,
        new_parent: INum,
        new_name: &str,
    ) -> Option<DirEntry> {
        let mut cache = self.cache.write().await;
        let old_parent_node = cache.get_mut(&old_parent).unwrap_or_else(|| {
            panic!(
                "impossible case when rename, the from parent i-node of ino={old_parent} should be in cache",
            )
        });
        let entry_to_move = match old_parent_node.remove_entry_for_rename(old_name) {
            None => panic!(
                "impossible case when rename, the from entry of name={:?} \
                        should be under from directory ino={} and name={:?}",
                old_name,
                old_parent,
                old_parent_node.get_name(),
            ),
            Some(old_entry) => DirEntry::new(
                new_name.to_owned(),
                Arc::clone(old_entry.file_attr_arc_ref()),
            ),
        };

        s3_node::rename_fullpath_recursive(entry_to_move.ino(), new_parent, &mut cache).await;

        let new_parent_node = cache.get_mut(&new_parent).unwrap_or_else(|| {
            panic!(
                "impossible case when rename, the to parent i-node of ino={new_parent} should be in cache"
            )
        });
        new_parent_node.insert_entry_for_rename(entry_to_move)
    }

    /// Rename exchange on local node
    async fn rename_exchange_local(&self, param: &RenameParam) -> DatenLordResult<()> {
        let old_parent = param.old_parent;
        let old_name = param.old_name.as_str();
        let new_parent = param.new_parent;
        let new_name = &param.new_name;
        let flags = param.flags;
        debug!(
            "rename(old parent={}, old name={:?}, new parent={}, new name={:?})",
            old_parent, old_name, new_parent, new_name,
        );
        let no_replace = flags == 1; // RENAME_NOREPLACE

        let pre_check_res = self
            .rename_pre_check(old_parent, old_name, new_parent, new_name, no_replace)
            .await;
        let (_, _, _, new_entry_ino) = match pre_check_res {
            Ok((old_parent_fd, old_entry_ino, new_parent_fd, new_entry_ino)) => {
                (old_parent_fd, old_entry_ino, new_parent_fd, new_entry_ino)
            }
            Err(e) => {
                debug!("rename() pre-check failed, the error is: {}", e);
                return Err(e);
            }
        };
        let new_entry_ino =
            new_entry_ino.unwrap_or_else(|| panic!("failed to get new entry inode"));

        let rename_in_cache_res = self
            .rename_in_cache_helper(old_parent, old_name, new_parent, new_name)
            .await;

        if let Some(replaced_entry) = rename_in_cache_res {
            debug_assert_eq!(
                new_entry_ino,
                replaced_entry.ino(),
                "rename_exchange_helper() replaced entry i-number not match"
            );
            let attr = Arc::clone(replaced_entry.file_attr_arc_ref());
            attr.write().ino = new_entry_ino;
            let exchange_entry = DirEntry::new(old_name.to_owned(), attr);

            let mut cache = self.cache.write().await;
            let old_parent_node = cache.get_mut(&old_parent).unwrap_or_else(|| {
                panic!(
                    "impossible case when rename, the from parent i-node of ino={old_parent} should be in cache",
                )
            });
            let insert_res = old_parent_node.insert_entry_for_rename(exchange_entry);
            debug_assert!(
                insert_res.is_none(),
                "impossible case when rename, the from i-node of name={:?} should have been \
                    moved out of from parent directory ino={} and name={:?}",
                old_name,
                old_parent,
                old_parent_node.get_name(),
            );
            // TODO: finish rename exchange when libc::rename2 is available
            // call rename2 here to exchange two nodes
            let exchanged_node = cache.get_mut(&new_entry_ino).unwrap_or_else(|| {
                panic!(
                    "impossible case when rename, the new entry i-node of ino={new_entry_ino} should be in cache",
                )
            });
            exchanged_node.set_parent_ino(old_parent);
            exchanged_node.set_name(old_name);
            let exchanged_attr = exchanged_node
                .load_attribute()
                .await
                .context(format!(
                    "rename_exchange_helper() failed to load attribute of \
                        to i-node of ino={new_entry_ino} and name={new_name:?} under parent directory",
                ))
                .unwrap_or_else(|e| {
                    panic!(
                        "rename_exchange_helper() failed to load attributed of to i-node of ino={} and name={:?}, \
                            the error is: {}",
                        exchanged_node.get_ino(), exchanged_node.get_name(),
                        crate::common::util::format_anyhow_error(&e),
                    )
                });
            debug_assert_eq!(exchanged_attr.ino, exchanged_node.get_ino());
            debug_assert_eq!(exchanged_attr.ino, new_entry_ino);
            panic!("rename2 system call has not been supported in libc to exchange two nodes yet!");
        } else {
            panic!(
                "impossible case, the child i-node of name={new_name:?} to be exchanged \
                    should be under to parent directory ino={new_parent}",
            );
        }
    }

    /// Rename to move on disk locally, it may replace destination entry
    async fn rename_may_replace_local(
        &self,
        param: &RenameParam,
        from_remote: bool,
    ) -> DatenLordResult<()> {
        let old_parent = param.old_parent;
        let old_name = &param.old_name;
        let new_parent = param.new_parent;
        let new_name = &param.new_name;
        let flags = param.flags;
        debug!(
            "rename(old parent={}, old name={:?}, new parent={}, new name={:?})",
            old_parent, old_name, new_parent, new_name,
        );
        let no_replace = flags == 1; // RENAME_NOREPLACE

        let pre_check_res = self
            .rename_pre_check(old_parent, old_name, new_parent, new_name, no_replace)
            .await;
        let (_, old_entry_ino, _, new_entry_ino) = match pre_check_res {
            Ok((old_parent_fd, old_entry_ino, new_parent_fd, new_entry_ino)) => {
                (old_parent_fd, old_entry_ino, new_parent_fd, new_entry_ino)
            }
            Err(e) => {
                debug!("rename() pre-check failed, the error is: {}", e);
                return Err(e);
            }
        };

        // Just replace new entry, may deferred delete
        if let Some(new_ino) = new_entry_ino {
            self.may_deferred_delete_node_helper(new_ino, from_remote)
                .await
                .context(format!(
                    "rename_may_replace_local() failed to \
                        maybe deferred delete the replaced i-node ino={new_ino}",
                ))?;
        }

        {
            let mut cache = self.cache.write().await;
            let moved_node = cache.get_mut(&old_entry_ino).unwrap_or_else(|| {
                panic!(
                "impossible case when rename, the from entry i-node of ino={old_entry_ino} should be in cache",
            )
            });
            moved_node.set_parent_ino(new_parent);
            moved_node.set_name(new_name);
            debug!(
                "rename_may_replace_local() successfully moved the from i-node \
                of ino={} and name={:?} under from parent ino={} to \
                the to i-node of ino={} and name={:?} under to parent ino={}",
                old_entry_ino, old_name, old_parent, old_entry_ino, new_name, new_parent,
            );
        }

        let rename_replace_res = self
            .rename_in_cache_helper(old_parent, old_name, new_parent, new_name)
            .await;
        debug_assert!(
            rename_replace_res.is_none(),
            "rename_may_replace_local() should already have \
                deleted the target i-node to be replaced",
        );
        Ok(())
    }

    /// Helper function to rename file locally
    pub(crate) async fn rename_local(&self, param: &RenameParam, from_remote: bool) {
        if param.flags == 2 {
            if let Err(e) = self.rename_exchange_local(param).await {
                panic!("failed to rename local param={param:?}, error is {e:?}");
            }
        } else if let Err(e) = self.rename_may_replace_local(param, from_remote).await {
            panic!("failed to rename local param={param:?}, error is {e:?}");
        } else {
        }
    }

    /// Helper function to remove node locally
    pub(crate) async fn remove_node_local(
        &self,
        parent: INum,
        node_name: &str,
        node_type: SFlag,
        from_remote: bool,
    ) -> DatenLordResult<()> {
        debug!(
            "remove_node_local() about to remove parent ino={:?}, \
            child_name={:?}, child_type={:?}",
            parent, node_name, node_type
        );
        let node_ino: INum;
        {
            // pre-checks
            let cache = self.cache.read().await;
            let parent_node = cache.get(&parent).unwrap_or_else(|| {
                panic!(
                    "remove_node_local() found fs is inconsistent, \
                        parent of ino={parent} should be in cache before remove its child",
                );
            });
            match parent_node.get_entry(node_name) {
                None => {
                    debug!(
                        "remove_node_local() failed to find i-node name={:?} \
                            under parent of ino={}",
                        node_name, parent,
                    );
                    return util::build_error_result_from_errno(
                        Errno::ENOENT,
                        format!(
                            "remove_node_local() failed to find i-node name={node_name:?} \
                                under parent of ino={parent}",
                        ),
                    );
                }
                Some(child_entry) => {
                    node_ino = child_entry.ino();
                    if let SFlag::S_IFDIR = node_type {
                        // check the directory to delete is empty
                        let dir_node = cache.get(&node_ino).unwrap_or_else(|| {
                            panic!(
                                "remove_node_local() found fs is inconsistent, \
                                    directory name={node_name:?} of ino={node_ino} \
                                    found under the parent of ino={parent}, \
                                    but no i-node found for this directory",
                            );
                        });
                        if !dir_node.is_node_data_empty() {
                            debug!(
                                "remove_node_local() cannot remove \
                                    the non-empty directory name={:?} of ino={} \
                                    under the parent directory of ino={}",
                                node_name, node_ino, parent,
                            );
                            return util::build_error_result_from_errno(
                                Errno::ENOTEMPTY,
                                format!(
                                    "remove_node_local() cannot remove \
                                        the non-empty directory name={node_name:?} of ino={node_ino} \
                                        under the parent directory of ino={parent}",
                                ),
                            );
                        }
                    }

                    let child_node = cache.get(&node_ino).unwrap_or_else(|| {
                        panic!(
                            "remove_node_local() found fs is inconsistent, \
                                i-node name={node_name:?} of ino={node_ino} found under the parent of ino={parent}, \
                                but no i-node found for this node"
                        )
                    });

                    debug_assert_eq!(node_ino, child_node.get_ino());
                    debug_assert_eq!(node_name, child_node.get_name());
                    debug_assert_eq!(parent, child_node.get_parent_ino());
                    debug_assert_eq!(node_type, child_node.get_type());
                    debug_assert_eq!(node_type, child_node.get_attr().kind);
                }
            }
        }
        {
            // all checks passed, ready to remove,
            // when deferred deletion, remove entry from directory first
            self.may_deferred_delete_node_helper(node_ino, from_remote)
                .await
                .context(format!(
                    "remove_node_local() failed to maybe deferred delete child i-node of ino={node_ino}, \
                        name={node_name:?} and type={node_type:?} under parent ino={parent}",
                ))?;
            // reply.ok().await?;
            debug!(
                "remove_node_local() successfully removed child i-node of ino={}, \
                    name={:?} and type={:?} under parent ino={}",
                node_ino, node_name, node_type, parent,
            );
        }
        Ok(())
    }

    /// Sync attr to other nodes
    async fn sync_attr_remote(&self, full_path: &str) {
        let inum = {
            let path2inum = self.path2inum.read().await;
            path2inum
                .get(full_path)
                .unwrap_or_else(|| panic!("failed to find inum of {full_path:?} from path2inum"))
                .to_owned()
        };
        let attr = self
            .cache
            .read()
            .await
            .get(&inum)
            .unwrap_or_else(|| panic!("failed to find inum={inum:?} path={full_path:?} from cache"))
            .get_attr();
        if let Err(e) = dist_client::push_attr(
            Arc::<EtcdDelegate>::clone(&self.etcd_client),
            &self.node_id,
            &self.volume_info,
            full_path,
            &attr,
        )
        .await
        {
            panic!("failed to sync attribute to others, error: {e}");
        }
    }

    /// Sync dir to other nodes
    async fn sync_dir_remote(
        &self,
        parent: &str,
        child_name: &str,
        child_attr: &FileAttr,
        target_path: Option<&Path>,
    ) {
        if let Err(e) = dist_client::update_dir(
            Arc::<EtcdDelegate>::clone(&self.etcd_client),
            &self.node_id,
            &self.volume_info,
            parent,
            child_name,
            child_attr,
            target_path,
        )
        .await
        {
            panic!("failed to sync dir to others, error: {e}");
        }
    }

    /// Sync rename request to other nodes
    async fn rename_remote(&self, args: RenameParam) {
        if let Err(e) = dist_client::rename(
            Arc::<EtcdDelegate>::clone(&self.etcd_client),
            &self.node_id,
            &self.volume_info,
            args,
        )
        .await
        {
            panic!("failed to sync rename request to others, error: {e}");
        }
    }

    /// Sync remove request to other nodes
    async fn remove_remote(&self, parent: INum, child_name: &str, child_type: SFlag) {
        if let Err(e) = dist_client::remove(
            Arc::<EtcdDelegate>::clone(&self.etcd_client),
            &self.node_id,
            &self.volume_info,
            parent,
            child_name,
            child_type,
        )
        .await
        {
            panic!("failed to sync rename request to others, error: {e}");
        }
    }

    /// Invalidate cache from other nodes
    async fn invalidate_remote(&self, full_path: &str, offset: i64, len: usize) {
        if let Err(e) = dist_client::invalidate(
            Arc::<EtcdDelegate>::clone(&self.etcd_client),
            &self.node_id,
            &self.volume_info,
            full_path,
            offset
                .overflow_div(self.data_cache.get_align().cast())
                .cast(),
            offset
                .overflow_add(len.cast())
                .overflow_sub(1)
                .overflow_div(self.data_cache.get_align().cast())
                .cast(),
        )
        .await
        {
            panic!("failed to invlidate others' cache, error: {e}");
        }
    }
}
