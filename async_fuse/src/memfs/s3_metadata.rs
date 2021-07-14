use super::cache::GlobalCache;
use super::dir::DirEntry;
use super::dist::server::CacheServer;
use super::fs_util;
use super::metadata::MetaData;
use super::node::Node;
use super::s3_node::{self, S3Node};
use super::s3_wrapper::S3BackEnd;
use super::RenameParam;
use crate::fuse::protocol::{FuseAttr, INum, FUSE_ROOT_ID};
use crate::util;
use anyhow::Context;
use async_trait::async_trait;
use common::etcd_delegate::EtcdDelegate;
use itertools::Itertools;
use log::debug;
use nix::errno::Errno;
use nix::fcntl::OFlag;
use nix::sys::stat::SFlag;
use smol::lock::{RwLock, RwLockWriteGuard};
use std::collections::{BTreeMap, BTreeSet};
use std::os::unix::io::RawFd;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{atomic::AtomicU32, Arc};
use std::time::Duration;

/// The time-to-live seconds of FUSE attributes
const MY_TTL_SEC: u64 = 3600; // TODO: should be a long value, say 1 hour
/// The generation ID of FUSE attributes
const MY_GENERATION: u64 = 1; // TODO: find a proper way to set generation
/// S3 information string delimiter
const S3_INFO_DELIMITER: char = ';';

/// File system in-memory meta-data
#[derive(Debug)]
pub struct S3MetaData<S: S3BackEnd + Send + Sync + 'static> {
    /// S3 backend
    s3_backend: Arc<S>,
    /// Etcd client
    pub(crate) etcd_client: Arc<EtcdDelegate>,
    /// The cache to hold opened directories and files
    pub(crate) cache: RwLock<BTreeMap<INum, S3Node<S>>>,
    /// The trash to hold deferred deleted directories and files
    trash: RwLock<BTreeSet<INum>>,
    /// Global data cache
    data_cache: Arc<GlobalCache>,
    /// Current available node number, it'll increase after using
    pub(crate) cur_inum: AtomicU32,
    /// Current available fd, it'll increase after using
    pub(crate) cur_fd: AtomicU32,
    /// Current service id
    pub(crate) node_id: u64,
    /// Volume Info
    pub(crate) volume_info: String,
    /// Full path and node mapping
    pub(crate) path2inum: RwLock<BTreeMap<String, INum>>,
}

fn parse_s3_info(info: &str) -> (&str, &str, &str, &str) {
    info.split(S3_INFO_DELIMITER)
        .next_tuple()
        .unwrap_or_else(|| panic!("parse s3 information failed. s3_info: {}", info))
}

#[async_trait]
impl<S: S3BackEnd + Sync + Send + 'static> MetaData for S3MetaData<S> {
    type N = S3Node<S>;

    async fn new(
        s3_info: &str,
        capacity: usize,
        ip: &str,
        port: &str,
        etcd_client: EtcdDelegate,
        node_id: u64,
        volume_info: &str,
    ) -> (Arc<Self>, Option<CacheServer>) {
        let (bucket_name, endpoint, access_key, secret_key) = parse_s3_info(s3_info);
        let s3_backend = Arc::new(
            match S::new(bucket_name, endpoint, access_key, secret_key).await {
                Ok(s) => s,
                Err(e) => panic!("{:?}", e),
            },
        );

        let etcd_arc = Arc::new(etcd_client);
        let data_cache = Arc::new(GlobalCache::new_dist_with_bz_and_capacity(
            10 * 1024 * 1024,
            capacity,
            etcd_arc.clone(),
            node_id,
        ));
        let meta = Arc::new(Self {
            s3_backend: Arc::clone(&s3_backend),
            cache: RwLock::new(BTreeMap::new()),
            etcd_client: etcd_arc,
            trash: RwLock::new(BTreeSet::new()),
            data_cache: data_cache.clone(),
            cur_inum: AtomicU32::new(2),
            cur_fd: AtomicU32::new(4),
            node_id,
            volume_info: volume_info.to_owned(),
            path2inum: RwLock::new(BTreeMap::new()),
        });

        let server = CacheServer::new(ip.to_owned(), port.to_owned(), data_cache, meta.clone());

        let root_inode = S3Node::open_root_node(FUSE_ROOT_ID, "/", s3_backend, Arc::clone(&meta))
            .await
            .context("failed to open FUSE root node")
            .unwrap_or_else(|e| {
                panic!("{}", e);
            });

        let full_path = root_inode.full_path().to_owned();
        meta.cache.write().await.insert(FUSE_ROOT_ID, root_inode);
        meta.path2inum.write().await.insert(full_path, FUSE_ROOT_ID);

        (meta, Some(server))
    }

    /// Get metadata cache
    fn cache(&self) -> &RwLock<BTreeMap<INum, Self::N>> {
        &self.cache
    }

    async fn delete_trash(&self, ino: &INum) -> bool {
        let mut trash = self.trash.write().await;
        if trash.contains(ino) {
            // deferred deletion
            trash.remove(ino);
            let mut cache = self.cache.write().await;
            let deleted_node = cache.remove(&ino).unwrap_or_else(|| {
                panic!(
                    "forget() found fs is inconsistent, i-node of ino={} \
                                found in trash, but no i-node found for deferred deletion",
                    ino,
                );
            });
            debug_assert_eq!(deleted_node.get_lookup_count(), 0);
            debug!(
                "forget() deferred deleted i-node of ino={} and name={:?}",
                ino,
                deleted_node.get_name(),
            );
            return true;
        }
        false
    }

    /// Helper function to create node
    async fn create_node_helper(
        &self,
        parent: INum,
        node_name: &str,
        mode: u32,
        node_type: SFlag,
        target_path: Option<&Path>,
    ) -> anyhow::Result<(Duration, FuseAttr, u64)> {
        // pre-check
        let mut cache = self.cache.write().await;
        let parent_node = self
            .create_node_pre_check(parent, &node_name, &mut cache)
            .await
            .context("create_node_helper() failed to pre check")?;
        let parent_name = parent_node.get_name().to_owned();
        // all checks are passed, ready to create new node
        let m_flags = fs_util::parse_mode(mode);
        let new_ino: u64;
        let node_name_clone = node_name.clone();
        let new_node = match node_type {
            SFlag::S_IFDIR => {
                debug!(
                    "create_node_helper() about to create a sub-directory with name={:?} and mode={:?} \
                        under parent directory of ino={} and name={:?}",
                    node_name, m_flags, parent, parent_name,
                );
                parent_node
                    .create_child_dir(node_name.clone(), m_flags)
                    .await
                    .context(format!(
                        "create_node_helper() failed to create directory with name={:?} and mode={:?} \
                            under parent directory of ino={} and name={:?}",
                        node_name, m_flags, parent, parent_name,
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
                    .create_child_file(node_name.clone(), o_flags, m_flags, self.data_cache.clone())
                    .await
                    .context(format!(
                        "create_node_helper() failed to create file with name={:?} and mode={:?} \
                            under parent directory of ino={} and name={:?}",
                        node_name, m_flags, parent, parent_name,
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
                        node_name.clone(),
                        target_path.unwrap_or_else(|| panic!(
                            "create_node_helper() failed to \
                                get target path when create symlink with name={:?} \
                                under parent directory of ino={} and name={:?}",
                            node_name, parent, parent_node.get_name(),
                        )).to_owned(),
                    )
                    .await
                    .context(format!(
                        "create_node_helper() failed to create symlink with name={:?} to target path={:?} \
                            under parent directory of ino={} and name={:?}",
                        node_name, target_path, parent, parent_name,
                    ))?
            }
            _ => {
                panic!(
                    "create_node_helper() found unsupported i-node type={:?} with name={:?} to create \
                        under parent directory of ino={} and name={:?}",
                        node_type, node_name, parent, parent_name,
                );
            }
        };
        new_ino = new_node.get_ino();
        let new_node_attr = new_node.get_attr();
        let new_node_full_path = new_node.full_path().to_owned();
        cache.insert(new_ino, new_node);

        self.path2inum
            .write()
            .await
            .insert(new_node_full_path, new_ino);

        let ttl = Duration::new(MY_TTL_SEC, 0);
        let fuse_attr = fs_util::convert_to_fuse_attr(new_node_attr);
        debug!(
            "create_node_helper() successfully created the new child name={:?} \
                of ino={} and type={:?} under parent ino={} and name={:?}",
            node_name_clone, new_ino, node_type, parent, parent_name,
        );
        Ok((ttl, fuse_attr, MY_GENERATION))
    }

    /// Helper function to remove node
    async fn remove_node_helper(
        &self,
        parent: INum,
        node_name: &str,
        node_type: SFlag,
    ) -> anyhow::Result<()> {
        let node_ino: INum;
        {
            // pre-checks
            let cache = self.cache.read().await;
            let parent_node = cache.get(&parent).unwrap_or_else(|| {
                panic!(
                    "remove_node_helper() found fs is inconsistent, \
                        parent of ino={} should be in cache before remove its child",
                    parent,
                );
            });
            match parent_node.get_entry(&node_name) {
                None => {
                    debug!(
                        "remove_node_helper() failed to find i-node name={:?} \
                            under parent of ino={}",
                        node_name, parent,
                    );
                    return util::build_error_result_from_errno(
                        Errno::ENOENT,
                        format!(
                            "remove_node_helper() failed to find i-node name={:?} \
                                under parent of ino={}",
                            node_name, parent,
                        ),
                    );
                }
                Some(child_entry) => {
                    node_ino = child_entry.ino();
                    if let SFlag::S_IFDIR = node_type {
                        // check the directory to delete is empty
                        let dir_node = cache.get(&node_ino).unwrap_or_else(|| {
                            panic!(
                                "remove_node_helper() found fs is inconsistent, \
                                    directory name={:?} of ino={} \
                                    found under the parent of ino={}, \
                                    but no i-node found for this directory",
                                node_name, node_ino, parent,
                            );
                        });
                        if !dir_node.is_node_data_empty() {
                            debug!(
                                "remove_node_helper() cannot remove \
                                    the non-empty directory name={:?} of ino={} \
                                    under the parent directory of ino={}",
                                node_name, node_ino, parent,
                            );
                            return util::build_error_result_from_errno(
                                Errno::ENOTEMPTY,
                                format!(
                                    "remove_node_helper() cannot remove \
                                        the non-empty directory name={:?} of ino={} \
                                        under the parent directory of ino={}",
                                    node_name, node_ino, parent,
                                ),
                            );
                        }
                    }

                    let child_inode = cache.get(&node_ino).unwrap_or_else(|| {
                        panic!(
                            "remove_node_helper() found fs is inconsistent, \
                                i-node name={:?} of ino={} found under the parent of ino={}, \
                                but no i-node found for this node",
                            node_name, node_ino, parent
                        )
                    });

                    if let SFlag::S_IFREG = node_type {
                        self.data_cache
                            .remove_file_cache(child_inode.full_path().as_bytes());
                    }

                    debug_assert_eq!(node_ino, child_inode.get_ino());
                    debug_assert_eq!(node_name, child_inode.get_name());
                    debug_assert_eq!(parent, child_inode.get_parent_ino());
                    debug_assert_eq!(node_type, child_inode.get_type());
                    debug_assert_eq!(node_type, child_inode.get_attr().kind);
                }
            }
        }
        {
            // all checks passed, ready to remove,
            // when deferred deletion, remove entry from directory first
            self.may_deferred_delete_node_helper(node_ino)
                .await
                .context(format!(
                    "remove_node_helper() failed to maybe deferred delete child i-node of ino={}, \
                        name={:?} and type={:?} under parent ino={}",
                    node_ino, node_name, node_type, parent,
                ))?;
            // reply.ok().await?;
            debug!(
                "remove_node_helper() successfully removed child i-node of ino={}, \
                    name={:?} and type={:?} under parent ino={}",
                node_ino, node_name, node_type, parent,
            );
            Ok(())
        }
    }

    /// Helper function to lookup
    async fn lookup_helper(
        &self,
        parent: INum,
        child_name: &str,
    ) -> anyhow::Result<(Duration, FuseAttr, u64)> {
        let pre_check_res = self.lookup_pre_check(parent, child_name).await;
        let (ino, child_type) = match pre_check_res {
            Ok((ino, child_type)) => (ino, child_type),
            Err(e) => {
                debug!(
                    "lookup() failed to pre-check, the error is: {}",
                    common::util::format_anyhow_error(&e),
                );
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
                "lookup_helper() cache missed when searching parent ino={}
                    and i-node of ino={} and name={:?}",
                parent, ino, child_name,
            );
            let mut cache = self.cache.write().await;
            let parent_node = cache.get_mut(&parent).unwrap_or_else(|| {
                panic!(
                    "lookup_helper() found fs is inconsistent, \
                        parent i-node of ino={} should be in cache",
                    parent,
                );
            });
            let parent_name = parent_node.get_name().to_owned();
            let mut child_node = match child_type {
                SFlag::S_IFDIR => parent_node
                    .open_child_dir(child_name)
                    .await
                    .context(format!(
                        "lookup_helper() failed to open sub-directory name={:?} \
                            under parent directory of ino={} and name={:?}",
                        child_name, parent, parent_name,
                    ))?,
                SFlag::S_IFREG => {
                    let oflags = OFlag::O_RDWR;
                    parent_node
                        .open_child_file(child_name, oflags, self.data_cache.clone())
                        .await
                        .context(format!(
                            "lookup_helper() failed to open child file name={:?} with flags={:?} \
                                under parent directory of ino={} and name={:?}",
                            child_name, oflags, parent, parent_name,
                        ))?
                }
                SFlag::S_IFLNK => {
                    parent_node
                        .load_child_symlink(child_name)
                        .await
                        .context(format!(
                            "lookup_helper() failed to read child symlink name={:?} \
                                under parent directory of ino={} and name={:?}",
                            child_name, parent, parent_name,
                        ))?
                }
                _ => panic!(
                    "lookup_helper() found unsupported file type={:?}",
                    child_type,
                ),
            };

            child_node.set_ino(ino);
            let child_ino = ino;
            let attr = child_node.lookup_attr();
            let full_path = child_node.full_path().to_owned();
            cache.insert(child_ino, child_node);

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
    async fn rename_exchange_helper(&self, param: RenameParam) -> anyhow::Result<()> {
        let old_parent = param.old_parent;
        let old_name = param.old_name.as_str();
        let new_parent = param.new_parent;
        let new_name = param.new_name;
        let flags = param.flags;
        debug!(
            "rename(old parent={}, old name={:?}, new parent={}, new name={:?})",
            old_parent, old_name, new_parent, new_name,
        );
        let no_replace = flags == 1; // RENAME_NOREPLACE

        let pre_check_res = self
            .rename_pre_check(old_parent, old_name, new_parent, &new_name, no_replace)
            .await;
        let (_, _, _, new_entry_ino) = match pre_check_res {
            Ok((old_parent_fd, old_entry_ino, new_parent_fd, new_entry_ino)) => {
                (old_parent_fd, old_entry_ino, new_parent_fd, new_entry_ino)
            }
            Err(e) => {
                debug!(
                    "rename() pre-check failed, the error is: {}",
                    common::util::format_anyhow_error(&e)
                );
                return Err(e);
            }
        };
        let new_entry_ino = new_entry_ino.unwrap();

        let rename_in_cache_res = self
            .rename_in_cache_helper(old_parent, old_name, new_parent, &new_name)
            .await;

        if let Some(replaced_entry) = rename_in_cache_res {
            debug_assert_eq!(
                new_entry_ino,
                replaced_entry.ino(),
                "rename_exchange_helper() replaced entry i-number not match"
            );
            let exchange_entry = DirEntry::new(
                new_entry_ino,
                old_name.to_string(),
                replaced_entry.entry_type(),
            );

            let mut cache = self.cache.write().await;
            let old_parent_node = cache.get_mut(&old_parent).unwrap_or_else(|| {
                panic!(
                    "impossible case when rename, the from parent i-node of ino={} should be in cache",
                    old_parent,
                )
            });
            let insert_res = old_parent_node
                .insert_entry_for_rename(exchange_entry)
                .await;
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
                    "impossible case when rename, the new entry i-node of ino={} should be in cache",
                    new_entry_ino,
                )
            });
            exchanged_node.set_parent_ino(old_parent);
            exchanged_node.set_name(old_name.clone());
            let exchanged_attr = exchanged_node
                .load_attribute()
                .await
                .context(format!(
                    "rename_exchange_helper() failed to load attribute of \
                        to i-node of ino={} and name={:?} under parent directory",
                    new_entry_ino, new_name,
                ))
                .unwrap_or_else(|e| {
                    panic!(
                        "rename_exchange_helper() failed to load attributed of to i-node of ino={} and name={:?}, \
                            the error is: {}",
                        exchanged_node.get_ino(), exchanged_node.get_name(),
                        common::util::format_anyhow_error(&e),
                    )
                });
            debug_assert_eq!(exchanged_attr.ino, exchanged_node.get_ino());
            debug_assert_eq!(exchanged_attr.ino, new_entry_ino);
            panic!("rename2 system call has not been supported in libc to exchange two nodes yet!");
        } else {
            panic!(
                "impossible case, the child i-node of name={:?} to be exchanged \
                    should be under to parent directory ino={}",
                new_name, new_parent,
            );
        }
    }

    /// Rename helper to move on disk, it may replace destination entry
    async fn rename_may_replace_helper(&self, param: RenameParam) -> anyhow::Result<()> {
        let old_parent = param.old_parent;
        let old_name = param.old_name;
        let new_parent = param.new_parent;
        let new_name = param.new_name;
        let flags = param.flags;
        debug!(
            "rename(old parent={}, old name={:?}, new parent={}, new name={:?})",
            old_parent, old_name, new_parent, new_name,
        );
        let no_replace = flags == 1; // RENAME_NOREPLACE

        let pre_check_res = self
            .rename_pre_check(old_parent, &old_name, new_parent, &new_name, no_replace)
            .await;
        let (_, old_entry_ino, _, new_entry_ino) = match pre_check_res {
            Ok((old_parent_fd, old_entry_ino, new_parent_fd, new_entry_ino)) => {
                (old_parent_fd, old_entry_ino, new_parent_fd, new_entry_ino)
            }
            Err(e) => {
                debug!(
                    "rename() pre-check failed, the error is: {}",
                    common::util::format_anyhow_error(&e)
                );
                return Err(e);
            }
        };

        // Just replace new entry, may deferred delete
        if let Some(new_ino) = new_entry_ino {
            self.may_deferred_delete_node_helper(new_ino)
                .await
                .context(format!(
                    "rename_may_replace_helper() failed to \
                        maybe deferred delete the replaced i-node ino={}",
                    new_ino,
                ))?;
        }

        {
            let mut cache = self.cache.write().await;
            let moved_node = cache.get_mut(&old_entry_ino).unwrap_or_else(|| {
                panic!(
                "impossible case when rename, the from entry i-node of ino={} should be in cache",
                old_entry_ino,
            )
            });
            moved_node.set_parent_ino(new_parent);
            moved_node.set_name(&new_name);
            debug!(
                "rename_may_replace_helper() successfully moved the from i-node \
                of ino={} and name={:?} under from parent ino={} to \
                the to i-node of ino={} and name={:?} under to parent ino={}",
                old_entry_ino, old_name, old_parent, old_entry_ino, new_name, new_parent,
            );
        }

        let rename_replace_res = self
            .rename_in_cache_helper(old_parent, &old_name, new_parent, &new_name)
            .await;
        debug_assert!(
            rename_replace_res.is_none(),
            "may_deferred_delete_node_helper() should already have \
                deleted the target i-node to be replaced",
        );
        Ok(())
    }

    /// Helper function of fsync
    async fn fsync_helper(
        &self,
        ino: u64,
        fh: u64,
        _datasync: bool,
        // reply: ReplyEmpty,
    ) -> anyhow::Result<()> {
        let mut cache = self.cache.write().await;
        let inode = cache.get_mut(&ino).unwrap_or_else(|| {
            panic!(
                "release() found fs is inconsistent, \
                    the i-node of ino={} should be in cache",
                ino,
            );
        });

        inode.flush(ino, fh).await;

        Ok(())
    }
}

impl<S: S3BackEnd + Send + Sync + 'static> S3MetaData<S> {
    pub(crate) fn cur_inum(&self) -> u32 {
        self.cur_inum.load(Ordering::Relaxed)
    }

    /// The pre-check before create node
    #[allow(single_use_lifetimes)]
    async fn create_node_pre_check<'a, 'b>(
        &self,
        parent: INum,
        node_name: &str,
        cache: &'b mut RwLockWriteGuard<'a, BTreeMap<INum, <S3MetaData<S> as MetaData>::N>>,
    ) -> anyhow::Result<&'b mut <S3MetaData<S> as MetaData>::N> {
        let parent_node = cache.get_mut(&parent).unwrap_or_else(|| {
            panic!(
                "create_node_pre_check() found fs is inconsistent, \
                    parent of ino={} should be in cache before create it new child",
                parent,
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

    /// Helper function to delete or deferred delete node
    async fn may_deferred_delete_node_helper(&self, ino: INum) -> anyhow::Result<()> {
        let parent_ino: INum;
        let node_name: String;
        let mut deferred_deletion = false;
        {
            // pre-check whether deferred delete or not
            let cache = self.cache.read().await;
            let node = cache.get(&ino).unwrap_or_else(|| {
                panic!(
                    "may_deferred_delete_node_helper() failed to \
                        find the i-node of ino={} to remove",
                    ino,
                );
            });
            parent_ino = node.get_parent_ino();
            node_name = node.get_name().to_string();

            debug_assert!(node.get_lookup_count() >= 0); // lookup count cannot be negative
            if node.get_lookup_count() > 0 {
                // TODO: support thread-safe to avoid race condition
                deferred_deletion = true;
            }
        }
        {
            // remove entry from parent i-node
            let mut cache = self.cache.write().await;
            let parent_node = cache.get_mut(&parent_ino).unwrap_or_else(|| {
                panic!(
                    "may_deferred_delete_node_helper() failed to \
                        find the parent of ino={} for i-node of ino={}",
                    parent_ino, ino,
                );
            });
            let deleted_entry = parent_node.unlink_entry(&node_name).await.context(format!(
                "may_deferred_delete_node_helper() failed to remove entry name={:?} \
                            and ino={} from parent directory ino={}",
                node_name, ino, parent_ino,
            ))?;
            debug_assert_eq!(node_name, deleted_entry.entry_name());
            debug_assert_eq!(deleted_entry.ino(), ino);
        }

        if deferred_deletion {
            // deferred deletion
            // TODO: support thread-safe
            let cache = self.cache.read().await;
            let node = cache.get(&ino).unwrap_or_else(|| {
                panic!(
                    "impossible case, may_deferred_delete_node_helper() \
                        should already find the i-node of ino={} to remove",
                    ino,
                );
            });
            {
                let mut trash = self.trash.write().await;
                let insert_result = trash.insert(ino); // check thread-safe in case of deferred deletion race
                debug_assert!(
                    insert_result,
                    "failed to insert i-node of ino={} into trash for deferred deletion",
                    ino,
                );
            }
            debug!(
                "may_deferred_delete_node_helper() defered removed \
                    the i-node name={:?} of ino={} under parent ino={}, \
                    open count={}, lookup count={}",
                node.get_name(),
                ino,
                parent_ino,
                node.get_open_count(),
                node.get_lookup_count(),
            );
        } else {
            // immediate deletion
            let mut cache = self.cache.write().await;
            let inode = cache.remove(&ino).unwrap_or_else(|| {
                panic!(
                    "impossible case, may_deferred_delete_node_helper() \
                    should remove the i-node of ino={} immediately",
                    ino,
                );
            }); // TODO: support thread-safe
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
    async fn lookup_pre_check(&self, parent: INum, name: &str) -> anyhow::Result<(INum, SFlag)> {
        // lookup child ino and type first
        let cache = self.cache.read().await;
        let parent_node = cache.get(&parent).unwrap_or_else(|| {
            panic!(
                "lookup_helper() found fs is inconsistent, \
                        the parent i-node of ino={} should be in cache",
                parent,
            );
        });
        if let Some(child_entry) = parent_node.get_entry(name) {
            let ino = child_entry.ino();
            let child_type = child_entry.entry_type();
            Ok((ino, child_type))
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
    ) -> anyhow::Result<(RawFd, INum, RawFd, Option<INum>)> {
        let cache = self.cache.read().await;
        let old_parent_node = cache.get(&old_parent).unwrap_or_else(|| {
            panic!(
                "rename() found fs is inconsistent, \
                    the parent i-node of ino={} should be in cache",
                old_parent,
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
                if cache.get(&old_entry.ino()).is_none() {
                    panic!(
                        "rename() found fs is inconsistent, the i-node of ino={} and name={:?} \
                            under parent directory of ino={} and name={:?} to rename should be in cache",
                        old_entry.ino(), old_name, old_parent, old_parent_node.get_name(),
                    );
                    // return;
                }
                old_entry.ino()
            }
        };

        let new_parent_node = cache.get(&new_parent).unwrap_or_else(|| {
            panic!(
                "rename() found fs is inconsistent, \
                    the new parent i-node of ino={} should be in cache",
                new_parent,
            );
        });
        let new_parent_fd = new_parent_node.get_fd();
        let new_entry_ino = if let Some(new_entry) = new_parent_node.get_entry(new_name) {
            debug_assert_eq!(&new_name, &new_entry.entry_name());
            let new_ino = new_entry.ino();
            if cache.get(&new_ino).is_none() {
                panic!(
                    "rename() found fs is inconsistent, the i-node of ino={} and name={:?} \
                        under parent directory of ino={} and name={:?} to replace should be in cache",
                    new_ino, new_name, new_parent, new_parent_node.get_name(),
                );
            }
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
        let entry_to_move = {
            // TODO: support thread-safe
            let mut cache = self.cache.write().await;
            let old_parent_node = cache.get_mut(&old_parent).unwrap_or_else(|| {
                panic!(
                    "impossible case when rename, the from parent i-node of ino={} should be in cache",
                    old_parent,
                )
            });
            match old_parent_node.remove_entry_for_rename(old_name).await {
                None => panic!(
                    "impossible case when rename, the from entry of name={:?} \
                        should be under from directory ino={} and name={:?}",
                    old_name,
                    old_parent,
                    old_parent_node.get_name(),
                ),
                Some(old_entry) => DirEntry::new(
                    old_entry.ino(),
                    new_name.to_string(),
                    old_entry.entry_type(),
                ),
            }
        };

        s3_node::rename_fullpath_recursive(entry_to_move.ino(), new_parent, &self.cache).await;

        {
            let mut cache = self.cache.write().await;
            let new_parent_node = cache.get_mut(&new_parent).unwrap_or_else(|| {
                panic!(
                    "impossible case when rename, the to parent i-node of ino={} should be in cache",
                    new_parent
                )
            });
            new_parent_node.insert_entry_for_rename(entry_to_move).await
        }
    }
}
