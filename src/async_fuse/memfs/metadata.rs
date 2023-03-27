use super::cache::GlobalCache;
use super::dir::DirEntry;
use super::dist::server::CacheServer;
use super::fs_util::{self, FileAttr};
use super::node::{self, DefaultNode, Node};
use super::RenameParam;
use crate::async_fuse::fuse::file_system::FsAsyncResultSender;
use crate::async_fuse::fuse::protocol::{FuseAttr, INum, FUSE_ROOT_ID};
use crate::async_fuse::util;
use crate::common::etcd_delegate::EtcdDelegate;
use anyhow::Context;
use async_trait::async_trait;
use clippy_utilities::Cast;
use log::debug;
use nix::errno::Errno;
use nix::fcntl::OFlag;
use nix::sys::stat::SFlag;
use nix::unistd;
use parking_lot::RwLock as SyncRwLock; // conflict with tokio RwLock
use std::collections::BTreeMap;
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock, RwLockWriteGuard};
use tokio::task::JoinHandle;

/// The time-to-live seconds of FUSE attributes
const MY_TTL_SEC: u64 = 3600; // TODO: should be a long value, say 1 hour
/// The generation ID of FUSE attributes
const MY_GENERATION: u64 = 1; // TODO: find a proper way to set generation

/// MetaData of fs
#[async_trait]
pub trait MetaData {
    /// Node type
    type N: Node + Send + Sync + 'static;

    #[allow(clippy::too_many_arguments)]
    /// Create `MetaData`
    async fn new(
        root_path: &str,
        capacity: usize,
        ip: &str,
        port: &str,
        etcd_client: EtcdDelegate,
        node_id: &str,
        volume_info: &str,
        fs_async_sender: FsAsyncResultSender,
    ) -> (Arc<Self>, Option<CacheServer>, Vec<JoinHandle<()>>);

    /// Helper function to create node
    async fn create_node_helper(
        &self,
        parent: INum,
        node_name: &str,
        mode: u32,
        node_type: SFlag,
        target_path: Option<&Path>,
    ) -> anyhow::Result<(Duration, FuseAttr, u64)>;

    /// Helper function to remove node
    async fn remove_node_helper(
        &self,
        parent: INum,
        node_name: &str,
        node_type: SFlag,
    ) -> anyhow::Result<()>;

    /// Helper function to lookup
    async fn lookup_helper(
        &self,
        parent: INum,
        name: &str,
    ) -> anyhow::Result<(Duration, FuseAttr, u64)>;

    /// Rename helper to exchange on disk
    async fn rename_exchange_helper(&self, param: RenameParam) -> anyhow::Result<()>;

    /// Rename helper to move on disk, it may replace destination entry
    async fn rename_may_replace_helper(&self, param: RenameParam) -> anyhow::Result<()>;

    /// Helper function of fsync
    async fn fsync_helper(&self, ino: u64, fh: u64, datasync: bool) -> anyhow::Result<()>;

    // TODO: Should hide this implementation detail
    /// Get metadata cache
    fn cache(&self) -> &RwLock<BTreeMap<INum, Self::N>>;

    /// Try to delete node that is marked as deferred deletion
    async fn try_delete_node(&self, ino: INum) -> bool;

    /// Helper function to write data
    async fn write_helper(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        data: Vec<u8>,
        flags: u32,
    ) -> anyhow::Result<usize>;

    /// Set fuse fd into `MetaData`
    async fn set_fuse_fd(&self, fuse_fd: RawFd);

    /// Stop all async tasks
    fn stop_all_async_tasks(&self);
}

/// File system in-memory meta-data
#[derive(Debug)]
#[allow(dead_code)]
pub struct DefaultMetaData {
    /// The root path and the mount point of the FUSE filesystem
    root_path: PathBuf,
    /// The cache to hold opened directories and files
    pub(crate) cache: RwLock<BTreeMap<INum, DefaultNode>>,
    /// Global data cache
    data_cache: Arc<GlobalCache>,
    /// Fuse fd
    fuse_fd: Mutex<RawFd>,
    /// Send async result to session
    fs_async_sender: FsAsyncResultSender,
}

#[async_trait]
impl MetaData for DefaultMetaData {
    type N = DefaultNode;

    async fn new(
        root_path: &str,
        capacity: usize,
        _: &str,
        _: &str,
        _: EtcdDelegate,
        _: &str,
        _: &str,
        fs_async_sender: FsAsyncResultSender,
    ) -> (Arc<Self>, Option<CacheServer>, Vec<JoinHandle<()>>) {
        let root_path = Path::new(root_path)
            .canonicalize()
            .with_context(|| format!("failed to canonicalize the mount path={root_path:?}"))
            .unwrap_or_else(|e| panic!("{}", e));

        let root_path = root_path
            .as_os_str()
            .to_str()
            .unwrap_or_else(|| panic!("failed to convert to utf8 string"));

        let meta = Arc::new(Self {
            root_path: root_path.into(),
            cache: RwLock::new(BTreeMap::new()),
            data_cache: Arc::new(GlobalCache::new_with_capacity(capacity)),
            fuse_fd: Mutex::new(-1_i32),
            fs_async_sender,
        });

        let root_inode =
            DefaultNode::open_root_node(FUSE_ROOT_ID, "/", root_path, Arc::clone(&meta))
                .await
                .context("failed to open FUSE root node")
                .unwrap_or_else(|e| {
                    panic!("{}", e);
                });
        meta.cache.write().await.insert(FUSE_ROOT_ID, root_inode);

        (meta, None, vec![])
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
        let inode = cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "try_delete_node() found fs is inconsistent, \
                    the i-node of ino={ino} is not in cache",
            );
        });

        if inode.get_open_count() == 0
            && inode.get_lookup_count() == 0
            && inode.is_deferred_deletion()
        {
            debug!(
                "try_delete_node() deleted i-node of ino={} and name={:?}",
                ino,
                inode.get_name(),
            );
            if let Some(inode) = cache.remove(&ino) {
                if let SFlag::S_IFREG = inode.get_type() {
                    self.data_cache
                        .remove_file_cache(inode.get_full_path().as_bytes())
                        .await;
                }
            }

            true
        } else {
            debug!(
                "try_delete_node() cannot deleted i-node of ino={} and name={:?},\
                     open_count={}, lookup_count={}, is_deferred_deletion={}",
                ino,
                inode.get_name(),
                inode.get_open_count(),
                inode.get_lookup_count(),
                inode.is_deferred_deletion(),
            );
            false
        }
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
        let parent_node = Self::create_node_pre_check(parent, node_name, &mut cache)
            .context("create_node_helper() failed to pre check")?;
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
                    .create_child_dir(0, node_name, m_flags)
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
                        0,
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
                        0,
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
        cache.insert(new_ino, new_node);

        let ttl = Duration::new(MY_TTL_SEC, 0);
        let fuse_attr = fs_util::convert_to_fuse_attr(new_node_attr);
        debug!(
            "create_node_helper() successfully created the new child name={:?} \
                of ino={} and type={:?} under parent ino={} and name={:?}",
            node_name, new_ino, node_type, parent, parent_name,
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
                        parent of ino={parent} should be in cache before remove its child",
                );
            });
            match parent_node.get_entry(node_name) {
                None => {
                    debug!(
                        "remove_node_helper() failed to find i-node name={:?} \
                            under parent of ino={}",
                        node_name, parent,
                    );
                    return util::build_error_result_from_errno(
                        Errno::ENOENT,
                        format!(
                            "remove_node_helper() failed to find i-node name={node_name:?} \
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
                                "remove_node_helper() found fs is inconsistent, \
                                    directory name={node_name:?} of ino={node_ino} \
                                    found under the parent of ino={parent}, \
                                    but no i-node found for this directory",
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
                                        the non-empty directory name={node_name:?} of ino={node_ino} \
                                        under the parent directory of ino={parent}",
                                ),
                            );
                        }
                    }

                    let child_inode = cache.get(&node_ino).unwrap_or_else(|| {
                        panic!(
                            "remove_node_helper() found fs is inconsistent, \
                                i-node name={node_name:?} of ino={node_ino} found under the parent of ino={parent}, \
                                but no i-node found for this node"
                        )
                    });
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
                    "remove_node_helper() failed to maybe deferred delete child i-node of ino={node_ino}, \
                        name={node_name:?} and type={node_type:?} under parent ino={parent}",
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
        let (ino, child_type, child_attr) = match pre_check_res {
            Ok((ino, child_type, child_attr)) => (ino, child_type, child_attr),
            Err(e) => {
                debug!(
                    "lookup() failed to pre-check, the error is: {}",
                    crate::common::util::format_anyhow_error(&e),
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
            let child_ino = child_node.get_ino();
            let attr = child_node.lookup_attr();
            cache.insert(child_ino, child_node);
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
                    crate::common::util::format_anyhow_error(&e)
                );
                return Err(e);
            }
        };
        let new_entry_ino = new_entry_ino.unwrap_or_else(|| panic!("new entry ino is None"));

        let rename_in_cache_res = self
            .rename_in_cache_helper(old_parent, old_name, new_parent, &new_name)
            .await;

        if let Some(replaced_entry) = rename_in_cache_res {
            debug_assert_eq!(
                new_entry_ino,
                replaced_entry.ino(),
                "rename_exchange_helper() replaced entry i-number not match"
            );

            //todo: check file attr logic carefully at here
            let exchange_entry = DirEntry::new(
                old_name.to_owned(),
                Arc::new(SyncRwLock::new(FileAttr {
                    ino: new_entry_ino,
                    ..*replaced_entry.file_attr_arc_ref().read()
                })),
            );

            // TODO: support thread-safe
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
        let (old_parent_fd, old_entry_ino, new_parent_fd, new_entry_ino) = match pre_check_res {
            Ok((old_parent_fd, old_entry_ino, new_parent_fd, new_entry_ino)) => {
                (old_parent_fd, old_entry_ino, new_parent_fd, new_entry_ino)
            }
            Err(e) => {
                debug!(
                    "rename() pre-check failed, the error is: {}",
                    crate::common::util::format_anyhow_error(&e)
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
                        maybe deferred delete the replaced i-node ino={new_ino}",
                ))?;
        }
        let old_name_clone = old_name.clone();
        let new_name_clone = new_name.clone();
        // Rename on disk
        tokio::task::spawn_blocking(move || {
            nix::fcntl::renameat(
                Some(old_parent_fd),
                Path::new(&old_name_clone),
                Some(new_parent_fd),
                Path::new(&new_name_clone),
            )
        })
        .await?
        .context(format!(
            "rename_may_replace_helper() failed to move the from i-node name={old_name:?} under \
                from parent ino={old_parent} to the to i-node name={new_name:?} under new parent ino={new_parent}",
        ))?;

        {
            let mut cache = self.cache.write().await;
            let moved_node = cache.get_mut(&old_entry_ino).unwrap_or_else(|| {
                panic!(
                "impossible case when rename, the from entry i-node of ino={old_entry_ino} should be in cache",
            )
            });
            moved_node.set_parent_ino(new_parent);
            moved_node.set_name(&new_name);
            let moved_attr = moved_node
                .load_attribute()
                .await
                .context(format!(
                    "rename_may_replace_helper() failed to \
                    load attribute of old entry i-node of ino={old_entry_ino}",
                ))
                .unwrap_or_else(|e| {
                    panic!(
                        "rename() failed, the error is: {}",
                        crate::common::util::format_anyhow_error(&e)
                    )
                });
            debug_assert_eq!(moved_attr.ino, moved_node.get_ino());
            debug_assert_eq!(moved_attr.ino, old_entry_ino);
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
        datasync: bool,
        // reply: ReplyEmpty,
    ) -> anyhow::Result<()> {
        // TODO: handle datasync
        #[cfg(target_os = "linux")]
        {
            // attributes are not allowed on if expressions
            if datasync {
                tokio::task::spawn_blocking(move || unistd::fdatasync(fh.cast()))
                    .await?
                    .context(format!(
                        "fsync_helper() failed to flush the i-node of ino={ino}"
                    ))?;
            } else {
                tokio::task::spawn_blocking(move || unistd::fsync(fh.cast()))
                    .await?
                    .context(format!(
                        "fsync_helper() failed to flush the i-node of ino={ino}"
                    ))?;
            }
        }
        #[cfg(target_os = "macos")]
        {
            tokio::task::spawn_blocking(|| unistd::fsync(fh.cast()))
                .await?
                .context(format!(
                    "fsync_helper() failed to flush the i-node of ino={}",
                    ino,
                ))?;
        }
        // reply.ok().await?;
        debug!(
            "fsync_helper() successfully sync the i-node of ino={}, fh={}, datasync={}",
            ino, fh, datasync,
        );
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
    ) -> anyhow::Result<usize> {
        let mut cache = self.cache().write().await;
        let inode = cache.get_mut(&ino).unwrap_or_else(|| {
            panic!(
                "write() found fs is inconsistent, \
                 the inode ino={ino} is not in cache"
            );
        });
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
        inode
            .write_file(fh, offset, data, o_flags, write_to_disk)
            .await
    }

    /// Stop all async tasks
    fn stop_all_async_tasks(&self) {}
}

impl DefaultMetaData {
    // FUSE operation helper functions

    /// The pre-check before create node
    #[allow(single_use_lifetimes)]
    fn create_node_pre_check<'b>(
        parent: INum,
        node_name: &str,
        cache: &'b mut RwLockWriteGuard<BTreeMap<INum, <Self as MetaData>::N>>,
    ) -> anyhow::Result<&'b mut <Self as MetaData>::N> {
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
    fn deferred_delete_pre_check(inode: &DefaultNode) -> (bool, INum, String) {
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
    async fn may_deferred_delete_node_helper(&self, ino: INum) -> anyhow::Result<()> {
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
    ) -> anyhow::Result<(INum, SFlag, Arc<SyncRwLock<FileAttr>>)> {
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
    ) -> anyhow::Result<(RawFd, INum, RawFd, Option<INum>)> {
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
                    new_ino, new_name, new_parent, new_parent_node.get_name());
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
        node::rename_fullpath_recursive(entry_to_move.ino(), new_parent, &mut cache);
        let new_parent_node = cache.get_mut(&new_parent).unwrap_or_else(|| {
            panic!(
                "impossible case when rename, the to parent i-node of ino={new_parent} should be in cache"
            )
        });
        new_parent_node.insert_entry_for_rename(entry_to_move)
    }
}
