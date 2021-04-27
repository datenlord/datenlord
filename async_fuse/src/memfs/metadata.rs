use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Context;
use log::debug;
use nix::errno::Errno;
use nix::fcntl::OFlag;
use nix::sys::{
    stat::{self, SFlag},
    time::TimeSpec,
};
use nix::unistd;
use smol::lock::{RwLock, RwLockWriteGuard};
use utilities::Cast;

use crate::fuse::fuse_reply::AsIoVec;
use crate::fuse::protocol::{FuseAttr, INum};
use crate::util;

use super::cache::{GlobalCache, IoMemBlock};
use super::dir::DirEntry;
use super::fs_util::{self, FileAttr};
use super::node::{self, Node};
use super::{RenameHelperParam, SetAttrParam};

/// The time-to-live seconds of FUSE attributes
const MY_TTL_SEC: u64 = 3600; // TODO: should be a long value, say 1 hour
/// The generation ID of FUSE attributes
const MY_GENERATION: u64 = 1; // TODO: find a proper way to set generation

/// File system in-memory meta-data
#[derive(Debug)]
pub(crate) struct MetaData {
    /// The root path and the mount point of the FUSE filesystem
    pub(crate) root_path: PathBuf,
    /// The cache to hold opened directories and files
    pub(crate) cache: RwLock<BTreeMap<INum, Node>>,
    /// The trash to hold deferred deleted directories and files
    pub(crate) trash: BTreeSet<INum>,
    /// Global data cache
    pub(crate) data_cache: Arc<GlobalCache>,
}

impl MetaData {
    // FUSE operation helper functions

    /// The pre-check before create node
    #[allow(single_use_lifetimes)]
    pub(crate) async fn create_node_pre_check<'a, 'b>(
        &self,
        parent: INum,
        node_name: &OsStr,
        cache: &'b mut RwLockWriteGuard<'a, BTreeMap<INum, Node>>,
    ) -> anyhow::Result<&'b mut Node> {
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

    /// Helper function to create node
    pub(crate) async fn create_node_helper(
        &mut self,
        parent: INum,
        node_name: OsString,
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
        cache.insert(new_ino, new_node);

        let ttl = Duration::new(MY_TTL_SEC, 0);
        let fuse_attr = fs_util::convert_to_fuse_attr(new_node_attr);
        debug!(
            "create_node_helper() successfully created the new child name={:?} \
                of ino={} and type={:?} under parent ino={} and name={:?}",
            node_name_clone, new_ino, node_type, parent, parent_name,
        );
        Ok((ttl, fuse_attr, MY_GENERATION))
    }

    /// Helper function to delete or deferred delete node
    pub(crate) async fn may_deferred_delete_node_helper(
        &mut self,
        ino: INum,
    ) -> anyhow::Result<()> {
        let parent_ino: INum;
        let node_name: OsString;
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
            node_name = node.get_name().into();

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
            let node_name_clone = node_name.clone();
            let deleted_entry =
                parent_node
                    .unlink_entry(node_name_clone)
                    .await
                    .context(format!(
                        "may_deferred_delete_node_helper() failed to remove entry name={:?} \
                            and ino={} from parent directory ino={}",
                        node_name, ino, parent_ino,
                    ))?;
            debug_assert_eq!(&node_name, deleted_entry.entry_name());
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
            let insert_result = self.trash.insert(ino); // check thread-safe in case of deferred deletion race
            debug_assert!(
                insert_result,
                "failed to insert i-node of ino={} into trash for deferred deletion",
                ino,
            );
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

    /// Helper function to remove node
    pub(crate) async fn remove_node_helper(
        &mut self,
        parent: INum,
        node_name: OsString,
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

    /// Lookup helper function to pre-check
    pub(crate) async fn lookup_pre_check(
        &self,
        parent: INum,
        name: &OsStr,
    ) -> anyhow::Result<(INum, SFlag)> {
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

    /// Helper function to lookup
    pub(crate) async fn lookup_helper(
        &mut self,
        parent: INum,
        ino: INum,
        name: &OsStr,
        child_type: SFlag,
    ) -> anyhow::Result<(Duration, FuseAttr, u64)> {
        let child_name = OsString::from(name);
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
            let child_node = match child_type {
                SFlag::S_IFDIR => parent_node
                    .open_child_dir(child_name)
                    .await
                    .context(format!(
                        "lookup_helper() failed to open sub-directory name={:?} \
                            under parent directory of ino={} and name={:?}",
                        name, parent, parent_name,
                    ))?,
                SFlag::S_IFREG => {
                    let oflags = OFlag::O_RDWR;
                    parent_node
                        .open_child_file(child_name, oflags, self.data_cache.clone())
                        .await
                        .context(format!(
                            "lookup_helper() failed to open child file name={:?} with flags={:?} \
                                under parent directory of ino={} and name={:?}",
                            name, oflags, parent, parent_name,
                        ))?
                }
                SFlag::S_IFLNK => {
                    parent_node
                        .load_child_symlink(child_name)
                        .await
                        .context(format!(
                            "lookup_helper() failed to read child symlink name={:?} \
                                under parent directory of ino={} and name={:?}",
                            name, parent, parent_name,
                        ))?
                }
                _ => panic!(
                    "lookup_helper() found unsupported file type={:?}",
                    child_type,
                ),
            };
            let child_ino = child_node.get_ino();
            let attr = child_node.lookup_attr();
            cache.insert(child_ino, child_node);
            let fuse_attr = fs_util::convert_to_fuse_attr(attr);
            debug!(
                "lookup_helper() successfully found the i-node of ino={} and name={:?} \
                under parent of ino={} and name={:?}",
                child_ino, name, parent, parent_name,
            );
            Ok((ttl, fuse_attr, MY_GENERATION))
        }
    }

    /// Rename helper function to pre-check
    pub(crate) async fn rename_pre_check(
        &self,
        old_parent: INum,
        old_name: &OsStr,
        new_parent: INum,
        new_name: &OsStr,
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
                // return;
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

    /// Rename helper to exchange on disk
    pub(crate) async fn rename_exchange_helper(
        &mut self,
        param: RenameHelperParam,
        new_entry_ino: INum,
    ) -> anyhow::Result<()> {
        let old_parent = param.old_parent;
        // let old_parent_fd = param.old_parent_fd;
        let old_name = param.old_name;
        let new_parent = param.new_parent;
        // let new_parent_fd = param.new_parent_fd;
        let new_name = param.new_name;

        let rename_in_cache_res = self
            .rename_in_cache_helper(old_parent, &old_name, new_parent, &new_name)
            .await;
        if let Some(replaced_entry) = rename_in_cache_res {
            debug_assert_eq!(
                new_entry_ino,
                replaced_entry.ino(),
                "rename_exchange_helper() replaced entry i-number not match"
            );
            let exchange_entry =
                DirEntry::new(new_entry_ino, old_name.clone(), replaced_entry.entry_type());

            // TODO: support thread-safe
            let mut cache = self.cache.write().await;
            let old_parent_node = cache.get_mut(&old_parent).unwrap_or_else(|| {
                panic!(
                    "impossible case when rename, the from parent i-node of ino={} should be in cache",
                    old_parent,
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
    pub(crate) async fn rename_may_replace_helper(
        &mut self,
        param: RenameHelperParam,
        new_entry_ino: Option<INum>,
    ) -> anyhow::Result<()> {
        let old_parent = param.old_parent;
        let old_parent_fd = param.old_parent_fd;
        let old_name = param.old_name;
        let old_entry_ino = param.old_entry_ino;
        let new_parent = param.new_parent;
        let new_parent_fd = param.new_parent_fd;
        let new_name = param.new_name;

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
        let old_name_clone = old_name.clone();
        let new_name_clone = new_name.clone();
        // Rename on disk
        smol::unblock(move || {
            nix::fcntl::renameat(
                Some(old_parent_fd),
                Path::new(&old_name_clone),
                Some(new_parent_fd),
                Path::new(&new_name_clone),
            )
        })
        .await
        .context(format!(
            "rename_may_replace_helper() failed to move the from i-node name={:?} under \
                from parent ino={} to the to i-node name={:?} under new parent ino={}",
            old_name, old_parent, new_name, new_parent,
        ))?;

        {
            let mut cache = self.cache.write().await;
            let moved_node = cache.get_mut(&old_entry_ino).unwrap_or_else(|| {
                panic!(
                "impossible case when rename, the from entry i-node of ino={} should be in cache",
                old_entry_ino,
            )
            });
            moved_node.set_parent_ino(new_parent);
            moved_node.set_name(new_name.to_os_string());
            let moved_attr = moved_node
                .load_attribute()
                .await
                .context(format!(
                    "rename_may_replace_helper() failed to \
                    load attribute of old entry i-node of ino={}",
                    old_entry_ino,
                ))
                .unwrap_or_else(|e| {
                    panic!(
                        "rename() failed, the error is: {}",
                        common::util::format_anyhow_error(&e)
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

    /// Rename in cache helper
    pub(crate) async fn rename_in_cache_helper(
        &mut self,
        old_parent: INum,
        old_name: &OsStr,
        new_parent: INum,
        new_name: &OsStr,
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
            match old_parent_node.remove_entry_for_rename(old_name) {
                None => panic!(
                    "impossible case when rename, the from entry of name={:?} \
                        should be under from directory ino={} and name={:?}",
                    old_name,
                    old_parent,
                    old_parent_node.get_name(),
                ),
                Some(old_entry) => DirEntry::new(
                    old_entry.ino(),
                    new_name.to_os_string(),
                    old_entry.entry_type(),
                ),
            }
        };
        node::rename_fullpath_recursive(entry_to_move.ino(), new_parent, &self.cache).await;
        {
            // TODO: support thread-safe
            let mut cache = self.cache.write().await;
            let new_parent_node = cache.get_mut(&new_parent).unwrap_or_else(|| {
                panic!(
                    "impossible case when rename, the to parent i-node of ino={} should be in cache",
                    new_parent
                )
            });
            new_parent_node.insert_entry_for_rename(entry_to_move)
        }
    }

    /// Helper function of fsync
    pub(crate) async fn fsync_helper(
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
                smol::unblock(move || unistd::fdatasync(fh.cast()))
                    .await
                    .context(format!(
                        "fsync_helper() failed to flush the i-node of ino={}",
                        ino
                    ))?;
            } else {
                smol::unblock(move || unistd::fsync(fh.cast()))
                    .await
                    .context(format!(
                        "fsync_helper() failed to flush the i-node of ino={}",
                        ino
                    ))?;
            }
        }
        #[cfg(target_os = "macos")]
        {
            smol::unblock(|| unistd::fsync(fh.cast()))
                .await
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

    /// Read helper
    pub(crate) fn read_helper(
        content: Vec<IoMemBlock>,
        size: usize,
    ) -> anyhow::Result<Vec<IoMemBlock>> {
        if content.iter().filter(|c| !c.can_convert()).count() > 0 {
            return util::build_error_result_from_errno(
                Errno::EINVAL,
                "The content is out of scope".to_string(),
            );
        }
        let content_total_len: usize = content.iter().map(|s| s.len()).sum();
        debug!("read {} data, expected size {}", content_total_len, size);
        Ok(content)
    }

    /// Set attribute helper function
    #[allow(clippy::too_many_lines)]
    pub(crate) async fn setattr_helper(
        fd: RawFd,
        param: SetAttrParam,
        mut attr: FileAttr,
    ) -> anyhow::Result<(bool, FileAttr)> {
        // TODO: change macOS times and flags
        // #[cfg(target_os = "macos")]
        // let crtime = param.crtime;
        // #[cfg(target_os = "macos")]
        // let chgtime = param.chgtime;
        // #[cfg(target_os = "macos")]
        // let bkuptime = param.bkuptime;
        // #[cfg(target_os = "macos")]
        // let flags = param.flags;
        let st_now = SystemTime::now();
        let mut attr_changed = false;
        let mut mtime_ctime_changed = false;
        if let Some(mode_bits) = param.mode {
            let nix_mode = fs_util::parse_mode(mode_bits);
            debug!(
                "setattr_helper() successfully parsed mode={:?} from bits={:#o}",
                nix_mode, mode_bits,
            );
            smol::unblock(move || stat::fchmod(fd, nix_mode))
                .await
                .context(format!(
                    "setattr_helper() failed to chmod with mode={}",
                    mode_bits,
                ))?;
            attr.perm = fs_util::parse_mode_bits(mode_bits);
            debug!(
                "setattr_helper() set permission={:#o}={} from input bits={:#o}={}",
                attr.perm, attr.perm, mode_bits, mode_bits,
            );
            let kind = fs_util::parse_sflag(mode_bits);
            debug_assert_eq!(kind, attr.kind);

            // Change mode also need to change ctime
            attr.ctime = st_now;
            attr_changed = true;
        }
        if param.u_id.is_some() || param.g_id.is_some() {
            let nix_user_id = param.u_id.map(unistd::Uid::from_raw);
            let nix_group_id = param.g_id.map(unistd::Gid::from_raw);
            smol::unblock(move || unistd::fchown(fd, nix_user_id, nix_group_id))
                .await
                .context(format!(
                    "setattr_helper() failed to set uid={:?} and gid={:?}",
                    nix_user_id, nix_group_id,
                ))?;
            if let Some(raw_uid) = param.u_id {
                attr.uid = raw_uid;
            }
            if let Some(raw_gid) = param.g_id {
                attr.gid = raw_gid;
            }
            // Change uid or gid also need to change ctime
            attr.ctime = st_now;
            attr_changed = true;
        }
        if let Some(file_size) = param.size {
            smol::unblock(move || unistd::ftruncate(fd, file_size.cast()))
                .await
                .context(format!(
                    "setattr_helper() failed to truncate file size to {}",
                    file_size
                ))?;
            attr.size = file_size;
            attr.mtime = st_now;
            attr.ctime = st_now;
            mtime_ctime_changed = true;
            attr_changed = true;
        }
        if param.a_time.is_some() || param.m_time.is_some() {
            if mtime_ctime_changed {
                panic!("setattr_helper() cannot change atime and mtime explicitly in the mean while with truncate");
            } else {
                let nix_access_time = param.a_time.map_or(
                    TimeSpec::from(libc::timespec {
                        tv_sec: 0,
                        tv_nsec: libc::UTIME_OMIT,
                    }),
                    |st_atime| {
                        let (seconds, nanoseconds) = fs_util::time_from_system_time(&st_atime);
                        TimeSpec::from(libc::timespec {
                            tv_sec: seconds.cast(),
                            tv_nsec: nanoseconds.cast(),
                        })
                    },
                );
                let nix_modify_time = param.a_time.map_or(
                    TimeSpec::from(libc::timespec {
                        tv_sec: 0,
                        tv_nsec: libc::UTIME_OMIT,
                    }),
                    |st_mtime| {
                        let (seconds, nanoseconds) = fs_util::time_from_system_time(&st_mtime);
                        TimeSpec::from(libc::timespec {
                            tv_sec: seconds.cast(),
                            tv_nsec: nanoseconds.cast(),
                        })
                    },
                );
                smol::unblock(move || stat::futimens(fd, &nix_access_time, &nix_modify_time))
                    .await
                    .context(format!(
                        "setattr_helper() failed to update atime={:?} or mtime={:?}",
                        param.a_time, param.m_time
                    ))?;
                if let Some(st_atime) = param.a_time {
                    attr.atime = st_atime;
                    // Change atime do not need to change ctime
                }
                if let Some(st_mtime) = param.a_time {
                    attr.mtime = st_mtime;
                    // Change mtime also need to change ctime
                    attr.ctime = st_now;
                }
                attr_changed = true;
            }
        }
        // TODO: change lock owner
        // #[cfg(feature = "abi-7-9")]
        // let lock_owner = param.lock_owner;
        #[cfg(feature = "abi-7-23")]
        if let Some(c_time) = param.c_time {
            attr.ctime = c_time;
            // TODO: how to change ctime directly on ext4?
        }
        Ok((attr_changed, attr))
    }

    // /// Return ENOSYS for not implemented operations
    // pub pub(crate) async fn not_implement_helper(
    //     &mut self,
    //     req: &Request<'_>,
    //     fd: RawFd,
    // ) -> nix::Result<usize> {
    //     let reply = ReplyEmpty::new(req.unique(), fd);
    //     reply.error_code(Errno::ENOSYS).await
    // }
}
