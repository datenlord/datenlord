//! The implementation of user space file system

use anyhow::Context;
use log::{debug, warn};
use nix::fcntl::OFlag;
use nix::sys::{stat::SFlag, statvfs};
use nix::unistd;
use smol::blocking;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::os::unix::io::{FromRawFd, IntoRawFd, RawFd};
use std::path::Path;
use std::time::{Duration, SystemTime};
use utilities::{Cast, OverflowArithmetic};

#[cfg(target_os = "macos")]
use super::fuse_reply::ReplyXTimes;
use super::fuse_reply::{
    ReplyAttr, ReplyBMap, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyLock, ReplyOpen, ReplyStatFs, ReplyWrite, ReplyXAttr, StatFsParam,
};
use super::fuse_request::Request;
use super::protocol::{FuseAttr, INum, FUSE_ROOT_ID};

mod dir;
mod node;
pub mod util;
use dir::DirEntry;
use node::Node;

/// The time-to-live seconds of FUSE attributes
const MY_TTL_SEC: u64 = 3600; // TODO: should be a long value, say 1 hour
/// The generation ID of FUSE attributes
const MY_GENERATION: u64 = 1; // TODO: find a proper way to set generation

/// File system in-memory meta-data
#[derive(Debug)]
pub struct FileSystem {
    /// The cache to hold opened directories and files
    cache: BTreeMap<INum, Node>,
    /// The trash to hold deferred deleted directories and files
    trash: BTreeSet<INum>,
}

/// Set attribute parameters
pub struct SetAttrParam {
    /// FUSE set attribute bit mask
    pub valid: u32,
    /// File handler
    pub fh: Option<u64>,
    /// File mode
    pub mode: Option<u32>,
    /// User ID
    pub u_id: Option<u32>,
    /// Group ID
    pub g_id: Option<u32>,
    /// File size
    pub size: Option<u64>,
    /// Lock owner
    #[cfg(feature = "abi-7-9")]
    pub lock_owner: Option<u64>,
    /// Access time
    pub a_time: Option<SystemTime>,
    /// Content modified time
    pub m_time: Option<SystemTime>,
    /// Meta-data changed time seconds
    #[cfg(feature = "abi-7-23")]
    pub c_time: Option<SystemTime>,
    /// Creation time, macOS only
    #[cfg(target_os = "macos")]
    pub crtime: Option<SystemTime>,
    /// macOS only
    #[cfg(target_os = "macos")]
    pub chgtime: Option<SystemTime>,
    /// Backup time, macOS only
    #[cfg(target_os = "macos")]
    pub bkuptime: Option<SystemTime>,
    /// See chflags(2)
    #[cfg(target_os = "macos")]
    pub flags: Option<u32>,
}

/// Rename parameters
pub struct RenameParam {
    /// Old parent directory i-number
    pub old_parent: INum,
    /// Old name
    pub old_name: OsString,
    /// New parent directory i-number
    pub new_parent: INum,
    /// New name
    pub new_name: OsString,
    /// Rename flags
    pub flags: u32,
}

/// Rename helper parameters
struct RenameHelperParam {
    /// Old parent directory i-number
    old_parent: INum,
    /// Old parent directory fd
    old_parent_fd: RawFd,
    /// Old name
    old_name: OsString,
    /// Old entry i-number
    old_entry_ino: INum,
    /// New parent directory i-number
    new_parent: INum,
    /// New parent directory fd
    new_parent_fd: RawFd,
    /// New name
    new_name: OsString,
}

/// POSIX file lock parameters
#[derive(Debug)]
pub struct FileLockParam {
    /// File hander
    pub fh: u64,
    /// Lock owner
    pub lock_owner: u64,
    /// Start offset
    pub start: u64,
    /// End offset
    pub end: u64,
    /// Lock type
    pub typ: u32,
    /// The process ID of the lock
    pub pid: u32,
}

impl FileSystem {
    /// Create `FileSystem`
    pub async fn new(root_path: &Path) -> anyhow::Result<Self> {
        let root_inode = Node::open_root_node(FUSE_ROOT_ID, OsString::from("/"), root_path)
            .await
            .context("failed to open FUSE root node")?;
        let mut cache = BTreeMap::new();
        cache.insert(FUSE_ROOT_ID, root_inode);
        let trash = BTreeSet::new(); // for deferred deletion

        Ok(Self { cache, trash })
    }

    // FUSE operation helper functions

    /// Helper function to create node
    async fn create_node_helper(
        &mut self,
        parent: INum,
        node_name: OsString,
        mode: u32,
        node_type: SFlag,
        // reply: ReplyEntry,
    ) -> anyhow::Result<(Duration, FuseAttr, u64)> {
        // pre-check
        let parent_node = self.cache.get_mut(&parent).unwrap_or_else(|| {
            panic!(
                "create_node_helper() found fs is inconsistent, \
                    parent of ino={} should be in cache before create it new child",
                parent,
            );
        });

        if let Some(occupied) = parent_node.get_entry(&node_name) {
            debug!(
                "create_node_helper() found the directory of ino={} \
                    already exists a child with name={:?} and ino={}",
                parent,
                node_name,
                occupied.ino(),
            );
            return util::build_error_result_from_errno(
                libc::EEXIST,
                format!(
                    "create_node_helper() found the directory of ino={} \
                        already exists a child with name={:?} and ino={}",
                    parent,
                    node_name,
                    occupied.ino(),
                ),
            );
            // reply.error_code(libc::EEXIST).await?;
            // return Ok(());
        }
        // all checks are passed, ready to create new node
        let m_flags = util::parse_mode(mode);
        let new_ino: u64;
        let node_name_clone = node_name.clone();
        let new_node = match node_type {
            SFlag::S_IFDIR => {
                debug!(
                    "create_node_helper() about to create a directory with name={:?}, mode={:?}",
                    node_name, m_flags,
                );
                parent_node
                    .create_child_dir(node_name.clone(), m_flags)
                    .await
                    .context(format!(
                        "create_node_helper() failed to create directory with name={:?}, mode={:?}",
                        node_name, m_flags,
                    ))?
            }
            SFlag::S_IFREG => {
                let o_flags = OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_RDWR;
                debug!(
                    "helper_create_node() about to \
                        create a file with name={:?}, oflags={:?}, mode={:?}",
                    node_name, o_flags, m_flags,
                );
                parent_node
                    .create_child_file(node_name.clone(), o_flags, m_flags)
                    .await
                    .context(format!(
                        "create_node_helper() failed to create node with name={:?}, mode={:?}",
                        node_name, m_flags,
                    ))?
            }
            _ => panic!(
                "create_node_helper() found unsupported file type={:?}",
                node_type
            ),
        };
        new_ino = new_node.get_ino();
        let new_node_attr = new_node.get_attr();
        self.cache.insert(new_ino, new_node);

        let ttl = Duration::new(MY_TTL_SEC, 0);
        let fuse_attr = util::convert_to_fuse_attr(new_node_attr);
        // reply.entry(ttl, fuse_attr, MY_GENERATION).await?;
        debug!(
            "create_node_helper() successfully created the new child name={:?} \
                of ino={} and type={:?} under parent ino={}",
            node_name_clone, new_ino, node_type, parent,
        );
        Ok((ttl, fuse_attr, MY_GENERATION))
    }

    /// Helper function to delete or deferred delete node
    async fn may_deferred_delete_node_helper(&mut self, ino: INum) -> anyhow::Result<()> {
        let parent_ino: INum;
        let node_name: OsString;
        let mut deferred_deletion = false;
        {
            // pre-check whether deferred delete or not
            let node = self.cache.get(&ino).unwrap_or_else(|| {
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
            let parent_node = self.cache.get_mut(&parent_ino).unwrap_or_else(|| {
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
            let node = self.cache.get(&ino).unwrap_or_else(|| {
                panic!(
                    "impossible case, may_deferred_delete_node_helper() \
                        should already find the i-node of ino={} to remove",
                    ino,
                );
            });
            let insert_result = self.trash.insert(ino); // check thread-safe in case of deferred deletion race
            debug_assert!(
                insert_result,
                "failed to insert node of ino={} into trash for deferred deletion",
                ino,
            );
            debug!(
                "may_deferred_delete_node_helper() defered removed \
                    the node name={:?} of ino={} under parent ino={}, \
                    open count={}, lookup count={}",
                node.get_name(),
                ino,
                parent_ino,
                node.get_open_count(),
                node.get_lookup_count(),
            );
        } else {
            // immediate deletion
            let inode = self.cache.remove(&ino).unwrap_or_else(|| {
                panic!(
                    "impossible case, may_deferred_delete_node_helper() \
                    should remove the i-node of ino={} immediately",
                    ino,
                );
            }); // TODO: support thread-safe
            debug!(
                "may_deferred_delete_node_helper() immediately removed \
                    the node name={:?} of ino={} under parent ino={}, \
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
    async fn remove_node_helper(
        &mut self,
        parent: INum,
        node_name: OsString,
        node_type: SFlag,
        // reply: ReplyEmpty,
    ) -> anyhow::Result<()> {
        let node_ino: INum;
        {
            // pre-checks
            let parent_node = self.cache.get(&parent).unwrap_or_else(|| {
                panic!(
                    "remove_node_helper() found fs is inconsistent, \
                        parent of ino={} should be in cache before remove its child",
                    parent,
                );
            });
            match parent_node.get_entry(&node_name) {
                None => {
                    debug!(
                        "remove_node_helper() failed to find node name={:?} \
                            under parent of ino={}",
                        node_name, parent,
                    );
                    return util::build_error_result_from_errno(
                        libc::ENOENT,
                        format!(
                            "remove_node_helper() failed to find node name={:?} \
                                under parent of ino={}",
                            node_name, parent,
                        ),
                    );
                    // reply.error_code(ENOENT).await?;
                    // return Ok(());
                }
                Some(child_entry) => {
                    node_ino = child_entry.ino();
                    if let SFlag::S_IFDIR = node_type {
                        // check the directory to delete is empty
                        let dir_node = self.cache.get(&node_ino).unwrap_or_else(|| {
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
                                libc::ENOTEMPTY,
                                format!(
                                    "remove_node_helper() cannot remove \
                                        the non-empty directory name={:?} of ino={} \
                                        under the parent directory of ino={}",
                                    node_name, node_ino, parent,
                                ),
                            );
                            // reply.error_code(ENOTEMPTY).await?;
                            // return Ok(());
                        }
                    }

                    let child_inode = self.cache.get(&node_ino).unwrap_or_else(|| {
                        panic!(
                            "remove_node_helper() found fs is inconsistent, \
                            node name={:?} of ino={} found under the parent of ino={}, \
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
                    "remove_node_helper() failed to maybe deferred delete child node of ino={}, \
                        name={:?} and type={:?} under parent ino={}",
                    node_ino, node_name, node_type, parent,
                ))?;
            // reply.ok().await?;
            debug!(
                "remove_node_helper() successfully removed child node of ino={}, \
                    name={:?} and type={:?} under parent ino={}",
                node_ino, node_name, node_type, parent,
            );
            Ok(())
        }
    }

    /// Helper function to lookup
    async fn lookup_helper(
        &mut self,
        parent: INum,
        name: &OsStr,
    ) -> anyhow::Result<(Duration, FuseAttr, u64)> {
        let child_name = OsString::from(name);

        let ino: INum;
        let child_type: SFlag;
        {
            // lookup child ino and type first
            let parent_node = self.cache.get(&parent).unwrap_or_else(|| {
                panic!(
                    "lookup_helper() found fs is inconsistent, \
                        the parent i-node of ino={} should be in cache",
                    parent,
                );
            });
            if let Some(child_entry) = parent_node.get_entry(&child_name) {
                ino = child_entry.ino();
                child_type = child_entry.entry_type();
            } else {
                debug!(
                    "lookup_helper() failed to find the file name={:?} \
                        under parent directory of ino={}",
                    child_name, parent
                );
                // lookup() didn't find anything, this is normal
                return util::build_error_result_from_errno(
                    libc::ENOENT,
                    format!(
                        "lookup_helper() failed to find the file name={:?} \
                            under parent directory of ino={}",
                        child_name, parent,
                    ),
                );
            }
        }

        let ttl = Duration::new(MY_TTL_SEC, 0);
        {
            // cache hit
            if let Some(node) = self.cache.get(&ino) {
                debug!(
                    "lookup_helper() cache hit when searching file of \
                        name={:?} and ino={} under parent ino={}",
                    child_name, ino, parent,
                );
                let attr = node.lookup_attr();
                let fuse_attr = util::convert_to_fuse_attr(attr);
                debug!(
                    "lookup_helper() successfully found the file name={:?} of \
                        ino={} under parent ino={}, the attr={:?}",
                    child_name, ino, parent, &attr,
                );
                return Ok((ttl, fuse_attr, MY_GENERATION));
            }
        }
        {
            // cache miss
            debug!(
                "lookup_helper() cache missed when searching parent ino={}
                    and file name={:?} of ino={}",
                parent, child_name, ino,
            );
            let parent_node = self.cache.get_mut(&parent).unwrap_or_else(|| {
                panic!(
                    "lookup_helper() found fs is inconsistent, \
                        parent i-node of ino={} should be in cache",
                    parent,
                );
            });
            let child_node = match child_type {
                SFlag::S_IFDIR => parent_node
                    .open_child_dir(child_name)
                    .await
                    .context(format!(
                        "lookup_helper() failed to open sub-directory name={:?}",
                        name
                    ))?,
                SFlag::S_IFREG => {
                    let oflags = OFlag::O_RDWR;
                    parent_node
                        .open_child_file(child_name, oflags)
                        .await
                        .context(format!(
                            "lookup_helper() failed to open child file name={:?} with flags={:?}",
                            name, oflags
                        ))?
                }
                _ => panic!(
                    "lookup_helper() found unsupported file type={:?}",
                    child_type
                ),
            };

            let child_ino = child_node.get_ino();
            let attr = child_node.lookup_attr();
            self.cache.insert(child_ino, child_node);
            let fuse_attr = util::convert_to_fuse_attr(attr);
            debug!(
                "lookup_helper() successfully found the file name={:?} of ino={} \
                under parent ino={}, the attr={:?}",
                name, ino, parent, &attr,
            );
            Ok((ttl, fuse_attr, MY_GENERATION))
        }
    }

    /// Rename helper function to pre-check
    fn rename_pre_check(
        &self,
        old_parent: INum,
        old_name: &OsStr,
        new_parent: INum,
        new_name: &OsStr,
        no_replace: bool,
    ) -> anyhow::Result<(RawFd, INum, RawFd, Option<INum>)> {
        let old_parent_node = self.cache.get(&old_parent).unwrap_or_else(|| {
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
                    "rename() failed to find child entry of name={:?} under parent directory ino={}",
                    old_name, old_parent,
                );
                return util::build_error_result_from_errno(
                    libc::ENOENT,
                    format!(
                        "rename_pre_check() failed to find child entry of name={:?} \
                            under parent directory ino={}",
                        old_name, old_parent,
                    ),
                );
            }
            Some(old_entry) => {
                debug_assert_eq!(&old_name, &old_entry.entry_name());
                if self.cache.get(&old_entry.ino()).is_none() {
                    panic!(
                        "rename() found fs is inconsistent, the i-node of name={:?} and ino={} to rename should be in cache",
                        old_name, old_entry.ino(),
                    );
                    // return;
                }
                old_entry.ino()
            }
        };

        let new_parent_node = self.cache.get(&new_parent).unwrap_or_else(|| {
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
            if self.cache.get(&new_ino).is_none() {
                panic!(
                    "rename() found fs is inconsistent, \
                        the i-node of name={:?} and ino={} to replace should be in cache",
                    new_name, new_ino,
                );
                // return;
            }
            if no_replace {
                debug!(
                    "rename() found i-node of ino={} and name={:?} under new parent ino={}, \
                        but RENAME_NOREPLACE is specified",
                    new_ino, new_name, new_parent,
                );
                return util::build_error_result_from_errno(
                    libc::EEXIST, // RENAME_NOREPLACE
                    format!(
                        "rename() found i-node of ino={} and name={:?} under new parent ino={}, \
                            but RENAME_NOREPLACE is specified",
                        new_ino, new_name, new_parent,
                    ),
                );
            }
            debug!(
                "rename() found the new parent directory of ino={} already has a child with name={:?}",
                new_parent, new_name,
            );
            Some(new_ino)
        } else {
            None
        };
        Ok((old_parent_fd, old_entry_ino, new_parent_fd, new_entry_ino))
    }

    /// Rename helper to exchange on disk
    async fn rename_exchange_helper(
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

        let rename_replace_res =
            self.rename_in_cache_helper(old_parent, &old_name, new_parent, &new_name);
        if let Some(replaced_entry) = rename_replace_res {
            debug_assert_eq!(
                new_entry_ino,
                replaced_entry.ino(),
                "rename_exchange_helper() replaced entry i-number not match"
            );
            let exchange_entry =
                DirEntry::new(new_entry_ino, old_name.clone(), replaced_entry.entry_type());

            // TODO: support thread-safe
            let old_parent_node = self.cache.get_mut(&old_parent).unwrap_or_else(|| {
                panic!(
                    "impossible case, the parent i-node of ino={} should be in cache",
                    old_parent,
                )
            });
            let insert_res = old_parent_node.insert_entry(exchange_entry);
            debug_assert!(
                insert_res.is_none(),
                "impossible case, the child entry of name={:?} should have been \
                    moved out of old parent directory ino={}",
                old_name,
                old_parent,
            );
            let exchanged_node = self.cache.get_mut(&new_entry_ino).unwrap_or_else(|| {
                panic!(
                    "impossible case, the new entry i-node of ino={} should be in cache",
                    new_entry_ino,
                )
            });
            exchanged_node.set_parent_ino(new_parent);
            exchanged_node.set_name(new_name.clone());
            let exchanged_attr = exchanged_node
                .load_attribute()
                .await
                .context(format!(
                    "rename_exchange_helper() failed to load attribute of \
                        new entry i-node of ino={} and name={:?}",
                    new_entry_ino, new_name,
                ))
                .unwrap_or_else(|e| {
                    panic!(
                        "rename_exchange_helper() failed, the error is: {}",
                        util::format_anyhow_error(&e),
                    )
                });
            debug_assert_eq!(exchanged_attr.ino, exchanged_node.get_ino());
            debug_assert_eq!(exchanged_attr.ino, new_entry_ino);
            // TODO: finish rename exchange when libc::rename2 is available
            todo!("rename2 system call has not been supported in libc to exchange two nodes yet!");
        } else {
            panic!(
                "impossible case, the child entry of name={:?} to be exchanged \
                    should be under new parent directory ino={}",
                new_name, new_parent,
            );
        }
    }

    /// Rename helper to move on disk, it may replace destination entry
    async fn rename_may_replace_helper(
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
                        maybe deferred delete node ino={}",
                    new_ino,
                ))?;
        }
        let old_name_clone = old_name.clone();
        let new_name_clone = new_name.clone();
        // Rename on disk
        blocking!(nix::fcntl::renameat(
            Some(old_parent_fd),
            Path::new(&old_name_clone),
            Some(new_parent_fd),
            Path::new(&new_name_clone),
        ))
        .context(format!(
            "rename_may_replace_helper() failed to move the old file name={:?} under \
                old parent ino={} to the new file name={:?} under new parent ino={}",
            old_name, old_parent, new_name, new_parent,
        ))?;

        let moved_node = self.cache.get_mut(&old_entry_ino).unwrap_or_else(|| {
            panic!(
                "impossible case, the old entry i-node of ino={} should be in cache",
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
                    util::format_anyhow_error(&e)
                )
            });
        debug_assert_eq!(moved_attr.ino, moved_node.get_ino());
        debug_assert_eq!(moved_attr.ino, old_entry_ino);
        debug!(
            "rename_may_replace_helper() successfully moved the old file \
                name={:?} of ino={} under old parent ino={} to \
                the new file name={:?} of ino={} under new parent ino={}",
            old_name, old_entry_ino, old_parent, new_name, old_entry_ino, new_parent,
        );

        let rename_replace_res =
            self.rename_in_cache_helper(old_parent, &old_name, new_parent, &new_name);
        debug_assert!(
            rename_replace_res.is_none(),
            "may_deferred_delete_node_helper() should already have \
                deleted the target i-node to b replaced",
        );
        // if let Some(replaced_entry) = rename_replace_res {
        //     debug_assert!(
        //         new_entry_ino.is_some(),
        //         "rename_may_replace_helper() should replace new entry",
        //     );
        //     debug_assert_eq!(
        //         new_entry_ino,
        //         Some(replaced_entry.ino()),
        //         "rename_may_replace_helper() replaced entry i-number not match",
        //     );
        // } else {
        //     debug_assert!(
        //         new_entry_ino.is_none(),
        //         "rename_may_replace_helper() should not replace new entry",
        //     );
        // }
        Ok(())
    }

    /// Rename in cache helper
    fn rename_in_cache_helper(
        &mut self,
        old_parent: INum,
        old_name: &OsStr,
        new_parent: INum,
        new_name: &OsStr,
    ) -> Option<DirEntry> {
        let entry_to_move = {
            // TODO: support thread-safe
            let old_parent_node = self.cache.get_mut(&old_parent).unwrap_or_else(|| {
                panic!(
                    "impossible case, the parent i-node of ino={} should be in cache",
                    old_parent,
                )
            });
            match old_parent_node.remove_entry(old_name) {
                None => panic!(
                    "impossible case, the child entry of name={:?} \
                        should be under parent directory ino={}",
                    old_name, old_parent
                ),
                Some(old_entry) => DirEntry::new(
                    old_entry.ino(),
                    new_name.to_os_string(),
                    old_entry.entry_type(),
                ),
            }
        };
        {
            // TODO: support thread-safe
            let new_parent_node = self.cache.get_mut(&new_parent).unwrap_or_else(|| {
                panic!(
                    "impossible case, the new parent i-node of ino={} should be in cache",
                    new_parent
                )
            });
            new_parent_node.insert_entry(entry_to_move)
        }
    }

    /// Helper function of fsync
    async fn fsync_helper(
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
                blocking!(unistd::fdatasync(fh.cast())).context(format!(
                    "fsync_helper() failed to flush the node of ino={}",
                    ino
                ))?;
            } else {
                blocking!(unistd::fsync(fh.cast())).context(format!(
                    "fsync_helper() failed to flush the node of ino={}",
                    ino
                ))?;
            }
        }
        #[cfg(target_os = "macos")]
        {
            blocking!(unistd::fsync(fh.cast())).context(format!(
                "fsync_helper() failed to flush the node of ino={}",
                ino
            ))?;
        }
        // reply.ok().await?;
        debug!(
            "fsync_helper() successfully sync the node of ino={}, fh={}, datasync={}",
            ino, fh, datasync,
        );
        Ok(())
    }

    /// Return ENOSYS for not implemented operations
    pub async fn not_implement_helper(
        &mut self,
        req: &Request<'_>,
        fd: RawFd,
    ) -> nix::Result<usize> {
        let reply = ReplyEmpty::new(req.unique(), fd);
        reply.error_code(libc::ENOSYS).await
    }

    // Implemented FUSE operations

    /// Initialize filesystem.
    /// Called before any other filesystem method.
    pub fn init(&self, req: &Request<'_>) -> nix::Result<()> {
        debug!(
            "init(req={:?}), cache size={}, trash size={}",
            req,
            self.cache.len(),
            self.trash.len(),
        );
        Ok(())
    }

    /// Clean up filesystem.
    /// Called on filesystem exit.
    pub fn destroy(&self, req: &Request<'_>) {
        debug!(
            "destroy(req={:?}), cache size={}, trash size={}",
            req,
            self.cache.len(),
            self.trash.len(),
        );
    }

    /// Look up a directory entry by name and get its attributes.
    pub async fn lookup(
        &mut self,
        req: &Request<'_>,
        parent: INum,
        name: &OsStr,
        reply: ReplyEntry,
    ) -> nix::Result<usize> {
        // let child_name = OsString::from(name);
        debug!("lookup(parent={}, name={:?}, req={:?})", parent, name, req,);
        let lookup_res = self.lookup_helper(parent, name).await;
        match lookup_res {
            Ok((ttl, fuse_attr, generation)) => reply.entry(ttl, fuse_attr, generation).await,
            Err(e) => {
                debug!(
                    "lookup() failed to find the file name={:?} under parent ino={}, \
                        the error is: {}",
                    name,
                    parent,
                    util::format_anyhow_error(&e),
                );
                reply.error(e).await
            }
        }
    }

    /// Get file attributes.
    pub async fn getattr(&mut self, req: &Request<'_>, reply: ReplyAttr) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!("getattr(ino={}, req={:?})", ino, req);

        let node = self.cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "getattr() found fs is inconsistent, \
                    the i-node of ino={} should be in cache",
                ino,
            );
        });
        let attr = node.get_attr();
        debug!(
            "getattr() cache hit when searching the attribute of ino={}",
            ino,
        );
        let ttl = Duration::new(MY_TTL_SEC, 0);
        let fuse_attr = util::convert_to_fuse_attr(attr);
        debug!(
            "getattr() successfully got the attribute of ino={}, the attr={:?}",
            ino, attr,
        );
        reply.attr(ttl, fuse_attr).await
    }

    /// Open a file.
    /// Open flags (with the exception of `O_CREAT`, `O_EXCL`, `O_NOCTTY` and `O_TRUNC`) are
    /// available in flags. Filesystem may store an arbitrary file handle (pointer, index,
    /// etc) in fh, and use this in other all other file operations (read, write, flush,
    /// release, fsync). Filesystem may also implement stateless file I/O and not store
    /// anything in fh. There are also some flags (`direct_io`, `keep_cache`) which the
    /// filesystem may set, to change the way the file is opened. See `fuse_file_info`
    /// structure in `fuse_common.h` for more details.
    pub async fn open(
        &mut self,
        req: &Request<'_>,
        flags: u32,
        reply: ReplyOpen,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!("open(ino={}, flags={}, req={:?})", ino, flags, req);

        let node = self.cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "open() found fs is inconsistent, the i-node of ino={} should be in cache",
                ino,
            );
        });
        let o_flags = util::parse_oflag(flags);
        let new_fd = node
            .dup_fd(o_flags)
            .await
            .context(format!(
                "failed to duplicate the fd of file name={:?} and ino={}",
                node.get_name(),
                node.get_ino(),
            ))
            .unwrap_or_else(|e| {
                panic!(
                    "open() failed, the error is: {}",
                    util::format_anyhow_error(&e)
                )
            });
        debug!(
            "open() successfully duplicated the file handler of ino={}, fd={}, flags={:?}",
            ino, new_fd, flags,
        );
        reply.opened(new_fd, flags).await
    }

    /// Forget about an inode.
    /// The nlookup parameter indicates the number of lookups previously performed on
    /// this inode. If the filesystem implements inode lifetimes, it is recommended that
    /// inodes acquire a single reference on each lookup, and lose nlookup references on
    /// each forget. The filesystem may ignore forget calls, if the inodes don't need to
    /// have a limited lifetime. On unmount it is not guaranteed, that all referenced
    /// inodes will receive a forget message.
    pub fn forget(&mut self, req: &Request<'_>, nlookup: u64) {
        let ino = req.nodeid();
        debug!("forget(ino={}, nlookup={}, req={:?})", ino, nlookup, req,);
        let current_count: i64;
        {
            let node = self.cache.get(&ino).unwrap_or_else(|| {
                panic!(
                    "forget() found fs is inconsistent, \
                        the i-node of ino={} should be in cache",
                    ino,
                );
            });
            let previous_count = node.dec_lookup_count_by(nlookup);
            current_count = node.get_lookup_count();
            debug_assert!(current_count >= 0);
            debug_assert_eq!(previous_count.overflow_sub(current_count), nlookup.cast()); // assert no race forget
            debug!(
                "forget() successfully reduced lookup count of ino={} from {} to {}",
                ino, previous_count, current_count,
            );
        }
        {
            if current_count == 0 {
                // TODO: support thread-safe
                if self.trash.contains(&ino) {
                    // deferred deletion
                    let deleted_node = self.cache.remove(&ino).unwrap_or_else(|| {
                        panic!(
                            "forget() found fs is inconsistent, node of ino={} \
                                found in trash, but no i-node found for deferred deletion",
                            ino,
                        );
                    });
                    self.trash.remove(&ino);
                    debug_assert_eq!(deleted_node.get_lookup_count(), 0);
                    debug!(
                        "forget() deferred deleted i-node of ino={}, the i-node={:?}",
                        ino, deleted_node,
                    );
                }
            }
        }
    }

    /// Set file attributes.
    pub async fn setattr(
        &mut self,
        req: &Request<'_>,
        param: SetAttrParam,
        reply: ReplyAttr,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        let valid = param.valid;
        let fh = param.fh;
        let mode = param.mode;
        let u_id = param.u_id;
        let g_id = param.g_id;
        let size = param.size;
        let a_time = param.a_time;
        let m_time = param.m_time;
        #[cfg(feature = "abi-7-9")]
        let lock_owner = param.lock_owner;
        #[cfg(feature = "abi-7-23")]
        let c_time = param.c_time;
        #[cfg(target_os = "macos")]
        let crtime = param.crtime;
        #[cfg(target_os = "macos")]
        let chgtime = param.chgtime;
        #[cfg(target_os = "macos")]
        let bkuptime = param.bkuptime;
        #[cfg(target_os = "macos")]
        let flags = param.flags;
        debug!(
            "setattr(ino={}, mode={:?}, uid={:?}, gid={:?}, size={:?}, \
                atime={:?}, mtime={:?}, fh={:?}, req={:?})",
            ino, mode, u_id, g_id, size, a_time, m_time, fh, req,
        );
        #[cfg(target_os = "macos")]
        debug!(
            "crtime={:?}, chgtime={:?}, bkuptime={:?}, flags={:?}",
            crtime, chgtime, bkuptime, flags,
        );

        let i_node = self.cache.get_mut(&ino).unwrap_or_else(|| {
            panic!(
                "setattr() found fs is inconsistent, \
                    the i-node of ino={} should be in cache",
                ino,
            );
        });
        let mut attr = i_node.get_attr();
        let ttl = Duration::new(MY_TTL_SEC, 0);

        if let Some(b) = mode {
            attr.perm = util::parse_mode_bits(b);
            debug!("setattr() set permission={}", attr.perm);

            let kind = util::parse_sflag(b);
            debug_assert_eq!(kind, attr.kind);
        }
        // no replace
        attr.uid = u_id.unwrap_or(attr.uid);
        attr.gid = g_id.unwrap_or(attr.gid);
        attr.size = size.unwrap_or(attr.size);
        attr.atime = a_time.unwrap_or(attr.atime);
        attr.mtime = m_time.unwrap_or(attr.mtime);
        #[cfg(target_os = "macos")]
        {
            attr.crtime = crtime.unwrap_or(attr.crtime);
            // attr.chgtime = chgtime.unwrap_or(attr.chgtime);
            // attr.bkuptime = bkuptime.unwrap_or(attr.bkuptime);
            attr.flags = flags.unwrap_or(attr.flags);
        }

        let sth_changed = mode.is_some()
            || u_id.is_some()
            || g_id.is_some()
            || size.is_some()
            || a_time.is_some()
            || m_time.is_some()
            || fh.is_some();
        #[cfg(feature = "abi-7-9")]
        let sth_changed = sth_changed || lock_owner.is_some();
        #[cfg(feature = "abi-7-23")]
        let sth_changed = sth_changed || c_time.is_some();
        #[cfg(target_os = "macos")]
        let sth_changed = sth_changed
            || crtime.is_some()
            || chgtime.is_some()
            || bkuptime.is_some()
            || flags.is_some();

        let fuse_attr = util::convert_to_fuse_attr(attr);
        if sth_changed {
            // update ctime, since meta data might change in setattr
            // attr.ctime = SystemTime::now();
            i_node.set_attr(attr);
            debug!(
                "setattr() successfully set the attribute of ino={}, the set attr={:?}",
                ino, attr,
            );
            // TODO: write attribute change to disk using chmod, chown, chflags
            reply.attr(ttl, fuse_attr).await
        } else if valid == 0 {
            // Nothing chagned, just reply the attribute
            reply.attr(ttl, fuse_attr).await
        } else {
            // reply.error_code(ENODATA).await?;
            panic!(
                "setattr() found all the input attributes are empty for the file of ino={}",
                ino,
            );
            // Err(anyhow!("no change to attr"))
        }
    }

    /// Create file node.
    /// Create a regular file, character device, block device, fifo or socket node.
    pub async fn mknod(
        &mut self,
        req: &Request<'_>,
        parent: INum,
        name: &OsStr,
        mode: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) -> nix::Result<usize> {
        debug!(
            "mknod(parent={}, name={:?}, mode={}, rdev={}, req={:?})",
            parent, name, mode, rdev, req,
        );

        let mknod_res = self
            .create_node_helper(parent, name.into(), mode, SFlag::S_IFREG)
            .await
            .context(format!(
                "mknod() failed to create a node name={:?} and mode={:?} under parent ino={},",
                name, mode, parent,
            ));
        match mknod_res {
            Ok((ttl, fuse_attr, generation)) => reply.entry(ttl, fuse_attr, generation).await,
            Err(e) => {
                debug!(
                    "mknod() failed to create a node name={:?} and mode={:?} under parent ino={}, \
                        the error is: {}",
                    name,
                    mode,
                    parent,
                    util::format_anyhow_error(&e),
                );
                reply.error(e).await
            }
        }
    }

    /// Create a directory.
    pub async fn mkdir(
        &mut self,
        req: &Request<'_>,
        parent: INum,
        name: &OsStr,
        mode: u32,
        reply: ReplyEntry,
    ) -> nix::Result<usize> {
        debug!(
            "mkdir(parent={}, name={:?}, mode={}, req={:?})",
            parent, name, mode, req,
        );

        let mkdir_res = self
            .create_node_helper(parent, name.into(), mode, SFlag::S_IFDIR)
            .await
            .context(format!(
                "mkdir() failed to create a directory name={:?} and mode={:?} under parent ino={}",
                name, mode, parent,
            ));
        match mkdir_res {
            Ok((ttl, fuse_attr, generation)) => reply.entry(ttl, fuse_attr, generation).await,
            Err(e) => {
                debug!(
                    "mkdir() failed to create a node name={:?} and mode={:?} under parent ino={}, \
                            the error is: {}",
                    name,
                    mode,
                    parent,
                    util::format_anyhow_error(&e),
                );
                reply.error(e).await
            }
        }
    }

    /// Remove a file.
    pub async fn unlink(
        &mut self,
        req: &Request<'_>,
        parent: INum,
        name: &OsStr,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        debug!("unlink(parent={}, name={:?}, req={:?}", parent, name, req,);
        let unlink_res = self
            .remove_node_helper(parent, name.into(), SFlag::S_IFREG)
            .await
            .context(format!(
                "unlink() failed to remove file name={:?} under parent ino={}",
                name, parent,
            ));
        match unlink_res {
            Ok(()) => reply.ok().await,
            Err(e) => {
                debug!(
                    "unlink() failed to remove file name={:?} under parent ino={}, \
                        the error is: {}",
                    name,
                    parent,
                    util::format_anyhow_error(&e),
                );
                reply.error(e).await
            }
        }
    }

    /// Remove a directory.
    pub async fn rmdir(
        &mut self,
        req: &Request<'_>,
        parent: INum,
        name: &OsStr,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        let dir_name = OsString::from(name);
        debug!(
            "rmdir(parent={}, name={:?}, req={:?})",
            parent, dir_name, req,
        );
        let rmdir_res = self
            .remove_node_helper(parent, dir_name, SFlag::S_IFDIR)
            .await
            .context(format!(
                "rmdir() failed to remove sub-directory name={:?} under parent ino={}",
                name, parent,
            ));
        match rmdir_res {
            Ok(()) => reply.ok().await,
            Err(e) => {
                debug!(
                    "rmdir() failed to remove sub-directory name={:?} under parent ino={}, \
                            the error is: {}",
                    name,
                    parent,
                    util::format_anyhow_error(&e),
                );
                reply.error(e).await
            }
        }
    }

    /// Rename a file
    ///
    /// If the target exists it should be atomically replaced. If
    /// the target's inode's lookup count is non-zero, the file
    /// system is expected to postpone any removal of the inode
    /// until the lookup count reaches zero (see description of the
    /// forget function).
    ///
    /// *flags* may be `RENAME_EXCHANGE` or `RENAME_NOREPLACE`. If
    /// `RENAME_NOREPLACE` is specified, the filesystem must not
    /// overwrite *newname* if it exists and return an error
    /// instead. If `RENAME_EXCHANGE` is specified, the filesystem
    /// must atomically exchange the two files, i.e. both must
    /// exist and neither may be deleted.
    pub async fn rename(
        &mut self,
        req: &Request<'_>,
        param: RenameParam,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        let old_parent = param.old_parent;
        let old_name = param.old_name;
        let new_parent = param.new_parent;
        let new_name = param.new_name;
        let flags = param.flags;
        debug!(
            "rename(old parent={}, old name={:?}, new parent={}, new name={:?}, req={:?})",
            old_parent, old_name, new_parent, new_name, req,
        );
        let no_replace = flags == 1; // RENAME_NOREPLACE
        let exchange = flags == 2; // RENAME_EXCHANGE

        let pre_check_res =
            self.rename_pre_check(old_parent, &old_name, new_parent, &new_name, no_replace);
        let (old_parent_fd, old_entry_ino, new_parent_fd, new_entry_ino) = match pre_check_res {
            Ok((old_parent_fd, old_entry_ino, new_parent_fd, new_entry_ino)) => {
                (old_parent_fd, old_entry_ino, new_parent_fd, new_entry_ino)
            }
            Err(e) => {
                debug!(
                    "rename() pre-check failed, the error is: {}",
                    util::format_anyhow_error(&e)
                );
                return reply.error(e).await;
            }
        };
        let helper_param = RenameHelperParam {
            old_parent,
            old_parent_fd,
            old_name,
            old_entry_ino,
            new_parent,
            new_parent_fd,
            new_name,
        };
        let rename_res = if let Some(new_ino) = new_entry_ino {
            if exchange {
                self.rename_exchange_helper(helper_param, new_ino).await
            } else {
                // Rename replace
                self.rename_may_replace_helper(helper_param, Some(new_ino))
                    .await
            }
        } else {
            // No need to replace
            self.rename_may_replace_helper(
                helper_param,
                None, // new_entry_ino
            )
            .await
        };
        match rename_res {
            Ok(()) => reply.ok().await,
            Err(e) => {
                debug!(
                    "rename() failed, the error is: {}",
                    util::format_anyhow_error(&e)
                );
                reply.error(e).await
            }
        }
    }

    /// Read data.
    /// Read should send exactly the number of bytes requested except on EOF or error,
    /// otherwise the rest of the data will be substituted with zeroes. An exception to
    /// this is when the file has been opened in `direct_io` mode, in which case the
    /// return value of the read system call will reflect the return value of this
    /// operation. fh will contain the value set by the open method, or will be undefined
    /// if the open method didn't set any value.
    pub async fn read(
        &mut self,
        req: &Request<'_>,
        fh: u64,
        offset: i64,
        size: u32,
        reply: ReplyData,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!(
            "read(ino={}, fh={}, offset={}, size={}, req={:?})",
            ino, fh, offset, size, req,
        );
        debug_assert!(
            !offset.is_negative(),
            "offset={} cannot be negative",
            offset
        );

        let read_helper = |content: &Vec<u8>| -> anyhow::Result<Vec<u8>> {
            match content.len().cmp(&(offset.cast())) {
                std::cmp::Ordering::Greater => {
                    let read_data = if offset.overflow_add(size.cast()).cast::<usize>()
                        <= content.len()
                    {
                        debug!("read exact {} bytes", size);
                        content
                            .get(offset.cast()..offset.overflow_add(size.cast()).cast::<usize>())
                            .ok_or_else(|| util::build_sys_error_from_errno(libc::EINVAL))
                            .context(format!(
                                "read_helper() failed to get file content from offset={} and size={}",
                                offset, size,
                            ))?
                    } else {
                        debug!(
                            "read_helper() read {} bytes only, less than expected size={}",
                            offset
                                .overflow_add(size.cast())
                                .cast::<usize>()
                                .overflow_sub(content.len()),
                            size,
                        );
                        content
                            .get(offset.cast()..)
                            .ok_or_else(|| util::build_sys_error_from_errno(libc::EINVAL))
                            .context(format!(
                                "read_helper() failed to get file content from offset={}",
                                offset
                            ))?
                    };
                    // TODO: consider zero copy
                    Ok(read_data.to_vec())
                }
                std::cmp::Ordering::Equal => {
                    debug!(
                        "read_helper() found offset={} equals file length={}, nothing to read",
                        offset,
                        content.len(),
                    );
                    Ok(Vec::new())
                }
                std::cmp::Ordering::Less => util::build_error_result_from_errno(
                    libc::EINVAL,
                    format!(
                        "read_helper() failed to read, offset={} beyond file length={}",
                        offset,
                        content.len(),
                    ),
                ),
            }
        };

        let node = self.cache.get_mut(&ino).unwrap_or_else(|| {
            panic!(
                "read() found fs is inconsistent, \
                    the i-node of ino={} should be in cache",
                ino,
            );
        });
        if node.need_load_file_data() {
            let load_res = node.load_data().await;
            if let Err(e) = load_res {
                debug!(
                    "read() failed to load file data, the error is: {}",
                    util::format_anyhow_error(&e)
                );
                return reply.error(e).await;
            }
        }
        let file_data = node.get_file_data();
        match read_helper(file_data) {
            Ok(read_data_vec) => {
                debug!(
                    "read() successfully read {} bytes from the file of ino={}",
                    read_data_vec.len(),
                    ino,
                );
                reply.data(read_data_vec).await
            }
            Err(e) => {
                debug!(
                    "read() failed, the error is: {}",
                    util::format_anyhow_error(&e),
                );
                reply.error(e).await
            }
        }
    }

    /// Write data.
    /// Write should return exactly the number of bytes requested except on error. An
    /// exception to this is when the file has been opened in `direct_io` mode, in
    /// which case the return value of the write system call will reflect the return
    /// value of this operation. fh will contain the value set by the open method, or
    /// will be undefined if the open method did not set any value.
    pub async fn write(
        &mut self,
        req: &Request<'_>,
        fh: u64,
        offset: i64,
        data: Vec<u8>,
        flags: u32,
        reply: ReplyWrite,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!(
            "write(ino={}, fh={}, offset={}, data-size={}, flags={})",
            // "write(ino={}, fh={}, offset={}, data-size={}, req={:?})",
            ino,
            fh,
            offset,
            data.len(),
            flags,
            // req.request,
        );

        let inode = self.cache.get_mut(&ino).unwrap_or_else(|| {
            panic!(
                "write() found fs is inconsistent, \
                    the i-node of ino={} should be in cache",
                ino,
            );
        });
        let o_flags = util::parse_oflag(flags);
        let write_to_disk = true;
        let data_len = data.len();
        let write_result = inode
            .write_file(fh, offset, data, o_flags, write_to_disk)
            .await;
        match write_result {
            Ok(written_size) => {
                debug!(
                    "write() successfully wrote {} byte data to file ino={} at offset={}",
                    data_len, ino, offset,
                );
                reply.written(written_size.cast()).await
            }
            Err(e) => {
                debug!(
                    "write() failed, the error is: {}",
                    util::format_anyhow_error(&e)
                );
                reply.error(e).await
            }
        }
    }

    /// Flush method.
    /// This is called on each close() of the opened file. Since file descriptors can
    /// be duplicated (dup, dup2, fork), for one open call there may be many flush
    /// calls. Filesystems should not assume that flush will always be called after some
    /// writes, or that if will be called at all. fh will contain the value set by the
    /// open method, or will be undefined if the open method did not set any value.
    /// NOTE: the name of the method is misleading, since (unlike fsync) the filesystem
    /// is not forced to flush pending writes. One reason to flush data, is if the
    /// filesystem wants to return write errors. If the filesystem supports file locking
    /// operations (setlk, getlk) it should remove all locks belonging to `lock_owner`.
    pub async fn flush(
        &mut self,
        req: &Request<'_>,
        fh: u64,
        lock_owner: u64,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!(
            "flush(ino={}, fh={}, lock_owner={}, req={:?})",
            ino, fh, lock_owner, req,
        );

        // This is called from every close on an open file, so call the
        // close on the underlying filesystem.	But since flush may be
        // called multiple times for an open file, this must not really
        // close the file. This is important if used on a network
        // filesystem like NFS which flush the data/metadata on close()
        let new_fd = blocking!(unistd::dup(fh.cast()))
            .context(format!(
                "flush() failed to duplicate the handler ino={} fh={:?}",
                ino, fh,
            ))
            .unwrap_or_else(|e| {
                panic!(
                    "flush() failed, the error is: {}",
                    util::format_anyhow_error(&e)
                )
            });
        blocking!(unistd::close(new_fd))
            .context(format!(
                "flush() failed to close the duplicated file handler={} of ino={}",
                new_fd, ino,
            ))
            .unwrap_or_else(|e| {
                panic!(
                    "flush() failed, the error is: {}",
                    util::format_anyhow_error(&e)
                )
            });
        reply.ok().await
    }

    /// Release an open file.
    /// Release is called when there are no more references to an open file: all file
    /// descriptors are closed and all memory mappings are unmapped. For every open
    /// call there will be exactly one release call. The filesystem may reply with an
    /// error, but error values are not returned to close() or munmap() which triggered
    /// the release. fh will contain the value set by the open method, or will be undefined
    /// if the open method didn't set any value. flags will contain the same flags as for
    /// open.
    pub async fn release(
        &mut self,
        req: &Request<'_>,
        fh: u64,
        flags: u32, // same as the open flags
        lock_owner: u64,
        flush: bool,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!(
            "release(ino={}, fh={}, flags={}, lock_owner={}, flush={}, req={:?})",
            ino, fh, flags, lock_owner, flush, req,
        );
        // TODO: handle lock_owner
        let node = self.cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "release() found fs is inconsistent, \
                    the i-node of ino={} should be in cache",
                ino,
            );
        });
        let fd = fh.cast();
        if flush {
            // TODO: double check the meaning of the flush flag
            blocking!(unistd::fsync(fd))
                .context(format!("release() failed to flush the file of ino={}", ino,))
                .unwrap_or_else(|e| {
                    panic!(
                        "release() failed, the error is: {}",
                        util::format_anyhow_error(&e)
                    );
                });
        }
        blocking!(unistd::close(fd))
            .context(format!(
                "release() failed to close the file handler={} of ino={}",
                fh, ino,
            ))
            .unwrap_or_else(|e| {
                panic!(
                    "release() failed, the error is: {}",
                    util::format_anyhow_error(&e)
                );
            });
        node.dec_open_count(); // decrease open count before reply in case reply failed
        debug!(
            "release() successfully closed the file handler={} of ino={}",
            fh, ino,
        );
        reply.ok().await
    }

    /// Synchronize file contents.
    /// If the datasync parameter is non-zero, then only the user data should be flushed,
    /// not the meta data.
    pub async fn fsync(
        &mut self,
        req: &Request<'_>,
        fh: u64,
        datasync: bool,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!(
            "fsync(ino={}, fh={}, datasync={}, req={:?})",
            ino, fh, datasync, req,
        );
        match Self::fsync_helper(ino, fh, datasync).await {
            Ok(()) => reply.ok().await,
            Err(e) => {
                debug!(
                    "fsync() failed, the error is: {}",
                    util::format_anyhow_error(&e)
                );
                reply.error(e).await
            }
        }
    }

    /// Open a directory.
    /// Filesystem may store an arbitrary file handle (pointer, index, etc) in fh, and
    /// use this in other all other directory stream operations (readdir, releasedir,
    /// fsyncdir). Filesystem may also implement stateless directory I/O and not store
    /// anything in fh, though that makes it impossible to implement standard conforming
    /// directory stream operations in case the contents of the directory can change
    /// between opendir and releasedir.
    pub async fn opendir(
        &mut self,
        req: &Request<'_>,
        flags: u32,
        reply: ReplyOpen,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!("opendir(ino={}, flags={}, req={:?})", ino, flags, req,);

        let node = self.cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "opendir() found fs is inconsistent, \
                    the i-node of ino={} should be in cache",
                ino,
            );
        });

        let o_flags = util::parse_oflag(flags);
        let dup_res = node.dup_fd(o_flags).await;
        match dup_res {
            Ok(new_fd) => {
                debug!(
                    "opendir() successfully duplicated the file handler of \
                        ino={}, new fd={}, flags={:?}",
                    ino, new_fd, o_flags,
                );
                reply.opened(new_fd, flags).await
            }
            Err(e) => {
                debug!(
                    "opendir() failed, the error is: {}",
                    util::format_anyhow_error(&e)
                );
                reply.error(e).await
            }
        }
    }

    /// Read directory.
    /// Send a buffer filled using buffer.fill(), with size not exceeding the
    /// requested size. Send an empty buffer on end of stream. fh will contain the
    /// value set by the opendir method, or will be undefined if the opendir method
    /// didn't set any value.
    pub async fn readdir(
        &mut self,
        req: &Request<'_>,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!(
            "readdir(ino={}, fh={}, offset={}, req={:?})",
            ino, fh, offset, req,
        );

        let readdir_helper = |data: &BTreeMap<OsString, DirEntry>| -> usize {
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
                    "readdir() found one child name={:?} ino={} offset={} entry={:?} \
                        under the directory of ino={}",
                    child_name,
                    child_ino,
                    offset.overflow_add(i.cast()).overflow_add(1),
                    child_entry,
                    ino,
                );
            }
            num_child_entries
        };

        let node = self.cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "readdir() found fs is inconsistent, \
                    the i-node of ino={} should be in cache",
                ino,
            );
        });
        let num_child_entries = node.read_dir(readdir_helper);
        debug!(
            "readdir() successfully read {} children \
            under the directory of ino={}",
            num_child_entries, ino,
        );
        reply.ok().await
    }

    /// Release an open directory.
    /// For every opendir call there will be exactly one releasedir call. fh will
    /// contain the value set by the opendir method, or will be undefined if the
    /// opendir method didn't set any value.
    pub async fn releasedir(
        &mut self,
        req: &Request<'_>,
        fh: u64,
        flags: u32,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!(
            "releasedir(ino={}, fh={}, flags={}, req={:?})",
            ino, fh, flags, req,
        );
        // TODO: handle flags
        let node = self.cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "releasedir() found fs is inconsistent, \
                    the i-node of ino={} should be in cache",
                ino,
            );
        });
        blocking!(unistd::close(fh.cast()))
            .context(format!(
                "releasedir() failed to close the file handler={} of ino={}",
                fh, ino,
            ))
            .unwrap_or_else(|e| {
                panic!(
                    "releasedir() failed, the error is: {}",
                    util::format_anyhow_error(&e),
                );
            });
        node.dec_open_count();
        debug!(
            "releasedir() successfully closed the file handler={} of ino={}",
            fh, ino,
        );
        reply.ok().await
    }

    /// Synchronize directory contents.
    /// If the datasync parameter is set, then only the directory contents should
    /// be flushed, not the meta data. fh will contain the value set by the opendir
    /// method, or will be undefined if the opendir method didn't set any value.
    pub async fn fsyncdir(
        &mut self,
        req: &Request<'_>,
        fh: u64,
        datasync: bool,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!(
            "fsyncdir(ino={}, fh={}, datasync={}, req={:?})",
            ino, fh, datasync, req,
        );
        // Self::fsync_helper(ino, fh, datasync, reply).await
        match Self::fsync_helper(ino, fh, datasync).await {
            Ok(()) => reply.ok().await,
            Err(e) => {
                debug!(
                    "fsyncdir() failed, the error is: {}",
                    util::format_anyhow_error(&e)
                );
                reply.error(e).await
            }
        }
    }

    /// Get file system statistics.
    /// The `f_favail`, `f_fsid` and `f_flag` fields are ignored
    pub async fn statfs(&mut self, req: &Request<'_>, reply: ReplyStatFs) -> nix::Result<usize> {
        let ino = if req.nodeid() == 0 {
            FUSE_ROOT_ID
        } else {
            req.nodeid()
        };
        debug!("statfs(ino={}, req={:?})", ino, req);

        let node = self.cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "statfs() found fs is inconsistent, \
                    the i-node of ino={} should be in cache",
                ino,
            );
        });
        let fd = node.get_fd();
        let statfs_res = blocking!(
            let file = unsafe { std::fs::File::from_raw_fd(fd) };
            let stat_res = statvfs::fstatvfs(&file); // statvfs is POSIX, whereas statfs is not
            let _fd = file.into_raw_fd(); // prevent fd to be closed by File
            stat_res
        )
        .context("statfs() failed to run statvfs()");
        match statfs_res {
            Ok(statvfs) => {
                debug!(
                    "statfs() successfully read the statvfs of ino={}, the statvfs={:?}",
                    ino, statvfs,
                );
                reply
                    .statfs(StatFsParam {
                        blocks: statvfs.blocks().cast(),
                        bfree: statvfs.blocks_free().cast(),
                        bavail: statvfs.blocks_available().cast(),
                        files: statvfs.files().cast(),
                        f_free: statvfs.files_free().cast(),
                        bsize: statvfs.block_size().cast(), // TODO: consider use customized block size
                        namelen: statvfs.name_max().cast(),
                        frsize: statvfs.fragment_size().cast(),
                    })
                    .await
            }
            Err(e) => {
                debug!(
                    "statfs() failed to read the statvfs of ino={}, the error is: {}",
                    ino,
                    util::format_anyhow_error(&e)
                );
                reply.error(e).await
            }
        }
    }

    // Un-implemented FUSE operations

    /// Interrupt another FUSE request
    pub fn interrupt(&self, req: &Request<'_>, unique: u64) {
        debug!(
            "interrupt(req={:?}), cache size={}, trash size={}",
            req,
            self.cache.len(),
            self.trash.len(),
        );
        // TODO: handle FUSE_INTERRUPT
        warn!(
            "FUSE INTERRUPT recieved, request w/ unique={} interrupted",
            unique
        );
    }

    /// Read symbolic link.
    pub async fn readlink(&mut self, _req: &Request<'_>, reply: ReplyData) -> nix::Result<usize> {
        reply.error_code(libc::ENOSYS).await
    }

    /// Create a symbolic link.
    pub async fn symlink(
        &mut self,
        _req: &Request<'_>,
        _parent: INum,
        _name: &OsStr,
        _link: &Path,
        reply: ReplyEntry,
    ) -> nix::Result<usize> {
        reply.error_code(libc::ENOSYS).await
    }

    /// Create a hard link.
    pub async fn link(
        &mut self,
        _req: &Request<'_>,
        _newparent: u64,
        _newname: &OsStr,
        reply: ReplyEntry,
    ) -> nix::Result<usize> {
        reply.error_code(libc::ENOSYS).await
    }

    /// Set an extended attribute.
    pub async fn setxattr(
        &mut self,
        _req: &Request<'_>,
        _name: &OsStr,
        _value: &[u8],
        _flags: u32,
        _position: u32,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        reply.error_code(libc::ENOSYS).await
    }

    /// Get an extended attribute.
    /// If `size` is 0, the size of the value should be sent with `reply.size()`.
    /// If `size` is not 0, and the value fits, send it with `reply.data()`, or
    /// `reply.error(ERANGE)` if it doesn't.
    pub async fn getxattr(
        &mut self,
        _req: &Request<'_>,
        _name: &OsStr,
        _size: u32,
        reply: ReplyXAttr,
    ) -> nix::Result<usize> {
        reply.error_code(libc::ENOSYS).await
    }

    /// List extended attribute names.
    /// If `size` is 0, the size of the value should be sent with `reply.size()`.
    /// If `size` is not 0, and the value fits, send it with `reply.data()`, or
    /// `reply.error(ERANGE)` if it doesn't.
    pub async fn listxattr(
        &mut self,
        _req: &Request<'_>,
        _size: u32,
        reply: ReplyXAttr,
    ) -> nix::Result<usize> {
        reply.error_code(libc::ENOSYS).await
    }

    /// Remove an extended attribute.
    pub async fn removexattr(
        &mut self,
        _req: &Request<'_>,
        _name: &OsStr,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        reply.error_code(libc::ENOSYS).await
    }

    /// Check file access permissions.
    /// This will be called for the `access()` system call. If the `default_permissions`
    /// mount option is given, this method is not called. This method is not called
    /// under Linux kernel versions 2.4.x
    pub async fn access(
        &mut self,
        _req: &Request<'_>,
        _mask: u32,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        reply.error_code(libc::ENOSYS).await
    }

    /// Create and open a file.
    /// If the file does not exist, first create it with the specified mode, and then
    /// open it. Open flags (with the exception of `O_NOCTTY`) are available in flags.
    /// Filesystem may store an arbitrary file handle (pointer, index, etc) in fh,
    /// and use this in other all other file operations (read, write, flush, release,
    /// fsync). There are also some flags (`direct_io`, `keep_cache`) which the
    /// filesystem may set, to change the way the file is opened. See `fuse_file_info`
    /// structure in `fuse_common.h` for more details. If this method is not
    /// implemented or under Linux kernel versions earlier than 2.6.15, the mknod()
    /// and open() methods will be called instead.
    pub async fn create(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        _flags: u32,
        reply: ReplyCreate,
    ) -> nix::Result<usize> {
        reply.error_code(libc::ENOSYS).await
    }

    /// Test for a POSIX file lock.
    pub async fn getlk(
        &mut self,
        _req: &Request<'_>,
        _lk_param: FileLockParam,
        reply: ReplyLock,
    ) -> nix::Result<usize> {
        reply.error_code(libc::ENOSYS).await
    }

    /// Acquire, modify or release a POSIX file lock.
    /// For POSIX threads (NPTL) there's a 1-1 relation between pid and owner, but
    /// otherwise this is not always the case.  For checking lock ownership,
    /// `fi->owner` must be used. The `l_pid` field in `struct flock` should only be
    /// used to fill in this field in `getlk()`. Note: if the locking methods are not
    /// implemented, the kernel will still allow file locking to work locally.
    /// Hence these are only interesting for network filesystems and similar.
    pub async fn setlk(
        &mut self,
        _req: &Request<'_>,
        _lk_param: FileLockParam,
        _sleep: bool,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        reply.error_code(libc::ENOSYS).await
    }

    /// Map block index within file to block index within device.
    /// Note: This makes sense only for block device backed filesystems mounted
    /// with the `blkdev` option
    pub async fn bmap(
        &mut self,
        _req: &Request<'_>,
        _blocksize: u32,
        _idx: u64,
        reply: ReplyBMap,
    ) -> nix::Result<usize> {
        reply.error_code(libc::ENOSYS).await
    }

    /// macOS only: Rename the volume. Set `fuse_init_out.flags` during init to
    /// `FUSE_VOL_RENAME` to enable
    #[cfg(target_os = "macos")]
    pub async fn setvolname(
        &mut self,
        _req: &Request<'_>,
        _name: &OsStr,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        reply.error_code(libc::ENOSYS).await
    }

    /// macOS only (undocumented)
    #[cfg(target_os = "macos")]
    pub async fn exchange(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _newparent: u64,
        _newname: &OsStr,
        _options: u64,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        reply.error_code(libc::ENOSYS).await
    }

    /// macOS only: Query extended times (`bkuptime` and `crtime`). Set `fuse_init_out.flags`
    /// during init to `FUSE_XTIMES` to enable
    #[cfg(target_os = "macos")]
    pub async fn getxtimes(
        &mut self,
        _req: &Request<'_>,
        reply: ReplyXTimes,
    ) -> nix::Result<usize> {
        reply.error_code(libc::ENOSYS).await
    }
}

#[cfg(test)]
mod test {

    use nix::sys::statvfs;
    use std::fs::File;
    #[test]
    fn test_statfs() -> anyhow::Result<()> {
        let file = File::open(".")?;
        let statvfs = statvfs::fstatvfs(&file)?;
        println!(
            "blocks={}, bfree={}, bavail={}, files={}, \
                ffree={}, bsize={}, namelen={}, frsize={}",
            statvfs.blocks(),
            statvfs.blocks_free(),
            statvfs.blocks_available(),
            statvfs.files(),
            statvfs.files_free(),
            statvfs.block_size(),
            statvfs.name_max(),
            statvfs.fragment_size(),
        );
        Ok(())
    }
}
