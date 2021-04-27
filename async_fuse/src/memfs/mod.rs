//! The implementation of user space file system
mod cache;
mod dir;
mod fs_util;
mod metadata;
mod node;
mod s3_wrapper;

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::os::unix::io::{FromRawFd, IntoRawFd, RawFd};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Context;
use async_trait::async_trait;
use log::{debug, warn};
use nix::errno::Errno;
use nix::sys::{stat::SFlag, statvfs};
use nix::unistd;
use smol::lock::{Mutex, RwLock};
use utilities::{Cast, OverflowArithmetic};

use crate::fuse::file_system::FileSystem;
use crate::fuse::fuse_reply::AsIoVec;
use crate::fuse::fuse_reply::{
    ReplyAttr, ReplyBMap, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyLock, ReplyOpen, ReplyStatFs, ReplyWrite, ReplyXAttr, StatFsParam,
};
use crate::fuse::fuse_request::Request;
use crate::fuse::protocol::{INum, FUSE_ROOT_ID};
use cache::GlobalCache;
use dir::DirEntry;
use metadata::MetaData;
use node::Node;
use std::os::unix::ffi::OsStringExt;

/// The time-to-live seconds of FUSE attributes
const MY_TTL_SEC: u64 = 3600; // TODO: should be a long value, say 1 hour

/// In-memory file system
pub struct MemFs(Arc<Mutex<MetaData>>);

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
    #[cfg(target_os = "linux")]
    pub flags: u32,
    /// Rename exchange options
    #[cfg(target_os = "macos")]
    pub flags: u64,
}

/// Rename helper parameters
pub(crate) struct RenameHelperParam {
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

impl MemFs {
    /// Create `FileSystem`
    pub async fn new(mount_point: &Path, capacity: usize) -> anyhow::Result<Self> {
        let root_path = mount_point
            .canonicalize()
            .with_context(|| format!("failed to canonicalize the mount path={:?}", mount_point))?;
        let root_inode = Node::open_root_node(FUSE_ROOT_ID, OsString::from("/"), &root_path)
            .await
            .context("failed to open FUSE root node")?;
        let mut cache = BTreeMap::new();
        cache.insert(FUSE_ROOT_ID, root_inode);
        let trash = BTreeSet::new(); // for deferred deletion nodes

        Ok(Self(Arc::new(Mutex::new(MetaData {
            root_path,
            cache: RwLock::new(cache),
            trash,
            data_cache: Arc::new(GlobalCache::new_with_capacity(capacity)),
        }))))
    }
}

#[async_trait]
impl FileSystem for MemFs {
    // Implemented FUSE operations

    /// Initialize filesystem.
    /// Called before any other filesystem method.
    async fn init(&self, req: &Request<'_>) -> nix::Result<()> {
        let this = self.0.lock().await;
        let cache = this.cache.read().await;
        debug!(
            "init(req={:?}), cache size={}, trash size={}",
            req,
            cache.len(),
            this.trash.len(),
        );
        Ok(())
    }

    /// Clean up filesystem.
    /// Called on filesystem exit.
    async fn destroy(&self, req: &Request<'_>) {
        let this = self.0.lock().await;
        let cache = this.cache.read().await;
        debug!(
            "destroy(req={:?}), cache size={}, trash size={}",
            req,
            cache.len(),
            this.trash.len(),
        );
    }

    /// Look up a directory entry by name and get its attributes.
    async fn lookup(
        &self,
        req: &Request<'_>,
        parent: INum,
        name: &OsStr,
        reply: ReplyEntry,
    ) -> nix::Result<usize> {
        let mut this = self.0.lock().await;

        debug!("lookup(parent={}, name={:?}, req={:?})", parent, name, req,);
        let pre_check_res = this.lookup_pre_check(parent, name).await;
        let (ino, child_type) = match pre_check_res {
            Ok((ino, child_type)) => (ino, child_type),
            Err(e) => {
                debug!(
                    "lookup() failed to pre-check, the error is: {}",
                    common::util::format_anyhow_error(&e),
                );
                return reply.error(e).await;
            }
        };
        let lookup_res = this.lookup_helper(parent, ino, name, child_type).await;
        match lookup_res {
            Ok((ttl, fuse_attr, generation)) => {
                debug!(
                    "lookup() successfully found the node name={:?} of ino={} \
                        under parent ino={}, the attr={:?}",
                    name, ino, parent, &fuse_attr,
                );
                reply.entry(ttl, fuse_attr, generation).await
            }
            Err(e) => {
                debug!(
                    "lookup() failed to find the node name={:?} under parent ino={}, \
                        the error is: {}",
                    name,
                    parent,
                    common::util::format_anyhow_error(&e),
                );
                reply.error(e).await
            }
        }
    }

    /// Get file attributes.
    async fn getattr(&self, req: &Request<'_>, reply: ReplyAttr) -> nix::Result<usize> {
        let this = self.0.lock().await;

        let ino = req.nodeid();
        debug!("getattr(ino={}, req={:?})", ino, req);

        let cache = this.cache.read().await;
        let node = cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "getattr() found fs is inconsistent, \
                    the i-node of ino={} should be in cache",
                ino,
            );
        });
        let attr = node.get_attr();
        debug!(
            "getattr() cache hit when searching the attribute of ino={} and name={:?}",
            ino,
            node.get_name(),
        );
        let ttl = Duration::new(MY_TTL_SEC, 0);
        let fuse_attr = fs_util::convert_to_fuse_attr(attr);
        debug!(
            "getattr() successfully got the attribute of ino={}, name={:?} and attr={:?}",
            ino,
            node.get_name(),
            attr,
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
    async fn open(&self, req: &Request<'_>, flags: u32, reply: ReplyOpen) -> nix::Result<usize> {
        let this = self.0.lock().await;

        let ino = req.nodeid();
        debug!("open(ino={}, flags={}, req={:?})", ino, flags, req);

        let cache = this.cache.read().await;
        let node = cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "open() found fs is inconsistent, the i-node of ino={} should be in cache",
                ino,
            );
        });
        let o_flags = fs_util::parse_oflag(flags);
        // TODO: handle open flags
        // <https://pubs.opengroup.org/onlinepubs/9699919799/functions/open.html>
        // let open_res = if let SFlag::S_IFLNK = node.get_type() {
        //     node.open_symlink_target(o_flags).await.context(format!(
        //         "open() failed to open symlink target={:?} with flags={}",
        //         node.get_symlink_target(),
        //         flags,
        //     ))
        // } else {
        let dup_res = node.dup_fd(o_flags).await.context(format!(
            "open() failed to duplicate the fd of file name={:?} and ino={}",
            node.get_name(),
            node.get_ino(),
        ));
        // };
        match dup_res {
            Ok(new_fd) => {
                debug!(
                    "open() successfully duplicated the file handler of ino={} and name={:?}, fd={}, flags={:?}",
                    ino, node.get_name(), new_fd, flags,
                );
                reply.opened(new_fd, flags).await
            }
            Err(e) => {
                debug!(
                    "open() failed, the error is: {}",
                    common::util::format_anyhow_error(&e)
                );
                reply.error(e).await
            }
        }
    }

    /// Forget about an inode.
    /// The nlookup parameter indicates the number of lookups previously performed on
    /// this inode. If the filesystem implements inode lifetimes, it is recommended that
    /// inodes acquire a single reference on each lookup, and lose nlookup references on
    /// each forget. The filesystem may ignore forget calls, if the inodes don't need to
    /// have a limited lifetime. On unmount it is not guaranteed, that all referenced
    /// inodes will receive a forget message.
    async fn forget(&self, req: &Request<'_>, nlookup: u64) {
        let mut this = self.0.lock().await;

        let ino = req.nodeid();
        debug!("forget(ino={}, nlookup={}, req={:?})", ino, nlookup, req,);
        let current_count: i64;
        {
            let cache = this.cache.read().await;
            let node = cache.get(&ino).unwrap_or_else(|| {
                panic!(
                    "forget() found fs is inconsistent, \
                        the i-node of ino={} should be in cache",
                    ino,
                );
            });
            let previous_count = node.dec_lookup_count_by(nlookup);
            current_count = node.get_lookup_count();
            debug_assert!(current_count >= 0);
            debug_assert_eq!(
                previous_count.overflow_sub(current_count),
                nlookup.cast::<i64>()
            ); // assert no race forget
            debug!(
                "forget() successfully reduced lookup count of ino={} and name={:?} from {} to {}",
                ino,
                node.get_name(),
                previous_count,
                current_count,
            );
        }
        {
            if current_count == 0 {
                // TODO: support thread-safe
                if this.trash.contains(&ino) {
                    // deferred deletion
                    this.trash.remove(&ino);
                    let mut cache = this.cache.write().await;
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
                }
            }
        }
    }

    /// Set file attributes.
    async fn setattr(
        &self,
        req: &Request<'_>,
        param: SetAttrParam,
        reply: ReplyAttr,
    ) -> nix::Result<usize> {
        let this = self.0.lock().await;
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
        let _lock_owner = param.lock_owner;
        #[cfg(feature = "abi-7-23")]
        let _c_time = param.c_time;
        #[cfg(target_os = "macos")]
        let crtime = param.crtime;
        #[cfg(target_os = "macos")]
        let chgtime = param.chgtime;
        #[cfg(target_os = "macos")]
        let bkuptime = param.bkuptime;
        #[cfg(target_os = "macos")]
        let flags = param.flags;
        debug!(
            "setattr(ino={}, valid={:?}, mode={:?}, uid={:?}, gid={:?}, size={:?}, \
                atime={:?}, mtime={:?}, fh={:?}, req={:?})",
            ino,
            valid,
            mode.map(|bits| format!("{:#o}", bits)),
            u_id,
            g_id,
            size,
            a_time,
            m_time,
            fh,
            req,
        );
        #[cfg(target_os = "macos")]
        debug!(
            "crtime={:?}, chgtime={:?}, bkuptime={:?}, flags={:?}",
            crtime, chgtime, bkuptime, flags,
        );
        if 0 == valid {
            warn!("setattr() enountered valid=0, the req={:?}", req);
        }

        let mut cache = this.cache.write().await;
        let inode = cache.get_mut(&ino).unwrap_or_else(|| {
            panic!(
                "setattr() found fs is inconsistent, \
                    the i-node of ino={} should be in cache",
                ino,
            );
        });
        let fd = inode.get_fd();
        let attr = inode.get_attr();
        let ttl = Duration::new(MY_TTL_SEC, 0);

        // if let Some(b) = mode {
        //     attr.perm = fs_util::parse_mode_bits(b);
        //     debug!("setattr() set permission={}", attr.perm);

        //     let kind = fs_util::parse_sflag(b);
        //     debug_assert_eq!(kind, attr.kind);
        // }
        // // no replace
        // attr.uid = u_id.unwrap_or(attr.uid);
        // attr.gid = g_id.unwrap_or(attr.gid);
        // attr.size = size.unwrap_or(attr.size);
        // attr.atime = a_time.unwrap_or(attr.atime);
        // attr.mtime = m_time.unwrap_or(attr.mtime);
        // #[cfg(target_os = "macos")]
        // {
        //     attr.crtime = crtime.unwrap_or(attr.crtime);
        //     // attr.chgtime = chgtime.unwrap_or(attr.chgtime);
        //     // attr.bkuptime = bkuptime.unwrap_or(attr.bkuptime);
        //     attr.flags = flags.unwrap_or(attr.flags);
        // }

        // let sth_changed = mode.is_some()
        //     || u_id.is_some()
        //     || g_id.is_some()
        //     || size.is_some()
        //     || a_time.is_some()
        //     || m_time.is_some()
        //     || fh.is_some();
        // #[cfg(feature = "abi-7-9")]
        // let sth_changed = sth_changed || lock_owner.is_some();
        // #[cfg(feature = "abi-7-23")]
        // let sth_changed = sth_changed || c_time.is_some();
        // #[cfg(target_os = "macos")]
        // let sth_changed = sth_changed
        //     || crtime.is_some()
        //     || chgtime.is_some()
        //     || bkuptime.is_some()
        //     || flags.is_some();

        // let fuse_attr = fs_util::convert_to_fuse_attr(attr);
        // if sth_changed {
        //     // update ctime, since meta data might change in setattr
        //     // attr.ctime = SystemTime::now();
        //     i_node.set_attr(attr);
        //     debug!(
        //         "setattr() successfully set the attribute of ino={}, the set attr={:?}",
        //         ino, attr,
        //     );
        //     // TODO: write attribute change to disk using chmod, chown, chflags
        //     reply.attr(ttl, fuse_attr).await
        // } else if valid == 0 {
        //     // Nothing chagned, just reply the attribute
        //     reply.attr(ttl, fuse_attr).await
        // } else {
        //     panic!(
        //         "setattr() found all the input attributes are empty for the file of ino={}",
        //         ino,
        //     );
        //     // Err(anyhow!("no change to attr"))
        // }

        let set_res = MetaData::setattr_helper(fd, param, attr).await;
        match set_res {
            Ok((attr_changed, file_attr)) => {
                if attr_changed {
                    inode.set_attr(file_attr);
                    debug!(
                        "setattr() successfully set the attribute of ino={} and name={:?}, the set attr={:?}",
                        ino, inode.get_name(), attr,
                    );
                } else {
                    warn!(
                        "setattr() did not change any attribute of ino={} and name={:?}",
                        ino,
                        inode.get_name(),
                    );
                }
                let fuse_attr = fs_util::convert_to_fuse_attr(file_attr);
                reply.attr(ttl, fuse_attr).await
            }
            Err(e) => {
                debug!(
                    "setattr() failed to set the attribute of ino={} and name={:?}, the error is: {}",
                    ino,
                    inode.get_name(),
                    common::util::format_anyhow_error(&e),
                );
                reply.error(e).await
            }
        }
    }

    /// Create file node.
    /// Create a regular file, character device, block device, fifo or socket node.
    async fn mknod(
        &self,
        req: &Request<'_>,
        parent: INum,
        name: &OsStr,
        mode: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) -> nix::Result<usize> {
        let mut this = self.0.lock().await;

        debug!(
            "mknod(parent={}, name={:?}, mode={}, rdev={}, req={:?})",
            parent, name, mode, rdev, req,
        );

        let mknod_res = this
            .create_node_helper(parent, name.into(), mode, SFlag::S_IFREG, None)
            .await
            .context(format!(
                "mknod() failed to create an i-node name={:?} and mode={:?} under parent ino={},",
                name, mode, parent,
            ));
        match mknod_res {
            Ok((ttl, fuse_attr, generation)) => reply.entry(ttl, fuse_attr, generation).await,
            Err(e) => {
                debug!(
                    "mknod() failed to create an i-node name={:?} and mode={:?} under parent ino={}, \
                        the error is: {}",
                    name,
                    mode,
                    parent,
                    common::util::format_anyhow_error(&e),
                );
                reply.error(e).await
            }
        }
    }

    /// Create a directory.
    async fn mkdir(
        &self,
        req: &Request<'_>,
        parent: INum,
        name: &OsStr,
        mode: u32,
        reply: ReplyEntry,
    ) -> nix::Result<usize> {
        let mut this = self.0.lock().await;

        debug!(
            "mkdir(parent={}, name={:?}, mode={}, req={:?})",
            parent, name, mode, req,
        );

        let mkdir_res = this
            .create_node_helper(parent, name.into(), mode, SFlag::S_IFDIR, None)
            .await
            .context(format!(
                "mkdir() failed to create a directory name={:?} and mode={:?} under parent ino={}",
                name, mode, parent,
            ));
        match mkdir_res {
            Ok((ttl, fuse_attr, generation)) => reply.entry(ttl, fuse_attr, generation).await,
            Err(e) => {
                debug!(
                    "mkdir() failed to create a directory name={:?} and mode={:?} under parent ino={}, \
                        the error is: {}",
                    name,
                    mode,
                    parent,
                    common::util::format_anyhow_error(&e),
                );
                reply.error(e).await
            }
        }
    }

    /// Remove a file.
    async fn unlink(
        &self,
        req: &Request<'_>,
        parent: INum,
        name: &OsStr,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        let mut this = self.0.lock().await;

        debug!("unlink(parent={}, name={:?}, req={:?}", parent, name, req,);
        let entry_type = {
            let cache = this.cache.read().await;
            let parent_node = cache.get(&parent).unwrap_or_else(|| {
                panic!(
                    "unlink() found fs is inconsistent, \
                        parent of ino={} should be in cache before remove its child",
                    parent,
                );
            });
            let child_entry = parent_node.get_entry(name).unwrap_or_else(|| {
                panic!(
                    "unlink() found fs is inconsistent, \
                        the child entry name={:?} to remove is not under parent of ino={}",
                    name, parent,
                );
            });
            let entry_type = child_entry.entry_type();
            debug_assert_ne!(
                SFlag::S_IFDIR,
                entry_type,
                "unlink() should not remove sub-directory name={:?} under parent ino={}",
                name,
                parent,
            );
            entry_type
        };

        let unlink_res = this
            .remove_node_helper(parent, name.into(), entry_type)
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
                    common::util::format_anyhow_error(&e),
                );
                reply.error(e).await
            }
        }
    }

    /// Remove a directory.
    async fn rmdir(
        &self,
        req: &Request<'_>,
        parent: INum,
        name: &OsStr,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        let mut this = self.0.lock().await;

        let dir_name = OsString::from(name);
        debug!(
            "rmdir(parent={}, name={:?}, req={:?})",
            parent, dir_name, req,
        );
        let rmdir_res = this
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
                    common::util::format_anyhow_error(&e),
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
    async fn rename(
        &self,
        req: &Request<'_>,
        param: RenameParam,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        let mut this = self.0.lock().await;

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

        let pre_check_res = this
            .rename_pre_check(old_parent, &old_name, new_parent, &new_name, no_replace)
            .await;
        let (old_parent_fd, old_entry_ino, new_parent_fd, new_entry_ino) = match pre_check_res {
            Ok((old_parent_fd, old_entry_ino, new_parent_fd, new_entry_ino)) => {
                (old_parent_fd, old_entry_ino, new_parent_fd, new_entry_ino)
            }
            Err(e) => {
                debug!(
                    "rename() pre-check failed, the error is: {}",
                    common::util::format_anyhow_error(&e)
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
                this.rename_exchange_helper(helper_param, new_ino).await
            } else {
                // Rename replace
                this.rename_may_replace_helper(helper_param, Some(new_ino))
                    .await
            }
        } else {
            // No need to replace
            this.rename_may_replace_helper(
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
                    common::util::format_anyhow_error(&e)
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
    async fn read(
        &self,
        req: &Request<'_>,
        fh: u64,
        offset: i64,
        size: u32,
        reply: ReplyData,
    ) -> nix::Result<usize> {
        let this = self.0.lock().await;
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

        let mut cache = this.cache.write().await;
        let inode = cache.get_mut(&ino).unwrap_or_else(|| {
            panic!(
                "read() found fs is inconsistent, \
                    the i-node of ino={} should be in cache",
                ino,
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
        if inode.need_load_file_data(offset.cast(), size.cast()) {
            let load_res = inode.load_data(offset.cast(), size.cast()).await;
            if let Err(e) = load_res {
                debug!(
                    "read() failed to load file data of ino={} and name={:?}, the error is: {}",
                    ino,
                    inode.get_name(),
                    common::util::format_anyhow_error(&e),
                );
                return reply.error(e).await;
            }
        }
        let file_data = inode.get_file_data(offset.cast(), size.cast());
        debug!("file_data is {:?}", file_data);
        // } else if SFlag::S_IFLNK == node_type {
        //     if node.need_load_symlink_target_data() {
        //         let load_res = node.load_data().await;
        //         if let Err(e) = load_res {
        //             debug!(
        //                 "read() failed to load symlink target data, the error is: {}",
        //                 common::util::format_anyhow_error(&e)
        //             );
        //             return reply.error(e).await;
        //         }
        //     }
        //     let target_path = node.get_symlink_target();
        //     let target_data_res = node.get_symlink_target_data();
        //     if let Some(target_data) = target_data_res {
        //         target_data.get_file_data()
        //     } else {
        //         panic!(
        //             "read() found fs is inconsistent, \
        //                 the symlink target path={:?} should not be broken",
        //             target_path,
        //         );
        //     }
        // } else {
        //     panic!("read() cannot read directory data");
        // };
        match MetaData::read_helper(file_data, size.cast()) {
            Ok(content) => {
                debug!(
                    "read() successfully read {} bytes from the file of ino={} and name={:?}",
                    content.iter().map(|s| AsIoVec::len(s)).sum::<usize>(),
                    ino,
                    inode.get_name(),
                );
                reply.data(content).await
            }
            Err(e) => {
                debug!(
                    "read() failed to read from the file of ino={} and name={:?}, the error is: {}",
                    ino,
                    inode.get_name(),
                    common::util::format_anyhow_error(&e),
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
    async fn write(
        &self,
        req: &Request<'_>,
        fh: u64,
        offset: i64,
        data: Vec<u8>,
        flags: u32,
        reply: ReplyWrite,
    ) -> nix::Result<usize> {
        let this = self.0.lock().await;

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

        let mut cache = this.cache.write().await;
        let inode = cache.get_mut(&ino).unwrap_or_else(|| {
            panic!(
                "write() found fs is inconsistent, \
                    the i-node of ino={} should be in cache",
                ino,
            );
        });
        let o_flags = fs_util::parse_oflag(flags);
        let write_to_disk = true;
        let data_len = data.len();
        let write_result = inode
            .write_file(fh, offset, data, o_flags, write_to_disk)
            .await;
        match write_result {
            Ok(written_size) => {
                debug!(
                    "write() successfully wrote {} byte data to \
                        the file of ino={} and name={:?} at offset={}",
                    data_len,
                    ino,
                    inode.get_name(),
                    offset,
                );
                reply.written(written_size.cast()).await
            }
            Err(e) => {
                debug!(
                    "write() failed to write to the file of ino={} and name={:?} at offset={}, \
                        the error is: {}",
                    ino,
                    inode.get_name(),
                    offset,
                    common::util::format_anyhow_error(&e),
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
    async fn flush(
        &self,
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
        let new_fd = smol::unblock(move || unistd::dup(fh.cast()))
            .await
            .context(format!(
                "flush() failed to duplicate the handler ino={} fh={:?}",
                ino, fh,
            ))
            .unwrap_or_else(|e| {
                panic!(
                    "flush() failed, the error is: {}",
                    common::util::format_anyhow_error(&e)
                )
            });
        smol::unblock(move || unistd::close(new_fd))
            .await
            .context(format!(
                "flush() failed to close the duplicated file handler={} of ino={}",
                new_fd, ino,
            ))
            .unwrap_or_else(|e| {
                panic!(
                    "flush() failed, the error is: {}",
                    common::util::format_anyhow_error(&e)
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
    async fn release(
        &self,
        req: &Request<'_>,
        fh: u64,
        flags: u32, // same as the open flags
        lock_owner: u64,
        flush: bool,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        let this = self.0.lock().await;

        let ino = req.nodeid();
        debug!(
            "release(ino={}, fh={}, flags={}, lock_owner={}, flush={}, req={:?})",
            ino, fh, flags, lock_owner, flush, req,
        );
        // TODO: handle lock_owner
        let cache = this.cache.read().await;
        let inode = cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "release() found fs is inconsistent, \
                    the i-node of ino={} should be in cache",
                ino,
            );
        });
        let fd = fh.cast();
        if flush {
            // TODO: double check the meaning of the flush flag
            smol::unblock(move || unistd::fsync(fd))
                .await
                .context(format!(
                    "release() failed to flush the file of ino={} and name={:?}",
                    ino,
                    inode.get_name(),
                ))
                .unwrap_or_else(|e| {
                    panic!(
                        "release() failed, the error is: {}",
                        common::util::format_anyhow_error(&e)
                    );
                });
        }
        smol::unblock(move || unistd::close(fd))
            .await
            .context(format!(
                "release() failed to close the file handler={} of ino={} and name={:?}",
                fh,
                ino,
                inode.get_name(),
            ))
            .unwrap_or_else(|e| {
                panic!(
                    "release() failed, the error is: {}",
                    common::util::format_anyhow_error(&e)
                );
            });
        inode.dec_open_count(); // decrease open count before reply in case reply failed
        debug!(
            "release() successfully closed the file handler={} of ino={} and name={:?}",
            fh,
            ino,
            inode.get_name(),
        );
        reply.ok().await
    }

    /// Synchronize file contents.
    /// If the datasync parameter is non-zero, then only the user data should be flushed,
    /// not the meta data.
    async fn fsync(
        &self,
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
        match MetaData::fsync_helper(ino, fh, datasync).await {
            Ok(()) => reply.ok().await,
            Err(e) => {
                debug!(
                    "fsync() failed, the error is: {}",
                    common::util::format_anyhow_error(&e)
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
    async fn opendir(&self, req: &Request<'_>, flags: u32, reply: ReplyOpen) -> nix::Result<usize> {
        let this = self.0.lock().await;

        let ino = req.nodeid();
        debug!("opendir(ino={}, flags={}, req={:?})", ino, flags, req,);

        let cache = this.cache.read().await;
        let inode = cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "opendir() found fs is inconsistent, \
                    the i-node of ino={} should be in cache",
                ino,
            );
        });

        let o_flags = fs_util::parse_oflag(flags);
        let dup_res = inode.dup_fd(o_flags).await;
        match dup_res {
            Ok(new_fd) => {
                debug!(
                    "opendir() successfully duplicated the file handler of \
                        ino={} and name={:?} with flags={:?}, the new fd={}",
                    ino,
                    inode.get_name(),
                    o_flags,
                    new_fd,
                );
                reply.opened(new_fd, flags).await
            }
            Err(e) => {
                debug!(
                    "opendir() failed to duplicate the file handler of ino={} and name={:?} with flags={:?}, \
                        the error is: {}",
                    ino, inode.get_name(), o_flags,
                    common::util::format_anyhow_error(&e)
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
    async fn readdir(
        &self,
        req: &Request<'_>,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) -> nix::Result<usize> {
        let this = self.0.lock().await;

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

        let mut cache = this.cache.write().await;
        let inode = cache.get_mut(&ino).unwrap_or_else(|| {
            panic!(
                "readdir() found fs is inconsistent, \
                    the i-node of ino={} should be in cache",
                ino,
            );
        });
        if inode.need_load_dir_data() {
            let load_res = inode.load_data(0usize, 0usize).await;
            if let Err(e) = load_res {
                debug!(
                    "readdir() failed to load the data for directory of ino={} and name={:?}, \
                        the error is: {}",
                    ino,
                    inode.get_name(),
                    common::util::format_anyhow_error(&e)
                );
                return reply.error(e).await;
            }
        }
        let num_child_entries = inode.read_dir(readdir_helper);
        debug!(
            "readdir() successfully read {} entries \
                under the directory of ino={} and name={:?}",
            num_child_entries,
            ino,
            inode.get_name(),
        );
        reply.ok().await
    }

    /// Release an open directory.
    /// For every opendir call there will be exactly one releasedir call. fh will
    /// contain the value set by the opendir method, or will be undefined if the
    /// opendir method didn't set any value.
    async fn releasedir(
        &self,
        req: &Request<'_>,
        fh: u64,
        flags: u32,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        let this = self.0.lock().await;

        let ino = req.nodeid();
        debug!(
            "releasedir(ino={}, fh={}, flags={}, req={:?})",
            ino, fh, flags, req,
        );
        // TODO: handle flags
        let cache = this.cache.read().await;
        let inode = cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "releasedir() found fs is inconsistent, \
                    the i-node of ino={} should be in cache",
                ino,
            );
        });
        smol::unblock(move || unistd::close(fh.cast()))
            .await
            .context(format!(
                "releasedir() failed to close the file handler={} of ino={} and name={:?}",
                fh,
                ino,
                inode.get_name(),
            ))
            .unwrap_or_else(|e| {
                panic!(
                    "releasedir() failed, the error is: {}",
                    common::util::format_anyhow_error(&e),
                );
            });
        inode.dec_open_count();
        debug!(
            "releasedir() successfully closed the file handler={} of ino={} and name={:?}",
            fh,
            ino,
            inode.get_name(),
        );
        reply.ok().await
    }

    /// Synchronize directory contents.
    /// If the datasync parameter is set, then only the directory contents should
    /// be flushed, not the meta data. fh will contain the value set by the opendir
    /// method, or will be undefined if the opendir method didn't set any value.
    async fn fsyncdir(
        &self,
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
        match MetaData::fsync_helper(ino, fh, datasync).await {
            Ok(()) => reply.ok().await,
            Err(e) => {
                debug!(
                    "fsyncdir() failed, the error is: {}",
                    common::util::format_anyhow_error(&e)
                );
                reply.error(e).await
            }
        }
    }

    /// Get file system statistics.
    /// The `f_favail`, `f_fsid` and `f_flag` fields are ignored
    async fn statfs(&self, req: &Request<'_>, reply: ReplyStatFs) -> nix::Result<usize> {
        let this = self.0.lock().await;

        let ino = if req.nodeid() == 0 {
            FUSE_ROOT_ID
        } else {
            req.nodeid()
        };
        debug!("statfs(ino={}, req={:?})", ino, req);

        let cache = this.cache.read().await;
        let inode = cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "statfs() found fs is inconsistent, \
                    the i-node of ino={} should be in cache",
                ino,
            );
        });
        let fd = inode.get_fd();
        let statfs_res = smol::unblock(move || {
            let file = unsafe { std::fs::File::from_raw_fd(fd) };
            let stat_res = statvfs::fstatvfs(&file); // statvfs is POSIX, whereas statfs is not
            let _fd = file.into_raw_fd(); // prevent fd to be closed by File
            stat_res
        })
        .await
        .context(format!(
            "statfs() failed to run statvfs() of ino={} and name={:?}",
            ino,
            inode.get_name(),
        ));
        match statfs_res {
            Ok(statvfs) => {
                debug!(
                    "statfs() successfully read the statvfs of ino={} and name={:?}, the statvfs={:?}",
                    ino, inode.get_name(), statvfs,
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
                    "statfs() failed to read the statvfs of ino={} and name={:?}, the error is: {}",
                    ino,
                    inode.get_name(),
                    common::util::format_anyhow_error(&e)
                );
                reply.error(e).await
            }
        }
    }

    /// Read symbolic link.
    async fn readlink(&self, req: &Request<'_>, reply: ReplyData) -> nix::Result<usize> {
        let this = self.0.lock().await;

        let ino = req.nodeid();
        debug!("readlink(ino={}, req={:?})", ino, req,);
        let cache = this.cache.read().await;
        let symlink_node = cache.get(&ino).unwrap_or_else(|| {
            panic!(
                "readlink() found fs is inconsistent, \
                    the symlink i-node of ino={} should be in cache",
                ino,
            );
        });
        let target_path = symlink_node.get_symlink_target();
        debug!(
            "readlink() successfully read the link of symlink node of ino={} and name={:?}, target_path={:?}",
            ino,
            symlink_node.get_name(),
            target_path,
        );
        reply
            .data(target_path.as_os_str().to_owned().into_vec())
            .await
    }

    /// Create a symbolic link.
    async fn symlink(
        &self,
        req: &Request<'_>,
        parent: INum,
        name: &OsStr,
        target_path: &Path,
        reply: ReplyEntry,
    ) -> nix::Result<usize> {
        let mut this = self.0.lock().await;

        debug!(
            "symlink(parent={}, name={:?}, target_path={:?}, req={:?})",
            parent, name, target_path, req
        );
        let symlink_res = this.create_node_helper(
            parent,
            name.to_owned(),
            0o777, // Symbolic links have no permissions
            SFlag::S_IFLNK,
            Some(target_path),
        )
        .await
        .context(format!(
            "symlink() failed to create a symlink name={:?} to target path={:?} under parent ino={}",
            name, target_path, parent,
        ));
        match symlink_res {
            Ok((ttl, fuse_attr, generation)) => reply.entry(ttl, fuse_attr, generation).await,
            Err(e) => {
                debug!(
                    "symlink() failed to create a symlink name={:?} to target path={:?} under parent ino={}, \
                        the error is: {}",
                    name,
                    target_path,
                    parent,
                    common::util::format_anyhow_error(&e),
                );
                reply.error(e).await
            }
        }
    }

    // Un-implemented FUSE operations

    /// Interrupt another FUSE request
    async fn interrupt(&self, req: &Request<'_>, unique: u64) {
        let this = self.0.lock().await;

        let cache = this.cache.read().await;
        debug!(
            "interrupt(req={:?}), cache size={}, trash size={}",
            req,
            cache.len(),
            this.trash.len(),
        );
        // TODO: handle FUSE_INTERRUPT
        warn!(
            "FUSE INTERRUPT recieved, request w/ unique={} interrupted",
            unique
        );
    }

    /// Create a hard link.
    async fn link(
        &self,
        _req: &Request<'_>,
        _newparent: u64,
        _newname: &OsStr,
        reply: ReplyEntry,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// Set an extended attribute.
    async fn setxattr(
        &self,
        _req: &Request<'_>,
        _name: &OsStr,
        _value: &[u8],
        _flags: u32,
        _position: u32,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// Get an extended attribute.
    /// If `size` is 0, the size of the value should be sent with `reply.size()`.
    /// If `size` is not 0, and the value fits, send it with `reply.data()`, or
    /// `reply.error(ERANGE)` if it doesn't.
    async fn getxattr(
        &self,
        _req: &Request<'_>,
        _name: &OsStr,
        _size: u32,
        reply: ReplyXAttr,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// List extended attribute names.
    /// If `size` is 0, the size of the value should be sent with `reply.size()`.
    /// If `size` is not 0, and the value fits, send it with `reply.data()`, or
    /// `reply.error(ERANGE)` if it doesn't.
    async fn listxattr(
        &self,
        _req: &Request<'_>,
        _size: u32,
        reply: ReplyXAttr,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// Remove an extended attribute.
    async fn removexattr(
        &self,
        _req: &Request<'_>,
        _name: &OsStr,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// Check file access permissions.
    /// This will be called for the `access()` system call. If the `default_permissions`
    /// mount option is given, this method is not called. This method is not called
    /// under Linux kernel versions 2.4.x
    async fn access(
        &self,
        _req: &Request<'_>,
        _mask: u32,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
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
    async fn create(
        &self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        _flags: u32,
        reply: ReplyCreate,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// Test for a POSIX file lock.
    async fn getlk(
        &self,
        _req: &Request<'_>,
        _lk_param: FileLockParam,
        reply: ReplyLock,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// Acquire, modify or release a POSIX file lock.
    /// For POSIX threads (NPTL) there's a 1-1 relation between pid and owner, but
    /// otherwise this is not always the case.  For checking lock ownership,
    /// `fi->owner` must be used. The `l_pid` field in `struct flock` should only be
    /// used to fill in this field in `getlk()`. Note: if the locking methods are not
    /// implemented, the kernel will still allow file locking to work locally.
    /// Hence these are only interesting for network filesystems and similar.
    async fn setlk(
        &self,
        _req: &Request<'_>,
        _lk_param: FileLockParam,
        _sleep: bool,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// Map block index within file to block index within device.
    /// Note: This makes sense only for block device backed filesystems mounted
    /// with the `blkdev` option
    async fn bmap(
        &self,
        _req: &Request<'_>,
        _blocksize: u32,
        _idx: u64,
        reply: ReplyBMap,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// macOS only: Rename the volume. Set `fuse_init_out.flags` during init to
    /// `FUSE_VOL_RENAME` to enable
    #[cfg(target_os = "macos")]
    async fn setvolname(
        &self,
        _req: &Request<'_>,
        _name: &OsStr,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// macOS only: Rename exchange
    #[cfg(target_os = "macos")]
    async fn exchange(
        &self,
        _req: &Request<'_>,
        _param: RenameParam,
        reply: ReplyEmpty,
    ) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
    }

    /// macOS only: Query extended times (`bkuptime` and `crtime`). Set `fuse_init_out.flags`
    /// during init to `FUSE_XTIMES` to enable
    #[cfg(target_os = "macos")]
    async fn getxtimes(&self, _req: &Request<'_>, reply: ReplyXTimes) -> nix::Result<usize> {
        reply.error_code(Errno::ENOSYS).await
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
