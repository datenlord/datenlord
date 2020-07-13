use anyhow::{self, Context};
use libc::{EEXIST, EINVAL, ENODATA, ENOENT, ENOSYS, ENOTEMPTY};
use log::debug;
use nix::fcntl::OFlag;
use nix::sys::{stat::SFlag, statvfs};
use nix::unistd;
use smol::blocking;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::os::unix::io::{FromRawFd, IntoRawFd, RawFd};
use std::path::Path;
use std::time::{Duration, SystemTime};

use super::fuse_reply::*;
use super::fuse_request::*;
use super::protocol::{INum, FUSE_ROOT_ID};

mod dir;
mod node;
mod util;
use dir::*;
use node::*;

const MY_TTL_SEC: u64 = 3600; // TODO: should be a long value, say 1 hour
const MY_GENERATION: u64 = 1; // TODO: find a proper way to set generation

#[derive(Debug)]
pub(crate) struct FileSystem {
    cache: BTreeMap<INum, Node>,
    trash: BTreeSet<INum>,
}

impl FileSystem {
    async fn create_node_helper(
        &mut self,
        parent: u64,
        node_name: OsString,
        mode: u32,
        node_type: SFlag,
        reply: ReplyEntry,
    ) -> anyhow::Result<()> {
        // pre-check
        let parent_node = self.cache.get_mut(&parent);
        debug_assert!(
            parent_node.is_some(),
            "create_node_helper() found fs is inconsistent, \
                parent of ino={} should be in cache before create it new child",
            parent,
        );
        let parent_node = parent_node.unwrap(); // safe to use unwrap() here
        if let Some(occupied) = parent_node.get_entry(&node_name) {
            debug!(
                "create_node_helper() found the directory of ino={} \
                    already exists a child with name={:?} and ino={}",
                parent,
                node_name,
                occupied.ino(),
            );
            reply.error(EEXIST).await?;
            return Ok(());
        }
        // all checks are passed, ready to create new node
        let mflags = util::parse_mode(mode);
        let new_ino: u64;
        let node_name_clone = node_name.clone();
        let new_node = match node_type {
            SFlag::S_IFDIR => {
                debug!(
                    "create_node_helper() about to \
                        create a directory with name={:?}, mode={:?}",
                    node_name, mflags,
                );
                parent_node.create_child_dir(node_name, mflags).await?
            }
            SFlag::S_IFREG => {
                let oflags = OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_RDWR;
                debug!(
                    "helper_create_node() about to \
                        create a file with name={:?}, oflags={:?}, mode={:?}",
                    node_name, oflags, mflags,
                );
                parent_node
                    .create_child_file(node_name, oflags, mflags)
                    .await?
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
        let fuse_attr = util::convert_to_fuse_attr(new_node_attr)?;
        reply.entry(ttl, fuse_attr, MY_GENERATION).await?;
        debug!(
            "create_node_helper() successfully created the new child name={:?} \
                of ino={} and type={:?} under parent ino={}",
            node_name_clone, new_ino, node_type, parent,
        );
        Ok(())
    }

    async fn may_deferred_delete_node_helper(&mut self, ino: u64) -> anyhow::Result<()> {
        let parent_ino: u64;
        let node_name: OsString;
        let mut deferred_deletion = false;
        {
            // pre-check whether deferred delete or not
            let node = self.cache.get(&ino);
            debug_assert!(
                node.is_some(),
                "may_deferred_delete_node_helper() failed to \
                        find the i-node of ino={} to remove",
                ino,
            );
            let node = node.unwrap(); // safe to use unwrap() here

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
            let parent_node = self.cache.get_mut(&parent_ino);
            debug_assert!(
                parent_node.is_some(),
                "helper_get_parent_inode() failed to \
                        find the parent of ino={} for i-node of ino={}",
                parent_ino,
                ino,
            );
            let parent_node = parent_node.unwrap(); // safe to use unwrap() here

            let node_name_clone = node_name.clone();
            let deleted_entry = parent_node.unlink_entry(node_name).await?;
            debug_assert_eq!(&node_name_clone, deleted_entry.entry_name());
            debug_assert_eq!(deleted_entry.ino(), ino);
        }

        if deferred_deletion {
            // deferred deletion
            // TODO: support thread-safe
            let node = self.cache.get(&ino).unwrap(); // safe to use unwrap() here
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
            let inode = self.cache.remove(&ino).unwrap(); // TODO: support thread-safe
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

    async fn remove_node_helper(
        &mut self,
        parent: u64,
        node_name: OsString,
        node_type: SFlag,
        reply: ReplyEmpty,
    ) -> anyhow::Result<()> {
        let node_ino: u64;
        {
            // pre-checks
            let parent_node = self.cache.get(&parent);
            debug_assert!(
                parent_node.is_some(),
                "remove_node_helper() found fs is inconsistent, \
                        parent of ino={} should be in cache before remove its child",
                parent,
            );
            let parent_node = parent_node.unwrap(); // safe to use unwrap() here
            match parent_node.get_entry(&node_name) {
                None => {
                    debug!(
                        "remove_node_helper() failed to find node name={:?} \
                            under parent of ino={}",
                        node_name, parent,
                    );
                    reply.error(ENOENT).await?;
                    return Ok(());
                }
                Some(child_entry) => {
                    node_ino = child_entry.ino();
                    if let SFlag::S_IFDIR = node_type {
                        // check the directory to delete is empty
                        let dir_node = self.cache.get(&node_ino);
                        debug_assert!(
                            dir_node.is_some(),
                            "remove_node_helper() found fs is inconsistent, \
                                    directory name={:?} of ino={} \
                                    found under the parent of ino={}, \
                                    but no i-node found for this directory",
                            node_name,
                            node_ino,
                            parent,
                        );
                        let dir_node = dir_node.unwrap(); // safe to use unwrap() here
                        if !dir_node.is_node_data_empty() {
                            debug!(
                                "remove_node_helper() cannot remove \
                                    the non-empty directory name={:?} of ino={} \
                                    under the parent directory of ino={}",
                                node_name, node_ino, parent,
                            );
                            reply.error(ENOTEMPTY).await?;
                            return Ok(());
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
            self.may_deferred_delete_node_helper(node_ino).await?;
            reply.ok().await?;
            Ok(())
        }
    }

    pub async fn new(full_mount_path: impl AsRef<Path>) -> anyhow::Result<FileSystem> {
        let root_path = full_mount_path.as_ref();
        let root_inode =
            Node::open_root_node(FUSE_ROOT_ID, OsString::from("/"), &root_path).await?;
        let mut cache = BTreeMap::new();
        cache.insert(FUSE_ROOT_ID, root_inode);
        let trash = BTreeSet::new(); // for deferred deletion

        Ok(FileSystem { cache, trash })
    }

    /// Initialize filesystem.
    /// Called before any other filesystem method.
    pub fn init(&mut self, _req: &Request<'_>) -> anyhow::Result<()> {
        Ok(())
    }

    /// Clean up filesystem.
    /// Called on filesystem exit.
    pub fn destroy(&mut self, _req: &Request<'_>) {}

    /// Look up a directory entry by name and get its attributes.
    pub async fn lookup(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: ReplyEntry,
    ) -> anyhow::Result<()> {
        let child_name = OsString::from(name);
        debug!(
            "lookup(parent={}, name={:?}, req={:?})",
            parent, child_name, req,
        );

        let ino: u64;
        let child_type: SFlag;
        {
            // lookup child ino and type first
            let parent_node = self.cache.get(&parent);
            debug_assert!(
                parent_node.is_some(),
                "lookup() found fs is inconsistent, \
                        the parent i-node of ino={} should be in cache",
                parent
            );
            let parent_node = parent_node.unwrap(); // safe to use unwrap() here
            match parent_node.get_entry(&child_name) {
                Some(child_entry) => {
                    ino = child_entry.ino();
                    child_type = child_entry.entry_type();
                }
                None => {
                    reply.error(ENOENT).await?;
                    debug!(
                        "lookup() failed to find the file name={:?} \
                            under parent directory of ino={}",
                        child_name, parent
                    );
                    // lookup() didn't find anything, this is normal
                    return Ok(());
                }
            }
        }

        let ttl = Duration::new(MY_TTL_SEC, 0);
        {
            // cache hit
            if let Some(node) = self.cache.get(&ino) {
                debug!(
                    "lookup() cache hit when searching file of \
                        name={:?} and ino={} under parent ino={}",
                    child_name, ino, parent,
                );
                let attr = node.lookup_attr();
                let fuse_attr = util::convert_to_fuse_attr(attr)?;
                reply.entry(ttl, fuse_attr, MY_GENERATION).await?;
                debug!(
                    "lookup() successfully found the file name={:?} of \
                        ino={} under parent ino={}, the attr={:?}",
                    child_name, ino, parent, &attr,
                );
                return Ok(());
            }
        }
        {
            // cache miss
            debug!(
                "lookup() cache missed when searching parent ino={}
                    and file name={:?} of ino={}",
                parent, child_name, ino,
            );
            let parent_node = self.cache.get_mut(&parent);
            debug_assert!(
                parent_node.is_some(),
                "lookup() found fs is inconsistent, \
                        parent i-node of ino={} should be in cache",
                parent,
            );
            let parent_node = parent_node.unwrap(); // safe to use unwrap() here
            let child_node = match child_type {
                SFlag::S_IFDIR => parent_node.open_child_dir(child_name).await?,
                SFlag::S_IFREG => {
                    let oflags = OFlag::O_RDWR;
                    parent_node.open_child_file(child_name, oflags).await?
                }
                _ => panic!("lookup() found unsupported file type={:?}", child_type),
            };

            let child_ino = child_node.get_ino();
            let attr = child_node.lookup_attr();
            self.cache.insert(child_ino, child_node);
            let fuse_attr = util::convert_to_fuse_attr(attr)?;
            reply.entry(ttl, fuse_attr, MY_GENERATION).await?;
            debug!(
                "lookup() successfully found the file name={:?} of ino={} \
                    under parent ino={}, the attr={:?}",
                name, ino, parent, &attr,
            );
            Ok(())
        }
    }

    /// Get file attributes.
    pub async fn getattr(
        &mut self,
        req: &Request<'_>,
        ino: INum,
        reply: ReplyAttr,
    ) -> anyhow::Result<()> {
        debug!("getattr(ino={}, req={:?})", ino, req);

        let node = self.cache.get(&ino);
        debug_assert!(
            node.is_some(),
            "getattr() found fs is inconsistent, \
                    the i-node of ino={} should be in cache",
            ino,
        );
        let node = node.unwrap(); // safe to use unwrap() here
        let attr = node.get_attr();
        debug!(
            "getattr() cache hit when searching the attribute of ino={}",
            ino,
        );
        let ttl = Duration::new(MY_TTL_SEC, 0);
        let fuse_attr = util::convert_to_fuse_attr(attr)?;
        reply.attr(ttl, fuse_attr).await?;
        debug!(
            "getattr() successfully got the attribute of ino={}, the attr={:?}",
            ino, attr,
        );
        Ok(())
    }

    /// Open a file.
    /// Open flags (with the exception of O_CREAT, O_EXCL, O_NOCTTY and O_TRUNC) are
    /// available in flags. Filesystem may store an arbitrary file handle (pointer, index,
    /// etc) in fh, and use this in other all other file operations (read, write, flush,
    /// release, fsync). Filesystem may also implement stateless file I/O and not store
    /// anything in fh. There are also some flags (direct_io, keep_cache) which the
    /// filesystem may set, to change the way the file is opened. See fuse_file_info
    /// structure in <fuse_common.h> for more details.
    pub async fn open(
        &mut self,
        req: &Request<'_>,
        ino: INum,
        flags: u32,
        reply: ReplyOpen,
    ) -> anyhow::Result<()> {
        debug!("open(ino={}, flags={}, req={:?})", ino, flags, req);

        let node = self.cache.get(&ino);
        debug_assert!(
            node.is_some(),
            "open() found fs is inconsistent, the i-node of ino={} should be in cache",
            ino,
        );
        let node = node.unwrap(); // safe to use unwrap() here
        let oflags = util::parse_oflag(flags);
        let new_fd = node.dup_fd(oflags).await?;
        reply.opened(new_fd as u64, flags).await?;
        debug!(
            "open() successfully duplicated the file handler of ino={}, fd={}, flags={:?}",
            ino, new_fd, flags,
        );
        Ok(())
    }

    /// Forget about an inode.
    /// The nlookup parameter indicates the number of lookups previously performed on
    /// this inode. If the filesystem implements inode lifetimes, it is recommended that
    /// inodes acquire a single reference on each lookup, and lose nlookup references on
    /// each forget. The filesystem may ignore forget calls, if the inodes don't need to
    /// have a limited lifetime. On unmount it is not guaranteed, that all referenced
    /// inodes will receive a forget message.
    pub fn forget(&mut self, req: &Request<'_>, ino: u64, nlookup: u64) {
        debug!("forget(ino={}, nlookup={}, req={:?})", ino, nlookup, req,);
        let current_count: i64;
        {
            let node = self.cache.get(&ino);
            debug_assert!(
                node.is_some(),
                "forget() found fs is inconsistent, \
                        the i-node of ino={} should be in cache",
                ino,
            );
            let node = node.unwrap(); // safe to use unwrap() here
            let previous_count = node.dec_lookup_count_by(nlookup);
            current_count = node.get_lookup_count();
            debug_assert!(current_count >= 0);
            debug_assert_eq!(previous_count - current_count, nlookup as i64); // assert no race forget
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
                    let deleted_node = self.cache.remove(&ino);
                    debug_assert!(
                        deleted_node.is_some(),
                        "forget() found fs is inconsistent, node of ino={} \
                                found in trash, but no i-node found for deferred deletion",
                        ino,
                    );
                    let deleted_node = deleted_node.unwrap(); // safe to use unwrap() here
                    self.trash.remove(&ino);
                    debug_assert_eq!(deleted_node.get_lookup_count(), 0);
                    debug!(
                        "forget() deferred deleted i-node of ino={}, the i-node={:?}",
                        ino, deleted_node
                    );
                }
            }
        }
    }

    /// Set file attributes.
    pub async fn setattr(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
        fh: Option<u64>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) -> anyhow::Result<()> {
        debug!(
            "setattr(ino={}, mode={:?}, uid={:?}, gid={:?}, size={:?}, \
                atime={:?}, mtime={:?}, fh={:?}, crtime={:?}, chgtime={:?}, \
                bkuptime={:?}, flags={:?}, req={:?})",
            ino, mode, uid, gid, size, atime, mtime, fh, crtime, chgtime, bkuptime, flags, req,
        );

        let node = self.cache.get_mut(&ino);
        debug_assert!(
            node.is_some(),
            "setattr() found fs is inconsistent, \
                    the i-node of ino={} should be in cache",
            ino,
        );
        let node = node.unwrap(); // safe to use unwrap() here
        let mut attr = node.get_attr();
        let ttl = Duration::new(MY_TTL_SEC, 0);
        let ts = SystemTime::now();

        if let Some(b) = mode {
            attr.perm = util::parse_mode_bits(b);
            debug!("setattr() set permission={}", attr.perm);

            let kind = util::parse_sflag(b);
            debug_assert_eq!(kind, attr.kind);
        }
        // no replace
        attr.uid = uid.unwrap_or(attr.uid);
        attr.gid = gid.unwrap_or(attr.gid);
        attr.size = size.unwrap_or(attr.size);
        attr.atime = atime.unwrap_or(attr.atime);
        attr.mtime = mtime.unwrap_or(attr.mtime);
        attr.crtime = crtime.unwrap_or(attr.crtime);
        attr.flags = flags.unwrap_or(attr.flags);

        if mode.is_some()
            || uid.is_some()
            || gid.is_some()
            || size.is_some()
            || atime.is_some()
            || mtime.is_some()
            || crtime.is_some()
            || chgtime.is_some()
            || bkuptime.is_some()
            || flags.is_some()
        {
            attr.ctime = ts; // update ctime, since meta data might change in setattr
            let fuse_attr = util::convert_to_fuse_attr(attr)?;
            node.set_attr(attr);
            reply.attr(ttl, fuse_attr).await?;
            debug!(
                "setattr() successfully set the attribute of ino={}, the set attr={:?}",
                ino, attr,
            );
            // TODO: write attribute change to disk using chmod, chown, chflags
            Ok(())
        } else {
            reply.error(ENODATA).await?;
            panic!(
                "setattr() found all the input attributes are empty for the file of ino={}",
                ino,
            );
            // Err(anyhow::anyhow!("no change to attr"))
        }
    }

    /// Read symbolic link.
    pub async fn readlink(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        reply: ReplyData,
    ) -> anyhow::Result<()> {
        reply.error(ENOSYS).await
    }

    /// Create file node.
    /// Create a regular file, character device, block device, fifo or socket node.
    pub async fn mknod(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) -> anyhow::Result<()> {
        debug!(
            "mknod(parent={}, name={:?}, mode={}, rdev={}, req={:?})",
            parent, name, mode, rdev, req,
        );

        self.create_node_helper(parent, name.into(), mode, SFlag::S_IFREG, reply)
            .await
    }

    /// Create a directory.
    pub async fn mkdir(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        reply: ReplyEntry,
    ) -> anyhow::Result<()> {
        debug!(
            "mkdir(parent={}, name={:?}, mode={}, req={:?})",
            parent, name, mode, req,
        );

        self.create_node_helper(parent, name.into(), mode, SFlag::S_IFDIR, reply)
            .await
    }

    /// Remove a file.
    pub async fn unlink(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: ReplyEmpty,
    ) -> anyhow::Result<()> {
        debug!("unlink(parent={}, name={:?}, req={:?}", parent, name, req,);
        self.remove_node_helper(parent, name.into(), SFlag::S_IFREG, reply)
            .await
    }

    /// Remove a directory.
    pub async fn rmdir(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: ReplyEmpty,
    ) -> anyhow::Result<()> {
        let dir_name = OsString::from(name);
        debug!(
            "rmdir(parent={}, name={:?}, req={:?})",
            parent, dir_name, req,
        );
        self.remove_node_helper(parent, dir_name, SFlag::S_IFDIR, reply)
            .await
    }

    /// Create a symbolic link.
    pub async fn symlink(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _link: &Path,
        reply: ReplyEntry,
    ) -> anyhow::Result<()> {
        reply.error(ENOSYS).await
    }

    /// Rename a file.
    pub async fn rename(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _newparent: u64,
        _newname: &OsStr,
        reply: ReplyEmpty,
    ) -> anyhow::Result<()> {
        reply.error(ENOSYS).await
    }

    /// Create a hard link.
    pub async fn link(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _newparent: u64,
        _newname: &OsStr,
        reply: ReplyEntry,
    ) -> anyhow::Result<()> {
        reply.error(ENOSYS).await
    }

    /// Read data.
    /// Read should send exactly the number of bytes requested except on EOF or error,
    /// otherwise the rest of the data will be substituted with zeroes. An exception to
    /// this is when the file has been opened in 'direct_io' mode, in which case the
    /// return value of the read system call will reflect the return value of this
    /// operation. fh will contain the value set by the open method, or will be undefined
    /// if the open method didn't set any value.
    pub async fn read(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        reply: ReplyData,
    ) -> anyhow::Result<()> {
        debug!(
            "read(ino={}, fh={}, offset={}, size={}, req={:?})",
            ino, fh, offset, size, req,
        );

        let read_helper = |content: &Vec<u8>| -> anyhow::Result<Vec<u8>> {
            match content.len().cmp(&(offset as usize)) {
                std::cmp::Ordering::Greater => {
                    let read_data = if ((offset + size as i64) as usize) <= content.len() {
                        debug!("read exact {} bytes", size);
                        &content[(offset as usize)..(offset + size as i64) as usize]
                    } else {
                        debug!(
                            "read {} bytes only, less than expected size={}",
                            ((offset + size as i64) as usize) - content.len(),
                            size,
                        );
                        &content[(offset as usize)..]
                    };
                    // TODO: consider zero copy
                    Ok(read_data.to_vec())
                }
                std::cmp::Ordering::Equal => {
                    debug!(
                        "offset={} equals file length={}, nothing to read",
                        offset,
                        content.len(),
                    );
                    Ok(Vec::new())
                }
                std::cmp::Ordering::Less => Err(anyhow::anyhow!(
                    "failed to read, offset={} beyond file length={}",
                    offset,
                    content.len(),
                )),
            }
        };

        let node = self.cache.get_mut(&ino);
        debug_assert!(
            node.is_some(),
            "read() found fs is inconsistent, the i-node of ino={} should be in cache",
            ino,
        );
        let node = node.unwrap(); // safe to use unwrap() here
        if node.need_load_file_data() {
            node.load_data().await?;
        }
        match node.read_file(read_helper) {
            Ok(read_data_vec) => {
                debug!(
                    "read() successfully from the file of ino={}, the read size={:?}",
                    ino,
                    read_data_vec.len(),
                );
                reply.data(read_data_vec).await?;
                Ok(())
            }
            Err(e) => {
                debug!(
                    "read() offset={} is beyond the length of the file of ino={}",
                    offset, ino
                );
                reply.error(EINVAL).await?;
                Err(e)
            }
        }
    }

    /// Write data.
    /// Write should return exactly the number of bytes requested except on error. An
    /// exception to this is when the file has been opened in 'direct_io' mode, in
    /// which case the return value of the write system call will reflect the return
    /// value of this operation. fh will contain the value set by the open method, or
    /// will be undefined if the open method didn't set any value.
    pub async fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: Vec<u8>,
        flags: u32,
        reply: ReplyWrite,
    ) -> anyhow::Result<()> {
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

        let inode = self.cache.get_mut(&ino);
        debug_assert!(
            inode.is_some(),
            "write() found fs is inconsistent, \
                    the i-node of ino={} should be in cache",
            ino,
        );
        let inode = inode.unwrap(); // safe to use unwrap() here
        let oflags = util::parse_oflag(flags);
        let write_to_disk = true;
        let data_len = data.len();
        let written_size = inode
            .write_file(fh, offset, data, oflags, write_to_disk)
            .await?;
        reply.written(written_size as u32).await?;
        debug!(
            "write() successfully wrote {} byte data to file ino={} at offset={}",
            data_len, ino, offset,
        );
        Ok(())
    }

    /// Flush method.
    /// This is called on each close() of the opened file. Since file descriptors can
    /// be duplicated (dup, dup2, fork), for one open call there may be many flush
    /// calls. Filesystems shouldn't assume that flush will always be called after some
    /// writes, or that if will be called at all. fh will contain the value set by the
    /// open method, or will be undefined if the open method didn't set any value.
    /// NOTE: the name of the method is misleading, since (unlike fsync) the filesystem
    /// is not forced to flush pending writes. One reason to flush data, is if the
    /// filesystem wants to return write errors. If the filesystem supports file locking
    /// operations (setlk, getlk) it should remove all locks belonging to 'lock_owner'.
    pub async fn flush(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        reply: ReplyEmpty,
    ) -> anyhow::Result<()> {
        debug!(
            "flush(ino={}, fh={}, lock_owner={}, req={:?})",
            ino, fh, lock_owner, req,
        );

        // This is called from every close on an open file, so call the
        // close on the underlying filesystem.	But since flush may be
        // called multiple times for an open file, this must not really
        // close the file.  This is important if used on a network
        // filesystem like NFS which flush the data/metadata on close()
        let new_fd = blocking!(unistd::dup(fh as RawFd)).context(format!(
            "flush() failed to duplicate the handler ino={} fh={:?}",
            ino, fh,
        ))?;
        blocking!(unistd::close(new_fd)).context(format!(
            "flush() failed to close the duplicated file handler={} of ino={}",
            new_fd, ino,
        ))?;
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
        ino: u64,
        fh: u64,
        flags: u32, // same as the open flags
        lock_owner: u64,
        flush: bool,
        reply: ReplyEmpty,
    ) {
        debug!(
            "release(ino={}, fh={}, flags={}, lock_owner={}, flush={}, req={:?})",
            ino, fh, flags, lock_owner, flush, req,
        );
        // TODO: handle lock_owner
        let node = self.cache.get(&ino);
        debug_assert!(
            node.is_some(),
            "release() found fs is inconsistent, \
                    the i-node of ino={} should be in cache",
            ino,
        );
        let node = node.unwrap(); // safe to use unwrap() here
        let fd = fh as RawFd;
        if flush {
            // TODO: double check the meaning of the flush flag
            blocking!(unistd::fsync(fd))
                .unwrap_or_else(|_| panic!("release() failed to flush the file of ino={}", ino));
        }
        blocking!(unistd::close(fd)).unwrap_or_else(|_| {
            panic!(
                "release() failed to close the file handler={} of ino={}",
                fh, ino
            )
        });
        node.dec_open_count(); // decrease open count before reply in case reply failed
        reply
            .ok()
            .await
            .expect("release() failed to send the FUSE reply");
        debug!(
            "release() successfully closed the file handler={} of ino={}",
            fh, ino,
        );
    }

    async fn fsync_helper(
        ino: u64,
        fh: u64,
        datasync: bool,
        reply: ReplyEmpty,
    ) -> anyhow::Result<()> {
        #[cfg(target_os = "linux")]
        {
            // attributes are not allowed on if expressions
            if datasync {
                blocking!(unistd::fdatasync(fh as RawFd)).context(format!(
                    "fsync_helper() failed to flush the node of ino={}",
                    ino
                ))?;
            } else {
                blocking!(unistd::fsync(fh as RawFd)).context(format!(
                    "fsync_helper() failed to flush the node of ino={}",
                    ino
                ))?;
            }
        }
        #[cfg(target_os = "macos")]
        {
            blocking!(unistd::fsync(fh as RawFd)).context(format!(
                "fsync_helper() failed to flush the node of ino={}",
                ino
            ))?;
        }

        reply.ok().await?;
        debug!(
            "fsync_helper() successfully sync the node of ino={}, fh={}, datasync={:?}",
            ino, fh, datasync,
        );
        Ok(())
    }

    /// Synchronize file contents.
    /// If the datasync parameter is non-zero, then only the user data should be flushed,
    /// not the meta data.
    pub async fn fsync(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        fh: u64,
        datasync: bool,
        reply: ReplyEmpty,
    ) -> anyhow::Result<()> {
        debug!(
            "fsync(ino={}, fh={}, datasync={}, req={:?})",
            ino, fh, datasync, req,
        );
        FileSystem::fsync_helper(ino, fh, datasync, reply).await
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
        ino: u64,
        flags: u32,
        reply: ReplyOpen,
    ) -> anyhow::Result<()> {
        debug!("opendir(ino={}, flags={}, req={:?})", ino, flags, req,);

        let node = self.cache.get(&ino);
        debug_assert!(
            node.is_some(),
            "opendir() found fs is inconsistent, the i-node of ino={} should be in cache",
            ino,
        );
        let node = node.unwrap(); // safe to use unwrap() here
        let oflags = util::parse_oflag(flags);
        let new_fd = node.dup_fd(oflags).await?;

        reply.opened(new_fd as u64, flags).await?;
        debug!(
            "opendir() successfully duplicated the file handler of ino={}, new fd={}, flags={:?}",
            ino, new_fd, oflags,
        );
        Ok(())
    }

    /// Read directory.
    /// Send a buffer filled using buffer.fill(), with size not exceeding the
    /// requested size. Send an empty buffer on end of stream. fh will contain the
    /// value set by the opendir method, or will be undefined if the opendir method
    /// didn't set any value.
    pub async fn readdir(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) -> anyhow::Result<()> {
        debug!(
            "readdir(ino={}, fh={}, offset={}, req={:?})",
            ino, fh, offset, req,
        );

        let readdir_helper = |data: &BTreeMap<OsString, DirEntry>| -> usize {
            let mut num_child_entries = 0;
            for (i, (child_name, child_entry)) in data.iter().enumerate().skip(offset as usize) {
                let child_ino = child_entry.ino();
                reply.add(
                    child_ino,
                    offset + i as i64 + 1, // i + 1 means the index of the next entry
                    child_entry.entry_type(),
                    child_name,
                );
                num_child_entries += 1;
                debug!(
                    "readdir() found one child name={:?} ino={} offset={} entry={:?} \
                        under the directory of ino={}",
                    child_name,
                    child_ino,
                    offset + i as i64 + 1,
                    child_entry,
                    ino,
                );
            }
            num_child_entries
        };

        let node = self.cache.get(&ino);
        debug_assert!(
            node.is_some(),
            "readdir() found fs is inconsistent, \
                    the i-node of ino={} should be in cache",
            ino,
        );
        let node = node.unwrap(); // safe to use unwrap() here
        let num_child_entries = node.read_dir(readdir_helper);
        reply.ok().await?;
        debug!(
            "readdir() successfully read {} children \
                under the directory of ino={}",
            num_child_entries, ino,
        );
        Ok(())
    }

    /// Release an open directory.
    /// For every opendir call there will be exactly one releasedir call. fh will
    /// contain the value set by the opendir method, or will be undefined if the
    /// opendir method didn't set any value.
    pub async fn releasedir(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        fh: u64,
        flags: u32,
        reply: ReplyEmpty,
    ) {
        debug!(
            "releasedir(ino={}, fh={}, flags={}, req={:?})",
            ino, fh, flags, req,
        );
        // TODO: handle flags
        let node = self.cache.get(&ino);
        debug_assert!(
            node.is_some(),
            "releasedir() found fs is inconsistent, \
                    the i-node of ino={} should be in cache",
            ino,
        );
        let node = node.unwrap(); // safe to use unwrap() here
        blocking!(unistd::close(fh as RawFd)).unwrap_or_else(|_| {
            panic!(
                "releasedir() failed to close the file handler={} of ino={}",
                fh, ino
            )
        });
        node.dec_open_count();
        reply
            .ok()
            .await
            .expect("releasedir() failed to send the FUSE reply");
        debug!(
            "releasedir() successfully closed the file handler={} of ino={}",
            fh, ino,
        );
    }

    /// Synchronize directory contents.
    /// If the datasync parameter is set, then only the directory contents should
    /// be flushed, not the meta data. fh will contain the value set by the opendir
    /// method, or will be undefined if the opendir method didn't set any value.
    pub async fn fsyncdir(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        fh: u64,
        datasync: bool,
        reply: ReplyEmpty,
    ) -> anyhow::Result<()> {
        debug!(
            "fsyncdir(ino={}, fh={}, datasync={}, req={:?})",
            ino, fh, datasync, req,
        );
        FileSystem::fsync_helper(ino, fh, datasync, reply).await
    }

    /// Get file system statistics.
    /// The 'f_favail', 'f_fsid' and 'f_flag' fields are ignored
    pub async fn statfs(
        &mut self,
        req: &Request<'_>,
        mut ino: u64,
        reply: ReplyStatFs,
    ) -> anyhow::Result<()> {
        debug!("statfs(ino={}, req={:?})", ino, req);

        if ino == 0 {
            ino = FUSE_ROOT_ID;
        }

        let node = self.cache.get(&ino);
        debug_assert!(
            node.is_some(),
            "statfs() found fs is inconsistent, \
                    the i-node of ino={} should be in cache",
            ino,
        );
        let node = node.unwrap(); // safe to use unwrap() here
        let fd = node.get_fd();
        let statvfs = blocking!(
            let file = unsafe { std::fs::File::from_raw_fd(fd) };
            let res = statvfs::fstatvfs(&file); // statvfs is POSIX, whereas statfs is not
            let _fd = file.into_raw_fd(); // prevent fd to be closed by File
            res
        )
        .context("statfs() failed to run statvfs()")?;
        // reply
        //     .statfs(
        //         // TODO: consider to avoid the numeric cast
        //         statvfs.blocks() as u64,
        //         statvfs.blocks_free() as u64,
        //         statvfs.blocks_available() as u64,
        //         statvfs.files() as u64,
        //         statvfs.files_free() as u64,
        //         statvfs.block_size() as u32, // TODO: consider use customized block size
        //         statvfs.name_max() as u32,
        //         statvfs.fragment_size() as u32,
        //     )
        //     .await?;
        reply.error(ENOSYS).await?;
        debug!(
            "statfs() successfully read the statvfs of ino={}, the statvfs={:?}",
            ino, statvfs,
        );
        Ok(())
    }

    /// Set an extended attribute.
    pub async fn setxattr(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _name: &OsStr,
        _value: &[u8],
        _flags: u32,
        _position: u32,
        reply: ReplyEmpty,
    ) -> anyhow::Result<()> {
        reply.error(ENOSYS).await
    }

    /// Get an extended attribute.
    /// If `size` is 0, the size of the value should be sent with `reply.size()`.
    /// If `size` is not 0, and the value fits, send it with `reply.data()`, or
    /// `reply.error(ERANGE)` if it doesn't.
    pub async fn getxattr(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _name: &OsStr,
        _size: u32,
        reply: ReplyXAttr,
    ) -> anyhow::Result<()> {
        reply.error(ENOSYS).await
    }

    /// List extended attribute names.
    /// If `size` is 0, the size of the value should be sent with `reply.size()`.
    /// If `size` is not 0, and the value fits, send it with `reply.data()`, or
    /// `reply.error(ERANGE)` if it doesn't.
    pub async fn listxattr(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _size: u32,
        reply: ReplyXAttr,
    ) -> anyhow::Result<()> {
        reply.error(ENOSYS).await
    }

    /// Remove an extended attribute.
    pub async fn removexattr(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _name: &OsStr,
        reply: ReplyEmpty,
    ) -> anyhow::Result<()> {
        reply.error(ENOSYS).await
    }

    /// Check file access permissions.
    /// This will be called for the access() system call. If the 'default_permissions'
    /// mount option is given, this method is not called. This method is not called
    /// under Linux kernel versions 2.4.x
    pub async fn access(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _mask: u32,
        reply: ReplyEmpty,
    ) -> anyhow::Result<()> {
        reply.error(ENOSYS).await
    }

    /// Create and open a file.
    /// If the file does not exist, first create it with the specified mode, and then
    /// open it. Open flags (with the exception of O_NOCTTY) are available in flags.
    /// Filesystem may store an arbitrary file handle (pointer, index, etc) in fh,
    /// and use this in other all other file operations (read, write, flush, release,
    /// fsync). There are also some flags (direct_io, keep_cache) which the
    /// filesystem may set, to change the way the file is opened. See fuse_file_info
    /// structure in <fuse_common.h> for more details. If this method is not
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
    ) -> anyhow::Result<()> {
        reply.error(ENOSYS).await
    }

    /// Test for a POSIX file lock.
    pub async fn getlk(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        _start: u64,
        _end: u64,
        _typ: u32,
        _pid: u32,
        reply: ReplyLock,
    ) -> anyhow::Result<()> {
        reply.error(ENOSYS).await
    }

    /// Acquire, modify or release a POSIX file lock.
    /// For POSIX threads (NPTL) there's a 1-1 relation between pid and owner, but
    /// otherwise this is not always the case.  For checking lock ownership,
    /// 'fi->owner' must be used. The l_pid field in 'struct flock' should only be
    /// used to fill in this field in getlk(). Note: if the locking methods are not
    /// implemented, the kernel will still allow file locking to work locally.
    /// Hence these are only interesting for network filesystems and similar.
    pub async fn setlk(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        _start: u64,
        _end: u64,
        _typ: u32,
        _pid: u32,
        _sleep: bool,
        reply: ReplyEmpty,
    ) -> anyhow::Result<()> {
        reply.error(ENOSYS).await
    }

    /// Map block index within file to block index within device.
    /// Note: This makes sense only for block device backed filesystems mounted
    /// with the 'blkdev' option
    pub async fn bmap(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _blocksize: u32,
        _idx: u64,
        reply: ReplyBMap,
    ) -> anyhow::Result<()> {
        reply.error(ENOSYS).await
    }

    /// macOS only: Rename the volume. Set fuse_init_out.flags during init to
    /// FUSE_VOL_RENAME to enable
    #[cfg(target_os = "macos")]
    pub async fn setvolname(
        &mut self,
        _req: &Request<'_>,
        _name: &OsStr,
        reply: ReplyEmpty,
    ) -> anyhow::Result<()> {
        reply.error(ENOSYS).await
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
    ) -> anyhow::Result<()> {
        reply.error(ENOSYS).await
    }

    /// macOS only: Query extended times (bkuptime and crtime). Set fuse_init_out.flags
    /// during init to FUSE_XTIMES to enable
    #[cfg(target_os = "macos")]
    pub async fn getxtimes(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        reply: ReplyXTimes,
    ) -> anyhow::Result<()> {
        reply.error(ENOSYS).await
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
