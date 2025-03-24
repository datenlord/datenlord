use std::fmt::Debug;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::metrics::FILESYSTEM_METRICS;
use async_trait::async_trait;
use clippy_utilities::Cast;
use libc::{RENAME_EXCHANGE, RENAME_NOREPLACE};
use nix::errno::Errno;
use nix::fcntl::OFlag;
use nix::sys::stat::SFlag;
use tokio::sync::Mutex;
use tracing::{debug, info, instrument, warn};

use super::id_alloc_used::INumAllocator;
use super::metadata::{error, MetaData, ReqContext};
use super::s3_node::S3Node;
use super::{check_type_supported, CreateParam, RenameParam, SetAttrParam, StorageType};
use crate::common::error::{
    Context as DatenLordContext, // conflict with anyhow::Context
    DatenLordResult,
};
use crate::fs::datenlordfs::check_name_length;
use crate::fs::datenlordfs::direntry::DirEntry;
use crate::fs::fs_util::{self, FileAttr, NEED_CHECK_PERM};
use crate::fs::fs_util::{build_error_result_from_errno, INum, StatFsParam, ROOT_ID};
use crate::fs::kv_engine::KeyType;
use crate::fs::kv_engine::ValueType;
use crate::fs::kv_engine::{KVEngine, KVEngineType, MetaTxn};
use crate::fs::node::Node;
use crate::new_storage::Storage;
use crate::{function_name, retry_txn};

/// A helper function to build [`DatenLordError::InconsistentFS`] with default
/// context and get the function name automatic.
macro_rules! build_inconsistent_fs {
    ($ino:expr) => {
        error::build_inconsistent_fs($ino, function_name!())
    };
}

/// The time-to-live seconds of FUSE attributes
pub const MY_TTL_SEC: u64 = 3600; // TODO: should be a long value, say 1 hour
/// The generation ID of FUSE attributes
const MY_GENERATION: u64 = 1; // TODO: find a proper way to set generation
/// The limit of transaction commit retrying times.
const TXN_RETRY_LIMIT: u32 = 10;

/// File system in-memory meta-data
#[derive(Debug)]
#[allow(dead_code)]
pub struct S3MetaData {
    /// Current available fd, it'll increase after using
    fd_allocator: AtomicU64,
    /// Current service id
    pub(crate) node_id: Arc<str>,
    /// Fuse fd
    fuse_fd: Mutex<RawFd>,
    /// KV engine
    pub(crate) kv_engine: Arc<KVEngineType>,
    /// Inum allocator
    inum_allocator: INumAllocator<KVEngineType>,
}

#[async_trait]
impl MetaData for S3MetaData {
    type N = S3Node;

    #[instrument(level = "debug", skip(self))]
    async fn release(
        &self,
        _ino: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> DatenLordResult<()> {
        Ok(())
    }

    #[instrument(level = "debug", skip(self), err, ret)]
    async fn readdir(
        &self,
        context: ReqContext,
        ino: u64,
        _fh: u64,
        _offset: i64,
    ) -> DatenLordResult<Vec<DirEntry>> {
        let inode = self
            .get_node_from_kv_engine(ino)
            .await?
            .ok_or_else(|| build_inconsistent_fs!(ino))?;
        inode.get_attr().check_perm(context.uid, context.gid, 5)?;

        match self.get_all_dir_entry(ino).await {
            Ok(dir_entries) => {
                debug!(
                    "metadata call readdir() get all dir entries, dir_entries={:?}",
                    dir_entries
                );
                Ok(dir_entries)
            }
            Err(err) => {
                warn!(
                    "metadata call readdir() failed to get all dir entries, err={}",
                    err
                );
                Err(err)
            }
        }
    }

    #[instrument(level = "debug", skip(self), err, ret)]
    async fn opendir(&self, context: ReqContext, ino: u64, flags: u32) -> DatenLordResult<u64> {
        match self.get_node_from_kv_engine(ino).await? {
            None => {
                warn!("metadata call opendir() failed to find ino={ino}");
                return build_error_result_from_errno(
                    Errno::ENOENT,
                    format!("opendir() failed to find ino={ino}"),
                );
            }
            Some(node) => {
                let o_flags = fs_util::parse_oflag(flags);
                node.open_pre_check(o_flags, context.uid, context.gid)?;
                debug!("metadata call opendir() ino={} success", ino);
                return Ok(self.allocate_fd());
            }
        }
    }

    #[instrument(level = "debug", skip(self))]
    async fn readlink(&self, ino: u64) -> DatenLordResult<Vec<u8>> {
        let node = self
            .get_node_from_kv_engine(ino)
            .await?
            .ok_or_else(|| build_inconsistent_fs!(ino))?;
        Ok(node.get_symlink_target().as_os_str().to_owned().into_vec())
    }

    #[instrument(level = "debug", skip(self), err, ret)]
    async fn statfs(&self, context: ReqContext, ino: u64) -> DatenLordResult<StatFsParam> {
        let node = self
            .get_node_from_kv_engine(ino)
            .await?
            .ok_or_else(|| build_inconsistent_fs!(ino))?;
        node.get_attr().check_perm(context.uid, context.gid, 5)?;
        node.statefs().await
    }

    #[instrument(level = "debug", skip(self))]
    async fn releasedir(&self, ino: u64, _fh: u64) -> DatenLordResult<()> {
        let inode = self.get_node_from_kv_engine(ino).await?;
        if inode.is_none() {
            return build_error_result_from_errno(
                Errno::ENOENT,
                format!("releasedir() failed to find ino={ino}"),
            );
        }
        Ok(())
    }

    #[instrument(level = "debug", skip(self), err, ret)]
    async fn open(
        &self,
        context: ReqContext,
        ino: u64,
        flags: u32,
    ) -> DatenLordResult<(u64, FileAttr)> {
        // TODO: handle open flags
        // <https://pubs.opengroup.org/onlinepubs/9699919799/functions/open.html>

        // Map open flags to OFlag, then parse it into `u8` for permission check
        let o_flags = fs_util::parse_oflag(flags);
        let access_mode = match o_flags & (OFlag::O_RDONLY | OFlag::O_WRONLY | OFlag::O_RDWR) {
            OFlag::O_RDONLY => 4,
            OFlag::O_WRONLY => 2,
            _ => 6,
        };

        // The file doesn't open by any process, so we need to open it
        match self.get_node_from_kv_engine(ino).await? {
            None => {
                return build_error_result_from_errno(
                    Errno::ENOENT,
                    format!("open() failed to find ino={ino}"),
                );
            }
            Some(node) => {
                let attr = node.get_attr();
                attr.check_perm(context.uid, context.gid, access_mode)?;
                return Ok((self.allocate_fd(), attr));
            }
        }
    }

    #[instrument(level = "debug", skip(self), err, ret)]
    async fn get_remote_attr(&self, ino: u64) -> DatenLordResult<(Duration, FileAttr)> {
        // If the file is not open, return the attr in kv engine
        let inode = self
            .get_node_from_kv_engine(ino)
            .await?
            .ok_or_else(|| build_inconsistent_fs!(ino))?;
        let attr = inode.get_attr();
        let ttl = Duration::new(MY_TTL_SEC, 0);
        Ok((ttl, attr))
    }

    #[instrument(level = "debug", skip(self, storage), err, ret)]
    async fn setattr_helper<M: MetaData + Send + Sync + 'static>(
        &self,
        context: ReqContext,
        ino: u64,
        param: &SetAttrParam,
        storage: &StorageType,
    ) -> DatenLordResult<(Duration, FileAttr)> {
        let ttl = Duration::new(MY_TTL_SEC, 0);
        let mut inode = self
            .get_node_from_kv_engine(ino)
            .await?
            .ok_or_else(|| build_inconsistent_fs!(ino))?;
        let mut old_attr = inode.get_attr();

        // If current file is open, try to update old_attr with local attr
        if let Ok(attr) = storage.getattr(ino).await {
            old_attr = attr;
        }

        let dirty_attr_for_reply =
            if let Some(dirty_attr) = old_attr.setattr_precheck(param, context.uid, context.gid)? {
                info!(
                    "setattr() ino={} new_attr={:?} old_attr={:?}",
                    ino, dirty_attr, old_attr
                );
                // Update attributes without size immediately
                let mut dirty_attr_without_size = dirty_attr;
                dirty_attr_without_size.size = old_attr.size;
                inode.set_attr(dirty_attr_without_size);
                // Update local attr with new attr without size
                storage.setattr(ino, dirty_attr_without_size).await;

                match self
                    .kv_engine
                    .set(
                        &KeyType::INum2Node(ino),
                        &ValueType::Node(inode.to_serial_node()),
                        None,
                    )
                    .await
                {
                    Ok(_) => {
                        debug!(
                            "setattr_helper() ino={} new_attr={:?} isok=true",
                            ino, dirty_attr
                        );
                    }
                    Err(e) => {
                        return build_error_result_from_errno(
                            Errno::EIO,
                            format!("setattr_helper() failed to set kv, err={e:?}"),
                        );
                    }
                }

                // Defer update size with filehandle
                if old_attr.size != dirty_attr.size {
                    // Make sure the file is open
                    storage.open(ino, dirty_attr_without_size).await;

                    // TODO: update truncate filehandle
                    storage
                        .truncate(
                            ino,
                            old_attr.size.cast(),
                            dirty_attr.size.cast(),
                            dirty_attr.version,
                        )
                        .await?;

                    // Update local attr with new size
                    storage.setattr(ino, dirty_attr).await;

                    storage.close(ino).await?;

                    debug!(
                        "update attr to local setattr() ino={} new_attr={:?} old_attr={:?}",
                        ino, dirty_attr, old_attr
                    );
                }

                // Update remote attr
                dirty_attr
            } else {
                // setattr did not change any attribute.
                debug!(
                    "no change setattr() ino={} new_attr={:?} old_attr={:?}",
                    ino, old_attr, old_attr
                );
                return Ok((ttl, old_attr));
            };

        Ok((ttl, dirty_attr_for_reply))
    }

    #[instrument(level = "debug", skip(self), err, ret)]
    async fn unlink(
        &self,
        context: ReqContext,
        parent: INum,
        name: &str,
        storage: &StorageType,
    ) -> DatenLordResult<Option<INum>> {
        let (res, retry) = retry_txn!(TXN_RETRY_LIMIT, {
            let mut txn = self.kv_engine.new_meta_txn().await;
            let mut parent_node = self.get_inode_from_txn(txn.as_mut(), parent).await?;
            let mut result = None;
            parent_node.check_is_dir()?;
            let child_entry = match self.try_get_dir_entry(txn.as_mut(), parent, name).await? {
                None => {
                    return build_error_result_from_errno(
                        Errno::ENOENT,
                        format!(
                            "failed to find child name={:?} \
                                under parent ino={} and name={:?}",
                            name,
                            parent,
                            parent_node.get_name(),
                        ),
                    );
                }
                Some(child_entry) => {
                    self.check_sticky_bit(&context, &parent_node, &child_entry, txn.as_mut())
                        .await?;
                    debug_assert_eq!(&name, &child_entry.name());
                    child_entry
                }
            };

            let child_ino = child_entry.ino();
            let child_node = self.get_inode_from_txn(txn.as_mut(), child_ino).await?;

            // If child is a directory, it must be empty
            if let SFlag::S_IFDIR = child_node.get_type() {
                let dir_entries = self.get_all_dir_entry(child_ino).await?;
                if !dir_entries.is_empty() {
                    return build_error_result_from_errno(
                        Errno::ENOTEMPTY,
                        format!(
                            "failed to unlink() a non-empty directory name={:?} \
                                under parent ino={} and name={:?}",
                            name,
                            parent,
                            parent_node.get_name(),
                        ),
                    );
                }
            }
            storage.remove(child_ino).await?;

            // Posix deferred deletion is for inode, not for dir entry
            // So we will remove the dir entry immediately
            txn.delete(&KeyType::DirEntryKey((parent, name.into())));
            parent_node.update_mtime_ctime_to_now();

            if let SFlag::S_IFREG = child_node.get_type() {
                result = Some(child_ino);
            }
            txn.delete(&KeyType::INum2Node(child_ino));
            txn.set(
                &KeyType::INum2Node(parent),
                &ValueType::Node(parent_node.to_serial_node()),
            );
            (txn.commit().await, result)
        });

        FILESYSTEM_METRICS.observe_storage_operation_throughput(retry, "unlink");
        res
    }

    async fn new(kv_engine: Arc<KVEngineType>, node_id: &str) -> DatenLordResult<Arc<Self>> {
        let meta = Arc::new(Self {
            fd_allocator: AtomicU64::new(4),
            node_id: Arc::<str>::from(node_id.to_owned()),
            fuse_fd: Mutex::new(-1_i32),
            inum_allocator: INumAllocator::new(Arc::clone(&kv_engine)),
            kv_engine,
        });

        let (res, _) = retry_txn!(TXN_RETRY_LIMIT, {
            let mut txn = meta.kv_engine.new_meta_txn().await;
            let prev = meta.try_get_inode_from_txn(txn.as_mut(), ROOT_ID).await?;
            if let Some(prev_root_node) = prev {
                info!(
                    "[init] root node already exists root_node file_attr {:?}, skip init",
                    prev_root_node.get_attr()
                );
                // We already see a prev root node, we don't have write operation
                // Txn is not needed for such read-only operation
                (Ok(true), ())
            } else {
                info!("[init] root node not exists, init root node");
                let root_inode = S3Node::open_root_node(ROOT_ID, "/", Arc::clone(&meta))
                    .await
                    .add_context("failed to open FUSE root node")?;
                // insert (ROOT_ID -> root_inode) into KV engine
                txn.set(
                    &KeyType::INum2Node(ROOT_ID),
                    &ValueType::Node(root_inode.to_serial_node()),
                );
                (txn.commit().await, ())
            }
        });
        res?;

        Ok(meta)
    }

    /// Set fuse fd into `MetaData`
    #[tracing::instrument(skip(self))]
    async fn set_fuse_fd(&self, fuse_fd: RawFd) {
        *self.fuse_fd.lock().await = fuse_fd;
    }

    #[instrument(level = "debug", skip(self), err, ret)]
    // Create a file, but do not open it. They are two separate steps.
    // If the file does not exist, first create it with
    // the specified mode, and then open it.
    #[allow(clippy::too_many_lines)]
    async fn mknod(&self, param: CreateParam) -> DatenLordResult<(Duration, FileAttr, u64)> {
        check_name_length(&param.name)?;
        check_type_supported(&param.node_type)?;
        let parent_ino = param.parent;
        let (res, retry) = retry_txn!(TXN_RETRY_LIMIT, {
            let mut txn = self.kv_engine.new_meta_txn().await;
            let mut parent_node = self.get_inode_from_txn(txn.as_mut(), parent_ino).await?;

            if self
                .try_get_dir_entry(txn.as_mut(), parent_ino, &param.name)
                .await?
                .is_some()
            {
                return build_error_result_from_errno(
                    Errno::EEXIST,
                    format!(
                        "failed to create file name={:?} under parent ino={} and name={:?}",
                        param.name,
                        parent_ino,
                        parent_node.get_name(),
                    ),
                );
            }

            let new_num = self.alloc_inum().await?;

            let new_node = parent_node
                .create_child_node(&param, new_num, txn.as_mut())
                .await?;
            let current_attr = new_node.get_attr();
            let ttl = Duration::new(MY_TTL_SEC, 0);
            txn.set(
                &KeyType::INum2Node(new_num),
                &ValueType::Node(new_node.to_serial_node()),
            );
            txn.set(
                &KeyType::INum2Node(parent_ino),
                &ValueType::Node(parent_node.to_serial_node()),
            );
            (txn.commit().await, (ttl, current_attr))
        });
        FILESYSTEM_METRICS.observe_storage_operation_throughput(retry, "mknod");
        let (ttl, current_attr) = res?;

        Ok((ttl, current_attr, MY_GENERATION))
    }

    #[instrument(level = "debug", skip(self), err, ret)]
    /// Helper function to lookup
    #[allow(clippy::too_many_lines)]
    async fn lookup_helper(
        &self,
        context: ReqContext,
        parent: INum,
        child_name: &str,
    ) -> DatenLordResult<(Duration, FileAttr, u64)> {
        let (res, retry) = retry_txn!(TXN_RETRY_LIMIT, {
            let mut txn = self.kv_engine.new_meta_txn().await;
            if NEED_CHECK_PERM {
                let parent_node = self.get_inode_from_txn(txn.as_mut(), parent).await?;
                parent_node
                    .get_attr()
                    .check_perm(context.uid, context.gid, 1)?;
            }

            let Some(child_entry) = self
                .try_get_dir_entry(txn.as_mut(), parent, child_name)
                .await?
            else {
                return build_error_result_from_errno(
                    Errno::ENOENT,
                    format!("failed to find child name={child_name:?} under parent ino={parent} "),
                );
            };

            let child_ino = child_entry.ino();
            let child_node = self.get_inode_from_txn(txn.as_mut(), child_ino).await?;
            let child_attr = child_node.get_attr();

            let ttl = Duration::new(MY_TTL_SEC, 0);
            txn.set(
                &KeyType::INum2Node(child_ino),
                &ValueType::Node(child_node.to_serial_node()),
            );
            (txn.commit().await, (ttl, child_attr, MY_GENERATION))
        });

        FILESYSTEM_METRICS.observe_storage_operation_throughput(retry, "lookup");
        res
    }

    #[allow(clippy::too_many_lines)] // TODO: refactor it into smaller functions
    #[instrument(level = "debug", skip(self), err, ret)]
    async fn rename(&self, context: ReqContext, param: RenameParam) -> DatenLordResult<()> {
        let old_parent = param.old_parent;
        let old_name = param.old_name.as_str();
        let new_parent = param.new_parent;
        let new_name = param.new_name.as_str();
        let flags = param.flags;
        // TODO: replace the new_node should delete its related data
        // TODO: we should also rename the node's name and parent_ino, not only the dir
        // entry
        let exchange = match flags {
            0 | RENAME_NOREPLACE => false,
            RENAME_EXCHANGE => true,
            _ => {
                return build_error_result_from_errno(
                    Errno::EINVAL,
                    format!("rename(): flags={flags} is not supported"),
                )
            }
        };

        if old_parent == new_parent && old_name == new_name {
            return Ok(());
        }

        let build_enoent = |name: &str, parent: INum| {
            build_error_result_from_errno(
                Errno::ENOENT,
                format!(
                    "exchange_pre_check() failed to find child entry of name={name:?} \
                        under parent directory ino={parent}"
                ),
            )
        };

        let (res, retry) = retry_txn!(TXN_RETRY_LIMIT, {
            let mut txn = self.kv_engine.new_meta_txn().await;
            let old_parent_node = Arc::new(Mutex::new(
                self.get_inode_from_txn(txn.as_mut(), old_parent).await?,
            ));
            let old_entry = {
                let old_parent_node = old_parent_node.lock().await;
                match self
                    .try_get_dir_entry(txn.as_mut(), old_parent, old_name)
                    .await?
                {
                    None => {
                        return build_enoent(old_name, old_parent);
                    }
                    Some(old_entry) => {
                        self.check_sticky_bit(&context, &old_parent_node, &old_entry, txn.as_mut())
                            .await?;
                        old_entry
                    }
                }
            };

            let new_parent_node = if old_parent == new_parent {
                Arc::clone(&old_parent_node)
            } else {
                Arc::new(Mutex::new(
                    self.get_inode_from_txn(txn.as_mut(), new_parent).await?,
                ))
            };

            let new_entry = self
                .try_get_dir_entry(txn.as_mut(), new_parent, new_name)
                .await?;
            match new_entry {
                None => {
                    // new_name does not exist under new_parent
                    if exchange {
                        // exchange is true, new name must exist
                        return build_enoent(new_name, new_parent);
                    }
                    // exchange is false, so we can do rename directly
                    // Remove from old_parent and insert into new_parent
                    txn.delete(&KeyType::DirEntryKey((old_parent, old_name.into())));
                    txn.set(
                        &KeyType::DirEntryKey((new_parent, new_name.into())),
                        &ValueType::DirEntry(DirEntry::new(
                            old_entry.ino(),
                            new_name.into(),
                            old_entry.file_type(),
                        )),
                    );
                }
                Some(new_entry) => {
                    // new_name exists under new_parent
                    if exchange {
                        // old_name -> new_entry
                        // new_name -> old_entry
                        txn.set(
                            &KeyType::DirEntryKey((old_parent, old_name.into())),
                            &ValueType::DirEntry(DirEntry::new(
                                new_entry.ino(),
                                old_name.into(),
                                new_entry.file_type(),
                            )),
                        );
                        txn.set(
                            &KeyType::DirEntryKey((new_parent, new_name.into())),
                            &ValueType::DirEntry(DirEntry::new(
                                old_entry.ino(),
                                new_name.into(),
                                old_entry.file_type(),
                            )),
                        );
                    } else {
                        // exchange is false, replace or no_replace
                        if flags & RENAME_NOREPLACE != 0 {
                            return build_error_result_from_errno(
                                Errno::EEXIST,
                                format!(
                                    "rename(): failed to rename() \
                                        because new_name={new_name:?} already exists",
                                ),
                            );
                        }
                        let delete_key = KeyType::DirEntryKey((old_parent, old_name.into()));
                        txn.delete(&delete_key);
                        txn.set(
                            &KeyType::DirEntryKey((new_parent, new_name.into())),
                            &ValueType::DirEntry(DirEntry::new(
                                old_entry.ino(),
                                new_name.into(),
                                old_entry.file_type(),
                            )),
                        );
                    }
                }
            };
            {
                old_parent_node.lock().await.update_mtime_ctime_to_now();
            }
            {
                new_parent_node.lock().await.update_mtime_ctime_to_now();
            }

            if old_parent == new_parent {
                txn.set(
                    &KeyType::INum2Node(old_parent),
                    &ValueType::Node(old_parent_node.lock().await.to_serial_node()),
                );
            } else {
                txn.set(
                    &KeyType::INum2Node(old_parent),
                    &ValueType::Node(old_parent_node.lock().await.to_serial_node()),
                );
                txn.set(
                    &KeyType::INum2Node(new_parent),
                    &ValueType::Node(new_parent_node.lock().await.to_serial_node()),
                );
            }

            (txn.commit().await, ())
        });

        FILESYSTEM_METRICS.observe_storage_operation_throughput(retry, "rename");
        res
    }

    /// Helper function to write data
    /// Helper function to write remote meta data
    #[instrument(level = "debug", skip(self), err, ret)]
    async fn write_remote_size_and_version_helper(
        &self,
        ino: u64,
        size: u64,
        version: u64,
    ) -> DatenLordResult<FileAttr> {
        debug!(
            "write_remote_size_and_version_helper() ino={} size={}",
            ino, size
        );
        let mut node = self
            .get_node_from_kv_engine(ino)
            .await?
            .ok_or_else(|| build_inconsistent_fs!(ino))?;
        node.update_mtime_ctime_to_now();
        let mut attr = node.get_attr();
        attr.size = size;
        attr.version = version;
        node.set_attr(attr);
        match self
            .kv_engine
            .set(
                &KeyType::INum2Node(ino),
                &ValueType::Node(node.to_serial_node()),
                None,
            )
            .await
        {
            Ok(_) => {
                debug!(
                    "write_remote_size_and_version_helper() ino={} size={} isok={:?}",
                    ino, size, true
                );
                Ok(attr)
            }
            Err(e) => {
                return build_error_result_from_errno(
                    Errno::EIO,
                    format!("write_remote_size_and_version_helper() failed to set kv, err={e:?}"),
                );
            }
        }
    }
}

impl S3MetaData {
    /// Allocate an fd
    fn allocate_fd(&self) -> u64 {
        self.fd_allocator.fetch_add(1, Ordering::SeqCst)
    }

    #[allow(clippy::unwrap_used)]
    /// Get a node from kv engine by inum
    pub async fn get_node_from_kv_engine(&self, inum: INum) -> DatenLordResult<Option<S3Node>> {
        let inum_key = KeyType::INum2Node(inum);
        let raw_data = self.kv_engine.get(&inum_key).await.add_context(format!(
            "{}() failed to get node of ino={inum} from kv engine",
            function_name!()
        ))?;

        // deserialize node
        Ok(raw_data.map(|value| value.into_s3_node(self)))
    }

    /// Allocate a new uinque inum for new node
    async fn alloc_inum(&self) -> DatenLordResult<INum> {
        let result = self.inum_allocator.alloc_inum_for_fnode().await;
        debug!("alloc_inum_for_fnode() result={result:?}");
        result
    }

    /// If sticky bit is set, only the owner of the directory, the owner of the
    /// file, or the superuser can rename or delete files.
    async fn check_sticky_bit<T: MetaTxn + ?Sized>(
        &self,
        context: &ReqContext,
        parent_node: &S3Node,
        child_entry: &DirEntry,
        txn: &mut T,
    ) -> DatenLordResult<()> {
        if !NEED_CHECK_PERM {
            return Ok(());
        }
        let parent_attr = parent_node.get_attr();
        let child_ino = child_entry.ino();
        let child_node = self.get_inode_from_txn(txn, child_ino).await?;
        let child_attr = child_node.get_attr();

        if NEED_CHECK_PERM
            && context.uid != 0
            && (parent_attr.perm & 0o1000 != 0)
            && context.uid != parent_attr.uid
            && context.uid != child_attr.uid
        {
            build_error_result_from_errno(Errno::EACCES, "Sticky bit set".to_owned())
        } else {
            Ok(())
        }
    }

    /// Helper function to get inode from `MetaTxn`
    async fn try_get_inode_from_txn<T: MetaTxn + ?Sized>(
        &self,
        txn: &mut T,
        ino: INum,
    ) -> DatenLordResult<Option<S3Node>> {
        let inode = txn
            .get(&KeyType::INum2Node(ino))
            .await
            .add_context(format!(
                "{}() failed to get i-node of ino={ino} from kv engine",
                function_name!()
            ))?;
        match inode {
            Some(inode) => Ok(Some(inode.into_s3_node(self))),
            None => Ok(None),
        }
    }

    /// Helper function to get inode that must exist from `MetaTxn`
    async fn get_inode_from_txn<T: MetaTxn + ?Sized>(
        &self,
        txn: &mut T,
        ino: INum,
    ) -> DatenLordResult<S3Node> {
        Ok(txn
            .get(&KeyType::INum2Node(ino))
            .await
            .add_context(format!(
                "{}() failed to get i-node of ino={ino} from kv engine",
                function_name!()
            ))?
            .ok_or_else(|| build_inconsistent_fs!(ino))? // inode must exist
            .into_s3_node(self))
    }

    /// Helper function to get dir entry from `MetaTxn`
    async fn try_get_dir_entry<T: MetaTxn + ?Sized>(
        &self,
        txn: &mut T,
        parent: INum,
        name: &str,
    ) -> DatenLordResult<Option<DirEntry>> {
        let key = KeyType::DirEntryKey((parent, name.to_owned()));
        let value = txn.get(&key).await.add_context(format!(
            "{}() failed to get dir entry of name={:?} \
                    under parent ino={} from kv engine",
            function_name!(),
            name,
            parent,
        ))?;
        match value {
            Some(value) => Ok(Some(value.into_dir_entry())),
            None => Ok(None),
        }
    }

    /// Helper function to get all dir etnry in a directory from `KVEngine`
    async fn get_all_dir_entry(&self, parent: INum) -> DatenLordResult<Vec<DirEntry>> {
        let key = KeyType::DirEntryKey((parent, String::new()));
        let raw_values = self.kv_engine.range(&key).await?;
        let mut values = Vec::with_capacity(raw_values.len());
        for raw_value in raw_values {
            values.push(raw_value.into_dir_entry());
        }
        Ok(values)
    }
}
