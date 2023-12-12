use std::fmt::Debug;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::io::RawFd;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Context;
use async_trait::async_trait;
use clippy_utilities::{Cast, OverflowArithmetic};
use datenlord::config::{StorageConfig, StorageS3Config};
use libc::{RENAME_EXCHANGE, RENAME_NOREPLACE};
use lockfree_cuckoohash::{pin, LockFreeCuckooHash as HashMap};
use nix::errno::Errno;
use nix::sys::stat::SFlag;
use tokio::sync::Mutex;
use tracing::{debug, info, instrument, warn};

use super::cache::policy::LruPolicy;
use super::cache::{Backend, Block, BlockCoordinate, GlobalCache, MemoryCache, StorageManager};
use super::fs_util::{self, FileAttr, NEED_CHECK_PERM};
use super::id_alloc_used::INumAllocator;
use super::kv_engine::{KVEngine, KVEngineType, MetaTxn, ValueType};
use super::metadata::{error, MetaData, ReqContext};
use super::node::Node;
use super::s3_node::S3Node;
use super::s3_wrapper::S3BackEnd;
use super::{check_type_supported, CreateParam, RenameParam, SetAttrParam};
use crate::async_fuse::fuse::fuse_reply::{ReplyDirectory, StatFsParam};
use crate::async_fuse::fuse::protocol::{FuseAttr, INum, FUSE_ROOT_ID};
use crate::async_fuse::memfs::check_name_length;
use crate::async_fuse::memfs::direntry::DirEntry;
use crate::async_fuse::memfs::kv_engine::KeyType;
use crate::async_fuse::util::build_error_result_from_errno;
use crate::common::error::{
    Context as DatenLordContext, // conflict with anyhow::Context
    DatenLordResult,
};
use crate::function_name;

/// A helper function to build [`DatenLordError::InconsistentFS`] with default
/// context and get the function name automatic.
macro_rules! build_inconsistent_fs {
    ($ino:expr) => {
        error::build_inconsistent_fs($ino, function_name!())
    };
}

/// The time-to-live seconds of FUSE attributes
const MY_TTL_SEC: u64 = 3600; // TODO: should be a long value, say 1 hour
/// The generation ID of FUSE attributes
const MY_GENERATION: u64 = 1; // TODO: find a proper way to set generation
#[allow(dead_code)]
/// The limit of transaction commit retrying times.
const TXN_RETRY_LIMIT: u32 = 10;

/// File system in-memory meta-data
#[derive(Debug)]
#[allow(dead_code)]
pub struct S3MetaData<S: S3BackEnd + Send + Sync + 'static> {
    /// S3 backend
    pub(crate) s3_backend: Arc<S>,
    /// Global data cache
    pub(crate) data_cache: Arc<GlobalCache>,
    /// Storage manager
    pub(crate) storage: Arc<StorageManager<<Self as MetaData>::St>>,
    /// Current available fd, it'll increase after using
    pub(crate) cur_fd: AtomicU32,
    /// Current service id
    pub(crate) node_id: Arc<str>,
    /// Storage config
    pub(crate) storage_config: Arc<StorageConfig>,
    /// Fuse fd
    fuse_fd: Mutex<RawFd>,
    /// KV engine
    pub(crate) kv_engine: Arc<KVEngineType>,
    /// Inum allocator
    inum_allocator: INumAllocator<KVEngineType>,
    /// Mtime and size cache in local
    local_mtime_and_size: HashMap<INum, (SystemTime, u64)>,
}

#[async_trait]
impl<S: S3BackEnd + Sync + Send + 'static> MetaData for S3MetaData<S> {
    type N = S3Node<S>;
    type St = Arc<MemoryCache<LruPolicy<BlockCoordinate>, Backend>>;

    #[instrument(skip(self))]
    async fn release(
        &self,
        ino: u64,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        flush: bool,
    ) -> DatenLordResult<()> {
        if flush {
            self.flush(ino, fh).await?;
        }
        retry_txn!(TXN_RETRY_LIMIT, {
            let mut txn = self.kv_engine.new_meta_txn().await;
            let mut inode = self.get_inode_from_txn(txn.as_mut(), ino).await?;
            inode.close().await;
            txn.set(
                &KeyType::INum2Node(ino),
                &ValueType::Node(inode.to_serial_node()),
            );
            (txn.commit().await, ())
        })?;
        Ok(())
    }

    #[instrument(skip(self), err, ret)]
    async fn readdir(
        &self,
        context: ReqContext,
        ino: u64,
        _fh: u64,
        offset: i64,
        reply: &mut ReplyDirectory,
    ) -> DatenLordResult<()> {
        let inode = self
            .get_node_from_kv_engine(ino)
            .await?
            .ok_or_else(|| build_inconsistent_fs!(ino))?;
        inode
            .get_attr()
            .check_perm(context.user_id, context.group_id, 5)?;

        let dir_entries = self.get_all_dir_entry(ino).await?;
        for (i, dir_etnry) in dir_entries.iter().enumerate().skip(offset.cast()) {
            reply.add(
                dir_etnry.ino(),
                offset.overflow_add(i.cast()).overflow_add(1), /* i + 1 means the index of
                                                                * the next entry */
                dir_etnry.file_type().into(),
                dir_etnry.name(),
            );
        }
        info!(
            "readdir() ino={} offset={} child_size={} reply={:?}",
            ino,
            offset,
            dir_entries.len(),
            reply
        );

        Ok(())
    }

    #[instrument(skip(self), err, ret)]
    async fn opendir(&self, context: ReqContext, ino: u64, flags: u32) -> DatenLordResult<RawFd> {
        let result = retry_txn!(TXN_RETRY_LIMIT, {
            let mut txn = self.kv_engine.new_meta_txn().await;
            let node = self.get_inode_from_txn(txn.as_mut(), ino).await?;
            let o_flags = fs_util::parse_oflag(flags);
            node.open_pre_check(o_flags, context.user_id, context.group_id)?;

            let result = node.dup_fd(o_flags).await?;
            txn.set(
                &KeyType::INum2Node(ino),
                &ValueType::Node(node.to_serial_node()),
            );
            (txn.commit().await, result)
        })?;
        Ok(result)
    }

    #[instrument(skip(self))]
    async fn readlink(&self, ino: u64) -> DatenLordResult<Vec<u8>> {
        let node = self
            .get_node_from_kv_engine(ino)
            .await?
            .ok_or_else(|| build_inconsistent_fs!(ino))?;
        Ok(node.get_symlink_target().as_os_str().to_owned().into_vec())
    }

    #[instrument(skip(self), err, ret)]
    async fn statfs(&self, context: ReqContext, ino: u64) -> DatenLordResult<StatFsParam> {
        let node = self
            .get_node_from_kv_engine(ino)
            .await?
            .ok_or_else(|| build_inconsistent_fs!(ino))?;
        node.get_attr()
            .check_perm(context.user_id, context.group_id, 5)?;
        node.statefs().await
    }

    #[instrument(skip(self))]
    async fn flush(&self, ino: u64, _fh: u64) -> DatenLordResult<()> {
        let local_mtime_and_size = {
            let guard = pin();

            self.local_mtime_and_size
                .remove_with_guard(&ino, &guard)
                .copied()
        };

        retry_txn!(TXN_RETRY_LIMIT, {
            let mut txn = self.kv_engine.new_meta_txn().await;
            let mut inode = self.get_inode_from_txn(txn.as_mut(), ino).await?;

            if inode.is_deferred_deletion() {
                // If a file is marked as deferred delete, there is no need to flush it.
                // But the local node may continue to access this file, which depends on
                // the local mtime and local size. Therefore, we need to insert the removed
                // local mtime and local size back to the cache.
                if let Some(local_mtime_and_size) = local_mtime_and_size {
                    self.local_mtime_and_size.insert(ino, local_mtime_and_size);
                }
                return Ok(());
            }

            // Flush the storage cache
            self.storage.flush(ino).await;

            let mut file_attr = inode.get_attr();

            if let Some((local_mtime, local_size)) = local_mtime_and_size {
                file_attr.mtime = local_mtime;
                file_attr.size = local_size;

                // `ctime` may be greater than `mtime`, use the greater one
                file_attr.ctime = local_mtime.max(file_attr.ctime);
            }

            inode.set_attr(file_attr);

            txn.set(
                &KeyType::INum2Node(ino),
                &ValueType::Node(inode.to_serial_node()),
            );
            (txn.commit().await, ())
        })?;
        Ok(())
    }

    #[instrument(skip(self))]
    async fn releasedir(&self, ino: u64, _fh: u64) -> DatenLordResult<()> {
        retry_txn!(TXN_RETRY_LIMIT, {
            let mut txn = self.kv_engine.new_meta_txn().await;
            let node = self.get_inode_from_txn(txn.as_mut(), ino).await?;
            node.closedir().await;
            let is_deleted = self.delete_check(&node).await?;
            if is_deleted {
                txn.delete(&KeyType::INum2Node(ino));
            } else {
                txn.set(
                    &KeyType::INum2Node(ino),
                    &ValueType::Node(node.to_serial_node()),
                );
            }
            (txn.commit().await, ())
        })?;
        Ok(())
    }

    #[instrument(skip(self), err, ret)]
    async fn read_helper(
        &self,
        ino: INum,
        _fh: u64,
        offset: i64,
        size: u32,
    ) -> DatenLordResult<Vec<Block>> {
        let inode = self
            .get_node_from_kv_engine(ino)
            .await?
            .ok_or_else(|| build_inconsistent_fs!(ino))?;

        let (mtime, file_size) = {
            let local_mtime_and_size = self.get_local_mtime_and_size(ino);
            let file_attr = inode.get_attr();
            local_mtime_and_size.unwrap_or((file_attr.mtime, file_attr.size))
        };

        if offset.cast::<u64>() >= file_size {
            return Ok(vec![]);
        }

        // Ensure `offset + size` is le than the size of the file
        let read_size: u64 = if offset.cast::<u64>().overflow_add(size.cast::<u64>()) > file_size {
            file_size.overflow_sub(offset.cast::<u64>())
        } else {
            size.cast()
        };

        let data = self
            .storage
            .load(ino, offset.cast(), read_size.cast(), mtime)
            .await;

        Ok(data)
    }

    #[instrument(skip(self), err, ret)]
    async fn open(&self, context: ReqContext, ino: u64, flags: u32) -> DatenLordResult<RawFd> {
        // TODO: handle open flags
        // <https://pubs.opengroup.org/onlinepubs/9699919799/functions/open.html>
        // let open_res = if let SFlag::S_IFLNK = node.get_type() {
        //     node.open_symlink_target(o_flags).await.add_context(format!(
        //         "open() failed to open symlink target={:?} with flags={}",
        //         node.get_symlink_target(),
        //         flags,
        //     ))
        // } else {
        retry_txn!(TXN_RETRY_LIMIT, {
            let mut txn = self.kv_engine.new_meta_txn().await;
            let node = self.get_inode_from_txn(txn.as_mut(), ino).await?;
            let o_flags = fs_util::parse_oflag(flags);
            node.open_pre_check(o_flags, context.user_id, context.group_id)?;

            let result = node.dup_fd(o_flags).await;
            txn.set(
                &KeyType::INum2Node(ino),
                &ValueType::Node(node.to_serial_node()),
            );
            (txn.commit().await, result)
        })?
    }

    #[instrument(skip(self), err, ret)]
    async fn getattr(&self, ino: u64) -> DatenLordResult<(Duration, FuseAttr)> {
        let inode = self
            .get_node_from_kv_engine(ino)
            .await?
            .ok_or_else(|| build_inconsistent_fs!(ino))?;
        let attr = self.apply_local_mtime_and_size(inode.get_attr());
        let ttl = Duration::new(MY_TTL_SEC, 0);
        let fuse_attr = fs_util::convert_to_fuse_attr(attr);
        Ok((ttl, fuse_attr))
    }

    #[instrument(skip(self))]
    async fn forget(&self, ino: u64, nlookup: u64) -> DatenLordResult<()> {
        retry_txn!(TXN_RETRY_LIMIT, {
            let mut txn = self.kv_engine.new_meta_txn().await;
            let inode = self.get_inode_from_txn(txn.as_mut(), ino).await?;
            inode.dec_lookup_count_by(nlookup);
            let is_deleted = self.delete_check(&inode).await?;
            if is_deleted {
                txn.delete(&KeyType::INum2Node(ino));
            } else {
                txn.set(
                    &KeyType::INum2Node(ino),
                    &ValueType::Node(inode.to_serial_node()),
                );
            }
            (txn.commit().await, ())
        })?;
        Ok(())
    }

    #[instrument(skip(self), err, ret)]
    async fn setattr_helper(
        &self,
        context: ReqContext,
        ino: u64,
        param: &SetAttrParam,
    ) -> DatenLordResult<(Duration, FuseAttr)> {
        let ttl = Duration::new(MY_TTL_SEC, 0);
        let result = retry_txn!(TXN_RETRY_LIMIT, {
            let mut txn = self.kv_engine.new_meta_txn().await;
            let mut inode = self.get_inode_from_txn(txn.as_mut(), ino).await?;

            let origin_attr = inode.get_attr();
            let applied_attr = if SFlag::S_IFREG == origin_attr.kind {
                self.apply_local_mtime_and_size(origin_attr)
            } else {
                origin_attr
            };

            let dirty_attr_for_reply = match applied_attr.setattr_precheck(
                param,
                context.user_id,
                context.group_id,
            )? {
                Some(mut dirty_attr) => {
                    // This is to be inserted in KV, whose `mtime` and `size` should be reset to
                    // origin, because changes of this field are invisible to other
                    // nodes until flush.
                    let mut dirty_attr_for_kv = dirty_attr;

                    // There are `mtime` and `size` caches for regular files. Check the change of
                    // these fields, and set them to the cache.
                    if SFlag::S_IFREG == applied_attr.kind {
                        if (dirty_attr.mtime, dirty_attr.size)
                            != (applied_attr.mtime, applied_attr.size)
                        {
                            self.local_mtime_and_size
                                .insert(ino, (dirty_attr.mtime, dirty_attr.size));
                        }
                        // Reset `mtime` and `size` to origin, because changes of these fields is
                        // invisible to other nodes until flush.
                        dirty_attr_for_kv.mtime = origin_attr.mtime;
                        dirty_attr_for_kv.size = origin_attr.size;

                        // The file also needs to be truncated, if the new size is shorter.
                        if dirty_attr.size < applied_attr.size {
                            let new_mtime = self
                                .storage
                                .truncate(
                                    ino,
                                    applied_attr.size.cast(),
                                    dirty_attr.size.cast(),
                                    applied_attr.mtime,
                                )
                                .await;
                            self.local_mtime_and_size
                                .insert(ino, (new_mtime, dirty_attr.size));
                            dirty_attr.mtime = new_mtime;
                        }
                    }

                    inode.set_attr(dirty_attr_for_kv);

                    debug!(
                        "setattr() successfully set the attribute of ino={} and name={:?}, the set attr={:?}",
                        ino, inode.get_name(), dirty_attr,
                    );

                    // The attr with new `size` and `mtime` should be replied.
                    dirty_attr
                }
                None => {
                    // setattr did not change any attribute.
                    return Ok((ttl, fs_util::convert_to_fuse_attr(origin_attr)));
                }
            };

            txn.set(
                &KeyType::INum2Node(ino),
                &ValueType::Node(inode.to_serial_node()),
            );

            (
                txn.commit().await,
                (ttl, fs_util::convert_to_fuse_attr(dirty_attr_for_reply)),
            )
        })?;
        Ok(result)
    }

    #[instrument(skip(self), err, ret)]
    async fn unlink(&self, context: ReqContext, parent: INum, name: &str) -> DatenLordResult<()> {
        retry_txn!(TXN_RETRY_LIMIT, {
            let mut txn = self.kv_engine.new_meta_txn().await;
            let mut parent_node = self.get_inode_from_txn(txn.as_mut(), parent).await?;
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

            txn.delete(&KeyType::DirEntryKey((parent, name.into())));
            parent_node.update_mtime_ctime_to_now();

            // Ready to unlink
            let deferred_deletion =
                child_node.get_open_count() > 0 || child_node.get_lookup_count() > 0;

            if deferred_deletion {
                child_node.mark_deferred_deletion();
                txn.set(
                    &KeyType::INum2Node(child_ino),
                    &ValueType::Node(child_node.to_serial_node()),
                );
            } else {
                if let SFlag::S_IFREG = child_node.get_type() {
                    self.local_mtime_and_size.remove(&child_ino);
                    self.storage.remove(child_ino).await;
                }
                txn.delete(&KeyType::INum2Node(child_ino));
            }
            txn.set(
                &KeyType::INum2Node(parent),
                &ValueType::Node(parent_node.to_serial_node()),
            );
            (txn.commit().await, ())
        })?;
        Ok(())
    }

    async fn new(
        capacity: usize,
        kv_engine: Arc<KVEngineType>,
        node_id: &str,
        storage_config: &StorageConfig,
        storage: StorageManager<Self::St>,
    ) -> DatenLordResult<Arc<Self>> {
        // TODO: Remove this when the `S3Backend` is removed.
        let s3_config = match storage_config.params {
            datenlord::config::StorageParams::S3(ref s3_config) => s3_config.clone(),
            datenlord::config::StorageParams::Fs(_) => StorageS3Config {
                endpoint_url: "http://127.0.0.1:9000".to_owned(),
                access_key_id: "test".to_owned(),
                secret_access_key: "test1234".to_owned(),
                bucket_name: "fuse-test-bucket".to_owned(),
            },
        };

        let bucket_name = &s3_config.bucket_name;
        let endpoint = &s3_config.endpoint_url;
        let access_key = &s3_config.access_key_id;
        let secret_key = &s3_config.secret_access_key;

        let s3_backend = Arc::new(
            S::new_backend(bucket_name, endpoint, access_key, secret_key)
                .await
                .context("Failed to create s3 backend.")?,
        );
        let data_cache = Arc::new(GlobalCache::new_dist_with_bz_and_capacity(
            10_485_760, // 10 * 1024 * 1024
            capacity,
            Arc::clone(&kv_engine),
            node_id,
        ));

        let meta = Arc::new(Self {
            s3_backend: Arc::clone(&s3_backend),
            data_cache: Arc::<GlobalCache>::clone(&data_cache),
            cur_fd: AtomicU32::new(4),
            node_id: Arc::<str>::from(node_id.to_owned()),
            storage_config: Arc::<StorageConfig>::from(storage_config.clone()),
            fuse_fd: Mutex::new(-1_i32),
            inum_allocator: INumAllocator::new(Arc::clone(&kv_engine)),
            kv_engine,
            storage: Arc::new(storage),
            local_mtime_and_size: HashMap::new(),
        });

        retry_txn!(TXN_RETRY_LIMIT, {
            let mut txn = meta.kv_engine.new_meta_txn().await;
            let prev = meta
                .try_get_inode_from_txn(txn.as_mut(), FUSE_ROOT_ID)
                .await?;
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
                let root_inode = S3Node::open_root_node(
                    FUSE_ROOT_ID,
                    "/",
                    Arc::<S>::clone(&s3_backend),
                    Arc::clone(&meta),
                )
                .await
                .add_context("failed to open FUSE root node")?;
                // insert (FUSE_ROOT_ID -> root_inode) into KV engine
                txn.set(
                    &KeyType::INum2Node(FUSE_ROOT_ID),
                    &ValueType::Node(root_inode.to_serial_node()),
                );
                (txn.commit().await, ())
            }
        })?;
        Ok(meta)
    }

    /// Set fuse fd into `MetaData`
    #[tracing::instrument(skip(self))]
    async fn set_fuse_fd(&self, fuse_fd: RawFd) {
        *self.fuse_fd.lock().await = fuse_fd;
    }

    #[instrument(skip(self, node), ret)]
    /// Try to delete, if the deletion succeed, returns `true`.
    async fn delete_check(&self, node: &S3Node<S>) -> DatenLordResult<bool> {
        let is_deleted = if node.get_open_count() == 0 && node.get_lookup_count() == 0 {
            if let SFlag::S_IFREG = node.get_type() {
                let ino = node.get_ino();
                self.storage.remove(ino).await;
                self.local_mtime_and_size.remove(&ino);
            }
            true
        } else {
            false
        };
        debug!(
            "try_delete_node()  is_deleted={} i-node of ino={} and name={:?} open_count={} lookup_count={}",
            is_deleted,
            node.get_ino(),
            node.get_name(),
            node.get_open_count(),
            node.get_lookup_count(),
        );
        Ok(is_deleted)
    }

    #[instrument(skip(self), err, ret)]
    // Create and open a file
    // If the file does not exist, first create it with
    // the specified mode, and then open it.
    #[allow(clippy::too_many_lines)]
    async fn mknod(&self, param: CreateParam) -> DatenLordResult<(Duration, FuseAttr, u64)> {
        check_name_length(&param.name)?;
        check_type_supported(&param.node_type)?;
        let parent_ino = param.parent;
        let (ttl, fuse_attr) = retry_txn!(TXN_RETRY_LIMIT, {
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
                .create_child_node(
                    &param,
                    new_num,
                    Arc::<GlobalCache>::clone(&self.data_cache),
                    txn.as_mut(),
                )
                .await?;
            let fuse_attr = fs_util::convert_to_fuse_attr(new_node.get_attr());
            let ttl = Duration::new(MY_TTL_SEC, 0);
            txn.set(
                &KeyType::INum2Node(new_num),
                &ValueType::Node(new_node.to_serial_node()),
            );
            txn.set(
                &KeyType::INum2Node(parent_ino),
                &ValueType::Node(parent_node.to_serial_node()),
            );
            (txn.commit().await, (ttl, fuse_attr))
        })?;
        Ok((ttl, fuse_attr, MY_GENERATION))
    }

    #[instrument(skip(self), err, ret)]
    /// Helper function to lookup
    #[allow(clippy::too_many_lines)]
    async fn lookup_helper(
        &self,
        context: ReqContext,
        parent: INum,
        child_name: &str,
    ) -> DatenLordResult<(Duration, FuseAttr, u64)> {
        retry_txn!(TXN_RETRY_LIMIT, {
            let mut txn = self.kv_engine.new_meta_txn().await;
            if NEED_CHECK_PERM {
                let parent_node = self.get_inode_from_txn(txn.as_mut(), parent).await?;
                parent_node
                    .get_attr()
                    .check_perm(context.user_id, context.group_id, 1)?;
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
            let child_attr = self.apply_local_mtime_and_size(child_node.lookup_attr());

            let ttl = Duration::new(MY_TTL_SEC, 0);
            let fuse_attr = fs_util::convert_to_fuse_attr(child_attr);
            txn.set(
                &KeyType::INum2Node(child_ino),
                &ValueType::Node(child_node.to_serial_node()),
            );
            (txn.commit().await, (ttl, fuse_attr, MY_GENERATION))
        })
    }

    #[allow(clippy::too_many_lines)] // TODO: refactor it into smaller functions
    #[instrument(skip(self), err, ret)]
    async fn rename(&self, context: ReqContext, param: RenameParam) -> DatenLordResult<()> {
        let old_parent = param.old_parent;
        let old_name = param.old_name.as_str();
        let new_parent = param.new_parent;
        let new_name = param.new_name.as_str();
        let flags = param.flags;
        // TODO: replace the new_node should delete its related data
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

        retry_txn!(TXN_RETRY_LIMIT, {
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
        })
    }

    #[instrument(skip(self), err, ret)]
    /// Helper function of fsync
    async fn fsync_helper(
        &self,
        ino: u64,
        fh: u64,
        _datasync: bool,
        // reply: ReplyEmpty,
    ) -> DatenLordResult<()> {
        // We do not have completed metadata cache,
        // so there are no need to handle the situation that the metadata is not synced.
        self.flush(ino, fh).await
    }

    #[instrument(skip(self, data), err, ret)]
    /// Helper function to write data
    async fn write_helper(
        &self,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: Vec<u8>,
        _flags: u32,
    ) -> DatenLordResult<usize> {
        let mut txn = self.kv_engine.new_meta_txn().await;
        let inode = self.get_inode_from_txn(txn.as_mut(), ino).await?;

        let (mtime, file_size) = {
            let local_mtime_and_size = self.get_local_mtime_and_size(ino);
            let file_attr = inode.get_attr();
            local_mtime_and_size.unwrap_or((file_attr.mtime, file_attr.size))
        };

        let new_mtime = self.storage.store(ino, offset.cast(), &data, mtime).await;
        let written_size = data.len();
        let new_size = file_size.max(offset.cast::<u64>().overflow_add(written_size.cast()));

        // In fact, no changes will be written to the `inode`,
        // and the `txn.set` here is just for atomic ensuring.
        txn.set(
            &KeyType::INum2Node(ino),
            &ValueType::Node(inode.to_serial_node()),
        );

        if !txn.commit().await? {
            // Transaction committed failed, but we don't retry here
            // Print a warning log and return the result
            warn!("write_helper() failed to commit txn");
            // Currently, we consider it as a IO error
            return build_error_result_from_errno(
                Errno::EIO,
                "write_helper() failed to commit txn".to_owned(),
            );
        }
        self.local_mtime_and_size.insert(ino, (new_mtime, new_size));
        Ok(written_size)
    }
}

impl<S: S3BackEnd + Send + Sync + 'static> S3MetaData<S> {
    #[allow(clippy::unwrap_used)]
    /// Get a node from kv engine by inum
    pub async fn get_node_from_kv_engine(&self, inum: INum) -> DatenLordResult<Option<S3Node<S>>> {
        let inum_key = KeyType::INum2Node(inum);
        let raw_data = self.kv_engine.get(&inum_key).await.add_context(format!(
            "{}() failed to get node of ino={inum} from kv engine",
            function_name!()
        ))?;

        // deserialize node
        Ok(raw_data.map(|value| value.into_s3_node(self)))
    }

    /// Get the local cache of `mtime` and `size` of a file.
    fn get_local_mtime_and_size(&self, ino: INum) -> Option<(SystemTime, u64)> {
        let guard = pin();

        self.local_mtime_and_size.get(&ino, &guard).copied()
    }

    /// Apply the local `mtime` and `size` to `attr`.
    fn apply_local_mtime_and_size(&self, mut attr: FileAttr) -> FileAttr {
        let ino = attr.ino;

        let Some((local_mtime, local_size)) = self.get_local_mtime_and_size(ino) else {
            return attr;
        };

        attr.mtime = local_mtime;
        attr.ctime = local_mtime.max(attr.ctime);
        attr.size = local_size;

        attr
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
        parent_node: &S3Node<S>,
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
            && context.user_id != 0
            && (parent_attr.perm & 0o1000 != 0)
            && context.user_id != parent_attr.uid
            && context.user_id != child_attr.uid
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
    ) -> DatenLordResult<Option<S3Node<S>>> {
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
    ) -> DatenLordResult<S3Node<S>> {
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
