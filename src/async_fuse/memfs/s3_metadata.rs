use std::collections::BTreeMap;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::io::RawFd;
use std::path::Path;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use clippy_utilities::{Cast, OverflowArithmetic};
use itertools::Itertools;
use nix::errno::Errno;
use nix::fcntl::OFlag;
use nix::sys::stat::SFlag;
use parking_lot::RwLock as SyncRwLock; // conflict with tokio RwLock
use tokio::sync::Mutex;
use tracing::{debug, instrument, warn};

use super::cache::{GlobalCache, IoMemBlock};
use super::dir::DirEntry;
use super::dist::client as dist_client;
use super::dist::server::CacheServer;
use super::fs_util::{self, FileAttr};
use super::id_alloc_used::INumAllocator;
use super::kv_engine::{KVEngine, KVEngineType, KeyType, ValueType};
use super::metadata::error;
use super::metadata::{MetaData, ReqContext};
use super::node::Node;
use super::s3_node::S3Node;
use super::s3_wrapper::S3BackEnd;
use super::{CreateParam, RenameParam, SetAttrParam};
#[cfg(feature = "abi-7-18")]
use crate::async_fuse::fuse::fuse_reply::FuseDeleteNotification;
use crate::async_fuse::fuse::fuse_reply::{ReplyDirectory, StatFsParam};
use crate::async_fuse::fuse::protocol::{FuseAttr, INum, FUSE_ROOT_ID};
use crate::async_fuse::memfs::check_name_length;
use crate::async_fuse::util::build_error_result_from_errno;
use crate::common::error::Context as DatenLordContext; // conflict with anyhow::Context
use crate::common::error::{DatenLordError, DatenLordResult};
use crate::common::etcd_delegate::EtcdDelegate;
use crate::function_name;

/// A helper function to build [`DatenLordError::InconsistentFS`] with default context and get the function name automatic.
macro_rules! build_inconsistent_fs {
    ($ino: expr) => {
        error::build_inconsistent_fs($ino, function_name!())
    };
}

/// The time-to-live seconds of FUSE attributes
const MY_TTL_SEC: u64 = 3600; // TODO: should be a long value, say 1 hour
/// The generation ID of FUSE attributes
const MY_GENERATION: u64 = 1; // TODO: find a proper way to set generation
/// S3 information string delimiter
const S3_INFO_DELIMITER: char = ';';

/// File system in-memory meta-data
#[derive(Debug)]
#[allow(dead_code)]
pub struct S3MetaData<S: S3BackEnd + Send + Sync + 'static> {
    /// S3 backend
    pub(crate) s3_backend: Arc<S>,
    /// Global data cache
    pub(crate) data_cache: Arc<GlobalCache>,
    /// Current available fd, it'll increase after using
    pub(crate) cur_fd: AtomicU32,
    /// Current service id
    pub(crate) node_id: Arc<str>,
    /// Volume Info
    pub(crate) volume_info: Arc<str>,
    /// Fuse fd
    fuse_fd: Mutex<RawFd>,
    /// KV engine
    pub(crate) kv_engine: Arc<KVEngineType>,
    /// Inum allocator
    inum_allocator: INumAllocator<KVEngineType>,
}

/// Parse S3 info
fn parse_s3_info(info: &str) -> DatenLordResult<(&str, &str, &str, &str)> {
    info.split(S3_INFO_DELIMITER)
        .next_tuple()
        .ok_or_else(|| anyhow::anyhow!("parse s3 information failed. s3_info: {info}").into())
}

#[async_trait]
impl<S: S3BackEnd + Sync + Send + 'static> MetaData for S3MetaData<S> {
    type N = S3Node<S>;

    #[instrument(skip(self))]
    async fn release(
        &self,
        ino: u64,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        flush: bool,
    ) -> DatenLordResult<()> {
        let mut inode = self
            .get_node_from_kv_engine(ino)
            .await?
            .ok_or_else(|| build_inconsistent_fs!(ino))?;
        inode.close(ino, fh, flush).await;
        debug!(
                "release() successfully closed the file handler={} of ino={} and name={:?} open_count={}",
                fh,
                ino,
                inode.get_name(),
                inode.get_open_count()
            );
        self.set_node_to_kv_engine(ino, inode).await?;
        self.try_delete_node(ino).await?;
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
        let mut readdir_helper = |data: &BTreeMap<String, DirEntry>| -> usize {
            let mut num_child_entries = 0;
            for (i, (child_name, child_entry)) in data.iter().enumerate().skip(offset.cast()) {
                let child_ino = child_entry.ino();
                reply.add(
                    child_ino,
                    offset.overflow_add(i.cast()).overflow_add(1), /* i + 1 means the index of
                                                                    * the next entry */
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

        let mut inode = self
            .get_node_from_kv_engine(ino)
            .await?
            .ok_or_else(|| build_inconsistent_fs!(ino))?;
        inode
            .get_attr()
            .check_perm(context.user_id, context.group_id, 5)?;
        if inode.need_load_dir_data() {
            inode.load_data(0_usize, 0_usize).await?;
        }
        let num_child_entries = inode.read_dir(&mut readdir_helper);
        debug!(
            "readdir() successfully read {} entries \
                under the directory of ino={} and name={:?}",
            num_child_entries,
            ino,
            inode.get_name(),
        );
        Ok(())
    }

    #[instrument(skip(self), err, ret)]
    async fn opendir(&self, context: ReqContext, ino: u64, flags: u32) -> DatenLordResult<RawFd> {
        let node = self
            .get_node_from_kv_engine(ino)
            .await?
            .ok_or_else(|| build_inconsistent_fs!(ino))?;
        let o_flags = fs_util::parse_oflag(flags);
        //   Open directory  need both read and execute permission
        node.get_attr()
            .check_perm(context.user_id, context.group_id, 5)?;
        let result = node.dup_fd(o_flags).await;
        self.set_node_to_kv_engine(ino, node).await?;
        result
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
    async fn flush(&self, ino: u64, fh: u64) -> DatenLordResult<()> {
        let mut node = self
            .get_node_from_kv_engine(ino)
            .await?
            .ok_or_else(|| build_inconsistent_fs!(ino))?;
        node.flush(ino, fh).await;
        Ok(())
    }

    #[instrument(skip(self))]
    async fn releasedir(&self, ino: u64, fh: u64) -> DatenLordResult<()> {
        {
            let node = self
                .get_node_from_kv_engine(ino)
                .await?
                .ok_or_else(|| build_inconsistent_fs!(ino))?;
            node.closedir(ino, fh).await;
            self.set_node_to_kv_engine(ino, node).await?;
        };
        self.try_delete_node(ino).await?;
        Ok(())
    }

    #[instrument(skip(self), err, ret)]
    async fn read_helper(
        &self,
        ino: INum,
        _fh: u64,
        offset: i64,
        size: u32,
    ) -> DatenLordResult<Vec<IoMemBlock>> {
        debug!(
            "read_helper() called, ino={}, offset={}, size={}",
            ino, offset, size
        );
        let mut inode = self
            .get_node_from_kv_engine(ino)
            .await?
            .ok_or_else(|| build_inconsistent_fs!(ino))?;

        let size: u64 =
            if offset.cast::<u64>().overflow_add(size.cast::<u64>()) > inode.get_attr().size {
                inode.get_attr().size.overflow_sub(offset.cast::<u64>())
            } else {
                size.cast()
            };

        // let node_type = node.get_type();
        // let file_data = if SFlag::S_IFREG == node_type {
        if inode.need_load_file_data(offset.cast(), size.cast()).await {
            debug!("read() need to load file data of ino={}", ino,);
            let load_res = inode.load_data(offset.cast(), size.cast()).await;
            if let Err(e) = load_res {
                debug!(
                    "read() failed to load file data of ino={} and name={:?}, the error is: {:?}",
                    ino,
                    inode.get_name(),
                    e,
                );
                return Err(e);
            }
        }
        return Ok(inode.get_file_data(offset.cast(), size.cast()).await);
    }

    #[instrument(skip(self), err, ret)]
    async fn open(&self, context: ReqContext, ino: u64, flags: u32) -> DatenLordResult<RawFd> {
        let node = self
            .get_node_from_kv_engine(ino)
            .await?
            .ok_or_else(|| build_inconsistent_fs!(ino))?;
        let o_flags = fs_util::parse_oflag(flags);
        node.open_pre_check(o_flags, context.user_id, context.group_id)?;
        // TODO: handle open flags
        // <https://pubs.opengroup.org/onlinepubs/9699919799/functions/open.html>
        // let open_res = if let SFlag::S_IFLNK = node.get_type() {
        //     node.open_symlink_target(o_flags).await.add_context(format!(
        //         "open() failed to open symlink target={:?} with flags={}",
        //         node.get_symlink_target(),
        //         flags,
        //     ))
        // } else {
        let result = node.dup_fd(o_flags).await;
        self.set_node_to_kv_engine(ino, node).await?;
        result
    }

    #[instrument(skip(self), err, ret)]
    async fn getattr(&self, ino: u64) -> DatenLordResult<(Duration, FuseAttr)> {
        let inode_wrap = self
            .get_node_from_kv_engine(ino)
            .await?
            .ok_or_else(|| build_inconsistent_fs!(ino))?;

        let inode = inode_wrap;
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

    #[instrument(skip(self))]
    async fn forget(&self, ino: u64, nlookup: u64) -> DatenLordResult<()> {
        let current_count: i64;
        {
            let inode = self
                .get_node_from_kv_engine(ino)
                .await?
                .ok_or_else(|| build_inconsistent_fs!(ino))?;

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
            self.set_node_to_kv_engine(ino, inode).await?;
        };
        self.try_delete_node(ino).await?;

        Ok(())
    }

    #[instrument(skip(self), err, ret)]
    async fn setattr_helper(
        &self,
        context: ReqContext,
        ino: u64,
        param: SetAttrParam,
    ) -> DatenLordResult<(Duration, FuseAttr)> {
        let ttl = Duration::new(MY_TTL_SEC, 0);
        let mut inode = self
            .get_node_from_kv_engine(ino)
            .await?
            .ok_or_else(|| build_inconsistent_fs!(ino))?;

        match inode
            .setattr_precheck(param, context.user_id, context.group_id)
            .await
        {
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
                self.set_node_to_kv_engine(ino, inode).await?;
                Ok((ttl, fs_util::convert_to_fuse_attr(file_attr)))
            }
            Err(e) => {
                debug!(
                    "setattr() failed to set the attribute of ino={} and name={:?}, the error is: {:?}",
                    ino,
                    inode.get_name(),
                    e,
                );
                Err(e)
            }
        }
    }

    #[instrument(skip(self), err, ret)]
    async fn unlink(&self, context: ReqContext, parent: INum, name: &str) -> DatenLordResult<()> {
        let entry_type = {
            let parent_node = self.get_node_from_kv_engine(parent).await?.ok_or_else(|| {
                error::build_inconsistent_fs_with_context(
                    function_name!(),
                    format!("parent of ino={parent} should be in cache before remove its child"),
                )
            })?;
            let child_entry = parent_node.get_entry(name).ok_or_else(|| error::build_inconsistent_fs_with_context(
                function_name!(),
                 format!("the child entry name={name:?} to remove is not under parent of ino={parent}"
                )
            ))?;
            let entry_type = child_entry.entry_type();
            debug_assert_ne!(
                SFlag::S_IFDIR,
                entry_type,
                "unlink() should not remove sub-directory name={name:?} under parent ino={parent}",
            );
            entry_type
        };

        self.remove_node_helper(context, parent, name, entry_type)
            .await
    }

    async fn new(
        s3_info: &str,
        capacity: usize,
        ip: &str,
        port: &str,
        _: EtcdDelegate,
        kv_engine: Arc<KVEngineType>,
        node_id: &str,
        volume_info: &str,
    ) -> DatenLordResult<(Arc<Self>, Option<CacheServer>)> {
        let (bucket_name, endpoint, access_key, secret_key) = parse_s3_info(s3_info)?;
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
            volume_info: Arc::<str>::from(volume_info.to_owned()),
            fuse_fd: Mutex::new(-1_i32),
            inum_allocator: INumAllocator::new(Arc::clone(&kv_engine)),
            kv_engine,
        });

        let server = CacheServer::new(ip.to_owned(), port.to_owned(), data_cache);

        let root_inode = S3Node::open_root_node(FUSE_ROOT_ID, "/", s3_backend, Arc::clone(&meta))
            .await
            .add_context("failed to open FUSE root node")?;
        // insert (FUSE_ROOT_ID -> root_inode) into KV engine
        meta.set_node_to_kv_engine(FUSE_ROOT_ID, root_inode).await?;
        Ok((meta, Some(server)))
    }

    /// Set fuse fd into `MetaData`
    #[tracing::instrument(skip(self))]
    async fn set_fuse_fd(&self, fuse_fd: RawFd) {
        *self.fuse_fd.lock().await = fuse_fd;
    }

    #[instrument(skip(self), ret)]
    /// Try to delete node that is marked as deferred deletion
    async fn try_delete_node(&self, ino: INum) -> DatenLordResult<bool> {
        let node = self.get_node_from_kv_engine(ino).await?.ok_or_else(|| {
            error::build_inconsistent_fs_with_context(
                function_name!(),
                format!("the i-node of ino={ino} is not in K/V"),
            )
        })?;
        debug!(
            "try_delete_node() try to delete i-node of ino={} and name={:?} open_count={} lookup_count={}",
            ino,
            node.get_name(),
            node.get_open_count(),
            node.get_lookup_count(),
        );
        if node.get_open_count() == 0 && node.get_lookup_count() == 0 {
            debug!(
                "try_delete_node() deleted i-node of ino={} and name={:?}",
                ino,
                node.get_name(),
            );
            self.remove_node_from_kv_engine(ino).await?;
            if let SFlag::S_IFREG = node.get_type() {
                self.data_cache.remove_file_cache(node.get_ino()).await;
            }
            Ok(true)
        } else {
            debug!(
                "try_delete_node() cannot deleted i-node of ino={} and name={:?},\
                     open_count={}, lookup_count={}",
                ino,
                node.get_name(),
                node.get_open_count(),
                node.get_lookup_count()
            );
            Ok(false)
        }
    }

    #[instrument(skip(self), err, ret)]
    // Create and open a file
    // If the file does not exist, first create it with
    // the specified mode, and then open it.
    #[allow(clippy::too_many_lines)]
    async fn create_node_helper(
        &self,
        param: CreateParam,
    ) -> DatenLordResult<(Duration, FuseAttr, u64)> {
        check_name_length(&param.name)?;
        let parent = param.parent;
        let node_name = &param.name;
        let mode = param.mode;
        let node_type = param.node_type;
        let target_path: Option<&Path> = match param.link {
            Some(ref path) => Some(path.as_ref()),
            None => None,
        };
        let uid = param.uid;
        let gid = param.gid;
        // pre-check : check whether the child name is valid
        let mut parent_node = self
            .create_node_pre_check(parent, node_name, uid, gid)
            .await
            .add_context(format!("{}() failed to pre check", function_name!()))?;
        // allocate a new i-node number
        let inum = self.alloc_inum().await?;

        let parent_name = parent_node.get_name().to_owned();
        // all checks are passed, ready to create new node
        let m_flags = fs_util::parse_mode(mode);
        let new_node = match node_type {
            SFlag::S_IFDIR => {
                parent_node
                .create_child_dir(inum,node_name, m_flags,param.uid,param.gid)
                .await
                .add_context(format!(
                    "{}() failed to create directory with name={node_name:?} and mode={m_flags:?} \
                        under parent directory of ino={parent} and name={parent_name:?}", function_name!()
                ))?
            }
            SFlag::S_IFREG => {
                let o_flags = OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_RDWR;
                parent_node
                    .create_child_file(
                        inum,
                        node_name,
                        o_flags,
                        m_flags,
                        uid,
                        gid,
                        Arc::<GlobalCache>::clone(&self.data_cache),
                    )
                    .await
                    .add_context(format!(
                    "{}() failed to create file with name={node_name:?} and mode={m_flags:?} \
                        under parent directory of ino={parent} and name={parent_name:?}", function_name!()
                ))?
            }
            SFlag::S_IFLNK => {
                parent_node
                .create_child_symlink(
                    inum,
                    node_name,
                    target_path.ok_or_else(|| anyhow::anyhow!("{}() failed to \
                    get target path when create symlink with name={:?} \
                    under parent directory of ino={} and name={:?}",
                    function_name!(), node_name, parent, parent_node.get_name()))?.to_owned(),
                )
                .await
                .add_context(format!(
                    "{}() failed to create symlink with name={node_name:?} to target path={target_path:?} \
                        under parent directory of ino={parent} and name={parent_name:?}", function_name!()
                ))?
            }
            _ => {
                return Err(
                    error::build_unsupported_inode_type(
                        node_type,
                        format!(
                            "{}() does not support i-node to be created under parent directory of ino={} and name={:?}",
                            function_name!(),
                            parent,
                            parent_name
                        )
                    )
                );
            }
        };
        let new_ino = new_node.get_ino();
        let new_node_attr = new_node.get_attr();
        let fuse_attr = fs_util::convert_to_fuse_attr(new_node_attr);
        debug!(
            "create_node_helper() successfully created the new child name={:?} \
                of ino={} and type={:?} under parent ino={} and name={:?} lookup_count={}",
            node_name,
            new_ino,
            node_type,
            parent,
            parent_name,
            new_node.get_lookup_count(),
        );

        self.set_node_to_kv_engine(new_ino, new_node).await?;
        self.set_node_to_kv_engine(parent, parent_node).await?;

        let ttl = Duration::new(MY_TTL_SEC, 0);
        Ok((ttl, fuse_attr, MY_GENERATION))
    }

    #[instrument(skip(self), err, ret)]
    /// Helper function to remove node
    async fn remove_node_helper(
        &self,
        context: ReqContext,
        parent: INum,
        node_name: &str,
        node_type: SFlag,
    ) -> DatenLordResult<()> {
        debug!(
            "remove_node_helper() about to remove parent ino={:?}, \
            child_name={:?}, child_type={:?}",
            parent, node_name, node_type
        );
        self.remove_node_local(context, parent, node_name, node_type, false)
            .await?;
        Ok(())
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
        let pre_check_res = self
            .lookup_pre_check(parent, child_name, context.user_id, context.group_id)
            .await;
        let (child_ino, _, _) = match pre_check_res {
            Ok((ino, child_type, child_attr)) => (ino, child_type, child_attr),
            Err(e) => {
                debug!("lookup() failed to pre-check, the error is: {:?}", e);
                return Err(e);
            }
        };

        let ttl = Duration::new(MY_TTL_SEC, 0);
        let child_node = self
            .get_node_from_kv_engine(child_ino)
            .await?
            .ok_or_else(|| build_inconsistent_fs!(child_ino))?;
        let attr = child_node.lookup_attr();
        debug!(
            "ino={} lookup_count={} lookup_attr={:?}",
            child_ino,
            child_node.get_lookup_count(),
            attr
        );
        self.set_node_to_kv_engine(child_ino, child_node).await?;
        let fuse_attr = fs_util::convert_to_fuse_attr(attr);
        Ok((ttl, fuse_attr, MY_GENERATION))
    }

    #[instrument(skip(self), err, ret)]
    /// Rename helper to exchange on disk
    async fn rename_exchange_helper(
        &self,
        context: ReqContext,
        param: RenameParam,
    ) -> DatenLordResult<()> {
        self.rename_exchange_local(context, &param).await?;
        Ok(())
    }

    #[instrument(skip(self), err, ret)]
    /// Rename helper to move on disk, it may replace destination entry
    async fn rename_may_replace_helper(
        &self,
        context: ReqContext,
        param: RenameParam,
    ) -> DatenLordResult<()> {
        self.rename_may_replace_local(context, &param, false)
            .await?;
        Ok(())
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
        let mut inode = self
            .get_node_from_kv_engine(ino)
            .await?
            .ok_or_else(|| build_inconsistent_fs!(ino))?;

        inode.flush(ino, fh).await;

        Ok(())
    }

    #[instrument(skip(self), err, ret)]
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
        let (result, _) = {
            let mut inode = self
                .get_node_from_kv_engine(ino)
                .await?
                .ok_or_else(|| build_inconsistent_fs!(ino))?;
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
            self.set_node_to_kv_engine(ino, inode).await?;
            (res, parent_ino)
        };
        self.invalidate_remote(ino, offset, data_len).await?;
        result
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
        Ok(match raw_data {
            Some(r) => Some(r.into_s3_node(self).await?),
            None => None,
        })
    }

    /// Set node to kv engine use inum
    pub async fn set_node_to_kv_engine(&self, inum: INum, node: S3Node<S>) -> DatenLordResult<()> {
        let inum_key = KeyType::INum2Node(inum);
        let node_value = ValueType::Node(node.into_serial_node());
        self.kv_engine
            .set(&inum_key, &node_value, None)
            .await
            .add_context(format!(
                "{}() failed to set node of ino={inum} to kv engine",
                function_name!()
            ))?;

        Ok(())
    }

    /// Remove node from kv engine use inum
    pub async fn remove_node_from_kv_engine(&self, inum: INum) -> DatenLordResult<()> {
        self.kv_engine
            .delete(&KeyType::INum2Node(inum), None)
            .await
            .add_context(format!(
                "{}() failed to remove node of ino={inum} from kv engine",
                function_name!()
            ))?;

        Ok(())
    }

    /// The pre-check before create node
    /// # Return
    /// If successful, returns the parent node
    async fn create_node_pre_check(
        &self,
        parent: INum,
        node_name: &str,
        user_id: u32,
        group_id: u32,
    ) -> DatenLordResult<S3Node<S>> {
        let parent_node = self.get_node_from_kv_engine(parent).await?.ok_or_else(|| {
            error::build_inconsistent_fs_with_context(
                function_name!(),
                format!("parent of ino={parent} should be in cache before create it new child"),
            )
        })?;
        parent_node.get_attr().check_perm(user_id, group_id, 2)?;
        if let Some(occupied) = parent_node.get_entry(node_name) {
            debug!(
                "create_node_pre_check() found the directory of ino={} and name={:?} \
                    already exists a child with name={:?} and ino={}",
                parent,
                parent_node.get_name(),
                node_name,
                occupied.ino(),
            );
            return build_error_result_from_errno(
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
        debug_assert!(inode.get_lookup_count() >= 0); // lookup count cannot be negative
        debug_assert!(inode.get_open_count() >= 0);
        // pre-check whether deferred delete or not
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
        let inode = self.get_node_from_kv_engine(ino).await?.ok_or_else(|| {
            anyhow::anyhow!(
                "{}() failed to \
                         find the i-node of ino={ino} to remove",
                function_name!()
            )
        })?;
        let (deferred_deletion, parent_ino, node_name) = Self::deferred_delete_pre_check(&inode);
        let mut parent_node = self
            .get_node_from_kv_engine(parent_ino)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "{}() failed to \
                     find the parent of ino={parent_ino} for i-node of ino={ino}",
                    function_name!()
                )
            })?;
        let deleted_entry = parent_node
            .unlink_entry(&node_name)
            .await
            .add_context(format!(
                "{}() failed to remove entry name={node_name:?} \
                 and ino={ino} from parent directory ino={parent_ino}",
                function_name!()
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
            let inode = self.get_node_from_kv_engine(ino).await?.unwrap_or_else(|| {
                unreachable!(
                    "impossible case, may_deferred_delete_node_helper() \
                     i-node of ino={ino} is not in cache",
                );
            });
            debug!(
                "may_deferred_delete_node_helper() deferred removed \
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
            self.remove_node_from_kv_engine(ino).await?;
        }
        self.set_node_to_kv_engine(parent_ino, parent_node).await?;
        Ok(())
    }

    /// Lookup helper function to pre-check
    async fn lookup_pre_check(
        &self,
        parent: INum,
        name: &str,
        user_id: u32,
        group_id: u32,
    ) -> DatenLordResult<(INum, SFlag, Arc<SyncRwLock<FileAttr>>)> {
        // lookup child ino and type first
        let parent_node = self
            .get_node_from_kv_engine(parent)
            .await?
            .ok_or_else(|| build_inconsistent_fs!(parent))?;
        parent_node.get_attr().check_perm(user_id, group_id, 1)?;
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
            build_error_result_from_errno(
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
    ///
    /// This function ensures:
    /// 1. The old parent, old entry (`old_name`) and the new parent exists.
    /// 2. The user renaming the file is permitted to do this operation when the sticky bit of one of the old entry or the new entry (if exists) is set.
    /// 3. The new entry does not exists, or the `no_replace` is false.
    ///
    /// When all checks above passed,
    /// this function returns a tuple containing the fd of old parent,
    /// the ino of old node,
    /// the fd of new parent and the ino of new node (if exists).
    ///
    /// Otherwise, it returns an `Err`.
    async fn rename_pre_check(
        &self,
        context: ReqContext,
        old_parent: INum,
        old_name: &str,
        new_parent: INum,
        new_name: &str,
        no_replace: bool,
    ) -> DatenLordResult<(RawFd, INum, RawFd, Option<INum>)> {
        let old_parent_node = self
            .get_node_from_kv_engine(old_parent)
            .await?
            .ok_or_else(|| {
                error::build_inconsistent_fs_with_context(
                    function_name!(),
                    format!("the parent i-node of ino={old_parent} should be in cache"),
                )
            })?;
        let old_parent_fd = old_parent_node.get_fd();
        let old_entry_ino = match old_parent_node.get_entry(old_name) {
            None => {
                debug!(
                    "rename() failed to find child entry of name={:?} under parent directory ino={} and name={:?}",
                    old_name, old_parent, old_parent_node.get_name(),
                );
                return build_error_result_from_errno(
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
                let parent_attr = old_parent_node.get_attr();
                if context.user_id != 0
                    && (parent_attr.perm & 0o1000 != 0)
                    && context.user_id != parent_attr.uid
                    && context.user_id != old_entry.file_attr_arc_ref().read().uid
                {
                    return build_error_result_from_errno(
                        Errno::EACCES,
                        "Sticky bit set".to_owned(),
                    );
                }
                debug_assert_eq!(&old_name, &old_entry.entry_name());
                old_entry.ino()
            }
        };

        let new_parent_node = self
            .get_node_from_kv_engine(new_parent)
            .await?
            .ok_or_else(|| {
                error::build_inconsistent_fs_with_context(
                    function_name!(),
                    format!("the new parent i-node of ino={new_parent} should be in cache"),
                )
            })?;
        let new_parent_fd = new_parent_node.get_fd();
        let new_entry_ino = if let Some(new_entry) = new_parent_node.get_entry(new_name) {
            let parent_attr = new_parent_node.get_attr();
            if context.user_id != 0
                && (parent_attr.perm & 0o1000 != 0)
                && context.user_id != parent_attr.uid
                && context.user_id != new_entry.file_attr_arc_ref().read().uid
            {
                return build_error_result_from_errno(Errno::EACCES, "Sticky bit set".to_owned());
            }
            debug_assert_eq!(&new_name, &new_entry.entry_name());
            let new_ino = new_entry.ino();
            if no_replace {
                debug!(
                    "rename() found i-node of ino={} and name={:?} under new parent ino={} and name={:?}, \
                        but RENAME_NOREPLACE is specified",
                    new_ino, new_name, new_parent, new_parent_node.get_name(),
                );
                return build_error_result_from_errno(
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
        debug!(
            "rename() pre-check passed, old parent ino={}, old name={:?}, new parent ino={}, new name={:?}, \
                old entry ino={}, new entry ino={:?}",
            old_parent, old_name, new_parent, new_name, old_entry_ino, new_entry_ino,
        );
        Ok((old_parent_fd, old_entry_ino, new_parent_fd, new_entry_ino))
    }

    /// Rename in cache helper
    async fn rename_in_cache_helper(
        &self,
        old_parent: INum,
        old_name: &str,
        new_parent: INum,
        new_name: &str,
    ) -> DatenLordResult<Option<DirEntry>> {
        let mut old_parent_node = self.get_node_from_kv_engine(old_parent).await?.unwrap_or_else(|| {
            unreachable!(
                "impossible case when rename, the from parent i-node of ino={old_parent} should be in cache",
            )
        });
        let entry_to_move = match old_parent_node.remove_entry_for_rename(old_name) {
            None => unreachable!(
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
        self.set_node_to_kv_engine(old_parent, old_parent_node)
            .await?;

        // TODO : the error is: {}

        let mut new_parent_node = self.get_node_from_kv_engine(new_parent).await?.unwrap_or_else(|| {
            unreachable!(
                "impossible case when rename, the to parent i-node of ino={new_parent} should be in cache"
            )
        });
        let result = new_parent_node.insert_entry_for_rename(entry_to_move);
        self.set_node_to_kv_engine(new_parent, new_parent_node)
            .await?;
        Ok(result)
    }

    /// Rename exchange on local node
    async fn rename_exchange_local(
        &self,
        context: ReqContext,
        param: &RenameParam,
    ) -> DatenLordResult<()> {
        let old_parent = param.old_parent;
        let old_name = param.old_name.as_str();
        let new_parent = param.new_parent;
        let new_name = param.new_name.as_str();
        let flags = param.flags;
        debug!(
            "rename(old parent={}, old name={:?}, new parent={}, new name={:?})",
            old_parent, old_name, new_parent, new_name,
        );
        let no_replace = flags == 1; // RENAME_NOREPLACE

        let pre_check_res = self
            .rename_pre_check(
                context, old_parent, old_name, new_parent, new_name, no_replace,
            )
            .await;
        let (_, _, _, new_entry_ino) = match pre_check_res {
            Ok((old_parent_fd, old_entry_ino, new_parent_fd, new_entry_ino)) => {
                (old_parent_fd, old_entry_ino, new_parent_fd, new_entry_ino)
            }
            Err(e) => {
                debug!("rename() pre-check failed, the error is: {:?}", e);
                return Err(e);
            }
        };
        let new_entry_ino = new_entry_ino.ok_or_else(|| {
            anyhow::anyhow!(
                "{}(): The new entry inode with name={new_name} must exists.",
                function_name!()
            )
        })?;

        let rename_in_cache_res = self
            .rename_in_cache_helper(old_parent, old_name, new_parent, new_name)
            .await?;

        if let Some(replaced_entry) = rename_in_cache_res {
            debug_assert_eq!(
                new_entry_ino,
                replaced_entry.ino(),
                "rename_exchange_helper() replaced entry i-number not match"
            );
            let attr = Arc::clone(replaced_entry.file_attr_arc_ref());
            attr.write().ino = new_entry_ino;
            let exchange_entry = DirEntry::new(old_name.to_owned(), attr);

            let mut old_parent_node = self.get_node_from_kv_engine(old_parent).await?.unwrap_or_else(|| {
                unreachable!(
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
            let mut exchanged_node = self.get_node_from_kv_engine(new_entry_ino).await?.unwrap_or_else(|| {
                unreachable!(
                    "impossible case when rename, the new entry i-node of ino={new_entry_ino} should be in cache",
                )
            });
            exchanged_node.set_parent_ino(old_parent);
            exchanged_node.set_name(old_name);
            panic!("rename2 system call has not been supported in libc to exchange two nodes yet!");
        } else {
            unreachable!(
                "impossible case, the child i-node of name={new_name:?} to be exchanged \
                    should be under to parent directory ino={new_parent}",
            );
        }
    }

    /// Rename to move on disk locally, it may replace destination entry
    async fn rename_may_replace_local(
        &self,
        context: ReqContext,
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
            .rename_pre_check(
                context, old_parent, old_name, new_parent, new_name, no_replace,
            )
            .await;
        let (_, old_entry_ino, _, new_entry_ino) = match pre_check_res {
            Ok((old_parent_fd, old_entry_ino, new_parent_fd, new_entry_ino)) => {
                (old_parent_fd, old_entry_ino, new_parent_fd, new_entry_ino)
            }
            Err(e) => {
                debug!("rename() pre-check failed, the error is: {:?}", e);
                return Err(e);
            }
        };

        // Just replace new entry, may deferred delete
        if let Some(new_ino) = new_entry_ino {
            self.may_deferred_delete_node_helper(new_ino, from_remote)
                .await
                .add_context(format!(
                    "{}() failed to \
                        maybe deferred delete the replaced i-node ino={new_ino}",
                    function_name!()
                ))?;
        }

        {
            let mut moved_node = self.get_node_from_kv_engine(old_entry_ino).await?.unwrap_or_else(|| {
                unreachable!(
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
            self.set_node_to_kv_engine(old_entry_ino, moved_node)
                .await?;
        };

        let rename_replace_res = self
            .rename_in_cache_helper(old_parent, old_name, new_parent, new_name)
            .await?;
        debug_assert!(
            rename_replace_res.is_none(),
            "rename_may_replace_local() should already have \
                deleted the target i-node to be replaced",
        );
        Ok(())
    }

    /// Helper function to remove node locally
    pub(crate) async fn remove_node_local(
        &self,
        context: ReqContext,
        parent: INum,
        node_name: &str,
        node_type: SFlag,
        from_remote: bool,
    ) -> DatenLordResult<()> {
        let node_ino: INum;
        {
            // pre-checks
            let parent_node = self.get_node_from_kv_engine(parent).await?.ok_or_else(|| {
                error::build_inconsistent_fs_with_context(
                    function_name!(),
                    format!("parent of ino={parent} should be in cache before remove its child"),
                )
            })?;
            match parent_node.get_entry(node_name) {
                None => {
                    debug!(
                        "remove_node_local() failed to find i-node name={:?} \
                            under parent of ino={}",
                        node_name, parent,
                    );
                    return build_error_result_from_errno(
                        Errno::ENOENT,
                        format!(
                            "remove_node_local() failed to find i-node name={node_name:?} \
                                under parent of ino={parent}",
                        ),
                    );
                }
                Some(child_entry) => {
                    let parent_attr = parent_node.get_attr();
                    if context.user_id != 0
                        && (parent_attr.perm & 0o1000 != 0)
                        && context.user_id != parent_attr.uid
                        && context.user_id != child_entry.file_attr_arc_ref().read().uid
                    {
                        return build_error_result_from_errno(
                            Errno::EACCES,
                            "Sticky bit set".to_owned(),
                        );
                    }
                    node_ino = child_entry.ino();
                    if let SFlag::S_IFDIR = node_type {
                        // check the directory to delete is empty
                        let dir_node =
                            self.get_node_from_kv_engine(node_ino)
                                .await?
                                .ok_or_else(|| {
                                    error::build_inconsistent_fs_with_context(
                                        function_name!(),
                                        format!(
                                            "directory name={node_name:?} of ino={node_ino} \
                                found under the parent of ino={parent}, \
                                but no i-node found for this directory"
                                        ),
                                    )
                                })?;
                        if !dir_node.is_node_data_empty() {
                            debug!(
                                "remove_node_local() cannot remove \
                                    the non-empty directory name={:?} of ino={} \
                                    under the parent directory of ino={}",
                                node_name, node_ino, parent,
                            );
                            return build_error_result_from_errno(
                                Errno::ENOTEMPTY,
                                format!(
                                    "remove_node_local() cannot remove \
                                        the non-empty directory name={node_name:?} of ino={node_ino} \
                                        under the parent directory of ino={parent}",
                                ),
                            );
                        }
                    }

                    let child_node = self.get_node_from_kv_engine(node_ino)
                            .await?
                            .ok_or_else(|| error::build_inconsistent_fs_with_context(
                                function_name!(),
                                format!("i-node name={node_name:?} of ino={node_ino} found under the parent of ino={parent}, but no i-node found for this node"
                                )))?;

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
                .add_context(format!(
                    "{}() failed to maybe deferred delete child i-node of ino={node_ino}, \
                        name={node_name:?} and type={node_type:?} under parent ino={parent}",
                    function_name!()
                ))?;
            // reply.ok().await?;
            debug!(
                "remove_node_local() successfully removed child i-node of ino={}, \
                    name={:?} and type={:?} under parent ino={}",
                node_ino, node_name, node_type, parent,
            );
        };
        Ok(())
    }

    /// Allocate a new uinque inum for new node
    async fn alloc_inum(&self) -> DatenLordResult<INum> {
        let result = self.inum_allocator.alloc_inum_for_fnode().await;
        debug!("alloc_inum_for_fnode() result={result:?}");
        result
    }

    /// Invalidate cache from other nodes
    async fn invalidate_remote(
        &self,
        full_ino: INum,
        offset: i64,
        len: usize,
    ) -> DatenLordResult<()> {
        dist_client::invalidate(
            &self.kv_engine,
            &self.node_id,
            &self.volume_info,
            full_ino,
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
        .map_err(DatenLordError::from)
        .add_context("failed to invlidate others' cache")
    }
}
