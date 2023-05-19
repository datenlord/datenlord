use crate::async_fuse::fuse::protocol::INum;
use crate::async_fuse::memfs::dist::etcd;
use crate::async_fuse::memfs::s3_wrapper::S3BackEnd;
use crate::async_fuse::memfs::{dist, S3MetaData};
use crate::common::etcd_delegate::EtcdDelegate;
use clippy_utilities::OverflowArithmetic;
use std::ops::Add;
use std::sync::Arc;
use tokio::sync::Mutex;

// Check the inode number management design at here ：https://github.com/datenlord/datenlord/issues/349

/// Inode management related state.
#[derive(Debug)]
pub(crate) struct InodeState {
    /// range of allocable inum for a node,
    /// .0 is begin, .1 is end
    /// need realloc when begin==end
    range_begin_end: Mutex<(INum, INum)>,
    /// recyle inum when there's conflict path
    recycle_unused: crossbeam_queue::SegQueue<INum>,
}

impl InodeState {
    /// new `InodeState`
    pub(crate) fn new() -> Self {
        Self {
            range_begin_end: Mutex::new((0, 0)),
            recycle_unused: crossbeam_queue::SegQueue::default(),
        }
    }
    /// just get a unique inum
    async fn alloc_inum(&self, etcd_client: &Arc<EtcdDelegate>) -> INum {
        /// the step length for prealloc a range of inum for a node.
        const INODE_RANGE: u64 = 10000;
        if let Some(inum) = self.recycle_unused.pop() {
            return inum;
        }
        let mut range_begin_end = self.range_begin_end.lock().await;
        if range_begin_end.0 == range_begin_end.1 {
            // need update
            let (begin, end) = etcd::fetch_add_inode_next_range(
                // here appeared an await, so we need to use tokio::sync::Mutex
                Arc::clone(etcd_client),
                INODE_RANGE,
            )
            .await
            .unwrap_or_else(|e| panic!("failed to fetch add inode next range, error is {e:?}"));
            // begin
            range_begin_end.0 = begin.add(1);
            // end
            range_begin_end.1 = end;
            begin
        } else {
            let ret = range_begin_end.0;
            range_begin_end.0 = range_begin_end.0.overflow_add(1);
            ret
        }
    }

    /// get a unique inum for a path when cache miss or creating a new file
    /// return (inum, `is_new`)
    async fn inode_get_inum_by_fullpath(
        &self,
        fullpath: &str,
        node_id: &str,
        volume_info: &str,
        etcd_client: &Arc<EtcdDelegate>,
    ) -> (INum, bool) {
        //  1. communicate with other nodes and try to get existing inode info
        let f_attr =
            dist::client::get_attr(Arc::clone(etcd_client), node_id, volume_info, fullpath)
                .await
                .unwrap_or_else(|e| panic!("failed to get attr, error is {e:?}"));
        match f_attr {
            None => {
                let inum = self.alloc_inum(etcd_client).await;
                // try write kv when there's none
                // if there's none, write success
                // if there's some, write failed and get old
                let marked_inum =
                    etcd::mark_fullpath_with_ino_in_etcd(etcd_client, volume_info, fullpath, inum)
                        .await
                        .unwrap_or_else(|e| {
                            panic!("failed to mark fullpath with ino in etcd, error is {e:?}")
                        });
                if marked_inum == inum {
                    // write success

                    (inum, true)
                } else {
                    // write failed
                    // unused inum
                    self.recycle_unused.push(inum);
                    (marked_inum, false)
                }
            }
            Some(attr) => {
                // update local cache
                (attr.ino, false)
            }
        }
        // TODO key should be removed after a while or file is in cache.
    }
}

impl<S: S3BackEnd + Send + Sync + 'static> S3MetaData<S> {
    /// alloc global conflict free inum for a path
    #[inline]
    pub(crate) async fn inode_get_inum_by_fullpath(&self, fullpath: &str) -> (INum, bool) {
        self.inode_state
            .inode_get_inum_by_fullpath(
                fullpath,
                &self.node_id,
                &self.volume_info,
                &self.etcd_client,
            )
            .await
    }
}
