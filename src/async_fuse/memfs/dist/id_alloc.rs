//! Allocate unique id between nodes.

use std::ops::Add;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clippy_utilities::OverflowArithmetic;
use tokio::sync::Mutex;
use tracing::debug;

use crate::async_fuse::memfs::kv_engine::{KVEngine, KeyType, LockKeyType, ValueType};
use crate::common::error::DatenLordResult;

/// Id type
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum IdType {
    /// inode number allocated when creating a file
    INum,
    /// file descriptor index allocated when opening a file
    Fd,
}

impl IdType {
    /// Convert to str for unique key
    /// The return value can't be same
    #[must_use]
    #[inline]
    pub fn to_unique_id(&self) -> u8 {
        match *self {
            IdType::INum => 0,
            IdType::Fd => 1,
        }
    }
}

/// the timeout for the lock of updating the alloc range
const ID_ALLOC_TIMEOUT_SEC: u64 = 2;

/// distribute id allocator to alloc unique id between nodes
#[derive(Debug)]
pub struct DistIdAllocator<K: KVEngine> {
    /// range of allocable inum for a node,
    /// .0 is begin, .1 is end
    /// need realloc when begin==end
    range_begin_end: Mutex<(u64, u64)>,
    /// recycle inum when there's conflict path
    recycle_unused: crossbeam_queue::SegQueue<u64>,
    /// use etcd transaction to avoid conflict
    kv_engine: Arc<K>,
    /// Id type
    id_type: IdType,
    /// Id begin
    id_begin: u64,
}

impl<K: KVEngine + 'static> DistIdAllocator<K> {
    /// new `DistIdAllocator`
    pub(crate) fn new(kv_engine: Arc<K>, id_type: IdType, id_begin: u64) -> Self {
        Self {
            range_begin_end: Mutex::new((0, 0)),
            recycle_unused: crossbeam_queue::SegQueue::default(),
            kv_engine,
            id_type,
            id_begin,
        }
    }

    /// just get a unique id
    pub(crate) async fn alloc_id(&self) -> DatenLordResult<u64> {
        /// the step length for prealloc a range of inum for a node.
        const INODE_RANGE: u64 = 10000;
        if let Some(inum) = self.recycle_unused.pop() {
            return Ok(inum);
        }
        let mut range_begin_end = self.range_begin_end.lock().await;
        if range_begin_end.0 == range_begin_end.1 {
            // need update
            let (begin, end) = self
                .fetch_add_id_next_range(INODE_RANGE)
                .await
                .with_context(|| "failed to fetch add id next range".to_owned())?;
            // begin
            range_begin_end.0 = begin.add(1);
            // end
            range_begin_end.1 = end;
            Ok(begin)
        } else {
            let ret = range_begin_end.0;
            range_begin_end.0 = range_begin_end.0.overflow_add(1);
            Ok(ret)
        }
    }

    /// increase the inode number range begin in global cluster
    pub(crate) async fn fetch_add_id_next_range(&self, range: u64) -> DatenLordResult<(u64, u64)> {
        // Use cas to replace the lock
        // The cost of clone is low
        let lock_key = LockKeyType::IdAllocatorLock(self.id_type.clone());
        let value_key = KeyType::IdAllocatorValue(self.id_type.clone());

        // Lock before rewrite
        let lock_key = self
            .kv_engine
            .lock(&lock_key, Duration::from_secs(ID_ALLOC_TIMEOUT_SEC))
            .await
            .with_context(|| format!("failed to lock id allocator range, key is {lock_key:?}"))?;

        let range_begin =
            match self.kv_engine.get(&value_key).await.with_context(|| {
                format!("failed to get id allocator range, key is {value_key:?}")
            })? {
                Some(v) => v.into_next_id_allocate_range_begin(),
                None => self.id_begin,
            };

        // Add up and store data back to etcd
        let next = range_begin.add(range);

        self.kv_engine
            .set(&value_key, &ValueType::NextIdAllocateRangeBegin(next), None)
            .await
            .with_context(|| format!("failed to set id allocator range, key is {value_key:?}"))?;

        self.kv_engine
            .unlock(lock_key.clone())
            .await
            .with_context(|| format!("failed to unlock id allocator range, key is {lock_key:?}"))?;
        debug!("node alloc inum range ({},{})", range_begin, next);
        Ok((range_begin, next))
    }

    /// recycle unused inum
    pub fn recycle_unused(&self, id: u64) {
        self.recycle_unused.push(id);
    }
}
