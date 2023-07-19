//! id allocator for inum and fd

use std::sync::Arc;

use anyhow::Context;

use crate::{
    async_fuse::{fuse::protocol::INum, memfs::kv_engine::RETRY_TXN_BREAK},
    common::error::DatenLordResult,
    retry_txn,
};

use super::{
    dist::{
        id_alloc::{DistIdAllocator, IdType},
        lock_manager::DistLockManager,
    },
    kv_engine::{KVEngine, KVEngineType, KeyType, ValueType},
};

/// Inum allocator
#[derive(Debug)]
pub struct INumAllocator {
    /// id allocator
    id_allocator: DistIdAllocator,
}

impl INumAllocator {
    /// new `INumAllocator`
    pub fn new(dist_lock_mananeger: Arc<DistLockManager>, kv_engine: Arc<KVEngineType>) -> Self {
        Self {
            id_allocator: DistIdAllocator::new(dist_lock_mananeger, kv_engine, IdType::INum, 2),
        }
    }
    /// get a unique inum for a path when cache miss or creating a new file
    /// return (inum, `is_new`)
    pub async fn alloc_inum_for_fnode(
        &self,
        kv_engine: &KVEngineType,
        fullpath: &str,
    ) -> DatenLordResult<(INum, bool)> {
        let key = KeyType::Path2INum(fullpath.to_owned());
        let mut allocated_id = None;

        let res = retry_txn!(3, {
            let mut txn = kv_engine.new_meta_txn().await;
            let value = txn
                .get(&key)
                .await
                .with_context(|| format!("failed to get inum for path {fullpath}"))?
                .map(ValueType::into_inum);
            if let Some(inum) = value {
                (RETRY_TXN_BREAK, (inum, false))
            } else {
                let inum = if let Some(inum) = allocated_id {
                    inum
                } else {
                    let inum = self.id_allocator.alloc_id().await?;
                    allocated_id = Some(inum);
                    inum
                };
                txn.set(&key, &ValueType::INum(inum));
                (txn.commit().await, (inum, true))
            }
        });
        if res.is_err() && allocated_id.is_some() {
            self.id_allocator
                .recycle_unused(allocated_id.unwrap_or_else(|| panic!("allocated_id is None")));
        }
        res
    }
}

/// Fd allocator
/// TODO: implement this
#[allow(dead_code)]
pub struct FdAllocator {
    /// inum allocator
    id_allocator: DistIdAllocator,
}
