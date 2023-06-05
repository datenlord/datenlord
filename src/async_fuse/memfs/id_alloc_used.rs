//! id allocator for inum and fd

use std::sync::Arc;

use anyhow::Context;

use crate::{
    async_fuse::{fuse::protocol::INum, memfs::kv_engine::RETRY_TXN_BREAK},
    common::error::DatenLordResult,
    retry_txn,
};

use super::{
    dist::id_alloc::{DistIdAllocator, IdType},
    kv_engine::{KVEngine, KeyType, ValueType},
};

/// Inum allocator
#[derive(Debug)]
pub struct INumAllocator<K: KVEngine + 'static> {
    /// id allocator
    id_allocator: DistIdAllocator<K>,
}

impl<K: KVEngine + 'static> INumAllocator<K> {
    /// new `INumAllocator`
    pub fn new(kv_engine: Arc<K>) -> Self {
        Self {
            id_allocator: DistIdAllocator::new(kv_engine, IdType::INum, 2),
        }
    }
    /// get a unique inum for a path when cache miss or creating a new file
    /// return (inum, `is_new`)
    pub async fn alloc_inum_for_fnode(
        &self,
        kv_engine: &K,
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
pub struct FdAllocator<K: KVEngine> {
    /// inum allocator
    id_allocator: DistIdAllocator<K>,
}
