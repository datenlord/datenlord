//! This is the storage managing mechanism implementation for the memfs

// TODO: Remove this after the storage is ready for product env.
#![allow(dead_code)]

mod backend;
mod block;
mod global_cache;
mod memory_cache;
mod storage;
mod storage_manager;

pub mod policy;

pub use backend::Backend;
pub use block::{Block, BlockCoordinate};
pub use global_cache::*;
pub use memory_cache::{MemoryCache, MemoryCacheBuilder, SoftLimit};
pub use storage::Storage;
pub use storage_manager::StorageManager;

/// The number of bytes in one KiB.
pub const KB_SIZE: usize = 1024;

/// The number of bytes in one MiB.
pub const MB_SIZE: usize = 1024 * KB_SIZE;

/// The number of bytes in one GiB.
pub const GB_SIZE: usize = 1024 * MB_SIZE;

/// The size of a block.
pub const BLOCK_SIZE_IN_BYTES: usize = 512 * KB_SIZE;

/// The capacity of `InMemoryCache` in bytes.
pub const MEMORY_CACHE_CAPACITY_IN_BYTES: usize = 8 * GB_SIZE;

/// The capacity of `InMemoryCache` in blocks.
pub const MEMORY_CACHE_CAPACITY_IN_BLOCKS: usize =
    MEMORY_CACHE_CAPACITY_IN_BYTES.wrapping_div(BLOCK_SIZE_IN_BYTES);

/// The type of block id.
pub type BlockId = usize;

#[cfg(test)]
mod mock;

#[cfg(test)]
pub use mock::MemoryStorage;
