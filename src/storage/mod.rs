//! This is the storage managing mechanism implementation for the memfs

mod backend;
mod block;
mod memory_cache;
mod storage_manager;
mod storage_trait;

pub mod policy;

pub use backend::{Backend, BackendBuilder};
pub use block::{Block, BlockCoordinate};
pub use memory_cache::{MemoryCache, MemoryCacheBuilder};
pub use storage_manager::StorageManager;
pub use storage_trait::Storage;

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
