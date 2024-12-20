//! The storage module of `DatenLord`.
//!
//! Designed by @xiaguan

#![allow(dead_code)] // TODO: Remove when this module is ready

mod backend;
mod block;
mod block_slice;
mod error;
mod memory_cache;
pub mod policy;
mod storage_manager;
mod storage_trait;
mod utils;

pub use backend::{Backend, BackendBuilder, BackendImpl};
pub use block::Block;
#[cfg(test)]
pub use block::BLOCK_SIZE;
pub use block_slice::{offset_to_slice, BlockSlice};
pub use error::{StorageError, StorageResult};
pub use memory_cache::{CacheKey, MemoryCache};
pub use storage_manager::handle::{FileHandle, Handles};
pub use storage_manager::StorageManager;
pub use storage_trait::Storage;
pub use utils::{format_file_path, format_path};
