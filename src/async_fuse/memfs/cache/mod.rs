//! This is the storage managing mechanism implementation for the memfs

// TODO: Remove this after the storage is ready for product env.
#![allow(dead_code)]

mod global_cache;
pub mod policy;

pub use global_cache::*;
