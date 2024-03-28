//! The storage module of `DatenLord`.
//!
//! Designed by @xiaguan

#![allow(dead_code)] // TODO: Remove when this module is ready

mod backend;
mod block;
mod block_slice;
pub mod error;
mod memory_cache;
pub mod policy;
