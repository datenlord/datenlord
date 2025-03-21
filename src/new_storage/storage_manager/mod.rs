//! The storage manager.

mod manager;

/// File handle for the storage manager.
pub mod handle;

#[cfg(test)]
mod tests;

pub use manager::StorageManager;
