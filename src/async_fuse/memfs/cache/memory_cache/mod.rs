//! The in-memory cache.

pub mod builder;
mod cache;
mod write_back_task;

pub use builder::MemoryCacheBuilder;
pub use cache::MemoryCache;

#[cfg(test)]
mod tests;
