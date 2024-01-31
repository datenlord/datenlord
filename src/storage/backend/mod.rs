//! The backend storage.

mod backend_impl;

pub use backend_impl::{Backend, BackendBuilder};

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod test;
