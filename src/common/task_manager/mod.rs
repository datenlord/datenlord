//! Task manager for managing async tasks and gracefully shutdown.

// TODO: Remove this when the task manager is completed.
#![allow(dead_code)]

mod manager;
mod task;

#[cfg(test)]
mod tests;

pub use manager::{wait_for_shutdown, SpawnError, TaskManager};
pub use task::TaskName;
