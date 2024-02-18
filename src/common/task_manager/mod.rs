//! Task manager for managing async tasks and gracefully shutdown.

// TODO: Remove this when the task manager is completed.
#![allow(dead_code)]

mod gc;
mod manager;
mod task;

#[cfg(test)]
mod tests;

pub use manager::{wait_for_shutdown, SpawnError, TaskManager, TASK_MANAGER};
pub use task::TaskName;
