//! The task manager implementation.

use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures::{Future, StreamExt};
use parking_lot::Mutex;
use signal_hook_tokio::Signals;
use thiserror::Error;
use tokio::task::AbortHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument};

use super::task::{Task, TaskName, EDGES};

/// Spawn error, occurs when spawnint a task after shutdown.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("Failed to spawn task: {0:?}")]
pub struct SpawnError(pub TaskName);

/// The task manager, which will shutdown all async tasks in proper order.
#[derive(Debug)]
pub struct TaskManager {
    /// Tasks in task manager.
    tasks: Mutex<HashMap<TaskName, Task>>,
    /// The status of task manager, `true` for shutting down.
    status: Arc<AtomicBool>,
}

impl TaskManager {
    /// Create a new task manager. The relationships between task nodes are
    /// defined in [`EDGES`] and will be initialized immediately.
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        let mut tasks = HashMap::new();
        let status = Arc::default();
        for (prev, next) in EDGES {
            tasks
                .entry(prev)
                .or_insert_with(|| Task::new(prev, Arc::clone(&status)))
                .add_dependency(next);
            tasks
                .entry(next)
                .or_insert_with(|| Task::new(next, Arc::clone(&status)))
                .inc_predecessor_count();
        }

        Self {
            tasks: Mutex::new(tasks),
            status,
        }
    }

    /// Dumps all edges of the dependency graph.
    #[cfg(test)]
    pub(super) fn edges(&self) -> Vec<(TaskName, TaskName)> {
        let mut result = vec![];
        let tasks = self.tasks.lock();

        for (&task_name, task_node) in tasks.iter() {
            result.extend(
                task_node
                    .dependencies()
                    .iter()
                    .map(|&dependency| (task_name, dependency)),
            );
        }
        result
    }

    #[cfg(test)]
    pub(super) fn predecessor_counts(&self) -> HashMap<TaskName, usize> {
        let tasks = self.tasks.lock();

        tasks
            .iter()
            .map(|(&name, task)| (name, task.predecessor_count()))
            .collect()
    }

    /// Spawn a new task with task name. The task will be managed in the task
    /// manager.
    ///
    /// # Errors
    /// Returns `Err` if the task manager is shutting down.
    ///
    /// # Panics
    /// Panics if this method is called from the outside of a tokio runtime.
    #[inline]
    pub fn spawn<F, Fu>(&self, name: TaskName, f: F) -> Result<AbortHandle, SpawnError>
    where
        F: FnOnce(CancellationToken) -> Fu,
        Fu: Future<Output = ()> + Send + 'static,
    {
        let mut tasks = self.tasks.lock();
        if self.is_shutdown() {
            return Err(SpawnError(name));
        }
        let node = tasks
            .get_mut(&name)
            .unwrap_or_else(|| unreachable!("Task {name:?} is not in the manager."));

        let handle = node.spawn(f);

        Ok(handle)
    }

    /// The status of task manager, `true` for shutting down.
    #[inline]
    pub fn is_shutdown(&self) -> bool {
        self.status.load(Ordering::Acquire)
    }

    /// Shutdown the task manager.
    ///
    /// After `shutdown` being called, no new task should be spawned via task
    /// manager.
    #[inline]
    #[instrument(skip(self))]
    pub async fn shutdown(&self) {
        let mut queue = VecDeque::from([TaskName::Root]);

        self.status.store(true, Ordering::Release);

        let mut tasks = std::mem::take(&mut *self.tasks.lock());

        while let Some(task_name) = queue.pop_front() {
            let Some(mut task_node) = tasks.remove(&task_name) else {
                continue;
            };

            info!("Shutdown task node: {task_name:?}");

            // Notify all pending tasks to shutdown. And wait all of them to finish or quit.
            task_node.token().cancel();
            for result in task_node.join_all().await {
                if let Err(e) = result {
                    error!("Background task failed with error: {e}.");
                }
            }

            for dependency_name in task_node.dependencies() {
                let Some(dependency_node) = tasks.get_mut(dependency_name) else {
                    continue;
                };
                dependency_node.dec_predecessor_count();
                if dependency_node.predecessor_count() == 0 {
                    queue.push_back(*dependency_name);
                }
            }
        }
    }
}

impl Default for TaskManager {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

/// Wait for signal `SIGTERM`, `SIGQUIT` and `SIGINT`, and shutdown the task
/// manager.
#[inline]
pub fn wait_for_shutdown(
    manager: Arc<TaskManager>,
) -> anyhow::Result<impl Future<Output = ()> + Send> {
    use signal_hook::consts::TERM_SIGNALS;

    let mut signals = Signals::new(TERM_SIGNALS)?;
    let handle = signals.handle();

    let future = async move {
        if let Some(signal) = signals.next().await {
            assert!(
                TERM_SIGNALS.contains(&signal),
                "The signal hook is not to handle signal {signal}."
            );
            info!("Signal {signal} raised, start to shutdown.");
        } else {
            info!("The signal stream is closed.");
        }

        handle.close();

        manager.shutdown().await;
    };

    Ok(future)
}
