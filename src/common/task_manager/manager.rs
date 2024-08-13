//! The task manager implementation.

use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures::{Future, StreamExt};
use once_cell::sync::Lazy;
use signal_hook_tokio::Signals;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument};

use super::gc::GcHandle;
use super::task::{Task, TaskName, EDGES, GC_TASKS};

/// The global task manager.
pub static TASK_MANAGER: Lazy<TaskManager> = Lazy::new(TaskManager::default);

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
    is_shutdown: Arc<AtomicBool>,
    /// Runtime of this task manager node.
    runtime: tokio::runtime::Runtime,
}

impl TaskManager {
    /// Create a new task manager. The relationships between task nodes are
    /// defined in [`EDGES`] and will be initialized immediately.
    ///
    /// # Panic
    ///
    /// This method panics when it's not called in the context of tokio runtime.
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        #[allow(clippy::unwrap_used)]
        // Get shared singleton tokio runtime.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(8)
            .enable_all()
            .build()
            .unwrap();

        let mut tasks = HashMap::new();
        let is_shutdown = Arc::default();
        for (prev, next) in EDGES {
            tasks
                .entry(prev)
                .or_insert_with(|| {
                    Task::new(prev, Arc::clone(&is_shutdown), runtime.handle().clone())
                })
                .add_dependency(next);
            tasks
                .entry(next)
                .or_insert_with(|| {
                    Task::new(next, Arc::clone(&is_shutdown), runtime.handle().clone())
                })
                .inc_predecessor_count();
        }

        // Start GC task
        for gc_task_name in GC_TASKS {
            tasks
                .entry(gc_task_name)
                .or_insert_with(|| {
                    Task::new(
                        gc_task_name,
                        Arc::clone(&is_shutdown),
                        runtime.handle().clone(),
                    )
                })
                .convert_to_gc_task();
        }

        Self {
            tasks: Mutex::new(tasks),
            is_shutdown,
            runtime,
        }
    }

    /// Dumps all edges of the dependency graph.
    #[cfg(test)]
    pub(super) async fn edges(&self) -> Vec<(TaskName, TaskName)> {
        let mut result = vec![];
        let tasks = self.tasks.lock().await;

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
    pub(super) async fn predecessor_counts(&self) -> HashMap<TaskName, usize> {
        let tasks = self.tasks.lock().await;

        tasks
            .iter()
            .map(|(&name, task)| (name, task.predecessor_count()))
            .collect()
    }

    /// Get a GC handle of the specified task.
    ///
    /// Returns `None`, if the task doesn't exist, or it's not a GC task.
    #[inline]
    #[must_use]
    pub async fn get_gc_handle(&self, name: TaskName) -> Option<GcHandle> {
        let tasks = self.tasks.lock().await;
        tasks.get(&name).and_then(Task::gc_handle)
    }

    /// Spawn a new task with task name. The task will be managed in the task
    /// manager.
    ///
    /// # Errors
    /// Returns `Err` if the task manager is shutting down.
    #[inline]
    pub async fn spawn<F, Fu>(&self, name: TaskName, f: F) -> Result<(), SpawnError>
    where
        F: FnOnce(CancellationToken) -> Fu,
        Fu: Future<Output = ()> + Send + 'static,
    {
        let mut tasks = self.tasks.lock().await;
        if self.is_shutdown() {
            return Err(SpawnError(name));
        }
        let node = tasks
            .get_mut(&name)
            .unwrap_or_else(|| unreachable!("Task {name:?} is not in the manager."));

        node.spawn(f).await
    }

    /// The status of task manager, `true` for shutting down.
    #[inline]
    pub fn is_shutdown(&self) -> bool {
        self.is_shutdown.load(Ordering::Acquire)
    }

    /// Shutdown the task manager.
    ///
    /// After `shutdown` being called, no new task should be spawned via task
    /// manager.
    #[inline]
    #[instrument(skip(self))]
    pub async fn shutdown(&self) {
        let mut queue = VecDeque::from([TaskName::Root]);

        self.is_shutdown.store(true, Ordering::Release);

        let mut tasks = std::mem::take(&mut *self.tasks.lock().await);

        while let Some(task_name) = queue.pop_front() {
            let Some(mut task_node) = tasks.remove(&task_name) else {
                error!("Task {task_name:?} is going to be shutdown but it's not existing in the TaskManager.");
                continue;
            };

            info!("Shutdown task node: {task_name:?}");

            // Notify all pending tasks to shutdown. And wait all of them to finish or quit.
            task_node.token().cancel();
            for result in task_node.join_all().await {
                if let Err(e) = result {
                    error!("Background task {task_name:?} failed with error: {e}.");
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
    manager: &TaskManager,
) -> anyhow::Result<impl Future<Output = ()> + Send + '_> {
    use signal_hook::consts::TERM_SIGNALS;

    let mut signals = Signals::new(TERM_SIGNALS)?;
    let handle = signals.handle();

    let future = async move {
        if let Some(signal) = signals.next().await {
            debug_assert!(
                TERM_SIGNALS.contains(&signal),
                "The signal hook is not to handle signal {signal}."
            );
            info!("Signal {signal} raised, start to shutdown.");
        } else {
            error!("The signal stream is closed.");
        }

        handle.close();

        manager.shutdown().await;
    };

    Ok(future)
}
