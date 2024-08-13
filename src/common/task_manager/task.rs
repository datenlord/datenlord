//! Task node and edges definitions.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use clippy_utilities::OverflowArithmetic;
use futures::Future;
use tokio::sync::mpsc;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;

use super::gc::{GcHandle, GcTask, DEFAULT_HANDLE_QUEUE_LIMIT, DEFAULT_TIMEOUT};
use super::SpawnError;

/// Name of the tasks
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskName {
    /// The root node of tasks.
    /// Exactly a dummy node.
    Root,
    /// The metrics server.
    Metrics,
    /// The tasks for flushing blocks.
    BlockFlush,
    /// FUSE operation handlers.
    FuseRequest,
    /// Async FUSE session.
    AsyncFuse,
    /// gRPC servers for CSI.
    Rpc,
    /// The write back task.
    WriteBack,
    /// The scheduler extender.
    SchedulerExtender,
}

/// The task handle(s) of the current task node.
#[derive(Debug)]
enum TaskHandle {
    /// Regular task(s).
    Regular(Vec<JoinHandle<()>>),
    /// GC task that runs in the background.
    /// A `GcHandle` is to spawn tasks into GC task.
    Gc(GcHandle, JoinHandle<GcTask>),
}

impl Default for TaskHandle {
    fn default() -> Self {
        Self::Regular(vec![])
    }
}

/// A task node in task manager.
#[derive(Debug)]
pub(super) struct Task {
    /// The name of this task node.
    name: TaskName,
    /// The cancellation token to send a signal to all the tasks in this node.
    token: CancellationToken,
    /// The status of task manager, `true` for shutting down.
    status: Arc<AtomicBool>,
    /// Handles of tasks in this node.
    handles: TaskHandle,
    /// Dependencies of this node.
    depends_on: Vec<TaskName>,
    /// The count of predecessors.
    predecessor_count: usize,
    /// Runtime of this task node.
    runtime: tokio::runtime::Runtime,
}

impl Task {
    /// Create a task node with `name`.
    pub fn new(name: TaskName, status: Arc<AtomicBool>) -> Self {
        #[allow(clippy::unwrap_used)]
        // Get shared singleton tokio runtime.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        Self {
            name,
            token: CancellationToken::new(),
            status,
            handles: TaskHandle::default(),
            depends_on: vec![],
            predecessor_count: 0,
            runtime,
        }
    }

    /// Convert this task into GC task.
    ///
    /// # Panic
    /// This method will panic, if:
    ///
    /// - This method is not called in the context of a tokio runtime.
    /// - This task node is already a GC task.
    /// - This task node is regular task, but there are already tasks spawned in
    ///   this node.
    pub fn convert_to_gc_task(&mut self) {
        if let TaskHandle::Regular(ref handles) = self.handles {
            assert!(
                handles.is_empty(),
                "Convert a regular task to GC task, when its inner handles are not empty."
            );
        } else {
            panic!("Try to convert a task to GC task, when it's already a GC task.");
        }

        let token = self.token();
        let (tx, rx) = mpsc::channel(DEFAULT_HANDLE_QUEUE_LIMIT);
        let gc_task = GcTask::new(self.name, rx, DEFAULT_TIMEOUT);

        let task_handle = self.runtime.spawn(gc_task.run(token.clone()));
        let gc_handle = GcHandle::new(self.name, Arc::clone(&self.status), token, tx);

        self.handles = TaskHandle::Gc(gc_handle, task_handle);
    }

    /// Get the GC handle of this task.
    ///
    /// Returns `None` if this task is not a GC task.
    pub fn gc_handle(&self) -> Option<GcHandle> {
        if let TaskHandle::Gc(ref gc_handle, _) = self.handles {
            Some(gc_handle.clone())
        } else {
            None
        }
    }

    /// Add a dependency of this node.
    pub fn add_dependency(&mut self, name: TaskName) {
        self.depends_on.push(name);
    }

    /// Increase the `predecessor_count`.
    pub fn inc_predecessor_count(&mut self) {
        self.predecessor_count = self.predecessor_count.overflow_add(1);
    }

    /// Decrease the `predecessor_count`.
    pub fn dec_predecessor_count(&mut self) {
        self.predecessor_count = self.predecessor_count.overflow_sub(1);
    }

    /// Returns the `predecessor_count`.
    pub fn predecessor_count(&self) -> usize {
        self.predecessor_count
    }

    /// Get the notifier of this node.
    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    /// Await all tasks in this node.
    pub async fn join_all(&mut self) -> Vec<Result<(), JoinError>> {
        let handles = std::mem::take(&mut self.handles);
        match handles {
            TaskHandle::Regular(handles) => futures::future::join_all(handles).await,
            TaskHandle::Gc(_, handle) => {
                vec![handle.await.map(|_| ())]
            }
        }
    }

    /// Returns the dependencies of this node.
    pub fn dependencies(&self) -> &[TaskName] {
        &self.depends_on
    }

    /// Spawn an async task in this task node.
    pub async fn spawn<F, Fu>(&mut self, f: F) -> Result<(), SpawnError>
    where
        F: FnOnce(CancellationToken) -> Fu,
        Fu: Future<Output = ()> + Send + 'static,
    {
        let token = self.token.clone();

        match self.handles {
            TaskHandle::Regular(ref mut handles) => {
                let handle = tokio::spawn(f(token));
                handles.push(handle);
                Ok(())
            }
            TaskHandle::Gc(ref gc_handle, _) => gc_handle.spawn(f).await,
        }
    }
}

/// Edges of the dependency graph of the tasks.
pub(super) const EDGES: [(TaskName, TaskName); 9] = [
    (TaskName::Root, TaskName::Metrics),
    (TaskName::Root, TaskName::BlockFlush),
    (TaskName::Root, TaskName::SchedulerExtender),
    (TaskName::BlockFlush, TaskName::AsyncFuse),
    (TaskName::BlockFlush, TaskName::FuseRequest),
    (TaskName::FuseRequest, TaskName::AsyncFuse),
    (TaskName::FuseRequest, TaskName::WriteBack),
    (TaskName::AsyncFuse, TaskName::Rpc),
    (TaskName::AsyncFuse, TaskName::WriteBack),
];

/// Nodes of GC tasks.
pub(super) const GC_TASKS: [TaskName; 2] = [TaskName::BlockFlush, TaskName::FuseRequest];
