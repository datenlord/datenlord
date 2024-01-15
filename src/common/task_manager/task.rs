//! Task node and edges definitions.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use clippy_utilities::OverflowArithmetic;
use futures::Future;
use tokio::task::{AbortHandle, JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;

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
    handles: Vec<JoinHandle<()>>,
    /// Dependencies of this node.
    depends_on: Vec<TaskName>,
    /// The count of predecessors.
    predecessor_count: usize,
}

impl Task {
    /// Create a task node with `name`.
    pub fn new(name: TaskName, status: Arc<AtomicBool>) -> Self {
        Self {
            name,
            token: CancellationToken::new(),
            status,
            handles: vec![],
            depends_on: vec![],
            predecessor_count: 0,
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
        futures::future::join_all(handles).await
    }

    /// Returns the dependencies of this node.
    pub fn dependencies(&self) -> &[TaskName] {
        &self.depends_on
    }

    /// Spawn an async task in this task node.
    ///
    /// # Panics
    /// Panics if this method is called from the outside of a tokio runtime.
    pub fn spawn<F, Fu>(&mut self, f: F) -> AbortHandle
    where
        F: FnOnce(CancellationToken) -> Fu,
        Fu: Future<Output = ()> + Send + 'static,
    {
        let token = self.token.clone();

        let handle = tokio::spawn(f(token));
        let abort_handle = handle.abort_handle();
        self.handles.push(handle);
        abort_handle
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
