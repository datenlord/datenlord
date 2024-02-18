//! The GC task implementation.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::{Future, FutureExt};
use tokio::select;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use super::{SpawnError, TaskName};

/// The default limitation of the channel of handles.
pub const DEFAULT_HANDLE_QUEUE_LIMIT: usize = 5000;

/// The default period of GC tasks.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(1);

/// A handle for spawning task into GC task.
#[derive(Debug, Clone)]
pub struct GcHandle {
    /// Name of the GC task.
    name: TaskName,
    /// The status of task manager.
    status: Arc<AtomicBool>,
    /// The listener
    token: CancellationToken,
    /// A sender of spawned tasks.
    tx: mpsc::Sender<BoxFuture<'static, ()>>,
}

impl GcHandle {
    /// Create a GC handle.
    #[inline]
    pub fn new(
        name: TaskName,
        status: Arc<AtomicBool>,
        token: CancellationToken,
        tx: mpsc::Sender<BoxFuture<'static, ()>>,
    ) -> Self {
        Self {
            name,
            status,
            token,
            tx,
        }
    }

    /// Checks if the task manager is shutdown.
    #[inline]
    #[must_use]
    pub fn is_shutdown(&self) -> bool {
        self.status.load(Ordering::Acquire)
    }

    /// Spawn a task, and send its handle to GC task for managing.
    #[inline]
    pub async fn spawn<F, Fu>(&self, f: F) -> Result<(), SpawnError>
    where
        F: FnOnce(CancellationToken) -> Fu,
        Fu: Future<Output = ()> + Send + 'static,
    {
        if self.is_shutdown() {
            return Err(SpawnError(self.name));
        }

        let token = self.token.clone();
        let future = f(token).boxed();

        self.tx
            .send(future)
            .await
            .map_err(|_e| SpawnError(self.name))?;
        Ok(())
    }
}

/// The GC task.
#[derive(Debug)]
pub(super) struct GcTask {
    /// Name of the task.
    name: TaskName,
    /// Tasks managed by the GC task.
    tasks: JoinSet<()>,
    /// A receiver to retrieve spawned tasks.
    rx: mpsc::Receiver<BoxFuture<'static, ()>>,
    /// The interval of a GC period.
    timeout: Duration,
}

impl GcTask {
    /// Create a GC task.
    pub fn new(
        name: TaskName,
        rx: mpsc::Receiver<BoxFuture<'static, ()>>,
        timeout: Duration,
    ) -> Self {
        Self {
            name,
            tasks: JoinSet::new(),
            rx,
            timeout,
        }
    }

    /// Run the GC task, consume the task itself.
    #[allow(clippy::pattern_type_mismatch)] // for `tokio::select!`
    pub async fn run(mut self, token: CancellationToken) -> Self {
        loop {
            select! {
                res = self.rx.recv() => {
                    if let Some(future) = res {
                        self.tasks.spawn(future);
                    } else {
                        info!("All senders of handles are closed, so GC task `{:?}` is exiting.", self.name);
                        break;
                    }
                },
                Some(res) = self.tasks.join_next() => {
                    if let Err(e) = res {
                        error!("Sub task in `{:?}` failed: {}", self.name, e);
                    }
                },
                () = token.cancelled() => {
                    info!("Shutdown signal received, so GC task `{:?}` is exiting.", self.name);
                    break;
                }
            }
        }

        // Clean up
        self.rx.close();
        // Retrieve all tasks.
        while let Some(future) = self.rx.recv().await {
            self.tasks.spawn(future);
        }

        loop {
            select! {
                () = tokio::time::sleep(self.timeout) => {
                    let len = self.tasks.len();
                    if len != 0 {
                        error!("`{:?}` gives up to wait {} tasks to quit.", self.name, len);
                        self.tasks.shutdown().await;
                    }
                    break;
                },
                res = self.tasks.join_next() => {
                    if let Some(res) = res {
                        if let Err(e) = res {
                            error!("Sub task in `{:?}` failed: {}", self.name, e);
                        }
                    } else {
                        info!("GC task `{:?}` exits.", self.name);
                        break;
                    }
                }
            }
        }

        self
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::default_numeric_fallback)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::{mpsc, oneshot};

    use super::{
        CancellationToken, GcHandle, GcTask, SpawnError, TaskName, DEFAULT_HANDLE_QUEUE_LIMIT,
    };

    fn make_gc_task() -> (Arc<AtomicBool>, GcTask, GcHandle) {
        let name = TaskName::Root;
        let timeout = Duration::from_millis(100);
        let (tx, rx) = mpsc::channel(DEFAULT_HANDLE_QUEUE_LIMIT);
        let task = GcTask::new(name, rx, timeout);

        let status = Arc::default();
        let token = CancellationToken::new();
        let handle = GcHandle::new(name, Arc::clone(&status), token, tx);

        (status, task, handle)
    }

    #[tokio::test]
    async fn test_gc_task() {
        let (status, gc_task, gc_handle) = make_gc_task();

        let handle = tokio::spawn(gc_task.run(gc_handle.token.clone()));

        let (tx, mut rx) = mpsc::channel(4);

        for _ in 0..4 {
            let tx = tx.clone();
            gc_handle
                .spawn(|_| async move {
                    tx.send(0).await.unwrap();
                })
                .await
                .unwrap();
        }

        drop(tx);

        // Shutdown the GC task.
        status.store(true, Ordering::Release);
        gc_handle.token.cancel();

        let mut values = vec![];
        while let Some(val) = rx.recv().await {
            values.push(val);
        }

        assert_eq!(values, [0, 0, 0, 0]);

        let gc_task = handle.await.unwrap();

        assert!(gc_task.tasks.is_empty());
    }

    #[tokio::test]
    async fn test_spawn_after_shutdown() {
        let (status, gc_task, gc_handle) = make_gc_task();

        let handle = tokio::spawn(gc_task.run(gc_handle.token.clone()));

        // Shutdown the GC task.
        status.store(true, Ordering::Release);
        gc_handle.token.cancel();

        let _: GcTask = handle.await.unwrap();

        let err = gc_handle.spawn(|_| async {}).await.unwrap_err();
        assert_eq!(err, SpawnError(TaskName::Root));
    }

    #[tokio::test]
    async fn test_timeout() {
        let (status, gc_task, gc_handle) = make_gc_task();

        let handle = tokio::spawn(gc_task.run(gc_handle.token.clone()));

        let (tx, rx) = oneshot::channel();

        gc_handle
            .spawn(|token| async move {
                token.cancelled().await;
                // This makes no progress in a period of GC task. Thus this task will be
                // aborted.
                tokio::time::sleep(Duration::from_secs(10)).await;
                tx.send(0).unwrap();
            })
            .await
            .unwrap();

        // Shutdown the GC task.
        status.store(true, Ordering::Release);
        gc_handle.token.cancel();

        handle.await.unwrap();

        // The timeout task is aborted, so the `rx` will never receive anything.
        let _: oneshot::error::RecvError = rx.await.unwrap_err();
    }
}
