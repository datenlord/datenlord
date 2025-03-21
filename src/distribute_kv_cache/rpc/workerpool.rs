use std::{
    fmt::{self, Debug},
    sync::Arc,
};

use async_trait::async_trait;
use tracing::debug;

use super::error::RpcError;

/// A job that can be executed by a worker.
type JobImpl = Box<dyn Job + Send + Sync + 'static>;

/// A worker that can execute async jobs.
#[derive(Clone)]
pub struct WorkerPool {
    /// The number of workers in the worker pool.
    /// Current implementation is that the worker pool with a fixed number of workers.
    /// TODO: Test if we need a dynamic worker pool.
    max_workers: usize,
    /// The maximum number of jobs that can be waiting in the job queue.
    max_waiting_jobs: usize,
    /// The job queue for the worker pool, with a maximum buffer capacity of `max_waiting_jobs`.
    /// 1. When the job queue is full, the worker pool will block to receive new jobs.
    /// 2. When sender is dropped, the receiver will return `None` and the worker pool will shutdown.
    /// 3. Check the receiver reference count to determine if the worker pool is still alive.
    job_sender: Arc<flume::Sender<JobImpl>>,
    /// The workers in the worker pool.
    worker_queue: Vec<Worker>,
}

impl Debug for WorkerPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerPool")
            .field("max_workers", &self.max_workers)
            .field("max_waiting_jobs", &self.max_waiting_jobs)
            .field("worker_queue_len", &self.worker_queue.len())
            .finish_non_exhaustive()
    }
}

impl WorkerPool {
    /// Create a new worker pool, which contains a number of max workers and max waiting jobs.
    #[must_use]
    pub fn new(max_workers: usize, max_waiting_jobs: usize) -> Self {
        let (job_sender, job_receiver) = flume::bounded::<JobImpl>(max_waiting_jobs);
        let mut worker_queue = Vec::new();

        // In current implementation, we create a fixed number of workers.
        let receiver = Arc::new(job_receiver);
        for _ in 0..max_workers {
            let worker = Worker::new(Arc::clone(&receiver));
            worker.start();
            worker_queue.push(worker);
        }

        Self {
            max_workers,
            max_waiting_jobs,
            job_sender: Arc::new(job_sender),
            worker_queue,
        }
    }

    /// Submit a job to the worker pool synchronously, and block until the job is completed.
    /// Other process will be blocked until the job is completed.
    /// If all job try to submit the job, the process will be blocked.
    pub fn submit_job(&self, job: JobImpl) -> Result<(), RpcError> {
        // Submit the job to the job queue.
        self.job_sender
            .send(job)
            .map_err(|_foo| RpcError::InternalError("Failed to submit job".to_owned()))?;

        Ok(())
    }

    /// Shutdown
    ///
    /// Drop the worker, and the worker will exit.
    pub fn shutdown(&self) {
        for worker in &self.worker_queue {
            worker.exit();
        }
    }
}

/// A worker that can execute async jobs.
#[allow(dead_code)]
#[derive(Clone)]
struct Worker {
    /// Job recv channel
    job_rx: Arc<flume::Receiver<JobImpl>>,
    /// Shutdown recv channel
    shutdown_rx: flume::Receiver<()>,
    /// Shutdown send channel
    shutdown_tx: flume::Sender<()>,
}

impl Worker {
    /// Create a new worker.
    fn new(receiver: Arc<flume::Receiver<JobImpl>>) -> Self {
        debug!("Create a new worker...");
        let (shutdown_tx, shutdown_rx) = flume::bounded::<()>(1);
        Self {
            job_rx: receiver,
            shutdown_rx,
            shutdown_tx,
        }
    }

    /// Start the worker.
    fn start(&self) {
        let shutdown_rx = self.shutdown_rx.clone();
        let receiver: Arc<flume::Receiver<Box<dyn Job + Send + Sync>>> = Arc::clone(&self.job_rx);
        tokio::task::spawn(async move {
            // Core worker loop
            loop {
                debug!("Worker is waiting for a job...");
                tokio::select! {
                    // Recv shutdown signal
                    _ = shutdown_rx.recv_async() => {
                        debug!("Worker received a shutdown signal...");
                        break;
                    }
                    // Receive a job from the job queue.
                    job = receiver.recv_async() => {
                        if let Ok(job) = job {
                            debug!("Worker received a job...");
                            // 2. Run the job asynchronously.
                            job.run().await;
                        }
                    }
                }
            }
        });
    }

    /// Exit the worker.
    fn exit(&self) {
        match self.shutdown_tx.send(()) {
            Ok(()) => debug!("Worker is exiting..."),
            Err(e) => debug!("Failed to send shutdown signal: {:?}", e),
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.exit();
    }
}

/// A trait that represents a job that can be executed by a worker.
#[async_trait]
pub trait Job {
    /// Run the job.
    async fn run(&self);
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {

    use core::time;

    use tokio::{task, time::Instant};
    use tracing::info;

    use super::*;

    struct TestJob;

    #[async_trait]
    impl Job for TestJob {
        async fn run(&self) {
            debug!("TestJob::run");
        }
    }

    #[tokio::test]
    async fn test_worker_pool() {
        // setup();
        let worker_pool = WorkerPool::new(4, 4);
        for _ in 0_i32..4_i32 {
            worker_pool.submit_job(Box::new(TestJob)).unwrap();
        }

        // If submit over 4 jobs, it will be blocked here, because the job queue is full.
        tokio::time::sleep(time::Duration::from_secs(1)).await;
        let res = worker_pool.submit_job(Box::new(TestJob));
        res.unwrap();

        drop(worker_pool);
    }

    #[tokio::test]
    async fn benchmark_worker_pool() {
        // setup();

        // Test to use 4 workers to submit 1000 jobs, and calculate the time cost.
        let worker_pool = Arc::new(WorkerPool::new(10, 1000));
        let start = Instant::now();
        for _ in 0_i32..1_000_i32 {
            let worker_pool = Arc::clone(&worker_pool);
            worker_pool.submit_job(Box::new(TestJob)).unwrap();
        }
        let end = start.elapsed();
        info!("Workerpool time cost: {:?}", end);

        // Test direct spawn 1000 tasks, and calculate the time cost.
        let start = Instant::now();
        let mut tasks: Vec<task::JoinHandle<()>> = Vec::new();
        for _ in 0_i32..1_000_i32 {
            let task = task::spawn(TestJob.run());
            tasks.push(task);
        }
        for task in tasks {
            task.await.unwrap();
        }
        let end = start.elapsed();
        info!("Direct spawn time cost: {:?}", end);
    }
}
