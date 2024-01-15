#![allow(clippy::unwrap_used)]

use std::collections::HashSet;
use std::sync::Arc;

use itertools::Itertools;
use nix::sys::signal::Signal::SIGTERM;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::TaskManager;
use crate::common::task_manager::manager::SpawnError;
use crate::common::task_manager::task::{TaskName, EDGES};
use crate::common::task_manager::wait_for_shutdown;

#[test]
fn test_dependency_graph() {
    let task_manager = TaskManager::new();
    let edges_dumped: HashSet<_> = task_manager.edges().into_iter().collect();
    let edges_expected: HashSet<_> = EDGES.into_iter().collect();

    assert_eq!(edges_dumped, edges_expected);

    let predecessor_counts_dumped = task_manager.predecessor_counts();

    let mut predecessor_counts_expected = EDGES.iter().counts_by(|&(_, task_name)| task_name);
    predecessor_counts_expected.insert(TaskName::Root, 0);

    assert_eq!(predecessor_counts_dumped, predecessor_counts_expected);
}

/// Create a future, that waits for a signal from `token`, then sends a
/// `value` via `sender`.
async fn test_task(token: CancellationToken, value: i32, sender: mpsc::Sender<i32>) {
    token.cancelled().await;
    sender.send(value).await.unwrap();
}

#[allow(clippy::needless_pass_by_value)]
fn spawn_tasks(task_manager: &TaskManager, sender: mpsc::Sender<i32>) -> anyhow::Result<()> {
    // The order of shutdown:
    // {Root} -> {Metrics, BlockFlush, SchedulerExtender} -> {FuseRequest} ->
    // {AsyncFuse} -> {Rpc, WriteBack}
    task_manager.spawn(TaskName::Metrics, |token| {
        test_task(token, 0, sender.clone())
    })?;
    task_manager.spawn(TaskName::BlockFlush, |token| {
        test_task(token, 0, sender.clone())
    })?;
    task_manager.spawn(TaskName::SchedulerExtender, |token| {
        test_task(token, 0, sender.clone())
    })?;

    task_manager.spawn(TaskName::FuseRequest, |token| {
        test_task(token, 1, sender.clone())
    })?;

    task_manager.spawn(TaskName::AsyncFuse, |token| {
        test_task(token, 2, sender.clone())
    })?;

    task_manager.spawn(TaskName::Rpc, |token| test_task(token, 3, sender.clone()))?;
    task_manager.spawn(TaskName::WriteBack, |token| {
        test_task(token, 3, sender.clone())
    })?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shutdown() {
    let task_manager = Arc::new(TaskManager::new());

    let (sender, mut receiver) = mpsc::channel(1);

    spawn_tasks(&task_manager, sender).unwrap();

    let collector = tokio::spawn(async move {
        let mut result = vec![];
        while let Some(res) = receiver.recv().await {
            result.push(res);
        }
        result
    });

    task_manager.shutdown().await;

    let result = collector.await.unwrap();

    let result_expected: &[i32] = &[0, 0, 0, 1, 2, 3, 3];
    // All tasks will be shutted down in a certain order.
    assert_eq!(result, result_expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_wait_for_shutdown() {
    let task_manager = Arc::new(TaskManager::new());

    let (sender, mut receiver) = mpsc::channel(1);

    spawn_tasks(&task_manager, sender).unwrap();

    let collector = tokio::spawn(async move {
        let mut result = vec![];
        while let Some(res) = receiver.recv().await {
            result.push(res);
        }
        result
    });

    let waiting = tokio::spawn(wait_for_shutdown(task_manager).unwrap());

    nix::sys::signal::raise(SIGTERM).unwrap();
    waiting.await.unwrap();

    let result = collector.await.unwrap();

    let result_expected: &[i32] = &[0, 0, 0, 1, 2, 3, 3];
    // All tasks will be shutted down in a certain order.
    assert_eq!(result, result_expected);
}

#[test]
#[should_panic(expected = "there is no reactor running")]
fn test_spawn_without_async_runtime() {
    let task_manager = Arc::new(TaskManager::new());
    task_manager.spawn(TaskName::Root, |_| async {}).unwrap();
}

#[tokio::test]
async fn test_spawn_after_shutdown() {
    let task_manager = Arc::new(TaskManager::new());
    Arc::clone(&task_manager).shutdown().await;
    let err = task_manager
        .spawn(TaskName::Root, |_| async {})
        .unwrap_err();
    assert_eq!(err, SpawnError(TaskName::Root));
}
