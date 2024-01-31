use std::io::ErrorKind as StdErrorKind;
use std::path::Path;

use opendal::ErrorKind as OpenDalErrorKind;
use tokio::fs;

use super::{prepare_backend, BACKEND_ROOT, BLOCK_CONTENT, BLOCK_SIZE_IN_BYTES};
use crate::storage::{Block, Storage, StorageError, StorageErrorInner, StorageOperation};

async fn cleanup(backend_root: impl AsRef<Path>) {
    if fs::try_exists(&backend_root).await.unwrap() {
        let root = backend_root.as_ref().to_str().unwrap();
        std::process::Command::new("chmod")
            .args(["-R", "755", root])
            .output()
            .unwrap();
        fs::remove_dir_all(backend_root).await.unwrap();
    }
}

#[tokio::test]
async fn test_failed_load() {
    let backend_root = format!("{BACKEND_ROOT}/failed_load");
    cleanup(&backend_root).await;
    fs::create_dir_all(&backend_root).await.unwrap();
    let (backend, filter) = prepare_backend(&backend_root);

    let backend_root = Path::new(&backend_root);
    let file_0_path = backend_root.join("0");
    fs::create_dir_all(&file_0_path).await.unwrap();
    fs::write(file_0_path.join("0.block"), BLOCK_CONTENT)
        .await
        .unwrap();

    // Forbid to read, but allow to get the reader
    filter.read.disable_read();
    let err = backend.load(0, 0).await.unwrap_err();

    assert!(
        matches!(
            err,
            StorageError {
                operation: StorageOperation::Load { ino: 0, block_id: 0 },
                inner: StorageErrorInner::StdIoError(ref e),
            }
            if e.kind() == StdErrorKind::PermissionDenied
        ),
        "Mismatched: error={err:?}"
    );

    // forbitd to get the reader
    filter.read.disable_get();
    let err = backend.load(0, 0).await.unwrap_err();

    assert!(
        matches!(
            err,
            StorageError {
                operation: StorageOperation::Load { ino: 0, block_id: 0 },
                inner: StorageErrorInner::OpenDalError(ref e),
            }
            if e.kind() == OpenDalErrorKind::PermissionDenied
        ),
        "Mismatched: error={err:?}"
    );

    cleanup(&backend_root).await;
}

#[tokio::test]
async fn test_failed_store() {
    let backend_root = format!("{BACKEND_ROOT}/failed_store");
    cleanup(&backend_root).await;
    fs::create_dir_all(&backend_root).await.unwrap();
    let (backend, filter) = prepare_backend(&backend_root);

    // Forbid to write the block
    filter.write.disable_write();

    let err = backend
        .store(0, 0, Block::new_zeroed(BLOCK_SIZE_IN_BYTES))
        .await
        .unwrap_err();

    assert!(
        matches!(
            err,
            StorageError {
                operation: StorageOperation::Store { ino: 0, block_id: 0 },
                inner: StorageErrorInner::StdIoError(ref e),
            }
            if e.kind() == StdErrorKind::Other  // openDAL mapped all write error to `StdErrorKind::Other`
        ),
        "Mismatched: error={err:?}"
    );

    // Allow to write, but forbid to close the writer
    filter.write.enable_write().disable_close();
    let err = backend
        .store(0, 0, Block::new_zeroed(BLOCK_SIZE_IN_BYTES))
        .await
        .unwrap_err();

    assert!(
        matches!(
            err,
            StorageError {
                operation: StorageOperation::Store { ino: 0, block_id: 0 },
                inner: StorageErrorInner::OpenDalError(ref e),
            }
            if e.kind() == OpenDalErrorKind::PermissionDenied
        ),
        "Mismatched: error={err:?}"
    );

    // Forbid to get the writer
    filter.write.enable_close().disable_get();
    let err = backend
        .store(0, 0, Block::new_zeroed(BLOCK_SIZE_IN_BYTES))
        .await
        .unwrap_err();

    assert!(
        matches!(
            err,
            StorageError {
                operation: StorageOperation::Store { ino: 0, block_id: 0 },
                inner: StorageErrorInner::OpenDalError(ref e),
            }
            if e.kind() == OpenDalErrorKind::PermissionDenied
        ),
        "Mismatched: error={err:?}"
    );

    cleanup(&backend_root).await;
}

#[tokio::test]
async fn test_failed_partial_store() {
    let backend_root = format!("{BACKEND_ROOT}/failed_partial_store");
    cleanup(&backend_root).await;
    fs::create_dir_all(&backend_root).await.unwrap();
    let (backend, filter) = prepare_backend(&backend_root);

    backend
        .store(0, 0, Block::new_zeroed(BLOCK_SIZE_IN_BYTES))
        .await
        .unwrap();

    // Forbid to read the existing block for merging
    filter.read.disable_read();
    let err = backend
        .store(
            0,
            0,
            Block::new_zeroed_with_range(BLOCK_SIZE_IN_BYTES, 0, 4),
        )
        .await
        .unwrap_err();

    assert!(
        matches!(
            err,
            StorageError {
                operation: StorageOperation::Load { ino: 0, block_id: 0 },
                inner: StorageErrorInner::OpenDalError(ref e),
            }
            if e.kind() == OpenDalErrorKind::PermissionDenied
        ),
        "Mismatched: error={err:?}"
    );

    // Allow to read the existing block for merging, forbid to write it back
    filter.read.enable_read();
    filter.write.disable_write();
    let err = backend
        .store(
            0,
            0,
            Block::new_zeroed_with_range(BLOCK_SIZE_IN_BYTES, 0, 4),
        )
        .await
        .unwrap_err();

    assert!(
        matches!(
            err,
            StorageError {
                operation: StorageOperation::Store { ino: 0, block_id: 0 },
                inner: StorageErrorInner::OpenDalError(ref e),
            }
            if e.kind() == OpenDalErrorKind::PermissionDenied
        ),
        "Mismatched: error={err:?}"
    );

    cleanup(&backend_root).await;
}

#[tokio::test]
async fn test_failed_remove() {
    let backend_root = format!("{BACKEND_ROOT}/failed_remove");
    cleanup(&backend_root).await;
    fs::create_dir_all(&backend_root).await.unwrap();
    let (backend, filter) = prepare_backend(&backend_root);

    backend
        .store(0, 0, Block::new_zeroed(BLOCK_SIZE_IN_BYTES))
        .await
        .unwrap();

    // Forbid to remove.
    filter.disable_remove();
    let err = backend.remove(0).await.unwrap_err();

    assert!(
        matches!(
            err,
            StorageError {
                operation: StorageOperation::Remove { ino: 0 },
                inner: StorageErrorInner::OpenDalError(ref e),
            }
            if e.kind() == OpenDalErrorKind::PermissionDenied
        ),
        "Mismatched: error={err:?}"
    );
    cleanup(&backend_root).await;
}

#[tokio::test]
async fn test_failed_truncate() {
    let backend_root = format!("{BACKEND_ROOT}/failed_truncate");
    cleanup(&backend_root).await;
    fs::create_dir_all(&backend_root).await.unwrap();
    let (backend, filter) = prepare_backend(&backend_root);

    for block_id in 0..8 {
        backend
            .store(0, block_id, Block::new_zeroed(BLOCK_SIZE_IN_BYTES))
            .await
            .unwrap();
    }

    filter.disable_remove();

    // Failed to remove the whole file
    let err = backend
        .truncate(0, 8, 0, BLOCK_SIZE_IN_BYTES)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            StorageError {
                operation: StorageOperation::Remove { ino: 0 },
                inner: StorageErrorInner::OpenDalError(ref e),
            }
            if e.kind() == OpenDalErrorKind::PermissionDenied
        ),
        "Mismatched: error={err:?}"
    );

    // Failed to remove some blocks.
    let err = backend
        .truncate(0, 8, 4, BLOCK_SIZE_IN_BYTES)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            StorageError {
                operation: StorageOperation::Truncate { ino: 0, from: 8, to: 4 },
                inner: StorageErrorInner::OpenDalError(ref e),
            }
            if e.kind() == OpenDalErrorKind::PermissionDenied
        ),
        "Mismatched: error={err:?}"
    );

    filter.enable_remove();
    filter.read.disable_read();

    // Failed to read the existing block for merging
    let err = backend.truncate(0, 8, 4, 4).await.unwrap_err();
    assert!(
        matches!(
            err,
            StorageError {
                operation: StorageOperation::Load { ino: 0, block_id: 3 },
                inner: StorageErrorInner::OpenDalError(ref e),
            }
            if e.kind() == OpenDalErrorKind::PermissionDenied
        ),
        "Mismatched: error={err:?}"
    );

    filter.read.enable_read();
    filter.write.disable_write();

    // Failed to write block back
    let err = backend.truncate(0, 4, 4, 4).await.unwrap_err();
    assert!(
        matches!(
            err,
            StorageError {
                operation: StorageOperation::Store { ino: 0, block_id: 3 },
                inner: StorageErrorInner::OpenDalError(ref e),
            }
            if e.kind() == OpenDalErrorKind::PermissionDenied
        ),
        "Mismatched: error={err:?}"
    );

    cleanup(&backend_root).await;
}
