use std::fs::Permissions;
use std::path::Path;

use smol::fs::unix::PermissionsExt;
use tokio::fs;

use super::{prepare_backend, BACKEND_ROOT, BLOCK_CONTENT, BLOCK_SIZE_IN_BYTES};
use crate::storage::{Block, Storage};

async fn cleanup(backend_root: &str) {
    if fs::try_exists(backend_root).await.unwrap() {
        std::process::Command::new("chmod")
            .args(["-R", "755", backend_root])
            .output()
            .unwrap();
        fs::remove_dir_all(backend_root).await.unwrap();
    }
}

#[tokio::test]
#[should_panic(expected = "backend")]
async fn test_failed_load() {
    let backend_root = format!("{BACKEND_ROOT}/failed_load");
    cleanup(&backend_root).await;
    fs::create_dir_all(&backend_root).await.unwrap();
    let backend = prepare_backend(&backend_root);

    let backend_root = Path::new(&backend_root);
    let file_0_path = backend_root.join("0");
    fs::create_dir_all(&file_0_path).await.unwrap();
    fs::write(file_0_path.join("0.block"), BLOCK_CONTENT)
        .await
        .unwrap();

    // permission: -w-------
    let permissions = Permissions::from_mode(0o200);
    fs::set_permissions(file_0_path.join("0.block"), permissions)
        .await
        .unwrap();

    // Permission Denied
    let _: Option<Block> = backend.load(0, 0).await.unwrap();
}

#[tokio::test]
#[should_panic(expected = "backend")]
async fn test_failed_store() {
    let backend_root = format!("{BACKEND_ROOT}/failed_store");
    cleanup(&backend_root).await;
    fs::create_dir_all(&backend_root).await.unwrap();
    let backend = prepare_backend(&backend_root);

    let backend_root = Path::new(&backend_root);

    // permission: r-xr-xr-x
    let permissions = Permissions::from_mode(0o555);
    fs::set_permissions(&backend_root, permissions)
        .await
        .unwrap();

    // Permission Denied
    backend
        .store(0, 0, Block::new_zeroed(BLOCK_SIZE_IN_BYTES))
        .await
        .unwrap();
}

#[tokio::test]
#[should_panic(expected = "backend")]
async fn test_failed_remove() {
    let backend_root = format!("{BACKEND_ROOT}/failed_remove");
    cleanup(&backend_root).await;
    fs::create_dir_all(&backend_root).await.unwrap();
    let backend = prepare_backend(&backend_root);

    backend
        .store(0, 0, Block::new_zeroed(BLOCK_SIZE_IN_BYTES))
        .await
        .unwrap();

    let backend_root = Path::new(&backend_root);

    // permission: r-xr-xr-x
    let permissions = Permissions::from_mode(0o555);
    fs::set_permissions(&backend_root, permissions)
        .await
        .unwrap();

    // Permission Denied
    backend.remove(0).await.unwrap();
}

#[tokio::test]
#[should_panic(expected = "truncate")]
async fn test_failed_truncate() {
    let backend_root = format!("{BACKEND_ROOT}/failed_truncate");
    cleanup(&backend_root).await;
    fs::create_dir_all(&backend_root).await.unwrap();
    let backend = prepare_backend(&backend_root);

    for block_id in 0..8 {
        backend
            .store(0, block_id, Block::new_zeroed(BLOCK_SIZE_IN_BYTES))
            .await
            .unwrap();
    }

    let backend_root = Path::new(&backend_root);

    // permission: r-xr-xr-x
    let permissions = Permissions::from_mode(0o555);
    fs::set_permissions(backend_root.join("0"), permissions)
        .await
        .unwrap();

    // Permission Denied
    backend.truncate(0, 8, 4, 4).await.unwrap();
}
