use std::path::Path;

use tokio::fs;

use super::{prepare_backend, BACKEND_ROOT, BLOCK_CONTENT, BLOCK_SIZE_IN_BYTES};
use crate::storage::{Block, Storage};

#[tokio::test]
async fn test_backend_read() {
    let backend_root = format!("{BACKEND_ROOT}/read");
    fs::create_dir_all(&backend_root).await.unwrap();

    let (backend, _) = prepare_backend(&backend_root);

    let backend_root = Path::new(&backend_root);

    let loaded = backend.load(0, 0).await.unwrap();
    assert!(loaded.is_none());

    fs::create_dir_all(backend_root.join("0")).await.unwrap();
    fs::write(backend_root.join("0").join("0.block"), BLOCK_CONTENT)
        .await
        .unwrap();

    let loaded = backend.load(0, 0).await.unwrap().unwrap();
    assert_eq!(loaded.as_slice(), BLOCK_CONTENT);

    fs::remove_dir_all(backend_root).await.unwrap();
}

#[tokio::test]
async fn test_write_whole_block() {
    let backend_root = format!("{BACKEND_ROOT}/write_whole_block");
    fs::create_dir_all(&backend_root).await.unwrap();
    let (backend, _) = prepare_backend(&backend_root);

    let backend_root = Path::new(&backend_root);

    let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
    backend.store(0, 0, block.clone()).await.unwrap();

    let block_path = backend_root.join("0").join("0.block");

    let read_block_content = fs::read(&block_path).await.unwrap();
    assert_eq!(BLOCK_CONTENT, &read_block_content[..BLOCK_SIZE_IN_BYTES]);

    let loaded = backend.load(0, 0).await.unwrap().unwrap();
    assert_eq!(loaded.as_slice(), BLOCK_CONTENT);

    fs::remove_dir_all(backend_root).await.unwrap();
}

#[tokio::test]
async fn test_write_in_middle() {
    let backend_root = format!("{BACKEND_ROOT}/write_in_middle");
    fs::create_dir_all(&backend_root).await.unwrap();
    let (backend, _) = prepare_backend(&backend_root);

    let backend_root = Path::new(&backend_root);

    let mut block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
    block.set_start(3);
    block.set_end(7);
    backend.store(0, 0, block).await.unwrap();
    let loaded = backend.load(0, 0).await.unwrap().unwrap();
    assert_eq!(loaded.as_slice(), b"\0\0\0 bar\0");

    fs::remove_dir_all(backend_root).await.unwrap();
}

#[tokio::test]
async fn test_write_append() {
    let backend_root = format!("{BACKEND_ROOT}/write_append");
    fs::create_dir_all(&backend_root).await.unwrap();
    let (backend, _) = prepare_backend(&backend_root);

    let backend_root = Path::new(&backend_root);

    let mut block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
    block.set_start(0);
    block.set_end(4);
    backend.store(0, 0, block).await.unwrap();
    let loaded = backend.load(0, 0).await.unwrap().unwrap();
    assert_eq!(loaded.as_slice(), b"foo \0\0\0\0");

    let mut block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
    block.set_start(4);
    block.set_end(8);
    backend.store(0, 0, block).await.unwrap();
    let loaded = backend.load(0, 0).await.unwrap().unwrap();
    assert_eq!(loaded.as_slice(), BLOCK_CONTENT);

    fs::remove_dir_all(backend_root).await.unwrap();
}

#[tokio::test]
async fn test_overwrite() {
    let backend_root = format!("{BACKEND_ROOT}/overwrite");
    fs::create_dir_all(&backend_root).await.unwrap();
    let (backend, _) = prepare_backend(&backend_root);

    let backend_root = Path::new(&backend_root);

    let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
    backend.store(0, 0, block).await.unwrap();
    let loaded = backend.load(0, 0).await.unwrap().unwrap();
    assert_eq!(loaded.as_slice(), BLOCK_CONTENT);

    let block = Block::from_slice_with_range(BLOCK_SIZE_IN_BYTES, 4, 8, b"foo ");
    backend.store(0, 0, block).await.unwrap();
    let loaded = backend.load(0, 0).await.unwrap().unwrap();
    assert_eq!(loaded.as_slice(), b"foo foo ");

    fs::remove_dir_all(backend_root).await.unwrap();
}

#[tokio::test]
async fn test_truncate() {
    let backend_root = format!("{BACKEND_ROOT}/truncate");
    fs::create_dir_all(&backend_root).await.unwrap();
    let (backend, _) = prepare_backend(&backend_root);

    let backend_root = Path::new(&backend_root);

    for block_id in 0..4 {
        backend
            .store(
                0,
                block_id,
                Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT),
            )
            .await
            .unwrap();
    }

    backend
        .truncate(0, 4, 1, BLOCK_SIZE_IN_BYTES)
        .await
        .unwrap();
    let loaded = backend.load(0, 0).await.unwrap().unwrap();
    assert_eq!(loaded.as_slice(), BLOCK_CONTENT);

    for block_id in 1..4 {
        let loaded = backend.load(0, block_id).await.unwrap();
        assert!(loaded.is_none());
    }

    // truncate to block_id=0 will remove the whole file.
    backend
        .truncate(0, 1, 0, BLOCK_SIZE_IN_BYTES)
        .await
        .unwrap();

    // check if the file is removed from the backend.
    let file_path = backend_root.join("0");
    let file_dir_exist = fs::try_exists(&file_path).await.unwrap();
    assert!(!file_dir_exist);

    fs::remove_dir_all(backend_root).await.unwrap();
}

#[tokio::test]
async fn test_truncate_block() {
    let backend_root = format!("{BACKEND_ROOT}/truncate_block");
    fs::create_dir_all(&backend_root).await.unwrap();
    let (backend, _) = prepare_backend(&backend_root);

    let backend_root = Path::new(&backend_root);

    for block_id in 0..4 {
        backend
            .store(
                0,
                block_id,
                Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT),
            )
            .await
            .unwrap();
    }

    backend.truncate(0, 4, 1, 4).await.unwrap();
    let loaded = backend.load(0, 0).await.unwrap().unwrap();
    assert_eq!(loaded.as_slice(), b"foo \0\0\0\0");

    fs::remove_dir_all(backend_root).await.unwrap();
}

#[tokio::test]
async fn test_truncate_not_found() {
    let backend_root = format!("{BACKEND_ROOT}/truncate_not_found");
    fs::create_dir_all(&backend_root).await.unwrap();
    let (backend, _) = prepare_backend(&backend_root);

    let backend_root = Path::new(&backend_root);

    backend
        .store(0, 0, Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT))
        .await
        .unwrap();

    fs::remove_file(backend_root.join("0").join("0.block"))
        .await
        .unwrap();

    // Should not panic
    backend.truncate(0, 4, 1, 4).await.unwrap();

    fs::remove_dir_all(backend_root).await.unwrap();
}

#[tokio::test]
async fn test_remove() {
    let backend_root = format!("{BACKEND_ROOT}/remove");
    fs::create_dir_all(&backend_root).await.unwrap();
    let (backend, _) = prepare_backend(&backend_root);

    let backend_root = Path::new(&backend_root);

    backend
        .store(0, 0, Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT))
        .await
        .unwrap();
    backend.remove(0).await.unwrap();

    assert!(!fs::try_exists(backend_root.join("0")).await.unwrap());

    fs::remove_dir_all(backend_root).await.unwrap();
}

#[tokio::test]
async fn test_unused() {
    let backend_root = format!("{BACKEND_ROOT}/unused");
    fs::create_dir_all(&backend_root).await.unwrap();
    let (backend, _) = prepare_backend(&backend_root);

    // Do nothing, and should not panic.
    backend.flush(0).await.unwrap();
    backend.flush_all().await.unwrap();
    backend.invalidate(0).await.unwrap();

    fs::remove_dir_all(backend_root).await.unwrap();
}
