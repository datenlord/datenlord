//! The implementation of backend.

use async_trait::async_trait;
use clippy_utilities::OverflowArithmetic;
use datenlord::config::{StorageParams, StorageS3Config};
use futures::{stream, AsyncReadExt, AsyncWriteExt, StreamExt};
use opendal::services::{Fs, S3};
use opendal::{ErrorKind, Operator};

use super::{Block, Storage};
use crate::async_fuse::fuse::protocol::INum;

/// Get file path by `ino`
fn get_file_path(ino: INum) -> String {
    format!("{ino}/")
}

/// Get block path by `ino` and `block_id`
fn get_block_path(ino: INum, block_id: usize) -> String {
    format!("{ino}/{block_id}.block")
}

/// A builder to build `BackendWrapper`.
#[derive(Debug)]
pub struct BackendBuilder {
    /// The storage config
    config: StorageParams,
    /// The size of a block
    block_size: usize,
}

impl BackendBuilder {
    /// Create a backend builder.
    #[must_use]
    pub fn new(config: StorageParams, block_size: usize) -> Self {
        Self { config, block_size }
    }

    /// Build the backend.
    pub fn build(self) -> opendal::Result<Backend> {
        let BackendBuilder { config, block_size } = self;

        let operator = match config {
            StorageParams::S3(StorageS3Config {
                ref endpoint_url,
                ref access_key_id,
                ref secret_access_key,
                ref bucket_name,
            }) => {
                let mut builder = S3::default();

                builder
                    .endpoint(endpoint_url)
                    .access_key_id(access_key_id)
                    .secret_access_key(secret_access_key)
                    .region("auto")
                    .bucket(bucket_name);

                Operator::new(builder)?.finish()
            }
            StorageParams::Fs(ref root) => {
                let mut builder = Fs::default();
                builder.root(root);
                Operator::new(builder)?.finish()
            }
        };

        Ok(Backend {
            operator,
            block_size,
        })
    }
}

/// The backend wrapper of `openDAL` operator.
#[derive(Debug)]
pub struct Backend {
    /// The inner `Operator`
    operator: Operator,
    /// Block size
    block_size: usize,
}

impl Backend {
    /// Create a new backend
    #[must_use]
    pub fn new(operator: Operator, block_size: usize) -> Self {
        Self {
            operator,
            block_size,
        }
    }
}

#[async_trait]
impl Storage for Backend {
    async fn load_from_self(&self, ino: INum, block_id: usize) -> Option<Block> {
        let mut block = Block::new_zeroed(self.block_size);

        if let Err(e) = self
            .operator
            .reader(&get_block_path(ino, block_id))
            .await
            .unwrap_or_else(|e| {
                panic!("Failed to get a reader where ino={ino} and block={block_id}: {e}")
            })
            .read(block.make_mut_slice())
            .await
        {
            if e.kind() == std::io::ErrorKind::NotFound {
                None
            } else {
                panic!(
                    "Failed to load a block from backend where ino={ino} and block={block_id}: {e}"
                );
            }
        } else {
            Some(block)
        }
    }

    async fn load_from_backend(&self, _: INum, _: usize) -> Option<Block> {
        // This storage has no backend.
        None
    }

    async fn cache_block_from_backend(&self, _: INum, _: usize, _: Block) {
        unreachable!("This storage has no backend, and has no cache.");
    }

    async fn store(&self, ino: INum, block_id: usize, block: Block) {
        let path = get_block_path(ino, block_id);

        let block_start = block.start();
        let block_end = block.end();

        if block_start == 0 && block_end == self.block_size {
            // To store a whole block
            let mut writer = self.operator.writer(&path).await.unwrap_or_else(|e| {
                panic!(
                    "Failed to get a writer to backend where ino={ino} and block={block_id}: {e}"
                )
            });
            writer.write_all(block.as_slice()).await.unwrap_or_else(|e| panic!("Failed to store a block to backend where ino={ino} and block={block_id}: {e}"));
            writer.close().await.unwrap_or_else(|e| {
                panic!("Failed to close the writer where ion={ino} and block={block_id}: {e}")
            });
            return;
        }

        let mut dest = self.operator.read(&path).await.unwrap_or_else(|e| {
            if e.kind() == ErrorKind::NotFound {
                // Create an empty block for overwriting is ok.
                vec![]
            } else {
                panic!("Failed to load a block to backend for overwrite where ino={ino} and block={block_id}: {e}");
            }
        });

        // Ensure that the vector is long enough to be overwritten
        if dest.len() < block_end {
            dest.resize(block_end, 0);
        }

        // merge two blocks
        dest.get_mut(block_start..block_end)
            .unwrap_or_else(|| unreachable!("The vector is ensured to be long enough."))
            .copy_from_slice(block.as_slice());
        self.operator.write(&path, dest).await.unwrap_or_else(|e| {
            panic!("Failed to store a block to backend where ino={ino} and block={block_id}: {e}")
        });
    }

    async fn remove(&self, ino: INum) {
        self.operator
            .remove_all(&get_file_path(ino))
            .await
            .unwrap_or_else(|e| {
                panic!("Failed to remove a file from the backend of ino={ino}: {e}")
            });
    }

    async fn invalidate(&self, _: INum) {
        // This storage has no cache, therefore, its contents cannot be
        // invalidated.
    }

    async fn flush(&self, _: INum) {
        // This storage has no cache and backend, therefore, there is no need to
        // flush its data.
    }

    async fn flush_all(&self) {
        // This storage has no cache and backend, therefore, there is no need to
        // flush its data.
    }

    async fn truncate(&self, ino: INum, from_block: usize, to_block: usize, fill_start: usize) {
        let paths =
            stream::iter(to_block..from_block).map(|block_id| get_block_path(ino, block_id));

        let file_path = get_file_path(ino);

        let file_exists = self.operator.is_exist(&file_path).await.unwrap_or(false);

        if file_exists {
            if to_block == 0 {
                self.operator
                    .remove_all(&file_path)
                    .await
                    .unwrap_or_else(|e| panic!("Failed to remove file a file of ino={ino}: {e}"));
                return;
            }

            self.operator.remove_via(paths).await.unwrap_or_else(|e| panic!("Failed to truncate file ino={ino} from block {from_block} to block {to_block}: {e}"));

            // truncate the last block
            if to_block > 0 && fill_start < self.block_size {
                let truncate_block_id = to_block.overflow_sub(1);
                let path = get_block_path(ino, truncate_block_id);
                match self.operator.read(&path).await {
                    Ok(mut dest) => {
                        dest.truncate(fill_start);
                        self.operator.write(&path, dest).await.unwrap_or_else(|e| panic!("Failed to store a block to backend for truncate where ino={ino} and block={truncate_block_id}: {e}"));
                    }
                    Err(e) => {
                        // It's OK that the block is not found for truncate.
                        assert!(e.kind() == ErrorKind::NotFound, "Failed to load a block from backend for truncate where ino={ino} and block={truncate_block_id}: {e}");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use std::path::Path;

    use opendal::services::Fs;
    use opendal::Operator;
    use tokio::fs;

    use super::{Backend, Block, Storage};

    const BLOCK_SIZE_IN_BYTES: usize = 8;
    const BLOCK_CONTENT: &[u8; BLOCK_SIZE_IN_BYTES] = b"foo bar ";
    const BACKEND_ROOT: &str = "/tmp/opendal";

    /// Prepare a backend
    fn prepare_backend(root: &str) -> Backend {
        let mut builder = Fs::default();
        builder.root(root);
        let op = Operator::new(builder).unwrap().finish();

        Backend::new(op, BLOCK_SIZE_IN_BYTES)
    }

    #[tokio::test]
    async fn test_backend_read() {
        let backend_root = format!("{BACKEND_ROOT}/read");
        fs::create_dir_all(&backend_root).await.unwrap();

        let backend = prepare_backend(&backend_root);

        let backend_root = Path::new(&backend_root);

        let loaded = backend.load(0, 0).await;
        assert!(loaded.is_none());

        fs::create_dir_all(backend_root.join("0")).await.unwrap();
        fs::write(backend_root.join("0").join("0.block"), BLOCK_CONTENT)
            .await
            .unwrap();

        let loaded = backend.load(0, 0).await.unwrap();
        assert_eq!(loaded.as_slice(), BLOCK_CONTENT);

        fs::remove_dir_all(backend_root).await.unwrap();
    }

    #[tokio::test]
    async fn test_write_whole_block() {
        let backend_root = format!("{BACKEND_ROOT}/write_whole_block");
        fs::create_dir_all(&backend_root).await.unwrap();
        let backend = prepare_backend(&backend_root);

        let backend_root = Path::new(&backend_root);

        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        backend.store(0, 0, block.clone()).await;

        let block_path = backend_root.join("0").join("0.block");

        let read_block_content = fs::read(&block_path).await.unwrap();
        assert_eq!(BLOCK_CONTENT, &read_block_content[..BLOCK_SIZE_IN_BYTES]);

        let loaded = backend.load(0, 0).await.unwrap();
        assert_eq!(loaded.as_slice(), BLOCK_CONTENT);

        fs::remove_dir_all(backend_root).await.unwrap();
    }

    #[tokio::test]
    async fn test_write_in_middle() {
        let backend_root = format!("{BACKEND_ROOT}/write_in_middle");
        fs::create_dir_all(&backend_root).await.unwrap();
        let backend = prepare_backend(&backend_root);

        let backend_root = Path::new(&backend_root);

        let mut block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        block.set_start(3);
        block.set_end(7);
        backend.store(0, 0, block).await;
        let loaded = backend.load(0, 0).await.unwrap();
        assert_eq!(loaded.as_slice(), b"\0\0\0 bar\0");

        fs::remove_dir_all(backend_root).await.unwrap();
    }

    #[tokio::test]
    async fn test_write_append() {
        let backend_root = format!("{BACKEND_ROOT}/write_append");
        fs::create_dir_all(&backend_root).await.unwrap();
        let backend = prepare_backend(&backend_root);

        let backend_root = Path::new(&backend_root);

        let mut block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        block.set_start(0);
        block.set_end(4);
        backend.store(0, 0, block).await;
        let loaded = backend.load(0, 0).await.unwrap();
        assert_eq!(loaded.as_slice(), b"foo \0\0\0\0");

        let mut block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        block.set_start(4);
        block.set_end(8);
        backend.store(0, 0, block).await;
        let loaded = backend.load(0, 0).await.unwrap();
        assert_eq!(loaded.as_slice(), BLOCK_CONTENT);

        fs::remove_dir_all(backend_root).await.unwrap();
    }

    #[tokio::test]
    async fn test_overwrite() {
        let backend_root = format!("{BACKEND_ROOT}/overwrite");
        fs::create_dir_all(&backend_root).await.unwrap();
        let backend = prepare_backend(&backend_root);

        let backend_root = Path::new(&backend_root);

        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        backend.store(0, 0, block).await;
        let loaded = backend.load(0, 0).await.unwrap();
        assert_eq!(loaded.as_slice(), BLOCK_CONTENT);

        let block = Block::from_slice_with_range(BLOCK_SIZE_IN_BYTES, 4, 8, b"foo ");
        backend.store(0, 0, block).await;
        let loaded = backend.load(0, 0).await.unwrap();
        assert_eq!(loaded.as_slice(), b"foo foo ");

        fs::remove_dir_all(backend_root).await.unwrap();
    }

    #[tokio::test]
    async fn test_truncate() {
        let backend_root = format!("{BACKEND_ROOT}/truncate");
        fs::create_dir_all(&backend_root).await.unwrap();
        let backend = prepare_backend(&backend_root);

        let backend_root = Path::new(&backend_root);

        for block_id in 0..4 {
            backend
                .store(
                    0,
                    block_id,
                    Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT),
                )
                .await;
        }

        backend.truncate(0, 4, 1, BLOCK_SIZE_IN_BYTES).await;
        let loaded = backend.load(0, 0).await.unwrap();
        assert_eq!(loaded.as_slice(), BLOCK_CONTENT);

        for block_id in 1..4 {
            let loaded = backend.load(0, block_id).await;
            assert!(loaded.is_none());
        }

        // truncate to block_id=0 will remove the whole file.
        backend.truncate(0, 1, 0, BLOCK_SIZE_IN_BYTES).await;

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
        let backend = prepare_backend(&backend_root);

        let backend_root = Path::new(&backend_root);

        for block_id in 0..4 {
            backend
                .store(
                    0,
                    block_id,
                    Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT),
                )
                .await;
        }

        backend.truncate(0, 4, 1, 4).await;
        let loaded = backend.load(0, 0).await.unwrap();
        assert_eq!(loaded.as_slice(), b"foo \0\0\0\0");

        fs::remove_dir_all(backend_root).await.unwrap();
    }

    #[tokio::test]
    async fn test_truncate_not_found() {
        let backend_root = format!("{BACKEND_ROOT}/truncate_not_found");
        fs::create_dir_all(&backend_root).await.unwrap();
        let backend = prepare_backend(&backend_root);

        let backend_root = Path::new(&backend_root);

        backend
            .store(0, 0, Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT))
            .await;

        fs::remove_file(backend_root.join("0").join("0.block"))
            .await
            .unwrap();

        // Should not panic
        backend.truncate(0, 4, 1, 4).await;

        fs::remove_dir_all(backend_root).await.unwrap();
    }

    #[tokio::test]
    async fn test_remove() {
        let backend_root = format!("{BACKEND_ROOT}/remove");
        fs::create_dir_all(&backend_root).await.unwrap();
        let backend = prepare_backend(&backend_root);

        let backend_root = Path::new(&backend_root);

        backend
            .store(0, 0, Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT))
            .await;
        backend.remove(0).await;

        assert!(!fs::try_exists(backend_root.join("0")).await.unwrap());

        fs::remove_dir_all(backend_root).await.unwrap();
    }

    #[tokio::test]
    async fn test_unused() {
        let backend_root = format!("{BACKEND_ROOT}/unused");
        fs::create_dir_all(&backend_root).await.unwrap();
        let backend = prepare_backend(&backend_root);

        // Do nothing, and should not panic.
        backend.flush(0).await;
        backend.flush_all().await;
        backend.invalidate(0).await;

        fs::remove_dir_all(backend_root).await.unwrap();
    }

    mod pessimistic {
        use std::fs::Permissions;
        use std::path::Path;

        use smol::fs::unix::PermissionsExt;
        use tokio::fs;

        use super::{prepare_backend, BACKEND_ROOT, BLOCK_CONTENT, BLOCK_SIZE_IN_BYTES};
        use crate::async_fuse::memfs::cache::{Block, Storage};

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
            let _: Option<Block> = backend.load(0, 0).await;
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
                .await;
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
                .await;

            let backend_root = Path::new(&backend_root);

            // permission: r-xr-xr-x
            let permissions = Permissions::from_mode(0o555);
            fs::set_permissions(&backend_root, permissions)
                .await
                .unwrap();

            // Permission Denied
            backend.remove(0).await;
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
                    .await;
            }

            let backend_root = Path::new(&backend_root);

            // permission: r-xr-xr-x
            let permissions = Permissions::from_mode(0o555);
            fs::set_permissions(backend_root.join("0"), permissions)
                .await
                .unwrap();

            // Permission Denied
            backend.truncate(0, 8, 4, 4).await;
        }
    }
}
