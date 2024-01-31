//! The backend implementation.

use async_trait::async_trait;
use clippy_utilities::OverflowArithmetic;
use datenlord::config::{StorageParams, StorageS3Config};
use futures::{stream, AsyncReadExt, AsyncWriteExt, StreamExt};
use opendal::services::{Fs, S3};
use opendal::{ErrorKind, Operator};

use crate::async_fuse::fuse::protocol::INum;
use crate::storage::error::StorageResult;
use crate::storage::{Block, Storage};

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
    async fn load_from_self(&self, ino: INum, block_id: usize) -> StorageResult<Option<Block>> {
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
                Ok(None)
            } else {
                panic!(
                    "Failed to load a block from backend where ino={ino} and block={block_id}: {e}"
                );
            }
        } else {
            Ok(Some(block))
        }
    }

    async fn load_from_backend(&self, _: INum, _: usize) -> StorageResult<Option<Block>> {
        // This storage has no backend.
        Ok(None)
    }

    async fn cache_block_from_backend(&self, _: INum, _: usize, _: Block) -> StorageResult<()> {
        unreachable!("This storage has no backend, and has no cache.");
    }

    async fn store(&self, ino: INum, block_id: usize, block: Block) -> StorageResult<()> {
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
            return Ok(());
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

        Ok(())
    }

    async fn remove(&self, ino: INum) -> StorageResult<()> {
        self.operator
            .remove_all(&get_file_path(ino))
            .await
            .unwrap_or_else(|e| {
                panic!("Failed to remove a file from the backend of ino={ino}: {e}")
            });

        Ok(())
    }

    async fn invalidate(&self, _: INum) -> StorageResult<()> {
        // This storage has no cache, therefore, its contents cannot be
        // invalidated.
        Ok(())
    }

    async fn flush(&self, _: INum) -> StorageResult<()> {
        // This storage has no cache and backend, therefore, there is no need to
        // flush its data.
        Ok(())
    }

    async fn flush_all(&self) -> StorageResult<()> {
        // This storage has no cache and backend, therefore, there is no need to
        // flush its data.
        Ok(())
    }

    async fn truncate(
        &self,
        ino: INum,
        from_block: usize,
        to_block: usize,
        fill_start: usize,
    ) -> StorageResult<()> {
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
                return Ok(());
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

        Ok(())
    }
}
