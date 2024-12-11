//! The storage trait, as a abstraction of the storage layers.

use std::sync::Arc;

use async_trait::async_trait;
use tracing::error;

use crate::fs::fs_util::INum;

use super::error::StorageResult;
use super::{Block, StorageError};

/// The `Storage` trait. It handles blocks with storage such as in-memory cache,
/// in-disk cache and `S3Backend`.
#[async_trait]
pub trait Storage {
    // Required methods

    /// Loads a block from `self` but not from its backend.
    async fn load_from_self(&self, ino: INum, block_id: usize) -> StorageResult<Option<Block>>;

    /// Loads a block from `self` but not from its backend, this function support load with version.
    async fn load_from_self_with_version(
        &self,
        _ino: INum,
        _block_id: usize,
        _version: u64,
    ) -> StorageResult<Option<Block>> {
        Err(StorageError::Unimplemented)
    }

    /// Loads a block from the `backend`.
    async fn load_from_backend(&self, ino: INum, block_id: usize) -> StorageResult<Option<Block>>;

    /// Loads a block from the `backend` with version.
    async fn load_from_backend_with_version(
        &self,
        _ino: INum,
        _block_id: usize,
        _version: u64,
    ) -> StorageResult<Option<Block>> {
        Err(StorageError::Unimplemented)
    }

    /// Caches a block that is just loaded from the backend.
    ///
    /// This may evict a block to the backend.
    async fn cache_block_from_backend(
        &self,
        ino: INum,
        block_id: usize,
        block: Block,
    ) -> StorageResult<()>;
    /// Store a block to the storage.
    async fn store(&self, ino: INum, block_id: usize, block: Block) -> StorageResult<()>;

    /// Remove a file from storage.
    async fn remove(&self, ino: INum) -> StorageResult<()>;

    /// Invalidate caches of a file, if the storage contains caches.
    async fn invalidate(&self, ino: INum) -> StorageResult<()>;

    /// Flush a file
    async fn flush(&self, ino: INum) -> StorageResult<()>;

    /// Flush all files
    async fn flush_all(&self) -> StorageResult<()>;

    /// Truncate a file from the block id of `from` to a lower one, and fill
    /// zeros in the end of the last block. Both `from` and `to` are in
    /// blocks, and are the next id to the last valid block of a file.
    /// If `fill_start` is set to block size, this method should not fill any
    /// zero. After the truncating, the block range of the file is
    /// `[0, to_block)`.
    async fn truncate(
        &self,
        ino: INum,
        from_block: usize,
        to_block: usize,
        fill_start: usize,
    ) -> StorageResult<()>;

    // Provided methods

    /// This function is used to enhance load with mtime version.
    /// We will merge this function to load_from_self_with_version in the future.
    async fn load_with_version(
        &self,
        ino: INum,
        block_id: usize,
        version: u64,
    ) -> StorageResult<Option<Block>> {
        match self
            .load_from_self_with_version(ino, block_id, version)
            .await
        {
            Ok(Some(block_in_cache)) => Ok(Some(block_in_cache)),
            Ok(None) => {
                error!("None in load_with_version");
                match self
                    .load_from_backend_with_version(ino, block_id, version)
                    .await
                {
                    Ok(Some(block)) => {
                        self.cache_block_from_backend(ino, block_id, block.clone())
                            .await?;
                        Ok(Some(block))
                    }
                    Ok(None) => Ok(None),
                    Err(e) => {
                        match e {
                            // This function is not implemented, we will use the default load function.
                            StorageError::Unimplemented => {
                                let res = self.load_from_backend(ino, block_id).await?;
                                if let Some(block) = res {
                                    self.cache_block_from_backend(ino, block_id, block.clone())
                                        .await?;
                                    Ok(Some(block))
                                } else {
                                    Ok(None)
                                }
                            }
                            _ => Err(e),
                        }
                    }
                }
            }
            Err(e) => {
                error!("Error in load_with_version: {:?}", e);
                match e {
                    // This function is not implemented, we will use the default load function.
                    StorageError::Unimplemented => {
                        if let Some(block_in_cache) = self.load_from_self(ino, block_id).await? {
                            Ok(Some(block_in_cache))
                        } else {
                            match self
                                .load_from_backend_with_version(ino, block_id, version)
                                .await
                            {
                                Ok(Some(block)) => {
                                    self.cache_block_from_backend(ino, block_id, block.clone())
                                        .await?;
                                    Ok(Some(block))
                                }
                                Ok(None) => Ok(None),
                                Err(e) => {
                                    match e {
                                        // This function is not implemented, we will use the default load function.
                                        StorageError::Unimplemented => {
                                            let res = self.load_from_backend(ino, block_id).await?;
                                            if let Some(block) = res {
                                                self.cache_block_from_backend(
                                                    ino,
                                                    block_id,
                                                    block.clone(),
                                                )
                                                .await?;
                                                Ok(Some(block))
                                            } else {
                                                Ok(None)
                                            }
                                        }
                                        _ => Err(e),
                                    }
                                }
                            }
                        }
                    }
                    _ => Err(e),
                }
            }
        }
    }

    /// Load a block from the storage.
    async fn load(&self, ino: INum, block_id: usize) -> StorageResult<Option<Block>> {
        if let Some(block_in_cache) = self.load_from_self(ino, block_id).await? {
            Ok(Some(block_in_cache))
        } else {
            let res = self.load_from_backend(ino, block_id).await?;
            if let Some(block) = res {
                self.cache_block_from_backend(ino, block_id, block.clone())
                    .await?;
                Ok(Some(block))
            } else {
                Ok(None)
            }
        }
    }
}

#[async_trait]
impl<T> Storage for Arc<T>
where
    T: Storage + Send + Sync,
{
    async fn load_from_self(&self, ino: INum, block_id: usize) -> StorageResult<Option<Block>> {
        self.as_ref().load_from_self(ino, block_id).await
    }

    async fn load_from_self_with_version(
        &self,
        ino: INum,
        block_id: usize,
        version: u64,
    ) -> StorageResult<Option<Block>> {
        self.as_ref()
            .load_from_self_with_version(ino, block_id, version)
            .await
    }

    async fn load_from_backend(&self, ino: INum, block_id: usize) -> StorageResult<Option<Block>> {
        self.as_ref().load_from_backend(ino, block_id).await
    }

    async fn load_from_backend_with_version(
        &self,
        ino: INum,
        block_id: usize,
        version: u64,
    ) -> StorageResult<Option<Block>> {
        self.as_ref()
            .load_from_backend_with_version(ino, block_id, version)
            .await
    }

    async fn cache_block_from_backend(
        &self,
        ino: INum,
        block_id: usize,
        block: Block,
    ) -> StorageResult<()> {
        self.as_ref()
            .cache_block_from_backend(ino, block_id, block)
            .await
    }

    async fn load(&self, ino: INum, block_id: usize) -> StorageResult<Option<Block>> {
        self.as_ref().load(ino, block_id).await
    }

    async fn store(&self, ino: INum, block_id: usize, block: Block) -> StorageResult<()> {
        self.as_ref().store(ino, block_id, block).await
    }

    async fn remove(&self, ino: INum) -> StorageResult<()> {
        self.as_ref().remove(ino).await
    }

    async fn invalidate(&self, ino: INum) -> StorageResult<()> {
        self.as_ref().invalidate(ino).await
    }

    async fn flush(&self, ino: INum) -> StorageResult<()> {
        self.as_ref().flush(ino).await
    }

    async fn flush_all(&self) -> StorageResult<()> {
        self.as_ref().flush_all().await
    }

    async fn truncate(
        &self,
        ino: INum,
        from_block: usize,
        to_block: usize,
        fill_start: usize,
    ) -> StorageResult<()> {
        self.as_ref()
            .truncate(ino, from_block, to_block, fill_start)
            .await
    }
}
