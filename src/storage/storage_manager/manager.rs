//! The storage manager implementation.

use std::sync::Arc;
use std::time::SystemTime;

use anyhow::Context;
use clippy_utilities::OverflowArithmetic;
use lockfree_cuckoohash::{pin, LockFreeCuckooHash as HashMap};
use tokio::task;

use super::super::{Block, Storage};
use crate::async_fuse::fuse::protocol::INum;
use crate::common::error::DatenLordResult;

/// The storage manager, which exposes the interfaces to `FileSystem` for
/// interacting with the storage layers.
#[derive(Debug)]
pub struct StorageManager<S> {
    /// The top-level storage, `InMemoryCache` for example
    storage: Arc<S>,
    /// Block size in bytes
    block_size: usize,
    /// Last modified times of the cache (on file level)
    mtimes: HashMap<INum, SystemTime>,
}

impl<S> StorageManager<S>
where
    S: Storage + Send + Sync + 'static,
{
    /// Create a `StorageManager` with the top-level `Storage`.
    pub fn new(storage: S, block_size: usize) -> Self {
        StorageManager {
            storage: Arc::new(storage),
            block_size,
            mtimes: HashMap::new(),
        }
    }

    /// Convert offset in byte to block id via the equation:
    ///
    /// `block_id = offset / block_size`
    fn offset_to_block_id(&self, offset: usize) -> usize {
        offset.overflow_div(self.block_size)
    }

    /// Load blocks from the storage concurrently.
    async fn load_blocks(
        &self,
        ino: INum,
        mtime: SystemTime,
        start_block: usize,
        end_block: usize,
    ) -> DatenLordResult<Vec<Block>> {
        let mut handles = vec![];

        // Convert time to u64
        let mtime = mtime
            .duration_since(SystemTime::UNIX_EPOCH)
            .context("Failed to convert time to u64")?
            .as_secs();

        for block_id in start_block..end_block {
            let storage = Arc::clone(&self.storage);
            let handle =
                task::spawn(async move { storage.load_with_version(ino, block_id, mtime).await });
            handles.push(handle);
        }

        let mut blocks = vec![];

        for (handle, block_id) in handles.into_iter().zip(start_block..end_block) {
            let block = handle
                .await?
                .context("Storage manager failed to load blocks.")?;
            if let Some(block) = block {
                blocks.push(block);
            } else {
                let mut zero_filled = Block::new_zeroed(self.block_size);
                zero_filled.set_dirty(true);
                self.storage
                    .store(ino, block_id, zero_filled.clone())
                    .await
                    .context("Storage Manager failed to store blocks")?;
                blocks.push(zero_filled);
            }
        }

        Ok(blocks)
    }

    /// Store blocks into the storage concurrently.
    async fn store_blocks(
        &self,
        ino: INum,
        start_block: usize,
        io_blocks: impl Iterator<Item = Block>,
    ) -> DatenLordResult<()> {
        let mut handles = vec![];

        for (mut io_block, block_id) in io_blocks.zip(start_block..) {
            io_block.set_dirty(true);
            let storage = Arc::clone(&self.storage);
            let handle = task::spawn(async move { storage.store(ino, block_id, io_block).await });
            handles.push(handle);
        }

        for handle in handles {
            handle
                .await?
                .context("Storage manager failed to store blocks")?;
        }

        Ok(())
    }

    /// Convert slice to `Block`s.
    fn make_blocks_from_slice(&self, offset: usize, data: &[u8]) -> Vec<Block> {
        let data_len = data.len();
        let block_num = {
            let end_offset = offset.overflow_add(data_len);
            let end_block_id = self.offset_to_block_id(end_offset.overflow_sub(1));
            let start_block_id = self.offset_to_block_id(offset);
            end_block_id.overflow_sub(start_block_id).overflow_add(1)
        };
        let mut blocks = vec![];

        // Handle the first block
        let first_block_offset = offset.overflow_rem(self.block_size);
        let first_block_end = first_block_offset
            .overflow_add(data_len)
            .min(self.block_size);
        let first_block_length = first_block_end.overflow_sub(first_block_offset);

        let (first_block_data, rest_data) = data.split_at(first_block_length);

        let first_block = Block::from_slice_with_range(
            self.block_size,
            first_block_offset,
            first_block_end,
            first_block_data,
        );
        blocks.push(first_block);

        // Handle the rest blocks
        if data_len > first_block_length {
            for (i, chunk) in rest_data.chunks(self.block_size).enumerate() {
                let end = if i == block_num.overflow_sub(2) {
                    first_block_offset
                        .overflow_add(data_len)
                        .overflow_sub(1)
                        .overflow_rem(self.block_size)
                        .overflow_add(1)
                } else {
                    self.block_size
                };

                let block = Block::from_slice_with_range(self.block_size, 0, end, chunk);
                blocks.push(block);
            }
        }

        blocks
    }

    /// Load data from storage.
    pub async fn load(
        &self,
        ino: INum,
        offset: usize,
        len: usize,
        mtime: SystemTime,
    ) -> DatenLordResult<Vec<Block>> {
        // Check if the cache is valid.
        let invalid = {
            let guard = pin();
            let cache_mtime = self.mtimes.get(&ino, &guard);
            cache_mtime != Some(&mtime)
        };

        if invalid {
            self.storage
                .invalidate(ino)
                .await
                .context("Storage manager failed to invalidate the cache of a file")?;
        }

        if len == 0 {
            return Ok(vec![]);
        }

        // Calculate the `[start_block, end_block)` range.
        let start_block = self.offset_to_block_id(offset);
        let end_block = self
            .offset_to_block_id(offset.overflow_add(len).overflow_sub(1))
            .overflow_add(1);

        let mut blocks = self.load_blocks(ino, mtime, start_block, end_block).await?;

        // If the cache is invalidated, it must be re-fetched from backend.
        // So the mtime of the cache should be updated to the passed-in one.
        if invalid {
            self.mtimes.insert(ino, mtime);
        }

        if let Some(first_block) = blocks.first_mut() {
            first_block.set_start(offset.overflow_rem(self.block_size));
        }

        if let Some(last_block) = blocks.last_mut() {
            last_block.set_end(
                offset
                    .overflow_add(len)
                    .overflow_sub(1)
                    .overflow_rem(self.block_size)
                    .overflow_add(1),
            );
        }

        Ok(blocks)
    }

    /// Store data into storage.
    pub async fn store(
        &self,
        ino: INum,
        offset: usize,
        data: &[u8],
        mtime: SystemTime,
    ) -> DatenLordResult<SystemTime> {
        // Check if the cache is valid.
        let invalid = {
            let guard = pin();
            let cache_mtime = self.mtimes.get(&ino, &guard);
            cache_mtime != Some(&mtime)
        };

        if invalid {
            self.storage
                .invalidate(ino)
                .await
                .context("Storage manager failed to invalidate the cache of a file")?;
        }

        if data.is_empty() {
            // The cache is invalid, but no new blocks will be loaded into the cache
            // thus the mtime of this file is removed.
            if invalid {
                self.mtimes.remove(&ino);
            }
            // No data will be written, so the passed-in mtime will be passed-out
            // changelessly.
            return Ok(mtime);
        }

        let start_block = self.offset_to_block_id(offset);

        let io_blocks = self.make_blocks_from_slice(offset, data);

        self.store_blocks(ino, start_block, io_blocks.into_iter())
            .await?;

        // As the cache is overwritten, the cache mtime should be set to now.
        let new_mtime = SystemTime::now();
        self.mtimes.insert(ino, new_mtime);

        Ok(new_mtime)
    }

    /// Remove a file from the storage.
    pub async fn remove(&self, ino: INum) -> DatenLordResult<()> {
        self.mtimes.remove(&ino);
        self.storage
            .remove(ino)
            .await
            .context("Storage manager failed to remove a file")?;
        Ok(())
    }

    /// Flush the cache to the persistent layer.
    pub async fn flush(&self, ino: INum) -> DatenLordResult<()> {
        self.storage
            .flush(ino)
            .await
            .context("Storage manager failed to flush a file")?;
        Ok(())
    }

    /// Flush all file in the cache to persistent layer.
    pub async fn flush_all(&self) -> DatenLordResult<()> {
        self.storage
            .flush_all()
            .await
            .context("Storage manager failed to flush all files")?;
        Ok(())
    }

    /// Truncate a file, from size of `from` to size of `to`.
    /// Both `from` and `to` are in bytes, and are excluded from the range.
    /// After the truncating, the valid range of a file in the storage
    /// is `[0, to)`.
    ///
    /// This method performs a store operation, therefore it also takes a
    /// `mtime` and returns a new `mtime` like `store` method.
    pub async fn truncate(
        &self,
        ino: INum,
        from: usize,
        to: usize,
        mtime: SystemTime,
    ) -> DatenLordResult<SystemTime> {
        let invalid = {
            let guard = pin();
            let cache_mtime = self.mtimes.get(&ino, &guard);
            cache_mtime != Some(&mtime)
        };

        if invalid {
            self.storage
                .invalidate(ino)
                .await
                .context("Storage manager failed to invalidate the cache of a file")?;
        }

        if from <= to {
            if invalid {
                self.mtimes.remove(&ino);
            }

            return Ok(mtime);
        }

        let from_block = self
            .offset_to_block_id(from.overflow_sub(1))
            .overflow_add(1);

        let to_block = if to == 0 {
            0
        } else {
            self.offset_to_block_id(to.overflow_sub(1)).overflow_add(1)
        };

        let fill_start = if to == 0 {
            0
        } else {
            to.overflow_sub(1)
                .overflow_rem(self.block_size)
                .overflow_add(1)
        };

        self.storage
            .truncate(ino, from_block, to_block, fill_start)
            .await
            .context("Storage manager failed to truncate a file")?;

        let new_mtime = SystemTime::now();
        self.mtimes.insert(ino, new_mtime);

        Ok(new_mtime)
    }
}
