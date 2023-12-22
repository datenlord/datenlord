//! Mock storages for test and local nodes.

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use clippy_utilities::OverflowArithmetic;
use parking_lot::Mutex;

use super::{Block, Storage};
use crate::async_fuse::fuse::protocol::INum;

/// A "persistent" storage layer in memory.
#[derive(Debug, Default)]
pub struct MemoryStorage {
    /// The inner map of this storage
    inner: Mutex<HashMap<INum, HashMap<usize, Block>>>,
    /// Records of the flushed files
    flushed: Mutex<HashSet<INum>>,
    /// The size of block
    block_size: usize,
}

impl MemoryStorage {
    /// Creates a memory storage with block size.
    #[must_use]
    pub fn new(block_size: usize) -> Self {
        Self {
            block_size,
            ..Default::default()
        }
    }

    /// Tests if the storage contains the block of `(ino, block_id)`
    pub fn contains(&self, ino: INum, block_id: usize) -> bool {
        self.inner
            .lock()
            .get(&ino)
            .map_or(false, |file| file.contains_key(&block_id))
    }

    /// Tests if the file is flushed,
    /// and after being tested, the flushed status of the file will be set to
    /// `false` again.
    pub fn flushed(&self, ino: INum) -> bool {
        self.flushed.lock().remove(&ino)
    }
}

#[async_trait]
impl Storage for MemoryStorage {
    async fn load_from_self(&self, ino: INum, block_id: usize) -> Option<Block> {
        self.inner
            .lock()
            .get(&ino)
            .and_then(|file| file.get(&block_id).cloned())
    }

    async fn load_from_backend(&self, _: INum, _: usize) -> Option<Block> {
        None
    }

    async fn cache_block_from_backend(&self, _: INum, _: usize, _: Block) {
        unreachable!("`MemoryStorage` does not have a backend.");
    }

    async fn store(&self, ino: INum, block_id: usize, block: Block) {
        self.inner
            .lock()
            .entry(ino)
            .or_default()
            .entry(block_id)
            .or_insert_with(|| Block::new_zeroed(self.block_size))
            .update(&block);
    }

    async fn remove(&self, ino: INum) {
        self.inner.lock().remove(&ino);
    }

    async fn invalidate(&self, _: INum) {}

    async fn flush(&self, ino: INum) {
        self.flushed.lock().insert(ino);
    }

    async fn flush_all(&self) {
        let inner = self.inner.lock();
        self.flushed.lock().extend(inner.keys());
    }

    async fn truncate(&self, ino: INum, from_block: usize, to_block: usize, fill_start: usize) {
        debug_assert!(from_block >= to_block);

        if let Some(file_cache) = self.inner.lock().get_mut(&ino) {
            for block_id in to_block..from_block {
                file_cache.remove(&block_id);
            }

            if to_block > 0 && fill_start < self.block_size {
                let fill_block_id = to_block.overflow_sub(1);
                if let Some(block) = file_cache.get_mut(&fill_block_id) {
                    if fill_start >= block.end() {
                        return;
                    }

                    block.set_start(block.start().min(fill_start));

                    let fill_len = block.end().overflow_sub(fill_start);
                    let fill_content = vec![0; fill_len];
                    let write_start = fill_start.overflow_sub(block.start());

                    block
                        .make_mut_slice()
                        .get_mut(write_start..)
                        .unwrap_or_else(|| {
                            unreachable!("`fill_start` is checked to be less than block size.")
                        })
                        .copy_from_slice(&fill_content);
                    block.set_dirty();
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{MemoryStorage, Storage};
    use crate::async_fuse::memfs::cache::Block;

    const BLOCK_SIZE_IN_BYTES: usize = 8;
    const BLOCK_CONTENT: &[u8; BLOCK_SIZE_IN_BYTES] = b"foo bar ";

    #[tokio::test]
    async fn test_read_write() {
        let ino = 0;
        let block_id = 0;
        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);

        let storage = MemoryStorage::new(BLOCK_SIZE_IN_BYTES);
        storage.store(ino, block_id, block).await;
        assert!(storage.contains(ino, block_id));

        let block = storage.load(ino, block_id).await.unwrap();
        assert_eq!(block.as_slice(), BLOCK_CONTENT);

        assert!(!storage.contains(ino, 1));
        let block = storage.load(ino, 1).await;
        assert!(block.is_none());

        assert!(!storage.contains(1, 1));
        let block = storage.load(1, 1).await;
        assert!(block.is_none());

        storage.remove(ino).await;
        assert!(!storage.contains(ino, block_id));
        let block = storage.load(ino, block_id).await;
        assert!(block.is_none());
    }

    #[tokio::test]
    async fn test_flush() {
        let storage = MemoryStorage::new(BLOCK_SIZE_IN_BYTES);
        storage.flush(0).await;
        assert!(storage.flushed(0));
        assert!(!storage.flushed(0));

        storage
            .store(0, 0, Block::new_zeroed(BLOCK_SIZE_IN_BYTES))
            .await;
        storage.flush_all().await;
        assert!(storage.flushed(0));
        assert!(!storage.flushed(0));
    }

    #[tokio::test]
    async fn test_invalid() {
        let storage = MemoryStorage::new(BLOCK_SIZE_IN_BYTES);
        storage.invalidate(0).await;
    }

    #[tokio::test]
    async fn test_truncate_whole_blocks() {
        let storage = MemoryStorage::new(BLOCK_SIZE_IN_BYTES);
        for block_id in 0..8 {
            storage
                .store(0, block_id, Block::new_zeroed(BLOCK_SIZE_IN_BYTES))
                .await;
            assert!(storage.contains(0, block_id));
        }

        storage.truncate(0, 8, 4, BLOCK_SIZE_IN_BYTES).await;
        for block_id in 0..4 {
            assert!(storage.contains(0, block_id));
        }
        for block_id in 4..8 {
            assert!(!storage.contains(0, block_id));
        }
    }

    #[tokio::test]
    async fn test_truncate_may_fill() {
        let storage = MemoryStorage::new(BLOCK_SIZE_IN_BYTES);
        for block_id in 0..8 {
            storage
                .store(0, block_id, Block::new_zeroed(BLOCK_SIZE_IN_BYTES))
                .await;
            assert!(storage.contains(0, block_id));
        }

        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);

        storage.store(0, 3, block).await;
        storage.truncate(0, 8, 4, 4).await;

        let loaded = storage.load(0, 3).await.unwrap();
        assert_eq!(loaded.as_slice(), b"foo \0\0\0\0");
    }

    #[tokio::test]
    async fn test_truncate_in_the_same_block() {
        let storage = MemoryStorage::new(BLOCK_SIZE_IN_BYTES);

        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);

        storage.store(0, 0, block).await;
        storage.truncate(0, 1, 1, 4).await;

        let loaded = storage.load(0, 0).await.unwrap();
        assert_eq!(loaded.as_slice(), b"foo \0\0\0\0");
    }
}
