//! The storage manager implementation.

use std::sync::Arc;

use async_trait::async_trait;
use clippy_utilities::{Cast, OverflowArithmetic};
use parking_lot::Mutex;

use super::super::policy::LruPolicy;
use super::super::{
    format_file_path, format_path, Backend, CacheKey, FileHandle, Handles, MemoryCache, OpenFlag,
    Storage, StorageResult,
};

/// The `Storage` struct represents a storage system that implements the
/// `MockIO` trait. It manages file handles, caching, and interacts with a
/// backend storage.
#[derive(Debug)]
pub struct StorageManager {
    /// The size of a block
    block_size: usize,
    /// The file handles.
    handles: Arc<Handles>,
    /// The cache manager.
    cache: Arc<Mutex<MemoryCache<CacheKey, LruPolicy<CacheKey>>>>,
    /// The backend storage system.
    backend: Arc<dyn Backend>,
}

#[async_trait]
impl Storage for StorageManager {
    /// Opens a file with the given inode number and flags, returning a new file
    /// handle.
    #[inline]
    fn open(&self, ino: u64, fh: u64, flag: OpenFlag) {
        let handle = FileHandle::new(
            fh,
            ino,
            self.block_size,
            Arc::clone(&self.cache),
            Arc::clone(&self.backend),
            flag,
        );
        self.handles.add_handle(handle);
    }

    /// Reads data from a file specified by the file handle, starting at the
    /// given offset and reading up to `len` bytes.
    #[inline]
    async fn read(&self, _ino: u64, fh: u64, offset: u64, len: usize) -> StorageResult<Vec<u8>> {
        let handle = self.get_handle(fh);
        handle.read(offset, len.cast()).await
    }

    /// Writes data to a file specified by the file handle, starting at the
    /// given offset.
    #[inline]
    async fn write(&self, _ino: u64, fh: u64, offset: u64, buf: &[u8]) -> StorageResult<()> {
        let handle = self.get_handle(fh);
        handle.write(offset, buf).await?;
        Ok(())
    }

    /// Flushes any pending writes to a file specified by the file handle.
    #[inline]
    async fn flush(&self, _ino: u64, fh: u64) -> StorageResult<()> {
        let handle = self.get_handle(fh);
        handle.flush().await?;
        Ok(())
    }

    /// Closes a file specified by the file handle.
    #[inline]
    async fn close(&self, fh: u64) -> StorageResult<()> {
        let handle = self
            .handles
            .remove_handle(fh)
            .unwrap_or_else(|| panic!("Cannot close a file that is not open."));
        handle.close().await?;
        Ok(())
    }

    /// Truncates a file specified by the inode number to a new size, given the
    /// old size.
    #[inline]
    async fn truncate(&self, ino: u64, old_size: u64, new_size: u64) -> StorageResult<()> {
        // If new_size == old_size, do nothing
        if new_size >= old_size {
            return Ok(());
        }

        // new_size < old_size, we may need to remove some blocks
        let end = (old_size - 1).overflow_div(self.block_size.cast::<u64>()) + 1;
        let start = if new_size == 0 {
            0
        } else {
            (new_size - 1).overflow_div(self.block_size.cast::<u64>()) + 1
        };
        for block_id in start..end {
            self.backend.remove(&format_path(ino, block_id)).await?;
        }

        // Fill zeros
        if start > 0 {
            let fill_offset_in_block = new_size.cast::<usize>() % self.block_size;
            let fill_size = self.block_size - fill_offset_in_block;
            if fill_size == self.block_size {
                return Ok(());
            }
            let handle = FileHandle::new(
                0,
                ino,
                self.block_size,
                Arc::clone(&self.cache),
                Arc::clone(&self.backend),
                OpenFlag::Write,
            );
            let fill_content = vec![0; fill_size];
            handle.write(new_size, &fill_content).await?;
            handle.close().await?;
        }

        Ok(())
    }

    async fn remove(&self, ino: u64) -> StorageResult<()> {
        self.backend.remove_all(&format_file_path(ino)).await?;

        Ok(())
    }
}

impl StorageManager {
    /// Creates a new `Storage` instance.
    #[inline]
    pub fn new(
        cache: Arc<Mutex<MemoryCache<CacheKey, LruPolicy<CacheKey>>>>,
        backend: Arc<dyn Backend>,
        block_size: usize,
    ) -> Self {
        StorageManager {
            block_size,
            handles: Arc::new(Handles::new()),
            cache,
            backend,
        }
    }

    /// Returns the number of items in the cache.
    #[inline]
    #[must_use]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.cache.lock().len()
    }

    /// Get a file handle with `fh`.
    ///
    /// # Panic
    /// Panics if the file of `fh` is not open.
    fn get_handle(&self, fh: u64) -> FileHandle {
        self.handles
            .get_handle(fh)
            .unwrap_or_else(|| panic!("Cannot get a file handle that is not open."))
    }
}
