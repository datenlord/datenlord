//! The storage manager implementation.

use std::sync::Arc;

use async_trait::async_trait;
use clippy_utilities::{Cast, OverflowArithmetic};
use parking_lot::Mutex;
use tracing::{error, info};

use crate::async_fuse::memfs::{FileAttr, MetaData};
use crate::new_storage::StorageError;

use super::super::policy::LruPolicy;
use super::super::{
    format_file_path, format_path, Backend, CacheKey, FileHandle, Handles, MemoryCache, Storage,
    StorageResult,
};

/// The `Storage` struct represents a storage system that implements the
/// `MockIO` trait. It manages file handles, caching, and interacts with a
/// backend storage.
#[derive(Debug)]
pub struct StorageManager<M: MetaData + Send + Sync + 'static> {
    /// The size of a block
    block_size: usize,
    /// The file handles.
    handles: Arc<Handles>,
    /// The cache manager.
    cache: Arc<Mutex<MemoryCache<CacheKey, LruPolicy<CacheKey>>>>,
    /// The backend storage system.
    backend: Arc<dyn Backend>,
    /// Fs metadata
    metadata_client: Arc<M>,
}

#[async_trait]
impl<M: MetaData + Send + Sync + 'static> Storage for StorageManager<M> {
    /// Opens a file with the given inode number, in client side will check the open flag, so we don't need to
    /// check it here.
    #[inline]
    #[allow(clippy::unwrap_used)]
    async fn open(&self, ino: u64, attr: FileAttr) {
        let flag = self
            .handles
            .reopen_or_create_handle(
                ino,
                self.block_size,
                Arc::clone(&self.cache),
                Arc::clone(&self.backend),
                Arc::clone(&self.metadata_client),
                attr,
            )
            .await;
        info!("open filehandle {:?} with flag {:?}", ino, flag);
    }

    /// Get the file attr of the file specified by the inode number.
    /// Default implementation returns None.
    async fn getattr(&self, ino: u64) -> StorageResult<FileAttr> {
        match self.get_handle(ino).await {
            Some(fh) => Ok(fh.getattr()),
            None => Err(StorageError::Internal(anyhow::anyhow!(
                "This file handle is not exists."
            ))),
        }
    }

    /// Set the file attr of the file specified by the inode number.
    /// Default implementation does nothing.
    #[inline]
    async fn setattr(&self, ino: u64, attr: FileAttr) {
        info!("setattr: ino: {} with attr: {:?}", ino, attr);
        match self.get_handle(ino).await {
            Some(fh) => fh.setattr(attr),
            None => {
                // Ignore the error here, because the file may be closed or not opened.
                error!("Cannot set attr for a file that is not open.");
            }
        }
    }

    /// Reads data from a file specified by the file handle, starting at the
    /// given offset and reading up to `len` bytes.
    #[inline]
    async fn read(
        &self,
        ino: u64,
        offset: u64,
        len: usize,
        version: u64,
    ) -> StorageResult<Vec<u8>> {
        match self.get_handle(ino).await {
            Some(fh) => fh.read(offset, len.cast(), version).await,
            None => Err(StorageError::Internal(anyhow::anyhow!(
                "This file handle is not exists."
            ))),
        }
    }

    /// Writes data to a file specified by the file handle, starting at the
    /// given offset.
    #[inline]
    async fn write(
        &self,
        ino: u64,
        offset: u64,
        buf: &[u8],
        size: u64,
        version: u64,
    ) -> StorageResult<()> {
        match self.get_handle(ino).await {
            Some(fh) => fh.write(offset, buf, size, version).await,
            None => {
                panic!("Cannot write to a file that is not open.");
            }
        }
    }

    /// Flushes any pending writes to a file specified by the file handle.
    #[inline]
    async fn flush(&self, ino: u64) -> StorageResult<()> {
        match self.get_handle(ino).await {
            Some(fh) => fh.flush().await,
            None => {
                panic!("Cannot flush a file that is not open.");
            }
        }
    }

    /// Closes a file specified by the file handle.
    #[inline]
    async fn close(&self, ino: u64) -> StorageResult<()> {
        info!("try to close filehandle {:?}", ino);
        if let Some(_fh) = self.handles.close_handle(ino).await? {
            info!("close filehandle {ino:?} ok");
            Ok(())
        } else {
            info!("close filehandle {ino:?} ok");
            Ok(())
        }
    }

    /// Truncates a file specified by the inode number to a new size, given the
    /// old size.
    #[inline]
    async fn truncate(
        &self,
        ino: u64,
        old_size: u64,
        new_size: u64,
        version: u64,
    ) -> StorageResult<()> {
        info!(
            "truncate: ino: {} old_size: {} new_size: {} version: {}",
            ino, old_size, new_size, version
        );
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

            let fill_content = vec![0; fill_size];
            match self.get_handle(ino).await {
                Some(fh) => {
                    fh.write(new_size, &fill_content, new_size, version).await?;
                }
                None => {
                    panic!("Cannot write to a file that is not open.");
                }
            }
        }

        Ok(())
    }

    async fn remove(&self, ino: u64) -> StorageResult<()> {
        self.backend.remove_all(&format_file_path(ino)).await?;

        Ok(())
    }
}

impl<M: MetaData + Send + Sync + 'static> StorageManager<M> {
    /// Creates a new `Storage` instance.
    #[inline]
    pub fn new(
        cache: Arc<Mutex<MemoryCache<CacheKey, LruPolicy<CacheKey>>>>,
        backend: Arc<dyn Backend>,
        block_size: usize,
        metadata_client: Arc<M>,
    ) -> Self {
        StorageManager {
            block_size,
            handles: Arc::new(Handles::new()),
            cache,
            backend,
            metadata_client,
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
    async fn get_handle(&self, fh: u64) -> Option<FileHandle> {
        self.handles.get_handle(fh).await
    }
}
