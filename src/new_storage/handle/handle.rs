//! The file handle implementation

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use clippy_utilities::Cast;
use parking_lot::{Mutex, RwLock};

use super::super::backend::Backend;
use super::super::block_slice::offset_to_slice;
use super::super::error::StorageResult;
use super::super::policy::LruPolicy;
use super::super::{CacheKey, MemoryCache};
use super::reader::Reader;
use super::writer::Writer;

/// The `FileHandleInner` struct represents the inner state of a file handle.
/// It contains the file handle, reader, and writer.
#[derive(Debug)]
pub struct FileHandleInner {
    /// The reader.
    reader: Option<Arc<Reader>>,
    /// The writer.
    writer: Option<Arc<Writer>>,
}

/// The `OpenFlag` enum represents the mode in which a file is opened.
#[derive(Debug, Clone, Copy)]
pub enum OpenFlag {
    /// Open the file for reading.
    Read,
    /// Open the file for writing.
    Write,
    /// Open the file for reading and writing.
    ReadAndWrite,
}

impl FileHandleInner {
    /// Creates a new `FileHandleInner` instance.
    #[inline]
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        ino: u64,
        block_size: usize,
        cache: Arc<Mutex<MemoryCache<CacheKey, LruPolicy<CacheKey>>>>,
        backend: Arc<dyn Backend>,
        flag: OpenFlag,
    ) -> Self {
        let reader = match flag {
            OpenFlag::Read | OpenFlag::ReadAndWrite => Some(Arc::new(Reader::new(
                ino,
                block_size,
                Arc::clone(&cache),
                Arc::clone(&backend),
            ))),
            OpenFlag::Write => None,
        };
        let writer = match flag {
            OpenFlag::Write | OpenFlag::ReadAndWrite => Some(Arc::new(Writer::new(
                ino,
                block_size,
                Arc::clone(&cache),
                Arc::clone(&backend),
            ))),
            OpenFlag::Read => None,
        };
        FileHandleInner { reader, writer }
    }
}

/// The `FileHandle` struct represents a handle to an open file.
/// It contains an `Arc` of `RwLock<FileHandleInner>`.
#[derive(Debug, Clone)]
pub struct FileHandle {
    /// The file handle.
    fh: u64,
    /// The block size in bytes
    block_size: usize,
    /// The inner file handle
    inner: Arc<RwLock<FileHandleInner>>,
}

impl FileHandle {
    /// Creates a new `FileHandle` instance.
    pub fn new(
        fh: u64,
        ino: u64,
        block_size: usize,
        cache: Arc<Mutex<MemoryCache<CacheKey, LruPolicy<CacheKey>>>>,
        backend: Arc<dyn Backend>,
        flag: OpenFlag,
    ) -> Self {
        let inner = FileHandleInner::new(ino, block_size, cache, backend, flag);
        let inner = Arc::new(RwLock::new(inner));
        FileHandle {
            fh,
            block_size,
            inner,
        }
    }

    /// Returns the file handle.
    #[must_use]
    pub fn fh(&self) -> u64 {
        self.fh
    }

    /// Gets a reader of this file handle.
    ///
    /// # Panic
    /// Panics if the file handle is not allowed to be read.
    fn reader(&self) -> Arc<Reader> {
        self.inner
            .read()
            .reader
            .clone()
            .unwrap_or_else(|| panic!("This file handle is not allowed to be read."))
    }

    /// Gets a writer of this file handle.
    ///
    /// # Panic
    /// Panics if the file handle is not allowed to be written.
    fn writer(&self) -> Arc<Writer> {
        self.inner
            .read()
            .writer
            .clone()
            .unwrap_or_else(|| panic!("This file handle is not allowed to be written."))
    }

    /// Reads data from the file starting at the given offset and up to the
    /// given length.
    pub async fn read(&self, offset: u64, len: u64) -> StorageResult<Vec<u8>> {
        let reader = self.reader();
        let slices = offset_to_slice(self.block_size.cast(), offset, len);
        let mut buf = Vec::with_capacity(len.cast());
        reader.read(&mut buf, &slices).await?;
        Ok(buf)
    }

    /// Writes data to the file starting at the given offset.
    pub async fn write(&self, offset: u64, buf: &[u8]) -> StorageResult<()> {
        let writer = self.writer();
        let slices = offset_to_slice(self.block_size.cast(), offset, buf.len().cast());
        writer.write(buf, &slices).await
    }

    /// Extends the file from the old size to the new size.
    pub async fn extend(&self, old_size: u64, new_size: u64) -> StorageResult<()> {
        let writer = self.writer();
        writer.extend(old_size, new_size).await
    }

    /// Flushes any pending writes to the file.
    pub async fn flush(&self) {
        let writer = self.writer();
        writer.flush().await;
    }

    /// Closes the writer associated with the file handle.
    async fn close_writer(&self) {
        let writer = {
            let handle = self.inner.read();
            handle.writer.clone()
        };
        if let Some(writer) = writer {
            writer.close().await;
        }
    }

    /// Closes the file handle, closing both the reader and writer.
    pub async fn close(&self) {
        self.close_writer().await;
        if let Some(reader) = self.inner.read().reader.as_ref() {
            reader.close();
        }
    }
}

/// Number of handle shards.
const HANDLE_SHARD_NUM: usize = 100;

/// The `Handles` struct represents a collection of file handles.
/// It uses sharding to avoid lock contention.
#[derive(Debug)]
pub struct Handles {
    /// Use shard to avoid lock contention
    handles: [Arc<RwLock<Vec<FileHandle>>>; HANDLE_SHARD_NUM],
}

impl Default for Handles {
    fn default() -> Self {
        Self::new()
    }
}

impl Handles {
    /// Creates a new `Handles` instance.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        let mut handles: Vec<_> = Vec::with_capacity(HANDLE_SHARD_NUM);
        for _ in 0..HANDLE_SHARD_NUM {
            handles.push(Arc::new(RwLock::new(Vec::new())));
        }
        let handles: [_; HANDLE_SHARD_NUM] = handles.try_into().unwrap_or_else(|_| {
            unreachable!("The length should match.");
        });
        Handles { handles }
    }

    /// Returns the shard index for the given file handle.
    fn hash(fh: u64) -> usize {
        let mut hasher = DefaultHasher::new();
        fh.hash(&mut hasher);
        (hasher.finish().cast::<usize>()) % HANDLE_SHARD_NUM
    }

    /// Gets a shard of the fh.
    fn get_shard(&self, fh: u64) -> &Arc<RwLock<Vec<FileHandle>>> {
        let idx = Self::hash(fh);
        self.handles
            .get(idx)
            .unwrap_or_else(|| unreachable!("The array is ensured to be long enough."))
    }

    /// Adds a file handle to the collection.
    pub fn add_handle(&self, fh: FileHandle) {
        let shard = self.get_shard(fh.fh());
        let mut shard_lock = shard.write();
        shard_lock.push(fh);
    }

    /// Removes a file handle from the collection.'
    #[must_use]
    pub fn remove_handle(&self, fh: u64) -> Option<FileHandle> {
        let shard = self.get_shard(fh);
        let mut shard_lock = shard.write();
        shard_lock
            .iter()
            .position(|h| h.fh() == fh)
            .map(|pos| shard_lock.remove(pos))
    }

    /// Returns a file handle from the collection.
    #[must_use]
    pub fn get_handle(&self, fh: u64) -> Option<FileHandle> {
        let shard = self.get_shard(fh);
        let shard_lock = shard.read();
        let fh = shard_lock.iter().find(|h| h.fh() == fh)?;
        Some(fh.clone())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::new_storage::backend::backend_impl::tmp_fs_backend;
    use crate::new_storage::block::BLOCK_SIZE;

    #[tokio::test]
    async fn test_file_handle() {
        let cache = Arc::new(Mutex::new(MemoryCache::new(100, BLOCK_SIZE)));
        let backend = Arc::new(tmp_fs_backend().unwrap());
        let handles = Arc::new(Handles::new());
        let ino = 1;
        let fh = 1;
        let file_handle =
            FileHandleInner::new(ino, BLOCK_SIZE, cache, backend, OpenFlag::ReadAndWrite);
        let file_handle = Arc::new(RwLock::new(file_handle));
        let file_handle = FileHandle {
            fh,
            block_size: BLOCK_SIZE,
            inner: file_handle,
        };
        handles.add_handle(file_handle.clone());
        let buf = vec![b'1', b'2', b'3', b'4'];
        file_handle.write(0, &buf).await.unwrap();
        let read_buf = file_handle.read(0, 4).await.unwrap();
        assert_eq!(read_buf, buf);
        file_handle.flush().await;
    }
}
