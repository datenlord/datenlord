//! The storage trait.

use async_trait::async_trait;

use crate::{fs::fs_util::FileAttr, new_storage::StorageError};

use super::StorageResult;

/// The trait defines an interface for I/O operations.
/// Current mapping is single inode -> single filehandle.
#[async_trait]
pub trait Storage {
    /// Opens a file with the given inode number and attr.
    async fn open(&self, ino: u64, attr: FileAttr);

    /// Get the file attr of the file specified by the inode number.
    /// Default implementation returns None.
    async fn getattr(&self, _ino: u64) -> StorageResult<FileAttr> {
        Err(StorageError::Internal(anyhow::anyhow!(
            "This file handle is not allowed to be written."
        )))
    }

    /// Set the file attr of the file specified by the inode number.
    /// Default implementation does nothing.
    async fn setattr(&self, _ino: u64, _attr: FileAttr) {}

    /// Reads data from a file specified by the inode number and file handle,
    /// starting at the given offset and reading up to `len` bytes.
    async fn read(
        &self,
        ino: u64,
        offset: u64,
        len: usize,
        version: u64,
        buf: &mut Vec<u8>,
    ) -> StorageResult<()>;

    /// Writes data to a file specified by the inode number and file handle,
    /// starting at the given offset.
    /// version is used to check if the file has been modified since the last write,
    /// current write implementation might need to read the file from storage first,
    /// so this function need to sync with read function.
    /// The `size` parameter is the size of hole file in this operation.
    async fn write(
        &self,
        ino: u64,
        offset: u64,
        buf: &[u8],
        size: u64,
        version: u64,
    ) -> StorageResult<()>;

    /// Truncates a file specified by the inode number to a new size,
    /// given the old size and the new size.
    async fn truncate(
        &self,
        ino: u64,
        old_size: u64,
        new_size: u64,
        version: u64,
    ) -> StorageResult<()>;

    /// Flushes any pending writes to a file specified by the inode number and
    /// file handle.
    async fn flush(&self, ino: u64) -> StorageResult<()>;

    /// Removes a file from the storage.
    async fn remove(&self, ino: u64) -> StorageResult<()>;

    /// Closes a file specified by the file handle.
    async fn close(&self, ino: u64) -> StorageResult<()>;
}
