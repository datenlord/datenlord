//! The file handle implementation

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use crate::new_storage::{format_path, Block, BlockSlice, StorageError};
use bytes::Bytes;
use clippy_utilities::Cast;
use nix::fcntl::OFlag;
use parking_lot::{Mutex, RwLock};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{error, warn};

use super::super::backend::Backend;
use super::super::block_slice::offset_to_slice;
use super::super::error::StorageResult;
use super::super::policy::LruPolicy;
use super::super::{CacheKey, MemoryCache};
use crate::async_fuse::memfs::MetaData;

/// The `FileHandleInner` struct represents the inner state of a file handle.
/// It contains the file handle, reader, and writer.
pub struct FileHandleInner {
    /// The inode number associated with the file being read.
    ino: u64,
    /// Integrate openfiles in current strutcture, representing an open file with its attributes and open count.
    /// The `attr` field contains the file attributes, while `open_cnt` keeps track
    /// of the number of times this file is currently opened.
    /// The number of times this file is currently opened.
    open_cnt: AtomicU32,
    /// The block size
    block_size: usize,
    /// The `MemoryCache`
    cache: Arc<Mutex<MemoryCache<CacheKey, LruPolicy<CacheKey>>>>,
    /// The backend storage system.
    backend: Arc<dyn Backend>,
    /// The access keys.
    access_keys: Mutex<Vec<CacheKey>>,
    /// The sender to send tasks to the write back worker.
    write_back_sender: Sender<Task>,
    /// The handle to the write back worker.
    write_back_handle: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for FileHandleInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileHandleInner")
            .field("ino", &self.ino)
            .field("block_size", &self.block_size)
            .field("cache", &self.cache)
            .field("backend", &self.backend)
            .field("access_keys", &self.access_keys)
            .finish_non_exhaustive()
    }
}

/// The `MetaTask` enum represents the different types of meta tasks that the
/// write back worker can perform.
#[derive(Debug)]
enum MetaTask {
    /// A pending write task.
    Pending(Arc<MetaCommitTask>),
    /// A flush task, which means we need to flush the meta to the meta data server.
    Flush(oneshot::Sender<Option<StorageError>>),
    /// A finish task.
    Finish(oneshot::Sender<Option<StorageError>>),
}

/// The `MetaCommitTask` struct represents a meta commit task.
#[derive(Debug)]
struct MetaCommitTask {
    /// The inode number associated with the file being written.
    ino: u64,
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

impl From<u32> for OpenFlag {
    fn from(value: u32) -> Self {
        let write_flags = OFlag::O_APPEND | OFlag::O_WRONLY;

        let flags = OFlag::from_bits_truncate(value.cast());
        if flags.intersects(write_flags) {
            Self::Write
        } else if flags.intersects(OFlag::O_RDWR) {
            Self::ReadAndWrite
        } else {
            Self::Read
        }
    }
}

/// The `Task` enum represents the different types of tasks that the write back
/// worker can perform.
#[derive(Debug)]
enum Task {
    /// A pending write task.
    Pending(Arc<WriteTask>),
    /// A flush task.
    Flush(oneshot::Sender<Option<StorageError>>),
    /// A finish task.
    Finish(oneshot::Sender<Option<StorageError>>),
}

/// The `WriteTask` struct represents a write task
#[derive(Debug)]
struct WriteTask {
    /// The cache manager.
    cache: Arc<Mutex<MemoryCache<CacheKey, LruPolicy<CacheKey>>>>,
    /// The backend storage system.
    backend: Arc<dyn Backend>,
    /// The inode number associated with the file being written.
    ino: u64,
    /// The block id.
    block_id: u64,
    /// The block to be written.
    block: Arc<RwLock<Block>>,
    /// The file size in this operation.
    file_size: u64,
}

/// Write a block back to the backend.
async fn write_back_block(task: Arc<WriteTask>) -> StorageResult<()> {
    let path = format_path(task.ino, task.block_id);
    loop {
        let (content, version) = {
            let block = task.block.read();
            if !block.dirty() {
                // The block has been flushed previously, skip
                return Ok(());
            }
            let content = Bytes::copy_from_slice(block.as_ref());
            let version = block.version();
            (content, version)
        };

        task.backend.write(&path, &content).await?;
        {
            let mut block = task.block.write();
            // Check version
            if block.version() != version {
                warn!(
                    "Version mismatch previous: {}, current: {}",
                    version,
                    block.version()
                );
                continue;
            }
            block.set_dirty(false);
        }
        {
            task.cache.lock().unpin(&CacheKey {
                ino: task.ino,
                block_id: task.block_id,
            });
            break;
        }
    }

    Ok(())
}

/// Write the blocks to the backend storage system concurrently.
async fn write_blocks<M: MetaData + Send + Sync + 'static>(
    tasks: &Vec<Arc<WriteTask>>,
    metadata_client: Arc<M>,
) -> Option<StorageError> {
    let mut handles = Vec::new();
    let mut result = None;
    for task in tasks {
        let handle = tokio::spawn(write_back_block(Arc::clone(task)));
        handles.push(handle);
    }
    // Make sure current blocks is finished.
    for handle in handles {
        match handle.await {
            Err(e) => {
                result = Some(StorageError::Internal(e.into()));
            }
            Ok(Err(e)) => {
                result = Some(e);
            }
            _ => {}
        }
    }

    // Commit current metadata to the meta data server.
    let task = tasks.get(0)?;
    if let Err(e) = metadata_client
        .write_remote_helper(task.ino.to_owned())
        .await
    {
        error!("Failed to commit meta data, the error is {e}.");
    }

    result
}

/// The `write_back_work` function represents the write back worker.
#[allow(clippy::pattern_type_mismatch)] // Raised by `tokio::select!`
async fn write_back_work<M: MetaData + Send + Sync + 'static>(
    mut write_back_receiver: Receiver<Task>,
    metadata_client: Arc<M>,
) {
    //  Create a timer to flush the cache every 200ms.
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(200));
    let mut tasks = Vec::new();
    let mut result = None;
    loop {
        tokio::select! {
            Some(task) = write_back_receiver.recv() => {
                match task {
                    Task::Pending(task) => {
                        tasks.push(task);
                        if tasks.len() >= 10 {
                            let res = write_blocks(&tasks, Arc::clone(&metadata_client)).await;
                            if let Some(e) = res {
                                result.get_or_insert(e);
                            }
                            tasks.clear();
                        }
                    }
                    Task::Flush(tx) => {
                        let res = write_blocks(&tasks, Arc::clone(&metadata_client)).await;
                        tasks.clear();
                        let res = result.take().or(res);
                        if let Err(Some(e)) = tx.send(res) {
                            error!("Failed to send storage error back to `Writer`, the error is {e}.");
                        }
                    }
                    Task::Finish(tx) => {
                        let res = write_blocks(&tasks, Arc::clone(&metadata_client)).await;
                        tasks.clear();
                        let res = result.take().or(res);
                        if let Err(Some(e)) = tx.send(res) {
                            error!("Failed to send storage error back to `Writer`, the error is {e}.");
                        }

                        // Check last write task size with mem attr
                        return;
                    }
                }
            }
            _ = interval.tick() => {
                write_blocks(&tasks, Arc::clone(&metadata_client)).await;
                tasks.clear();
            }
        }
    }
}

impl FileHandleInner {
    /// Creates a new `FileHandleInner` instance.
    #[inline]
    #[allow(clippy::needless_pass_by_value)]
    pub fn new<M: MetaData + Send + Sync + 'static>(
        ino: u64,
        block_size: usize,
        cache: Arc<Mutex<MemoryCache<CacheKey, LruPolicy<CacheKey>>>>,
        backend: Arc<dyn Backend>,
        metadata_client: Arc<M>,
    ) -> Self {
        let (write_back_tx, write_back_rx) = tokio::sync::mpsc::channel(100);
        // TODO: Move to task manager
        // let handle = TASK_MANAGER.spawn(TaskName::WriteBack, write_back_work(rx)).await;
        let write_back_handle = tokio::spawn(write_back_work(write_back_rx, metadata_client));

        FileHandleInner {
            ino,
            block_size,
            cache,
            backend,
            // The open count is initialized to 0, open() method will increase this flag.
            open_cnt: AtomicU32::new(0),
            access_keys: Mutex::new(Vec::new()),
            write_back_sender: write_back_tx,
            write_back_handle: tokio::sync::Mutex::new(Some(write_back_handle)),
        }
    }

    /// Record the block access.
    fn access(&self, block_id: u64) {
        let key = CacheKey {
            ino: self.ino,
            block_id,
        };
        let mut access_keys = self.access_keys.lock();
        access_keys.push(key);
    }

    /// Fetch the block from the cache manager.
    #[inline]
    pub async fn fetch_block(&self, block_id: u64) -> StorageResult<Arc<RwLock<Block>>> {
        let key = CacheKey {
            ino: self.ino,
            block_id,
        };

        // Fetch the block from the cache manager.
        {
            let cache = self.cache.lock();
            if let Some(block) = cache.fetch(&key) {
                return Ok(block);
            }
        }

        // Fetch the block from the backend storage system.
        let path = format_path(self.ino, block_id);
        // There is a gap between the block is created and the content is read from the
        // backend. But according to the current design, concurrency
        // read/write is not supported.
        let mut buf = vec![0; self.block_size];
        self.backend.read(&path, &mut buf).await?;
        let block = {
            let mut cache = self.cache.lock();
            cache
                .new_block(&key, &buf)
                .ok_or(StorageError::OutOfMemory)?
        };

        Ok(block)
    }

    /// Reads data from the file starting at the given offset and up to the
    /// given length.
    pub async fn read(&self, buf: &mut Vec<u8>, slices: &[BlockSlice]) -> StorageResult<usize> {
        for slice in slices {
            let block_id = slice.block_id;
            self.access(block_id);
            // Block's pin count is increased by 1.
            let block = self.fetch_block(block_id).await?;
            {
                // Copy the data from the block to the buffer.
                let block = block.read();
                assert!(block.pin_count() >= 1);
                let offset = slice.offset.cast();
                let size: usize = slice.size.cast();
                let end = offset + size;
                let block_size = block.len();
                assert!(
                    block_size >= end,
                    "The size of block should be greater than {end}, but {block_size} found."
                );
                let slice = block
                    .get(offset..end)
                    .unwrap_or_else(|| unreachable!("The block is checked to be big enough."));
                buf.extend_from_slice(slice);
            }
            self.cache.lock().unpin(&CacheKey {
                ino: self.ino,
                block_id,
            });
        }
        Ok(buf.len())
    }

    /// Writes data to the file starting at the given offset.
    #[inline]
    pub async fn write(&self, buf: &[u8], slices: &[BlockSlice], size: u64) -> StorageResult<()> {
        let mut consume_index = 0;
        for slice in slices {
            let block_id = slice.block_id;
            let end = consume_index + slice.size.cast::<usize>();
            let len = buf.len();
            assert!(
                len >= end,
                "The `buf` should be longer than {end} bytes, but {len} found."
            );
            let write_content = buf
                .get(consume_index..end)
                .unwrap_or_else(|| unreachable!("The `buf` is checked to be long enough."));
            self.access(block_id);
            let block = self.fetch_block(block_id).await?;
            {
                let mut block = block.write();
                block.set_dirty(true);
                let start = slice.offset.cast();
                let end = start + slice.size.cast::<usize>();
                let block_size = block.len();
                assert!(
                    block_size >= end,
                    "The size of block should be greater than {end}, but {block_size} found."
                );
                block
                    .get_mut(start..end)
                    .unwrap_or_else(|| unreachable!("The block is checked to be big enough."))
                    .copy_from_slice(write_content);
                consume_index += slice.size.cast::<usize>();
                block.inc_version();
            }
            let task = Arc::new(WriteTask {
                cache: Arc::clone(&self.cache),
                backend: Arc::clone(&self.backend),
                ino: self.ino,
                block_id,
                block,
                file_size: size,
            });
            self.write_back_sender
                .send(Task::Pending(task))
                .await
                .unwrap_or_else(|_| {
                    panic!("Should not send command to write back task when the task quits.");
                });
        }

        Ok(())
    }

    /// Flushes any pending writes to the file.
    #[inline]
    pub async fn flush(&self) -> StorageResult<()> {
        let (tx, rx) = oneshot::channel();

        self.write_back_sender
            .send(Task::Flush(tx))
            .await
            .unwrap_or_else(|_| {
                panic!("Should not send command to write back task when the task quits.");
            });

        rx.await
            .unwrap_or_else(|_| panic!("The sender should not be closed."))
            .map_or(Ok(()), Err)
    }

    /// Extends the file from the old size to the new size.
    /// It is only called by the truncate method in the storage system.
    #[inline]
    pub async fn extend(&self, old_size: u64, new_size: u64) -> StorageResult<()> {
        let slices = offset_to_slice(self.block_size.cast(), old_size, new_size - old_size);
        for slice in slices {
            let buf = vec![0_u8; slice.size.cast()];
            self.write(&buf, &[slice], new_size).await?;
        }

        Ok(())
    }

    /// Increase the open count of the file handle.
    pub fn open(&self) {
        self.open_cnt
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    /// Closes the writer associated with the file handle.
    pub async fn close(&self) -> StorageResult<()> {
        if self
            .open_cnt
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
            > 0
        {
            return Ok(());
        }

        let (tx, rx) = oneshot::channel();
        self.write_back_sender
            .send(Task::Finish(tx))
            .await
            .unwrap_or_else(|_| {
                panic!("Should not send command to write back task when the task quits.");
            });
        // TODO: handle it by `TaskManager`
        self.write_back_handle
            .lock()
            .await
            .take()
            .unwrap_or_else(|| {
                unreachable!("The `JoinHandle` should not be None.");
            })
            .await
            .unwrap_or_else(|e| {
                panic!("Failed to join the write back task: {e}");
            });

        {
            let keys = self.access_keys.lock();
            for key in keys.iter() {
                self.cache.lock().remove(key);
            }
        }

        rx.await
            .unwrap_or_else(|_| panic!("The sender should not be closed."))
            .map_or(Ok(()), Err)
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
    /// The open flag for this file handle (read, write, or read and write)
    flag: OpenFlag,
    /// The inner file handle
    inner: Arc<FileHandleInner>,
}

impl FileHandle {
    /// Creates a new `FileHandle` instance.
    pub fn new<M: MetaData + Send + Sync + 'static>(
        fh: u64,
        ino: u64,
        block_size: usize,
        cache: Arc<Mutex<MemoryCache<CacheKey, LruPolicy<CacheKey>>>>,
        backend: Arc<dyn Backend>,
        flag: OpenFlag,
        metadata_client: Arc<M>,
    ) -> Self {
        let inner = FileHandleInner::new(ino, block_size, cache, backend, metadata_client);
        let inner = Arc::new(inner);
        FileHandle {
            fh,
            block_size,
            flag,
            inner,
        }
    }

    /// Returns the file handle.
    #[must_use]
    pub fn fh(&self) -> u64 {
        self.fh
    }

    /// Reads data from the file starting at the given offset and up to the
    /// given length.
    pub async fn read(&self, offset: u64, len: u64) -> StorageResult<Vec<u8>> {
        let reader = match self.flag {
            OpenFlag::Read | OpenFlag::ReadAndWrite => Arc::clone(&self.inner),
            OpenFlag::Write => {
                return Err(StorageError::Internal(anyhow::anyhow!(
                    "This file handle is not allowed to be read."
                )))
            }
        };
        let slices = offset_to_slice(self.block_size.cast(), offset, len);
        let mut buf = Vec::with_capacity(len.cast());
        reader.read(&mut buf, &slices).await?;
        Ok(buf)
    }

    /// Writes data to the file starting at the given offset.
    pub async fn write(&self, offset: u64, buf: &[u8], size: u64) -> StorageResult<()> {
        let writer = match self.flag {
            OpenFlag::Write | OpenFlag::ReadAndWrite => Arc::clone(&self.inner),
            OpenFlag::Read => {
                return Err(StorageError::Internal(anyhow::anyhow!(
                    "This file handle is not allowed to be written."
                )))
            }
        };
        let slices = offset_to_slice(self.block_size.cast(), offset, buf.len().cast());
        writer.write(buf, &slices, size).await
    }

    /// Extends the file from the old size to the new size.
    pub async fn extend(&self, old_size: u64, new_size: u64) -> StorageResult<()> {
        let writer = match self.flag {
            OpenFlag::Write | OpenFlag::ReadAndWrite => Arc::clone(&self.inner),
            OpenFlag::Read => {
                return Err(StorageError::Internal(anyhow::anyhow!(
                    "This file handle is not allowed to be written."
                )))
            }
        };
        writer.extend(old_size, new_size).await
    }

    /// Flushes any pending writes to the file.
    ///
    /// Flush and fsync do not need to check how many times a file handle has been opened.
    pub async fn flush(&self) -> StorageResult<()> {
        self.inner.flush().await
    }

    /// Increase the open count of the file handle.
    pub fn open(&self) {
        self.inner.open();
    }

    /// Closes the file handle, closing both the reader and writer.
    pub async fn close(&self) -> StorageResult<()> {
        self.inner.close().await
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
    use crate::async_fuse::memfs::kv_engine::{KVEngine, KVEngineType};
    use crate::async_fuse::memfs::{self, S3MetaData};
    use crate::new_storage::backend::backend_impl::tmp_fs_backend;
    use crate::new_storage::block::BLOCK_SIZE;

    const TEST_NODE_ID: &str = "test_node";
    const TEST_ETCD_ENDPOINT: &str = "127.0.0.1:2379";
    const IO_SIZE: usize = 128 * 1024;

    #[tokio::test]
    async fn test_file_handle() {
        let kv_engine: Arc<memfs::kv_engine::etcd_impl::EtcdKVEngine> = Arc::new(
            KVEngineType::new(vec![TEST_ETCD_ENDPOINT.to_owned()])
                .await
                .unwrap(),
        );
        let metadata_client = S3MetaData::new(kv_engine, TEST_NODE_ID).await.unwrap();

        let cache = Arc::new(Mutex::new(MemoryCache::new(100, BLOCK_SIZE)));
        let backend = Arc::new(tmp_fs_backend().unwrap());
        let handles = Arc::new(Handles::new());
        let ino = 1;
        let fh = 1;
        let file_handle = FileHandleInner::new(ino, BLOCK_SIZE, cache, backend, metadata_client);
        let file_handle = Arc::new(file_handle);
        let file_handle = FileHandle {
            fh,
            block_size: BLOCK_SIZE,
            flag: OpenFlag::ReadAndWrite,
            inner: file_handle,
        };
        handles.add_handle(file_handle.clone());
        let buf = vec![b'1', b'2', b'3', b'4'];
        file_handle.write(0, &buf, 4).await.unwrap();
        let read_buf = file_handle.read(0, 4).await.unwrap();
        assert_eq!(read_buf, buf);
        file_handle.flush().await.unwrap();
    }
}
