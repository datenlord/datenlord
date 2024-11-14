//! The file handle implementation

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use crate::new_storage::{format_path, Block, BlockSlice, StorageError};
use bytes::Bytes;
use clippy_utilities::Cast;
use parking_lot::{Mutex, RwLock};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tracing::{debug, error, info, warn};

use super::super::backend::Backend;
use super::super::block_slice::offset_to_slice;
use super::super::error::StorageResult;
use super::super::policy::LruPolicy;
use super::super::{CacheKey, MemoryCache};
use crate::async_fuse::memfs::{FileAttr, MetaData};

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
    /// Current file attributes.
    attr: Arc<RwLock<FileAttr>>,
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

/// The `Task` enum represents the different types of tasks that the write back
/// worker can perform.
#[derive(Debug)]
pub enum Task {
    /// A pending write task.
    Pending(Arc<WriteTask>),
    /// A flush task.
    Flush(oneshot::Sender<Option<StorageError>>),
    /// A finish task.
    Finish(oneshot::Sender<Option<StorageError>>),
}

/// The `WriteTask` struct represents a write task
#[derive(Debug)]
pub struct WriteTask {
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

impl FileHandleInner {
    /// Creates a new `FileHandleInner` instance.
    #[inline]
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        ino: u64,
        block_size: usize,
        cache: Arc<Mutex<MemoryCache<CacheKey, LruPolicy<CacheKey>>>>,
        backend: Arc<dyn Backend>,
        write_back_tx: Sender<Task>,
    ) -> Self {
        FileHandleInner {
            ino,
            block_size,
            cache,
            backend,
            // The open count is initialized to 0, open() method will increase this flag.
            open_cnt: AtomicU32::new(1),
            // init the file attributes
            attr: Arc::new(RwLock::new(FileAttr::default())),
            access_keys: Mutex::new(Vec::new()),
            write_back_sender: write_back_tx,
        }
    }

    /// Get the open file attributes.
    #[inline]
    pub fn getattr(&self) -> FileAttr {
        let attr = self.attr.read();
        debug!("Get attr for ino: {} attr: {:?}", self.ino, *attr);
        *attr
    }

    /// Set the open file attributes.
    #[inline]
    pub fn setattr(&self, attr: FileAttr) {
        debug!("Set attr for ino: {} attr: {:?}", self.ino, attr);
        let mut old_attr = self.attr.write();
        *old_attr = attr;
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

    /// Get the open count of the file handle.
    #[must_use]
    pub fn open_cnt(&self) -> u32 {
        self.open_cnt.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Closes the writer associated with the file handle.
    /// If the open count is greater than 0, it will return false and do not remove this handle.
    /// Otherwise, it will return true and remove this handle.
    pub async fn close(&self) -> StorageResult<bool> {
        // Decrease the open count of the file handle, fetch sub will return the previous value.
        if self
            .open_cnt
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
            > 1
        {
            debug!("The file handle is still open by other processes.");
            return Ok(false);
        }

        debug!("The file handle is closed, remove the file handle.");

        let (tx, rx) = oneshot::channel();
        self.write_back_sender
            .send(Task::Finish(tx))
            .await
            .unwrap_or_else(|_| {
                panic!("Should not send command to write back task when the task quits.");
            });

        rx.await
            .unwrap_or_else(|_| panic!("The sender should not be closed."))
            .map_or(Ok(true), Err)
    }

    /// Write the blocks to the backend storage system concurrently.
    async fn write_blocks<M: MetaData + Send + Sync + 'static>(
        self: Arc<Self>,
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
        let ino = self.ino;
        let current_attr = self.getattr();
        info!(
            "Commit meta data for ino: {} with attr size: {:?}",
            ino, current_attr.size
        );
        if let Err(e) = metadata_client
            .write_remote_size_helper(ino, current_attr.size)
            .await
        {
            error!("Failed to commit meta data, the error is {e}.");
        }

        result
    }

    /// The `write_back_work` function represents the write back worker.
    #[allow(clippy::pattern_type_mismatch)] // Raised by `tokio::select!`
    async fn write_back_work<M: MetaData + Send + Sync + 'static>(
        self: Arc<Self>,
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
                            info!("Get pending Write back the block. ino: {:?}", self.ino);
                            tasks.push(task);
                            if tasks.len() >= 10 {
                                let slf_clone = Arc::clone(&self);
                                let res = slf_clone.write_blocks(&tasks, Arc::clone(&metadata_client)).await;
                                if let Some(e) = res {
                                    result.get_or_insert(e);
                                }
                                tasks.clear();
                            }
                        }
                        Task::Flush(tx) => {
                            info!("Flush the cache. ino: {:?}", self.ino);
                            let slf_clone = Arc::clone(&self);
                            let res = slf_clone.write_blocks(&tasks, Arc::clone(&metadata_client)).await;
                            tasks.clear();
                            let res = result.take().or(res);
                            if let Err(Some(e)) = tx.send(res) {
                                error!("Failed to send storage error back to `Writer`, the error is {e}.");
                            }
                        }
                        Task::Finish(tx) => {
                            info!("Finish the write back task. ino: {:?}", self.ino);
                            let slf_clone = Arc::clone(&self);
                            let res = slf_clone.write_blocks(&tasks, Arc::clone(&metadata_client)).await;
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
                    let slf_clone = Arc::clone(&self);
                    slf_clone.write_blocks(&tasks, Arc::clone(&metadata_client)).await;
                    tasks.clear();
                }
            }
        }
    }
}

/// The `FileHandle` struct represents a handle to an open file.
/// It contains an `Arc` of `RwLock<FileHandleInner>`.
#[derive(Debug, Clone)]
pub struct FileHandle {
    /// The file handle(inode).
    fh: u64,
    /// The block size in bytes
    block_size: usize,
    /// The inner file handle
    inner: Arc<FileHandleInner>,
    /// The write back handle
    write_back_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// The lock to synchronize open and close calls.
    open_close_lock: Arc<tokio::sync::Mutex<()>>,
}

impl FileHandle {
    /// Creates a new `FileHandle` instance.
    pub fn new<M: MetaData + Send + Sync + 'static>(
        ino: u64,
        block_size: usize,
        cache: Arc<Mutex<MemoryCache<CacheKey, LruPolicy<CacheKey>>>>,
        backend: Arc<dyn Backend>,
        metadata_client: Arc<M>,
    ) -> Self {
        let (write_back_tx, write_back_rx) = tokio::sync::mpsc::channel(100);
        let inner = Arc::new(FileHandleInner::new(
            ino,
            block_size,
            cache,
            backend,
            write_back_tx,
        ));
        let inner_clone = Arc::clone(&inner);
        // TODO: Move handle to task manager
        let write_back_handle =
            tokio::spawn(inner_clone.write_back_work(write_back_rx, metadata_client));

        FileHandle {
            fh: ino,
            block_size,
            inner,
            write_back_handle: Arc::new(Mutex::new(Some(write_back_handle))),
            open_close_lock: Arc::new(tokio::sync::Mutex::new(())),
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
        let slices = offset_to_slice(self.block_size.cast(), offset, len);
        let mut buf = Vec::with_capacity(len.cast());
        self.inner.read(&mut buf, &slices).await?;
        Ok(buf)
    }

    /// Writes data to the file starting at the given offset.
    pub async fn write(&self, offset: u64, buf: &[u8], size: u64) -> StorageResult<()> {
        let slices: smallvec::SmallVec<[BlockSlice; 2]> =
            offset_to_slice(self.block_size.cast(), offset, buf.len().cast());
        self.inner.write(buf, &slices, size).await
    }

    /// Extends the file from the old size to the new size.
    pub async fn extend(&self, old_size: u64, new_size: u64) -> StorageResult<()> {
        self.inner.extend(old_size, new_size).await
    }

    /// Flushes any pending writes to the file.
    ///
    /// Flush and fsync do not need to check how many times a file handle has been opened.
    pub async fn flush(&self) -> StorageResult<()> {
        self.inner.flush().await
    }

    /// Increase the open count of the file handle.
    pub async fn open(&self) {
        let _guard = self.open_close_lock.lock().await;
        println!("try to get open filehandle lock ok {:?}", self.fh);
        self.inner.open();
    }

    /// Get and try to increase the open count of the file handle.
    pub async fn get_and_open(&self) -> bool {
        let _guard = self.open_close_lock.lock().await;
        println!("try to get open filehandle lock ok {:?}", self.fh);
        if self.inner.open_cnt() > 0 {
            // Current file handle is opened by other process, increase the open count.
            self.inner.open();
            return true;
        }

        false
    }

    /// Get the open count of the file handle.
    /// Be careful, do not split the `open_cnt` with open and close operation.
    #[must_use]
    pub async fn open_cnt(&self) -> u32 {
        let _guard = self.open_close_lock.lock().await;
        println!("try to get open cnt filehandle lock ok {:?}", self.fh);
        self.inner.open_cnt()
    }

    /// Closes the file handle, closing both the reader and writer.
    pub async fn close(&self) -> StorageResult<bool> {
        let _guard = self.open_close_lock.lock().await;
        println!("try to close filehandle lock ok {:?}", self.fh);
        info!("Close the file handle, ino: {}", self.fh);
        match self.inner.close().await {
            Ok(true) => {
                let write_back_handle = self.write_back_handle.lock().take().unwrap_or_else(|| {
                    unreachable!("The write back handle should be initialized.")
                });
                write_back_handle.await.unwrap_or_else(|e| {
                    error!("Failed to join the write back task: {e}");
                });
                println!("filehandle {:?} drop close filehandle ok", self.fh);
                Ok(true)
            }
            Ok(false) => {
                println!("filehandle {:?} drop filehandle lock ok", self.fh);
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }

    /// Get the open file attributes.
    #[must_use]
    pub fn getattr(&self) -> FileAttr {
        self.inner.getattr()
    }

    /// Set the open file attributes.
    pub fn setattr(&self, attr: FileAttr) {
        self.inner.setattr(attr);
    }
}

/// Number of handle shards.
const HANDLE_SHARD_NUM: usize = 100;

/// The `Handles` struct represents a collection of file handles.
/// It uses sharding to avoid lock contention.
#[derive(Debug)]
pub struct Handles {
    /// Use shard to avoid lock contention
    /// Update vec to hashmap to avoid duplicate file handle.
    handles: [Arc<tokio::sync::RwLock<HashMap<u64, FileHandle>>>; HANDLE_SHARD_NUM],
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
            handles.push(Arc::new(tokio::sync::RwLock::new(HashMap::new())));
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
    fn get_shard(&self, fh: u64) -> &Arc<tokio::sync::RwLock<HashMap<u64, FileHandle>>> {
        let idx = Self::hash(fh);
        self.handles
            .get(idx)
            .unwrap_or_else(|| unreachable!("The array is ensured to be long enough."))
    }

    /// Adds a file handle to the collection.
    pub async fn add_handle(&self, fh: FileHandle) {
        let shard = self.get_shard(fh.fh());
        let mut shard_lock = shard.write().await;
        shard_lock.insert(fh.fh(), fh);
    }

    /// Removes a file handle from the collection.'
    #[must_use]
    pub async fn remove_handle(&self, fh: u64) -> Option<FileHandle> {
        let shard = self.get_shard(fh);
        let mut shard_lock = shard.write().await;
        shard_lock.remove(&fh)
    }

    /// Returns a file handle from the collection.
    #[must_use]
    pub async fn get_handle(&self, fh: u64) -> Option<FileHandle> {
        let shard = self.get_shard(fh);
        let shard_lock = shard.read().await;
        let fh = shard_lock.get(&fh)?;
        Some(fh.clone())
    }

    /// Reopen or create a new filehandle, if current filehandle is new, return true
    /// otherwise return false.
    /// If true, we need to update the file handle attr later.
    pub async fn reopen_or_create_handle<M: MetaData + Send + Sync + 'static>(
        &self,
        fh: u64,
        block_size: usize,
        cache: Arc<Mutex<MemoryCache<CacheKey, LruPolicy<CacheKey>>>>,
        backend: Arc<dyn Backend>,
        metadata_client: Arc<M>,
        attr: FileAttr,
    ) -> bool {
        let shard = self.get_shard(fh);
        let mut shard_lock = shard.write().await;
        // If the file handle is already open, reopen it and return false
        if let Some(file_handle) = shard_lock.get(&fh) {
            let open_cnt = file_handle.open_cnt().await;
            info!("Reopen file handle for ino: {fh} with opencnt: {open_cnt}");
            file_handle.open().await;

            false
        } else {
            info!("Create a new file handle for ino: {}", fh);
            // If the file handle is not open, create a new file handle and return true
            let file_handle = FileHandle::new(fh, block_size, cache, backend, metadata_client);
            file_handle.setattr(attr);
            shard_lock.insert(fh, file_handle);

            true
        }
    }

    /// Close and remove current filehandle.
    pub async fn close_handle(&self, fh: u64) -> StorageResult<Option<FileHandle>> {
        let shard = self.get_shard(fh);
        let mut shard_lock = shard.write().await;
        let filehandle = shard_lock.get(&fh).ok_or_else(|| {
            StorageError::Internal(anyhow::anyhow!("Cannot close a file that is not open."))
        })?;
        let need_remove_flag = filehandle.close().await?;
        if need_remove_flag {
            // Remove the file handle from the handles map
            match shard_lock.remove(&fh) {
                Some(filehandle) => return Ok(Some(filehandle)),
                None => {
                    panic!("Cannot close a file that is not open.");
                }
            }
        }

        Ok(None)
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

        let (write_back_tx, write_back_rx) = tokio::sync::mpsc::channel::<Task>(100);
        let file_handle = FileHandleInner::new(ino, BLOCK_SIZE, cache, backend, write_back_tx);
        let file_handle = Arc::new(file_handle);
        let file_handle_inner_clone = Arc::clone(&file_handle);
        let write_back_handle =
            tokio::spawn(file_handle_inner_clone.write_back_work(write_back_rx, metadata_client));
        let file_handle = FileHandle {
            fh,
            block_size: BLOCK_SIZE,
            inner: file_handle,
            write_back_handle: Arc::new(Mutex::new(Some(write_back_handle))),
            open_close_lock: Arc::new(tokio::sync::Mutex::new(())),
        };
        handles.add_handle(file_handle.clone()).await;
        let buf = vec![b'1', b'2', b'3', b'4'];
        file_handle.write(0, &buf, 4).await.unwrap();
        let read_buf = file_handle.read(0, 4).await.unwrap();
        assert_eq!(read_buf, buf);
        file_handle.flush().await.unwrap();
    }
}
