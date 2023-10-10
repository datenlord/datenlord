use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;

use dashmap::mapref::one::RefMut;
use dashmap::DashMap;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::async_fuse::fuse::protocol::INum;
use crate::async_fuse::memfs::cache::block::{Block, BLOCK_SIZE};
use crate::async_fuse::memfs::cache::BlockId;
use crate::common::error::DatenLordResult;

/// `FileCache` is a map from `BlockId` to whether the block exists.
/// But we don't need the value, so we use bool.
/// Later we may use a more efficient data structure like a bitset.
pub type FileCache = Mutex<HashMap<BlockId, bool>>;

/// Disk cache size is 1GB
const DEFAULT_DISK_CACHE_SIZE: usize = 1024 * 1024 * 1024;

/// Returns the file path for the given inum based on the given base path.
fn path_of_inum(base: impl AsRef<Path>, inum: INum) -> PathBuf {
    base.as_ref().join(format!("{inum}"))
}

/// Returns the file path for the given inum and `BlockId` based on the given
/// base path.
fn path_of_block(base: impl AsRef<Path>, inum: INum, block_id: BlockId) -> PathBuf {
    path_of_inum(base, inum).join(format!("{block_id}"))
}

/// `DiskCache` is a cache for blocks on disk.
pub struct DiskCache {
    /// `INum` -> `BlockId` -> Block_existed
    map: DashMap<INum, FileCache>,
    /// Cache root path
    root_path: PathBuf,
    /// Capacity of the cache
    capacity: usize,
    /// Current size of the cache
    size: AtomicUsize,
}

impl DiskCache {
    /// Creates a new `DiskCache` with the given root path and default capacity.
    pub async fn open(root_path: impl AsRef<Path>) -> DatenLordResult<Self> {
        tokio::fs::create_dir_all(root_path.as_ref()).await?;
        Ok(DiskCache {
            map: DashMap::new(),
            root_path: root_path.as_ref().to_path_buf(),
            capacity: DEFAULT_DISK_CACHE_SIZE,
            size: AtomicUsize::new(0),
        })
    }

    /// Returns the current size of the cache.
    pub fn size(&self) -> usize {
        self.size.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Returns the capacity of the cache.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Gets or creates the block map for the given inum for set operation.
    async fn get_or_create_block_map(&self, inum: INum) -> RefMut<INum, FileCache> {
        // Get or insert
        loop {
            if let Some(entry) = self.map.try_entry(inum) {
                return entry.or_insert_with(|| Mutex::new(HashMap::new()));
            }
            // None means the lock is already held by another thread.
            tokio::task::yield_now().await;
        }
    }

    /// Sets the block data for the given inum and `BlockId`.
    pub async fn set(&self, inum: INum, block_id: BlockId, block: &Block) -> DatenLordResult<()> {
        let file_cache_ref = self.get_or_create_block_map(inum).await;
        let mut file_cache = file_cache_ref.lock().await;
        // Check if file_cache's directory exists
        if file_cache.len() == 0 {
            tokio::fs::create_dir_all(path_of_inum(&self.root_path, inum)).await?;
        }
        let path = path_of_block(&self.root_path, inum, block_id);
        let mut file = OpenOptions::new()
            .write(true)
            .read(false)
            .create_new(true)
            .open(path)
            .await?;
        file.write_all(block.get_data()).await?;
        file.sync_all().await?;
        file_cache.insert(block_id, true);
        self.size
            .fetch_add(BLOCK_SIZE, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    /// Gets the block data for the given inum and `BlockId`.
    pub async fn get(&self, inum: INum, block_id: BlockId) -> DatenLordResult<Option<Block>> {
        if let Some(file_cache_guard) = self.map.get(&inum) {
            let file_cache = file_cache_guard.lock().await;
            if let Some(existed) = file_cache.get(&block_id) {
                if *existed {
                    let path = path_of_block(&self.root_path, inum, block_id);
                    let data = tokio::fs::read(path).await?;
                    return Ok(Some(Block::from(data)));
                }
            }
        }
        Ok(None)
    }

    /// Removes the block data for the given inum and `BlockId`.
    pub async fn remove_block(&self, inum: INum, block_id: BlockId) -> DatenLordResult<()> {
        if let Some(file_cache_guard) = self.map.get(&inum) {
            let mut file_cache = file_cache_guard.lock().await;
            if let Some(existed) = file_cache.get(&block_id) {
                if *existed {
                    let path = path_of_block(&self.root_path, inum, block_id);
                    tokio::fs::remove_file(path).await?;
                    file_cache.remove(&block_id);
                    self.size
                        .fetch_sub(BLOCK_SIZE, std::sync::atomic::Ordering::SeqCst);
                }
            }
        }
        Ok(())
    }

    /// Clears the cache.
    pub async fn clear(&self) -> DatenLordResult<()> {
        tokio::fs::remove_dir_all(&self.root_path).await?;
        tokio::fs::create_dir_all(&self.root_path).await?;
        self.map.clear();
        self.size.store(0, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    /// Test basic set and get operations.
    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn test_disk_cache_set_get() {
        let tempdir = tempfile::tempdir().unwrap();
        let disk_cache = DiskCache::open(tempdir).await.unwrap();
        assert_eq!(disk_cache.size(), 0);
        assert_eq!(disk_cache.capacity(), DEFAULT_DISK_CACHE_SIZE);
        let (inum, block_id, block) = generate_random_inum_block_id_block();
        disk_cache.set(inum, block_id, &block).await.unwrap();
        assert_eq!(disk_cache.size(), BLOCK_SIZE);
        let block = disk_cache.get(inum, block_id).await.unwrap().unwrap();
        assert_eq!(block.get_data(), block.get_data());
        disk_cache.remove_block(inum, block_id).await.unwrap();
        assert!(disk_cache.get(inum, block_id).await.unwrap().is_none());
        assert_eq!(disk_cache.size(), 0);
        disk_cache.clear().await.unwrap();
    }

    // Generates a random INum, `BlockId` and Block.
    fn generate_random_inum_block_id_block() -> (INum, BlockId, Block) {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let inum = rng.gen::<INum>();
        let block_id = rng.gen::<BlockId>();
        let mut data = vec![0; BLOCK_SIZE];
        rng.fill(data.as_mut_slice());
        let block = Block::from(data);
        (inum, block_id, block)
    }

    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn test_disk_cache_multi_thread() {
        let tempdir = tempfile::tempdir().unwrap();
        let disk_cache = Arc::new(DiskCache::open(tempdir).await.unwrap());
        let mut tasks = Vec::new();
        for _ in 0_i32..2_i32 {
            let disk_cache = Arc::<DiskCache>::clone(&disk_cache);
            let task = tokio::spawn(async move {
                for _ in 0_i32..100_i32 {
                    let (inum, block_id, block) = generate_random_inum_block_id_block();
                    disk_cache.set(inum, block_id, &block).await.unwrap();
                    let block = disk_cache.get(inum, block_id).await.unwrap().unwrap();
                    assert_eq!(block.get_data(), block.get_data());
                    disk_cache.remove_block(inum, block_id).await.unwrap();
                    assert!(disk_cache.get(inum, block_id).await.unwrap().is_none());
                }
            });
            tasks.push(task);
        }
        for task in tasks {
            task.await.unwrap();
        }
        disk_cache.clear().await.unwrap();
    }
}
