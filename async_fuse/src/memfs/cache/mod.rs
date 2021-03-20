use aligned_utils::bytes::AlignedBytes;
use common::error::DatenLordResult;
use lockfree_cuckoohash::{pin, LockFreeCuckooHash as Hash};
use log::debug;
use parking_lot::RwLock;
use std::ops::Deref;
use std::sync::Arc;

/// The size of a page
const PAGE_SIZE: usize = 4096;
/// The size of a memory block in KB, default is 64kB
const MEMORY_BLOCK_SIZE_IN_KB: usize = 64;
/// The size of a memory block in byte, default is 64kB
const MEMORY_BLOCK_SIZE_IN_BYTE: usize = MEMORY_BLOCK_SIZE_IN_KB * 1024;
/// The number of memory block in a Memory Bucket
const MEMORY_BUCKET_VEC_SIZE: usize = 16;
/// The size of a memory bucket in byte default is, 64 * 16 kB.
const MEMORY_BUCKET_SIZE_IN_BYTE: usize = MEMORY_BUCKET_VEC_SIZE * MEMORY_BLOCK_SIZE_IN_KB * 1024;

/// A mapping from file name to per-file memory block mapping
struct GlobalCache {
    inner: Hash<String, Hash<usize, MemBlockBucket>>,
}

/// This is global cache, which store the cache for all the files
impl GlobalCache {
    #[allow(dead_code)]
    /// Constructor
    pub(crate) fn new() -> Self {
        Self { inner: Hash::new() }
    }

    #[allow(dead_code)]
    /// Get a number of continous MemoryBlock from cache
    /// Some element in the return value could be None, which means there's no buffer in this range.
    ///
    /// The request can not be MemoryBlock size aligned. The return value will try to cover all the memory range.
    pub(crate) fn get_file_cache(
        &self,
        file_name: &str,
        offset: usize,
        len: usize,
    ) -> Vec<Option<MemBlock>> {
        let guard = pin();
        let file_cache = match self.inner.get(file_name, &guard) {
            Some(cache) => cache,
            None => return vec![],
        };

        let mut result = Vec::with_capacity(len / MEMORY_BLOCK_SIZE_IN_BYTE + 1);
        let bucket_start_offset = (offset % MEMORY_BUCKET_SIZE_IN_BYTE) / MEMORY_BLOCK_SIZE_IN_BYTE;
        // This end_offset is included
        let bucket_end_offset =
            ((offset + len - 1) % MEMORY_BUCKET_SIZE_IN_BYTE) / MEMORY_BLOCK_SIZE_IN_BYTE;
        let start_index = offset / MEMORY_BUCKET_SIZE_IN_BYTE;
        // This index is included
        let end_index = (offset + len - 1) / MEMORY_BUCKET_SIZE_IN_BYTE;

        for i in start_index..=end_index {
            let s = if i == start_index {
                bucket_start_offset
            } else {
                0
            };
            let l = if i == end_index {
                bucket_end_offset + 1
            } else {
                MEMORY_BUCKET_VEC_SIZE
            };

            match file_cache.get(&i, &guard) {
                Some(bucket) => {
                    result.extend((bucket.read()[s..l]).iter().cloned());
                }
                None => {
                    for _ in s..l {
                        result.push(None)
                    }
                }
            }
        }
        result
    }

    #[allow(dead_code)]
    /// Update the Cache.
    ///
    /// 1. `offset` be MemoryBlock aligned.
    /// 2. `len` should be multiple times of MemoryBlock Size unless it contains the file's last MemoryBlock.
    pub(crate) fn write_or_update(
        &self,
        file_name: &str,
        offset: usize,
        len: usize,
        buf: &AlignedBytes,
    ) -> DatenLordResult<()> {
        let guard = pin();
        let file_cache = match self.inner.get(file_name, &guard) {
            Some(cache) => cache,
            None => {
                debug!("cache for {} is empty, create one", file_name);
                // Here maybe a racing case where two thread are updating the cache
                self.inner.insert(file_name.to_string(), Hash::new());
                self.inner.get(file_name, &guard).unwrap_or_else(|| {
                    panic!("Just insert a file cache ({}) into global cache mapping, but cannot get it.", file_name)
                })
            }
        };

        if offset % MEMORY_BLOCK_SIZE_IN_BYTE != 0 {
            panic!("offset should be align with {}", MEMORY_BLOCK_SIZE_IN_BYTE);
        }

        let bucket_start_offset = (offset % MEMORY_BUCKET_SIZE_IN_BYTE) / MEMORY_BLOCK_SIZE_IN_BYTE;
        // This end_offset is included
        let bucket_end_offset =
            ((offset + len - 1) % MEMORY_BUCKET_SIZE_IN_BYTE) / MEMORY_BLOCK_SIZE_IN_BYTE;
        let start_index = offset / MEMORY_BUCKET_SIZE_IN_BYTE;
        // This index is included
        let end_index = (offset + len - 1) / MEMORY_BUCKET_SIZE_IN_BYTE;

        let mut have_read: usize = 0;
        let mut copy_fn = |b: &mut Option<MemBlock>| {
            debug!("copy one memblock");
            let end = std::cmp::min(have_read + MEMORY_BLOCK_SIZE_IN_BYTE, len);
            if b.is_none() {
                *b = Some(MemBlock::new());
            }
            b.as_ref().unwrap().overwrite(&(*buf)[have_read..end]);
            have_read = end;
        };

        for i in start_index..=end_index {
            let s = if i == start_index {
                bucket_start_offset
            } else {
                0
            };
            let l = if i == end_index {
                bucket_end_offset + 1
            } else {
                MEMORY_BUCKET_VEC_SIZE
            };
            let _ = match file_cache.get(&i, &guard) {
                Some(bucket) => bucket.write()[s..l].iter_mut().for_each(&mut copy_fn),
                None => {
                    // Here might be a racing case
                    debug!(
                        "memory_block_bucket for file {} index {} is empty, create one",
                        file_name, start_index
                    );
                    debug!("start offset {}, end offset {}", s, l);
                    file_cache.insert(i, MemBlockBucket::new());
                    file_cache
                        .get(&i, &guard)
                        .unwrap_or_else(|| {
                            panic!("Just insert MemBlockBucket into file cache, but cannot get it.")
                        })
                        .write()[s..l]
                        .iter_mut()
                        .for_each(&mut copy_fn);
                }
            };
        }

        return Ok(());
    }
}

// This is a memory block collection
struct MemBlockBucket {
    inner: Arc<RwLock<Vec<Option<MemBlock>>>>,
}

/// A vector holding `MEMORY_BUCKET_VEC_SIZE` MemBlock, wrapped in an Arc.
/// Arc is used to shared this vector with priority queue.
impl MemBlockBucket {
    #[allow(dead_code)]
    /// Init with None values
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(
                std::iter::repeat_with(|| None)
                    .take(MEMORY_BUCKET_VEC_SIZE)
                    .collect(),
            )),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn insert(&mut self, index: usize, mem: MemBlock) {
        self.inner.write()[index] = Some(mem);
    }
}

impl Deref for MemBlockBucket {
    type Target = RwLock<Vec<Option<MemBlock>>>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

struct MemBlock {
    inner: Arc<RwLock<AlignedBytes>>,
}

impl MemBlock {
    #[allow(dead_code)]
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(AlignedBytes::new_zeroed(
                MEMORY_BLOCK_SIZE_IN_BYTE,
                PAGE_SIZE,
            ))),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn new_from_slice(slice: &[u8]) -> Self {
        Self {
            inner: Arc::new(RwLock::new(AlignedBytes::new_from_slice(slice, PAGE_SIZE))),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn overwrite(&self, slice: &[u8]) {
        (*(*(self.inner).write()))[..slice.len()].copy_from_slice(slice);
    }
}

impl Deref for MemBlock {
    type Target = RwLock<AlignedBytes>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Clone for MemBlock {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{GlobalCache, MEMORY_BLOCK_SIZE_IN_BYTE};
    use aligned_utils::bytes::AlignedBytes;

    #[test]
    fn test_get_empty_cache() {
        let global = GlobalCache::new();
        let cache = global.get_file_cache("test_file", 0, 1024);
        assert!(cache.is_empty())
    }

    #[test]
    fn test_insert_one_byte_cache() {
        env_logger::init();
        let global = GlobalCache::new();
        let file_name = "test_file";
        let content = AlignedBytes::new_from_slice(&[b'a'], 1);
        let result = global.write_or_update(file_name, 0, 1, &content);

        assert!(result.is_ok());

        let cache = global.get_file_cache(file_name, 0, 1);
        assert_eq!(cache.len(), 1);
        assert!(cache[0].is_some());
        assert_eq!(cache[0].as_ref().unwrap().read()[0], b'a');
    }

    #[test]
    fn test_get_partial_result() {
        env_logger::init();
        let global = GlobalCache::new();
        let file_name = "test_file";
        let content = AlignedBytes::new_from_slice(&[b'a'], 1);
        let result = global.write_or_update(file_name, MEMORY_BLOCK_SIZE_IN_BYTE, 1, &content);
        assert!(result.is_ok());

        let cache = global.get_file_cache(file_name, 0, MEMORY_BLOCK_SIZE_IN_BYTE + 1);
        assert_eq!(cache.len(), 2);
        assert!(cache[0].is_none());
        assert!(cache[1].is_some());
        assert_eq!(cache[1].as_ref().unwrap().read()[0], b'a');
    }

    #[test]
    #[should_panic(expected = "offset should be align with 65536")]
    fn test_panic_write_unaligned_data() {
        env_logger::init();
        let global = GlobalCache::new();
        let file_name = "test_file";
        let content = AlignedBytes::new_from_slice(&[b'a'], 1);
        let _ = global.write_or_update(file_name, 1, 1, &content);
    }
}
