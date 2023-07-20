//! This is the cache implementation for the memfs

use std::fmt::{Debug, Error, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use aligned_utils::bytes::AlignedBytes;
use clippy_utilities::{Cast, OverflowArithmetic};
use lockfree_cuckoohash::{pin, LockFreeCuckooHash as HashMap};
use log::debug;
use nix::sys::uio::IoVec;
use parking_lot::{Mutex, RwLock};
use priority_queue::PriorityQueue;

// TODO: use smol RwLock
use super::dist::request::Index;
use super::kv_engine::kv_utils::{add_node_to_file_list, remove_node_from_file_list};
use super::kv_engine::KVEngineType;
use crate::async_fuse::fuse::fuse_reply::{AsIoVec, CouldBeAsIoVecList};
use crate::async_fuse::fuse::protocol::INum;

/// Page Size
const PAGE_SIZE: usize = 4096;
/// The size of a memory block in KB, default is 64kB
const MEMORY_BLOCK_SIZE_IN_KB: usize = 64;
/// The size of a memory block in byte, default is 64kB
const MEMORY_BLOCK_SIZE_IN_BYTE: usize = MEMORY_BLOCK_SIZE_IN_KB * 1024;
/// The number of memory block in a Memory Bucket
const MEMORY_BUCKET_VEC_SIZE: usize = 16;
/// The size of a memory bucket in byte default is, 64 * 16 kB.
#[allow(dead_code)]
const MEMORY_BUCKET_SIZE_IN_BYTE: usize = MEMORY_BUCKET_VEC_SIZE * MEMORY_BLOCK_SIZE_IN_KB * 1024;
/// The default capacity in bytes, 10GB
const GLOBAL_CACHE_DEFAULT_CAPACITY: usize = 10 * 1024 * 1024 * 1024;

/// A mapping from file name to per-file memory block mapping
pub struct GlobalCache {
    /// Map from file identifier to cache content map
    inner: HashMap<INum, HashMap<usize, MemBlockBucket>>,
    /// Priority queue to track bucket usage
    queue: Mutex<PriorityQueue<MemBlockBucket, i64>>,
    /// The current size of this global cache in byte
    size: AtomicUsize,
    /// The capacity of this global cache
    capacity: usize,
    /// Block size
    block_size: usize,
    /// Count of block size in a bucket
    bucket_size_in_block: usize,
    /// Etcd client, only distributed fs need this
    kv_engine: Option<Arc<KVEngineType>>,
    /// Node Id, only distributed fs need this
    node_id: Option<String>,
}

impl Debug for GlobalCache {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        f.debug_tuple("")
            .field(&self.size.load(Ordering::Relaxed))
            .field(&self.capacity)
            .finish()
    }
}

/// This is global cache, which store the cache for all the files
impl GlobalCache {
    #[allow(dead_code)]
    /// Constructor with 10GB default capacity
    pub(crate) fn new() -> Self {
        Self {
            inner: HashMap::new(),
            queue: Mutex::new(PriorityQueue::new()),
            size: AtomicUsize::new(0),
            capacity: GLOBAL_CACHE_DEFAULT_CAPACITY,
            block_size: MEMORY_BLOCK_SIZE_IN_BYTE,
            bucket_size_in_block: MEMORY_BUCKET_VEC_SIZE,
            kv_engine: None,
            node_id: None,
        }
    }

    #[allow(dead_code)]
    /// Constructor with capacity
    pub(crate) fn new_with_capacity(capacity: usize) -> Self {
        Self {
            inner: HashMap::new(),
            queue: Mutex::new(PriorityQueue::new()),
            size: AtomicUsize::new(0),
            capacity,
            block_size: MEMORY_BLOCK_SIZE_IN_BYTE,
            bucket_size_in_block: MEMORY_BUCKET_VEC_SIZE,
            kv_engine: None,
            node_id: None,
        }
    }

    #[allow(dead_code)]
    /// Constructor with capacity and `block_size`
    pub(crate) fn new_with_bz_and_capacity(block_size: usize, capacity: usize) -> Self {
        Self {
            inner: HashMap::new(),
            queue: Mutex::new(PriorityQueue::new()),
            size: AtomicUsize::new(0),
            capacity,
            block_size,
            bucket_size_in_block: MEMORY_BUCKET_VEC_SIZE,
            kv_engine: None,
            node_id: None,
        }
    }

    #[allow(dead_code)]
    /// Constructor with capacity and `block_size`
    /// TODO: refactor with builder
    pub(crate) fn new_dist_with_bz_and_capacity(
        block_size: usize,
        capacity: usize,
        kv_engine: Arc<KVEngineType>,
        node_id: &str,
    ) -> Self {
        Self {
            inner: HashMap::new(),
            queue: Mutex::new(PriorityQueue::new()),
            size: AtomicUsize::new(0),
            capacity,
            block_size,
            bucket_size_in_block: MEMORY_BUCKET_VEC_SIZE,
            kv_engine: Some(kv_engine),
            node_id: Some(node_id.to_owned()),
        }
    }

    /// Get the alignment of this cache
    #[inline]
    pub(crate) const fn get_align(&self) -> usize {
        self.block_size
    }

    /// Get current size of the cache
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn get_size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    /// Get a number of continuous `MemoryBlock` from cache
    /// Some element in the return value could be None, which means there's no
    /// buffer in this range.
    ///
    /// The request can not be `MemoryBlock` size aligned. The return value will
    /// try to cover all the memory range.
    #[allow(dead_code)]
    pub(crate) fn get_file_cache(
        &self,
        file_ino: INum,
        offset: usize,
        len: usize,
    ) -> Vec<IoMemBlock> {
        let guard = pin();
        let Some(file_cache) = self.inner.get(&file_ino, &guard) else {
            return vec![];
        };

        if len == 0 {
            return vec![];
        }

        let mut result =
            Vec::with_capacity(len.overflow_div(self.block_size).overflow_add(1_usize));

        let bucket_size = self.bucket_size_in_block.overflow_mul(self.block_size);
        // The key in the hashmap, the end key is included
        let start_key = offset.overflow_div(bucket_size);
        let end_key = offset
            .overflow_add(len)
            .overflow_sub(1)
            .overflow_div(bucket_size);
        // The index in the bucket, the end index is included
        let bucket_start_index = offset
            .overflow_rem(bucket_size)
            .overflow_div(self.block_size);
        let bucket_end_index = offset
            .overflow_add(len)
            .overflow_sub(1)
            .overflow_rem(bucket_size)
            .overflow_div(self.block_size);
        // The offset in the block, the end offset is included
        let start_offset = offset.overflow_rem(self.block_size);
        let end_offset = offset
            .overflow_add(len)
            .overflow_sub(1)
            .overflow_rem(self.block_size);

        debug!(
            "start_key = {}, end_key = {}, bucket_start_index = {}, bucket_end_index = {}, start_offset = {}, end_offset = {}",
            start_key, end_key, bucket_start_index, bucket_end_index, start_offset, end_offset,
        );

        for k in start_key..=end_key {
            let start_index = if k == start_key {
                bucket_start_index
            } else {
                0
            };
            let end_index = if k == end_key {
                bucket_end_index.overflow_add(1)
            } else {
                self.bucket_size_in_block
            };

            match file_cache.get(&k, &guard) {
                Some(bucket) => {
                    self.queue
                        .lock()
                        .push(bucket.clone(), 0.overflow_sub(now_monotonic()));
                    for (pos, block) in bucket
                        .read()
                        .get(start_index..end_index)
                        .unwrap_or_else(|| {
                            panic!(
                                "error when getting range of {start_index}..{end_index} in the cache bucket"
                            )
                        })
                        .iter()
                        .cloned()
                        .enumerate()
                    {
                        let s = if k == start_key && pos == 0 {
                            start_offset
                        } else {
                            0
                        };

                        let e = if k == end_key
                            && pos == end_index.overflow_sub(start_index).overflow_sub(1)
                        {
                            end_offset.overflow_add(1)
                        } else {
                            self.block_size
                        };

                        result.push(IoMemBlock::new(block, s, e));
                    }
                }
                None => {
                    for _ in start_index..end_index {
                        result.push(IoMemBlock::new(None, 0, 0));
                    }
                }
            }
        }
        result
    }

    /// Invalidate file's cache
    pub(crate) fn invalidate(&self, file_ino: INum, index: Vec<Index>) {
        let guard = pin();
        let Some(file_cache) = self.inner.get(&file_ino, &guard) else {
            debug!(
                "cache for {:?} is empty, don't need to invalidate",
                file_ino
            );
            return;
        };

        let bucket_size = self.bucket_size_in_block;

        let dealloc_fn = |global_index: usize| {
            let hash_index = global_index.overflow_div(bucket_size);
            let bucket = file_cache.get(&hash_index, &guard);
            let mut bucket = if let Some(b) = bucket {
                b.write()
            } else {
                return;
            };

            let block = bucket
                .get_mut(global_index.overflow_rem(bucket_size))
                .unwrap_or_else(|| {
                    panic!(
                        "error when getting range of {} in the cache bucket",
                        global_index.overflow_rem(bucket_size)
                    )
                });

            if block.is_some() {
                block.take();
            }
        };

        for i in index {
            match i {
                Index::Point(p) => dealloc_fn(p),
                Index::Range(s, e) => {
                    for i in s..=e {
                        dealloc_fn(i);
                    }
                }
            }
        }
    }

    /// Check if file is available in cache
    pub(crate) fn check_available(&self, file_ino: INum, index: Vec<Index>) -> (Vec<Index>, bool) {
        let guard = pin();
        let Some(file_cache) = self.inner.get(&file_ino, &guard) else {
            debug!(
                "cache for {:?} is empty, don't need to invalidate",
                file_ino,
            );
            return (vec![], false);
        };

        let bucket_size = self.bucket_size_in_block;

        let check_fn = |global_index: usize| -> Option<usize> {
            let hash_index = global_index.overflow_div(bucket_size);
            let bucket_opt = file_cache.get(&hash_index, &guard);
            // if let Some(bucket) = bucket_opt {
            // if bucket
            // .write()
            // .get(global_index.overflow_rem(bucket_size))
            // .unwrap_or_else(|| {
            // panic!(
            // "error when getting range of {} in the cache bucket",
            // global_index.overflow_rem(bucket_size)
            // )
            // })
            // .is_some()
            // {
            // Some(global_index)
            // } else {
            // None
            // }
            // } else {
            // None
            // }

            bucket_opt.and_then(|bucket| {
                bucket
                    .write()
                    .get(global_index.overflow_rem(bucket_size))
                    .unwrap_or_else(|| {
                        panic!(
                            "error when getting range of {} in the cache bucket",
                            global_index.overflow_rem(bucket_size)
                        )
                    })
                    .as_ref()
                    .map(|_| global_index)
            })
        };

        let mut result = Vec::new();

        let mut all_hit = true;

        for i in index {
            match i {
                Index::Point(p) => {
                    if let Some(index) = check_fn(p) {
                        result.push(Index::Point(index));
                    } else {
                        all_hit = false;
                    }
                }
                Index::Range(s, e) => {
                    for i in s..=e {
                        if let Some(index) = check_fn(i) {
                            result.push(Index::Point(index));
                        } else {
                            all_hit = false;
                        }
                    }
                }
            }
        }

        (result, all_hit)
    }

    /// Read file from cache
    pub(crate) fn read(&self, file_ino: INum, index: Vec<Index>) -> Vec<IoMemBlock> {
        let guard = pin();
        let Some(file_cache) = self.inner.get(&file_ino, &guard) else {
            debug!(
                "cache for {:?} is empty, don't need to invalidate",
                file_ino,
            );
            return vec![];
        };

        let bucket_size = self.bucket_size_in_block;
        let block_size = self.block_size;

        let read_fn = |global_index: usize| -> IoMemBlock {
            let hash_index = global_index.overflow_div(bucket_size);
            let bucket_opt = file_cache.get(&hash_index, &guard);
            // if let Some(bucket) = bucket_opt {
            // if let Some(block) = bucket
            // .write()
            // .get(global_index.overflow_rem(bucket_size))
            // .unwrap_or_else(|| {
            // panic!(
            // "error when getting range of {} in the cache bucket",
            // global_index.overflow_rem(bucket_size)
            // )
            // })
            // {
            // IoMemBlock::new(Some(block.clone()), 0, block_size)
            // } else {
            // IoMemBlock::new(None, 0, 0)
            // }
            // } else {
            // IoMemBlock::new(None, 0, 0)
            // }

            bucket_opt.map_or_else(
                || IoMemBlock::new(None, 0, 0),
                |bucket| {
                    bucket
                        .write()
                        .get(global_index.overflow_rem(bucket_size))
                        .unwrap_or_else(|| {
                            panic!(
                                "error when getting range of {} in the cache bucket",
                                global_index.overflow_rem(bucket_size)
                            )
                        })
                        .as_ref()
                        .map_or_else(
                            || IoMemBlock::new(None, 0, 0),
                            |block| IoMemBlock::new(Some(block.clone()), 0, block_size),
                        )
                },
            )
        };

        let mut result = Vec::new();

        for i in index {
            match i {
                Index::Point(p) => {
                    result.push(read_fn(p));
                }
                Index::Range(s, e) => {
                    for i in s..=e {
                        result.push(read_fn(i));
                    }
                }
            }
        }

        result
    }

    /// Update the Cache.
    ///
    /// 1. `offset` be `MemoryBlock` aligned.
    /// 2. `len` should be multiple times of `MemoryBlock` Size unless it
    ///    contains the file's last `MemoryBlock`.
    pub(crate) async fn write_or_update(
        &self,
        file_ino: INum,
        offset: usize,
        len: usize,
        buf: &[u8],
        overwrite: bool,
    ) {
        let exist = self.write_or_update_helper(file_ino, offset, len, buf, overwrite);
        if !exist {
            if let Some(ref kv_engine) = self.kv_engine {
                if let Some(ref id) = self.node_id {
                    if let Err(e) = add_node_to_file_list(kv_engine, id, file_ino).await {
                        panic!("Cannot add node {id} to file {file_ino:?} node list, error: {e}");
                    }
                }
            }
        }
    }

    /// Update the Cache Helper
    ///
    /// 1. `offset` be `MemoryBlock` aligned.
    /// 2. `len` should be multiple times of `MemoryBlock` Size unless it
    ///    contains the file's last `MemoryBlock`.
    #[allow(clippy::too_many_lines)]
    fn write_or_update_helper(
        &self,
        file_ino: INum,
        offset: usize,
        len: usize,
        buf: &[u8],
        overwrite: bool,
    ) -> bool {
        let guard = pin();
        let (exist, file_cache) = if let Some(cache) = self.inner.get(&file_ino, &guard) {
            (true, cache)
        } else {
            debug!("cache for {:?} is empty, create one", file_ino);
            // Here maybe a racing case where two thread are updating the cache
            self.inner.insert(file_ino, HashMap::new());
            let file_cache = self.inner.get(&file_ino, &guard).unwrap_or_else(|| {
                panic!(
                    "Just insert a file cache ({file_ino:?}) into global cache mapping, but cannot get it."
                )
            });

            (false, file_cache)
        };

        if len == 0 {
            return exist;
        }

        let bucket_size = self.bucket_size_in_block.overflow_mul(self.block_size);
        let bucket_start_offset = offset
            .overflow_rem(bucket_size)
            .overflow_div(self.block_size);
        // This end_offset is included
        let bucket_end_offset = offset
            .overflow_add(len)
            .overflow_sub(1)
            .overflow_rem(bucket_size)
            .overflow_div(self.block_size);
        let start_key = offset.overflow_div(bucket_size);
        // This index is included
        let end_key = offset
            .overflow_add(len)
            .overflow_sub(1)
            .overflow_div(bucket_size);

        let mut have_read: usize = 0;
        let mut grow_memory: usize = 0;
        let mut is_first_block: bool = true;
        let mut copy_fn = |b: &mut Option<MemBlock>| {
            if b.is_some() && !overwrite {
                return;
            }

            if b.is_none() {
                *b = Some(MemBlock::new(self.block_size));
                grow_memory = grow_memory.overflow_add(1);
            }

            let end = if is_first_block {
                let off = offset.overflow_rem(self.block_size);
                std::cmp::min(
                    have_read.overflow_add(self.block_size).overflow_sub(off),
                    len,
                )
            } else {
                std::cmp::min(have_read.overflow_add(self.block_size), len)
            };

            assert!(b.is_some());
            if let Some(ref block) = *b {
                if is_first_block {
                    is_first_block = false;
                    let off = offset.overflow_rem(self.block_size);
                    block.overwrite_offset(
                        off,
                        (*buf).get(have_read..end).unwrap_or_else(|| {
                            panic!("should not reach here, buf out of range in cache write")
                        }),
                    );
                } else {
                    block.overwrite((*buf).get(have_read..end).unwrap_or_else(|| {
                        panic!("should not reach here, buf out of range in cache write")
                    }));
                }
                have_read = end;
            }
        };

        for i in start_key..=end_key {
            let s = if i == start_key {
                bucket_start_offset
            } else {
                0
            };
            let l = if i == end_key {
                bucket_end_offset.overflow_add(1)
            } else {
                self.bucket_size_in_block
            };

            if let Some(bucket) = file_cache.get(&i, &guard) {
                self.queue
                    .lock()
                    .push(bucket.clone(), 0.overflow_sub(now_monotonic()));
                bucket
                    .write()
                    .get_mut(s..l)
                    .unwrap_or_else(|| {
                        panic!("Just insert MemBlockBucket into file cache, but cannot get it.")
                    })
                    .iter_mut()
                    .for_each(&mut copy_fn);
            } else {
                // Here might be a racing case
                debug!(
                    "memory_block_bucket for file {:?} index {} is empty, create one",
                    file_ino, start_key
                );
                debug!("start offset {}, end offset {}", s, l);
                let bucket = MemBlockBucket::new();
                self.queue
                    .lock()
                    .push(bucket.clone(), 0.overflow_sub(now_monotonic()));
                file_cache.insert(i, bucket);
                file_cache
                    .get(&i, &guard)
                    .unwrap_or_else(|| {
                        panic!("Just insert MemBlockBucket into file cache, but cannot get it.")
                    })
                    .write()
                    .get_mut(s..l)
                    .unwrap_or_else(|| panic!("should not reach here, out of range in cache write"))
                    .iter_mut()
                    .for_each(&mut copy_fn);
            };
        }

        self.size
            .fetch_add(grow_memory.overflow_mul(self.block_size), Ordering::Relaxed);

        let mut dealloc_cnt = 0;
        let mut dealloc_fn = |b: &mut Option<MemBlock>| {
            if b.is_some() {
                dealloc_cnt = dealloc_cnt.overflow_add(1);
                b.take();
            }
        };

        let mut exceed: i64 = self
            .size
            .load(Ordering::Relaxed)
            .cast::<i64>()
            .overflow_sub(self.capacity.cast::<i64>());
        while exceed > 0 {
            match self.queue.lock().pop() {
                Some((bucket, _)) => {
                    let mut bucket_lock = bucket.write();
                    let removed_count = bucket_lock.iter_mut().filter(|b| b.is_some()).count();
                    bucket_lock
                        .iter_mut()
                        .filter(|b| b.is_some())
                        .for_each(&mut dealloc_fn);
                    exceed = exceed
                        .overflow_sub(removed_count.overflow_mul(self.block_size).cast::<i64>());
                }
                None => break,
            };
        }

        if dealloc_cnt > 0 {
            self.size
                .fetch_sub(dealloc_cnt.overflow_mul(self.block_size), Ordering::Relaxed);
        }
        exist
    }

    /// Remove file cache
    pub(crate) async fn remove_file_cache(&self, file_ino: INum) -> bool {
        if let Some(ref kv_engine) = self.kv_engine {
            if let Some(ref id) = self.node_id {
                if let Err(e) = remove_node_from_file_list(kv_engine, id, file_ino).await {
                    panic!("Cannot remove node {id} to file {file_ino:?} node list, error: {e}");
                }
            }
        }
        self.inner.remove(&file_ino)
    }

    /// Round down `value` to `align`, align must be power of 2
    #[inline]
    pub fn round_down(&self, value: usize) -> usize {
        (value.cast::<i64>() & (!(self.get_align().cast::<i64>().overflow_sub(1)))).cast()
    }

    /// Round up `value` to `align`, align must be power of 2
    #[inline]
    pub fn round_up(&self, value: usize) -> usize {
        match value & (self.get_align().overflow_sub(1)) {
            0 => value,
            _ => self.round_down(value).overflow_add(self.get_align()),
        }
    }
}

/// Get current time
#[inline]
fn now_monotonic() -> i64 {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let ret = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_COARSE, &mut time) };
    assert!(ret == 0_i32);
    time.tv_sec
        .overflow_mul(1000)
        .overflow_add(time.tv_nsec.overflow_div(1_000_000))
}

/// A memory block collection
struct MemBlockBucket {
    /// The inner is read-write lock protected vector of `MemBlock`
    inner: Arc<RwLock<Vec<Option<MemBlock>>>>,
}

impl Hash for MemBlockBucket {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (*self.inner).data_ptr().hash(state);
    }
}

impl PartialEq<Self> for MemBlockBucket {
    fn eq(&self, other: &Self) -> bool {
        (*self.inner).data_ptr() == (*other.inner).data_ptr()
    }
}

impl Eq for MemBlockBucket {}

impl Clone for MemBlockBucket {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::<
                parking_lot::lock_api::RwLock<parking_lot::RawRwLock, Vec<Option<MemBlock>>>,
            >::clone(&self.inner),
        }
    }
}

/// A vector holding `MEMORY_BUCKET_VEC_SIZE` `MemBlock`, wrapped in an Arc.
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

    /// Insert one `MemBlock` into the `index` position of the bucket
    #[allow(dead_code)]
    pub(crate) fn insert(&mut self, index: usize, mem: MemBlock) {
        if let Some(memblock) = self.inner.write().get_mut(index) {
            *memblock = Some(mem);
        } else {
            panic!("index={index} is out of bound of MemBlockBucket");
        }
    }
}

impl Deref for MemBlockBucket {
    type Target = RwLock<Vec<Option<MemBlock>>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// A wrapper for `IoMemBlock`, which is used for I/O operations
pub struct IoMemBlock {
    /// The inner `MemBlock` that contains data
    inner: Option<MemBlock>,
    /// The offset for this `MemBlock`
    offset: usize,
    /// The end offset for this `MemBlock`
    end: usize,
}

impl Debug for IoMemBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IoMemBlock")
            .field("offset", &self.offset)
            .field("end", &self.end)
            .finish()
    }
}

impl IoMemBlock {
    /// The constructor of `IoMemBlock`
    const fn new(inner: Option<MemBlock>, offset: usize, end: usize) -> Self {
        Self { inner, offset, end }
    }

    /// Turn `IoMemBlock` into slice
    #[allow(dead_code)]
    pub(crate) unsafe fn as_slice(&self) -> &[u8] {
        assert!(
            self.inner.is_some(),
            "IoMemBlock inner is None, cannot as_slice"
        );

        if let Some(ref block) = self.inner {
            let ptr = block.as_ptr_from_offset(self.offset);
            std::slice::from_raw_parts(ptr, self.end.overflow_sub(self.offset))
        } else {
            panic!("the inner Option<MemBlock> should not be none");
        }
    }
}

impl CouldBeAsIoVecList for IoMemBlock {}

impl AsIoVec for IoMemBlock {
    fn as_io_vec(&self) -> IoVec<&[u8]> {
        assert!(self.can_convert(), "Please check before convert to IoVec");
        IoVec::from_slice(unsafe { self.as_slice() })
    }

    fn can_convert(&self) -> bool {
        self.inner.is_some()
    }

    fn len(&self) -> usize {
        self.end.overflow_sub(self.offset)
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Clone for IoMemBlock {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            offset: self.offset,
            end: self.end,
        }
    }
}

/// `Memblock` that holds a block of memory
struct MemBlock {
    /// A rwlock wrapper of the inner memory block
    inner: Arc<RwLock<AlignedBytes>>,
}

impl MemBlock {
    /// constructor for the `MemBlock` with capacity setting
    #[allow(dead_code)]
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(RwLock::new(AlignedBytes::new_zeroed(capacity, PAGE_SIZE))),
        }
    }

    /// constructor for the `MemBlock` from a u8 slice
    #[allow(dead_code)]
    pub(crate) fn new_from_slice(slice: &[u8]) -> Self {
        Self {
            inner: Arc::new(RwLock::new(AlignedBytes::new_from_slice(slice, PAGE_SIZE))),
        }
    }

    #[allow(dead_code)]
    /// Overwrite the content in the `MemBlock`
    pub(crate) fn overwrite(&self, slice: &[u8]) {
        (*(*(self.inner).write()))
            .get_mut(..slice.len())
            .unwrap_or_else(|| {
                panic!(
                    "overflow when overrwite Memblock, slice length {} is too long",
                    slice.len()
                )
            })
            .copy_from_slice(slice);
    }

    #[allow(dead_code)]
    /// Overwrite the content in the `Memblock` starting from offset
    pub(crate) fn overwrite_offset(&self, offset: usize, slice: &[u8]) {
        (*(*(self.inner).write()))
            .get_mut(offset..slice.len().overflow_add(offset))
            .unwrap_or_else(|| {
                panic!(
                    "overflow when overrwite Memblock, slice length {} from offset  {} is too long",
                    slice.len(),
                    offset,
                )
            })
            .copy_from_slice(slice);
    }

    /// Get the pointer point to the inner memory starting from offset
    pub(crate) fn as_ptr_from_offset(&self, offset: usize) -> *mut u8 {
        (**self.write())
            .get_mut(offset..)
            .unwrap_or_else(|| {
                panic!("overflow when get pointer for Memblock from offset {offset}")
            })
            .as_mut_ptr()
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
            inner: Arc::<RwLock<AlignedBytes>>::clone(&self.inner),
        }
    }
}

#[cfg(test)]
mod test {
    use aligned_utils::bytes::AlignedBytes;

    use super::{
        GlobalCache, MEMORY_BLOCK_SIZE_IN_BYTE, MEMORY_BUCKET_SIZE_IN_BYTE, MEMORY_BUCKET_VEC_SIZE,
    };
    use crate::async_fuse::fuse::fuse_reply::AsIoVec;

    #[test]
    fn test_get_empty_cache() {
        let global = GlobalCache::new();
        let cache = global.get_file_cache(0, 0, 1024);
        assert!(cache.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_insert_one_byte_cache() {
        let global = GlobalCache::new();
        let file_ino = 6789;
        let content = AlignedBytes::new_from_slice(&[b'a'], 1);
        global.write_or_update(file_ino, 0, 1, &content, true).await;

        let cache = global.get_file_cache(file_ino, 0, 1);
        assert_eq!(cache.len(), 1);
        assert!(cache
            .get(0)
            .unwrap_or_else(|| panic!("index error"))
            .can_convert());
        assert_eq!(
            unsafe {
                cache
                    .get(0)
                    .unwrap_or_else(|| panic!("index error"))
                    .as_slice()
                    .first()
                    .unwrap_or_else(|| panic!("index error"))
            },
            &b'a'
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_partial_result() {
        let global = GlobalCache::new();
        let file_ino = 1;
        let content = AlignedBytes::new_from_slice(&[b'a'], 1);
        global
            .write_or_update(file_ino, MEMORY_BLOCK_SIZE_IN_BYTE, 1, &content, true)
            .await;

        let cache = global.get_file_cache(file_ino, 0, MEMORY_BLOCK_SIZE_IN_BYTE + 1);
        assert_eq!(cache.len(), 2);
        assert!(!cache
            .get(0)
            .unwrap_or_else(|| panic!("index error"))
            .can_convert());
        assert!(cache
            .get(1)
            .unwrap_or_else(|| panic!("index error"))
            .can_convert());
        assert_eq!(
            unsafe {
                cache
                    .get(1)
                    .unwrap_or_else(|| panic!("index error"))
                    .as_slice()
                    .first()
                    .unwrap_or_else(|| panic!("index error"))
            },
            &b'a'
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_panic_write_unaligned_data() {
        let global = GlobalCache::new();
        let file_ino = 1;
        let content = AlignedBytes::new_from_slice(&[b'a'], 1);
        global.write_or_update(file_ino, 1, 1, &content, true).await;

        let cache = global.get_file_cache(file_ino, 0, 2);
        assert_eq!(cache.len(), 1);
        assert!(cache
            .get(0)
            .unwrap_or_else(|| panic!("index error"))
            .can_convert());
        assert_eq!(
            unsafe {
                cache
                    .get(0)
                    .unwrap_or_else(|| panic!("index error"))
                    .as_slice()
                    .get(..2)
                    .unwrap_or_else(|| panic!("index error"))
            },
            [b'\0', b'a']
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_eviction() {
        let global = GlobalCache::new_with_capacity(MEMORY_BLOCK_SIZE_IN_BYTE);
        let file_ino = 1;
        let block_one = AlignedBytes::new_from_slice(&[b'a'], 1);
        global
            .write_or_update(file_ino, 0, 1, &block_one, true)
            .await;

        let block_two = AlignedBytes::new_from_slice(&[b'b'], 1);
        global
            .write_or_update(file_ino, MEMORY_BUCKET_SIZE_IN_BYTE, 1, &block_two, true)
            .await;

        // First cache should be evicted
        let cache = global.get_file_cache(file_ino, 0, MEMORY_BUCKET_SIZE_IN_BYTE + 1);
        assert_eq!(cache.len(), 17);
        for item in cache.iter().take(MEMORY_BUCKET_VEC_SIZE) {
            assert!(!item.can_convert());
        }
        assert!(cache
            .get(MEMORY_BUCKET_VEC_SIZE)
            .unwrap_or_else(|| panic!("index error"))
            .can_convert());
        assert_eq!(
            unsafe {
                cache
                    .get(MEMORY_BUCKET_VEC_SIZE)
                    .unwrap_or_else(|| panic!("index error"))
                    .as_slice()
                    .first()
                    .unwrap_or_else(|| panic!("index error"))
            },
            &b'b'
        );
        assert_eq!(global.get_size(), MEMORY_BLOCK_SIZE_IN_BYTE);
    }
}
