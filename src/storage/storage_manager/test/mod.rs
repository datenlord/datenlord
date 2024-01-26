mod common;
mod concurrency;
mod latency;

const BLOCK_SIZE_IN_BYTES: usize = 8;
const BLOCK_CONTENT: &[u8; BLOCK_SIZE_IN_BYTES] = b"foo bar ";
const CACHE_CAPACITY_IN_BLOCKS: usize = 4;
