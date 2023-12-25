//! Utilities for blocks.

use std::fmt::Formatter;
use std::sync::Arc;

use aligned_utils::bytes::AlignedBytes;
use clippy_utilities::OverflowArithmetic;
use nix::sys::uio::IoVec;

use crate::async_fuse::fuse::fuse_reply::{AsIoVec, CouldBeAsIoVecList};
use crate::async_fuse::fuse::protocol::INum;

/// Page Size
const PAGE_SIZE: usize = 4096;

/// A common coordinate to locate a block.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct BlockCoordinate(pub INum, pub usize);

/// Make a partial slice of `u8` slice for debugging.
fn make_partial_u8_slice(src: &[u8], start: usize, len: usize) -> String {
    let end = start.overflow_add(len).min(src.len());
    let show_length = end.overflow_sub(start);
    let slice = src.get(start..end).unwrap_or(&[]);

    let mut result = String::from("[");

    for (i, &ch) in slice.iter().enumerate() {
        result.push_str(format!("{ch}").as_str());
        if i != show_length.overflow_sub(1) {
            result.push_str(", ");
        }
    }

    if end != src.len() {
        result.push_str(", ...");
    }

    result.push(']');

    result
}

/// The minimum unit of data in the storage layers.
#[derive(Clone)]
pub struct Block {
    /// The underlying data of a block. Shared with `Arc`.
    inner: Arc<AlignedBytes>,
    /// A flag that if this block is dirty.
    dirty: bool,
}

impl std::fmt::Debug for Block {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let inner_slice = make_partial_u8_slice(self.inner.as_ref(), 0, 8);

        let result = format!("Block {{ inner: {inner_slice}, dirty: {} }}", self.dirty());

        write!(f, "{result}")
    }
}

impl Block {
    /// Create a block with `capacity`, which usually equals the `block_size` of
    /// storage manager.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Block {
            inner: Arc::new(AlignedBytes::new_zeroed(capacity, PAGE_SIZE)),
            dirty: false,
        }
    }

    /// Returns the length of the block, which usually equals the `block_size`
    /// of storage manager.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Check if the block is with size of 0.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Get a mutable slice of the underlying data, copy them if there are other
    /// blocks hold the same data with `Arc`. See also [`Arc::make_mut`](fn@
    /// `std::sync::Arc::make_mut`).
    pub fn make_mut(&mut self) -> &mut [u8] {
        Arc::make_mut(&mut self.inner).as_mut()
    }

    /// Checks if the block is dirty.
    #[must_use]
    pub fn dirty(&self) -> bool {
        self.dirty
    }

    /// Sets the block to be dirty.
    pub fn set_dirty(&mut self) {
        self.dirty = true;
    }
}

/// A wrapper for `IoBlock`, which is used for I/O operations
#[derive(Clone)]
pub struct IoBlock {
    /// The inner `Block` that contains data
    inner: Block,
    /// The offset for this `Block`
    offset: usize,
    /// The end offset for this `Block`
    end: usize,
}

impl std::fmt::Debug for IoBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let inner_slice = make_partial_u8_slice(self.as_slice(), 0, 8);

        let result = format!(
            "IoBlock {{ inner: {inner_slice}, offset: {}, end: {}, dirty: {} }}",
            self.offset,
            self.end,
            self.dirty()
        );

        write!(f, "{result}")
    }
}

impl IoBlock {
    /// The constructor of `IoBlock`
    #[must_use]
    pub fn new(inner: Block, offset: usize, end: usize) -> Self {
        debug_assert!(offset <= end);
        debug_assert!(
            end <= inner.len(),
            "The end {end} of slice is out of range of the inner block."
        );
        Self { inner, offset, end }
    }

    /// The inner block
    #[must_use]
    pub fn block(&self) -> &Block {
        &self.inner
    }

    /// The offset of valid bytes of the inner block
    #[must_use]
    pub const fn offset(&self) -> usize {
        self.offset
    }

    /// The end offset of valid bytes of the inner block
    #[must_use]
    pub const fn end(&self) -> usize {
        self.end
    }

    /// Turn `IoBlock` into slice
    pub(crate) fn as_slice(&self) -> &[u8] {
        self.inner
            .inner
            .get(self.offset..self.end)
            .unwrap_or_else(|| {
                unreachable!(
                    "`{}..{}` is checked not to be out of range of inner block.",
                    self.offset, self.end,
                )
            })
    }

    /// Checks if the inner block is dirty.
    #[must_use]
    pub fn dirty(&self) -> bool {
        self.inner.dirty
    }

    /// Sets the inner block to be dirty.
    pub fn set_dirty(&mut self) {
        self.inner.dirty = true;
    }
}

#[cfg(test)]
impl From<Block> for IoBlock {
    fn from(block: Block) -> Self {
        let len = block.len();
        IoBlock::new(block, 0, len)
    }
}

impl CouldBeAsIoVecList for IoBlock {}

impl AsIoVec for IoBlock {
    fn as_io_vec(&self) -> IoVec<&[u8]> {
        IoVec::from_slice(self.as_slice())
    }

    fn can_convert(&self) -> bool {
        true
    }

    fn len(&self) -> usize {
        self.end.overflow_sub(self.offset)
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use clippy_utilities::OverflowArithmetic;

    use super::{Arc, Block, IoBlock};
    use crate::async_fuse::fuse::fuse_reply::AsIoVec;

    const BLOCK_SIZE_IN_BYTES: usize = 8;
    const BLOCK_CONTENT: &[u8; BLOCK_SIZE_IN_BYTES] = b"foo bar ";

    #[test]
    fn test_block() {
        let mut block = Block::new(BLOCK_SIZE_IN_BYTES);
        assert_eq!(block.len(), BLOCK_SIZE_IN_BYTES);
        assert!(!block.is_empty());

        block.make_mut().copy_from_slice(BLOCK_CONTENT);
        assert_eq!(block.inner.get(..), Some(BLOCK_CONTENT.as_slice()));

        let _another_block = block.clone();
        assert_eq!(Arc::strong_count(&block.inner), 2);

        assert_eq!(
            format!("{block:?}"),
            "Block { inner: [102, 111, 111, 32, 98, 97, 114, 32], dirty: false }"
        );
    }

    #[test]
    fn test_small_block() {
        let mut block = Block::new(4);
        assert_eq!(block.len(), 4);

        block.make_mut().copy_from_slice(&BLOCK_CONTENT[..4]);
        assert_eq!(block.inner.get(..), Some(&BLOCK_CONTENT[..4]));

        assert_eq!(
            format!("{block:?}"),
            "Block { inner: [102, 111, 111, 32], dirty: false }"
        );
    }

    #[test]
    fn test_large_block() {
        let mut block = Block::new(BLOCK_SIZE_IN_BYTES.overflow_mul(2));
        assert_eq!(block.len(), BLOCK_SIZE_IN_BYTES.overflow_mul(2));

        block
            .make_mut()
            .copy_from_slice(BLOCK_CONTENT.repeat(2).as_slice());

        assert_eq!(
            format!("{block:?}"),
            "Block { inner: [102, 111, 111, 32, 98, 97, 114, 32, ...], dirty: false }"
        );
    }

    #[test]
    fn test_io_block() {
        let mut block = Block::new(BLOCK_SIZE_IN_BYTES.overflow_mul(2));
        block
            .make_mut()
            .copy_from_slice(BLOCK_CONTENT.repeat(2).as_slice());

        let mut io_block = IoBlock::from(block);
        assert_eq!(io_block.len(), BLOCK_SIZE_IN_BYTES.overflow_mul(2));
        assert_eq!(io_block.as_slice(), BLOCK_CONTENT.repeat(2).as_slice());

        io_block.offset = 1;
        io_block.end = 5;
        assert_eq!(io_block.len(), 4);
        assert_eq!(io_block.as_slice(), b"oo b");

        assert_eq!(
            format!("{io_block:?}"),
            "IoBlock { inner: [111, 111, 32, 98], offset: 1, end: 5, dirty: false }"
        );

        io_block.offset = 0;
        io_block.end = BLOCK_SIZE_IN_BYTES.overflow_mul(2);
        assert_eq!(
            format!("{io_block:?}"),
            "IoBlock { inner: [102, 111, 111, 32, 98, 97, 114, 32, ...], offset: 0, end: 16, dirty: false }"
        );
    }

    #[test]
    #[should_panic(expected = "out of range")]
    fn test_io_block_out_of_range() {
        let io_block = IoBlock::new(Block::new(8), 0, 16);
        let _: &[u8] = io_block.as_slice();
    }

    #[test]
    fn test_dirty_block() {
        let mut block = Block::new(8);
        assert!(!block.dirty());
        block.set_dirty();
        assert!(block.dirty());

        let io_block = IoBlock::from(block);
        assert!(io_block.dirty());

        let mut io_block = IoBlock::new(Block::new(8), 0, 8);
        assert!(!io_block.dirty());
        io_block.set_dirty();
        assert!(io_block.dirty());
    }
}
