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
    /// The start offset for this `Block`
    start: usize,
    /// The end offset for this `Block`
    end: usize,
    /// A flag that if this block is dirty.
    dirty: bool,
}

impl std::fmt::Debug for Block {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let inner_slice = make_partial_u8_slice(self.inner.as_ref(), 0, 8);

        write!(
            f,
            "Block {{ inner: {inner_slice}, start: {}, end: {}, dirty: {} }}",
            self.start,
            self.end,
            self.dirty()
        )
    }
}

impl Block {
    /// Creates a block with `size`.
    #[must_use]
    pub fn new_zeroed(size: usize) -> Self {
        Block {
            inner: Arc::new(AlignedBytes::new_zeroed(size, PAGE_SIZE)),
            start: 0,
            end: size,
            dirty: false,
        }
    }

    /// Creates a block with `size` and a range.
    #[must_use]
    pub fn new_zeroed_with_range(size: usize, start: usize, end: usize) -> Self {
        Block {
            inner: Arc::new(AlignedBytes::new_zeroed(size, PAGE_SIZE)),
            start,
            end,
            dirty: false,
        }
    }

    /// Creates a block with `size` and a data slice.
    ///
    /// If `data` is longer than the block, the rest of the slice will be
    /// ignored; if `data` is shorter than the block, the rest part of the
    /// block will remains 0.
    #[must_use]
    pub fn from_slice(size: usize, data: &[u8]) -> Self {
        let mut inner = AlignedBytes::new_zeroed(size, PAGE_SIZE);

        let write_len = data.len().min(size);

        let (dest, src) = inner
            .get_mut(..write_len)
            .zip(data.get(..write_len))
            .unwrap_or_else(|| unreachable!("write len is checked not to be out of range."));
        dest.copy_from_slice(src);

        Block {
            inner: Arc::new(inner),
            start: 0,
            end: size,
            dirty: false,
        }
    }

    /// Creates a block with `size` and a data slice, the content of data slice
    /// will be written to the block from `start` offset.
    ///
    /// If `data` is longer than the block, the rest of the slice will be
    /// ignored; if `data` is shorter than the block, the rest part of the
    /// block will remains 0.
    #[must_use]
    pub fn from_slice_with_range(size: usize, start: usize, end: usize, data: &[u8]) -> Self {
        debug_assert!(start <= end);
        debug_assert!(
            end <= size,
            "The end {end} of slice is out of range of the inner block."
        );

        let mut inner = AlignedBytes::new_zeroed(size, PAGE_SIZE);

        let block_len = end.overflow_sub(start);
        let write_len = data.len().min(block_len);
        let block_write_end = start.overflow_add(write_len);

        let (dest, src) = inner
            .get_mut(start..block_write_end)
            .zip(data.get(..write_len))
            .unwrap_or_else(|| unreachable!("write len is checked not to be out of range."));
        dest.copy_from_slice(src);

        Block {
            inner: Arc::new(inner),
            start,
            end,
            dirty: false,
        }
    }

    /// Returns the start offset of valid bytes of the inner block
    #[must_use]
    pub const fn start(&self) -> usize {
        self.start
    }

    /// Returns the end offset of valid bytes of the inner block
    #[must_use]
    pub const fn end(&self) -> usize {
        self.end
    }

    /// Sets the start offset of valid bytes of the inner block
    pub fn set_start(&mut self, start: usize) {
        self.start = start;
    }

    /// Sets the end offset of valid bytes of the inner block
    pub fn set_end(&mut self, end: usize) {
        self.end = end;
    }

    /// Gets the valid range of this block.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        self.inner.get(self.start..self.end).unwrap_or_else(|| {
            unreachable!(
                "`{}..{}` is checked not to be out of range of the block.",
                self.start, self.end,
            )
        })
    }

    /// Gets a mutable slice of the the valid range, copy them if there are
    /// other blocks hold the same data via `Arc`. See also
    /// [`Arc::make_mut`](fn@ `std::sync::Arc::make_mut`).
    pub fn make_mut_slice(&mut self) -> &mut [u8] {
        Arc::make_mut(&mut self.inner)
            .get_mut(self.start..self.end)
            .unwrap_or_else(|| {
                unreachable!(
                    "`{}..{}` is checked not to be out of range of the block.",
                    self.start, self.end,
                )
            })
    }

    /// Updates `self` with another block.
    ///
    /// Contents in `self` that overlaps with the `other` range will be
    /// overwritten, if the range of `other` is out of range of `self`, the
    /// range of `self` will be extended.
    ///
    /// This method calls `make_mut_slice` internal.
    pub fn update(&mut self, other: &Block) {
        // TODO: Return error instead of panic.
        assert!(self.inner.len() >= other.end, "out of range");

        self.start = other.start.min(self.start);
        self.end = other.end.max(self.end);

        let write_start = other.start.overflow_sub(self.start);
        let write_end = other.end.overflow_sub(self.start);

        self.make_mut_slice()
            .get_mut(write_start..write_end)
            .unwrap_or_else(|| unreachable!("Write range is checked not to be out of range."))
            .copy_from_slice(other.as_slice());
    }

    /// Checks if the block is dirty.
    #[must_use]
    pub fn dirty(&self) -> bool {
        self.dirty
    }

    /// Sets the block to be dirty.
    pub fn set_dirty(&mut self, dirty: bool) {
        self.dirty = dirty;
    }
}

impl CouldBeAsIoVecList for Block {}

impl AsIoVec for Block {
    fn as_io_vec(&self) -> IoVec<&[u8]> {
        IoVec::from_slice(self.as_slice())
    }

    fn can_convert(&self) -> bool {
        true
    }

    fn len(&self) -> usize {
        self.end.overflow_sub(self.start)
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use clippy_utilities::OverflowArithmetic;

    use super::{Arc, Block};
    use crate::async_fuse::fuse::fuse_reply::AsIoVec;

    const BLOCK_SIZE_IN_BYTES: usize = 8;
    const BLOCK_CONTENT: &[u8; BLOCK_SIZE_IN_BYTES] = b"foo bar ";

    #[test]
    fn test_block() {
        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        assert_eq!(block.len(), BLOCK_SIZE_IN_BYTES);
        assert!(!block.is_empty());
        assert_eq!(block.inner.get(..), Some(BLOCK_CONTENT.as_slice()));

        let _another_block = block.clone();
        assert_eq!(Arc::strong_count(&block.inner), 2);

        assert_eq!(
            format!("{block:?}"),
            "Block { inner: [102, 111, 111, 32, 98, 97, 114, 32], start: 0, end: 8, dirty: false }"
        );
    }

    #[test]
    fn test_small_block() {
        let block = Block::from_slice(4, BLOCK_CONTENT);
        assert_eq!(block.len(), 4);
        assert_eq!(block.inner.get(..), Some(&BLOCK_CONTENT[..4]));

        assert_eq!(
            format!("{block:?}"),
            "Block { inner: [102, 111, 111, 32], start: 0, end: 4, dirty: false }"
        );
    }

    #[test]
    fn test_large_block() {
        let block = Block::from_slice(
            BLOCK_SIZE_IN_BYTES.overflow_mul(2),
            BLOCK_CONTENT.repeat(2).as_slice(),
        );
        assert_eq!(block.len(), BLOCK_SIZE_IN_BYTES.overflow_mul(2));

        assert_eq!(
            format!("{block:?}"),
            "Block { inner: [102, 111, 111, 32, 98, 97, 114, 32, ...], start: 0, end: 16, dirty: false }"
        );
    }

    #[test]
    fn test_make_mut() {
        let mut block = Block::new_zeroed(BLOCK_SIZE_IN_BYTES);

        assert_eq!(block.len(), BLOCK_SIZE_IN_BYTES);

        block.make_mut_slice().copy_from_slice(BLOCK_CONTENT);
        assert_eq!(block.inner.get(..), Some(BLOCK_CONTENT.as_slice()));
    }

    #[test]
    fn test_make_mut_slice() {
        let mut block = Block::new_zeroed_with_range(BLOCK_SIZE_IN_BYTES, 1, 4);

        assert_eq!(block.len(), 3);

        block.make_mut_slice().copy_from_slice(&BLOCK_CONTENT[..3]);
        assert_eq!(block.inner.get(..), Some(b"\0foo\0\0\0\0".as_slice()));
    }

    #[test]
    fn test_empty_block() {
        let block = Block::new_zeroed_with_range(BLOCK_SIZE_IN_BYTES, 0, 0);

        assert_eq!(block.len(), 0);
        assert!(block.is_empty());
    }

    #[test]
    fn test_as_slice() {
        let mut block = Block::from_slice(
            BLOCK_SIZE_IN_BYTES.overflow_mul(2),
            BLOCK_CONTENT.repeat(2).as_slice(),
        );

        assert_eq!(block.len(), BLOCK_SIZE_IN_BYTES.overflow_mul(2));
        assert_eq!(block.as_slice(), BLOCK_CONTENT.repeat(2).as_slice());

        block.set_start(1);
        block.set_end(5);
        assert_eq!(block.len(), 4);
        assert_eq!(block.as_slice(), b"oo b");
    }

    #[test]
    fn test_from_slice_with_range() {
        let block = Block::from_slice_with_range(BLOCK_SIZE_IN_BYTES, 1, 4, BLOCK_CONTENT);
        assert_eq!(block.start(), 1);
        assert_eq!(block.end(), 4);
        assert_eq!(block.len(), 3);
        assert_eq!(block.inner.get(..), Some(b"\0foo\0\0\0\0".as_slice()));

        let block = Block::from_slice_with_range(BLOCK_SIZE_IN_BYTES, 1, 4, &BLOCK_CONTENT[..1]);
        assert_eq!(block.inner.get(..), Some(b"\0f\0\0\0\0\0\0".as_slice()));
    }

    #[test]
    #[should_panic(expected = "out of range")]
    fn test_io_block_out_of_range() {
        let _block = Block::from_slice_with_range(4, 0, 8, b"abcd");
    }

    #[test]
    fn test_dirty_block() {
        let mut block = Block::new_zeroed(8);
        assert!(!block.dirty());
        block.set_dirty(true);
        assert!(block.dirty());
    }

    #[test]
    fn test_update() {
        let mut block_dest = Block::new_zeroed(BLOCK_SIZE_IN_BYTES);
        let block_src = Block::from_slice_with_range(BLOCK_SIZE_IN_BYTES, 0, 4, BLOCK_CONTENT);

        block_dest.update(&block_src);

        assert_eq!(block_dest.as_slice(), b"foo \0\0\0\0");
    }

    #[test]
    fn test_update_extend_left() {
        let mut block_dest = Block::new_zeroed_with_range(BLOCK_SIZE_IN_BYTES, 4, 4);
        let block_src = Block::from_slice_with_range(BLOCK_SIZE_IN_BYTES, 0, 4, BLOCK_CONTENT);

        block_dest.update(&block_src);

        assert_eq!(block_dest.start(), 0);
        assert_eq!(block_dest.end(), 4);
        assert_eq!(block_dest.as_slice(), b"foo ");
    }

    #[test]
    fn test_update_extend_right() {
        let mut block_dest = Block::new_zeroed_with_range(BLOCK_SIZE_IN_BYTES, 4, 4);
        let block_src = Block::from_slice_with_range(BLOCK_SIZE_IN_BYTES, 4, 7, BLOCK_CONTENT);

        block_dest.update(&block_src);

        assert_eq!(block_dest.start(), 4);
        assert_eq!(block_dest.end(), 7);
        assert_eq!(block_dest.as_slice(), b"foo");
    }

    #[test]
    fn test_update_extend_both() {
        let mut block_dest = Block::new_zeroed_with_range(BLOCK_SIZE_IN_BYTES, 4, 4);
        let block_src = Block::from_slice_with_range(BLOCK_SIZE_IN_BYTES, 0, 7, BLOCK_CONTENT);

        block_dest.update(&block_src);

        assert_eq!(block_dest.start(), 0);
        assert_eq!(block_dest.end(), 7);
        assert_eq!(block_dest.as_slice(), b"foo bar");
    }
}
