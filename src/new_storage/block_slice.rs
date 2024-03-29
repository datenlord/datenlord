//! The definition of the `BlockSlice`.

use clippy_utilities::OverflowArithmetic;
use smallvec::SmallVec;

/// Represents a slice of a block.
///
/// A `BlockSlice` contains the block ID, offset within the block, and size of
/// the slice.
#[derive(Debug, Clone, Copy)]
pub struct BlockSlice {
    /// The block ID.
    pub block_id: u64,
    /// The offset within the block.
    pub offset: u64,
    /// The size of the slice.
    pub size: u64,
}

impl BlockSlice {
    /// Creates a new `BlockSlice` with the given block ID, offset, and size.
    #[must_use]
    #[inline]
    pub fn new(block_id: u64, offset: u64, size: u64) -> Self {
        BlockSlice {
            block_id,
            offset,
            size,
        }
    }
}

/// Converts an offset and length into a sequence of `BlockSlice`s.
///
/// Given a block size, offset, and length, this function calculates the
/// corresponding `BlockSlice`s that cover the specified range. The slices are
/// returned as a `SmallVec` with a maximum inline capacity of 2.
///
/// # Arguments
///
/// * `block_size` - The size of each block.
/// * `offset` - The starting offset of the range.
/// * `len` - The length of the range.
///
/// # Returns
///
/// A `SmallVec` containing the `BlockSlice`s that cover the specified range.
#[must_use]
pub fn offset_to_slice(block_size: u64, offset: u64, len: u64) -> SmallVec<[BlockSlice; 2]> {
    let mut slices = SmallVec::new();
    let mut current_offset = offset;
    let mut remaining_len = len;

    while remaining_len > 0 {
        let current_block = current_offset.overflow_div(block_size);
        let block_internal_offset = current_offset.overflow_rem(block_size);
        let space_in_block = block_size.overflow_sub(block_internal_offset);
        let size_to_read = remaining_len.min(space_in_block);

        slices.push(BlockSlice {
            block_id: current_block,
            offset: block_internal_offset,
            size: size_to_read,
        });

        remaining_len -= size_to_read;
        current_offset += size_to_read;
    }

    slices
}

#[cfg(test)]
#[allow(clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_to_slice_single_block() {
        let slices = offset_to_slice(4, 2, 2);
        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0].block_id, 0);
        assert_eq!(slices[0].offset, 2);
        assert_eq!(slices[0].size, 2);
    }

    #[test]
    fn test_offset_to_slice_cross_blocks() {
        let slices = offset_to_slice(4, 3, 6);
        assert_eq!(slices.len(), 3); // Expecting to cross 3 blocks
        assert_eq!(slices[0].block_id, 0);
        assert_eq!(slices[0].offset, 3);
        assert_eq!(slices[0].size, 1);

        assert_eq!(slices[1].block_id, 1);
        assert_eq!(slices[1].offset, 0);
        assert_eq!(slices[1].size, 4);

        assert_eq!(slices[2].block_id, 2);
        assert_eq!(slices[2].offset, 0);
        assert_eq!(slices[2].size, 1);
    }
}
