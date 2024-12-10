//! The definition of the `Block`.

use std::fmt::Debug;

use clippy_utilities::OverflowArithmetic;

/// The size of a block in bytes (for test).
#[cfg(test)]
pub const BLOCK_SIZE: usize = 4 * 1024 * 1024;

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

/// Represents a block of data.
///
/// A `Block` contains a vector of bytes representing the block data,
/// along with metadata such as pin count, dirty flag, and version.
pub struct Block {
    /// The underlying data
    data: Vec<u8>,
    /// The pin count
    pin_count: u32,
    /// The dirty flag
    dirty: bool,
    /// The version of the block
    version: usize,
}

impl Debug for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Block")
            .field(
                "data",
                &make_partial_u8_slice(&self.data, 0, self.data.len()),
            )
            .field("pin_count", &self.pin_count)
            .field("dirty", &self.dirty)
            .field("version", &self.version)
            .finish()
    }
}

impl Block {
    /// Creates a new `Block`.
    #[must_use]
    pub fn zeroed(size: usize) -> Self {
        Block {
            data: vec![0; size],
            pin_count: 0,
            dirty: false,
            version: 0,
        }
    }

    /// Returns the current version of the block.
    #[must_use]
    pub fn version(&self) -> usize {
        self.version
    }

    /// Increments the version of the block.
    pub fn inc_version(&mut self) {
        self.version += 1;
    }

    /// Returns the current pin count of the block.
    #[must_use]
    pub fn pin_count(&self) -> u32 {
        self.pin_count
    }

    /// Returns whether the block is marked as dirty.
    #[must_use]
    pub fn dirty(&self) -> bool {
        self.dirty
    }

    /// Sets the dirty flag of the block.
    pub fn set_dirty(&mut self, dirty: bool) {
        self.dirty = dirty;
    }

    /// Increments the pin count of the block.
    pub fn pin(&mut self) {
        self.pin_count += 1;
    }

    /// Decrements the pin count of the block.
    ///
    /// # Panics
    ///
    /// Panics if the pin count is already zero.
    #[inline]
    pub fn unpin(&mut self) {
        assert!(self.pin_count > 0);
        self.pin_count -= 1;
    }

    /// Clears the block data and resets metadata.
    ///
    /// This method marks the block as not dirty, resets the pin
    /// count to zero, and resets the version to zero.
    pub fn clear(&mut self) {
        self.dirty = false;
        self.pin_count = 0;
        self.version = 0;
    }
}

impl std::ops::Deref for Block {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl std::ops::DerefMut for Block {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}
