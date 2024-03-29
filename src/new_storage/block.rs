//! The definition of the `Block`.

/// The size of a block in bytes.
pub const BLOCK_SIZE: usize = 4 * 1024 * 1024;

/// Represents a block of data.
///
/// A `Block` contains a vector of bytes representing the block data,
/// along with metadata such as pin count, dirty flag, and version.
#[derive(Debug)]
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

impl Block {
    /// Creates a new `Block` with the given data.
    ///
    /// The length of the provided data must be equal to `BLOCK_SIZE`.
    #[must_use]
    pub fn new(data: Vec<u8>) -> Self {
        debug_assert!(data.len() == BLOCK_SIZE);
        Block {
            data,
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
