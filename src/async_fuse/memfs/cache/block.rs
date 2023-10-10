/// Block size is 4KB
pub const BLOCK_SIZE: usize = 4 * 1024;

/// Block is the basic unit of data in the cache.
pub struct Block {
    /// Block data
    data: Vec<u8>,
}

impl Block {
    /// Returns the data of the block.
    pub fn get_data(&self) -> &[u8] {
        &self.data
    }
}

/// Impl Block from Vec<u8>
impl From<Vec<u8>> for Block {
    fn from(data: Vec<u8>) -> Self {
        Self { data }
    }
}
