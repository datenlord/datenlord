use std::hash::{Hash, Hasher};

/// The size of a block in bytes.
pub const BLOCK_SIZE: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug)]
/// Provide current node meta infos
pub struct MetaData {
    /// File attr inum
    inum: u64,
    /// Block write time, generated with snowflake
    version: i64,
    /// Block offset in file
    offset: u64,
    /// Block size
    size: u64,
}

impl Eq for MetaData {
    #[inline]
    fn assert_receiver_is_total_eq(&self) {}
}

impl PartialEq for MetaData {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.inum == other.inum
            && self.version == other.version
            && self.offset == other.offset
            && self.size == other.size
    }

    #[inline]
    #[allow(clippy::partialeq_ne_impl)]
    fn ne(&self, other: &Self) -> bool {
        !self.eq(other)
    }
}

impl Hash for MetaData {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inum.hash(state);
        self.version.hash(state);
        self.offset.hash(state);
        self.size.hash(state);
    }

    #[inline]
    fn hash_slice<H: Hasher>(data: &[Self], state: &mut H)
    where
        Self: Sized,
    {
        for item in data {
            item.hash(state);
        }
    }
}

impl Default for MetaData {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl MetaData {
    /// Create a new `MetaData` instance
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        MetaData {
            inum: 0,
            version: 0,
            offset: 0,
            size: 0,
        }
    }

    /// Get the inum of the `MetaData`
    #[inline]
    #[must_use]
    pub fn get_inum(&self) -> u64 {
        self.inum
    }

    /// Get the version of the `MetaData`
    #[inline]
    #[must_use]
    pub fn get_version(&self) -> i64 {
        self.version
    }

    /// Get the offset of the `MetaData`
    #[inline]
    #[must_use]
    pub fn get_offset(&self) -> u64 {
        self.offset
    }

    /// Get the size of the `MetaData`
    #[inline]
    #[must_use]
    pub fn get_size(&self) -> u64 {
        self.size
    }

    /// Convert the `MetaData` to a string
    #[inline]
    #[must_use]
    pub fn to_id(&self) -> String {
        format!(
            "{}_{}_{}_{}",
            self.inum, self.version, self.offset, self.size
        )
    }

    /// Create a `MetaData` from a string
    #[inline]
    #[must_use]
    pub fn from_id(id: &str) -> Option<Self> {
        let parts: Vec<&str> = id.split('_').collect();
        if parts.len() != 4 {
            return None;
        }

        Some(MetaData {
            inum: parts.first()?.parse().ok()?,
            version: parts.get(1)?.parse().ok()?,
            offset: parts.get(2)?.parse().ok()?,
            size: parts.get(3)?.parse().ok()?,
        })
    }
}

/// Block struct to hold block data and metadata
#[derive(Clone, Debug)]
pub struct Block {
    /// The metadata of the block
    meta_data: MetaData,
    /// The data of the block
    data: Vec<u8>,
}

impl Block {
    /// Create a new Block instance
    #[inline]
    #[must_use]
    pub fn new(meta_data: MetaData, data: Vec<u8>) -> Self {
        // Make sure data length is BLOCK_SIZE
        debug_assert!(data.len() == BLOCK_SIZE);

        Block { meta_data, data }
    }

    /// Get the block inner data of the Block
    #[inline]
    #[must_use]
    pub fn get_data(&self) -> Vec<u8> {
        self.data.clone()
    }

    /// Get the block meta data of the Block
    #[inline]
    #[must_use]
    pub fn get_meta_data(&self) -> MetaData {
        self.meta_data.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_id() {
        let meta_data = MetaData {
            inum: 123,
            version: 456,
            offset: 789,
            size: 10,
        };
        let expected_id = "123_456_789_10".to_owned();
        assert_eq!(meta_data.to_id(), expected_id);
    }

    #[test]
    fn test_from_id_valid() {
        let id = "123_456_789_10";
        let expected_meta_data = MetaData {
            inum: 123,
            version: 456,
            offset: 789,
            size: 10,
        };
        assert_eq!(MetaData::from_id(id), Some(expected_meta_data));
    }

    #[test]
    fn test_from_id_invalid() {
        let id = "123_456_789";
        assert_eq!(MetaData::from_id(id), None);
    }
}
