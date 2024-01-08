// Use of external crates and modules
use datenlord::common::error::DatenLordError;
use nix::sys::stat::SFlag;
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::async_fuse::fuse::protocol::INum;

/// Represents the type of a file in a filesystem.
///
/// This enum is used to distinguish between directories, files, and symlinks.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum FileType {
    /// A directory.
    Dir,
    /// A regular file.
    File,
    /// A symbolic link.
    Symlink,
}

impl TryFrom<SFlag> for FileType {
    type Error = DatenLordError;

    /// Attempts to convert an `SFlag` value into a `FileType`.
    ///
    /// # Arguments
    ///
    /// * `value` - The `SFlag` value representing file type at the OS level.
    ///
    /// # Returns
    ///
    /// * `Ok(FileType)` - If the conversion is successful.
    /// * `Err(DatenLordError)` - If the `SFlag` value does not correspond to a
    ///   known `FileType`.
    fn try_from(value: SFlag) -> Result<Self, Self::Error> {
        match value {
            SFlag::S_IFDIR => Ok(Self::Dir),
            SFlag::S_IFREG => Ok(Self::File),
            SFlag::S_IFLNK => Ok(Self::Symlink),
            _ => {
                error!("Try convert {:?} to FileType failed.", value);
                Err(DatenLordError::ArgumentInvalid { context: vec![] })
            }
        }
    }
}

/// Represents a directory entry in a filesystem.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DirEntry {
    /// The inode number of the child
    inum: INum,
    /// The name of the child
    name: String,
    /// The type of the child
    file_type: FileType,
}

impl DirEntry {
    /// Creates a new `DirEntry`.
    ///
    /// # Arguments
    ///
    /// * `inum` - The inode number of the file or directory.
    /// * `name` - The name of the file or directory.
    /// * `file_type` - The type of the file (directory, file, or symlink).
    ///
    /// # Returns
    ///
    /// * `DirEntry` - The new `DirEntry` instance.
    #[must_use]
    pub fn new(inum: INum, name: String, file_type: FileType) -> Self {
        Self {
            inum,
            name,
            file_type,
        }
    }

    /// Returns the inode number of the file or directory.
    #[must_use]
    pub fn inum(&self) -> INum {
        self.inum
    }

    /// Returns the name of the file or directory.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the type of the file (directory, file, or symlink).
    #[must_use]
    pub fn file_type(&self) -> FileType {
        self.file_type.clone()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
#[allow(clippy::assertions_on_result_states)]
mod tests {
    use super::*;

    #[test]
    fn test_file_type_serialization() {
        let file_type = FileType::Dir;
        let file_type_bytes = bincode::serialize(&file_type).unwrap();
        let file_type2: FileType = bincode::deserialize(&file_type_bytes).unwrap();
        assert_eq!(file_type, file_type2);
    }

    #[test]
    fn test_unsupported_file_type_conversion() {
        let unsupported_sflag = SFlag::S_IFSOCK; // Assuming S_IFSOCK is not supported
        let file_type_result = FileType::try_from(unsupported_sflag);
        assert!(file_type_result.is_err());
    }

    #[test]
    fn test_dir_entry_serialization() {
        let dir_entry = DirEntry::new(1, "test".to_owned(), FileType::Dir);
        let dir_entry_bytes = bincode::serialize(&dir_entry).unwrap();
        let dir_entry2: DirEntry = bincode::deserialize(&dir_entry_bytes).unwrap();
        assert_eq!(dir_entry, dir_entry2);
    }
}
