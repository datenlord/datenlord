//! Some utilities functions.

/// Formats a block path string given the block ID and inode number.
///
/// The block path is formatted as `{inode}/{block_id}`.
#[must_use]
#[inline]
pub fn format_path(ino: u64, block_id: u64) -> String {
    format!("{ino}/{block_id}")
}

/// Get block from given path
#[must_use]
#[inline]
pub fn get_block_from_path(path: &str) -> (u64, u64) {
    let mut iter = path.split('/').map(|x| x.parse::<u64>().unwrap());
    (iter.next().unwrap(), iter.next().unwrap())
}

/// Formats a file path in the backend, which equals to `{inode}/`
#[must_use]
#[inline]
pub fn format_file_path(ino: u64) -> String {
    format!("{ino}/")
}
