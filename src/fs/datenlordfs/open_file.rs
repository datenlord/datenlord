use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::fs::fs_util::{FileAttr, INum};

/// A structure representing an open file with its attributes and open count.
///
/// The `attr` field contains the file attributes, while `open_cnt` keeps track
/// of the number of times this file is currently opened.
#[derive(Debug)]
#[allow(clippy::partial_pub_fields)] // Pub attr for simplicity.
pub struct RawOpenFile {
    /// The file attributes.
    pub attr: FileAttr,
    /// The number of times this file is currently opened.
    open_cnt: u32,
}

/// A thread-safe reference counted wrapper around `RawOpenFile`.
///
/// This type uses `Arc` for shared ownership and `RwLock` for thread-safe
/// mutability, allowing multiple readers or one writer at the same time.
pub type OpenFile = Arc<RwLock<RawOpenFile>>;

/// Internal structure to keep track of all open files.
///
/// This structure uses a `HashMap` to associate inode numbers (`INum`) with
/// their corresponding `OpenFile` instances.
#[derive(Debug)]
struct RawOpenFiles {
    /// The collection of open files.
    open_files: HashMap<INum, OpenFile>,
}

/// A thread-safe, mutable collection of open files.
///
/// This structure provides an interface to interact with open files,
/// encapsulating the internal locking mechanism through `Mutex`.
#[derive(Debug)]
pub struct OpenFiles {
    /// The internal collection of open files.
    inner: Arc<Mutex<RawOpenFiles>>,
}

impl OpenFiles {
    /// Constructs a new `OpenFiles` collection.
    ///
    /// This initializes an empty collection of open files.
    pub fn new() -> Self {
        OpenFiles {
            inner: Arc::new(Mutex::new(RawOpenFiles {
                open_files: HashMap::new(),
            })),
        }
    }

    /// Try to retrieves an `OpenFile` by its inode number.
    ///
    /// Returns `Some(OpenFile)` if the file is open, or `None` if not found.
    pub fn try_get(&self, inum: INum) -> Option<OpenFile> {
        let inner = self.inner.lock();
        let open_file = inner.open_files.get(&inum).map(Arc::clone);
        open_file
    }

    /// Retrieves an `OpenFile` by its inode number.
    ///
    /// Panics if the file is not found.
    pub fn get(&self, inum: INum) -> OpenFile {
        self.try_get(inum)
            .unwrap_or_else(|| panic!("Couldn't find the open file with inum={inum}"))
    }

    /// Opens a file, adding it to the collection
    ///
    /// Returns a reference to the newly opened file.
    pub fn open(&self, inum: INum, attr: FileAttr) -> OpenFile {
        let mut inner = self.inner.lock();
        let open_file = inner
            .open_files
            .entry(inum)
            .or_insert_with(|| Arc::new(RwLock::new(RawOpenFile { attr, open_cnt: 0 })));
        {
            let mut open_file = open_file.write();
            open_file.open_cnt += 1;
        }
        Arc::clone(open_file)
    }

    /// Tries to open a file if it is already in the collection.
    ///
    /// Increments the open count if the file is found. Returns `Some(OpenFile)`
    /// if successful, or `None` if the file is not in the collection.
    pub fn try_open(&self, inum: INum) -> Option<OpenFile> {
        let mut inner = self.inner.lock();
        let open_file = inner.open_files.get_mut(&inum)?;
        {
            let mut open_file = open_file.write();
            open_file.open_cnt += 1;
        }
        Some(Arc::clone(open_file))
    }

    /// Closes an open file, identified by its inode number.
    ///
    /// Decrements the open count and removes the file from the collection
    /// if its open count reaches zero. Returns the `OpenFile` if it was
    /// removed, or `None` if it remains open.
    #[allow(clippy::unwrap_in_result)]
    pub fn close(&self, inum: INum) -> Option<OpenFile> {
        let mut inner = self.inner.lock();
        let open_count = {
            let open_file = inner
                .open_files
                .get_mut(&inum)
                .unwrap_or_else(|| panic!("Couldn't find the open file with inum={inum}"));
            let mut open_file = open_file.write();
            debug_assert!(open_file.open_cnt > 0);
            open_file.open_cnt -= 1;
            open_file.open_cnt
        };
        if open_count == 0 {
            inner.open_files.remove(&inum)
        } else {
            None
        }
    }
}
