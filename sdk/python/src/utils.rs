use std::ffi::c_char;
use std::os::raw::c_int;
use std::{collections::VecDeque, path::Path, sync::Arc, time::Duration};

use clippy_utilities::Cast;
use datenlord::fs::fs_util::INum;
use datenlord::{
    common::error::{DatenLordError, DatenLordResult},
    fs::{
        datenlordfs::{direntry::FileType, DatenLordFs, S3MetaData},
        fs_util::{FileAttr, ROOT_ID},
        virtualfs::VirtualFs,
    },
};
use nix::fcntl::OFlag;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::pymethods;

/// A bytes-like object that implements buffer protocol.
/// Reference to opendal lib.
#[pyclass]
#[derive(Debug)]
pub struct Buffer {
    /// The inner buffer.
    inner: Vec<u8>,
}

impl Buffer {
    /// Create a new buffer.
    #[inline]
    pub fn new(inner: Vec<u8>) -> Self {
        Buffer { inner }
    }

    /// Consume self to build a bytes
    #[inline]
    #[allow(clippy::as_conversions)]
    pub fn into_bytes(&self, py: Python) -> PyResult<Py<PyAny>> {
        // let buffer = self.into_py(py);
        let ptr = self.inner.as_ptr().cast::<c_char>();
        let len: ffi::Py_ssize_t = self.inner.len().try_into()?;

        unsafe { PyObject::from_owned_ptr_or_err(py, ffi::PyBytes_FromStringAndSize(ptr, len)) }
    }
}

/// A directory entry type.
#[pyclass]
#[derive(Debug)]
pub struct Entry {
    /// The name of the entry.
    pub name: String,
    /// The inode number of the entry.
    pub ino: INum,
    /// The type of the entry.
    pub file_type: FileType,
}

#[pymethods]
impl Entry {
    /// Get the name of the entry.
    #[getter]
    #[inline]
    pub fn name(&self) -> String {
        self.name.clone()
    }

    /// Get the inode number of the entry.
    #[getter]
    #[inline]
    pub fn ino(&self) -> INum {
        self.ino
    }

    /// Get the type of the entry.
    #[getter]
    #[inline]
    pub fn file_type(&self) -> String {
        match self.file_type {
            FileType::File => "file".to_owned(),
            FileType::Dir => "dir".to_owned(),
            FileType::Symlink => "symlink".to_owned(),
        }
    }

    /// Get the string representation of the entry.
    fn __str__(&self) -> String {
        let file_type = match self.file_type {
            FileType::File => "file".to_owned(),
            FileType::Dir => "dir".to_owned(),
            FileType::Symlink => "symlink".to_owned(),
        };
        format!(
            "Entry {{ name: {}, ino: {}, file_type: {} }}",
            self.name, self.ino, file_type
        )
    }

    /// Get the string representation of the entry.
    fn __repr__(&self) -> String {
        let file_type = match self.file_type {
            FileType::File => "file".to_owned(),
            FileType::Dir => "dir".to_owned(),
            FileType::Symlink => "symlink".to_owned(),
        };
        format!(
            "Entry {{ name: {}, ino: {}, file_type: {} }}",
            self.name, self.ino, file_type
        )
    }
}

/// Find the parent inode and attribute of the given path.
#[inline]
pub async fn find_parent_attr(
    path: &str,
    fs: Arc<DatenLordFs<S3MetaData>>,
) -> DatenLordResult<(Duration, FileAttr)> {
    let path = Path::new(path);

    // If the path is root, return root inode
    if path.parent().is_none() {
        return fs.getattr(ROOT_ID).await;
    }

    // Delete the last component to find the parent inode
    let parent_path = path.parent().ok_or(DatenLordError::ArgumentInvalid {
        context: vec!["Cannot find parent path".to_owned()],
    })?;
    let parent_path_components = parent_path.components();

    // Find the file from parent inode
    let mut current_inode = ROOT_ID;
    for component in parent_path_components {
        if let Some(name) = component.as_os_str().to_str() {
            match fs.lookup(0, 0, current_inode, name).await {
                Ok((_duration, attr, _generation)) => {
                    current_inode = attr.ino;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        } else {
            return Err(DatenLordError::ArgumentInvalid {
                context: vec!["Invalid path component".to_owned()],
            });
        }
    }
    fs.getattr(current_inode).await
}

/// The current implementation searches for items and places them into a queue.
/// It continues doing so until the subdirectory is found to be empty, at which point it deletes it.
/// This method introduces some overhead due to repeated searches.
/// An optimization could be applied to reduce the query overhead.
#[inline]
pub async fn recursive_delete_dir(
    fs: Arc<DatenLordFs<S3MetaData>>,
    dir_path: &str,
    recursive: bool,
) -> DatenLordResult<()> {
    let mut dir_stack = VecDeque::new();
    dir_stack.push_back(dir_path.to_owned());

    while let Some(current_dir_path) = dir_stack.pop_front() {
        let (_, parent_attr) = find_parent_attr(&current_dir_path, Arc::clone(&fs)).await?;
        let path = Path::new(&current_dir_path);

        let current_name = path
            .file_name()
            .ok_or(DatenLordError::ArgumentInvalid {
                context: vec!["Invalid file path".to_owned()],
            })?
            .to_str()
            .ok_or(DatenLordError::ArgumentInvalid {
                context: vec!["Invalid file path".to_owned()],
            })?;

        let (_, dir_attr, _) = fs.lookup(0, 0, parent_attr.ino, current_name).await?;
        let current_dir_ino = dir_attr.ino;

        // Open directory
        let dir_handle = fs
            .opendir(0, 0, current_dir_ino, OFlag::O_RDONLY.bits().cast())
            .await?;

        // Read directory entries
        let entries = match fs.readdir(0, 0, current_dir_ino, dir_handle, 0).await {
            Ok(e) => e,
            Err(e) => {
                // Release the directory handle before returning the error
                fs.releasedir(current_dir_ino, dir_handle, 0).await?;
                return Err(e);
            }
        };

        for entry in &entries {
            let entry_path = Path::new(&current_dir_path).join(entry.name());

            if entry.file_type() == FileType::Dir {
                if recursive {
                    dir_stack.push_front(entry_path.to_string_lossy().to_string());
                }
            } else {
                fs.unlink(0, 0, current_dir_ino, entry.name()).await?;
            }
        }

        // Always release the directory handle
        fs.releasedir(current_dir_ino, dir_handle, 0).await?;

        if recursive || entries.is_empty() {
            fs.rmdir(0, 0, parent_attr.ino, current_name).await?;
        }

        if !recursive {
            return Ok(());
        }
    }

    Ok(())
}

#[pymethods]
#[allow(clippy::multiple_inherent_impl)]
impl Buffer {
    /// Fill the view with the buffer data.
    #[allow(clippy::as_conversions)]
    #[allow(clippy::needless_pass_by_value)]
    unsafe fn __getbuffer__(
        slf: PyRefMut<Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let bytes = slf.inner.as_slice();
        let ret = unsafe {
            ffi::PyBuffer_FillInfo(
                view,
                slf.as_ptr(),
                bytes.as_ptr() as *mut _,
                bytes.len().try_into()?,
                1, // read only
                flags,
            )
        };
        if ret == -1_i32 {
            return Err(PyErr::fetch(slf.py()));
        }
        Ok(())
    }

    /// Release the buffer.
    #[allow(clippy::unused_self)]
    unsafe fn __releasebuffer__(&mut self, view: *mut ffi::Py_buffer) {
        unsafe {
            ffi::PyBuffer_Release(view);
        }
    }

    /// Get the length of the buffer.
    fn get_len(&self) -> usize {
        self.inner.len()
    }
}
