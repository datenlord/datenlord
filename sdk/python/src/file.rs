use std::sync::Arc;

use bytes::BytesMut;
use datenlord::fs::{
    datenlordfs::{DatenLordFs, S3MetaData},
    fs_util::FileAttr,
    virtualfs::VirtualFs,
};
use pyo3::{
    exceptions::PyException, pyclass, pymethods, types::{PyBytes, PyBytesMethods}, Bound, IntoPy, PyAny, PyRef, PyResult, Python
};
use pyo3_asyncio::tokio::future_into_py;
use tokio::sync::Mutex;
use tracing::error;

use crate::utils::Buffer;

/// A file object that implements read, write, seek, and tell.
#[pyclass]
pub struct File {
    /// Current file ino, we use file descriptor to represent the file
    attr: FileAttr,
    /// File descriptor, pyo3 only support u32 now, we need to cast u64 to u32
    fd: u64,
    /// File flags
    #[allow(unused)]
    flags: u32,
    /// File system
    fs: Arc<DatenLordFs<S3MetaData>>,
    // File offset here, start from 0 and don't support atomic update
    // TODO: Change to atomic
    offset: Arc<Mutex<u64>>,
}

impl File {
    pub fn new(attr: FileAttr, fd: u64, flags: u32, fs: Arc<DatenLordFs<S3MetaData>>) -> Self {
        Self {
            attr,
            fd,
            flags,
            fs,
            offset: Arc::new(Mutex::new(0)),
        }
    }
}

#[pymethods]
impl File {
    /// Read and return at most size bytes, or if size is not given, until EOF.
    /// Read with seek(offset)
    #[pyo3(signature = (size=None,))]
    pub fn read<'a>(&'a mut self, py: Python<'a>, size: Option<usize>) -> PyResult<Bound<PyAny>> {
        let attr = self.attr.clone();
        let fd = self.fd;
        let fs = Arc::clone(&self.fs);
        // This operation does not support atomic update
        let offset = Arc::clone(&self.offset);

        // thread nums for preheat
        let res = future_into_py(py, async move {
            let size = size.unwrap_or(attr.size as usize);
            let mut offset_guard = offset.lock().await;
            let offset = *offset_guard;
            // Read the file
            let mut buffer = BytesMut::with_capacity(size);
            let read_size = fs
                .read(attr.ino, fd, offset, buffer.capacity() as u32, &mut buffer)
                .await
                .map_err(|e| PyException::new_err(format!("read failed: {:?}", e)))?;

            if read_size == 0 {
                return Ok(Python::with_gil(|py| py.None()));
            }

            // Update current data offset
            *offset_guard += read_size as u64;

            let bf = buffer.freeze().to_vec();
            Python::with_gil(|py| Buffer::new(bf).into_bytes(py))
        });

        return res;
    }

    /// Write the given bytes-like object, return the number of bytes written.
    pub fn write<'a>(&'a self, py: Python<'a>, data: &Bound<PyBytes>) -> PyResult<Bound<PyAny>> {
        let attr = self.attr.clone();
        let fd = self.fd;
        let fs = Arc::clone(&self.fs);
        let offset = Arc::clone(&self.offset);
        let data = data.as_bytes().to_owned();

        future_into_py(py, async move {
            let offset_guard = offset.lock().await;
            let offset = *offset_guard;
            // Write the file
            match fs.write(attr.ino, fd, offset as i64, &data, 0).await {
                Ok(()) => Ok(()),
                Err(e) => Err(PyException::new_err(format!("write failed: {:?}", e))),
            }
        })
    }

    /// Seek to the given offset in the file.
    ///
    /// 0 - start of the file, offset should be positive
    /// 1 - current file position
    /// 2 - end of the file, offset can be negative
    #[pyo3(signature = (offset, whence = 0))]
    pub fn seek<'a>(&'a mut self, py: Python<'a>, offset: i64, whence: u8) -> PyResult<Bound<PyAny>> {
        let attr = self.attr.clone();
        let current_offset = Arc::clone(&self.offset);

        future_into_py(py, async move {
            let mut offset_guard = current_offset.lock().await;
            let current_offset = *offset_guard;

            // Seek the file
            let new_offset = match whence {
                0 => offset,
                1 => current_offset as i64 + offset,
                2 => attr.size as i64 + offset,
                _ => {
                    error!("Invalid whence: {}", whence);
                    return Err(PyException::new_err("Invalid whence"));
                }
            };

            *offset_guard = new_offset as u64;

            Ok(new_offset)
        })
    }

    /// Tell the current file position, start from 0
    pub fn tell<'a>(&'a self, py: Python<'a>) -> PyResult<Bound<PyAny>> {
        let offset = Arc::clone(&self.offset);

        future_into_py(py, async move {
            let offset_guard = offset.lock().await;
            Ok(*offset_guard as i64)
        })
    }

    /// Close the file
    pub fn close<'a>(&'a self, py: Python<'a>) -> PyResult<Bound<PyAny>> {
        let attr = self.attr.clone();
        let fd = self.fd;
        let fs = Arc::clone(&self.fs);

        future_into_py(py, async move {
            // Close the file
            match fs.release(attr.ino, fd, 0, 0, true).await {
                Ok(()) => Ok(()),
                Err(e) => Err(PyException::new_err(format!("close failed: {:?}", e))),
            }
        })
    }

    /// Support async context manager
    fn __aenter__<'a>(slf: PyRef<'a, Self>, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let slf = slf.into_py(py);
        future_into_py(py, async move { Ok(slf) })
    }

    /// Support async context manager
    fn __aexit__<'a>(
        &'a mut self,
        py: Python<'a>,
        _exc_type: &Bound<'a, PyAny>,
        _exc_value: &Bound<'a, PyAny>,
        _traceback: &Bound<'a, PyAny>,
    ) -> PyResult<Bound<'a, PyAny>> {
        self.close(py)
    }
}
