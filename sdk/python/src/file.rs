use std::sync::Arc;

use bytes::BytesMut;
use datenlord::fs::{
    datenlordfs::{DatenLordFs, S3MetaData},
    fs_util::FileAttr,
    virtualfs::VirtualFs,
};
use pyo3::{
    exceptions::PyException, pyclass, pymethods, Bound, IntoPy, PyAny, PyRef, PyResult, Python,
};
use pyo3_asyncio::tokio::future_into_py;

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
}

impl File {
    pub fn new(attr: FileAttr, fd: u64, flags: u32, fs: Arc<DatenLordFs<S3MetaData>>) -> Self {
        Self {
            attr,
            fd,
            flags,
            fs,
        }
    }
}

#[pymethods]
impl File {
    /// Read and return at most size bytes, or if size is not given, until EOF.
    #[pyo3(signature = (size=None,))]
    pub fn read<'a>(&'a self, py: Python<'a>, size: Option<usize>) -> PyResult<Bound<PyAny>> {
        let attr = self.attr.clone();
        let fd = self.fd;
        let fs = Arc::clone(&self.fs);

        future_into_py(py, async move {
            let size = size.unwrap_or(attr.size as usize);
            // Read the file
            let mut buffer = BytesMut::with_capacity(size);
            match fs
                .read(attr.ino, fd, 0, buffer.capacity() as u32, &mut buffer)
                .await
            {
                Ok(read_size) => {
                    // convert to [u8]
                    unsafe {
                        buffer.set_len(read_size as usize);
                    }
                    // TODO: change type to pyany
                    Ok(buffer.freeze().to_vec())
                    // Ok(buffer[..read_size])
                }
                Err(e) => Err(PyException::new_err(format!("read failed: {:?}", e))),
            }
        })
    }

    /// Write the given bytes-like object, return the number of bytes written.
    pub fn write<'a>(&'a self, py: Python<'a>, data: Vec<u8>) -> PyResult<Bound<PyAny>> {
        let attr = self.attr.clone();
        let fd = self.fd;
        let fs = Arc::clone(&self.fs);

        future_into_py(py, async move {
            // Write the file
            match fs.write(attr.ino, fd, 0, &data, 0).await {
                Ok(()) => Ok(()),
                Err(e) => Err(PyException::new_err(format!("write failed: {:?}", e))),
            }
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
