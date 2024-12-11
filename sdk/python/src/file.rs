use std::sync::Arc;

use clippy_utilities::Cast;
use datenlord::fs::{
    datenlordfs::{DatenLordFs, S3MetaData},
    fs_util::FileAttr,
    virtualfs::VirtualFs,
};
use pyo3::{
    exceptions::PyException,
    pyclass, pymethods,
    types::{PyBytes, PyBytesMethods},
    Bound, IntoPy, PyAny, PyRef, PyResult, Python,
};
use pyo3_asyncio::tokio::future_into_py;
use tokio::sync::Mutex;
use tracing::error;

use crate::utils::Buffer;

/// A file object that implements read, write, seek, and tell.
#[pyclass]
#[derive(Debug)]
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
    /// File offset here, start from 0 and don't support atomic update
    /// TODO: Change to atomic
    offset: Arc<Mutex<u64>>,
}

impl File {
    /// Create a new file object.
    #[inline]
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
#[allow(clippy::multiple_inherent_impl)]
impl File {
    /// Read and return at most size bytes, or if size is not given, until EOF.
    /// Read with seek(offset)
    #[pyo3(signature = (size=None,))]
    #[inline]
    pub fn read<'a>(&'a mut self, _py: Python<'a>, size: Option<usize>) -> PyResult<Buffer> {
        let attr = self.attr;
        let fd = self.fd;
        let fs = Arc::clone(&self.fs);
        // This operation does not support atomic update
        let offset = Arc::clone(&self.offset);

        let size = size.unwrap_or(attr.size.cast());
        let mut buffer = vec![0; size];
        // thread nums for preheat
        // let res = future_into_py(py, async move {
        let result = pyo3_asyncio::tokio::get_runtime().handle().block_on(async {
            let mut offset_guard = offset.lock().await;
            let offset = *offset_guard;
            let start_time = tokio::time::Instant::now();

            error!("alloc buffer time: {:?}", start_time.elapsed());
            let read_size = fs
                .read(
                    attr.ino,
                    fd,
                    offset,
                    buffer.capacity().cast::<u32>(),
                    &mut buffer,
                )
                .await
                .map_err(|e| PyException::new_err(format!("read failed: {e:?}")))?;

            if read_size == 0 {
                // return Ok(Python::with_gil(|py| py.None()));
                return Err(PyException::new_err("EOF"));
            }

            // Update current data offset
            *offset_guard += read_size.cast::<u64>();

            // Print current timestamp
            error!("read time: {:?}", start_time.elapsed());

            // let bf = buffer.freeze().to_vec();
            // Python::with_gil(|py| {
            //     // Buffer::new(bf).into_bytes(py)
            //     // Buffer::new(buffer).into_bytes(py)
            // let pybytes = PyBytes::new_bound_with(py, size, |data| {
            //     Ok(())
            // });
            // Buffer::new(vec![0;1]).into_bytes(py)
            //     // Ok(buffer.into_py(py))
            //     // unsafe { PyObject::from_owned_ptr_or_err(py, ffi::PyBytes_FromObject(bf.as_ptr())) }
            // })

            Ok(())
        });

        match result {
            Ok(()) => Ok(Buffer::new(buffer)),
            Err(e) => Err(e),
        }
    }

    /// Write the given bytes-like object, return the number of bytes written.
    #[inline]
    pub fn write<'a>(&'a self, py: Python<'a>, data: &Bound<PyBytes>) -> PyResult<Bound<PyAny>> {
        let attr = self.attr;
        let fd = self.fd;
        let fs = Arc::clone(&self.fs);
        let offset = Arc::clone(&self.offset);
        let data = data.as_bytes().to_owned();

        future_into_py(py, async move {
            let offset_guard = offset.lock().await;
            let offset = *offset_guard;
            // Write the file
            match fs.write(attr.ino, fd, offset.cast(), &data, 0).await {
                Ok(()) => Ok(()),
                Err(e) => Err(PyException::new_err(format!("write failed: {e:?}"))),
            }
        })
    }

    /// Seek to the given offset in the file.
    ///
    /// 0 - start of the file, offset should be positive
    /// 1 - current file position
    /// 2 - end of the file, offset can be negative
    #[pyo3(signature = (offset, whence = 0))]
    #[inline]
    pub fn seek<'a>(
        &'a mut self,
        py: Python<'a>,
        offset: i64,
        whence: u8,
    ) -> PyResult<Bound<PyAny>> {
        let attr = self.attr;
        let current_offset = Arc::clone(&self.offset);

        future_into_py(py, async move {
            let mut offset_guard = current_offset.lock().await;
            let current_offset = *offset_guard;

            // Seek the file
            let new_offset = match whence {
                0 => offset,
                1 => current_offset.cast::<i64>() + offset,
                2 => attr.size.cast::<i64>() + offset,
                _ => {
                    error!("Invalid whence: {}", whence);
                    return Err(PyException::new_err("Invalid whence"));
                }
            };

            *offset_guard = new_offset.cast();

            Ok(new_offset)
        })
    }

    /// Tell the current file position, start from 0
    #[inline]
    pub fn tell<'a>(&'a self, py: Python<'a>) -> PyResult<Bound<PyAny>> {
        let offset = Arc::clone(&self.offset);

        future_into_py(py, async move {
            let offset_guard = offset.lock().await;
            let offset_guard: u64 = *offset_guard;
            let result: Result<i64, _> = offset_guard.try_into();
            match result {
                Ok(val) => Ok(val),
                Err(_) => Err(PyException::new_err("Offset is too large")),
            }
        })
    }

    /// Close the file
    #[inline]
    pub fn close<'a>(&'a self, py: Python<'a>) -> PyResult<Bound<PyAny>> {
        let attr = self.attr;
        let fd = self.fd;
        let fs = Arc::clone(&self.fs);

        future_into_py(py, async move {
            // Close the file
            match fs.release(attr.ino, fd, 0, 0, true).await {
                Ok(()) => Ok(()),
                Err(e) => Err(PyException::new_err(format!("close failed: {e:?}"))),
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
