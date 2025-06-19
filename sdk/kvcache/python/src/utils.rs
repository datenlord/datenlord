use std::ffi::c_char;
use std::os::raw::c_int;

use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::pymethods;

/// A bytes-like object that implements buffer protocol.
/// Reference to opendal lib.
#[pyclass]
pub struct Buffer {
    inner: bytes::Bytes,
}

impl Buffer {
    pub fn new(inner: bytes::Bytes) -> Self {
        Buffer { inner }
    }

    /// Consume self to build a bytes
    pub fn into_bytes(&self, py: Python) -> PyResult<Py<PyAny>> {
        // let buffer = self.into_py(py);
        let ptr = self.inner.as_ptr() as *const c_char;
        let len = self.inner.len() as ffi::Py_ssize_t;

        unsafe { PyObject::from_owned_ptr_or_err(py, ffi::PyBytes_FromStringAndSize(ptr, len)) }
    }
}

#[pymethods]
impl Buffer {
    unsafe fn __getbuffer__(
        slf: PyRef<Self>,
        // slf: PyRefMut<Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        // let bytes = slf.inner.as_slice();
        let bytes = slf.inner.as_ref();
        let ret = ffi::PyBuffer_FillInfo(
            view,
            slf.as_ptr() as *mut _,
            bytes.as_ptr() as *mut _,
            bytes.len().try_into().unwrap(),
            1, // read only
            flags,
        );
        if ret == -1 {
            return Err(PyErr::fetch(slf.py()));
        }
        Ok(())
    }

    unsafe fn __releasebuffer__(&mut self, view: *mut ffi::Py_buffer) {
        // unsafe {
            // ffi::PyBuffer_Release(view);
        // }
    }

    fn get_len(&self) -> usize {
        self.inner.len()
    }
}