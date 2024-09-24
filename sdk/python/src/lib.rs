//! DatenLord Python SDK

use pyo3::{pymodule, types::PyModule, Bound, PyResult, Python};
use sdk::DatenLordSDK;
use file::File;

/// DatenLord Python SDK file
pub mod file;
/// DatenLord Python SDK
pub mod sdk;
/// DatenLord Python SDK utils
pub mod utils;

#[pymodule]
fn datenlordsdk(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<DatenLordSDK>()?;
    m.add_class::<File>()?;
    Ok(())
}
