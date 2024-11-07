//! DatenLord KVCache Python SDK

use file::File;
use pyo3::{pymodule, types::PyModule, Bound, PyResult, Python};
use sdk::DatenLordSDK;
use utils::{Buffer, Entry};

/// DatenLord KVCache Python SDK file
pub mod file;
/// DatenLord KVCache Python SDK
pub mod sdk;
/// DatenLord KVCache Python SDK utils
pub mod utils;

#[pymodule]
fn datenlordsdk(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<DatenLordSDK>()?;
    m.add_class::<File>()?;
    m.add_class::<Buffer>()?;
    m.add_class::<Entry>()?;

    Ok(())
}