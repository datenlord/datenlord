use super::error::Context;
use super::error::DatenLordResult;
use serde::de::DeserializeOwned;
extern crate alloc;
use alloc::string::ToString;
use std::fmt::Write;

/// Decode from bytes
#[inline]
pub fn decode_from_bytes<T: DeserializeOwned>(bytes: &[u8]) -> DatenLordResult<T> {
    let decoded_value = bincode::deserialize(bytes)
        .with_context(|| format!("failed to decode bytes to {}", core::any::type_name::<T>(),))?;
    Ok(decoded_value)
}

/// Format `anyhow::Error`
#[must_use]
#[inline]
pub fn format_anyhow_error(error: &anyhow::Error) -> String {
    let err_msg_vec = anyhow::Error::chain(error)
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let mut err_msg = String::new();
    let _ignore = write!(
        err_msg,
        "{}, root cause: {}",
        err_msg_vec.as_slice().join(", caused by: "),
        error.root_cause()
    );

    err_msg
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn setup_test_log_debug() {
    use std::env::set_var;
    set_var("RUST_LOG", "debug");
    let _log_init_res = env_logger::try_init();
}
