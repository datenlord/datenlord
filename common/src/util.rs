use super::error::Context;
use super::error::DatenLordResult;
use serde::de::DeserializeOwned;

/// Decode from bytes
pub fn decode_from_bytes<T: DeserializeOwned>(bytes: &[u8]) -> DatenLordResult<T> {
    let decoded_value = bincode::deserialize(bytes)
        .with_context(|| format!("failed to decode bytes to {}", std::any::type_name::<T>(),))?;
    Ok(decoded_value)
}
