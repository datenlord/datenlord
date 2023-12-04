//! FUSE protocol deserializer

use std::ffi::OsStr;
use std::{mem, slice};

use better_as::pointer;
use memchr::memchr;
use tracing::trace;

use super::abi_marker::FuseAbiData;
use super::context::ProtoVersion;

/// FUSE protocol deserializer
#[derive(Debug)]
pub struct Deserializer<'b> {
    /// inner bytes
    bytes: &'b [u8],
}

/// Types which can be decoded from bytes
#[allow(single_use_lifetimes)]
pub trait Deserialize<'b>: Sized {
    /// Deserialize from bytes
    fn deserialize(
        de: &'_ mut Deserializer<'b>,
        proto_version: ProtoVersion,
    ) -> Result<Self, DeserializeError>;
}

/// The error returned by `Deserializer`
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum DeserializeError {
    /// Expected more data
    #[error("NotEnough")]
    NotEnough,

    /// The pointer's alignment mismatched with the type
    #[error("AlignMismatch")]
    AlignMismatch,

    /// Data is more then expected
    #[allow(dead_code)]
    #[error("TooMuchData")]
    TooMuchData,

    /// Number overflow during decoding
    #[error("NumOverflow")]
    NumOverflow,

    /// The value of the target type is invalid
    #[allow(dead_code)]
    #[error("InvalidValue")]
    InvalidValue,

    /// An unknown opcode of FUSE request found
    #[error("Unknown OpCode={code}")]
    UnknownOpCode {
        /// The opcode
        code: u32,
        /// The id of request
        unique: Option<u64>,
    },
}

/// checks pointer alignment, returns `AlignMismatch` if failed
#[inline]
fn check_align<T>(ptr: *const u8) -> Result<(), DeserializeError> {
    let addr = pointer::to_address(ptr);
    let align = mem::align_of::<T>();
    if addr.wrapping_rem(align) != 0 {
        trace!(
            "failed to convert bytes to type {}, \
                    pointer={:p} is not a multiple of alignment={}",
            std::any::type_name::<T>(),
            ptr,
            align,
        );
        return Err(DeserializeError::AlignMismatch);
    }
    Ok(())
}

/// checks whether there are enough bytes
#[inline]
fn check_size(len: usize, need: usize) -> Result<(), DeserializeError> {
    if len < need {
        trace!(
            "no enough bytes to fetch, remaining {} bytes but to fetch {} bytes",
            len,
            need,
        );
        return Err(DeserializeError::NotEnough);
    }
    Ok(())
}

impl<'b> Deserializer<'b> {
    /// Create `Deserializer`
    pub const fn new(bytes: &'b [u8]) -> Deserializer<'b> {
        Self { bytes }
    }

    /// pop some bytes without length check
    unsafe fn pop_bytes_unchecked(&mut self, len: usize) -> &'b [u8] {
        let bytes = self.bytes.get_unchecked(..len);
        self.bytes = self.bytes.get_unchecked(len..);
        bytes
    }

    /// Get the length of the remaining bytes
    pub const fn remaining_len(&self) -> usize {
        self.bytes.len()
    }

    /// Fetch all remaining bytes
    pub fn fetch_all_bytes(&mut self) -> &'b [u8] {
        unsafe {
            let bytes = self.bytes;
            self.bytes = slice::from_raw_parts(self.bytes.as_ptr(), 0);
            bytes
        }
    }

    /// Fetch specified amount of bytes
    #[allow(dead_code)]
    pub fn fetch_bytes(&mut self, amt: usize) -> Result<&'b [u8], DeserializeError> {
        check_size(self.bytes.len(), amt)?;
        unsafe { Ok(self.pop_bytes_unchecked(amt)) }
    }

    /// Fetch some bytes and transmute to `&T`
    pub fn fetch_ref<T: FuseAbiData + Sized>(&mut self) -> Result<&'b T, DeserializeError> {
        let ty_size: usize = mem::size_of::<T>();
        let ty_align: usize = mem::align_of::<T>();
        debug_assert!(ty_size > 0 && ty_size.wrapping_rem(ty_align) == 0);

        check_size(self.bytes.len(), ty_size)?;
        check_align::<T>(self.bytes.as_ptr())?;

        unsafe {
            let bytes = self.pop_bytes_unchecked(ty_size);
            Ok(&*(bytes.as_ptr().cast()))
        }
    }

    /// Fetch remaining bytes and transmute to a slice of target instances
    #[cfg(feature = "abi-7-16")]
    pub fn fetch_all_as_slice<T: FuseAbiData + Sized>(
        &mut self,
    ) -> Result<&'b [T], DeserializeError> {
        let ty_size: usize = mem::size_of::<T>();
        let ty_align: usize = mem::align_of::<T>();
        debug_assert!(ty_size > 0 && ty_size.wrapping_rem(ty_align) == 0);

        if self.bytes.len() < ty_size || self.bytes.len().wrapping_rem(ty_size) != 0 {
            trace!(
                "no enough bytes to fetch, remaining {} bytes but to fetch (n * {}) bytes",
                self.bytes.len(),
                ty_size,
            );
            return Err(DeserializeError::NotEnough);
        }

        check_align::<T>(self.bytes.as_ptr())?;

        let bytes = self.fetch_all_bytes();
        unsafe {
            let base: *const T = bytes.as_ptr().cast();
            let len = bytes.len().wrapping_div(ty_size);
            Ok(slice::from_raw_parts(base, len))
        }
    }

    /// Fetch some nul-terminated bytes.
    ///
    /// [`std::ffi::CStr::to_bytes`](https://doc.rust-lang.org/stable/std/ffi/struct.CStr.html#method.to_bytes)
    /// will take O(n) time in the future.
    ///
    /// `slice::len` is always O(1)
    pub fn fetch_c_str(&mut self) -> Result<&'b [u8], DeserializeError> {
        let strlen = memchr(0, self.bytes)
            .ok_or_else(|| {
                trace!("no trailing zero in bytes, cannot fetch c-string");
                DeserializeError::NotEnough
            })?
            .wrapping_add(1);
        debug_assert!(strlen <= self.bytes.len());
        unsafe { Ok(self.pop_bytes_unchecked(strlen)) }
    }

    #[allow(dead_code)]
    /// Fetch some nul-terminated bytes and return an `OsStr` without the nul
    /// byte.
    pub fn fetch_os_str(&mut self) -> Result<&'b OsStr, DeserializeError> {
        use std::os::unix::ffi::OsStrExt;

        let bytes_with_nul = self.fetch_c_str()?;

        let bytes_without_nul: &[u8] = unsafe {
            let len = bytes_with_nul.len().wrapping_sub(1);
            bytes_with_nul.get_unchecked(..len)
        };

        Ok(OsStrExt::from_bytes(bytes_without_nul))
    }

    /// Fetch some nul-terminated bytes and return an `str` without the nul
    /// byte.
    pub fn fetch_str(&mut self) -> Result<&'b str, DeserializeError> {
        let bytes_with_nul = self.fetch_c_str()?;

        let bytes_without_nul: &[u8] = unsafe {
            let len = bytes_with_nul.len().wrapping_sub(1);
            bytes_with_nul.get_unchecked(..len)
        };

        Ok(std::str::from_utf8(bytes_without_nul)
            .unwrap_or_else(|e| panic!("failed to convert to utf8 string, error is {e:?}")))
    }

    /// Returns `TooMuchData` if the bytes is not completely consumed
    #[allow(dead_code)]
    pub fn all_consuming<T, F>(&mut self, f: F) -> Result<T, DeserializeError>
    where
        F: FnOnce(&mut Self) -> Result<T, DeserializeError>,
    {
        let ret = f(self)?;
        if !self.bytes.is_empty() {
            return Err(DeserializeError::TooMuchData);
        }
        Ok(ret)
    }
}

#[cfg(test)]
#[allow(clippy::host_endian_bytes)] // For test only
mod tests {
    use aligned_utils::stack::Align8;

    use super::Deserializer;

    #[test]
    fn fetch_all_bytes() {
        let buf: [u8; 8] = [0; 8];
        let mut de = Deserializer::new(&buf);
        assert_eq!(de.fetch_all_bytes(), &[0; 8]);
        assert_eq!(de.bytes.len(), 0);
    }

    #[test]
    fn fetch_bytes() {
        let buf: [u8; 8] = [0; 8];

        let mut de = Deserializer::new(&buf);
        assert_eq!(
            de.fetch_bytes(5)
                .unwrap_or_else(|err| panic!("failed to fetch 5 bytes, the error is: {err}",)),
            &[0; 5]
        );
        assert_eq!(de.bytes.len(), 3);

        #[allow(clippy::unwrap_used)]
        de.fetch_bytes(5).unwrap_err();
        assert_eq!(de.bytes.len(), 3);
    }

    #[test]
    fn fetch_ref() {
        // this buffer contains two `u32` or one `u64`
        // so it is aligned to 8 bytes
        let buf: Align8<[u8; 8]> = Align8([0, 1, 2, 3, 4, 5, 6, 7]);

        {
            let mut de = Deserializer::new(&*buf);
            assert_eq!(
                de.fetch_ref::<u32>()
                    .unwrap_or_else(|err| panic!("failed to fetch u32, the error is: {err}")),
                &u32::from_ne_bytes([0, 1, 2, 3])
            );
            assert_eq!(de.bytes.len(), 4);
        }

        {
            let mut de = Deserializer::new(&*buf);
            assert_eq!(
                de.fetch_ref::<u64>()
                    .unwrap_or_else(|err| panic!("failed to fetch u64, the error is: {err}")),
                &u64::from_ne_bytes([0, 1, 2, 3, 4, 5, 6, 7])
            );
            assert_eq!(de.bytes.len(), 0);
        }
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    #[cfg(feature = "abi-7-16")]
    fn fetch_all_as_slice() {
        // this buffer contains two `u32`
        // so it can be aligned to 4 bytes
        // it is aligned to 8 bytes here
        let buf: Align8<[u8; 8]> = Align8([0, 1, 2, 3, 4, 5, 6, 7]);

        {
            let mut de = Deserializer::new(&*buf);
            assert_eq!(
                de.fetch_all_as_slice::<u32>().unwrap_or_else(|err| panic!(
                    "failed to fetch all data and build slice of u32, the error is: {err}",
                )),
                &[
                    u32::from_ne_bytes([0, 1, 2, 3]),
                    u32::from_ne_bytes([4, 5, 6, 7]),
                ]
            );
            assert_eq!(de.bytes.len(), 0);
        }

        {
            let mut de = Deserializer::new(&*buf);
            de.fetch_bytes(3).unwrap();
            assert_eq!(
                de.fetch_all_as_slice::<u32>().unwrap_err(),
                super::DeserializeError::NotEnough
            );
            assert_eq!(de.bytes.len(), 5);
        }
    }

    #[test]
    fn fetch_c_str() {
        let buf: [u8; 12] = *b"hello\0world\0";

        let mut de = Deserializer::new(&buf);
        assert_eq!(
            de.fetch_c_str()
                .unwrap_or_else(|err| panic!("failed to fetch C-String, the error is: {err}")),
            b"hello\0".as_ref()
        );
        assert_eq!(
            de.fetch_c_str()
                .unwrap_or_else(|err| panic!("failed to fetch C-String, the error is: {err}")),
            b"world\0".as_ref()
        );
        assert_eq!(de.bytes.len(), 0);
    }
}
