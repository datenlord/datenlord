//! Utility functions

use std::error::Error;
use std::ffi::{CStr, CString};
use std::mem::MaybeUninit;
use std::os::raw::{c_char, c_int};
use std::{io, ptr, slice};

use memchr::memchr;
use nix::errno::Errno;
use nix::sys::stat::SFlag;

use crate::common::error::{DatenLordError, DatenLordResult};

/// Format `nix::Error`
// TODO: refactor this
#[must_use]
#[inline]
pub fn format_nix_error(error: nix::Error) -> String {
    format!("{}, root cause: {:?}", error, error.source())
}

/// Build error result from `nix` error code
/// # Errors
///
/// Return the built `Err(anyhow::Error(..))`
pub fn build_error_result_from_errno<T>(error_code: Errno, err_msg: String) -> DatenLordResult<T> {
    Err(DatenLordError::from(
        anyhow::Error::new(error_code).context(err_msg),
    ))
}

/// Convert `nix::errno::Errno` to `c_int`
#[allow(clippy::as_conversions)]
#[must_use]
pub const fn convert_nix_errno_to_cint(error_no: Errno) -> c_int {
    error_no as c_int
}

/// Build file mode from `SFlag` and file permission
/// # Panics
/// Panics if `SFlag` is unknown
#[must_use]
pub fn mode_from_kind_and_perm(kind: SFlag, perm: u16) -> u32 {
    let file_type = match kind {
        SFlag::S_IFIFO => libc::S_IFIFO,
        SFlag::S_IFCHR => libc::S_IFCHR,
        SFlag::S_IFBLK => libc::S_IFBLK,
        SFlag::S_IFDIR => libc::S_IFDIR,
        SFlag::S_IFREG => libc::S_IFREG,
        SFlag::S_IFLNK => libc::S_IFLNK,
        SFlag::S_IFSOCK => libc::S_IFSOCK,
        _ => panic!("unknown SFlag type={kind:?}"),
    };
    let file_perm: u32 = perm.into();

    #[cfg(target_os = "linux")]
    {
        file_type | file_perm
    }
}

/// Stores short bytes on stack, stores long bytes on heap and provides
/// [`CStr`].
///
/// The threshold of allocation is [`libc::PATH_MAX`] (4096 on linux).
///
/// # Errors
/// Returns [`io::Error`]
///
/// Generates `InvalidInput` if the input bytes contain an interior nul byte
#[cfg(target_os = "linux")]
#[inline]
pub fn with_c_str<T>(bytes: &[u8], f: impl FnOnce(&CStr) -> io::Result<T>) -> io::Result<T> {
    /// The threshold of allocation
    #[allow(clippy::as_conversions)]
    const STACK_BUF_SIZE: usize = libc::PATH_MAX as usize; // 4096

    if memchr(0, bytes).is_some() {
        let err = io::Error::new(
            io::ErrorKind::InvalidInput,
            "input bytes contain an interior nul byte",
        );
        return Err(err);
    }

    if bytes.len() >= STACK_BUF_SIZE {
        let c_string = unsafe { CString::from_vec_unchecked(Vec::from(bytes)) };
        return f(&c_string);
    }

    let mut buf: MaybeUninit<[u8; STACK_BUF_SIZE]> = MaybeUninit::uninit();

    unsafe {
        let buf: *mut u8 = buf.as_mut_ptr().cast();
        ptr::copy_nonoverlapping(bytes.as_ptr(), buf, bytes.len());
        buf.add(bytes.len()).write(0);

        let bytes_with_nul = slice::from_raw_parts(buf, bytes.len().wrapping_add(1));
        let c_str = CStr::from_bytes_with_nul_unchecked(bytes_with_nul);

        f(c_str)
    }
}

/// cast `&[c_char]` to `&[u8]`
#[must_use]
#[inline]
pub fn cstr_to_bytes(s: &[c_char]) -> &[u8] {
    unsafe { slice::from_raw_parts(s.as_ptr().cast(), s.len()) }
}

/// Returns the platform-specific value of errno
#[cfg(target_os = "linux")]
#[must_use]
#[inline]
pub fn errno() -> i32 {
    unsafe { *libc::__errno_location() }
}

/// Sets the platform-specific errno to no-error
#[cfg(target_os = "linux")]
#[inline]
pub fn clear_errno() {
    unsafe { *libc::__errno_location() = 0_i32 }
}

/// Converts [`nix::Error`] to [`io::Error`]
#[must_use]
pub fn nix_to_io_error(err: nix::Error) -> io::Error {
    err.into()
}

/// Converts [`u32`] to [`usize`]
#[allow(clippy::missing_const_for_fn)] // <- false positive
#[inline]
#[must_use]
pub fn u32_to_usize(x: u32) -> usize {
    #[allow(clippy::as_conversions)]
    #[cfg(not(target_pointer_width = "16"))]
    {
        x as usize
    }
    #[cfg(target_pointer_width = "16")]
    {
        use std::convert::TryInto;
        x.try_into().expect("number cast failed")
    }
}

/// Converts [`usize`] to [`u64`]
#[allow(clippy::missing_const_for_fn)] // <- false positive
#[inline]
#[must_use]
pub fn usize_to_u64(x: usize) -> u64 {
    #[allow(clippy::as_conversions)]
    #[cfg(not(target_pointer_width = "128"))]
    {
        x as u64
    }
    #[cfg(target_pointer_width = "128")]
    {
        use std::convert::TryInto;
        x.try_into().expect("number cast failed")
    }
}

/// Converts [`u64`] to [`*const ()`]
#[allow(clippy::missing_const_for_fn)] // <- false positive
#[inline]
#[must_use]
pub fn u64_to_ptr(x: u64) -> *const () {
    #[allow(clippy::as_conversions)]
    #[cfg(not(any(target_pointer_width = "16", target_pointer_width = "32")))]
    {
        x as *const ()
    }
    #[cfg(any(target_pointer_width = "16", target_pointer_width = "32"))]
    {
        use std::convert::TryInto;
        x.try_into().expect("number cast failed")
    }
}

/// Round up `len` to a multiple of `align`.
///
/// <https://doc.rust-lang.org/std/alloc/struct.Layout.html#method.padding_needed_for>
///
/// <https://doc.rust-lang.org/src/core/alloc/layout.rs.html#226-250>
#[must_use]
pub const fn round_up(len: usize, align: usize) -> usize {
    len.wrapping_add(align).wrapping_sub(1) & !align.wrapping_sub(1)
}
