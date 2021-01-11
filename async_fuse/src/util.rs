//! Utility functions

use std::error::Error;
use std::ffi::{CStr, CString};
use std::mem::MaybeUninit;
use std::os::raw::{c_char, c_int};
use std::{io, ptr, slice};

use anyhow::Context;
use memchr::memchr;
use nix::errno::Errno;
use nix::sys::stat::SFlag;

/// Format `anyhow::Error`
// TODO: refactor this
#[must_use]
pub fn format_anyhow_error(error: &anyhow::Error) -> String {
    let err_msg_vec = error
        .chain()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>();
    let mut err_msg = err_msg_vec.as_slice().join(", caused by: ");
    err_msg.push_str(&format!(", root cause: {}", error.root_cause()));
    err_msg
}

/// Format `nix::Error`
// TODO: refactor this
#[must_use]
pub fn format_nix_error(error: nix::Error) -> String {
    format!("{}, root cause: {:?}", error, error.source())
}

/// Build `nix::Error::Sys(..)` from `libc` error code
#[must_use]
pub fn build_sys_error_from_errno(error_code: Errno) -> nix::Error {
    nix::Error::from_errno(error_code)
}

/// Build error result from `nix` error code
/// # Errors
///
/// Return the built `Err(anyhow::Error(..))`
pub fn build_error_result_from_errno<T>(error_code: Errno, err_msg: String) -> anyhow::Result<T> {
    Err(build_sys_error_from_errno(error_code)).context(err_msg)
}

/// Convert `nix::errno::Errno` to `c_int`
#[allow(clippy::as_conversions)]
#[must_use]
pub const fn convert_nix_errno_to_cint(error_no: Errno) -> c_int {
    error_no as c_int
}

/// Build file mode from `SFlag` and file permission
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
        _ => panic!("unknown SFlag type={:?}", kind),
    };
    let file_perm: u32 = perm.into();

    #[cfg(target_os = "linux")]
    {
        file_type | file_perm
    }
    #[cfg(target_os = "macos")]
    {
        let ftype: u32 = file_type.into();
        file_perm | ftype
    }
}

/// Stores short bytes on stack, stores long bytes on heap and provides [`CStr`].
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
    unsafe { *libc::__errno_location() = 0 }
}

/// Converts [`nix::Error`] to [`io::Error`]
#[must_use]
pub fn nix_to_io_error(err: nix::Error) -> io::Error {
    match err {
        nix::Error::Sys(errno) => errno.into(),
        nix::Error::InvalidPath | nix::Error::InvalidUtf8 | nix::Error::UnsupportedOperation => {
            io::Error::new(io::ErrorKind::Other, err)
        }
    }
}
