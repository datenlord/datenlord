//! Utility functions

use anyhow::Context;
use nix::errno::Errno;
use nix::sys::stat::SFlag;
use std::error::Error;
use std::os::raw::c_int;

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
