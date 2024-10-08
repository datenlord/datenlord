use std::{
    ffi::CStr, os::raw::{c_char, c_longlong, c_ulonglong}, slice, sync::Arc
};

use datenlord::fs::{
    datenlordfs::{DatenLordFs, S3MetaData},
    virtualfs::VirtualFs,
};
use nix::libc::mode_t;
use tracing::{debug, error};

use crate::{
    sdk::{datenlord_sdk, RUNTIME},
    utils::find_parent_attr,
};

/// Open a file return current fd
///
/// sdk: datenlord_sdk
/// pathname: file path
/// mode_t: file mode and permission bits
///
/// If the file is opened successfully, return the file descriptor
/// Otherwise, return -1.
#[no_mangle]
#[allow(clippy::unwrap_used)]
pub extern "C" fn dl_open(
    sdk: *mut datenlord_sdk,
    pathname: *const c_char,
    mode: mode_t,
) -> c_longlong {
    if sdk.is_null() || pathname.is_null() {
        return -1;
    }

    let file_path_str = unsafe { CStr::from_ptr(pathname).to_str().unwrap() };
    let sdk_ref = unsafe { &*sdk };
    let fs = unsafe { Arc::from_raw(sdk_ref.datenlordfs as *const DatenLordFs<S3MetaData>) };

    let result = RUNTIME.handle().block_on(async {
        debug!("Writing file: {:?}", file_path_str);
        match find_parent_attr(&file_path_str, fs.clone()).await {
            Ok((_, parent_attr)) => {
                let path_components: Vec<&str> =
                    file_path_str.split('/').filter(|s| !s.is_empty()).collect();
                let file_name = path_components.last().unwrap();

                match fs.lookup(0, 0, parent_attr.ino, file_name).await {
                    Ok((_, file_attr, _)) => match fs.open(0, 0, file_attr.ino, mode).await {
                        Ok(fh) => {
                            return Ok(fh);
                        }
                        Err(e) => {
                            error!("Failed to open file: {:?}", e);
                            return Err(e);
                        }
                    },
                    Err(e) => {
                        error!("Failed to lookup file: {:?}", e);
                        return Err(e);
                    }
                }
            }
            Err(e) => {
                error!("Failed to find parent attr: {:?}", e);
                return Err(e);
            }
        }
    });

    let _ = Arc::into_raw(fs);

    match result {
        // Return i64 to match c_longlong
        Ok(fh) => fh as i64,
        Err(e) => {
            error!("Failed to open file: {:?}", e);
            return -1;
        }
    }
}

/// Close a opened file
///
/// sdk: datenlord_sdk
/// ino: file inode, which is returned by stat
/// fd: file descriptor, which is returned by dl_open
///
/// If the file is closed successfully, return 0
/// Otherwise, return -1.
#[no_mangle]
#[allow(clippy::unwrap_used)]
pub extern "C" fn dl_close(
    sdk: *mut datenlord_sdk,
    ino: c_ulonglong,
    fd: c_ulonglong,
) -> c_longlong {
    if sdk.is_null() {
        error!("Invalid arguments");
        return -1;
    }

    let sdk_ref = unsafe { &*sdk };
    let fs = unsafe { Arc::from_raw(sdk_ref.datenlordfs as *const DatenLordFs<S3MetaData>) };

    let result = RUNTIME.handle().block_on(async {
        match fs.release(ino, fd, 0, 0, true).await {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("Failed to close file: {:?}", e);
                return Err(e);
            }
        }
    });

    let _ = Arc::into_raw(fs);
    match result {
        Ok(()) => {
            debug!("Closed file: {:?}", fd);
            return 0;
        }
        Err(_) => {
            error!("Failed to close file: {:?}", fd);
            return -1;
        }
    }
}

/// Write to a opened file
///
/// sdk: datenlord_sdk
/// ino: file inode, which is returned by stat
/// fd: file descriptor, which is returned by dl_open
/// buf: data to write
/// count: data size
///
/// If the file is written successfully, return 0
/// Otherwise, return -1.
#[no_mangle]
pub extern "C" fn dl_write(
    sdk: *mut datenlord_sdk,
    ino: c_ulonglong,
    fd: c_ulonglong,
    buf: *const u8,
    count: c_ulonglong,
) -> c_longlong {
    if sdk.is_null() || buf.is_null() || count == 0 {
        error!("Invalid arguments");
        return -1;
    }

    let data = unsafe { std::slice::from_raw_parts(buf, count as usize) };

    let sdk_ref = unsafe { &*sdk };
    let fs = unsafe { Arc::from_raw(sdk_ref.datenlordfs as *const DatenLordFs<S3MetaData>) };

    let result = RUNTIME.handle().block_on(async {
        debug!("Writing to fd: {:?}", fd);
        match fs.write(ino, fd, 0, data, 0).await {
            Ok(()) => {
                debug!("Writing succeeded for fd: {:?}", fd);
                Ok(())
            }
            Err(e) => {
                error!("Failed to write to fd: {:?}", e);
                Err(e)
            }
        }
    });

    let _ = Arc::into_raw(fs);

    match result {
        Ok(()) => {
            debug!("Write succeeded for fd: {:?}", fd);
            // Current write does not return successed bytes, so we return the count directly
            count as i64
        }
        Err(e) => {
            error!("Failed to write to fd: {:?}", e);
            -1
        }
    }
}

/// Read from a opened file
///
/// sdk: datenlord_sdk
/// ino: file inode, which is returned by stat
/// fd: file descriptor, which is returned by dl_open
/// buf: buffer to store read data
/// count: buffer size
///
/// If the file is read successfully, return the read size
/// Otherwise, return -1.
#[no_mangle]
pub extern "C" fn dl_read(
    sdk: *mut datenlord_sdk,
    ino: c_ulonglong,
    fd: c_ulonglong,
    buf: *mut u8,
    count: c_ulonglong,
) -> c_longlong {
    if sdk.is_null() || buf.is_null() {
        error!("Invalid arguments");
        return -1;
    }

    let sdk_ref = unsafe { &*sdk };
    let fs = unsafe { Arc::from_raw(sdk_ref.datenlordfs as *const DatenLordFs<S3MetaData>) };

    let result = RUNTIME.handle().block_on(async {
        unsafe {
            let buffer = slice::from_raw_parts_mut(buf, count as usize);
            match fs.read(ino, fd, 0, count as u32, buffer).await {
                Ok(read_size) => {
                    debug!("Read from fd: {:?}", fd);
                    Ok(read_size)
                }
                Err(e) => {
                    error!("Failed to read from fd: {:?}", e);
                    Err(e)
                }
            }
        }
    });

    let _ = Arc::into_raw(fs);

    match result {
        Ok(read_size) => {
            debug!("Read succeeded for fd: {:?}", fd);
            read_size as i64
        }
        Err(e) => {
            error!("Failed to read from fd: {:?}", e);
            -1
        }
    }
}
