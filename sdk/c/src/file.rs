use std::{
    ffi::{c_void, CStr},
    os::raw::{c_char, c_longlong, c_uint, c_ulonglong},
    sync::Arc,
};

use bytes::BytesMut;
use datenlord::fs::{
    datenlordfs::{DatenLordFs, S3MetaData},
    fs_util::FileAttr,
    virtualfs::VirtualFs,
};
use nix::{fcntl::OFlag, libc::mode_t};
use tokio::sync::Mutex;
use tracing::{debug, error, info};

use crate::{
    error::datenlord_error,
    sdk::{datenlord_bytes, datenlord_sdk, RUNTIME},
    utils::find_parent_attr,
};

/// Open a file return current fd
/// mode_t is a type that represents file mode and permission bits
#[no_mangle]
#[allow(clippy::unwrap_used)]
pub extern "C" fn dl_open(
    sdk: *mut datenlord_sdk,
    file_path: *const c_char,
    mode: mode_t,
) -> c_ulonglong {
    if sdk.is_null() || file_path.is_null() {
        return 0;
    }

    let file_path_str = unsafe { CStr::from_ptr(file_path).to_str().unwrap() };
    let sdk_ref = unsafe { &*sdk };
    let fs = unsafe { Arc::from_raw(sdk_ref.datenlordfs as *const DatenLordFs<S3MetaData>) };

    let result = RUNTIME.handle().block_on(async {
        info!("Writing file: {:?}", file_path_str);
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
        Ok(fh) => fh,
        Err(e) => {
            error!("Failed to open file: {:?}", e);
            return 0;
        }
    }
}

/// Close a opened file
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
#[no_mangle]
pub extern "C" fn dl_write(
    sdk: *mut datenlord_sdk,
    ino: c_ulonglong,
    fd: c_ulonglong,
    content: datenlord_bytes,
) -> c_longlong {
    if sdk.is_null() || content.data.is_null() || content.len == 0 {
        error!("Invalid arguments");
        return -1;
    }

    let data = unsafe { std::slice::from_raw_parts(content.data, content.len) };

    let sdk_ref = unsafe { &*sdk };
    let fs = unsafe { Arc::from_raw(sdk_ref.datenlordfs as *const DatenLordFs<S3MetaData>) };

    let result = RUNTIME.handle().block_on(async {
        info!("Writing to fd: {:?}", fd);
        match fs.write(ino, fd, 0, data, 0).await {
            Ok(()) => {
                info!("Writing succeeded for fd: {:?}", fd);
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
            0
        }
        Err(e) => {
            error!("Failed to write to fd: {:?}", e);
            -1
        }
    }
}

/// Read from a opened file
#[no_mangle]
pub extern "C" fn dl_read(
    sdk: *mut datenlord_sdk,
    ino: c_ulonglong,
    fd: c_ulonglong,
    out_content: *mut datenlord_bytes,
    size: c_uint,
) -> c_longlong {
    if sdk.is_null() || out_content.is_null() {
        error!("Invalid arguments");
        return -1;
    }

    let size = size as u32;
    let sdk_ref = unsafe { &*sdk };
    let fs = unsafe { Arc::from_raw(sdk_ref.datenlordfs as *const DatenLordFs<S3MetaData>) };

    let result = RUNTIME.handle().block_on(async {
        let mut buffer = BytesMut::with_capacity(size as usize);

        match fs
            .read(ino, fd, 0, buffer.capacity() as u32, &mut buffer)
            .await
        {
            Ok(read_size) => {
                unsafe {
                    (*out_content).data = buffer.as_ptr();
                    (*out_content).len = read_size;
                }
                debug!("Read from fd: {:?}", fd);
                Ok(())
            }
            Err(e) => {
                error!("Failed to read from fd: {:?}", e);
                Err(e)
            }
        }
    });

    let _ = Arc::into_raw(fs);

    match result {
        Ok(()) => {
            debug!("Read succeeded for fd: {:?}", fd);
            0
        }
        Err(e) => {
            error!("Failed to read from fd: {:?}", e);
            -1
        }
    }
}
