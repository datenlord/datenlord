use bytes::BytesMut;
use clippy_utilities::OverflowArithmetic;
use core::panic;
use datenlord::common::error::{DatenLordError, DatenLordResult};
use datenlord::common::logger::init_logger;
use datenlord::common::task_manager::{TaskName, TASK_MANAGER};
use datenlord::config::{self, InnerConfig, NodeRole};
use datenlord::fs::datenlordfs::{DatenLordFs, MetaData, S3MetaData};
use datenlord::fs::fs_util::{CreateParam, FileAttr, RenameParam, ROOT_ID};
use datenlord::fs::kv_engine::etcd_impl::EtcdKVEngine;
use datenlord::fs::kv_engine::{KVEngine, KVEngineType};
use datenlord::fs::virtualfs::VirtualFs;
use datenlord::metrics;
use datenlord::new_storage::{BackendBuilder, MemoryCache, StorageManager};
use nix::fcntl::OFlag;
use nix::sys::stat::SFlag;
use once_cell::sync::Lazy;
use parking_lot;
use std::ffi::{c_void, CStr};
use std::os::raw::{c_char, c_longlong, c_ulonglong};
use std::path::Path;
use std::ptr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error};

use crate::utils;

/// Lazy runtime for current thread
pub static RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

/// File attributes
/// This structure is used to store the file attributes, which are used to store the file metadata.
#[repr(C)]
#[allow(non_camel_case_types)]
pub struct datenlord_file_stat {
    /// Inode number
    pub ino: u64,
    /// Size in bytes
    pub size: u64,
    /// Size in blocks
    pub blocks: u64,
    /// Permissions
    pub perm: u16,
    /// Number of hard links
    pub nlink: u32,
    /// User id
    pub uid: u32,
    /// Group id
    pub gid: u32,
    /// Rdev
    pub rdev: u32,
}

#[repr(C)]
#[allow(non_camel_case_types)]
/// DatenLord SDK core data structure
/// This structure is used to store the SDK instance, which is used to interact with the DatenLord SDK.
/// We need to use init_sdk to initialize the SDK and free_sdk to release the SDK manually.
pub struct datenlord_sdk {
    // Do not expose the internal structure, use c_void instead
    pub datenlordfs: *mut c_void,
}

/// Helper function to find the parent directory's attributes by the given path
async fn find_parent_attr(
    path: &str,
    fs: Arc<DatenLordFs<S3MetaData>>,
) -> DatenLordResult<(Duration, FileAttr)> {
    let mut path_components: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    if path_components.is_empty() || path_components.len() == 1 {
        return fs.getattr(ROOT_ID).await;
    }

    // Delete the last component to find the parent inode
    path_components.pop();

    // Find the file from parent inode
    let mut current_inode = ROOT_ID;
    for component in &path_components {
        match fs.lookup(0, 0, current_inode, component).await {
            Ok((_duration, attr, _generation)) => {
                current_inode = attr.ino;
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
    fs.getattr(current_inode).await
}

#[no_mangle]
#[allow(clippy::unwrap_used)]
/// Initialize the DatenLord SDK by the given config file
///
/// config: path to the config file
pub extern "C" fn dl_init_sdk(config: *const c_char) -> *mut datenlord_sdk {
    // Provide a config file for initialization
    if config.is_null() {
        return ptr::null_mut();
    }

    let config_str = unsafe { CStr::from_ptr(config).to_str().unwrap_or("config.toml") };
    println!("Config file: {}", config_str);

    // Parse the config file and initialize the SDK
    let mut arg_conf = config::Config::default();
    arg_conf.config_file = Some(config_str.to_string());

    let arg_conf = config::Config::load_from_args(arg_conf) // Load config from file
        .unwrap_or_else(|e| {
            println!("Failed to load config: {:?}", e);
            panic!("Failed to load config: {:?}", e);
        });
    let config = InnerConfig::try_from(arg_conf).unwrap_or_else(|e| {
        println!("Failed to parse config: {:?}", e);
        panic!("Failed to parse config: {:?}", e);
    });

    init_logger(config.role.into(), config.log_level);

    // Initialize the runtime
    let datenlord_fs = RUNTIME.handle().block_on(async {
        match config.role {
            NodeRole::SDK => {
                let kv_engine: Arc<EtcdKVEngine> =
                    Arc::new(KVEngineType::new(config.kv_addrs.clone()).await.unwrap());
                let node_id = config.node_name.clone();

                TASK_MANAGER
                    .spawn(TaskName::Metrics, metrics::start_metrics_server)
                    .await
                    .unwrap();

                let storage = {
                    let storage_param = &config.storage.params;
                    let memory_cache_config = &config.storage.memory_cache_config;

                    let block_size = config.storage.block_size;
                    let capacity_in_blocks = memory_cache_config.capacity.overflow_div(block_size);

                    let cache = Arc::new(parking_lot::Mutex::new(MemoryCache::new(
                        capacity_in_blocks,
                        block_size,
                    )));
                    let backend = Arc::new(
                        BackendBuilder::new(storage_param.clone())
                            .build()
                            .await
                            .unwrap(),
                    );
                    StorageManager::new(cache, backend, block_size)
                };

                // Initialize the SDK and convert to void ptr.
                let metadata = S3MetaData::new(kv_engine, node_id.as_str()).await.unwrap();
                Arc::new(DatenLordFs::new(metadata, storage))
            }
            _ => {
                panic!("Invalid role for SDK");
            }
        }
    });

    let fs_ptr = Arc::into_raw(datenlord_fs) as *mut c_void;

    let sdk = Box::new(datenlord_sdk {
        datenlordfs: fs_ptr,
    });

    Box::into_raw(sdk)
}

/// Free the SDK instance
///
/// sdk: datenlord_sdk instance
#[no_mangle]
pub extern "C" fn dl_free_sdk(sdk: *mut datenlord_sdk) {
    // Stop daemon tasks
    if !sdk.is_null() {
        unsafe {
            let _ = Box::from_raw(sdk);
        }
    }
}

/// Check if the given path exists
///
/// sdk: datenlord_sdk instance
/// dir_path: path to the directory
///
/// Return: true if the path exists, otherwise false
#[no_mangle]
pub extern "C" fn dl_exists(sdk: *mut datenlord_sdk, dir_path: *const c_char) -> bool {
    if sdk.is_null() || dir_path.is_null() {
        return false;
    }

    let sdk = unsafe { &*sdk };
    // let rt = unsafe { &*(sdk.rt as *const runtime::Runtime) };
    let fs = unsafe { Arc::from_raw(sdk.datenlordfs as *const DatenLordFs<S3MetaData>) };

    let dir_path_str = unsafe { CStr::from_ptr(dir_path).to_string_lossy().into_owned() };

    let result = RUNTIME.handle().block_on(async {
        match find_parent_attr(&dir_path_str, fs.clone()).await {
            Ok((_, attr)) => {
                // Get current dir or file name from path and find current inode
                let path_components: Vec<&str> =
                    dir_path_str.split('/').filter(|s| !s.is_empty()).collect();
                fs.lookup(0, 0, attr.ino, path_components.last().unwrap())
                    .await
            }
            Err(e) => Err(e.into()),
        }
    });

    let _ = Arc::into_raw(fs);

    match result {
        Ok(_) => true,
        Err(_) => false,
    }
}

/// Create a directory
///
/// sdk: datenlord_sdk instance
/// dir_path: path to the directory
///
/// If the directory is created successfully, return the inode number, otherwise -1
#[no_mangle]
pub extern "C" fn dl_mkdir(sdk: *mut datenlord_sdk, dir_path: *const c_char) -> c_longlong {
    if sdk.is_null() || dir_path.is_null() {
        error!("Invalid SDK or directory path");
        return -1;
    }

    let sdk = unsafe { &*sdk };
    let fs = unsafe { Arc::from_raw(sdk.datenlordfs as *const DatenLordFs<S3MetaData>) };

    let dir_path_str = unsafe { CStr::from_ptr(dir_path).to_string_lossy().into_owned() };

    let result = RUNTIME.handle().block_on(async {
        let param = CreateParam {
            parent: ROOT_ID,
            name: dir_path_str,
            mode: 0o777,
            rdev: 0,
            uid: 0,
            gid: 0,
            node_type: SFlag::S_IFDIR,
            link: None,
        };
        fs.mkdir(param).await
    });

    let _ = Arc::into_raw(fs);

    match result {
        Ok((duration, attr, ino)) => {
            debug!(
                "Created directory duration:{:?} attr: {:?} with ino {:?}",
                duration, attr, ino
            );
            ino as c_longlong
        }
        Err(e) => {
            error!("Failed to create directory: {:?}", e);
            -1
        }
    }
}

/// Remove a directory
///
/// sdk: datenlord_sdk instance
/// dir_path: path to the directory
/// recursive: whether to remove the directory recursively, current not used
///
/// If the directory is removed successfully, return 0, otherwise -1
#[no_mangle]
pub extern "C" fn dl_rmdir(
    sdk: *mut datenlord_sdk,
    dir_path: *const c_char,
    recursive: bool,
) -> c_longlong {
    if sdk.is_null() || dir_path.is_null() {
        error!("Invalid SDK or directory path");
        return -1;
    }

    let sdk = unsafe { &*sdk };

    let fs = unsafe { Arc::from_raw(sdk.datenlordfs as *const DatenLordFs<S3MetaData>) };

    let dir_path_str = unsafe { CStr::from_ptr(dir_path).to_string_lossy().into_owned() };

    let result = RUNTIME.handle().block_on(async {
        utils::recursive_delete_dir(Arc::clone(&fs), &dir_path_str, recursive).await
    });

    let _ = Arc::into_raw(fs);

    match result {
        Ok(()) => 0,
        Err(e) => {
            error!("Failed to remove directory: {:?}", e);
            -1
        }
    }
}

/// Remove a file
///
/// sdk: datenlord_sdk instance
/// file_path: path to the file
///
/// If the file is removed successfully, return 0, otherwise -1
#[no_mangle]
pub extern "C" fn dl_remove(sdk: *mut datenlord_sdk, file_path: *const c_char) -> c_longlong {
    if sdk.is_null() || file_path.is_null() {
        error!("Invalid SDK or file path");
        return -1;
    }

    let sdk = unsafe { &*sdk };

    let fs = unsafe { Arc::from_raw(sdk.datenlordfs as *const DatenLordFs<S3MetaData>) };

    let file_path_str = unsafe { CStr::from_ptr(file_path).to_string_lossy().into_owned() };

    let result = RUNTIME.handle().block_on(async {
        match utils::find_parent_attr(&file_path_str, fs.clone()).await {
            Ok((_, parent_attr)) => {
                let filename = Path::new(&file_path_str)
                    .file_name()
                    .ok_or(DatenLordError::ArgumentInvalid {
                        context: vec!["Invalid file path".to_string()],
                    })?
                    .to_str()
                    .ok_or(DatenLordError::ArgumentInvalid {
                        context: vec!["Invalid file path".to_string()],
                    })?;

                match fs.lookup(0, 0, parent_attr.ino, filename).await {
                    Ok((_, _, _)) => {
                        // Check current file is exists
                        match fs.unlink(0, 0, parent_attr.ino, filename).await {
                            Ok(_) => Ok(()),
                            Err(e) => Err(DatenLordError::ArgumentInvalid {
                                context: vec![format!("Failed to remove file {e}")],
                            }),
                        }
                    }
                    Err(e) => Err(DatenLordError::ArgumentInvalid {
                        context: vec![format!("Failed to lookup file {e}")],
                    }),
                }
            }
            Err(e) => Err(DatenLordError::ArgumentInvalid {
                context: vec![format!("Failed to find parent attr {e}")],
            }),
        }
    });

    let _ = Arc::into_raw(fs);

    match result {
        Ok(()) => 0,
        Err(e) => {
            error!("Failed to remove file: {:?}", e);
            -1
        }
    }
}

/// Rename a file
///
/// sdk: datenlord_sdk instance
/// src_path: source file path
/// dest_path: destination file path
///
/// If the file is renamed successfully, return 0, otherwise -1
#[no_mangle]
pub extern "C" fn dl_rename(
    sdk: *mut datenlord_sdk,
    src_path: *const c_char,
    dest_path: *const c_char,
) -> c_longlong {
    if sdk.is_null() || src_path.is_null() || dest_path.is_null() {
        error!("Invalid SDK or file path");
        return -1;
    }

    let sdk = unsafe { &*sdk };

    let fs = unsafe { Arc::from_raw(sdk.datenlordfs as *const DatenLordFs<S3MetaData>) };

    let src_path_str = unsafe { CStr::from_ptr(src_path).to_string_lossy().into_owned() };
    let dest_path_str = unsafe { CStr::from_ptr(dest_path).to_string_lossy().into_owned() };

    let result = RUNTIME.handle().block_on(async {
        let param = RenameParam {
            old_parent: ROOT_ID,
            old_name: src_path_str,
            new_parent: ROOT_ID,
            new_name: dest_path_str,
            flags: 0, // TODO
        };
        fs.rename(0, 0, param).await
    });

    let _ = Arc::into_raw(fs);

    match result {
        Ok(()) => 0,
        Err(e) => {
            error!("Failed to rename file: {:?}", e);
            -1
        }
    }
}

/// Create a file
///
/// sdk: datenlord_sdk instance
/// file_path: path to the file
///
/// If the file is created successfully, return the inode number, otherwise -1
#[no_mangle]
pub extern "C" fn dl_mknod(sdk: *mut datenlord_sdk, file_path: *const c_char) -> c_longlong {
    if sdk.is_null() || file_path.is_null() {
        error!("Invalid SDK or file path");
        return -1;
    }

    let sdk = unsafe { &*sdk };

    let fs = unsafe { Arc::from_raw(sdk.datenlordfs as *const DatenLordFs<S3MetaData>) };

    let file_path_str = unsafe { CStr::from_ptr(file_path).to_string_lossy().into_owned() };

    let result = RUNTIME.handle().block_on(async {
        match find_parent_attr(&file_path_str, fs.clone()).await {
            Ok((_, attr)) => {
                // Get current dir or file name from path and find current inode
                let path_components: Vec<&str> =
                    file_path_str.split('/').filter(|s| !s.is_empty()).collect();
                let param = CreateParam {
                    parent: attr.ino,
                    name: path_components.last().unwrap().to_string(),
                    mode: 0o777,
                    rdev: 0,
                    uid: 0,
                    gid: 0,
                    node_type: SFlag::S_IFREG,
                    link: None,
                };
                fs.mknod(param).await
            }
            Err(e) => Err(e),
        }
    });

    let _ = Arc::into_raw(fs);

    match result {
        Ok((_, _, ino)) => {
            debug!("Created file: {:?} with ino {:?}", file_path_str, ino);
            ino as c_longlong
        }
        Err(e) => {
            error!("Failed to create file: {:?}", e);
            -1
        }
    }
}

/// Get the file attributes
///
/// sdk: datenlord_sdk instance
/// file_path: path to the file
/// file_metadata: datenlord_file_stat instance
///
/// If the file attributes are retrieved successfully, return 0, otherwise -1
#[no_mangle]
pub extern "C" fn dl_stat(
    sdk: *mut datenlord_sdk,
    file_path: *const c_char,
    file_metadata: *mut datenlord_file_stat,
) -> c_longlong {
    if sdk.is_null() || file_path.is_null() {
        error!("Invalid SDK or file path");
        return -1;
    }
    if file_metadata.is_null() {
        error!("Invalid file metadata");
        return -1;
    }

    let path = unsafe { CStr::from_ptr(file_path).to_str().unwrap_or_default() };
    let sdk_ref = unsafe { &*sdk };
    let file_metadata: &mut datenlord_file_stat = unsafe { &mut *file_metadata };
    let fs = unsafe { Arc::from_raw(sdk_ref.datenlordfs as *const DatenLordFs<S3MetaData>) };

    let result = RUNTIME.handle().block_on(async {
        // Find the file from parent inode
        match find_parent_attr(path, Arc::clone(&fs)).await {
            Ok((_, attr)) => {
                // Get current dir or file name from path and find current inode
                let path_components: Vec<&str> =
                    path.split('/').filter(|s| !s.is_empty()).collect();
                fs.lookup(0, 0, attr.ino, path_components.last().unwrap())
                    .await
            }
            Err(e) => Err(e),
        }
    });

    let _ = Arc::into_raw(fs);

    match result {
        Ok((_, attr, _)) => {
            (*file_metadata).ino = attr.ino;
            (*file_metadata).size = attr.size;
            (*file_metadata).blocks = attr.blocks;
            (*file_metadata).perm = attr.perm;
            (*file_metadata).nlink = attr.nlink;
            (*file_metadata).uid = attr.uid;
            (*file_metadata).gid = attr.gid;
            (*file_metadata).rdev = attr.rdev;
            0
        }
        Err(e) => {
            // TODO: Convert error to datenlord_error with specific code
            error!("Failed to stat file: {:?}", e);
            -1
        }
    }
}

/// Write data to a file
///
/// sdk: datenlord_sdk instance
/// file_path: path to the file
/// buf: buffer to store the file content
/// count: the size of the buffer
///
/// If the file is written successfully, return the number of bytes written, otherwise -1
#[no_mangle]
pub extern "C" fn dl_write_file(
    sdk: *mut datenlord_sdk,
    file_path: *const c_char,
    buf: *const u8,
    count: c_ulonglong,
) -> c_longlong {
    if sdk.is_null() || file_path.is_null() || buf.is_null() || count == 0 {
        error!("Invalid arguments");
        return -1;
    }

    let file_path_str = unsafe { CStr::from_ptr(file_path).to_string_lossy().into_owned() };

    let data = unsafe { std::slice::from_raw_parts(buf, count as usize) };

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
                    Ok((_, file_attr, _)) => {
                        match fs
                            .open(0, 0, file_attr.ino, OFlag::O_WRONLY.bits() as u32)
                            .await
                        {
                            Ok(fh) => match fs.write(file_attr.ino, fh, 0, data, 0).await {
                                Ok(_) => {
                                    debug!("Writing the file ok: {:?}", file_path_str);
                                    match fs.release(file_attr.ino, fh, 0, 0, true).await {
                                        Ok(_) => Ok(()),
                                        Err(e) => {
                                            error!("Failed to release file handle: {:?}", e);
                                            Err(e)
                                        }
                                    }
                                }
                                Err(e) => Err(e),
                            },
                            Err(e) => Err(e),
                        }
                    }
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    });

    let _ = Arc::into_raw(fs);

    match result {
        Ok(()) => {
            debug!("Write file: {:?}", file_path_str);
            count as i64
        }
        Err(e) => {
            error!("Failed to write file: {:?}", e);
            -1
        }
    }
}

/// Read a hole file
///
/// sdk: datenlord_sdk instance
/// file_path: path to the file
/// buf: buffer to store the file content
/// count: the size of the buffer
///
/// If the file is read successfully, return the number of bytes read, otherwise -1
#[no_mangle]
pub extern "C" fn dl_read_file(
    sdk: *mut datenlord_sdk,
    file_path: *const c_char,
    mut buf: *const u8,
    count: c_ulonglong,
) -> c_longlong {
    if sdk.is_null() || file_path.is_null() || buf.is_null() {
        error!("Invalid arguments");
        return -1;
    }

    let file_path_str = unsafe { CStr::from_ptr(file_path).to_string_lossy().into_owned() };

    let sdk_ref = unsafe { &*sdk };
    let fs = unsafe { Arc::from_raw(sdk_ref.datenlordfs as *const DatenLordFs<S3MetaData>) };

    let result = RUNTIME.handle().block_on(async {
        // Find current attribute
        match find_parent_attr(&file_path_str, fs.clone()).await {
            Ok((_, attr)) => {
                // Get current dir or file name from path and find current inode
                let path_components: Vec<&str> =
                    file_path_str.split('/').filter(|s| !s.is_empty()).collect();
                let (_, current_attr, _) = fs
                    .lookup(0, 0, attr.ino, path_components.last().unwrap())
                    .await
                    .unwrap();

                // Get current file handle
                match fs
                    .open(0, 0, current_attr.ino, OFlag::O_RDONLY.bits() as u32)
                    .await
                {
                    Ok(fh) => {
                        // TODO: convert raw ptr to buffer
                        let mut buffer = BytesMut::with_capacity(current_attr.size as usize);
                        match fs
                            .read(
                                current_attr.ino,
                                fh,
                                0,
                                buffer.capacity() as u32,
                                &mut buffer,
                            )
                            .await
                        {
                            Ok(read_size) => {
                                buf = buffer.as_ptr();
                                debug!("Read file: {:?} with size: {:?}", file_path_str, read_size);
                                // Close this file handle
                                fs.release(current_attr.ino, 0, 0, 0, true).await
                            }
                            Err(e) => Err(e),
                        }
                    }
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    });

    let _ = Arc::into_raw(fs);

    match result {
        Ok(()) => {
            debug!("Read file: {:?}", file_path_str);
            count as i64
        }
        Err(e) => {
            error!("Failed to read file: {:?}", e);
            -1
        }
    }
}
