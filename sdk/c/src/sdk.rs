use bytes::BytesMut;
use clap::Parser;
use clippy_utilities::OverflowArithmetic;
use core::panic;
use datenlord::common::error::{DatenLordError, DatenLordResult};
use datenlord::common::logger::init_logger;
use datenlord::common::task_manager::{TaskName, TASK_MANAGER};
use datenlord::config::{self, InnerConfig, NodeRole};
use datenlord::fs::datenlordfs::direntry::FileType;
use datenlord::fs::datenlordfs::{DatenLordFs, MetaData, S3MetaData};
use datenlord::fs::fs_util::{CreateParam, FileAttr, INum, RenameParam, ROOT_ID};
use datenlord::fs::kv_engine::etcd_impl::EtcdKVEngine;
use datenlord::fs::kv_engine::{KVEngine, KVEngineType};
use datenlord::fs::virtualfs::VirtualFs;
use datenlord::metrics;
use datenlord::new_storage::{BackendBuilder, MemoryCache, StorageManager};
use nix::fcntl::OFlag;
use nix::sys::stat::SFlag;
use once_cell::sync::Lazy;
use parking_lot;
use std::collections::VecDeque;
use std::ffi::{c_void, CStr};
use std::fs::File;
use std::io::{Read, Write};
use std::os::raw::{c_char, c_longlong, c_uint};
use std::path::Path;
use std::ptr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tokio::runtime;
use tracing::{debug, error, info};

use crate::error::datenlord_error;
use crate::utils;

// Lazy runtime for current thread
pub static RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

/// File attributes
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
pub struct datenlord_bytes {
    pub data: *const u8,
    pub len: usize,
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct datenlord_sdk {
    // Do not expose the internal structure, use c_void instead
    pub datenlordfs: *mut c_void,
}

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
    // arg_conf.storage.storage_type = "s3".to_string();
    // arg_conf.storage.block_size = 524288;
    // arg_conf.storage.s3_storage_config.access_key_id = "minioadmin".to_string();
    // arg_conf.storage.s3_storage_config.endpoint_url = "http://localhost:9000".to_string();
    // arg_conf.storage.s3_storage_config.secret_access_key = "minioadmin".to_string();
    // arg_conf.storage.s3_storage_config.bucket_name = "mybucket".to_string();
    // arg_conf.role = Some("sdk".to_string());
    // arg_conf.node_name = Some("sdk".to_string());
    // arg_conf.node_ip = Some("127.0.0.1".to_string());
    // arg_conf.kv_server_list = vec!["127.0.0.1:2379".to_string()];
    // arg_conf.mount_path = Some("/tmp/datenlord_data_dir".to_string());

    println!("Config: {:?}", arg_conf);
    let arg_conf = config::Config::load_from_args(arg_conf) // Load config from file
        .unwrap_or_else(|e| {
            println!("Failed to load config: {:?}", e);
            panic!("Failed to load config: {:?}", e);
        });
    let config = InnerConfig::try_from(arg_conf).unwrap_or_else(|e| {
        println!("Failed to parse config: {:?}", e);
        panic!("Failed to parse config: {:?}", e);
    });

    println!("Config: {:?}", config);
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

#[no_mangle]
pub extern "C" fn dl_free_sdk(sdk: *mut datenlord_sdk) {
    // Stop daemon tasks
    if !sdk.is_null() {
        unsafe {
            let _ = Box::from_raw(sdk);
        }
    }
}

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

#[no_mangle]
pub extern "C" fn dl_write_file(
    sdk: *mut datenlord_sdk,
    file_path: *const c_char,
    content: datenlord_bytes,
) -> c_longlong {
    if sdk.is_null() || file_path.is_null() || content.data.is_null() || content.len == 0 {
        error!("Invalid arguments");
        return -1;
    }

    let file_path_str = unsafe { CStr::from_ptr(file_path).to_string_lossy().into_owned() };

    let data = unsafe { std::slice::from_raw_parts(content.data, content.len) };

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
                    Ok((_, file_attr, _)) => {
                        match fs
                            .open(0, 0, file_attr.ino, OFlag::O_WRONLY.bits() as u32)
                            .await
                        {
                            Ok(fh) => {
                                info!("Writing the file: {:?}", file_path_str);
                                info!("Writing the data: {:?}", data);
                                match fs.write(file_attr.ino, fh, 0, data, 0).await {
                                    Ok(_) => {
                                        info!("Writing the file ok: {:?}", file_path_str);
                                        match fs.release(file_attr.ino, fh, 0, 0, true).await {
                                            Ok(_) => Ok(()),
                                            Err(e) => {
                                                error!("Failed to release file handle: {:?}", e);
                                                Err(e)
                                            }
                                        }
                                    }
                                    Err(e) => Err(e),
                                }
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
        Ok(()) => 0,
        Err(e) => {
            error!("Failed to write file: {:?}", e);
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn dl_read_file(
    sdk: *mut datenlord_sdk,
    file_path: *const c_char,
    out_content: *mut datenlord_bytes,
) -> c_longlong {
    if sdk.is_null() || file_path.is_null() || out_content.is_null() {
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
                                unsafe {
                                    (*out_content).data = buffer.as_ptr();
                                    (*out_content).len = read_size;
                                }
                                debug!("Read file: {:?}", file_path_str);
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
        Ok(()) => 0,
        Err(e) => {
            error!("Failed to read file: {:?}", e);
            -1
        }
    }
}
