use bytes::BytesMut;
use clap::Parser;
use clippy_utilities::OverflowArithmetic;
use datenlord::common::logger::init_logger;
use datenlord::common::task_manager::{TaskName, TASK_MANAGER};
use datenlord::config::{self, InnerConfig, NodeRole};
use datenlord::fs::datenlordfs::{DatenLordFs, MetaData, S3MetaData};
use datenlord::fs::fs_util::{CreateParam, INum, RenameParam, ROOT_ID};
use datenlord::fs::kv_engine::etcd_impl::EtcdKVEngine;
use datenlord::fs::kv_engine::{KVEngine, KVEngineType};
use datenlord::fs::virtualfs::VirtualFs;
use datenlord::metrics;
use datenlord::new_storage::{BackendBuilder, MemoryCache, StorageManager};
use nix::sys::stat::SFlag;
use core::panic;
use std::ffi::{c_void, CStr};
use std::os::raw::{c_char, c_uint};
use std::ptr;
use parking_lot;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tokio::runtime;


#[repr(C)]
#[allow(non_camel_case_types)]
pub struct datenlord_error {
    pub code: c_uint,
    pub message: datenlord_bytes,
}

/// File attributes
#[repr(C)]
#[allow(non_camel_case_types)]
pub struct datenlord_file_stat {
    /// Inode number
    pub ino: INum,
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

impl datenlord_error {
    fn new(code: c_uint, message: String) -> *mut datenlord_error {
        let message_bytes = message.into_bytes();
        let error = Box::new(datenlord_error {
            code,
            message: datenlord_bytes {
                data: message_bytes.as_ptr(),
                len: message_bytes.len(),
            },
        });
        Box::into_raw(error)
    }
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct datenlord_sdk {
    // Do not expose the internal structure, use c_void instead
    datenlordfs: *mut c_void,
    // Runtime for async operations
    rt: *mut c_void,
}

#[no_mangle]
#[allow(clippy::unwrap_used)]
pub extern "C" fn init(config: *const c_char) -> *mut datenlord_sdk {
    // Provide a config file for initialization
    if config.is_null() {
        return ptr::null_mut();
    }

    let config_str = unsafe { CStr::from_ptr(config).to_str().unwrap_or("config.toml") };

    // Parse the config file and initialize the SDK
    let arg_conf = config::Config::parse();
    let config = InnerConfig::try_from(config::Config::load_from_args(arg_conf).unwrap()).unwrap();

    init_logger(config.role.into(), config.log_level);

    // Initialize the runtime
    let rt = TASK_MANAGER.runtime();
    let rt_ptr = Box::into_raw(Box::new(rt)) as *mut c_void;

    let datenlord_fs = rt.block_on(async {
        match config.role {
            NodeRole::SDK => {
                let kv_engine: Arc<EtcdKVEngine> = Arc::new(KVEngineType::new(config.kv_addrs.clone()).await.unwrap());
                let node_id = config.node_name.clone();
                let ip_address = config.node_ip;
                let mount_dir = config.mount_path.clone();

                TASK_MANAGER
                    .spawn(TaskName::Metrics, metrics::start_metrics_server)
                    .await
                    .unwrap();

                let storage = {
                    let storage_param = &config.storage.params;
                    let memory_cache_config = &config.storage.memory_cache_config;

                    let block_size = config.storage.block_size;
                    let capacity_in_blocks = memory_cache_config.capacity.overflow_div(block_size);

                    let cache = Arc::new(parking_lot::Mutex::new(MemoryCache::new(capacity_in_blocks, block_size)));
                    let backend = Arc::new(BackendBuilder::new(storage_param.clone()).build().await.unwrap());
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
        rt: rt_ptr,
    });

    Box::into_raw(sdk)
}

#[no_mangle]
pub extern "C" fn free_sdk(sdk: *mut datenlord_sdk) {
    if !sdk.is_null() {
        unsafe {
            let _ = Box::from_raw(sdk);
        }
    }
}

#[no_mangle]
pub extern "C" fn exists(sdk: *mut datenlord_sdk, dir_path: *const c_char) -> bool {
    if sdk.is_null() || dir_path.is_null() {
        return false;
    }

    let sdk = unsafe { &*sdk };
    let rt = unsafe { &*(sdk.rt as *const runtime::Runtime) };
    let fs = unsafe { Arc::from_raw(sdk.datenlordfs as *const DatenLordFs<S3MetaData>) };

    let dir_path_str = unsafe { CStr::from_ptr(dir_path).to_string_lossy().into_owned() };

    let result = rt.block_on(async {
        fs.lookup(0, 0, ROOT_ID, &dir_path_str).await
    });

    let _ = Arc::into_raw(fs);
    result.is_ok()
}

#[no_mangle]
pub extern "C" fn mkdir(sdk: *mut datenlord_sdk, dir_path: *const c_char) -> *mut datenlord_error {
    if sdk.is_null() || dir_path.is_null() {
        return datenlord_error::new(1, "Invalid SDK or directory path".to_string());
    }

    let sdk = unsafe { &*sdk };
    let rt = unsafe { &*(sdk.rt as *const runtime::Runtime) };
    let fs = unsafe { Arc::from_raw(sdk.datenlordfs as *const DatenLordFs<S3MetaData>) };

    let dir_path_str = unsafe { CStr::from_ptr(dir_path).to_string_lossy().into_owned() };

    let result = rt.block_on(async {
        let param = CreateParam {
            parent: ROOT_ID,
            name: dir_path_str,
            mode: 0o755,
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
        Ok(_) => ptr::null_mut(),
        Err(e) => datenlord_error::new(2, format!("{:?}", e)),
    }
}


#[no_mangle]
pub extern "C" fn deldir(sdk: *mut datenlord_sdk, dir_path: *const c_char, recursive: bool) -> *mut datenlord_error {
    if sdk.is_null() || dir_path.is_null() {
        return datenlord_error::new(1, "Invalid SDK or directory path".to_string());
    }

    let sdk = unsafe { &*sdk };
    let rt = unsafe { &*(sdk.rt as *const runtime::Runtime) };
    let fs = unsafe { Arc::from_raw(sdk.datenlordfs as *const DatenLordFs<S3MetaData>) };

    let dir_path_str = unsafe { CStr::from_ptr(dir_path).to_string_lossy().into_owned() };

    let result = rt.block_on(async {
        if recursive {
            fs.rmdir(0, 0, ROOT_ID, &dir_path_str).await
        } else {
            fs.unlink(0, 0, ROOT_ID, &dir_path_str).await.map(|_| None)
        }
    });

    let _ = Arc::into_raw(fs);

    match result {
        Ok(_) => ptr::null_mut(),
        Err(e) => datenlord_error::new(3, format!("{:?}", e)),
    }
}

#[no_mangle]
pub extern "C" fn rename_path(sdk: *mut datenlord_sdk, src_path: *const c_char, dest_path: *const c_char) -> *mut datenlord_error {
    if sdk.is_null() || src_path.is_null() || dest_path.is_null() {
        return datenlord_error::new(1, "Invalid SDK or paths".to_string());
    }

    let sdk = unsafe { &*sdk };
    let rt = unsafe { &*(sdk.rt as *const runtime::Runtime) };
    let fs = unsafe { Arc::from_raw(sdk.datenlordfs as *const DatenLordFs<S3MetaData>) };

    let src_path_str = unsafe { CStr::from_ptr(src_path).to_string_lossy().into_owned() };
    let dest_path_str = unsafe { CStr::from_ptr(dest_path).to_string_lossy().into_owned() };

    let result = rt.block_on(async {
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
        Ok(_) => ptr::null_mut(),
        Err(e) => datenlord_error::new(4, format!("{:?}", e)),
    }
}


#[no_mangle]
pub extern "C" fn copy_from_local_file(
    sdk: *mut datenlord_sdk,
    overwrite: bool,
    local_file_path: *const c_char,
    dest_file_path: *const c_char,
) -> *mut datenlord_error {
    if sdk.is_null() || local_file_path.is_null() || dest_file_path.is_null() {
        return datenlord_error::new(1, "Invalid SDK or file paths".to_string());
    }

    let sdk = unsafe { &*sdk };
    let rt = unsafe { &*(sdk.rt as *const runtime::Runtime) };
    let fs = unsafe { Arc::from_raw(sdk.datenlordfs as *const DatenLordFs<S3MetaData>) };

    let local_file_path_str = unsafe { CStr::from_ptr(local_file_path).to_string_lossy().into_owned() };
    let dest_file_path_str = unsafe { CStr::from_ptr(dest_file_path).to_string_lossy().into_owned() };

    let result = rt.block_on(async {
        // TODO
        // fs.copy_from_local_file(&local_file_path_str, &dest_file_path_str, overwrite).await
    });

    let _ = Arc::into_raw(fs);

    match result {
        Ok(_) => ptr::null_mut(),
        Err(e) => datenlord_error::new(5, format!("{:?}", e)),
    }
}

#[no_mangle]
pub extern "C" fn copy_to_local_file(
    sdk: *mut datenlord_sdk,
    src_file_path: *const c_char,
    local_file_path: *const c_char,
) -> *mut datenlord_error {
    if sdk.is_null() || src_file_path.is_null() || local_file_path.is_null() {
        return datenlord_error::new(1, "Invalid SDK or file paths".to_string());
    }

    let sdk = unsafe { &*sdk };
    let rt = unsafe { &*(sdk.rt as *const runtime::Runtime) };
    let fs = unsafe { Arc::from_raw(sdk.datenlordfs as *const DatenLordFs<S3MetaData>) };

    let src_file_path_str = unsafe { CStr::from_ptr(src_file_path).to_string_lossy().into_owned() };
    let local_file_path_str = unsafe { CStr::from_ptr(local_file_path).to_string_lossy().into_owned() };

    let result = rt.block_on(async {
        // TODO
        // fs.copy_to_local_file(&src_file_path_str, &local_file_path_str).await
    });

    let _ = Arc::into_raw(fs);

    match result {
        Ok(_) => ptr::null_mut(),
        Err(e) => datenlord_error::new(6, format!("{:?}", e)),
    }
}


#[no_mangle]
pub extern "C" fn create_file(sdk: *mut datenlord_sdk, file_path: *const c_char) -> *mut datenlord_error {
    if sdk.is_null() || file_path.is_null() {
        return datenlord_error::new(1, "Invalid SDK or file path".to_string());
    }

    let sdk = unsafe { &*sdk };
    let rt = unsafe { &*(sdk.rt as *const runtime::Runtime) };
    let fs = unsafe { Arc::from_raw(sdk.datenlordfs as *const DatenLordFs<S3MetaData>) };

    let file_path_str = unsafe { CStr::from_ptr(file_path).to_string_lossy().into_owned() };

    let result = rt.block_on(async {
        let param = CreateParam {
            parent: ROOT_ID,
            name: file_path_str,
            mode: 0o644,
            rdev: 0,
            uid: 0,
            gid: 0,
            node_type: SFlag::S_IFREG,
            link: None,
        };
        fs.mknod(param).await
    });

    let _ = Arc::into_raw(fs);

    match result {
        Ok(_) => ptr::null_mut(),
        Err(e) => datenlord_error::new(7, format!("{:?}", e)),
    }
}


#[no_mangle]
pub extern "C" fn stat(
    sdk: *mut datenlord_sdk,
    file_path: *const c_char,
    file_metadata: *mut datenlord_file_stat,
) -> *mut datenlord_error {
    if sdk.is_null() || file_path.is_null() {
        return datenlord_error::new(1, "Invalid arguments".to_string());
    }
    if file_metadata.is_null() {
        return datenlord_error::new(1, "Invalid arguments".to_string());
    }

    let path = unsafe { CStr::from_ptr(file_path).to_str().unwrap_or_default() };
    let sdk_ref = unsafe { &*sdk };
    let file_metadata: &mut datenlord_file_stat = unsafe { &mut *file_metadata };

    let rt = Runtime::new().unwrap();
    let result = rt.block_on(async {
        let localfs = sdk_ref.localfs.lock().unwrap();
        localfs.getattr(1).await // 示例 inode
    });

    match result {
        Ok(attr) => {
            println!("File duration: {:?}, attr: {:?}", attr.0, attr.1);
            // Convert to file metadata
            file_metadata.size = attr.1.size;
            file_metadata.uid = attr.1.uid;
            file_metadata.gid = attr.1.gid;
            file_metadata.nlink = attr.1.nlink;
            file_metadata.rdev = attr.1.rdev;

            std::ptr::null_mut()
        }
        Err(_) => datenlord_error::new(1, "Failed to get file metadata".to_string()),
    }
}

#[no_mangle]
pub extern "C" fn write_file(
    sdk: *mut datenlord_sdk,
    file_path: *const c_char,
    content: datenlord_bytes,
) -> *mut datenlord_error {
    if sdk.is_null() || file_path.is_null() {
        return datenlord_error::new(1, "Invalid arguments".to_string());
    }

    let path = unsafe { CStr::from_ptr(file_path).to_str().unwrap_or_default() };
    let data = unsafe { std::slice::from_raw_parts(content.data, content.len) };

    println!(
        "Writing file: {} data size: {} data {}",
        path,
        data.len(),
        String::from_utf8_lossy(data)
    );

    let sdk_ref = unsafe { &*sdk };

    let rt = Runtime::new().unwrap();
    let result = rt.block_on(async {
        let localfs = sdk_ref.localfs.lock().unwrap();
        // demo params
        localfs.write(34734588, 0, 0, data, 0).await
    });

    match result {
        Ok(_) => std::ptr::null_mut(),
        Err(_) => datenlord_error::new(1, "Failed to write file".to_string()),
    }
}

#[no_mangle]
pub extern "C" fn read_file(
    sdk: *mut datenlord_sdk,
    file_path: *const c_char,
    out_content: *mut datenlord_bytes,
) -> *mut datenlord_error {
    if sdk.is_null() || file_path.is_null() || out_content.is_null() {
        return datenlord_error::new(1, "Invalid arguments".to_string());
    }

    let path = unsafe { CStr::from_ptr(file_path).to_str().unwrap_or_default() };

    let sdk_ref = unsafe { &*sdk };

    let rt = Runtime::new().unwrap();
    // TODO, use outside buffer
    let result = rt.block_on(async {
        let localfs = sdk_ref.localfs.lock().unwrap();

        // Convert buffer to c buffer
        let out_content_data = unsafe { (*out_content).data as *mut u8 };
        let out_content_len = unsafe { (*out_content).len };
        // let buffer: &mut [u8] =
        //     unsafe { std::slice::from_raw_parts_mut(out_content_data, out_content_len) };
        let mut buffer = BytesMut::with_capacity(out_content_len);

        localfs
            .read(34734588, 0, 0, buffer.len() as u32, &mut buffer)
            .await
    });

    match result {
        Ok(size) => {
            unsafe {
                (*out_content).len = size;
            }
            std::ptr::null_mut()
        }
        Err(_) => datenlord_error::new(1, "Failed to read file".to_string()),
    }
}
