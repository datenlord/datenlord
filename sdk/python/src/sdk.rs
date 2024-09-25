/// `DatenLordSDK` is a Python class that provides an interface to interact with the DatenLord filesystem.
/// It allows users to perform various file operations such as creating directories, copying files,
/// renaming paths, and reading/writing files.
///
/// # Attributes
/// - `datenlordfs`: An `Arc` wrapped `DatenLordFs` instance that represents the filesystem.
///
/// # Methods
/// - `new(py: Python, config_path: String) -> PyResult<Self>`: Initializes a new instance of `DatenLordSDK`
///   by loading the configuration from the specified path and setting up the filesystem.
/// - `py_close<'a>(&'a self, py: Python<'a>) -> PyResult<Bound<PyAny>>`: Closes the SDK and releases all resources.
/// - `py_exists<'a>(&'a self, py: Python<'a>, path: String) -> PyResult<Bound<PyAny>>`: Checks if a file or directory exists at the specified path.
/// - `py_mkdir<'a>(&'a self, py: Python<'a>, path: &'a str) -> PyResult<Bound<PyAny>>`: Creates a new directory at the specified path.
/// - `py_deldir<'a>(&'a self, py: Python<'a>, path: &'a str, recursive: bool) -> PyResult<Bound<PyAny>>`: Deletes a directory at the specified path, optionally recursively.
/// - `py_copy_from_local_file<'a>(&'a self, py: Python<'a>, local_path: &'a str, dest_path: &'a str, overwrite: bool) -> PyResult<Bound<PyAny>>`: Copies a file from the local filesystem to the DatenLord filesystem.
/// - `py_copy_to_local_file<'a>(&'a self, py: Python<'a>, src_path: &'a str, local_path: &'a str) -> PyResult<Bound<PyAny>>`: Copies a file from the DatenLord filesystem to the local filesystem.
/// - `py_create_file<'a>(&'a self, py: Python<'a>, file_path: &'a str) -> PyResult<Bound<PyAny>>`: Creates a new file at the specified path.
/// - `py_rename_path<'a>(&'a self, py: Python<'a>, src_path: &'a str, dest_path: &'a str) -> PyResult<Bound<PyAny>>`: Renames a file or directory from `src_path` to `dest_path`.
/// - `py_stat<'a>(&'a self, py: Python<'a>, file_path: &'a str) -> PyResult<Bound<PyAny>>`: Retrieves the metadata of a file or directory at the specified path.
/// - `py_write_file<'a>(&'a self, py: Python<'a>, file_path: &'a str, content: &'a [u8]) -> PyResult<Bound<PyAny>>`: Writes content to a file at the specified path.
/// - `py_read_file<'a>(&'a self, py: Python<'a>, file_path: &'a str) -> PyResult<Bound<PyAny>>`: Reads the content of a file at the specified path.
/// - `py_open<'a>(&'a self, py: Python<'a>, file_path: String) -> PyResult<File>`: Opens a file at the specified path and returns a `File` object.
///
use bytes::BytesMut;
use clippy_utilities::OverflowArithmetic;
use datenlord::common::logger::init_logger;
use datenlord::common::task_manager::{TaskName, TASK_MANAGER};
use datenlord::config::{self, InnerConfig, NodeRole};
use datenlord::fs::datenlordfs::{DatenLordFs, MetaData, S3MetaData};
use datenlord::fs::fs_util::{CreateParam, RenameParam, ROOT_ID};
use datenlord::fs::kv_engine::etcd_impl::EtcdKVEngine;
use datenlord::fs::kv_engine::{KVEngine, KVEngineType};
use datenlord::fs::virtualfs::VirtualFs;
use datenlord::metrics;
use datenlord::new_storage::{BackendBuilder, MemoryCache, StorageManager};
use nix::fcntl::OFlag;
use nix::sys::stat::SFlag;
use pyo3::exceptions::PyException;
use pyo3::{pyclass, pymethods, Bound, PyAny, PyResult, Python};
use pyo3_asyncio::tokio::future_into_py;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use tracing::info;

use crate::file::File;
use crate::utils::{self, Buffer, Entry};

#[pyclass]
pub struct DatenLordSDK {
    datenlordfs: Arc<DatenLordFs<S3MetaData>>,
}

#[pymethods]
impl DatenLordSDK {
    #[new]
    pub fn new(_py: Python, config_path: String) -> PyResult<Self> {
        // let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let config_str = config_path.to_string();

        // Parse the config file and initialize the SDK
        let mut arg_conf = config::Config::default();
        arg_conf.config_file = Some(config_str.to_string());
        let config =
            InnerConfig::try_from(config::Config::load_from_args(arg_conf).unwrap()).unwrap();
        init_logger(config.role.into(), config.log_level);
        println!("Config: {:?}", config);

        // Initialize the runtime
        // let datenlordfs = runtime.handle().block_on(async {
        pyo3_asyncio::tokio::get_runtime().handle().block_on(async {
            // TASK_MANAGER.;
            info!("Starting DatenLord SDK");
        });

        // let datenlordfs = runtime.handle().block_on(async {
        let datenlordfs = pyo3_asyncio::tokio::get_runtime().handle().block_on(async {
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
                        let capacity_in_blocks =
                            memory_cache_config.capacity.overflow_div(block_size);

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

        Ok(DatenLordSDK {
            datenlordfs: datenlordfs,
        })
    }

    /// Close the SDK
    /// This function will shutdown the SDK and release all resources.
    /// and will block until all tasks are finished.
    /// Current pyo3 does not support __del__ method, so this function should be called explicitly.
    /// https://pyo3.rs/v0.22.3/class/protocols.html?highlight=__del#class-customizations
    #[pyo3(name = "close")]
    fn py_close<'a>(&'a self, py: Python<'a>) -> PyResult<Bound<PyAny>> {
        future_into_py(py, async move {
            TASK_MANAGER.shutdown().await;
            Ok(())
        })
    }

    #[pyo3(name = "exists")]
    fn py_exists<'a>(&'a self, py: Python<'a>, path: String) -> PyResult<Bound<PyAny>> {
        let fs = Arc::clone(&self.datenlordfs);

        future_into_py(py, async move {
            let result = match utils::find_parent_attr(&path, fs.clone()).await {
                Ok((_, attr)) => {
                    // Get current dir or file name from path and find current inode
                    let path_components: Vec<&str> =
                        path.split('/').filter(|s| !s.is_empty()).collect();
                    fs.lookup(0, 0, attr.ino, path_components.last().unwrap())
                        .await
                }
                Err(e) => Err(e.into()),
            };
            match result {
                Ok(_) => Ok(true),
                Err(_) => Ok(false),
            }
        })
    }

    #[pyo3(name = "mkdir")]
    fn py_mkdir<'a>(&'a self, py: Python<'a>, path: &'a str) -> PyResult<Bound<PyAny>> {
        let fs = Arc::clone(&self.datenlordfs);
        let path_str = path.to_string();

        future_into_py(py, async move {
            // Find parent inode
            let (_, parent_attr) = match utils::find_parent_attr(&path_str, fs.clone()).await {
                Ok(attr) => attr,
                Err(e) => return Err(PyException::new_err(format!("mkdir failed: {:?}", e))),
            };

            // File name
            let path_components: Vec<&str> =
                path_str.split('/').filter(|s| !s.is_empty()).collect();

            let param = CreateParam {
                parent: parent_attr.ino,
                name: path_components.last().unwrap().to_string(),
                mode: 0o777,
                rdev: 0,
                uid: 0,
                gid: 0,
                node_type: SFlag::S_IFDIR,
                link: None,
            };
            match fs.mkdir(param).await {
                Ok(_) => Ok(()),
                Err(e) => Err(PyException::new_err(format!("mkdir failed: {:?}", e))),
            }
        })
    }

    #[pyo3(name = "deldir")]
    fn py_deldir<'a>(
        &'a self,
        py: Python<'a>,
        path: &'a str,
        recursive: bool,
    ) -> PyResult<Bound<PyAny>> {
        let fs = Arc::clone(&self.datenlordfs);
        let path_str = path.to_string();

        future_into_py(py, async move {
            match utils::recursive_delete_dir(fs.clone(), &path_str, recursive).await {
                Ok(_) => Ok(()),
                Err(e) => Err(PyException::new_err(format!("deldir failed: {:?}", e))),
            }
        })
    }

    #[pyo3(name = "copy_from_local_file")]
    fn py_copy_from_local_file<'a>(
        &'a self,
        py: Python<'a>,
        local_path: &'a str,
        dest_path: &'a str,
        overwrite: bool,
    ) -> PyResult<Bound<PyAny>> {
        let fs = Arc::clone(&self.datenlordfs);
        let local_path_str = local_path.to_string();
        let dest_path_str = dest_path.to_string();

        future_into_py(py, async move {
            if !overwrite {
                if let Ok((_, _, _)) = fs.lookup(0, 0, ROOT_ID, &dest_path_str).await {
                    return Err(PyException::new_err("File already exists"));
                }
            }

            match std::fs::File::open(&local_path_str) {
                Ok(mut local_file) => {
                    let mut buffer = Vec::new();
                    if let Err(e) = local_file.read_to_end(&mut buffer) {
                        return Err(PyException::new_err(format!("read_to_end failed: {:?}", e)));
                    }

                    let path_components: Vec<&str> =
                        dest_path_str.split('/').filter(|s| !s.is_empty()).collect();
                    match utils::find_parent_attr(&dest_path_str, fs.clone()).await {
                        Ok((_, parent_attr)) => {
                            let param = CreateParam {
                                parent: parent_attr.ino,
                                name: path_components.last().unwrap().to_string(),
                                mode: 0o777,
                                rdev: 0,
                                uid: 0,
                                gid: 0,
                                node_type: SFlag::S_IFREG,
                                link: None,
                            };

                            match fs.mknod(param).await {
                                Ok((_, file_attr, _)) => {
                                    match fs
                                        .open(0, 0, file_attr.ino, OFlag::O_WRONLY.bits() as u32)
                                        .await
                                    {
                                        Ok(fh) => {
                                            match fs.write(file_attr.ino, fh, 0, &buffer, 0).await {
                                                Ok(_) => Ok(()),
                                                Err(e) => Err(PyException::new_err(format!(
                                                    "write failed: {:?}",
                                                    e
                                                ))),
                                            }
                                        }
                                        Err(e) => Err(PyException::new_err(format!(
                                            "open failed: {:?}",
                                            e
                                        ))),
                                    }
                                }
                                Err(e) => {
                                    Err(PyException::new_err(format!("mknod failed: {:?}", e)))
                                }
                            }
                        }
                        Err(e) => Err(PyException::new_err(format!(
                            "utils::find_parent_attr failed: {:?}",
                            e
                        ))),
                    }
                }
                Err(e) => Err(PyException::new_err(format!("File open error: {:?}", e))),
            }
        })
    }

    #[pyo3(name = "copy_to_local_file")]
    fn py_copy_to_local_file<'a>(
        &'a self,
        py: Python<'a>,
        src_path: &'a str,
        local_path: &'a str,
    ) -> PyResult<Bound<PyAny>> {
        let fs = Arc::clone(&self.datenlordfs);
        let src_path_str = src_path.to_string();
        let local_path_str = local_path.to_string();

        future_into_py(py, async move {
            match utils::find_parent_attr(&src_path_str, fs.clone()).await {
                Ok((_, attr)) => {
                    let path_components: Vec<&str> =
                        src_path_str.split('/').filter(|s| !s.is_empty()).collect();
                    let (_, file_attr, _) = fs
                        .lookup(0, 0, attr.ino, path_components.last().unwrap())
                        .await
                        .unwrap();

                    match fs
                        .open(0, 0, file_attr.ino, OFlag::O_RDONLY.bits() as u32)
                        .await
                    {
                        Ok(fh) => {
                            let mut buffer = BytesMut::with_capacity(file_attr.size as usize);
                            match fs
                                .read(file_attr.ino, fh, 0, buffer.capacity() as u32, &mut buffer)
                                .await
                            {
                                Ok(read_size) => {
                                    let mut local_file = std::fs::File::create(local_path_str)?;
                                    local_file.write_all(&buffer[..read_size])?;
                                    fs.release(file_attr.ino, fh, 0, 0, true)
                                        .await
                                        .map_err(|e| {
                                            PyException::new_err(format!(
                                                "Failed to release file handle: {:?}",
                                                e
                                            ))
                                        })
                                }
                                Err(e) => {
                                    Err(PyException::new_err(format!("read failed: {:?}", e)))
                                }
                            }
                        }
                        Err(e) => Err(PyException::new_err(format!("open failed: {:?}", e))),
                    }
                }
                Err(e) => Err(PyException::new_err(format!(
                    "utils::find_parent_attr failed: {:?}",
                    e
                ))),
            }
        })
    }

    #[pyo3(name = "create_file")]
    fn py_create_file<'a>(&'a self, py: Python<'a>, file_path: &'a str) -> PyResult<Bound<PyAny>> {
        let fs = Arc::clone(&self.datenlordfs);
        let file_path_str = file_path.to_string();

        future_into_py(py, async move {
            match utils::find_parent_attr(&file_path_str, fs.clone()).await {
                Ok((_, attr)) => {
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
                    match fs.mknod(param).await {
                        Ok(_) => Ok(()),
                        Err(e) => Err(PyException::new_err(format!("mknod failed: {:?}", e))),
                    }
                }
                Err(e) => Err(PyException::new_err(format!(
                    "utils::find_parent_attr failed: {:?}",
                    e
                ))),
            }
        })
    }

    #[pyo3(name = "rename_path")]
    fn py_rename_path<'a>(
        &'a self,
        py: Python<'a>,
        src_path: &'a str,
        dest_path: &'a str,
    ) -> PyResult<Bound<PyAny>> {
        let fs = Arc::clone(&self.datenlordfs);
        let src_path_str = src_path.to_string();
        let dest_path_str = dest_path.to_string();

        future_into_py(py, async move {
            // Find parent inode
            match utils::find_parent_attr(&src_path_str, fs.clone()).await {
                Ok((_, attr)) => {
                    let src_path_components: Vec<&str> =
                        src_path_str.split('/').filter(|s| !s.is_empty()).collect();
                    let dest_path_components: Vec<&str> =
                        dest_path_str.split('/').filter(|s| !s.is_empty()).collect();
                    let param = RenameParam {
                        old_parent: attr.ino,
                        old_name: src_path_components.last().unwrap().to_string(),
                        new_parent: attr.ino,
                        new_name: dest_path_components.last().unwrap().to_string(),
                        flags: 0,
                    };
                    match fs.rename(0, 0, param).await {
                        Ok(_) => Ok(()),
                        Err(e) => Err(PyException::new_err(format!("rename failed: {:?}", e))),
                    }
                }
                Err(e) => return Err(PyException::new_err(format!("rename failed: {:?}", e))),
            }
        })
    }

    #[pyo3(name = "stat")]
    fn py_stat<'a>(&'a self, py: Python<'a>, file_path: &'a str) -> PyResult<Bound<PyAny>> {
        let fs = Arc::clone(&self.datenlordfs);
        let file_path_str = file_path.to_string();

        future_into_py(py, async move {
            match utils::find_parent_attr(&file_path_str, fs.clone()).await {
                Ok((_, parent_attr)) => {
                    let path_components: Vec<&str> =
                        file_path_str.split('/').filter(|s| !s.is_empty()).collect();
                    match fs
                        .lookup(0, 0, parent_attr.ino, path_components.last().unwrap())
                        .await
                    {
                        Ok((_, file_attr, _)) => {
                            let file_stat = (
                                file_attr.ino,
                                file_attr.size,
                                file_attr.blocks,
                                file_attr.perm,
                                file_attr.nlink,
                                file_attr.uid,
                                file_attr.gid,
                                file_attr.rdev,
                            );
                            Ok(file_stat)
                        }
                        Err(e) => Err(PyException::new_err(format!("stat failed: {:?}", e))),
                    }
                }
                Err(e) => Err(PyException::new_err(format!(
                    "utils::find_parent_attr failed: {:?}",
                    e
                ))),
            }
        })
    }

    #[pyo3(name = "write_file")]
    fn py_write_file<'a>(
        &'a self,
        py: Python<'a>,
        file_path: &'a str,
        content: &'a [u8],
    ) -> PyResult<Bound<PyAny>> {
        let fs = Arc::clone(&self.datenlordfs);
        let file_path_str = file_path.to_string();
        let data = content.to_vec();

        future_into_py(py, async move {
            match utils::find_parent_attr(&file_path_str, fs.clone()).await {
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
                                    match fs.write(file_attr.ino, fh, 0, &data, 0).await {
                                        Ok(_) => {
                                            match fs.release(file_attr.ino, fh, 0, 0, true).await {
                                                Ok(_) => {
                                                    // Sleep for a while to ensure the file is written to the storage backend
                                                    // tokio::time::sleep(Duration::from_secs(5)).await;
                                                    Ok(())
                                                }
                                                Err(e) => Err(PyException::new_err(format!(
                                                    "Failed to release file handle: {:?}",
                                                    e
                                                ))),
                                            }
                                        }
                                        Err(e) => Err(PyException::new_err(format!(
                                            "write failed: {:?}",
                                            e
                                        ))),
                                    }
                                }
                                Err(e) => {
                                    Err(PyException::new_err(format!("open failed: {:?}", e)))
                                }
                            }
                        }
                        Err(e) => Err(PyException::new_err(format!("lookup failed: {:?}", e))),
                    }
                }
                Err(e) => Err(PyException::new_err(format!(
                    "utils::find_parent_attr failed: {:?}",
                    e
                ))),
            }
        })
    }

    #[pyo3(name = "read_file")]
    fn py_read_file<'a>(&'a self, py: Python<'a>, file_path: &'a str) -> PyResult<Bound<PyAny>> {
        let fs = Arc::clone(&self.datenlordfs);
        let file_path_str = file_path.to_string();

        future_into_py(py, async move {
            match utils::find_parent_attr(&file_path_str, fs.clone()).await {
                Ok((_, parent_attr)) => {
                    let path_components: Vec<&str> =
                        file_path_str.split('/').filter(|s| !s.is_empty()).collect();
                    let file_name = path_components.last().unwrap();

                    match fs.lookup(0, 0, parent_attr.ino, file_name).await {
                        Ok((_, file_attr, _)) => {
                            match fs
                                .open(0, 0, file_attr.ino, OFlag::O_RDONLY.bits() as u32)
                                .await
                            {
                                Ok(fh) => {
                                    let mut buffer =
                                        BytesMut::with_capacity(file_attr.size as usize);
                                    let _ = fs
                                        .read(
                                            file_attr.ino,
                                            fh,
                                            0,
                                            buffer.capacity() as u32,
                                            &mut buffer,
                                        )
                                        .await
                                        .map_err(|e| {
                                            PyException::new_err(format!("read failed: {:?}", e))
                                        })?;
                                    Python::with_gil(|py| {
                                        Buffer::new(buffer.freeze().to_vec()).into_bytes(py)
                                    })
                                }
                                Err(e) => {
                                    Err(PyException::new_err(format!("open failed: {:?}", e)))
                                }
                            }
                        }
                        Err(e) => Err(PyException::new_err(format!("lookup failed: {:?}", e))),
                    }
                }
                Err(e) => Err(PyException::new_err(format!(
                    "utils::find_parent_attr failed: {:?}",
                    e
                ))),
            }
        })
    }

    /// Open a file and return a `File` object. mode is a string that represents the file open mode.
    #[pyo3(name = "open")]
    fn py_open<'a>(&'a self, _py: Python<'a>, file_path: String, mode: String) -> PyResult<File> {
        let fs = Arc::clone(&self.datenlordfs);
        // Convert mode string to OFlag
        let mode = match mode.as_str() {
            "r" => OFlag::O_RDONLY,
            "w" => OFlag::O_WRONLY,
            "a" => OFlag::O_APPEND,
            _ => OFlag::O_RDWR,
        };

        pyo3_asyncio::tokio::get_runtime().handle().block_on(async {
            match utils::find_parent_attr(&file_path, Arc::clone(&fs)).await {
                Ok((_, parent_attr)) => {
                    let path = Path::new(&file_path);
                    let file_name = path
                        .file_name()
                        .ok_or(PyException::new_err(format!(
                            "Invalid file path: {:?}",
                            file_path
                        )))?
                        .to_str()
                        .ok_or(PyException::new_err(format!(
                            "Invalid file path: {:?}",
                            file_path
                        )))?;

                    match fs.lookup(0, 0, parent_attr.ino, file_name).await {
                        Ok((_, file_attr, _)) => {
                            match fs.open(0, 0, file_attr.ino, mode.bits() as u32).await {
                                Ok(fh) => {
                                    let f = File::new(
                                        file_attr,
                                        fh,
                                        mode.bits() as u32,
                                        Arc::clone(&fs),
                                    );
                                    Ok(f)
                                }
                                Err(e) => {
                                    Err(PyException::new_err(format!("open failed: {:?}", e)))
                                }
                            }
                        }
                        Err(e) => Err(PyException::new_err(format!("lookup failed: {:?}", e))),
                    }
                }
                Err(e) => Err(PyException::new_err(format!(
                    "utils::find_parent_attr failed: {:?}",
                    e
                ))),
            }
        })
    }

    /// List dirs and files in a directory
    /// return a list of file names
    #[pyo3(name = "list_dir")]
    fn py_list_dir<'a>(&'a self, py: Python<'a>, dir_path: String) -> PyResult<Bound<PyAny>> {
        let fs = Arc::clone(&self.datenlordfs);

        future_into_py(py, async move {
            match utils::find_parent_attr(&dir_path, Arc::clone(&fs)).await {
                Ok((_, parent_attr)) => {
                    let path = Path::new(&dir_path);
                    let dir_name = path
                        .file_name()
                        .ok_or(PyException::new_err(format!(
                            "Invalid dir path: {:?}",
                            dir_path
                        )))?
                        .to_str()
                        .ok_or(PyException::new_err(format!(
                            "Invalid dir path: {:?}",
                            dir_path
                        )))?;

                    // Get list of files
                    match fs.lookup(0, 0, parent_attr.ino, dir_name).await {
                        Ok((_, dir_attr, _)) => {
                            // Current readdir does not support fh and offset
                            let dir_entries = fs.readdir(0, 0, dir_attr.ino, 0, 0).await.map_err(
                                |e| {
                                    PyException::new_err(format!("readdir failed: {:?}", e))
                                })?;
                            Ok(dir_entries
                                .into_iter()
                                .map(|e| Entry{
                                    name: e.name().to_owned(),
                                    ino: e.ino(),
                                    file_type: e.file_type(),
                                })
                                .collect::<Vec<_>>())
                        }
                        Err(e) => Err(PyException::new_err(format!("lookup failed: {:?}", e))),
                    }
                },
                Err(e) => Err(PyException::new_err(format!(
                    "utils::find_parent_attr failed: {:?}",
                    e
                ))),
            }
        })
    }
}
