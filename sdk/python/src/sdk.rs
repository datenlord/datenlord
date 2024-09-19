use bytes::BytesMut;
use clap::Parser;
use clippy_utilities::OverflowArithmetic;
use datenlord::common::logger::init_logger;
use datenlord::common::task_manager::{TaskName, TASK_MANAGER};
use datenlord::config::{self, InnerConfig, NodeRole};
use datenlord::fs::kv_engine::etcd_impl::EtcdKVEngine;
use datenlord::fs::kv_engine::{KVEngine, KVEngineType};
use datenlord::fs::virtualfs::VirtualFs;
use datenlord::metrics;
use pyo3::exceptions::PyException;
use pyo3::types::PyModule;
use pyo3::{pyclass, pymethods, pymodule, Bound, PyAny, PyResult, Python};
use pyo3_asyncio::tokio::future_into_py;
use tracing::info;
use std::io::{Read, Write};
use std::sync::Arc;
use std::time::Duration;
use datenlord::common::error::DatenLordResult;
use datenlord::fs::datenlordfs::{DatenLordFs, MetaData, S3MetaData};
use datenlord::fs::fs_util::{CreateParam, FileAttr, RenameParam, ROOT_ID};
use datenlord::new_storage::{BackendBuilder, MemoryCache, StorageManager};
use nix::fcntl::OFlag;
use nix::sys::stat::SFlag;
use std::collections::VecDeque;

#[pyclass]
pub struct DatenLordSDK {
    datenlordfs: Arc<DatenLordFs<S3MetaData>>,
}

#[pymodule]
fn datenlordsdk(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<DatenLordSDK>()?;
    Ok(())
}

#[pymethods]
impl DatenLordSDK {
    #[new]
    pub fn new(config_path: &str) -> PyResult<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let config_str = config_path.to_string();

        // Parse the config file and initialize the SDK
        let mut arg_conf = config::Config::default();
        arg_conf.config_file = Some(config_str.to_string());
        let config = InnerConfig::try_from(config::Config::load_from_args(arg_conf).unwrap()).unwrap();
        init_logger(config.role.into(), config.log_level);
        println!("Config: {:?}", config);

        // Initialize the runtime
        let datenlordfs = runtime.handle().block_on(async {
            match config.role {
                NodeRole::SDK => {
                    let kv_engine: Arc<EtcdKVEngine> = Arc::new(KVEngineType::new(config.kv_addrs.clone()).await.unwrap());
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

        Ok(DatenLordSDK { datenlordfs: datenlordfs })
    }

    #[pyo3(name = "exists")]
    fn py_exists<'a>(&'a self, py: Python<'a>, path: String) -> PyResult<Bound<PyAny>> {
        let fs = Arc::clone(&self.datenlordfs);

        future_into_py(py, async move {
            let result = match find_parent_attr(&path, fs.clone()).await {
                Ok((_, attr)) => {
                    // Get current dir or file name from path and find current inode
                    let path_components: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
                    fs.lookup(0, 0, attr.ino, path_components.last().unwrap()).await
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
            let (_, parent_attr) = match find_parent_attr(&path_str, fs.clone()).await {
                Ok(attr) => attr,
                Err(e) => return Err(PyException::new_err(format!("mkdir failed: {:?}", e))),
            };

            // File name
            let path_components: Vec<&str> = path_str.split('/').filter(|s| !s.is_empty()).collect();

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
    fn py_deldir<'a>(&'a self, py: Python<'a>, path: &'a str, recursive: bool) -> PyResult<Bound<PyAny>> {
        let fs = Arc::clone(&self.datenlordfs);
        let path_str = path.to_string();

        future_into_py(py, async move {
            match recursive_delete_dir(fs.clone(), &path_str, recursive).await {
                Ok(_) => Ok(()),
                Err(e) => Err(PyException::new_err(format!("deldir failed: {:?}", e))),
            }
        })
    }

    #[pyo3(name = "copy_from_local_file")]
    fn py_copy_from_local_file<'a>(&'a self, py: Python<'a>, local_path: &'a str, dest_path: &'a str, overwrite: bool) -> PyResult<Bound<PyAny>> {
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

                    let path_components: Vec<&str> = dest_path_str.split('/').filter(|s| !s.is_empty()).collect();
                    match find_parent_attr(&dest_path_str, fs.clone()).await {
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
                                    match fs.open(0, 0, file_attr.ino, OFlag::O_WRONLY.bits() as u32).await {
                                        Ok(fh) => {
                                            match fs.write(file_attr.ino, fh, 0, &buffer, 0).await {
                                                Ok(_) => Ok(()),
                                                Err(e) => Err(PyException::new_err(format!("write failed: {:?}", e))),
                                            }
                                        }
                                        Err(e) => Err(PyException::new_err(format!("open failed: {:?}", e))),
                                    }
                                }
                                Err(e) => Err(PyException::new_err(format!("mknod failed: {:?}", e))),
                            }
                        }
                        Err(e) => Err(PyException::new_err(format!("find_parent_attr failed: {:?}", e))),
                    }
                }
                Err(e) => Err(PyException::new_err(format!("File open error: {:?}", e))),
            }
        })
    }

    #[pyo3(name = "copy_to_local_file")]
    fn py_copy_to_local_file<'a>(&'a self, py: Python<'a>, src_path: &'a str, local_path: &'a str) -> PyResult<Bound<PyAny>> {
        let fs = Arc::clone(&self.datenlordfs);
        let src_path_str = src_path.to_string();
        let local_path_str = local_path.to_string();

        future_into_py(py, async move {
            match find_parent_attr(&src_path_str, fs.clone()).await {
                Ok((_, attr)) => {
                    let path_components: Vec<&str> = src_path_str.split('/').filter(|s| !s.is_empty()).collect();
                    let (_, file_attr, _) = fs.lookup(0, 0, attr.ino, path_components.last().unwrap()).await.unwrap();

                    match fs.open(0, 0, file_attr.ino, OFlag::O_RDONLY.bits() as u32).await {
                        Ok(fh) => {
                            let mut buffer = BytesMut::with_capacity(file_attr.size as usize);
                            match fs.read(file_attr.ino, fh, 0, buffer.capacity() as u32, &mut buffer).await {
                                Ok(read_size) => {
                                    let mut local_file = std::fs::File::create(local_path_str)?;
                                    local_file.write_all(&buffer[..read_size])?;
                                    fs.release(file_attr.ino, fh, 0, 0, true).await.map_err(
                                        |e| PyException::new_err(format!("Failed to release file handle: {:?}", e)),
                                    )
                                }
                                Err(e) => Err(PyException::new_err(format!("read failed: {:?}", e))),
                            }
                        }
                        Err(e) => Err(PyException::new_err(format!("open failed: {:?}", e))),
                    }
                }
                Err(e) => Err(PyException::new_err(format!("find_parent_attr failed: {:?}", e))),
            }
        })
    }

    /// 创建文件
    #[pyo3(name = "create_file")]
    fn py_create_file<'a>(&'a self, py: Python<'a>, file_path: &'a str) -> PyResult<Bound<PyAny>> {
        let fs = Arc::clone(&self.datenlordfs);
        let file_path_str = file_path.to_string();

        future_into_py(py, async move {
            match find_parent_attr(&file_path_str, fs.clone()).await {
                Ok((_, attr)) => {
                    let path_components: Vec<&str> = file_path_str.split('/').filter(|s| !s.is_empty()).collect();
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
                Err(e) => Err(PyException::new_err(format!("find_parent_attr failed: {:?}", e))),
            }
        })
    }

    #[pyo3(name = "rename_path")]
    fn py_rename_path<'a>(&'a self, py: Python<'a>, src_path: &'a str, dest_path: &'a str) -> PyResult<Bound<PyAny>> {
        let fs = Arc::clone(&self.datenlordfs);
        let src_path_str = src_path.to_string();
        let dest_path_str = dest_path.to_string();

        future_into_py(py, async move {
            let param = RenameParam {
                old_parent: ROOT_ID,
                old_name: src_path_str.clone(),
                new_parent: ROOT_ID,
                new_name: dest_path_str.clone(),
                flags: 0,
            };

            match fs.rename(0, 0, param).await {
                Ok(_) => Ok(()),
                Err(e) => Err(PyException::new_err(format!("rename failed: {:?}", e))),
            }
        })
    }

    #[pyo3(name = "stat")]
    fn py_stat<'a>(&'a self, py: Python<'a>, file_path: &'a str) -> PyResult<Bound<PyAny>> {
        let fs = Arc::clone(&self.datenlordfs);
        let file_path_str = file_path.to_string();

        future_into_py(py, async move {
            match find_parent_attr(&file_path_str, fs.clone()).await {
                Ok((_, parent_attr)) => {
                    let path_components: Vec<&str> = file_path_str.split('/').filter(|s| !s.is_empty()).collect();
                    match fs.lookup(0, 0, parent_attr.ino, path_components.last().unwrap()).await {
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
                Err(e) => Err(PyException::new_err(format!("find_parent_attr failed: {:?}", e))),
            }
        })
    }

    #[pyo3(name = "write_file")]
    fn py_write_file<'a>(&'a self, py: Python<'a>, file_path: &'a str, content: &'a [u8]) -> PyResult<Bound<PyAny>> {
    let fs = Arc::clone(&self.datenlordfs);
    let file_path_str = file_path.to_string();
    let data = content.to_vec();

    future_into_py(py, async move {
        match find_parent_attr(&file_path_str, fs.clone()).await {
            Ok((_, parent_attr)) => {
                let path_components: Vec<&str> = file_path_str.split('/').filter(|s| !s.is_empty()).collect();
                let file_name = path_components.last().unwrap();

                match fs.lookup(0, 0, parent_attr.ino, file_name).await {
                    Ok((_, file_attr, _)) => {
                        match fs.open(0, 0, file_attr.ino, OFlag::O_WRONLY.bits() as u32).await {
                            Ok(fh) => {
                                match fs.write(file_attr.ino, fh, 0, &data, 0).await {
                                    Ok(_) => {
                                        match fs.release(file_attr.ino, fh, 0, 0, true).await {
                                            Ok(_) => Ok(()),
                                            Err(e) => Err(PyException::new_err(format!("Failed to release file handle: {:?}", e))),
                                        }
                                    }
                                    Err(e) => Err(PyException::new_err(format!("write failed: {:?}", e))),
                                }
                            }
                            Err(e) => Err(PyException::new_err(format!("open failed: {:?}", e))),
                        }
                    }
                    Err(e) => Err(PyException::new_err(format!("lookup failed: {:?}", e))),
                }
            }
            Err(e) => Err(PyException::new_err(format!("find_parent_attr failed: {:?}", e))),
        }
    })
}

}

async fn find_parent_attr(
    path: &str,
    fs: Arc<DatenLordFs<S3MetaData>>,
) -> DatenLordResult<(Duration, FileAttr)> {
    let path_components: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    let mut current_inode = ROOT_ID;
    for component in path_components {
        match fs.lookup(0, 0, current_inode, component).await {
            Ok((_, attr, _)) => {
                current_inode = attr.ino;
            }
            Err(e) => return Err(e),
        }
    }
    fs.getattr(current_inode).await
}

async fn recursive_delete_dir(
    fs: Arc<DatenLordFs<S3MetaData>>,
    dir_path: &str,
    recursive: bool,
) -> DatenLordResult<()> {
    let mut dir_stack = VecDeque::new();
    dir_stack.push_back(dir_path.to_string());

    while let Some(current_dir_path) = dir_stack.pop_back() {
        let (_, parent_attr) = find_parent_attr(&current_dir_path, fs.clone()).await?;
        let path_components: Vec<&str> = current_dir_path.split('/').filter(|s| !s.is_empty()).collect();
        let (_, dir_attr, _) = fs.lookup(0, 0, parent_attr.ino, path_components.last().unwrap()).await?;

        let dir_handle = fs.opendir(0, 0, dir_attr.ino, OFlag::O_RDWR.bits() as u32).await?;

        let entries = fs.readdir(0, 0, ROOT_ID, dir_handle, 0).await?;
        for entry in entries {
            let entry_path = format!("{}/{}", current_dir_path, entry.name());
            if entry.file_type() == datenlord::fs::datenlordfs::direntry::FileType::Dir {
                dir_stack.push_back(entry_path);
            } else {
                fs.unlink(0, 0, ROOT_ID, &entry_path).await?;
            }
        }

        fs.releasedir(dir_attr.ino, dir_handle, 0).await?;
        fs.rmdir(0, 0, dir_attr.ino, path_components.last().unwrap()).await?;

        if !recursive {
            break;
        }
    }
    Ok(())
}
