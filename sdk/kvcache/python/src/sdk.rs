use datenlord::common::logger::{self, init_logger};
use datenlord::common::task_manager::TASK_MANAGER;
use datenlord::distribute_kv_cache::cluster::cluster_manager::ClusterManager;
use datenlord::distribute_kv_cache::cluster::node::Node;
use datenlord::distribute_kv_cache::kvclient::DistributeKVCacheClient;
use datenlord::fs::kv_engine::etcd_impl::EtcdKVEngine;
use datenlord::fs::kv_engine::{KVEngine, KVEngineType};
use pyo3::exceptions::PyException;
use pyo3::types::{PyBytes, PyBytesMethods};
use pyo3::{pyclass, pymethods, Bound, IntoPy, PyAny, PyRef, PyResult, Python};
use pyo3_asyncio::tokio::future_into_py;
use std::sync::Arc;
use tracing::info;

use crate::utils::Buffer;

/// `DatenLordSDK` is a Python class that provides an interface to interact with the DatenLord filesystem.
#[pyclass]
pub struct DatenLordSDK {
    /// The `DistributeKVCacheClient` instance that represents the filesystem.
    datenlord_client: Arc<DistributeKVCacheClient<u32>>,
}

#[pymethods]
impl DatenLordSDK {
    /// Initializes a new instance of `DatenLordSDK` by loading the configuration from the specified path
    #[new]
    pub fn new(_py: Python, block_size: u64, kv_engine_address: Vec<String>, log_level: String) -> PyResult<Self> {
        // let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        init_logger(logger::LogRole::SDK, log_level.parse().map_err(
            |e| PyException::new_err(format!("Invalid log level: {:?}", e)),
        )?);
        println!("block_size: {} kv_engine_address: {:?} log_level: {}", block_size, kv_engine_address, log_level);
        info!("block_size: {} kv_engine_address: {:?} log_level: {}", block_size, kv_engine_address, log_level);

        // Initialize the runtime
        // let datenlord_client = runtime.handle().block_on(async {
        pyo3_asyncio::tokio::get_runtime().handle().block_on(async {
            // TASK_MANAGER.;
            info!("Starting DatenLord SDK");
        });

        // let datenlordfs = runtime.handle().block_on(async {
        let datenlord_client = pyo3_asyncio::tokio::get_runtime().handle().block_on(async {
            let kv_engine = match KVEngineType::new(kv_engine_address.clone()).await {
                Ok(kv_engine) => kv_engine,
                Err(e) => {
                    panic!("Failed to create KVEngine: {:?}", e);
                }
            };

            let kv_engine: Arc<EtcdKVEngine> = Arc::new(kv_engine);
            let node = Node::default();
            let cluster_manager = Arc::new(ClusterManager::new(kv_engine, node));

            let kvcacheclient: Arc<DistributeKVCacheClient<u32>> = Arc::new(DistributeKVCacheClient::new(cluster_manager, block_size));
            let kvcacheclient_clone = Arc::clone(&kvcacheclient);
            match kvcacheclient_clone.start_watch().await {
                Ok(()) => {
                    info!("DistributeKVCacheClient start_watch ok");
                }
                Err(e) => {
                    panic!("start_watch failed: {:?}", e);
                }
            }

            kvcacheclient
        });

        Ok(DatenLordSDK {
            datenlord_client: datenlord_client,
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

    /// Support async context manager
    fn __aenter__<'a>(slf: PyRef<'a, Self>, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let slf = slf.into_py(py);
        future_into_py(py, async move { Ok(slf) })
    }

    /// Support async context manager
    fn __aexit__<'a>(
        &'a mut self,
        py: Python<'a>,
        _exc_type: &Bound<'a, PyAny>,
        _exc_value: &Bound<'a, PyAny>,
        _traceback: &Bound<'a, PyAny>,
    ) -> PyResult<Bound<'a, PyAny>> {
        self.py_close(py)
    }

    /// Try to match a kv cache block
    /// Return matched_prefix if the block is found, otherwise return None.
    /// matched_prefix is the longest prefix of the key that matches the block.
    /// buffer is the value of the block.
    #[pyo3(name = "match_prefix")]
    fn py_match_prefix<'a>(
        &'a self,
        py: Python<'a>,
        prefix: Vec<u32>,
    ) -> PyResult<Bound<PyAny>> {
        let datenlord_client = Arc::clone(&self.datenlord_client);

        future_into_py(py, async move {
            match datenlord_client.match_prefix(prefix).await {
                Ok(prefix) => {
                    Ok(prefix)
                }
                Err(e) => Err(PyException::new_err(format!("try_load failed: {:?}", e))),
            }
        })
    }

    /// Try to match a kv cache block, sync version
    /// Return matched_prefix if the block is found, otherwise return None.
    /// matched_prefix is the longest prefix of the key that matches the block.
    /// buffer is the value of the block.
    #[pyo3(name = "match_prefix_sync")]
    fn py_match_prefix_sync<'a>(
        &'a self,
        _py: Python<'a>,
        prefix: Vec<u32>,
    ) -> PyResult<Vec<u32>> {
        let datenlord_client = Arc::clone(&self.datenlord_client);

        let match_prefix_result = pyo3_asyncio::tokio::get_runtime().handle().block_on(async {
            match datenlord_client.match_prefix(prefix).await {
                Ok(prefix) => {
                    Ok(prefix)
                }
                Err(e) => Err(PyException::new_err(format!("try_load failed: {:?}", e))),
            }
        });

        match match_prefix_result {
            Ok(prefix) => Ok(prefix),
            Err(e) => Err(e),
        }
    }

    /// Try to load a kv cache block
    /// Return (matched_prefix, buffer) if the block is found, otherwise return None.
    /// matched_prefix is the longest prefix of the key that matches the block.
    /// buffer is the value of the block.
    #[pyo3(name = "try_load")]
    fn py_try_load<'a>(
        &'a self,
        py: Python<'a>,
        prefix: Vec<u32>,
    ) -> PyResult<Bound<PyAny>> {
        let datenlord_client = Arc::clone(&self.datenlord_client);

        future_into_py(py, async move {
            match datenlord_client.try_load(prefix).await {
                Ok((prefix, data)) => {
                    // TODO: return buffer here.
                    Ok((prefix, Buffer::new(data)))
                    // Ok((prefix, Buffer::new(data.to_vec())))
                    // Ok((prefix, Buffer::new(data.try_into().unwrap())))
                }
                Err(e) => Err(PyException::new_err(format!("try_load failed: {:?}", e))),
            }
        })
    }

    /// Try to load a kv cache block, sync version
    /// Return (matched_prefix, buffer) if the block is found, otherwise return None.
    /// matched_prefix is the longest prefix of the key that matches the block.
    /// buffer is the value of the block.
    #[pyo3(name = "try_load_sync")]
    fn py_try_load_sync<'a>(
        &'a self,
        _py: Python<'a>,
        prefix: Vec<u32>,
    ) -> PyResult<(Vec<u32>, Buffer)> {
        let datenlord_client = Arc::clone(&self.datenlord_client);

        let try_load_result = pyo3_asyncio::tokio::get_runtime().handle().block_on(async {
            match datenlord_client.try_load(prefix).await {
                Ok((prefix, data)) => {
                    // Ok((prefix, Buffer::new(data.to_vec())))
                    Ok((prefix, Buffer::new(data)))
                    // Ok((prefix, Buffer::new(data.try_into().unwrap())))
                }
                Err(e) => Err(PyException::new_err(format!("try_load failed: {:?}", e))),
            }
        });

        match try_load_result {
            Ok((prefix, data)) => Ok((prefix, data)),
            Err(e) => Err(e),
        }
    }

    /// Insert a block to the distributed kv cache
    /// The `key` is the key of the block to be inserted.
    /// The `data` is the value of the block to be inserted.
    #[pyo3(name = "insert")]
    fn py_insert<'a>(
        &'a self,
        py: Python<'a>,
        key: Vec<u32>,
        data: &Bound<PyBytes>,
    ) -> PyResult<Bound<PyAny>> {
        let datenlord_client = Arc::clone(&self.datenlord_client);
        let data = data.as_bytes().to_owned();

        future_into_py(py, async move {
            match datenlord_client.insert(key, data).await {
                Ok(_) => Ok(()),
                Err(e) => Err(PyException::new_err(format!("insert failed: {:?}", e))),
            }
        })
    }

    /// Insert a block to the distributed kv cache, sync version
    /// The `key` is the key of the block to be inserted.
    /// The `data` is the value of the block to be inserted.
    #[pyo3(name = "insert_sync")]
    fn py_insert_sync<'a>(
        &'a self,
        _py: Python<'a>,
        key: Vec<u32>,
        data: Vec<u8>,
    ) -> PyResult<()> {
        let datenlord_client = Arc::clone(&self.datenlord_client);

        let insert_result = pyo3_asyncio::tokio::get_runtime().handle().block_on(async {
            match datenlord_client.insert(key, data).await {
                Ok(_) => Ok(()),
                Err(e) => Err(PyException::new_err(format!("insert failed: {:?}", e))),
            }
        });

        match insert_result {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
}