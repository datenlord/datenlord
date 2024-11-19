use clippy_utilities::Cast;
use datenlord::common::logger::init_logger;
use datenlord::common::task_manager::TASK_MANAGER;
use datenlord::config::{self, InnerConfig};
use datenlord::distribute_kv_cache::cluster::cluster_manager::ClusterManager;
use datenlord::distribute_kv_cache::cluster::node::{Node, NodeStatus};
use datenlord::distribute_kv_cache::kvclient::DistributeKVCacheClient;
use datenlord::fs::kv_engine::etcd_impl::EtcdKVEngine;
use datenlord::fs::kv_engine::{KVEngine, KVEngineType};
use pyo3::exceptions::PyException;
use pyo3::types::{PyBytes, PyBytesMethods};
use pyo3::{pyclass, pymethods, Bound, IntoPy, PyAny, PyRef, PyResult, Python};
use pyo3_asyncio::tokio::future_into_py;
use std::sync::Arc;
use tracing::info;

/// `DatenLordSDK` is a Python class that provides an interface to interact with the DatenLord filesystem.
#[pyclass]
pub struct DatenLordSDK {
    /// The `DistributeKVCacheClient` instance that represents the filesystem.
    datenlord_client: Arc<DistributeKVCacheClient>,
}

#[pymethods]
impl DatenLordSDK {
    /// Initializes a new instance of `DatenLordSDK` by loading the configuration from the specified path
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
        // let datenlord_client = runtime.handle().block_on(async {
        pyo3_asyncio::tokio::get_runtime().handle().block_on(async {
            // TASK_MANAGER.;
            info!("Starting DatenLord SDK");
        });

        // let datenlordfs = runtime.handle().block_on(async {
        let datenlord_client = pyo3_asyncio::tokio::get_runtime().handle().block_on(async {
            let kv_engine: Arc<EtcdKVEngine> =
                Arc::new(KVEngineType::new(config.kv_addrs.clone()).await.unwrap());
            let node = Node::new(config.node_ip.to_string(), config.server_port, 1, NodeStatus::Initializing);
            let cluster_manager = Arc::new(ClusterManager::new(kv_engine, node));

            let kvcacheclient = Arc::new(DistributeKVCacheClient::new(cluster_manager, config.storage.block_size.cast()));
            let kvcacheclient_clone = Arc::clone(&kvcacheclient);
            tokio::spawn(async move {
                match kvcacheclient_clone.start_watch().await {
                    Ok(()) => {
                        info!("DistributeKVCacheClient start_watch finished");
                    }
                    Err(e) => {
                        panic!("start_watch failed: {:?}", e);
                    }
                }
            });

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

    /// Try to load a kv cache block
    /// Return (matched_prefix, buffer) if the block is found, otherwise return None.
    /// matched_prefix is the longest prefix of the key that matches the block.
    /// buffer is the value of the block.
    #[pyo3(name = "try_load")]
    fn py_try_load<'a>(
        &'a self,
        py: Python<'a>,
        prefix: String,
    ) -> PyResult<Bound<PyAny>> {
        let datenlord_client = Arc::clone(&self.datenlord_client);

        future_into_py(py, async move {
            match datenlord_client.try_load(prefix).await {
                Ok(value) => Ok(value),
                Err(e) => Err(PyException::new_err(format!("try_load failed: {:?}", e))),
            }
        })
    }

    /// Insert a block to the distributed kv cache
    /// The `key` is the key of the block to be inserted.
    /// The `data` is the value of the block to be inserted.
    #[pyo3(name = "insert")]
    fn py_insert<'a>(
        &'a self,
        py: Python<'a>,
        key: String,
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
}