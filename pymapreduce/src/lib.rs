#![allow(non_local_definitions)]
mod ipc;
use pyo3::prelude::*;
use std::time::Duration;
use mapreduce::driver::config::{JobConfig as CoreJobConfig, TaskInput as CoreTaskInput};
use mapreduce::types::task::TaskKind as CoreTaskKind;

#[pyclass]
#[derive(Clone, PartialEq)]
pub enum TaskKind {
    Map,
    Reduce,
}

#[pymethods]
impl TaskKind {
    fn __eq__(&self, other: &Self) -> bool {
        self == other
    }
}

impl From<TaskKind> for CoreTaskKind {
    fn from(val: TaskKind) -> Self {
        match val {
            TaskKind::Map => CoreTaskKind::Map,
            TaskKind::Reduce => CoreTaskKind::Reduce,
        }
    }
}

#[pyclass]
#[derive(Clone, PartialEq)]
pub enum SchedulerStrategy {
    RoundRobin,
    WeightedCapacity,
    LeastLoad,
    LocalityFirst,
    Adaptive,
}

#[pymethods]
impl SchedulerStrategy {
    fn __eq__(&self, other: &Self) -> bool {
        self == other
    }
}

impl SchedulerStrategy {
    fn to_string_name(&self) -> String {
        match self {
            Self::RoundRobin => "RoundRobin".to_string(),
            Self::WeightedCapacity => "WeightedCapacity".to_string(),
            Self::LeastLoad => "LeastLoad".to_string(),
            Self::LocalityFirst => "LocalityFirst".to_string(),
            Self::Adaptive => "Adaptive".to_string(),
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct TaskInput {
    pub(crate) inner: CoreTaskInput,
}

#[pymethods]
impl TaskInput {
    #[new]
    fn new(py: Python<'_>, kind: TaskKind, payload: &PyAny) -> PyResult<Self> {
        let pickle = py.import("cloudpickle").or_else(|_| py.import("pickle"))?;
        let serialized = pickle.call_method1("dumps", (payload,))?;
        let bytes: Vec<u8> = serialized.extract()?;

        Ok(Self {
            inner: CoreTaskInput::new(kind.into(), bytes),
        })
    }

    #[staticmethod]
    fn new_actor_creation(py: Python<'_>, actor_id: String, class_name: String, payload: &PyAny) -> PyResult<Self> {
        let pickle = py.import("cloudpickle").or_else(|_| py.import("pickle"))?;
        let serialized = pickle.call_method1("dumps", (payload,))?;
        let bytes: Vec<u8> = serialized.extract()?;

        let actor_uuid = uuid::Uuid::parse_str(&actor_id).map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(Self {
            inner: CoreTaskInput::new(
                CoreTaskKind::ActorCreation { actor_id: actor_uuid, class_name },
                bytes
            ),
        })
    }

    #[staticmethod]
    fn new_actor_task(py: Python<'_>, actor_id: String, method_name: String, payload: &PyAny) -> PyResult<Self> {
        let pickle = py.import("cloudpickle").or_else(|_| py.import("pickle"))?;
        let serialized = pickle.call_method1("dumps", (payload,))?;
        let bytes: Vec<u8> = serialized.extract()?;

        let actor_uuid = uuid::Uuid::parse_str(&actor_id).map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(Self {
            inner: CoreTaskInput::new(
                CoreTaskKind::ActorTask { actor_id: actor_uuid, method_name },
                bytes
            ),
        })
    }

    #[staticmethod]
    fn new_actor_destruction(actor_id: String) -> PyResult<Self> {
        let actor_uuid = uuid::Uuid::parse_str(&actor_id).map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(Self {
            inner: CoreTaskInput::new(
                CoreTaskKind::ActorDestruction { actor_id: actor_uuid },
                Vec::new()
            ),
        })
    }

    fn with_dependencies(&mut self, deps: Vec<String>) -> PyResult<()> {
        let mut uuid_deps = Vec::new();
        for d in deps {
            let u = uuid::Uuid::parse_str(&d).map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            uuid_deps.push(u);
        }
        self.inner.dependencies = uuid_deps;
        Ok(())
    }
}

#[pyclass]
#[derive(Clone)]
pub struct JobConfig {
    pub(crate) inner: CoreJobConfig,
}

#[pymethods]
impl JobConfig {
    #[new]
    #[pyo3(signature = (name, max_time=None))]
    fn new(name: String, max_time: Option<u64>) -> Self {
        let mut inner = CoreJobConfig::new(name);
        if let Some(secs) = max_time {
            inner = inner.with_max_time(Duration::from_secs(secs));
        }
        Self { inner }
    }

    fn add_task(&mut self, task: TaskInput) {
        let new_inner = self.inner.clone().add_task(task.inner);
        self.inner = new_inner;
    }

    fn with_max_time(&mut self, seconds: u64) {
        let new_inner = self.inner.clone().with_max_time(Duration::from_secs(seconds));
        self.inner = new_inner;
    }
    
    fn with_runtime_env(&mut self, env_id: String) -> PyResult<()> {
        let uuid = uuid::Uuid::parse_str(&env_id)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        let new_inner = self.inner.clone().with_runtime_env(uuid);
        self.inner = new_inner;
        Ok(())
    }
    
    fn __repr__(&self) -> String {
        format!(
            "<JobConfig name='{}', tasks={}, max_time={:?}>", 
            self.inner.name, 
            self.inner.tasks.len(), 
            self.inner.max_time
        )
    }
}

use mapreduce::driver::Driver as CoreDriver;
use std::sync::Arc;

#[pyclass]
#[derive(Clone)]
pub struct Driver {
    inner: Arc<CoreDriver>,
}

#[pymethods]
impl Driver {
    #[staticmethod]
    #[pyo3(signature = (address=None, scheduler=None))]
    fn init<'py>(py: Python<'py>, address: Option<String>, scheduler: Option<SchedulerStrategy>) -> PyResult<&'py PyAny> {
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let addr_clone = address.clone();
            let strategy_name = scheduler.map(|s| s.to_string_name());
            
            // Call the core init which handles local daemon startup
            let _handle = CoreDriver::init(address, strategy_name)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            
            let connect_addr = addr_clone.unwrap_or_else(|| "127.0.0.1:7777".to_string());
            let driver = CoreDriver::connect(&connect_addr, None)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                
            let driver_wrapper = Driver {
                inner: Arc::new(driver),
            };
            
            Python::with_gil(|py| {
                let m = py.import("pymapreduce").unwrap();
                m.setattr("_global_driver", driver_wrapper.clone().into_py(py)).unwrap();
            });
                
            Ok(driver_wrapper)
        })
    }

    #[staticmethod]
    #[pyo3(signature = (head_node, workers=None))]
    fn connect<'py>(py: Python<'py>, head_node: String, workers: Option<Vec<String>>) -> PyResult<&'py PyAny> {
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let driver = CoreDriver::connect(&head_node, workers)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            let driver_wrapper = Driver {
                inner: Arc::new(driver),
            };
            
            Python::with_gil(|py| {
                let m = py.import("pymapreduce").unwrap();
                m.setattr("_global_driver", driver_wrapper.clone().into_py(py)).unwrap();
            });
            
            Ok(driver_wrapper)
        })
    }

    fn submit<'py>(&self, py: Python<'py>, config: JobConfig) -> PyResult<&'py PyAny> {
        let inner = self.inner.clone();
        let core_config = config.inner.clone();

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let handle = inner
                .submit(core_config)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            let results = handle
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            Python::with_gil(|py| -> PyResult<Vec<PyObject>> {
                let pickle = py.import("cloudpickle").or_else(|_| py.import("pickle"))?;
                let mut py_results = Vec::new();
                for res_bytes in results {
                    let bytes_obj = pyo3::types::PyBytes::new(py, &res_bytes);
                    let py_obj = match pickle.call_method1("loads", (bytes_obj,)) {
                        Ok(obj) => obj.to_object(py),
                        Err(_) => bytes_obj.to_object(py),
                    };
                    py_results.push(py_obj);
                }
                Ok(py_results)
            })
        })
    }

    fn put<'py>(&self, py: Python<'py>, object_id: String, data: Vec<u8>) -> PyResult<&'py PyAny> {
        let inner = self.inner.clone();
        
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let uuid = uuid::Uuid::parse_str(&object_id)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
                
            inner.put(uuid, data)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                
            Ok(())
        })
    }

    fn get<'py>(&self, py: Python<'py>, object: &'py PyAny) -> PyResult<&'py PyAny> {
        let object_id: String = if let Ok(oid) = object.getattr("object_id") {
            oid.extract::<String>()?
        } else {
            object.extract::<String>()?
        };
        let inner = self.inner.clone();
        
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let uuid = uuid::Uuid::parse_str(&object_id)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
                
            let data = inner.get(uuid, 0, u64::MAX)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                
            Python::with_gil(|py| -> PyResult<PyObject> {
                let pickle = py.import("cloudpickle").or_else(|_| py.import("pickle"))?;
                let bytes_obj = pyo3::types::PyBytes::new(py, &data);
                let py_obj = pickle.call_method1("loads", (bytes_obj,))?;
                Ok(py_obj.to_object(py))
            })
        })
    }
    fn _gc_object(&self, object_id: String) -> PyResult<()> {
        let inner = self.inner.clone();
        let rt = pyo3_asyncio::tokio::get_runtime();
        rt.spawn(async move {
            if let Ok(uuid) = uuid::Uuid::parse_str(&object_id) {
                let _ = inner.delete_object(uuid).await;
            }
        });
        Ok(())
    }

    fn get_job_status<'py>(&self, py: Python<'py>, job_id: String) -> PyResult<&'py PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let uuid = uuid::Uuid::parse_str(&job_id)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            let status = inner.get_job_status(uuid)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(status)
        })
    }

    fn shutdown<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let inner = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            inner.shutdown().await.map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(())
        })
    }
}

#[pyclass]
#[derive(Clone)]
pub struct WorkerHandle {
    inner: Arc<mapreduce::cluster::worker::WorkerNode>,
}

#[pymethods]
impl WorkerHandle {
    #[pyo3(signature = (graceful=true))]
    fn disconnect<'py>(&self, py: Python<'py>, graceful: bool) -> PyResult<&'py PyAny> {
        let worker = self.inner.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            worker.disconnect(graceful).await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(())
        })
    }

    fn id(&self) -> String {
        self.inner.id.to_string()
    }
}

use mapreduce::Executor;
use mapreduce::types::task::Task;
use mapreduce::types::error::ExecutorError;
use async_trait::async_trait;

use dashmap::DashMap;

use crate::ipc::IpcWorker;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Mutex;
use tokio::time::{sleep, Instant};

struct WorkerState {
    id: usize,
    worker: Mutex<IpcWorker>,
    env_id: Option<uuid::Uuid>,
    last_used: Mutex<Instant>,
}

#[allow(dead_code)]
pub struct PythonExecutor {
    workers: Arc<DashMap<usize, Arc<WorkerState>>>,
    actor_to_worker: Arc<DashMap<uuid::Uuid, usize>>,
    max_cpus: usize,
    next_worker_id: AtomicUsize,
}

impl PythonExecutor {
    pub fn new(max_cpus: usize, idle_timeout_mins: u32) -> Self {
        let workers: Arc<DashMap<usize, Arc<WorkerState>>> = Arc::new(DashMap::new());
        let actor_to_worker = Arc::new(DashMap::new());
        
        if idle_timeout_mins > 0 {
            let workers_clone = workers.clone();
            tokio::spawn(async move {
                let timeout = std::time::Duration::from_secs((idle_timeout_mins as u64) * 60);
                loop {
                    sleep(std::time::Duration::from_secs(60)).await;
                    let now = Instant::now();
                    let mut to_remove = Vec::new();
                    
                    for entry in workers_clone.iter() {
                        let state = entry.value();
                        if let Ok(_lock) = state.worker.try_lock() {
                            let last = *state.last_used.lock().await;
                            if now.duration_since(last) > timeout {
                                to_remove.push(state.id);
                            }
                        }
                    }
                    
                    for id in to_remove {
                        workers_clone.remove(&id);
                    }
                }
            });
        }
        
        Self {
            workers,
            actor_to_worker,
            max_cpus,
            next_worker_id: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl Executor for PythonExecutor {
    async fn execute(&self, task: &Task, store: Arc<mapreduce::object_store::store::ObjectStore>) -> Result<(Vec<u8>, Option<uuid::Uuid>), ExecutorError> {
        let env_id = task.runtime_env;
        
        let mut resolved_deps = std::collections::HashMap::new();
        let mut dep_shm_paths = std::collections::HashMap::new();

        for dep in &task.dependencies {
            if let Ok(shm_path) = store.ensure_shm_path(*dep).await {
                dep_shm_paths.insert(dep.to_string(), shm_path.to_string_lossy().to_string());
            } else if let Ok(data) = store.get_any(*dep).await {
                resolved_deps.insert(dep.to_string(), data.to_vec());
            }
        }
        
        let worker_state = match &task.kind {
            mapreduce::types::task::TaskKind::ActorTask { actor_id, .. } => {
                if let Some(w_id) = self.actor_to_worker.get(actor_id) {
                    if let Some(w) = self.workers.get(&w_id) {
                        w.clone()
                    } else {
                        let w = self.get_or_spawn_worker(env_id).await?;
                        self.actor_to_worker.insert(*actor_id, w.id);
                        w
                    }
                } else {
                    let w = self.get_or_spawn_worker(env_id).await?;
                    self.actor_to_worker.insert(*actor_id, w.id);
                    w
                }
            },
            mapreduce::types::task::TaskKind::ActorDestruction { actor_id } => {
                if let Some(w_id) = self.actor_to_worker.get(actor_id) {
                    if let Some(w) = self.workers.get(&w_id) {
                        w.clone()
                    } else {
                        self.get_or_spawn_worker(env_id).await?
                    }
                } else {
                    self.get_or_spawn_worker(env_id).await?
                }
            },
            _ => {
                self.get_or_spawn_worker(env_id).await?
            }
        };
        
        let result_bytes = {
            let mut worker_lock = worker_state.worker.lock().await;
            *worker_state.last_used.lock().await = Instant::now();
            
            let envelope_bytes = Python::with_gil(|py| -> Result<Vec<u8>, ExecutorError> {
                let pickle = py.import("cloudpickle").or_else(|_| py.import("pickle"))
                    .map_err(|e| ExecutorError::Failed(e.to_string()))?;
                let dict = pyo3::types::PyDict::new(py);
                
                match &task.kind {
                    mapreduce::types::task::TaskKind::Map | mapreduce::types::task::TaskKind::Reduce => {
                        dict.set_item("kind", "Map").unwrap();
                    },
                    mapreduce::types::task::TaskKind::ActorCreation { actor_id, .. } => {
                        dict.set_item("kind", "ActorCreation").unwrap();
                        dict.set_item("actor_id", actor_id.to_string()).unwrap();
                    },
                    mapreduce::types::task::TaskKind::ActorTask { actor_id, method_name } => {
                        dict.set_item("kind", "ActorTask").unwrap();
                        dict.set_item("actor_id", actor_id.to_string()).unwrap();
                        dict.set_item("method_name", method_name).unwrap();
                    },
                    mapreduce::types::task::TaskKind::ActorDestruction { actor_id } => {
                        dict.set_item("kind", "ActorDestruction").unwrap();
                        dict.set_item("actor_id", actor_id.to_string()).unwrap();
                    }
                }
                
                let py_payload = pyo3::types::PyBytes::new(py, &task.payload);
                dict.set_item("payload", py_payload).unwrap();
                
                // Pass fallback bytes
                let py_deps = pyo3::types::PyDict::new(py);
                for (k, v) in &resolved_deps {
                    let v_bytes = pyo3::types::PyBytes::new(py, v);
                    py_deps.set_item(k, v_bytes).unwrap();
                }
                dict.set_item("deps", py_deps).unwrap();

                // Pass shared memory paths for zero-copy loading
                let py_dep_shm = pyo3::types::PyDict::new(py);
                for (k, v) in &dep_shm_paths {
                    py_dep_shm.set_item(k, v).unwrap();
                }
                dict.set_item("dep_shm", py_dep_shm).unwrap();
                
                let dumps = pickle.call_method1("dumps", (dict,)).map_err(|e| ExecutorError::Failed(e.to_string()))?;
                dumps.extract().map_err(|e| ExecutorError::Failed(e.to_string()))
            })?;
            
            match worker_lock.send_and_receive(envelope_bytes).await {
                Ok(res) => res,
                Err(crate::ipc::IpcError::AppError(e)) => {
                    return Err(ExecutorError::Failed(format!("App Error: {}", e)));
                }
                Err(crate::ipc::IpcError::IoError(e)) => {
                    // BROKEN PIPE! Evict this worker!
                    self.workers.remove(&worker_state.id);
                    return Err(ExecutorError::Failed(format!("IPC Crash (OOM or Segfault): {}", e)));
                }
            }
        };
        
        if let mapreduce::types::task::TaskKind::ActorCreation { actor_id, .. } = &task.kind {
            self.actor_to_worker.insert(*actor_id, worker_state.id);
        } else if let mapreduce::types::task::TaskKind::ActorDestruction { actor_id } = &task.kind {
            self.actor_to_worker.remove(actor_id);
        }
        
        let object_id = uuid::Uuid::new_v4();
        store.put(object_id, bytes::Bytes::from(result_bytes))
            .await
            .map_err(|e| ExecutorError::Failed(format!("Store put failed: {:?}", e)))?;
            
        let serialized_ref = Python::with_gil(|py| -> Result<Vec<u8>, ExecutorError> {
            let pickle = py.import("cloudpickle").or_else(|_| py.import("pickle"))
                .map_err(|e| ExecutorError::Failed(e.to_string()))?;
            let tuple = pyo3::types::PyTuple::new(py, ["__ObjectRef__", &object_id.to_string()]);
            let bytes_obj = pickle.call_method1("dumps", (tuple,))
                .map_err(|e| ExecutorError::Failed(e.to_string()))?;
            bytes_obj.extract().map_err(|e| ExecutorError::Failed(e.to_string()))
        })?;
        
        Ok((serialized_ref, Some(object_id)))
    }
}

impl PythonExecutor {
    async fn get_or_spawn_worker(&self, env_id: Option<uuid::Uuid>) -> Result<Arc<WorkerState>, ExecutorError> {
        loop {
            for entry in self.workers.iter() {
                let state = entry.value();
                if state.env_id == env_id {
                    if let Ok(_lock) = state.worker.try_lock() {
                        return Ok(state.clone());
                    }
                }
            }
            
            if self.workers.len() < self.max_cpus {
                let id = self.next_worker_id.fetch_add(1, Ordering::SeqCst);
                let worker = IpcWorker::spawn(env_id).await.map_err(|e| ExecutorError::Failed(e))?;
                let state = Arc::new(WorkerState {
                    id,
                    worker: Mutex::new(worker),
                    env_id,
                    last_used: Mutex::new(Instant::now()),
                });
                self.workers.insert(id, state.clone());
                return Ok(state);
            }
            
            let mut best_evict: Option<usize> = None;
            let mut oldest_time = Instant::now();
            
            for entry in self.workers.iter() {
                let state = entry.value();
                if state.env_id != env_id {
                    if let Ok(_lock) = state.worker.try_lock() {
                        let t = *state.last_used.lock().await;
                        if t < oldest_time {
                            oldest_time = t;
                            best_evict = Some(state.id);
                        }
                    }
                }
            }
            
            if let Some(evict_id) = best_evict {
                self.workers.remove(&evict_id);
                continue;
            }
            
            if let Some(state) = self.workers.iter().find(|e| e.value().env_id == env_id).map(|e| e.value().clone()) {
                return Ok(state);
            }
            
            sleep(Duration::from_millis(50)).await;
        }
    }
}

#[pyfunction]
#[pyo3(signature = (head_addr, workers=None, idle_timeout=0))]
fn start_worker<'py>(py: Python<'py>, head_addr: String, workers: Option<usize>, idle_timeout: u32) -> PyResult<&'py PyAny> {
    pyo3_asyncio::tokio::future_into_py(py, async move {
        use mapreduce::cluster::worker::WorkerNode;
        use mapreduce::types::node::NodeCapacity;
        
        let cpus = workers.unwrap_or_else(|| num_cpus::get());
        let capacity = NodeCapacity {
            physical_cpus: cpus,
            ..Default::default()
        };
        
        let executor = Arc::new(PythonExecutor::new(cpus, idle_timeout));
        let worker = WorkerNode::new(capacity, executor, head_addr);
        
        match worker.run().await {
            Ok(_) => Ok(()),
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("Connection reset by peer")
                    || err_str.contains("Broken pipe")
                    || err_str.contains("os error 104")
                    || err_str.contains("os error 32")
                    || err_str.contains("Connection refused") {
                    Ok(())
                } else {
                    Err(pyo3::exceptions::PyRuntimeError::new_err(err_str))
                }
            }
        }
    })
}

#[pyfunction]
#[pyo3(signature = (head_addr, cpus=None, idle_timeout=0))]
fn connect_worker<'py>(py: Python<'py>, head_addr: String, cpus: Option<usize>, idle_timeout: u32) -> PyResult<&'py PyAny> {
    pyo3_asyncio::tokio::future_into_py(py, async move {
        use mapreduce::cluster::worker::WorkerNode;
        use mapreduce::types::node::NodeCapacity;

        let actual_cpus = cpus.unwrap_or_else(|| num_cpus::get());
        let capacity = NodeCapacity {
            physical_cpus: actual_cpus,
            ..Default::default()
        };

        let executor = Arc::new(PythonExecutor::new(actual_cpus, idle_timeout));
        let worker = Arc::new(WorkerNode::new(capacity, executor, head_addr));
        let worker_clone = worker.clone();

        tokio::spawn(async move {
            let _ = worker_clone.run().await;
        });

        Ok(WorkerHandle { inner: worker })
    })
}

#[pymodule]
fn _pymapreduce(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<TaskKind>()?;
    m.add_class::<SchedulerStrategy>()?;
    m.add_class::<TaskInput>()?;
    m.add_class::<JobConfig>()?;
    m.add_class::<Driver>()?;
    m.add_class::<WorkerHandle>()?;
    m.add_function(wrap_pyfunction!(start_worker, m)?)?;
    m.add_function(wrap_pyfunction!(connect_worker, m)?)?;
    
    m.add("_global_driver", py.None())?;
    m.add("_global_runtime_env_id", py.None())?;

    let code = r###"
import os
import sys
import inspect
import uuid
import fnmatch
import atexit

if "PYTHON_EXECUTABLE" not in os.environ:
    os.environ["PYTHON_EXECUTABLE"] = sys.executable

def _auto_cleanup():
    try:
        import pymapreduce
        driver = getattr(pymapreduce, "_global_driver", None)
        env_id = getattr(pymapreduce, "_global_runtime_env_id", None)
        if driver is not None and env_id is not None:
            driver._gc_object(env_id)
    except Exception:
        pass

atexit.register(_auto_cleanup)

async def _async_gc(driver, object_id):
    try:
        await driver._gc_object(object_id)
    except Exception:
        pass

class ObjectRef:
    def __init__(self, object_id, pinned=False):
        self.object_id = object_id
        self._pinned = pinned

    def pin(self):
        """Pin this object reference to prevent garbage collection."""
        self._pinned = True
        return self

    def unpin(self):
        """Unpin this object reference so it can be garbage collected when out of scope."""
        self._pinned = False
        return self

    async def get(self):
        """Fetch the value referenced by this ObjectRef."""
        import pymapreduce
        driver = getattr(pymapreduce, "_global_driver", None)
        if driver is None:
            raise RuntimeError("Driver is not initialized.")
        return await driver.get(self.object_id)

    def __del__(self):
        try:
            if getattr(self, "_pinned", False):
                return  # Do not collect pinned objects

            import pymapreduce
            driver = getattr(pymapreduce, "_global_driver", None)
            if driver is not None:
                driver._gc_object(self.object_id)
        except Exception:
            pass

def _set_global_driver(driver):
    import pymapreduce
    pymapreduce._global_driver = driver

def _set_global_runtime_env(env_id):
    import pymapreduce
    pymapreduce._global_runtime_env_id = env_id

def _rebuild_object_ref(object_id):
    import pymapreduce
    return pymapreduce.ObjectRef(object_id)

class DriverWrapper:
    @staticmethod
    async def init(address=None, runtime_env=None, scheduler=None):
        import pymapreduce
        d = await pymapreduce._original_init(address, scheduler)
        _set_global_driver(d)
        await _process_runtime_env(d, runtime_env)
        return d

    @staticmethod
    async def connect(head_node, workers=None, runtime_env=None):
        import pymapreduce
        d = await pymapreduce._original_connect(head_node, workers)
        _set_global_driver(d)
        await _process_runtime_env(d, runtime_env)
        return d

DEFAULT_EXCLUDES = [
    ".git", ".github", ".gitignore", ".gitattributes",
    "__pycache__", "*.pyc", "*.pyo", "*.pyd",
    ".venv", "venv", "env", ".env", "virtualenv", "conda-env",
    "target", "build", "dist", "*.egg-info", "*.egg",
    ".pytest_cache", ".mypy_cache", ".ruff_cache", ".coverage", "htmlcov",
    ".idea", ".vscode", "*.swp", "*~", ".DS_Store", "Thumbs.db",
    "node_modules",
    "*.log", "*.tmp", "*.temp"
]

def _parse_gitignore(working_dir):
    import os
    gitignore_path = os.path.join(working_dir, ".gitignore")
    patterns = []
    if os.path.isfile(gitignore_path):
        try:
            with open(gitignore_path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        patterns.append(line.rstrip("/"))
        except Exception:
            pass
    return patterns

def _should_exclude(rel_path, name, patterns):
    import fnmatch
    rel_path_norm = rel_path.replace("\\", "/").strip("/")
    name_norm = name.replace("\\", "/").strip("/")
    
    for pat in patterns:
        pat_clean = pat.replace("\\", "/").strip("/")
        if not pat_clean:
            continue
            
        if fnmatch.fnmatch(name_norm, pat_clean):
            return True
        if fnmatch.fnmatch(rel_path_norm, pat_clean) or fnmatch.fnmatch(rel_path_norm, f"*/{pat_clean}"):
            return True
        if rel_path_norm == pat_clean or rel_path_norm.startswith(f"{pat_clean}/"):
            return True
    return False

async def _process_runtime_env(driver, runtime_env):
    if runtime_env and "working_dir" in runtime_env:
        import os
        import zipfile
        import io
        import uuid
        
        working_dir = os.path.abspath(runtime_env["working_dir"])
        if not os.path.isdir(working_dir):
            raise ValueError(f"working_dir does not exist or is not a directory: {working_dir}")
            
        custom_excludes = runtime_env.get("excludes", [])
        if isinstance(custom_excludes, str):
            custom_excludes = [custom_excludes]
            
        gitignore_patterns = _parse_gitignore(working_dir)
        all_exclude_patterns = DEFAULT_EXCLUDES + gitignore_patterns + list(custom_excludes)
        
        mem_zip = io.BytesIO()
        with zipfile.ZipFile(mem_zip, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            for root, dirs, files in os.walk(working_dir):
                rel_root = os.path.relpath(root, working_dir)
                if rel_root == ".":
                    rel_root = ""
                    
                # Prune excluded directories in-place to avoid traversing into heavy trees (.venv, .git, target, etc.)
                dirs[:] = [
                    d for d in dirs
                    if not _should_exclude(
                        os.path.join(rel_root, d) if rel_root else d, 
                        d, 
                        all_exclude_patterns
                    )
                ]
                
                for file in files:
                    rel_file = os.path.join(rel_root, file) if rel_root else file
                    if not _should_exclude(rel_file, file, all_exclude_patterns):
                        file_path = os.path.join(root, file)
                        zf.write(file_path, rel_file)
                        
        zip_data = mem_zip.getvalue()
        env_id = str(uuid.uuid4())
        await driver.put(env_id, zip_data)
        _set_global_runtime_env(env_id)

class RemoteDestructionMethod:
    def __init__(self, actor_handle):
        self.actor_handle = actor_handle

    async def remote(self, *args, **kwargs):
        from pymapreduce import _global_driver, _global_runtime_env_id, JobConfig, TaskInput
        if _global_driver is None:
            raise RuntimeError("pymapreduce is not initialized.")

        job = JobConfig(f"DestroyActor_{self.actor_handle._actor_id}")
        if _global_runtime_env_id:
            job.with_runtime_env(_global_runtime_env_id)

        task = TaskInput.new_actor_destruction(str(self.actor_handle._actor_id))
        job.add_task(task)

        results = await _global_driver.submit(job)
        return True

class RemoteMethod:
    def __init__(self, actor_handle, method_name):
        self.actor_handle = actor_handle
        self.method_name = method_name
        
    async def remote(self, *args, **kwargs):
        from pymapreduce import _global_driver, _global_runtime_env_id, JobConfig, TaskInput
        if _global_driver is None:
            raise RuntimeError("pymapreduce is not initialized.")
            
        job = JobConfig(f"ActorTask_{self.method_name}")
        if _global_runtime_env_id:
            job.with_runtime_env(_global_runtime_env_id)
            
        payload = (args, kwargs)
        
        deps = []
        _scan_dependencies(args, deps)
        _scan_dependencies(kwargs, deps)
        
        task = TaskInput.new_actor_task(
            str(self.actor_handle._actor_id), 
            self.method_name, 
            payload
        )
        if deps:
            task.with_dependencies(deps)
        job.add_task(task)
        
        results = await _global_driver.submit(job)
        import pymapreduce
        processed = []
        for res in results:
            if isinstance(res, tuple) and len(res) == 2 and res[0] == "__ObjectRef__":
                processed.append(pymapreduce.ObjectRef(res[1]))
            else:
                processed.append(res)
        return processed[0]

class ActorHandle:
    def __init__(self, actor_id):
        self._actor_id = str(actor_id)
        
    def __getattr__(self, name):
        if name in ("destroy", "kill"):
            return RemoteDestructionMethod(self)
        return RemoteMethod(self, name)

class ActorClass:
    def __init__(self, cls, idle_timeout=None):
        self.cls = cls
        self.idle_timeout = idle_timeout
        
    async def remote(self, *args, **kwargs):
        from pymapreduce import _global_driver, _global_runtime_env_id, JobConfig, TaskInput
        if _global_driver is None:
            raise RuntimeError("pymapreduce is not initialized.")
            
        job = JobConfig(f"CreateActor_{self.cls.__name__}")
        if _global_runtime_env_id:
            job.with_runtime_env(_global_runtime_env_id)
            
        actor_id = str(uuid.uuid4())
        payload = (self.cls, args, kwargs, self.idle_timeout)
        
        deps = []
        _scan_dependencies(args, deps)
        _scan_dependencies(kwargs, deps)
        
        task = TaskInput.new_actor_creation(
            actor_id,
            self.cls.__name__,
            payload
        )
        if deps:
            task.with_dependencies(deps)
        job.add_task(task)
        
        await _global_driver.submit(job)
        return ActorHandle(actor_id)

class RemoteFunction:
    def __init__(self, func):
        self.func = func
        
    async def remote(self, *args, **kwargs):
        from pymapreduce import _global_driver, _global_runtime_env_id, JobConfig, TaskInput, TaskKind
        if _global_driver is None:
            raise RuntimeError("pymapreduce is not initialized.")
            
        job = JobConfig(self.func.__name__)
        if _global_runtime_env_id:
            job.with_runtime_env(_global_runtime_env_id)
            
        deps = []
        _scan_dependencies(args, deps)
        
        task = TaskInput(TaskKind.Map, (self.func, args))
        if deps:
            task.with_dependencies(deps)
        job.add_task(task)
        
        results = await _global_driver.submit(job)
        import pymapreduce
        processed = []
        for res in results:
            if isinstance(res, tuple) and len(res) == 2 and res[0] == "__ObjectRef__":
                processed.append(pymapreduce.ObjectRef(res[1]))
            else:
                processed.append(res)
        return processed[0]

def remote(cls_or_func=None, *, idle_timeout=None):
    def wrap(target):
        if inspect.isclass(target):
            return ActorClass(target, idle_timeout=idle_timeout)
        else:
            return RemoteFunction(target)

    if cls_or_func is None:
        return wrap
    return wrap(cls_or_func)

async def kill(actor_handle):
    if hasattr(actor_handle, "kill"):
        return await actor_handle.kill.remote()
    raise ValueError("Object is not a valid ActorHandle")

def _resolve_args(obj, py_deps):
    import pymapreduce
    if isinstance(obj, pymapreduce.ObjectRef):
        if obj.object_id in py_deps:
            return py_deps[obj.object_id]
        return obj
    elif isinstance(obj, list):
        return [_resolve_args(x, py_deps) for x in obj]
    elif isinstance(obj, tuple):
        if type(obj) == tuple and len(obj) == 2 and obj[0] == "__ObjectRef__":
            if obj[1] in py_deps:
                return py_deps[obj[1]]
        return tuple(_resolve_args(x, py_deps) for x in obj)
    elif isinstance(obj, dict):
        return {k: _resolve_args(v, py_deps) for k, v in obj.items()}
    return obj

def _scan_dependencies(obj, deps):
    import pymapreduce
    if isinstance(obj, pymapreduce.ObjectRef):
        deps.append(obj.object_id)
    elif isinstance(obj, list) or isinstance(obj, tuple):
        if type(obj) == tuple and len(obj) == 2 and obj[0] == "__ObjectRef__":
            deps.append(obj[1])
        else:
            for item in obj:
                _scan_dependencies(item, deps)
    elif isinstance(obj, dict):
        for val in obj.values():
            _scan_dependencies(val, deps)
"###;

    let module = PyModule::from_code(py, code, "actor.py", "actor")?;
    m.add("remote", module.getattr("remote")?)?;
    m.add("kill", module.getattr("kill")?)?;
    m.add("DriverWrapper", module.getattr("DriverWrapper")?)?;
    m.add("ObjectRef", module.getattr("ObjectRef")?)?;
    m.add("_resolve_args", module.getattr("_resolve_args")?)?;
    m.add("_scan_dependencies", module.getattr("_scan_dependencies")?)?;

    let driver_class = m.getattr("Driver")?;
    m.add("_original_init", driver_class.getattr("init")?)?;
    m.add("_original_connect", driver_class.getattr("connect")?)?;

    let wrapper_class = m.getattr("DriverWrapper")?;
    
    driver_class.setattr("init", wrapper_class.getattr("init")?)?;
    driver_class.setattr("connect", wrapper_class.getattr("connect")?)?;

    Ok(())
}
