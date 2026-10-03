use crate::object_store::store::ObjectStore;
use crate::protocol::codec::MessageCodec;
use crate::protocol::message::{Message, TaskReconcileStatus};
use crate::types::error::FrameworkError;
use crate::types::executor::Executor;
use crate::types::node::{NodeCapacity, WorkerId};
use crate::types::task::{Task, TaskId};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio_util::codec::Framed;

pub fn get_runtime_env_dir(env_id: uuid::Uuid) -> PathBuf {
    let shm_base = std::path::Path::new("/dev/shm");
    if shm_base.is_dir() {
        shm_base.join("pymapreduce").join("env").join(env_id.to_string())
    } else {
        std::env::temp_dir().join("pymapreduce").join("env").join(env_id.to_string())
    }
}

#[derive(Clone)]
pub struct WorkerNode {
    pub id: WorkerId,
    pub capacity: NodeCapacity,
    executor: Arc<dyn Executor>,
    pub head_addr: String,
    pub store: Arc<ObjectStore>,
    pub listener_addr: Option<String>,
    pub task_tracker: Arc<DashMap<TaskId, TaskReconcileStatus>>,
    pub running_tasks: Arc<DashMap<TaskId, tokio::task::AbortHandle>>,
    pub is_draining: Arc<AtomicBool>,
    control_tx: mpsc::Sender<WorkerControl>,
    control_rx: Arc<Mutex<mpsc::Receiver<WorkerControl>>>,
}

#[derive(Clone)]
enum WorkerControl {
    Disconnect { graceful: bool },
}

impl WorkerNode {
    pub fn new(capacity: NodeCapacity, executor: Arc<dyn Executor>, head_addr: String) -> Self {
        let (control_tx, control_rx) = mpsc::channel(10);
        Self {
            id: uuid::Uuid::new_v4(),
            capacity,
            executor,
            head_addr,
            store: ObjectStore::new_default(),
            listener_addr: None,
            task_tracker: Arc::new(DashMap::new()),
            running_tasks: Arc::new(DashMap::new()),
            is_draining: Arc::new(AtomicBool::new(false)),
            control_tx,
            control_rx: Arc::new(Mutex::new(control_rx)),
        }
    }

    pub fn with_listener(mut self, listener_addr: String) -> Self {
        self.listener_addr = Some(listener_addr);
        self
    }

    pub async fn disconnect(&self, graceful: bool) -> Result<(), FrameworkError> {
        self.control_tx
            .send(WorkerControl::Disconnect { graceful })
            .await
            .map_err(|_| FrameworkError::Other("Worker control channel closed".into()))
    }

    pub async fn run(&self) -> Result<(), FrameworkError> {
        let backoffs = [1, 2, 4, 6, 8, 9];
        let mut control_rx = self.control_rx.lock().await;

        loop {
            // Check draining/shutdown status before connecting
            if self.is_draining.load(Ordering::Relaxed) {
                break;
            }

            // Attempt TCP connection to HeadNode
            let stream = match TcpStream::connect(&self.head_addr).await {
                Ok(s) => s,
                Err(_) => {
                    let mut connected = None;
                    for delay in backoffs {
                        tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                        if let Ok(s) = TcpStream::connect(&self.head_addr).await {
                            connected = Some(s);
                            break;
                        }
                    }
                    match connected {
                        Some(s) => s,
                        None => {
                            // HeadNode is unresponsive after all backoff retries.
                            // Clean up intermediate data and tracker of finished tasks to prevent memory leak.
                            for entry in self.task_tracker.iter() {
                                if let TaskReconcileStatus::Completed { object_id, .. } = entry.value() {
                                    if let Some(oid) = object_id {
                                        let _ = self.store.delete(*oid).await;
                                    }
                                }
                            }
                            self.task_tracker.clear();

                            // If other assigned tasks are still actively computing on CPU cores, continue processing.
                            // Only enter deep idle sleep when all running tasks have completely finished.
                            if self.running_tasks.is_empty() {
                                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                            } else {
                                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                            }
                            continue;
                        }
                    }
                }
            };

            let mut framed = Framed::new(stream, MessageCodec::new());

            // Report ready with capacity and optional listener address
            let report_ready = Message::ReportReady {
                worker_id: self.id,
                cache_hint: None,
                capacity: self.capacity.clone(),
                listener_addr: self.listener_addr.clone(),
            };

            if framed.send(report_ready).await.is_err() {
                continue;
            }

            // Resend completed tasks that were finished while HeadNode was offline
            let mut sent_task_ids = Vec::new();
            for entry in self.task_tracker.iter() {
                if let TaskReconcileStatus::Completed { job_id, output, object_id } = entry.value() {
                    let msg = Message::TaskSuccess {
                        task_id: *entry.key(),
                        job_id: *job_id,
                        output: output.clone(),
                        object_id: *object_id,
                        observed_bandwidth_mbps: None,
                    };
                    if framed.send(msg).await.is_ok() {
                        sent_task_ids.push(*entry.key());
                    }
                }
            }
            for task_id in sent_task_ids {
                self.task_tracker.remove(&task_id);
            }

            let semaphore = Arc::new(Semaphore::new(self.capacity.physical_cpus));
            let executor = self.executor.clone();
            let store = self.store.clone();
            let (tx, mut rx) = mpsc::channel::<Message>(100);
            let env_locks: Arc<DashMap<uuid::Uuid, Arc<Mutex<()>>>> = Arc::new(DashMap::new());

            // Heartbeat task
            let tx_heartbeat = tx.clone();
            let worker_id = self.id;
            let heartbeat_handle = tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
                loop {
                    interval.tick().await;
                    if tx_heartbeat.send(Message::Heartbeat { worker_id }).await.is_err() {
                        break;
                    }
                }
            });

            let mut should_reconnect = true;

            while should_reconnect {
                tokio::select! {
                    ctrl = control_rx.recv() => {
                        match ctrl {
                            Some(WorkerControl::Disconnect { graceful }) => {
                                if graceful {
                                    self.is_draining.store(true, Ordering::Relaxed);
                                    let _ = tx.send(Message::WorkerDraining { worker_id: self.id }).await;
                                }
                                should_reconnect = false;
                                break;
                            }
                            None => {
                                should_reconnect = false;
                                break;
                            }
                        }
                    }
                    msg = framed.next() => {
                        let Some(msg_result) = msg else { break; };
                        let Ok(message) = msg_result else { break; };

                        match message {
                            Message::AssignTask { task } => {
                                if self.is_draining.load(Ordering::Relaxed) {
                                    continue;
                                }

                                self.task_tracker.insert(task.id, TaskReconcileStatus::Running);

                                let tx_clone = tx.clone();
                                let semaphore_clone = semaphore.clone();
                                let executor_clone = executor.clone();
                                let store_clone = store.clone();
                                let env_locks = env_locks.clone();
                                let tracker_clone = self.task_tracker.clone();
                                let running_tasks_clone = self.running_tasks.clone();

                                tokio::spawn(async move {
                                    let mut all_deps = task.dependencies.clone();
                                    if let Some(env_id) = task.runtime_env {
                                        all_deps.push(env_id);
                                    }

                                    for dep_id in all_deps {
                                        if store_clone.get_any(dep_id).await.is_err() {
                                            let _ = tx_clone.send(Message::GetObject {
                                                object_id: dep_id,
                                                offset: 0,
                                                len: u64::MAX,
                                            }).await;
                                            store_clone.wait_for_object(dep_id).await;
                                        }
                                    }

                                    if let Some(env_id) = task.runtime_env {
                                        let env_dir = get_runtime_env_dir(env_id);
                                        let ready_marker = env_dir.join(".ready");

                                        if !ready_marker.exists() {
                                            let lock = env_locks.entry(env_id).or_insert_with(|| Arc::new(Mutex::new(()))).clone();
                                            let _guard = lock.lock().await;

                                            if !ready_marker.exists() {
                                                if let Ok(data) = store_clone.get_any(env_id).await {
                                                    let reader = std::io::Cursor::new(data.to_vec());
                                                    if let Ok(mut archive) = zip::ZipArchive::new(reader) {
                                                        let _ = std::fs::create_dir_all(&env_dir);
                                                        if archive.extract(&env_dir).is_ok() {
                                                            let _ = std::fs::write(&ready_marker, b"1");
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    Self::spawn_task(semaphore_clone, executor_clone, tx_clone, store_clone, tracker_clone, running_tasks_clone, task);
                                });
                            }
                            Message::CancelTask { task_id } => {
                                if let Some((_, handle)) = self.running_tasks.remove(&task_id) {
                                    handle.abort();
                                }
                                self.task_tracker.remove(&task_id);
                            }
                            Message::QueryTaskStatus { task_id, job_id } => {
                                let status = self.task_tracker.get(&task_id)
                                    .map(|s| s.clone())
                                    .unwrap_or(TaskReconcileStatus::NotFound);
                                let _ = tx.send(Message::TaskStatusResponse { task_id, job_id, status }).await;
                            }
                            Message::ObjectData { object_id, chunk, .. } => {
                                let _ = store.put(object_id, bytes::Bytes::from(chunk)).await;
                            }
                            Message::GetObjectWorker { object_id } => {
                                let data = store.get_any(object_id).await.ok().map(|d| d.to_vec());
                                let _ = tx.send(Message::ObjectDataWorker { object_id, data }).await;
                            }
                            Message::Shutdown => {
                                should_reconnect = false;
                                break;
                            }
                            Message::DeleteObject { object_id } => {
                                let _ = store.delete(object_id).await;
                                let env_shm = get_runtime_env_dir(object_id);
                                if env_shm.exists() {
                                    let _ = tokio::fs::remove_dir_all(env_shm).await;
                                }
                                let env_temp = std::env::temp_dir().join("pymapreduce").join("env").join(object_id.to_string());
                                if env_temp.exists() {
                                    let _ = tokio::fs::remove_dir_all(env_temp).await;
                                }
                            }
                            _ => {}
                        }
                    }
                    Some(msg) = rx.recv() => {
                        if framed.send(msg).await.is_err() {
                            break;
                        }
                    }
                }
            }

            heartbeat_handle.abort();
            if !should_reconnect {
                break;
            }
        }

        Ok(())
    }

    fn spawn_task(
        semaphore: Arc<tokio::sync::Semaphore>,
        executor: Arc<dyn Executor>,
        tx: mpsc::Sender<Message>,
        store: Arc<ObjectStore>,
        tracker: Arc<DashMap<TaskId, TaskReconcileStatus>>,
        running_tasks: Arc<DashMap<TaskId, tokio::task::AbortHandle>>,
        task: Box<Task>,
    ) {
        let task_id = task.id;
        let running_clone = running_tasks.clone();
        let join_handle = tokio::spawn(async move {
            let Ok(permit) = semaphore.acquire_owned().await else { return; };
            let result = executor.execute(&task, store).await;
            running_clone.remove(&task_id);
            let (msg, status) = match result {
                Ok((output, object_id)) => (
                    Message::TaskSuccess {
                        task_id: task.id,
                        job_id: task.job_id,
                        output: output.clone(),
                        object_id,
                        observed_bandwidth_mbps: None,
                    },
                    TaskReconcileStatus::Completed {
                        job_id: task.job_id,
                        output,
                        object_id,
                    },
                ),
                Err(e) => (
                    Message::TaskFailure {
                        task_id: task.id,
                        job_id: task.job_id,
                        error: e.to_string(),
                    },
                    TaskReconcileStatus::Failed(e.to_string()),
                ),
            };

            tracker.insert(task.id, status);
            let _ = tx.send(msg).await;
            drop(permit);
        });

        running_tasks.insert(task_id, join_handle.abort_handle());
    }
}
