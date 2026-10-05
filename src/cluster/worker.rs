use crate::object_store::store::ObjectStore;
use crate::protocol::codec::MessageCodec;
use crate::protocol::message::{Message, ObjectId, TaskReconcileStatus};
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

/// Resolves the file directory path for task runtime environments.
pub fn get_runtime_env_dir(env_id: uuid::Uuid) -> PathBuf {
    let shm_base = std::path::Path::new("/dev/shm");
    if shm_base.is_dir() {
        shm_base.join("pymapreduce").join("env").join(env_id.to_string())
    } else {
        std::env::temp_dir().join("pymapreduce").join("env").join(env_id.to_string())
    }
}

#[derive(Clone)]
enum WorkerControl {
    Disconnect { graceful: bool },
}

enum SessionAction {
    Continue,
    Break,
}

impl SessionAction {
    fn is_break(&self) -> bool {
        matches!(self, SessionAction::Break)
    }
}

/// Represents an active connection session between WorkerNode and HeadNode.
#[derive(Clone)]
struct WorkerSession {
    worker: WorkerNode,
    tx: mpsc::Sender<Message>,
    semaphore: Arc<Semaphore>,
    env_locks: Arc<DashMap<uuid::Uuid, Arc<Mutex<()>>>>,
}

impl WorkerSession {
    fn new(worker: WorkerNode, tx: mpsc::Sender<Message>) -> Self {
        let semaphore = Arc::new(Semaphore::new(worker.capacity.physical_cpus));
        Self {
            worker,
            tx,
            semaphore,
            env_locks: Arc::new(DashMap::new()),
        }
    }

    fn start_heartbeat(&self) -> tokio::task::JoinHandle<()> {
        let tx = self.tx.clone();
        let worker_id = self.worker.id;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                interval.tick().await;
                if tx.send(Message::Heartbeat { worker_id }).await.is_err() {
                    break;
                }
            }
        })
    }

    async fn handle_incoming_message(&self, message: Message) -> SessionAction {
        match message {
            Message::AssignTask { task } => {
                self.handle_assign_task(task).await;
            }
            Message::CancelTask { task_id } => {
                self.handle_cancel_task(task_id).await;
            }
            Message::QueryTaskStatus { task_id, job_id } => {
                self.handle_query_task_status(task_id, job_id).await;
            }
            Message::ObjectData { object_id, chunk, .. } => {
                let _ = self.worker.store.put(object_id, bytes::Bytes::from(chunk)).await;
            }
            Message::GetObjectWorker { object_id } => {
                let data = self.worker.store.get_any(object_id).await.ok().map(|d| d.to_vec());
                let _ = self.tx.send(Message::ObjectDataWorker { object_id, data }).await;
            }
            Message::Shutdown => {
                return SessionAction::Break;
            }
            Message::DeleteObject { object_id } => {
                self.handle_delete_object(object_id).await;
            }
            _ => {}
        }
        SessionAction::Continue
    }

    async fn handle_assign_task(&self, task: Box<Task>) {
        if self.worker.is_draining.load(Ordering::Relaxed) {
            return;
        }

        self.worker.task_tracker.insert(task.id, TaskReconcileStatus::Running);

        let session = self.clone();
        tokio::spawn(async move {
            session.fetch_dependencies(&task).await;

            if let Some(env_id) = task.runtime_env {
                session.prepare_runtime_env(env_id).await;
            }

            session.spawn_task(task);
        });
    }

    async fn handle_cancel_task(&self, task_id: TaskId) {
        if let Some((_, handle)) = self.worker.running_tasks.remove(&task_id) {
            handle.abort();
        }
        let executor_cancel = self.worker.executor.clone();
        tokio::spawn(async move {
            executor_cancel.cancel(task_id).await;
        });
        if let Some((_, TaskReconcileStatus::Completed { object_id: Some(oid), .. })) =
            self.worker.task_tracker.remove(&task_id)
        {
            let _ = self.worker.store.delete(oid).await;
        }
    }

    async fn handle_query_task_status(&self, task_id: TaskId, job_id: uuid::Uuid) {
        let status = self
            .worker
            .task_tracker
            .get(&task_id)
            .map(|s| s.clone())
            .unwrap_or(TaskReconcileStatus::NotFound);
        let _ = self
            .tx
            .send(Message::TaskStatusResponse { task_id, job_id, status })
            .await;
    }

    async fn handle_delete_object(&self, object_id: ObjectId) {
        let _ = self.worker.store.delete(object_id).await;
        let env_shm = get_runtime_env_dir(object_id);
        if env_shm.exists() {
            let _ = tokio::fs::remove_dir_all(env_shm).await;
        }
        let env_temp = std::env::temp_dir()
            .join("pymapreduce")
            .join("env")
            .join(object_id.to_string());
        if env_temp.exists() {
            let _ = tokio::fs::remove_dir_all(env_temp).await;
        }
    }

    async fn fetch_dependencies(&self, task: &Task) {
        let mut all_deps = task.dependencies.clone();
        if let Some(env_id) = task.runtime_env {
            all_deps.push(env_id);
        }

        for dep_id in all_deps {
            if self.worker.store.get_any(dep_id).await.is_err() {
                let _ = self
                    .tx
                    .send(Message::GetObject {
                        object_id: dep_id,
                        offset: 0,
                        len: u64::MAX,
                    })
                    .await;
                self.worker.store.wait_for_object(dep_id).await;
            }
        }
    }

    async fn prepare_runtime_env(&self, env_id: uuid::Uuid) {
        let env_dir = get_runtime_env_dir(env_id);
        let ready_marker = env_dir.join(".ready");

        if ready_marker.exists() {
            return;
        }

        let lock = self
            .env_locks
            .entry(env_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;

        if !ready_marker.exists() {
            if let Ok(data) = self.worker.store.get_any(env_id).await {
                let env_dir_clone = env_dir.clone();
                let ready_marker_clone = ready_marker.clone();

                let _ = tokio::task::spawn_blocking(move || {
                    let reader = std::io::Cursor::new(data.to_vec());
                    if let Ok(mut archive) = zip::ZipArchive::new(reader) {
                        let _ = std::fs::create_dir_all(&env_dir_clone);
                        if archive.extract(&env_dir_clone).is_ok() {
                            let _ = std::fs::write(&ready_marker_clone, b"1");
                        }
                    }
                })
                .await;
            }
        }
    }

    fn spawn_task(&self, task: Box<Task>) {
        let task_id = task.id;
        let running_clone = self.worker.running_tasks.clone();
        let semaphore_clone = self.semaphore.clone();
        let executor_clone = self.worker.executor.clone();
        let store_clone = self.worker.store.clone();
        let tracker_clone = self.worker.task_tracker.clone();
        let tx_clone = self.tx.clone();

        let join_handle = tokio::spawn(async move {
            let Ok(permit) = semaphore_clone.acquire_owned().await else { return; };
            let result = executor_clone.execute(&task, store_clone).await;
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

            tracker_clone.insert(task.id, status);
            let _ = tx_clone.send(msg).await;
            drop(permit);
        });

        self.worker.running_tasks.insert(task_id, join_handle.abort_handle());
    }
}

/// Core WorkerNode in charge of receiving tasks from HeadNode and executing them.
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

    async fn connect_with_retry(&self, backoffs: &[u64]) -> Option<TcpStream> {
        if let Ok(stream) = TcpStream::connect(&self.head_addr).await {
            return Some(stream);
        }

        for &delay in backoffs {
            tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
            if let Ok(stream) = TcpStream::connect(&self.head_addr).await {
                return Some(stream);
            }
        }
        None
    }

    async fn clean_unreconciled_tasks(&self) {
        for entry in self.task_tracker.iter() {
            if let TaskReconcileStatus::Completed { object_id: Some(oid), .. } = entry.value() {
                let _ = self.store.delete(*oid).await;
            }
        }
        self.task_tracker.clear();

        if self.running_tasks.is_empty() {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        } else {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    async fn reconcile_and_report(&self, framed: &mut Framed<TcpStream, MessageCodec>) -> Result<(), FrameworkError> {
        let report_ready = Message::ReportReady {
            worker_id: self.id,
            cache_hint: None,
            capacity: self.capacity.clone(),
            listener_addr: self.listener_addr.clone(),
        };

        framed
            .send(report_ready)
            .await
            .map_err(|e| FrameworkError::Network(e.to_string()))?;

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
        Ok(())
    }

    async fn run_session(
        &self,
        mut framed: Framed<TcpStream, MessageCodec>,
        control_rx: &mut mpsc::Receiver<WorkerControl>,
    ) -> bool {
        let (tx, mut rx) = mpsc::channel::<Message>(100);
        let session = WorkerSession::new(self.clone(), tx.clone());
        let heartbeat_handle = session.start_heartbeat();

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

                    if session.handle_incoming_message(message).await.is_break() {
                        should_reconnect = false;
                        break;
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
        should_reconnect
    }

    pub async fn run(&self) -> Result<(), FrameworkError> {
        let backoffs = [1, 2, 4, 6, 8, 9];
        let mut control_rx = self.control_rx.lock().await;

        loop {
            if self.is_draining.load(Ordering::Relaxed) {
                break;
            }

            let stream = match self.connect_with_retry(&backoffs).await {
                Some(s) => s,
                None => {
                    self.clean_unreconciled_tasks().await;
                    continue;
                }
            };

            let mut framed = Framed::new(stream, MessageCodec::new());

            if self.reconcile_and_report(&mut framed).await.is_err() {
                continue;
            }

            let should_reconnect = self.run_session(framed, &mut control_rx).await;
            if !should_reconnect {
                break;
            }
        }

        Ok(())
    }
}
