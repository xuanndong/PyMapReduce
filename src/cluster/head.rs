use crate::fault::monitor::HealthMonitor;
use crate::fault::wal::WriteAheadLog;
use crate::gcs::state::{ActorRecord, ActorStatus, GcsState, JobRecord, ObjectLocation, TaskRecord};
use crate::gcs::store::Gcs;
use crate::object_store::store::ObjectStore;
use crate::protocol::codec::MessageCodec;
use crate::protocol::message::{Message, ObjectId, TaskReconcileStatus};
use crate::scheduler::dispatcher::Dispatcher;
use crate::types::error::FrameworkError;
use crate::types::job::{Job, JobId, JobStatus};
use crate::types::node::{NodeCapacity, NodeInfo, WorkerId};
use crate::types::task::{Task, TaskId, TaskKind, TaskStatus};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex};
use tokio_util::codec::Framed;
use uuid::Uuid;

/// Encapsulates all shared cluster state and communication channels for the HeadNode.
#[derive(Clone)]
pub struct HeadContext {
    pub gcs: Arc<Gcs>,
    pub dispatcher: Arc<Dispatcher>,
    pub health_monitor: Arc<Mutex<HealthMonitor>>,
    pub wal: Arc<Mutex<WriteAheadLog>>,
    pub object_store: Arc<ObjectStore>,
    pub driver_connections: Arc<DashMap<JobId, mpsc::Sender<Message>>>,
    pub worker_connections: Arc<DashMap<WorkerId, mpsc::Sender<Message>>>,
    pub pending_object_gets: Arc<DashMap<ObjectId, Vec<mpsc::Sender<Message>>>>,
    pub shutdown_tx: tokio::sync::broadcast::Sender<()>,
}

impl HeadContext {
    pub async fn log_wal_if_needed(&self, msg: &Message) {
        let should_log = matches!(
            msg,
            Message::SubmitJob { .. }
                | Message::TaskSuccess { .. }
                | Message::TaskFailure { .. }
                | Message::Shutdown
                | Message::PutObject { .. }
                | Message::DeleteObject { .. }
        );
        if should_log {
            let mut wal_lock = self.wal.lock().await;
            let _ = wal_lock.append(msg).await;
        }
    }

    pub async fn cancel_job(&self, job_id: JobId, reason: String) {
        let pruned_tasks = self.dispatcher.cancel_job(job_id);
        let mut tasks_to_cancel = Vec::new();
        let mut running_tasks_to_cancel = Vec::new();

        {
            let mut state = self.gcs.write();

            let task_ids = state
                .jobs
                .get_mut(&job_id)
                .filter(|j| j.status == JobStatus::Running)
                .map(|j| {
                    j.status = JobStatus::Failed(reason.clone());
                    j.tasks.clone()
                })
                .unwrap_or_default();

            let mut workers_to_decrement = Vec::new();
            for tid in &task_ids {
                let Some(task_rec) = state.active_tasks.get_mut(tid) else { continue; };
                if !matches!(task_rec.status, TaskStatus::Pending | TaskStatus::Running) {
                    continue;
                }

                if let (TaskStatus::Running, Some(w_id)) = (&task_rec.status, task_rec.assigned_to) {
                    workers_to_decrement.push(w_id);
                    running_tasks_to_cancel.push((w_id, *tid));
                }
                task_rec.status = TaskStatus::Failed(reason.clone());
                tasks_to_cancel.push(*tid);
            }

            for w_id in workers_to_decrement {
                if let Some(load) = state.worker_load.get_mut(&w_id) {
                    *load = load.saturating_sub(1);
                }
            }

            for tid in &task_ids {
                state.active_tasks.remove(tid);
            }

            state.mark_job_finished(job_id);
        }

        for (w_id, task_id) in running_tasks_to_cancel {
            if let Some(worker_tx) = self.worker_connections.get(&w_id) {
                let _ = worker_tx.send(Message::CancelTask { task_id }).await;
            }
        }

        if let Some(driver_tx) = self.driver_connections.remove(&job_id) {
            let _ = driver_tx.1.send(Message::JobFailed {
                job_id,
                reason: reason.clone(),
            }).await;
        }

        println!(
            "[Cancel] Job {} cancelled: {} ({} running tasks cancelled, {} queued tasks pruned)",
            job_id, reason, tasks_to_cancel.len(), pruned_tasks.len()
        );
    }

    pub async fn try_dispatch_to_worker(&self, w_id: WorkerId, tx: &mpsc::Sender<Message>) {
        let Some(info) = self.gcs.read().nodes.get(&w_id).cloned() else { return; };
        let max_capacity = info.capacity.physical_cpus * 2;

        loop {
            let current_load = self.gcs.read().worker_load.get(&w_id).copied().unwrap_or(0);
            if current_load >= max_capacity {
                break;
            }

            let task_opt = {
                let state = self.gcs.read();
                self.dispatcher.dispatch(&info, &state)
            };

            let Some(task) = task_opt else { break; };

            {
                let mut state = self.gcs.write();
                if let Some(record) = state.active_tasks.get_mut(&task.id) {
                    record.assigned_to = Some(w_id);
                    record.status = TaskStatus::Running;
                }
                *state.worker_load.entry(w_id).or_insert(0) += 1;
            }

            let _ = tx.send(Message::AssignTask { task: Box::new(task) }).await;
        }
    }

    pub async fn handle_message(&self, msg: Message, tx: &mpsc::Sender<Message>, peer_addr: &str) {
        self.log_wal_if_needed(&msg).await;

        match msg {
            Message::ReportReady { worker_id, capacity, listener_addr, .. } => {
                self.handle_report_ready(worker_id, capacity, listener_addr, peer_addr, tx).await;
            }
            Message::AttachJob { job_id } => {
                self.handle_attach_job(job_id, tx).await;
            }
            Message::WorkerDraining { worker_id } => {
                self.gcs.write().nodes.remove(&worker_id);
            }
            Message::TaskStatusResponse { task_id, job_id, status } => {
                self.handle_task_status_response(task_id, job_id, status);
            }
            Message::GetJobStatus { job_id } => {
                self.handle_get_job_status(job_id, tx).await;
            }
            Message::Heartbeat { worker_id } => {
                self.health_monitor.lock().await.record_heartbeat(worker_id);
            }
            Message::Shutdown => {
                self.handle_shutdown().await;
            }
            Message::SubmitJob { job_id, tasks, max_time_secs } => {
                self.handle_submit_job(job_id, tasks, max_time_secs, tx).await;
            }
            Message::TaskSuccess { task_id, job_id, output, object_id, observed_bandwidth_mbps } => {
                self.handle_task_success(task_id, job_id, output, object_id, observed_bandwidth_mbps).await;
            }
            Message::TaskFailure { task_id, job_id, error } => {
                self.handle_task_failure(task_id, job_id, error).await;
            }
            Message::PutObject { object_id, data } => {
                self.handle_put_object(object_id, data).await;
            }
            Message::DeleteObject { object_id } => {
                self.handle_delete_object(object_id).await;
            }
            Message::GetObject { object_id, offset, len } => {
                self.handle_get_object(object_id, offset, len, tx).await;
            }
            Message::ObjectDataWorker { object_id, data } => {
                self.handle_object_data_worker(object_id, data).await;
            }
            Message::CancelJob { job_id } => {
                self.cancel_job(job_id, "Cancelled by user".to_string()).await;
            }
            _ => {}
        }
    }

    async fn handle_report_ready(
        &self,
        worker_id: WorkerId,
        capacity: NodeCapacity,
        listener_addr: Option<String>,
        peer_addr: &str,
        tx: &mpsc::Sender<Message>,
    ) {
        self.worker_connections.insert(worker_id, tx.clone());
        let actual_addr = listener_addr.unwrap_or_else(|| peer_addr.to_string());
        {
            let mut state = self.gcs.write();
            state.nodes.insert(
                worker_id,
                NodeInfo {
                    id: worker_id,
                    capacity,
                    addr: actual_addr,
                },
            );
            state.worker_load.entry(worker_id).or_insert(0);
        }
        let _ = tx.send(Message::WakeUp).await;
    }

    async fn handle_attach_job(&self, job_id: JobId, tx: &mpsc::Sender<Message>) {
        self.driver_connections.insert(job_id, tx.clone());
        let response = {
            let state = self.gcs.read();
            state.jobs.get(&job_id).and_then(|job_record| match &job_record.status {
                JobStatus::Completed => {
                    let mut results = Vec::new();
                    for task_id in &job_record.tasks {
                        if let Some(res) = job_record.results.get(task_id) {
                            results.push(res.clone());
                        }
                    }
                    Some(Message::JobComplete { job_id, results })
                }
                JobStatus::Failed(reason) => {
                    Some(Message::JobFailed { job_id, reason: reason.clone() })
                }
                _ => None,
            })
        };
        if let Some(msg) = response {
            let _ = tx.send(msg).await;
        }
    }

    fn handle_task_status_response(&self, task_id: TaskId, job_id: JobId, status: TaskReconcileStatus) {
        match status {
            TaskReconcileStatus::Completed { output, .. } => {
                let mut state = self.gcs.write();
                if let Some(record) = state.active_tasks.get_mut(&task_id) {
                    record.status = TaskStatus::Completed;
                }
                if let Some(job_record) = state.jobs.get_mut(&job_id) {
                    job_record.results.insert(task_id, output);
                    if job_record.results.len() == job_record.tasks.len() {
                        job_record.status = JobStatus::Completed;
                    }
                }
            }
            TaskReconcileStatus::Failed(error) => {
                let mut state = self.gcs.write();
                if let Some(record) = state.active_tasks.get_mut(&task_id) {
                    record.status = TaskStatus::Failed(error);
                }
            }
            _ => {}
        }
    }

    async fn handle_get_job_status(&self, job_id: JobId, tx: &mpsc::Sender<Message>) {
        let status_msg = {
            let state = self.gcs.read();
            if let Some(job_record) = state.jobs.get(&job_id) {
                Message::JobProgress {
                    job_id,
                    completed: job_record.results.len() as u32,
                    total: job_record.tasks.len() as u32,
                }
            } else {
                Message::JobFailed {
                    job_id,
                    reason: "Job not found".to_string(),
                }
            }
        };
        let _ = tx.send(status_msg).await;
    }

    async fn handle_shutdown(&self) {
        println!("HeadNode received Shutdown command from client, broadcasting to cluster.");
        for mut entry in self.worker_connections.iter_mut() {
            let w_tx = entry.value_mut();
            let _ = w_tx.send(Message::Shutdown).await;
        }
        let _ = self.shutdown_tx.send(());
    }

    async fn handle_submit_job(
        &self,
        job_id: JobId,
        tasks: Vec<Vec<u8>>,
        max_time_secs: Option<u64>,
        tx: &mpsc::Sender<Message>,
    ) {
        self.driver_connections.insert(job_id, tx.clone());

        if tasks.is_empty() {
            self.driver_connections.remove(&job_id);
            let _ = tx.send(Message::JobComplete {
                job_id,
                results: Vec::new(),
            }).await;
            return;
        }

        let actor_validation = {
            let state = self.gcs.read();
            validate_actor_tasks(&state, &tasks)
        };
        if let Err(err_msg) = actor_validation {
            self.driver_connections.remove(&job_id);
            let _ = tx.send(Message::JobFailed { job_id, reason: err_msg }).await;
            return;
        }

        let is_redundant = {
            let state = self.gcs.read();
            is_redundant_destruction(&state, &tasks)
        };
        if is_redundant {
            self.driver_connections.remove(&job_id);
            let _ = tx.send(Message::JobComplete {
                job_id,
                results: vec![vec![0x80, 0x04, 0x88, 0x2e]],
            }).await;
            return;
        }

        {
            let mut state = self.gcs.write();
            let mut task_ids = Vec::new();

            for task_bytes in tasks {
                if let Ok(task) = bincode::deserialize::<Task>(&task_bytes) {
                    task_ids.push(task.id);

                    if let TaskKind::ActorCreation { actor_id, class_name } = &task.kind {
                        state.actors.insert(
                            *actor_id,
                            ActorRecord {
                                actor_id: *actor_id,
                                class_name: class_name.clone(),
                                worker_id: None,
                                status: ActorStatus::Creating,
                                creation_payload: task.payload.clone(),
                            },
                        );
                    }

                    state.active_tasks.insert(
                        task.id,
                        TaskRecord {
                            task: task.clone(),
                            status: TaskStatus::Pending,
                            assigned_to: None,
                            retry_count: 0,
                            output_object_id: None,
                        },
                    );
                    self.dispatcher.enqueue(task);
                }
            }

            state.jobs.insert(
                job_id,
                JobRecord {
                    job: Job {
                        id: job_id,
                        name: "Job".to_string(),
                    },
                    status: JobStatus::Running,
                    tasks: task_ids,
                    results: HashMap::new(),
                },
            );
        }

        let _ = tx.send(Message::JobAccepted { job_id }).await;

        for worker_entry in self.worker_connections.iter() {
            let w_id = *worker_entry.key();
            let w_tx = worker_entry.value();
            self.try_dispatch_to_worker(w_id, w_tx).await;
        }

        if let Some(timeout_secs) = max_time_secs {
            let ctx = self.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(timeout_secs)).await;
                let should_fail = {
                    let state = ctx.gcs.read();
                    state.jobs.get(&job_id).map(|j| j.status == JobStatus::Running).unwrap_or(false)
                };
                if should_fail {
                    println!("[Timeout] Job {} exceeded time limit of {}s", job_id, timeout_secs);
                    ctx.cancel_job(job_id, format!("Job timed out after {}s", timeout_secs)).await;
                }
            });
        }
    }

    async fn handle_task_success(
        &self,
        task_id: TaskId,
        job_id: JobId,
        output: Vec<u8>,
        object_id: Option<ObjectId>,
        observed_bandwidth_mbps: Option<f64>,
    ) {
        let mut job_finished = false;
        let mut job_results = Vec::new();
        let mut notify_worker = None;
        let mut effective_job_id = job_id;
        let mut is_orphan_success = false;
        let orphan_worker_id;

        {
            let mut state = self.gcs.write();
            let (assigned_worker, actor_created, actor_destroyed, task_job_id) =
                if let Some(rec) = state.active_tasks.get_mut(&task_id) {
                    rec.status = TaskStatus::Completed;
                    rec.output_object_id = object_id;
                    let a_created = match &rec.task.kind {
                        TaskKind::ActorCreation { actor_id, .. } => rec.assigned_to.map(|w_id| (*actor_id, w_id)),
                        _ => None,
                    };
                    let a_destroyed = match &rec.task.kind {
                        TaskKind::ActorDestruction { actor_id } => Some(*actor_id),
                        _ => None,
                    };
                    (rec.assigned_to, a_created, a_destroyed, rec.task.job_id)
                } else {
                    (None, None, None, Uuid::nil())
                };

            orphan_worker_id = assigned_worker;

            if let Some(w_id) = assigned_worker {
                if let Some(load) = state.worker_load.get_mut(&w_id) {
                    *load = load.saturating_sub(1);
                }
            }

            if let Some((a_id, w_id)) = actor_created {
                if let Some(actor_rec) = state.actors.get_mut(&a_id) {
                    actor_rec.status = ActorStatus::Alive;
                    actor_rec.worker_id = Some(w_id);
                }
            }

            if let Some(actor_id) = actor_destroyed {
                if let Some(actor_rec) = state.actors.get_mut(&actor_id) {
                    actor_rec.status = ActorStatus::Dead("Explicitly destroyed".to_string());
                }
            }

            if let Some(worker_id) = assigned_worker {
                if let Some(bw) = observed_bandwidth_mbps {
                    let alpha = crate::gcs::store::EMA_ALPHA;
                    let old = state.bandwidth_ema.get(&worker_id).cloned().unwrap_or(bw);
                    state.bandwidth_ema.insert(worker_id, alpha * bw + (1.0 - alpha) * old);
                }

                if let Some(oid) = object_id {
                    state
                        .object_catalog
                        .entry(oid)
                        .or_insert_with(|| ObjectLocation {
                            object_id: oid,
                            size_bytes: 0,
                            nodes: HashSet::new(),
                        })
                        .nodes
                        .insert(worker_id);

                    if self.pending_object_gets.contains_key(&oid) {
                        notify_worker = Some((worker_id, oid));
                    }
                }
            }

            if effective_job_id.is_nil() {
                effective_job_id = task_job_id;
            }

            let mut tasks_to_purge = Vec::new();
            if effective_job_id.is_nil() {
                tasks_to_purge.push(task_id);
            } else {
                match state.jobs.get_mut(&effective_job_id) {
                    Some(job_record) if job_record.status == JobStatus::Running => {
                        job_record.results.insert(task_id, output);

                        if job_record.results.len() == job_record.tasks.len() {
                            job_finished = true;
                            job_record.status = JobStatus::Completed;
                            for tid in &job_record.tasks {
                                if let Some(res) = job_record.results.get(tid) {
                                    job_results.push(res.clone());
                                }
                            }
                            tasks_to_purge = job_record.tasks.clone();
                        }
                    }
                    _ => {
                        is_orphan_success = true;
                        if let Some(oid) = object_id {
                            state.object_catalog.remove(&oid);
                        }
                    }
                }
            }

            for tid in &tasks_to_purge {
                state.active_tasks.remove(tid);
            }

            if job_finished {
                state.mark_job_finished(effective_job_id);
            }
        }

        if is_orphan_success {
            if let (Some(w_id), Some(oid)) = (orphan_worker_id, object_id) {
                if let Some(worker_tx) = self.worker_connections.get(&w_id) {
                    let _ = worker_tx.value().send(Message::DeleteObject { object_id: oid }).await;
                }
            }
        }

        if let Some((w_id, oid)) = notify_worker {
            if let Some(worker_tx) = self.worker_connections.get(&w_id) {
                let _ = worker_tx.value().send(Message::GetObjectWorker { object_id: oid }).await;
            }
        }

        if job_finished {
            let final_job_id = if job_id.is_nil() { effective_job_id } else { job_id };
            if let Some(driver_tx) = self.driver_connections.remove(&final_job_id) {
                let _ = driver_tx.1.send(Message::JobComplete {
                    job_id: final_job_id,
                    results: job_results,
                }).await;
            }
        }
    }

    async fn handle_task_failure(&self, task_id: TaskId, job_id: JobId, error: String) {
        const MAX_TASK_RETRIES: u32 = 3;
        let mut should_requeue = None;
        let mut should_fail_job = false;

        {
            let mut state = self.gcs.write();
            let mut failed_actor = None;
            let mut assigned_worker = None;

            if let Some(record) = state.active_tasks.get_mut(&task_id) {
                assigned_worker = record.assigned_to;
                if let TaskKind::ActorCreation { actor_id, .. } = record.task.kind {
                    failed_actor = Some(actor_id);
                }

                record.retry_count += 1;

                if record.retry_count < MAX_TASK_RETRIES {
                    record.status = TaskStatus::Pending;
                    record.assigned_to = None;
                    should_requeue = Some(record.task.clone());
                    println!("[Retry] Task {} failed (attempt {}/{}): {}", task_id, record.retry_count, MAX_TASK_RETRIES, error);
                } else {
                    record.status = TaskStatus::Failed(error.clone());
                    record.assigned_to = None;
                    should_fail_job = true;
                    println!("[Failed] Task {} exceeded retry limit ({}/{}): {}", task_id, record.retry_count, MAX_TASK_RETRIES, error);
                }
            }

            if let Some(w_id) = assigned_worker {
                if let Some(load) = state.worker_load.get_mut(&w_id) {
                    *load = load.saturating_sub(1);
                }
            }

            if let Some(a_id) = failed_actor {
                if let Some(actor_rec) = state.actors.get_mut(&a_id) {
                    actor_rec.status = ActorStatus::Dead(error.clone());
                }
            }

            if should_fail_job {
                if let Some(job_record) = state.jobs.get_mut(&job_id) {
                    job_record.status = JobStatus::Failed(error.clone());
                }
            }
        }

        if let Some(task) = should_requeue {
            self.dispatcher.requeue_priority(task);
        } else if should_fail_job {
            if !job_id.is_nil() {
                self.cancel_job(
                    job_id,
                    format!("Task {} failed after {} retries: {}", task_id, MAX_TASK_RETRIES, error),
                ).await;
            } else {
                let mut state = self.gcs.write();
                state.active_tasks.remove(&task_id);
            }
        }
    }

    async fn handle_put_object(&self, object_id: ObjectId, data: Vec<u8>) {
        let _ = self.object_store.put(object_id, bytes::Bytes::from(data.clone())).await;
        if let Some(subscribers) = self.pending_object_gets.remove(&object_id) {
            for driver_tx in subscribers.1 {
                let _ = driver_tx.send(Message::ObjectData {
                    object_id,
                    chunk: data.clone(),
                    offset: 0,
                }).await;
            }
        }
    }

    async fn handle_delete_object(&self, object_id: ObjectId) {
        let _ = self.object_store.delete(object_id).await;
        {
            let mut state = self.gcs.write();
            state.object_catalog.remove(&object_id);
        }
        for mut entry in self.worker_connections.iter_mut() {
            let w_tx = entry.value_mut();
            let _ = w_tx.send(Message::DeleteObject { object_id }).await;
        }
    }

    async fn handle_get_object(&self, object_id: ObjectId, offset: u64, len: u64, tx: &mpsc::Sender<Message>) {
        if let Ok(data) = self.object_store.get_any(object_id).await {
            let chunk = if offset as usize >= data.len() {
                Vec::new()
            } else {
                let end = (offset as usize + len as usize).min(data.len());
                data[offset as usize..end].to_vec()
            };
            let _ = tx.send(Message::ObjectData {
                object_id,
                chunk,
                offset,
            }).await;
        } else {
            let worker_opt = {
                let state = self.gcs.read();
                state.object_catalog.get(&object_id).and_then(|loc| loc.nodes.iter().next().cloned())
            };

            {
                let mut subscribers = self.pending_object_gets.entry(object_id).or_default();
                subscribers.push(tx.clone());
            }

            if let Some(worker_id) = worker_opt {
                if let Some(worker_tx) = self.worker_connections.get(&worker_id) {
                    let _ = worker_tx.value().send(Message::GetObjectWorker { object_id }).await;
                }
            }
        }
    }

    async fn handle_object_data_worker(&self, object_id: ObjectId, data: Option<Vec<u8>>) {
        if let Some(subscribers) = self.pending_object_gets.remove(&object_id) {
            let chunk = data.unwrap_or_default();
            for driver_tx in subscribers.1 {
                let _ = driver_tx.send(Message::ObjectData {
                    object_id,
                    chunk: chunk.clone(),
                    offset: 0,
                }).await;
            }
        }
    }
}

fn validate_actor_tasks(state: &GcsState, tasks: &[Vec<u8>]) -> Result<(), String> {
    for task_bytes in tasks {
        let Ok(task) = bincode::deserialize::<Task>(task_bytes) else { continue; };
        if let TaskKind::ActorTask { actor_id, .. } = &task.kind {
            match state.actors.get(actor_id) {
                Some(actor_rec) => {
                    if let ActorStatus::Dead(reason) = &actor_rec.status {
                        return Err(format!("Actor '{}' is dead: {}", actor_id, reason));
                    }
                }
                None => {
                    return Err(format!("Actor '{}' not found in cluster registry.", actor_id));
                }
            }
        }
    }
    Ok(())
}

fn is_redundant_destruction(state: &GcsState, tasks: &[Vec<u8>]) -> bool {
    if tasks.len() == 1 {
        match bincode::deserialize::<Task>(&tasks[0]) {
            Ok(task) if matches!(task.kind, TaskKind::ActorDestruction { .. }) => {
                if let TaskKind::ActorDestruction { actor_id } = task.kind {
                    match state.actors.get(&actor_id) {
                        Some(actor_rec) => matches!(actor_rec.status, ActorStatus::Dead(_)),
                        None => true,
                    }
                } else {
                    false
                }
            }
            _ => false,
        }
    } else {
        false
    }
}

pub struct HeadNode {
    pub gcs: Arc<Gcs>,
    pub dispatcher: Arc<Dispatcher>,
    pub health_monitor: Arc<Mutex<HealthMonitor>>,
    pub wal: Arc<Mutex<WriteAheadLog>>,
    pub object_store: Arc<ObjectStore>,
    pub driver_connections: Arc<DashMap<JobId, mpsc::Sender<Message>>>,
    pub worker_connections: Arc<DashMap<WorkerId, mpsc::Sender<Message>>>,
    pub pending_object_gets: Arc<DashMap<ObjectId, Vec<mpsc::Sender<Message>>>>,
    pub shutdown_tx: tokio::sync::broadcast::Sender<()>,
    addr: String,
}

impl HeadNode {
    pub fn new(gcs: Arc<Gcs>, dispatcher: Arc<Dispatcher>, wal: WriteAheadLog, addr: String) -> Self {
        let driver_connections = Arc::new(DashMap::new());
        let health_monitor = HealthMonitor::new(
            gcs.clone(),
            dispatcher.clone(),
            driver_connections.clone(),
        );

        Self {
            gcs,
            dispatcher,
            health_monitor: Arc::new(Mutex::new(health_monitor)),
            wal: Arc::new(Mutex::new(wal)),
            object_store: ObjectStore::new_default(),
            driver_connections,
            worker_connections: Arc::new(DashMap::new()),
            pending_object_gets: Arc::new(DashMap::new()),
            shutdown_tx: tokio::sync::broadcast::channel(1).0,
            addr,
        }
    }

    fn to_context(&self) -> HeadContext {
        HeadContext {
            gcs: self.gcs.clone(),
            dispatcher: self.dispatcher.clone(),
            health_monitor: self.health_monitor.clone(),
            wal: self.wal.clone(),
            object_store: self.object_store.clone(),
            driver_connections: self.driver_connections.clone(),
            worker_connections: self.worker_connections.clone(),
            pending_object_gets: self.pending_object_gets.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
        }
    }

    pub async fn recover_from_wal(&self) -> Result<(), FrameworkError> {
        let snapshot_path = std::path::PathBuf::from("/tmp/pymapreduce/snapshot.bin");
        if snapshot_path.exists() {
            if let Ok(data) = tokio::fs::read(&snapshot_path).await {
                if let Ok(snapshot_state) = bincode::deserialize::<GcsState>(&data) {
                    *self.gcs.write() = snapshot_state;
                    println!("[Disaster Recovery] Loaded GCS Snapshot successfully!");
                }
            }
        }

        let mut wal = self.wal.lock().await;
        let messages = match wal.replay().await {
            Ok(msgs) => msgs,
            Err(e) => {
                println!("Failed to replay WAL: {}. Starting fresh.", e);
                return Ok(());
            }
        };

        if messages.is_empty() {
            return Ok(());
        }

        println!("[Disaster Recovery] Recovering GCS state from {} WAL records...", messages.len());

        let mut state = self.gcs.write();

        for msg in messages {
            match msg {
                Message::SubmitJob { job_id, tasks, .. } => {
                    let mut task_ids = Vec::new();
                    for task_bytes in tasks {
                        if let Ok(task) = bincode::deserialize::<Task>(&task_bytes) {
                            task_ids.push(task.id);
                            if let TaskKind::ActorCreation { actor_id, class_name } = &task.kind {
                                state.actors.insert(
                                    *actor_id,
                                    ActorRecord {
                                        actor_id: *actor_id,
                                        class_name: class_name.clone(),
                                        worker_id: None,
                                        status: ActorStatus::Creating,
                                        creation_payload: task.payload.clone(),
                                    },
                                );
                            }
                            state.active_tasks.insert(
                                task.id,
                                TaskRecord {
                                    task: task.clone(),
                                    status: TaskStatus::Pending,
                                    assigned_to: None,
                                    retry_count: 0,
                                    output_object_id: None,
                                },
                            );
                        }
                    }
                    state.jobs.insert(
                        job_id,
                        JobRecord {
                            job: Job {
                                id: job_id,
                                name: "Job".to_string(),
                            },
                            status: JobStatus::Running,
                            tasks: task_ids,
                            results: HashMap::new(),
                        },
                    );
                }
                Message::TaskSuccess { task_id, job_id, output, object_id, .. } => {
                    let actor_update = if let Some(record) = state.active_tasks.get_mut(&task_id) {
                        record.status = TaskStatus::Completed;
                        match &record.task.kind {
                            TaskKind::ActorCreation { actor_id, .. } => {
                                Some((*actor_id, true, record.assigned_to))
                            }
                            TaskKind::ActorDestruction { actor_id } => {
                                Some((*actor_id, false, None))
                            }
                            _ => None,
                        }
                    } else {
                        None
                    };

                    if let Some((actor_id, is_alive, assigned_to)) = actor_update {
                        if let Some(actor_rec) = state.actors.get_mut(&actor_id) {
                            if is_alive {
                                actor_rec.status = ActorStatus::Alive;
                                actor_rec.worker_id = assigned_to;
                            } else {
                                actor_rec.status = ActorStatus::Dead("Explicitly destroyed".to_string());
                            }
                        }
                    }

                    if let Some(oid) = object_id {
                        state.object_catalog.entry(oid).or_insert_with(|| ObjectLocation {
                            object_id: oid,
                            size_bytes: 0,
                            nodes: HashSet::new(),
                        });
                    }

                    if let Some(job_record) = state.jobs.get_mut(&job_id) {
                        job_record.results.insert(task_id, output);
                        if job_record.results.len() == job_record.tasks.len() {
                            job_record.status = JobStatus::Completed;
                        }
                    }
                }
                Message::TaskFailure { task_id, job_id, error } => {
                    if let Some(record) = state.active_tasks.get_mut(&task_id) {
                        record.status = TaskStatus::Failed(error.clone());
                    }
                    if let Some(job_record) = state.jobs.get_mut(&job_id) {
                        job_record.status = JobStatus::Failed(error.clone());
                    }
                }
                _ => {}
            }
        }

        let mut queued_count = 0;
        for record in state.active_tasks.values() {
            if record.status == TaskStatus::Pending {
                let should_enqueue = if record.task.job_id.is_nil() {
                    true
                } else if let Some(job) = state.jobs.get(&record.task.job_id) {
                    job.status == JobStatus::Running
                } else {
                    false
                };

                if should_enqueue {
                    self.dispatcher.enqueue(record.task.clone());
                    queued_count += 1;
                }
            }
        }

        println!("[Disaster Recovery] WAL Recovery complete. Re-queued {} pending tasks.", queued_count);
        Ok(())
    }

    pub async fn run(self) -> Result<(), FrameworkError> {
        let ctx = self.to_context();

        let wal_clone = ctx.wal.clone();
        let gcs_clone = ctx.gcs.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(300));
            loop {
                interval.tick().await;
                let snapshot = {
                    let mut state = gcs_clone.write();
                    state.enforce_job_retention(
                        GcsState::DEFAULT_MAX_FINISHED_JOBS,
                        GcsState::DEFAULT_JOB_TTL_SECS,
                    );
                    bincode::serialize(&*state).unwrap_or_default()
                };
                if !snapshot.is_empty() {
                    let tmp_path = std::path::PathBuf::from("/tmp/pymapreduce/snapshot.bin.tmp");
                    let snapshot_path = std::path::PathBuf::from("/tmp/pymapreduce/snapshot.bin");

                    if tokio::fs::write(&tmp_path, snapshot).await.is_ok()
                        && tokio::fs::rename(&tmp_path, &snapshot_path).await.is_ok()
                    {
                        let mut wal_lock = wal_clone.lock().await;
                        let _ = wal_lock.truncate().await;
                        println!("[Disaster Recovery] Checkpoint created. WAL truncated.");
                    }
                }
            }
        });

        self.recover_from_wal().await?;

        let listener = TcpListener::bind(&self.addr)
            .await
            .map_err(|e| FrameworkError::Network(e.to_string()))?;

        let health_monitor = ctx.health_monitor.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
            loop {
                interval.tick().await;
                let mut monitor = health_monitor.lock().await;
                monitor.scan_and_recover().await;
            }
        });

        let mut shutdown_rx = ctx.shutdown_tx.subscribe();

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    println!("HeadNode received Shutdown signal, stopping listener loop.");
                    break;
                }
                accept_result = listener.accept() => {
                    let (stream, peer_addr) = accept_result.map_err(|e| FrameworkError::Network(e.to_string()))?;
                    let peer_addr_str = peer_addr.to_string();
                    let framed = Framed::new(stream, MessageCodec::new());

                    let ctx_conn = ctx.clone();
                    let (tx, mut rx) = mpsc::channel::<Message>(100);
                    let (mut sink, mut stream) = framed.split();

                    tokio::spawn(async move {
                        while let Some(msg) = rx.recv().await {
                            let _ = sink.send(msg).await;
                        }
                    });

                    tokio::spawn(async move {
                        let mut current_worker_id = None;
                        let peer_addr = peer_addr_str;

                        while let Some(msg_result) = stream.next().await {
                            let Ok(msg) = msg_result else { break; };

                            if let Message::ReportReady { worker_id, .. } = &msg {
                                current_worker_id = Some(*worker_id);
                            }

                            ctx_conn.handle_message(msg, &tx, &peer_addr).await;

                            if let Some(w_id) = current_worker_id {
                                ctx_conn.try_dispatch_to_worker(w_id, &tx).await;
                            }
                        }

                        if let Some(w_id) = current_worker_id {
                            println!("Worker {} disconnected. Cleaning up immediately.", w_id);
                            ctx_conn.worker_connections.remove(&w_id);
                            ctx_conn.health_monitor.lock().await.remove_dead_worker(w_id).await;
                            ctx_conn.handle_message(Message::WakeUp, &tx, &peer_addr).await;
                        }
                    });
                }
            }
        }
        Ok(())
    }
}
