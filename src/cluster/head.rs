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
use crate::types::node::{NodeCapacity, NodeInfo, NodeStatus, WorkerId};
use crate::types::task::{Task, TaskId, TaskKind, TaskStatus};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex};
use tokio_util::codec::Framed;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct HeadNodeConfig {
    pub auto_scale: bool,
    pub scale_out_delay_secs: u64,
    pub scale_out_min_tasks: usize,
    pub scale_out_cooldown_secs: u64,
}

impl Default for HeadNodeConfig {
    fn default() -> Self {
        Self {
            auto_scale: false,
            scale_out_delay_secs: 5,
            scale_out_min_tasks: 8,
            scale_out_cooldown_secs: 30,
        }
    }
}

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
    pub config: HeadNodeConfig,
    pub elastic_workers: Arc<DashMap<WorkerId, std::time::Instant>>,
    pub last_scale_out: Arc<parking_lot::Mutex<Option<std::time::Instant>>>,
    pub backlog_start_time: Arc<parking_lot::Mutex<Option<std::time::Instant>>>,
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
            let worker_tx = self.worker_connections.get(&w_id).map(|r| r.value().clone());
            if let Some(worker_tx) = worker_tx {
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
        if info.status != NodeStatus::Active {
            return;
        }
        let max_capacity = info.capacity.physical_cpus * 2;

        loop {
            let task_opt = {
                let state = self.gcs.read();
                let current_load = state.worker_load.get(&w_id).copied().unwrap_or(0);
                if current_load >= max_capacity {
                    None
                } else {
                    self.dispatcher.dispatch(&info, &state)
                }
            };

            let Some(task) = task_opt else { break; };

            let should_dispatch = {
                let mut state = self.gcs.write();
                let is_job_dead = !task.job_id.is_nil()
                    && state
                        .jobs
                        .get(&task.job_id)
                        .map_or(false, |j| j.status != JobStatus::Running);

                if is_job_dead {
                    false
                } else if let Some(record) = state.active_tasks.get_mut(&task.id) {
                    if record.status != TaskStatus::Pending {
                        false
                    } else {
                        record.assigned_to = Some(w_id);
                        record.status = TaskStatus::Running;
                        *state.worker_load.entry(w_id).or_insert(0) += 1;
                        true
                    }
                } else {
                    *state.worker_load.entry(w_id).or_insert(0) += 1;
                    true
                }
            };

            if !should_dispatch {
                continue;
            }

            if tx.send(Message::AssignTask { task: Box::new(task.clone()) }).await.is_err() {
                let mut state = self.gcs.write();
                if let Some(record) = state.active_tasks.get_mut(&task.id) {
                    record.assigned_to = None;
                    record.status = TaskStatus::Pending;
                }
                if let Some(load) = state.worker_load.get_mut(&w_id) {
                    *load = load.saturating_sub(1);
                }
                self.dispatcher.requeue_priority(task);
                break;
            }
        }
    }

    pub async fn dispatch_all_active_workers(&self) {
        let connections: Vec<(WorkerId, mpsc::Sender<Message>)> = self
            .worker_connections
            .iter()
            .map(|e| (*e.key(), e.value().clone()))
            .collect();

        for (w_id, w_tx) in connections {
            self.try_dispatch_to_worker(w_id, &w_tx).await;
        }
    }

    pub async fn handle_worker_disconnect(&self, w_id: WorkerId) {
        let tasks_to_requeue = {
            let mut state = self.gcs.write();
            if let Some(node) = state.nodes.get_mut(&w_id) {
                node.status = NodeStatus::Offline;
            }

            let dead_jobs: HashSet<JobId> = state
                .jobs
                .iter()
                .filter(|(_, j)| j.status != JobStatus::Running)
                .map(|(id, _)| *id)
                .collect();

            let tasks = state
                .active_tasks
                .values_mut()
                .filter(|rec| {
                    rec.assigned_to == Some(w_id)
                        && matches!(rec.status, TaskStatus::Running | TaskStatus::Pending)
                        && !dead_jobs.contains(&rec.task.job_id)
                        && matches!(rec.task.kind, TaskKind::Map | TaskKind::Reduce)
                })
                .map(|rec| {
                    rec.assigned_to = None;
                    rec.status = TaskStatus::Pending;
                    rec.task.clone()
                })
                .collect::<Vec<_>>();

            let load_dec = tasks.len();
            if let Some(load) = state.worker_load.get_mut(&w_id) {
                *load = load.saturating_sub(load_dec);
            }

            tasks
        };

        for task in tasks_to_requeue {
            self.dispatcher.requeue_priority(task);
        }

        self.dispatch_all_active_workers().await;
    }

    pub async fn handle_message(
        &self,
        msg: Message,
        tx: &mpsc::Sender<Message>,
        peer_addr: &str,
        sender_worker_id: Option<WorkerId>,
    ) {
        self.log_wal_if_needed(&msg).await;

        match msg {
            Message::ReportReady { worker_id, capacity, listener_addr, standby, .. } => {
                self.handle_report_ready(worker_id, capacity, listener_addr, peer_addr, standby, tx).await;
            }
            Message::WorkerEnteringStandby { worker_id } => {
                let mut state = self.gcs.write();
                if let Some(node) = state.nodes.get_mut(&worker_id) {
                    node.status = NodeStatus::Standby;
                    println!("[AutoScaler] Worker {} confirmed in Standby mode.", worker_id);
                }
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
                self.handle_task_success(task_id, job_id, output, object_id, observed_bandwidth_mbps, sender_worker_id).await;
            }
            Message::TaskFailure { task_id, job_id, error } => {
                self.handle_task_failure(task_id, job_id, error, sender_worker_id).await;
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
        standby_requested: bool,
        tx: &mpsc::Sender<Message>,
    ) {
        self.worker_connections.insert(worker_id, tx.clone());
        let actual_addr = listener_addr.unwrap_or_else(|| peer_addr.to_string());

        let initial_status = if standby_requested {
            if self.config.auto_scale {
                println!("[AutoScaler] Worker {} registered as STANDBY (Elastic Worker).", worker_id);
                self.elastic_workers.insert(worker_id, std::time::Instant::now());
                NodeStatus::Standby
            } else {
                println!(
                    "[AutoScaler Warning] Worker {} requested Standby mode, but HeadNode is running without --auto-scale.\n\
                    --> Auto-adapting: Overriding worker {} state to ACTIVE to prevent permanent starvation.",
                    worker_id, worker_id
                );
                NodeStatus::Active
            }
        } else {
            NodeStatus::Active
        };

        let is_elastic = standby_requested && self.config.auto_scale;

        {
            let mut state = self.gcs.write();
            state.nodes.insert(
                worker_id,
                NodeInfo {
                    id: worker_id,
                    capacity,
                    addr: actual_addr,
                    status: initial_status,
                    is_elastic,
                },
            );
            state.worker_load.entry(worker_id).or_insert(0);
        }

        self.health_monitor.lock().await.record_heartbeat(worker_id);

        let _ = tx.send(Message::RegisterAck { auto_scale_enabled: self.config.auto_scale }).await;
        if initial_status == NodeStatus::Active {
            let _ = tx.send(Message::WakeUp).await;
        }
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
            TaskReconcileStatus::Failed { error, .. } => {
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
        let senders: Vec<mpsc::Sender<Message>> = self
            .worker_connections
            .iter()
            .map(|e| e.value().clone())
            .collect();
        for w_tx in senders {
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

        self.dispatch_all_active_workers().await;

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
        sender_worker_id: Option<WorkerId>,
    ) {
        let mut job_finished = false;
        let mut job_results = Vec::new();
        let mut notify_worker = None;
        let mut cancel_worker = None;
        let mut effective_job_id = job_id;
        let mut is_orphan_success = false;
        let orphan_worker_id;

        {
            let mut state = self.gcs.write();
            let (assigned_worker, actor_created, actor_destroyed, task_job_id, is_duplicate) =
                if let Some(rec) = state.active_tasks.get_mut(&task_id) {
                    if rec.status == TaskStatus::Completed {
                        (rec.assigned_to, None, None, rec.task.job_id, true)
                    } else {
                        let prev_assigned = rec.assigned_to;
                        rec.status = TaskStatus::Completed;
                        rec.output_object_id = object_id;
                        let actual_worker = sender_worker_id.or(prev_assigned);
                        rec.assigned_to = actual_worker;
                        let a_created = match &rec.task.kind {
                            TaskKind::ActorCreation { actor_id, .. } => actual_worker.map(|w_id| (*actor_id, w_id)),
                            _ => None,
                        };
                        let a_destroyed = match &rec.task.kind {
                            TaskKind::ActorDestruction { actor_id } => Some(*actor_id),
                            _ => None,
                        };

                        if let (Some(assigned_w), Some(sender_w)) = (prev_assigned, sender_worker_id) {
                            if assigned_w != sender_w {
                                cancel_worker = Some((assigned_w, task_id));
                            }
                        }

                        (prev_assigned, a_created, a_destroyed, rec.task.job_id, false)
                    }
                } else {
                    (None, None, None, Uuid::nil(), true)
                };

            if is_duplicate {
                is_orphan_success = true;
                orphan_worker_id = sender_worker_id.or(assigned_worker);
            } else {
                orphan_worker_id = sender_worker_id.or(assigned_worker);

                if let Some(w_id) = assigned_worker {
                    if let Some(load) = state.worker_load.get_mut(&w_id) {
                        *load = load.saturating_sub(1);
                    }
                    if self.elastic_workers.contains_key(&w_id) {
                        self.elastic_workers.insert(w_id, std::time::Instant::now());
                    }
                }

                if let Some(sender_id) = sender_worker_id {
                    if self.elastic_workers.contains_key(&sender_id) {
                        self.elastic_workers.insert(sender_id, std::time::Instant::now());
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

                let actual_worker = sender_worker_id.or(assigned_worker);
                if let Some(worker_id) = actual_worker {
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
        }

        if let Some((w_id, tid)) = cancel_worker {
            let worker_tx = self.worker_connections.get(&w_id).map(|r| r.value().clone());
            if let Some(worker_tx) = worker_tx {
                let _ = worker_tx.send(Message::CancelTask { task_id: tid }).await;
                println!(
                    "[Speculative Execution] Task {} completed by reconnected worker; cancelled duplicate execution on Worker {}.",
                    tid, w_id
                );
            }
        }

        if is_orphan_success {
            if let (Some(w_id), Some(oid)) = (orphan_worker_id, object_id) {
                let worker_tx = self.worker_connections.get(&w_id).map(|r| r.value().clone());
                if let Some(worker_tx) = worker_tx {
                    let _ = worker_tx.send(Message::DeleteObject { object_id: oid }).await;
                }
            }
        }

        if let Some((w_id, oid)) = notify_worker {
            let worker_tx = self.worker_connections.get(&w_id).map(|r| r.value().clone());
            if let Some(worker_tx) = worker_tx {
                let _ = worker_tx.send(Message::GetObjectWorker { object_id: oid }).await;
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

    async fn handle_task_failure(
        &self,
        task_id: TaskId,
        job_id: JobId,
        error: String,
        sender_worker_id: Option<WorkerId>,
    ) {
        const MAX_TASK_RETRIES: u32 = 3;
        let mut should_requeue = None;
        let mut should_fail_job = false;
        let mut failed_actor = None;

        {
            let mut state = self.gcs.write();
            let mut worker_to_decrement = None;

            if let Some(record) = state.active_tasks.get_mut(&task_id) {
                // If task already succeeded, ignore late failures from speculative / duplicate workers
                if record.status == TaskStatus::Completed {
                    println!(
                        "[TaskFailure Ignored] Task {} already completed. Ignoring late failure from Worker {:?}",
                        task_id, sender_worker_id
                    );
                    return;
                }

                // If failure came from a stale worker that doesn't match current assigned worker, ignore it
                if let (Some(sender), Some(assigned)) = (sender_worker_id, record.assigned_to) {
                    if sender != assigned {
                        println!(
                            "[Stale TaskFailure] Task {} is currently assigned to Worker {}, ignoring failure from stale Worker {}",
                            task_id, assigned, sender
                        );
                        return;
                    }
                }

                worker_to_decrement = record.assigned_to.or(sender_worker_id);

                record.retry_count += 1;

                if record.retry_count < MAX_TASK_RETRIES {
                    let already_pending = record.status == TaskStatus::Pending && record.assigned_to.is_none();
                    record.status = TaskStatus::Pending;
                    record.assigned_to = None;
                    if !already_pending {
                        should_requeue = Some(record.task.clone());
                    }
                    println!("[Retry] Task {} failed (attempt {}/{}): {}", task_id, record.retry_count, MAX_TASK_RETRIES, error);
                } else {
                    if let TaskKind::ActorCreation { actor_id, .. } = record.task.kind {
                        failed_actor = Some(actor_id);
                    }
                    record.status = TaskStatus::Failed(error.clone());
                    record.assigned_to = None;
                    should_fail_job = true;
                    println!("[Failed] Task {} exceeded retry limit ({}/{}): {}", task_id, record.retry_count, MAX_TASK_RETRIES, error);
                }
            }

            if let Some(w_id) = worker_to_decrement {
                if let Some(load) = state.worker_load.get_mut(&w_id) {
                    *load = load.saturating_sub(1);
                }
                if self.elastic_workers.contains_key(&w_id) {
                    self.elastic_workers.insert(w_id, std::time::Instant::now());
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
            if let Some(a_id) = failed_actor {
                let stuck_tasks = self.dispatcher.drain_actor_mailbox(&a_id);
                if !stuck_tasks.is_empty() {
                    let mut state = self.gcs.write();
                    for stuck in &stuck_tasks {
                        if let Some(record) = state.active_tasks.get_mut(&stuck.id) {
                            record.status = TaskStatus::Failed(format!("Actor Dead ({})", error));
                        }
                    }
                }
                for stuck in &stuck_tasks {
                    let tx_opt = self.driver_connections.get(&stuck.job_id).map(|r| r.value().clone());
                    if let Some(driver_tx) = tx_opt {
                        let _ = driver_tx.send(Message::TaskFailure {
                            task_id: stuck.id,
                            job_id: stuck.job_id,
                            error: format!("Actor Dead ({})", error),
                        }).await;
                    }
                }
            }
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
        self.pending_object_gets.remove(&object_id);
        {
            let mut state = self.gcs.write();
            state.object_catalog.remove(&object_id);
        }
        let senders: Vec<mpsc::Sender<Message>> = self
            .worker_connections
            .iter()
            .map(|e| e.value().clone())
            .collect();
        for w_tx in senders {
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

            let worker_tx_opt = worker_opt.and_then(|w_id| {
                self.worker_connections.get(&w_id).map(|r| r.value().clone())
            });

            if let Some(worker_tx) = worker_tx_opt {
                {
                    let mut subscribers = self.pending_object_gets.entry(object_id).or_default();
                    subscribers.push(tx.clone());
                }
                let _ = worker_tx.send(Message::GetObjectWorker { object_id }).await;
            } else {
                let _ = tx.send(Message::ObjectData {
                    object_id,
                    chunk: Vec::new(),
                    offset,
                }).await;
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

    pub async fn run_autoscaler_loop(&self) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        println!(
            "[AutoScaler] Watchdog loop started (Delay: {}s, Min Tasks: {}, Cooldown: {}s)",
            self.config.scale_out_delay_secs,
            self.config.scale_out_min_tasks,
            self.config.scale_out_cooldown_secs
        );

        loop {
            interval.tick().await;
            self.check_and_scale_out().await;
            self.check_and_scale_in().await;
        }
    }

    fn is_cluster_saturated(&self) -> bool {
        let state = self.gcs.read();
        let mut total_active_capacity = 0;
        let mut total_active_load = 0;

        for (w_id, info) in &state.nodes {
            if info.status == NodeStatus::Active {
                total_active_capacity += info.capacity.physical_cpus * 2;
                total_active_load += state.worker_load.get(w_id).copied().unwrap_or(0);
            }
        }

        total_active_capacity == 0 || total_active_load >= total_active_capacity
    }

    fn should_trigger_scale_out(&self, pending_tasks: usize, now: std::time::Instant) -> bool {
        let mut start_lock = self.backlog_start_time.lock();
        let start = start_lock.get_or_insert(now);

        // Three-layer gate: sustained delay + queue depth + cooldown period
        let delay_met = now.duration_since(*start).as_secs() >= self.config.scale_out_delay_secs;
        let depth_met = pending_tasks >= self.config.scale_out_min_tasks;
        let cooldown_met = match *self.last_scale_out.lock() {
            Some(last_scale) => now.duration_since(last_scale).as_secs() >= self.config.scale_out_cooldown_secs,
            None => true,
        };

        delay_met && depth_met && cooldown_met
    }

    async fn wake_standby_worker(&self, w_id: WorkerId, now: std::time::Instant, pending_tasks: usize) {
        {
            let mut state = self.gcs.write();
            if let Some(info) = state.nodes.get_mut(&w_id) {
                info.status = NodeStatus::Active;
            }
        }

        self.elastic_workers.insert(w_id, now);
        *self.last_scale_out.lock() = Some(now);
        *self.backlog_start_time.lock() = None;

        let tx_opt = self.worker_connections.get(&w_id).map(|r| r.value().clone());
        if let Some(tx) = tx_opt {
            let _ = tx.send(Message::WakeUp).await;
            self.try_dispatch_to_worker(w_id, &tx).await;
        }

        println!(
            "[AutoScaler] Scale-Out triggered! Woke up Standby Worker {} (Pending Tasks: {}).",
            w_id, pending_tasks
        );
    }

    async fn check_and_scale_out(&self) {
        let pending_tasks = self.dispatcher.stateless_task_count();
        if pending_tasks < self.config.scale_out_min_tasks || !self.is_cluster_saturated() {
            *self.backlog_start_time.lock() = None;
            return;
        }

        let now = std::time::Instant::now();
        if !self.should_trigger_scale_out(pending_tasks, now) {
            return;
        }

        let standby_worker = self.gcs.read().nodes.iter()
            .find(|(_, info)| info.status == NodeStatus::Standby)
            .map(|(w_id, _)| *w_id);

        if let Some(w_id) = standby_worker {
            self.wake_standby_worker(w_id, now, pending_tasks).await;
        }
    }

    async fn transition_worker_to_standby(&self, w_id: WorkerId, now: std::time::Instant) {
        let transitioned = {
            let mut state = self.gcs.write();
            let current_load = state.worker_load.get(&w_id).copied().unwrap_or(0);
            let has_running_tasks = state
                .active_tasks
                .values()
                .any(|t| t.assigned_to == Some(w_id) && t.status == TaskStatus::Running);
            let holds_actors = state
                .actors
                .values()
                .any(|a| a.worker_id == Some(w_id) && matches!(a.status, ActorStatus::Alive | ActorStatus::Creating));

            if current_load > 0 || has_running_tasks || holds_actors {
                false
            } else {
                if let Some(node) = state.nodes.get_mut(&w_id) {
                    node.status = NodeStatus::Standby;
                }
                true
            }
        };

        if transitioned {
            let tx_opt = self.worker_connections.get(&w_id).map(|r| r.value().clone());
            if let Some(tx) = tx_opt {
                let _ = tx.send(Message::EnterStandby).await;
            }

            self.elastic_workers.insert(w_id, now);
            println!("[AutoScaler] Scale-In triggered! Put Elastic Worker {} to STANDBY.", w_id);
        }
    }

    async fn check_and_scale_in(&self) {
        let now = std::time::Instant::now();
        let mut workers_to_standby = Vec::new();

        {
            let state = self.gcs.read();
            let elastic_snapshots: Vec<(WorkerId, std::time::Instant)> = self
                .elastic_workers
                .iter()
                .map(|e| (*e.key(), *e.value()))
                .collect();

            for (w_id, last_idle) in elastic_snapshots {
                let Some(node_info) = state.nodes.get(&w_id) else {
                    self.elastic_workers.remove(&w_id);
                    continue;
                };
                if node_info.status != NodeStatus::Active {
                    continue;
                }

                let current_load = state.worker_load.get(&w_id).copied().unwrap_or(0);
                let has_running_tasks = state
                    .active_tasks
                    .values()
                    .any(|t| t.assigned_to == Some(w_id) && t.status == TaskStatus::Running);
                let holds_deps = worker_holds_needed_objects(&state, w_id);
                let holds_actors = state
                    .actors
                    .values()
                    .any(|a| a.worker_id == Some(w_id) && matches!(a.status, ActorStatus::Alive | ActorStatus::Creating));

                if current_load > 0 || has_running_tasks || holds_deps || holds_actors {
                    self.elastic_workers.insert(w_id, now);
                    continue;
                }

                // Default idle timeout threshold: 60 seconds
                if now.duration_since(last_idle).as_secs() >= 60 {
                    workers_to_standby.push(w_id);
                }
            }
        }

        for w_id in workers_to_standby {
            self.transition_worker_to_standby(w_id, now).await;
        }
    }
}

fn worker_holds_needed_objects(state: &GcsState, worker_id: WorkerId) -> bool {
    for (oid, loc) in &state.object_catalog {
        if !loc.nodes.contains(&worker_id) {
            continue;
        }
        let is_needed = state.active_tasks.values().any(|rec| {
            matches!(rec.status, TaskStatus::Pending | TaskStatus::Running)
                && rec.task.dependencies.contains(oid)
        });
        if is_needed {
            return true;
        }
    }
    false
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
    pub config: HeadNodeConfig,
    pub elastic_workers: Arc<DashMap<WorkerId, std::time::Instant>>,
    pub last_scale_out: Arc<parking_lot::Mutex<Option<std::time::Instant>>>,
    pub backlog_start_time: Arc<parking_lot::Mutex<Option<std::time::Instant>>>,
    addr: String,
}

impl HeadNode {
    pub fn new(gcs: Arc<Gcs>, dispatcher: Arc<Dispatcher>, wal: WriteAheadLog, addr: String) -> Self {
        Self::new_with_config(gcs, dispatcher, wal, addr, HeadNodeConfig::default())
    }

    pub fn new_with_config(
        gcs: Arc<Gcs>,
        dispatcher: Arc<Dispatcher>,
        wal: WriteAheadLog,
        addr: String,
        config: HeadNodeConfig,
    ) -> Self {
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
            config,
            elastic_workers: Arc::new(DashMap::new()),
            last_scale_out: Arc::new(parking_lot::Mutex::new(None)),
            backlog_start_time: Arc::new(parking_lot::Mutex::new(None)),
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
            config: self.config.clone(),
            elastic_workers: self.elastic_workers.clone(),
            last_scale_out: self.last_scale_out.clone(),
            backlog_start_time: self.backlog_start_time.clone(),
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
        let ctx_monitor = ctx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
            loop {
                interval.tick().await;
                let mut monitor = health_monitor.lock().await;
                let dead_count = monitor.scan_and_recover().await;
                if dead_count > 0 {
                    ctx_monitor.dispatch_all_active_workers().await;
                }
            }
        });

        if ctx.config.auto_scale {
            let ctx_autoscaler = ctx.clone();
            tokio::spawn(async move {
                ctx_autoscaler.run_autoscaler_loop().await;
            });
        }

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
                            if sink.send(msg).await.is_err() {
                                break;
                            }
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

                            ctx_conn.handle_message(msg, &tx, &peer_addr, current_worker_id).await;

                            if let Some(w_id) = current_worker_id {
                                ctx_conn.try_dispatch_to_worker(w_id, &tx).await;
                            }
                        }

                        if let Some(w_id) = current_worker_id {
                            let is_current = ctx_conn
                                .worker_connections
                                .get(&w_id)
                                .map(|current_tx| current_tx.same_channel(&tx))
                                .unwrap_or(false);

                            if is_current {
                                println!("Worker {} socket disconnected. Transitioning to OFFLINE (grace period active).", w_id);
                                ctx_conn.worker_connections.remove(&w_id);
                                ctx_conn.elastic_workers.remove(&w_id);
                                ctx_conn.handle_worker_disconnect(w_id).await;
                            } else {
                                println!("Stale connection for Worker {} closed; keeping newer active connection.", w_id);
                            }
                        }
                    });
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::task::{Task, TaskKind};

    fn dummy_task() -> Task {
        Task {
            id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            kind: TaskKind::Map,
            payload: vec![],
            runtime_env: None,
            dependencies: vec![],
        }
    }

    async fn setup_test_context(config: HeadNodeConfig) -> (HeadContext, std::path::PathBuf) {
        let wal_path = std::env::temp_dir().join(format!("wal_{}.log", Uuid::new_v4()));
        let wal = WriteAheadLog::new(wal_path.clone()).await.unwrap();
        let gcs = Arc::new(Gcs::new());
        let dispatcher = Arc::new(Dispatcher::new(Box::new(crate::scheduler::strategy::LeastLoadStrategy)));
        let head = HeadNode::new_with_config(gcs, dispatcher, wal, "127.0.0.1:0".to_string(), config);
        (head.to_context(), wal_path)
    }

    #[tokio::test]
    async fn test_auto_scale_disabled_fallback() {
        let (ctx, wal_path) = setup_test_context(HeadNodeConfig {
            auto_scale: false,
            ..Default::default()
        }).await;

        let worker_id = Uuid::new_v4();
        let capacity = NodeCapacity { physical_cpus: 4, total_ram_mb: 8192 };
        let (tx, mut rx) = mpsc::channel(10);

        ctx.handle_report_ready(worker_id, capacity, None, "127.0.0.1:9001", true, &tx).await;

        // Verify status was forced to Active (fallback adaptation)
        let state = ctx.gcs.read();
        let node = state.nodes.get(&worker_id).expect("Worker should be registered");
        assert_eq!(node.status, NodeStatus::Active);
        assert!(!node.is_elastic);

        // Verify RegisterAck has auto_scale_enabled = false
        let msg1 = rx.recv().await.expect("Expected RegisterAck");
        assert_eq!(msg1, Message::RegisterAck { auto_scale_enabled: false });

        // Verify WakeUp was sent immediately because it was adapted to Active
        let msg2 = rx.recv().await.expect("Expected WakeUp");
        assert_eq!(msg2, Message::WakeUp);

        let _ = tokio::fs::remove_file(wal_path).await;
    }

    #[tokio::test]
    async fn test_auto_scale_enabled_registers_standby() {
        let (ctx, wal_path) = setup_test_context(HeadNodeConfig {
            auto_scale: true,
            ..Default::default()
        }).await;

        let worker_id = Uuid::new_v4();
        let capacity = NodeCapacity { physical_cpus: 4, total_ram_mb: 8192 };
        let (tx, mut rx) = mpsc::channel(10);

        ctx.handle_report_ready(worker_id, capacity, None, "127.0.0.1:9001", true, &tx).await;

        // Verify status is Standby and tracked as elastic worker
        let state = ctx.gcs.read();
        let node = state.nodes.get(&worker_id).expect("Worker should be registered");
        assert_eq!(node.status, NodeStatus::Standby);
        assert!(node.is_elastic);
        assert!(ctx.elastic_workers.contains_key(&worker_id));

        // Verify RegisterAck has auto_scale_enabled = true
        let msg1 = rx.recv().await.expect("Expected RegisterAck");
        assert_eq!(msg1, Message::RegisterAck { auto_scale_enabled: true });

        // No WakeUp should be sent
        assert!(rx.try_recv().is_err());

        let _ = tokio::fs::remove_file(wal_path).await;
    }

    #[tokio::test]
    async fn test_scale_out_triggered_when_saturated() {
        let (ctx, wal_path) = setup_test_context(HeadNodeConfig {
            auto_scale: true,
            scale_out_delay_secs: 0,
            scale_out_min_tasks: 2,
            scale_out_cooldown_secs: 0,
        }).await;

        let active_w_id = Uuid::new_v4();
        let standby_w_id = Uuid::new_v4();
        let (active_tx, _active_rx) = mpsc::channel(10);
        let (standby_tx, mut standby_rx) = mpsc::channel(10);

        // Register active worker (1 CPU -> max capacity = 2)
        ctx.handle_report_ready(
            active_w_id,
            NodeCapacity { physical_cpus: 1, total_ram_mb: 2048 },
            None,
            "127.0.0.1:9001",
            false,
            &active_tx,
        ).await;

        // Register standby worker
        ctx.handle_report_ready(
            standby_w_id,
            NodeCapacity { physical_cpus: 4, total_ram_mb: 8192 },
            None,
            "127.0.0.1:9002",
            true,
            &standby_tx,
        ).await;

        // Drain initial RegisterAck from standby_rx
        let _ = standby_rx.recv().await;

        // Saturate active worker (load = 2 >= capacity 2)
        ctx.gcs.write().worker_load.insert(active_w_id, 2);

        // Enqueue 2 pending tasks
        ctx.dispatcher.enqueue(dummy_task());
        ctx.dispatcher.enqueue(dummy_task());

        // Run check_and_scale_out
        ctx.check_and_scale_out().await;

        // Standby worker must have transitioned to Active
        assert_eq!(ctx.gcs.read().nodes.get(&standby_w_id).unwrap().status, NodeStatus::Active);

        // Standby worker must receive WakeUp
        let msg = standby_rx.recv().await.expect("Expected WakeUp");
        assert_eq!(msg, Message::WakeUp);

        let _ = tokio::fs::remove_file(wal_path).await;
    }

    #[tokio::test]
    async fn test_scale_in_with_safety_filters() {
        let (ctx, wal_path) = setup_test_context(HeadNodeConfig {
            auto_scale: true,
            ..Default::default()
        }).await;

        let elastic_w_id = Uuid::new_v4();
        let (tx, mut rx) = mpsc::channel(10);

        // Register elastic worker as Active (woken up previously)
        ctx.handle_report_ready(
            elastic_w_id,
            NodeCapacity { physical_cpus: 4, total_ram_mb: 8192 },
            None,
            "127.0.0.1:9001",
            true,
            &tx,
        ).await;

        // Wake it up
        ctx.gcs.write().nodes.get_mut(&elastic_w_id).unwrap().status = NodeStatus::Active;
        // Set idle time in past (> 60s)
        ctx.elastic_workers.insert(elastic_w_id, std::time::Instant::now() - std::time::Duration::from_secs(70));

        // Drain RegisterAck
        let _ = rx.recv().await;

        // Filter 1 violation: Worker has a running task assigned -> Should NOT scale in
        let running_task = dummy_task();
        let run_task_id = running_task.id;
        ctx.gcs.write().active_tasks.insert(run_task_id, TaskRecord {
            task: running_task,
            status: TaskStatus::Running,
            assigned_to: Some(elastic_w_id),
            retry_count: 0,
            output_object_id: None,
        });

        ctx.check_and_scale_in().await;
        assert_eq!(ctx.gcs.read().nodes.get(&elastic_w_id).unwrap().status, NodeStatus::Active);

        // Remove running task
        ctx.gcs.write().active_tasks.remove(&run_task_id);

        // Filter 2 violation: Worker holds an Object needed as dependency for a pending task -> Should NOT scale in
        let needed_obj_id = Uuid::new_v4();
        ctx.gcs.write().object_catalog.insert(needed_obj_id, ObjectLocation {
            object_id: needed_obj_id,
            size_bytes: 1024,
            nodes: [elastic_w_id].into_iter().collect(),
        });
        let mut dependent_task = dummy_task();
        dependent_task.dependencies.push(needed_obj_id);
        let dep_task_id = dependent_task.id;
        ctx.gcs.write().active_tasks.insert(dep_task_id, TaskRecord {
            task: dependent_task,
            status: TaskStatus::Pending,
            assigned_to: None,
            retry_count: 0,
            output_object_id: None,
        });

        ctx.check_and_scale_in().await;
        assert_eq!(ctx.gcs.read().nodes.get(&elastic_w_id).unwrap().status, NodeStatus::Active);

        // Complete the dependent task so the object is no longer needed
        ctx.gcs.write().active_tasks.remove(&dep_task_id);

        // Simulate idle timeout passing (> 60s) after dependency released
        ctx.elastic_workers.insert(elastic_w_id, std::time::Instant::now() - std::time::Duration::from_secs(70));

        // Now all Filters pass (0 tasks, 0 needed objects) -> Should SCALE IN!
        ctx.check_and_scale_in().await;

        assert_eq!(ctx.gcs.read().nodes.get(&elastic_w_id).unwrap().status, NodeStatus::Standby);
        let msg = rx.recv().await.expect("Expected EnterStandby");
        assert_eq!(msg, Message::EnterStandby);

        let _ = tokio::fs::remove_file(wal_path).await;
    }

    #[tokio::test]
    async fn test_scale_out_ignores_actor_creations() {
        let (ctx, wal_path) = setup_test_context(HeadNodeConfig {
            auto_scale: true,
            scale_out_delay_secs: 0,
            scale_out_min_tasks: 2,
            scale_out_cooldown_secs: 0,
        }).await;

        let active_w_id = Uuid::new_v4();
        let standby_w_id = Uuid::new_v4();
        let (active_tx, _active_rx) = mpsc::channel(10);
        let (standby_tx, mut standby_rx) = mpsc::channel(10);

        ctx.handle_report_ready(
            active_w_id,
            NodeCapacity { physical_cpus: 1, total_ram_mb: 2048 },
            None,
            "127.0.0.1:9001",
            false,
            &active_tx,
        ).await;

        ctx.handle_report_ready(
            standby_w_id,
            NodeCapacity { physical_cpus: 4, total_ram_mb: 8192 },
            None,
            "127.0.0.1:9002",
            true,
            &standby_tx,
        ).await;

        let _ = standby_rx.recv().await; // Drain RegisterAck

        // Saturate active worker
        ctx.gcs.write().worker_load.insert(active_w_id, 2);

        // Enqueue 2 ActorCreation tasks (standby cannot run these)
        let actor_task1 = Task {
            id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            kind: TaskKind::ActorCreation {
                actor_id: Uuid::new_v4(),
                class_name: "TestActor".to_string(),
            },
            payload: vec![],
            runtime_env: None,
            dependencies: vec![],
        };
        let actor_task2 = Task {
            id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            kind: TaskKind::ActorCreation {
                actor_id: Uuid::new_v4(),
                class_name: "TestActor".to_string(),
            },
            payload: vec![],
            runtime_env: None,
            dependencies: vec![],
        };
        ctx.dispatcher.enqueue(actor_task1);
        ctx.dispatcher.enqueue(actor_task2);

        // Should NOT trigger scale out because only ActorCreation tasks are pending
        ctx.check_and_scale_out().await;
        assert_eq!(ctx.gcs.read().nodes.get(&standby_w_id).unwrap().status, NodeStatus::Standby);
        assert!(standby_rx.try_recv().is_err());

        // Now enqueue 2 Map tasks (stateless)
        ctx.dispatcher.enqueue(dummy_task());
        ctx.dispatcher.enqueue(dummy_task());

        // Should trigger scale out now!
        ctx.check_and_scale_out().await;
        assert_eq!(ctx.gcs.read().nodes.get(&standby_w_id).unwrap().status, NodeStatus::Active);
        let msg = standby_rx.recv().await.expect("Expected WakeUp");
        assert_eq!(msg, Message::WakeUp);

        let _ = tokio::fs::remove_file(wal_path).await;
    }

    #[tokio::test]
    async fn test_speculative_duplicate_execution_cancels_secondary_worker() {
        let (ctx, wal_path) = setup_test_context(HeadNodeConfig::default()).await;

        let w1_id = Uuid::new_v4();
        let w2_id = Uuid::new_v4();
        let (w1_tx, _w1_rx) = mpsc::channel(10);
        let (w2_tx, mut w2_rx) = mpsc::channel(10);

        ctx.handle_report_ready(
            w1_id,
            NodeCapacity { physical_cpus: 4, total_ram_mb: 8192 },
            None,
            "127.0.0.1:9001",
            false,
            &w1_tx,
        ).await;

        ctx.handle_report_ready(
            w2_id,
            NodeCapacity { physical_cpus: 4, total_ram_mb: 8192 },
            None,
            "127.0.0.1:9002",
            false,
            &w2_tx,
        ).await;

        // Drain initial handshake messages (RegisterAck and WakeUp) from Worker 2
        let _ = w2_rx.recv().await; // RegisterAck
        let _ = w2_rx.recv().await; // WakeUp

        let job_id = Uuid::new_v4();
        let task = dummy_task();
        let task_id = task.id;

        // Register job and active task currently assigned to Worker 2
        {
            let mut state = ctx.gcs.write();
            state.jobs.insert(
                job_id,
                JobRecord {
                    job: Job { id: job_id, name: "SpeculativeJob".to_string() },
                    status: JobStatus::Running,
                    tasks: vec![task_id],
                    results: HashMap::new(),
                },
            );
            state.active_tasks.insert(
                task_id,
                TaskRecord {
                    task: task.clone(),
                    status: TaskStatus::Running,
                    assigned_to: Some(w2_id),
                    retry_count: 0,
                    output_object_id: None,
                },
            );
            state.worker_load.insert(w2_id, 1);
        }

        // Worker 1 (which computed task before blip) reconnects and delivers result!
        let result_payload = vec![1, 2, 3, 4];
        let msg = Message::TaskSuccess {
            task_id,
            job_id,
            output: result_payload.clone(),
            object_id: None,
            observed_bandwidth_mbps: None,
        };
        ctx.handle_message(msg, &w1_tx, "127.0.0.1:9001", Some(w1_id)).await;

        // Verify task was recorded as Completed
        {
            let state = ctx.gcs.read();
            let job = state.jobs.get(&job_id).expect("Job should exist");
            assert_eq!(job.status, JobStatus::Completed);
            assert_eq!(job.results.get(&task_id).unwrap(), &result_payload);
        }

        // Verify Worker 2 received CancelTask so it doesn't waste CPU!
        let cancel_msg = w2_rx.recv().await.expect("Expected CancelTask for W2");
        assert_eq!(cancel_msg, Message::CancelTask { task_id });

        let _ = tokio::fs::remove_file(wal_path).await;
    }

    #[tokio::test]
    async fn test_stale_task_failure_does_not_overwrite_completed_task() {
        let (ctx, wal_path) = setup_test_context(HeadNodeConfig::default()).await;

        let w1_id = Uuid::new_v4();
        let w2_id = Uuid::new_v4();
        let (w1_tx, _w1_rx) = mpsc::channel(10);
        let (w2_tx, _w2_rx) = mpsc::channel(10);

        ctx.handle_report_ready(
            w1_id,
            NodeCapacity { physical_cpus: 4, total_ram_mb: 8192 },
            None,
            "127.0.0.1:9001",
            false,
            &w1_tx,
        ).await;

        ctx.handle_report_ready(
            w2_id,
            NodeCapacity { physical_cpus: 4, total_ram_mb: 8192 },
            None,
            "127.0.0.1:9002",
            false,
            &w2_tx,
        ).await;

        let job_id = Uuid::new_v4();
        let task = dummy_task();
        let task_id = task.id;

        {
            let mut state = ctx.gcs.write();
            state.jobs.insert(
                job_id,
                JobRecord {
                    job: Job { id: job_id, name: "Job".to_string() },
                    status: JobStatus::Completed,
                    tasks: vec![task_id],
                    results: HashMap::from([(task_id, vec![1, 2, 3])]),
                },
            );
            state.active_tasks.insert(
                task_id,
                TaskRecord {
                    task: task.clone(),
                    status: TaskStatus::Completed,
                    assigned_to: Some(w2_id),
                    retry_count: 0,
                    output_object_id: None,
                },
            );
            state.worker_load.insert(w2_id, 0);
        }

        // Late failure arrives from W1
        let fail_msg = Message::TaskFailure {
            task_id,
            job_id,
            error: "Connection broken".to_string(),
        };
        ctx.handle_message(fail_msg, &w1_tx, "127.0.0.1:9001", Some(w1_id)).await;

        // Verify task is still Completed and job is still Completed
        {
            let state = ctx.gcs.read();
            assert_eq!(state.active_tasks.get(&task_id).unwrap().status, TaskStatus::Completed);
            assert_eq!(state.jobs.get(&job_id).unwrap().status, JobStatus::Completed);
            assert_eq!(state.worker_load.get(&w2_id).copied().unwrap_or(0), 0);
        }

        let _ = tokio::fs::remove_file(wal_path).await;
    }

    #[tokio::test]
    async fn test_worker_disconnect_transitions_to_offline_and_preserves_objects() {
        let (ctx, wal_path) = setup_test_context(HeadNodeConfig::default()).await;

        let w1_id = Uuid::new_v4();
        let (w1_tx, _w1_rx) = mpsc::channel(10);

        ctx.handle_report_ready(
            w1_id,
            NodeCapacity { physical_cpus: 4, total_ram_mb: 8192 },
            None,
            "127.0.0.1:9001",
            false,
            &w1_tx,
        ).await;

        let job_id = Uuid::new_v4();
        let task = dummy_task();
        let task_id = task.id;
        let cached_object_id = Uuid::new_v4();

        {
            let mut state = ctx.gcs.write();
            state.jobs.insert(
                job_id,
                JobRecord {
                    job: Job { id: job_id, name: "Job".to_string() },
                    status: JobStatus::Running,
                    tasks: vec![task_id],
                    results: HashMap::new(),
                },
            );
            state.active_tasks.insert(
                task_id,
                TaskRecord {
                    task: task.clone(),
                    status: TaskStatus::Running,
                    assigned_to: Some(w1_id),
                    retry_count: 0,
                    output_object_id: None,
                },
            );
            state.worker_load.insert(w1_id, 1);
            state.object_catalog.insert(
                cached_object_id,
                ObjectLocation {
                    object_id: cached_object_id,
                    size_bytes: 1024,
                    nodes: HashSet::from([w1_id]),
                },
            );
        }

        // Simulate socket disconnect (enters grace period)
        ctx.handle_worker_disconnect(w1_id).await;

        // Verify:
        // 1. Worker status is Offline
        // 2. Running task is requeued as Pending
        // 3. Cached object is still in object_catalog (grace period active)
        {
            let state = ctx.gcs.read();
            assert_eq!(state.nodes.get(&w1_id).unwrap().status, NodeStatus::Offline);
            assert_eq!(state.active_tasks.get(&task_id).unwrap().status, TaskStatus::Pending);
            assert_eq!(state.active_tasks.get(&task_id).unwrap().assigned_to, None);
            assert!(state.object_catalog.contains_key(&cached_object_id));
        }

        let _ = tokio::fs::remove_file(wal_path).await;
    }

    #[tokio::test]
    async fn test_dispatch_skips_already_completed_tasks() {
        let (ctx, wal_path) = setup_test_context(HeadNodeConfig::default()).await;

        let w1_id = Uuid::new_v4();
        let (w1_tx, mut w1_rx) = mpsc::channel(10);

        ctx.handle_report_ready(
            w1_id,
            NodeCapacity { physical_cpus: 4, total_ram_mb: 8192 },
            None,
            "127.0.0.1:9001",
            false,
            &w1_tx,
        ).await;

        let _ = w1_rx.recv().await; // Drain RegisterAck
        let _ = w1_rx.recv().await; // Drain WakeUp

        let job_id = Uuid::new_v4();
        let task = dummy_task();
        let task_id = task.id;

        {
            let mut state = ctx.gcs.write();
            state.jobs.insert(
                job_id,
                JobRecord {
                    job: Job { id: job_id, name: "Job".to_string() },
                    status: JobStatus::Running,
                    tasks: vec![task_id],
                    results: HashMap::new(),
                },
            );
            // Task was already completed by reconnected worker
            state.active_tasks.insert(
                task_id,
                TaskRecord {
                    task: task.clone(),
                    status: TaskStatus::Completed,
                    assigned_to: Some(Uuid::new_v4()),
                    retry_count: 0,
                    output_object_id: None,
                },
            );
        }

        // Stale task left in queue from disconnect
        ctx.dispatcher.requeue_priority(task);

        // Try dispatch to w1
        ctx.try_dispatch_to_worker(w1_id, &w1_tx).await;

        // W1 should NOT receive AssignTask
        assert!(w1_rx.try_recv().is_err());

        // W1 load should be 0
        assert_eq!(ctx.gcs.read().worker_load.get(&w1_id).copied().unwrap_or(0), 0);

        // Task record should remain Completed
        assert_eq!(ctx.gcs.read().active_tasks.get(&task_id).unwrap().status, TaskStatus::Completed);

        let _ = tokio::fs::remove_file(wal_path).await;
    }
}


