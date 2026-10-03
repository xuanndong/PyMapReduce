use crate::fault::monitor::HealthMonitor;
use crate::fault::wal::WriteAheadLog;
use crate::gcs::store::Gcs;
use crate::object_store::store::ObjectStore;
use crate::protocol::codec::MessageCodec;
use crate::protocol::message::Message;
use crate::scheduler::dispatcher::Dispatcher;
use crate::types::error::FrameworkError;
use crate::types::job::JobId;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_util::codec::Framed;
pub struct HeadNode {
    pub gcs: Arc<Gcs>,
    pub dispatcher: Arc<Dispatcher>,
    pub health_monitor: Arc<Mutex<HealthMonitor>>,
    pub wal: Arc<Mutex<WriteAheadLog>>,
    pub object_store: Arc<ObjectStore>,
    pub driver_connections: Arc<DashMap<JobId, tokio::sync::mpsc::Sender<Message>>>,
    pub worker_connections:
        Arc<DashMap<crate::types::node::WorkerId, tokio::sync::mpsc::Sender<Message>>>,
    pub pending_object_gets:
        Arc<DashMap<crate::protocol::message::ObjectId, Vec<tokio::sync::mpsc::Sender<Message>>>>,
    pub shutdown_tx: tokio::sync::broadcast::Sender<()>,
    addr: String,
}

impl HeadNode {
    pub fn new(
        gcs: Arc<Gcs>,
        dispatcher: Arc<Dispatcher>,
        wal: WriteAheadLog,
        addr: String,
    ) -> Self {
        let driver_connections = Arc::new(DashMap::new());
        let health_monitor = crate::fault::monitor::HealthMonitor::new(
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

    pub async fn recover_from_wal(&self) -> Result<(), FrameworkError> {
        // Step 1: Load GCS Snapshot if exists
        let snapshot_path = std::path::PathBuf::from("/tmp/pymapreduce/snapshot.bin");
        if snapshot_path.exists() {
            if let Ok(data) = tokio::fs::read(&snapshot_path).await {
                if let Ok(snapshot_state) = bincode::deserialize::<crate::gcs::state::GcsState>(&data) {
                    *self.gcs.write() = snapshot_state;
                    println!("[Disaster Recovery] Loaded GCS Snapshot successfully!");
                }
            }
        }

        // Step 2: Replay WAL for any events that happened after the snapshot
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
                        if let Ok(task) = bincode::deserialize::<crate::types::task::Task>(&task_bytes) {
                            task_ids.push(task.id);
                            if let crate::types::task::TaskKind::ActorCreation { actor_id, class_name } = &task.kind {
                                state.actors.insert(
                                    *actor_id,
                                    crate::gcs::state::ActorRecord {
                                        actor_id: *actor_id,
                                        class_name: class_name.clone(),
                                        worker_id: None,
                                        status: crate::gcs::state::ActorStatus::Creating,
                                        creation_payload: task.payload.clone(),
                                    }
                                );
                            }
                            state.active_tasks.insert(
                                task.id,
                                crate::gcs::state::TaskRecord {
                                    task: task.clone(),
                                    status: crate::types::task::TaskStatus::Pending,
                                    assigned_to: None,
                                    retry_count: 0,
                                    output_object_id: None,
                                },
                            );
                        }
                    }
                    state.jobs.insert(
                        job_id,
                        crate::gcs::state::JobRecord {
                            job: crate::types::job::Job {
                                id: job_id,
                                name: "Job".to_string(),
                            },
                            status: crate::types::job::JobStatus::Running,
                            tasks: task_ids,
                            results: std::collections::HashMap::new(),
                        },
                    );
                }
                Message::TaskSuccess { task_id, job_id, output, object_id, .. } => {
                    let actor_update = if let Some(record) = state.active_tasks.get_mut(&task_id) {
                        record.status = crate::types::task::TaskStatus::Completed;
                        match &record.task.kind {
                            crate::types::task::TaskKind::ActorCreation { actor_id, .. } => {
                                Some((*actor_id, true, record.assigned_to))
                            }
                            crate::types::task::TaskKind::ActorDestruction { actor_id } => {
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
                                actor_rec.status = crate::gcs::state::ActorStatus::Alive;
                                actor_rec.worker_id = assigned_to;
                            } else {
                                actor_rec.status = crate::gcs::state::ActorStatus::Dead("Explicitly destroyed".to_string());
                            }
                        }
                    }
                    
                    if let Some(oid) = object_id {
                        let _entry = state.object_catalog.entry(oid).or_insert(
                            crate::gcs::state::ObjectLocation {
                                object_id: oid,
                                size_bytes: 0,
                                nodes: std::collections::HashSet::new(),
                            },
                        );
                    }

                    if let Some(job_record) = state.jobs.get_mut(&job_id) {
                        job_record.results.insert(task_id, output);
                        if job_record.results.len() == job_record.tasks.len() {
                            job_record.status = crate::types::job::JobStatus::Completed;
                        }
                    }
                }
                Message::TaskFailure { task_id, job_id, error } => {
                    if let Some(record) = state.active_tasks.get_mut(&task_id) {
                        record.status = crate::types::task::TaskStatus::Failed(error.clone());
                    }
                    if let Some(job_record) = state.jobs.get_mut(&job_id) {
                        job_record.status = crate::types::job::JobStatus::Failed(error.clone());
                    }
                }
                _ => {}
            }
        }

        // Restore Dispatcher Queue with tasks that are still Pending
        let mut queued_count = 0;
        for record in state.active_tasks.values() {
            if record.status == crate::types::task::TaskStatus::Pending {
                self.dispatcher.enqueue(record.task.clone());
                queued_count += 1;
            }
        }

        println!("[Disaster Recovery] WAL Recovery complete. Re-queued {} pending tasks.", queued_count);
        Ok(())
    }

    pub async fn run(mut self) -> Result<(), FrameworkError> {
        // Start background checkpointing task (Phase 2 Disaster Recovery)
        let driver_connections: Arc<DashMap<JobId, tokio::sync::mpsc::Sender<Message>>> = Arc::new(DashMap::new());
        let worker_connections: Arc<DashMap<crate::types::node::WorkerId, tokio::sync::mpsc::Sender<Message>>> = Arc::new(DashMap::new());

        let health_monitor = Arc::new(tokio::sync::Mutex::new(crate::fault::monitor::HealthMonitor::new(
            self.gcs.clone(),
            self.dispatcher.clone(),
            driver_connections.clone(),
        )));

        self.driver_connections = driver_connections.clone();
        self.worker_connections = worker_connections.clone();
        self.health_monitor = health_monitor.clone();

        let wal_clone = self.wal.clone();
        let gcs_clone = self.gcs.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(300)); // Every 5 mins
            loop {
                interval.tick().await;
                let snapshot = {
                    let state = gcs_clone.read();
                    bincode::serialize(&*state).unwrap_or_default()
                };
                if !snapshot.is_empty() {
                    let tmp_path = std::path::PathBuf::from("/tmp/pymapreduce/snapshot.bin.tmp");
                    let snapshot_path = std::path::PathBuf::from("/tmp/pymapreduce/snapshot.bin");
                    
                    // Write to tmp file first
                    if let Ok(_) = tokio::fs::write(&tmp_path, snapshot).await {
                        // Atomic rename prevents corrupted snapshot file if crash happens during write
                        if let Ok(_) = tokio::fs::rename(&tmp_path, &snapshot_path).await {
                            let mut wal_lock = wal_clone.lock().await;
                            let _ = wal_lock.truncate().await;
                            println!("[Disaster Recovery] Checkpoint created. WAL truncated.");
                        }
                    }
                }
            }
        });

        // Recover state from WAL before accepting new connections
        self.recover_from_wal().await?;

        let listener = TcpListener::bind(&self.addr)
            .await
            .map_err(|e| FrameworkError::Network(e.to_string()))?;

        let health_monitor = self.health_monitor.clone();

        // Spawn health monitor loop
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
            loop {
                interval.tick().await;
                let mut monitor = health_monitor.lock().await;
                monitor.scan_and_recover().await;
            }
        });

        let mut shutdown_rx = self.shutdown_tx.subscribe();

        // Accept connections
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

                    let gcs = self.gcs.clone();
                    let dispatcher = self.dispatcher.clone();
                    let health_monitor = self.health_monitor.clone();
                    let wal = self.wal.clone();
                    let object_store = self.object_store.clone();
                    let driver_connections = self.driver_connections.clone();
                    let worker_connections = self.worker_connections.clone();
                    let pending_object_gets = self.pending_object_gets.clone();
                    let shutdown_tx = self.shutdown_tx.clone();

                    let (tx, mut rx) = tokio::sync::mpsc::channel::<Message>(100);
                    let (mut sink, mut stream) = framed.split();

                    // Writer task
                    tokio::spawn(async move {
                        while let Some(msg) = rx.recv().await {
                            let _ = sink.send(msg).await;
                        }
                    });

                    // Reader task
                    tokio::spawn(async move {
                        let mut current_worker_id = None;
                        let peer_addr = peer_addr_str;

                        while let Some(msg_result) = stream.next().await {
                            let Ok(msg) = msg_result else { break; };

                            if let Message::ReportReady { worker_id, .. } = &msg {
                                current_worker_id = Some(*worker_id);
                            }

                            let shutdown_tx = shutdown_tx.clone();
                            Self::handle_message(
                                msg, &gcs, &dispatcher, &health_monitor, &wal,
                                &object_store, &driver_connections, &worker_connections, &tx, &pending_object_gets, &shutdown_tx, &peer_addr
                            ).await;

                            // Try to schedule a task if this connection is a worker
                            if let Some(w_id) = current_worker_id {
                                Self::try_dispatch_to_worker(w_id, &gcs, &dispatcher, &tx).await;
                            }
                        }

                        // TCP Connection closed or error occurred. Clean up immediately!
                        if let Some(w_id) = current_worker_id {
                            println!("Worker {} disconnected. Cleaning up immediately.", w_id);
                            worker_connections.remove(&w_id);
                            health_monitor.lock().await.remove_dead_worker(w_id).await;
                            
                            // Wake up all remaining workers so they can pick up the requeued tasks!
                            let shutdown_tx = shutdown_tx.clone();
                            Self::handle_message(
                                Message::WakeUp, &gcs, &dispatcher, &health_monitor, &wal,
                                &object_store, &driver_connections, &worker_connections, &tx, &pending_object_gets, &shutdown_tx, &peer_addr
                            ).await;
                        }
                    });
                }
            }
        }
        Ok(())
    }

    async fn try_dispatch_to_worker(
        w_id: crate::types::node::WorkerId,
        gcs: &Arc<Gcs>,
        dispatcher: &Arc<Dispatcher>,
        tx: &tokio::sync::mpsc::Sender<Message>,
    ) {
        let worker_info = {
            let state = gcs.read();
            state.nodes.get(&w_id).cloned()
        };
        if let Some(info) = worker_info {
            let max_capacity = (info.capacity.physical_cpus as usize) * 2;
            loop {
                let current_load = {
                    let state = gcs.read();
                    state.worker_load.get(&w_id).copied().unwrap_or(0)
                };
                
                if current_load >= max_capacity {
                    break;
                }

                let task_opt = {
                    let state = gcs.read();
                    dispatcher.dispatch(&info, &state)
                };
                if let Some(task) = task_opt {
                    {
                        let mut state = gcs.write();
                        if let Some(record) = state.active_tasks.get_mut(&task.id) {
                            record.assigned_to = Some(w_id);
                            record.status = crate::types::task::TaskStatus::Running;
                        }
                        *state.worker_load.entry(w_id).or_insert(0) += 1;
                    }
                    let _ = tx
                        .send(Message::AssignTask {
                            task: Box::new(task),
                        })
                        .await;
                } else {
                    break;
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_message(
        msg: Message,
        gcs: &Arc<Gcs>,
        dispatcher: &Arc<Dispatcher>,
        health_monitor: &Arc<Mutex<HealthMonitor>>,
        wal: &Arc<Mutex<WriteAheadLog>>,
        object_store: &Arc<ObjectStore>,
        driver_connections: &Arc<DashMap<JobId, tokio::sync::mpsc::Sender<Message>>>,
        worker_connections: &Arc<
            DashMap<crate::types::node::WorkerId, tokio::sync::mpsc::Sender<Message>>,
        >,
        tx: &tokio::sync::mpsc::Sender<Message>,
        pending_object_gets: &Arc<
            DashMap<
                crate::protocol::message::ObjectId,
                Vec<tokio::sync::mpsc::Sender<Message>>,
            >,
        >,
        shutdown_tx: &tokio::sync::broadcast::Sender<()>,
        peer_addr: &str,
    ) {
        // Log to WAL (only state-changing messages, skip transient ones like Heartbeat)
        {
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
                let mut wal_lock = wal.lock().await;
                let _ = wal_lock.append(&msg).await;
            }
        }

        // Process message
        match msg {
            Message::ReportReady {
                worker_id,
                capacity,
                listener_addr,
                ..
            } => {
                worker_connections.insert(worker_id, tx.clone());
                let actual_addr = listener_addr.unwrap_or_else(|| peer_addr.to_string());
                {
                    let mut state = gcs.write();
                    state.nodes.insert(
                        worker_id,
                        crate::types::node::NodeInfo {
                            id: worker_id,
                            capacity,
                            addr: actual_addr,
                        },
                    );
                    state.worker_load.entry(worker_id).or_insert(0);
                }
                let _ = tx.send(Message::WakeUp).await;
            }
            Message::AttachJob { job_id } => {
                driver_connections.insert(job_id, tx.clone());
                let response = {
                    let state = gcs.read();
                    state.jobs.get(&job_id).and_then(|job_record| {
                        match &job_record.status {
                            crate::types::job::JobStatus::Completed => {
                                let mut results = Vec::new();
                                for task_id in &job_record.tasks {
                                    if let Some(res) = job_record.results.get(task_id) {
                                        results.push(res.clone());
                                    }
                                }
                                Some(Message::JobComplete { job_id, results })
                            }
                            crate::types::job::JobStatus::Failed(reason) => {
                                Some(Message::JobFailed { job_id, reason: reason.clone() })
                            }
                            _ => None,
                        }
                    })
                };
                if let Some(msg) = response {
                    let _ = tx.send(msg).await;
                }
            }
            Message::WorkerDraining { worker_id } => {
                let mut state = gcs.write();
                state.nodes.remove(&worker_id);
            }
            Message::TaskStatusResponse { task_id, job_id, status } => {
                match status {
                    crate::protocol::message::TaskReconcileStatus::Completed { output, .. } => {
                        let mut state = gcs.write();
                        if let Some(record) = state.active_tasks.get_mut(&task_id) {
                            record.status = crate::types::task::TaskStatus::Completed;
                        }
                        if let Some(job_record) = state.jobs.get_mut(&job_id) {
                            job_record.results.insert(task_id, output);
                            if job_record.results.len() == job_record.tasks.len() {
                                job_record.status = crate::types::job::JobStatus::Completed;
                            }
                        }
                    }
                    crate::protocol::message::TaskReconcileStatus::Failed(error) => {
                        let mut state = gcs.write();
                        if let Some(record) = state.active_tasks.get_mut(&task_id) {
                            record.status = crate::types::task::TaskStatus::Failed(error);
                        }
                    }
                    _ => {}
                }
            }
            Message::GetJobStatus { job_id } => {
                let status_msg = {
                    let state = gcs.read();
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
            Message::Heartbeat { worker_id } => {
                let mut monitor = health_monitor.lock().await;
                monitor.record_heartbeat(worker_id);
            }
            Message::Shutdown => {
                println!(
                    "HeadNode received Shutdown command from client, broadcasting to cluster."
                );

                // Broadcast to all connected workers
                for mut entry in worker_connections.iter_mut() {
                    let w_tx = entry.value_mut();
                    let _ = w_tx.send(Message::Shutdown).await;
                }

                // Signal HeadNode listener to stop
                let _ = shutdown_tx.send(());
            }
            Message::SubmitJob {
                job_id,
                tasks,
                max_time_secs,
            } => {
                driver_connections.insert(job_id, tx.clone());

                // [ACTOR VALIDATION] Fail immediately if any ActorTask targets a dead or missing actor
                let dead_actor_err = {
                    let state = gcs.read();
                    let mut err = None;
                    for task_bytes in &tasks {
                        if let Ok(task) =
                            bincode::deserialize::<crate::types::task::Task>(task_bytes)
                        {
                            if let crate::types::task::TaskKind::ActorTask { actor_id, .. } = &task.kind {
                                if let Some(actor_rec) = state.actors.get(actor_id) {
                                    if let crate::gcs::state::ActorStatus::Dead(reason) = &actor_rec.status {
                                        err = Some(format!("Actor '{}' is dead: {}", actor_id, reason));
                                        break;
                                    }
                                } else {
                                    err = Some(format!("Actor '{}' not found in cluster registry.", actor_id));
                                    break;
                                }
                            }
                        }
                    }
                    err
                };

                if let Some(err_msg) = dead_actor_err {
                    driver_connections.remove(&job_id);
                    let _ = tx.send(Message::JobFailed { job_id, reason: err_msg }).await;
                    return;
                }

                // [ACTOR DESTRUCTION IDEMPOTENCY] If destroying an actor that is already dead or missing, complete immediately
                let already_dead_destruction = {
                    let state = gcs.read();
                    let mut is_dead = false;
                    if tasks.len() == 1 {
                        if let Ok(task) = bincode::deserialize::<crate::types::task::Task>(&tasks[0]) {
                            if let crate::types::task::TaskKind::ActorDestruction { actor_id } = &task.kind {
                                if let Some(actor_rec) = state.actors.get(actor_id) {
                                    if let crate::gcs::state::ActorStatus::Dead(_) = &actor_rec.status {
                                        is_dead = true;
                                    }
                                } else {
                                    is_dead = true;
                                }
                            }
                        }
                    }
                    is_dead
                };

                if already_dead_destruction {
                    driver_connections.remove(&job_id);
                    let _ = tx.send(Message::JobComplete {
                        job_id,
                        results: vec![vec![0x80, 0x04, 0x88, 0x2e]],
                    }).await;
                    return;
                }

                {
                    let mut state = gcs.write();

                    let mut task_ids = Vec::new();
                    for task_bytes in tasks {
                        if let Ok(task) =
                            bincode::deserialize::<crate::types::task::Task>(&task_bytes)
                        {
                            task_ids.push(task.id);
                            
                            // [ACTOR] If this is an ActorCreation task, register it in GCS
                            if let crate::types::task::TaskKind::ActorCreation { actor_id, class_name } = &task.kind {
                                state.actors.insert(
                                    *actor_id,
                                    crate::gcs::state::ActorRecord {
                                        actor_id: *actor_id,
                                        class_name: class_name.clone(),
                                        worker_id: None,
                                        status: crate::gcs::state::ActorStatus::Creating,
                                        creation_payload: task.payload.clone(),
                                    }
                                );
                            }

                            state.active_tasks.insert(
                                task.id,
                                crate::gcs::state::TaskRecord {
                                    task: task.clone(),
                                    status: crate::types::task::TaskStatus::Pending,
                                    assigned_to: None,
                                    retry_count: 0,
                                    output_object_id: None,
                                },
                            );
                            dispatcher.enqueue(task);
                        }
                    }

                    state.jobs.insert(
                        job_id,
                        crate::gcs::state::JobRecord {
                            job: crate::types::job::Job {
                                id: job_id,
                                name: "Job".to_string(),
                            },
                            status: crate::types::job::JobStatus::Running,
                            tasks: task_ids,
                            results: std::collections::HashMap::new(),
                        },
                    );
                }

                let _ = tx.send(Message::JobAccepted { job_id }).await;

                // Trigger scheduling for all workers
                for worker_entry in worker_connections.iter() {
                    let w_id = *worker_entry.key();
                    let w_tx = worker_entry.value();
                    Self::try_dispatch_to_worker(w_id, gcs, dispatcher, w_tx).await;
                }

                // [TIMEOUT] Spawn job timeout watchdog if max_time_secs is set
                if let Some(timeout_secs) = max_time_secs {
                    let gcs_timeout = gcs.clone();
                    let driver_conns_timeout = driver_connections.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(std::time::Duration::from_secs(timeout_secs)).await;
                        
                        // Check if job is still running
                        let should_fail = {
                            let state = gcs_timeout.read();
                            state.jobs.get(&job_id)
                                .map(|j| j.status == crate::types::job::JobStatus::Running)
                                .unwrap_or(false)
                        };
                        
                        if should_fail {
                            println!("[Timeout] Job {} exceeded time limit of {}s", job_id, timeout_secs);
                            {
                                let mut state = gcs_timeout.write();
                                if let Some(job_record) = state.jobs.get_mut(&job_id) {
                                    job_record.status = crate::types::job::JobStatus::Failed(
                                        format!("Job timed out after {}s", timeout_secs)
                                    );
                                }
                            }
                            if let Some(driver_tx) = driver_conns_timeout.remove(&job_id) {
                                let _ = driver_tx.1.send(Message::JobFailed {
                                    job_id,
                                    reason: format!("Job timed out after {}s", timeout_secs),
                                }).await;
                            }
                        }
                    });
                }
            }
            Message::TaskSuccess {
                task_id,
                job_id,
                output,
                object_id,
                observed_bandwidth_mbps,
            } => {
                let mut job_finished = false;
                let mut job_results = Vec::new();
                let mut notify_worker = None;
                let mut effective_job_id = job_id;

                {
                    let mut state = gcs.write();
                    let (assigned_worker, actor_to_alive) = if let Some(record) = state.active_tasks.get_mut(&task_id) {
                        record.status = crate::types::task::TaskStatus::Completed;
                        record.output_object_id = object_id;
                        let actor_info = if let crate::types::task::TaskKind::ActorCreation { actor_id, .. } = record.task.kind {
                            record.assigned_to.map(|w_id| (actor_id, w_id))
                        } else {
                            None
                        };
                        (record.assigned_to, actor_info)
                    } else {
                        (None, None)
                    };

                    if let Some(w_id) = assigned_worker {
                        if let Some(load) = state.worker_load.get_mut(&w_id) {
                            *load = load.saturating_sub(1);
                        }
                    }

                    if let Some((a_id, w_id)) = actor_to_alive {
                        if let Some(actor_rec) = state.actors.get_mut(&a_id) {
                            actor_rec.status = crate::gcs::state::ActorStatus::Alive;
                            actor_rec.worker_id = Some(w_id);
                        }
                    }

                    let actor_destruction_id = if let Some(record) = state.active_tasks.get(&task_id) {
                        if let crate::types::task::TaskKind::ActorDestruction { actor_id } = &record.task.kind {
                            Some(*actor_id)
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    if let Some(actor_id) = actor_destruction_id {
                        if let Some(actor_rec) = state.actors.get_mut(&actor_id) {
                            actor_rec.status = crate::gcs::state::ActorStatus::Dead("Explicitly destroyed".to_string());
                        }
                    }

                    if let Some(worker_id) = assigned_worker {
                        if let Some(bw) = observed_bandwidth_mbps {
                            let alpha = crate::gcs::store::EMA_ALPHA;
                            let old = state.bandwidth_ema.get(&worker_id).cloned().unwrap_or(bw);
                            state.bandwidth_ema.insert(worker_id, alpha * bw + (1.0 - alpha) * old);
                        }

                        // Track Object Location in GCS!
                        if let Some(oid) = object_id {
                            let _entry = state.object_catalog.entry(oid).or_insert(
                                crate::gcs::state::ObjectLocation {
                                    object_id: oid,
                                    size_bytes: 0,
                                    nodes: std::collections::HashSet::new(),
                                },
                            );
                            _entry.nodes.insert(worker_id);
                            
                            // [PUB/SUB] If anyone is waiting for this object, tell the worker to send it!
                            if pending_object_gets.contains_key(&oid) {
                                notify_worker = Some((worker_id, oid));
                            }
                        }
                    }

                    if effective_job_id.is_nil() {
                        if let Some(record) = state.active_tasks.get(&task_id) {
                            effective_job_id = record.task.job_id;
                        }
                    }

                    let mut tasks_to_purge = Vec::new();
                    if let Some(job_record) = state.jobs.get_mut(&effective_job_id) {
                        job_record.results.insert(task_id, output);

                        if job_record.results.len() == job_record.tasks.len() {
                            job_finished = true;
                            job_record.status = crate::types::job::JobStatus::Completed;
                            for tid in &job_record.tasks {
                                if let Some(res) = job_record.results.get(tid) {
                                    job_results.push(res.clone());
                                }
                            }
                            tasks_to_purge = job_record.tasks.clone();
                        }
                    }

                    // Clean up active_tasks for this completed job to prevent memory leak
                    for tid in &tasks_to_purge {
                        state.active_tasks.remove(tid);
                    }
                }

                if let Some((w_id, oid)) = notify_worker {
                    if let Some(worker_tx) = worker_connections.get(&w_id) {
                        let _ = worker_tx.value().send(Message::GetObjectWorker { object_id: oid }).await;
                    }
                }

                if job_finished {
                    let final_job_id = if job_id.is_nil() { effective_job_id } else { job_id };
                    if let Some(driver_tx) = driver_connections.remove(&final_job_id) {
                        let _ = driver_tx
                            .1
                            .send(Message::JobComplete {
                                job_id: final_job_id,
                                results: job_results,
                            })
                            .await;
                    }
                }
            }
            Message::TaskFailure {
                task_id,
                job_id,
                error,
            } => {
                const MAX_TASK_RETRIES: u32 = 3;

                let mut should_requeue = None;
                let mut should_fail_job = false;

                {
                    let mut state = gcs.write();
                    
                    let mut failed_actor = None;
                    let mut assigned_worker = None;
                    if let Some(record) = state.active_tasks.get_mut(&task_id) {
                        assigned_worker = record.assigned_to;
                        if let crate::types::task::TaskKind::ActorCreation { actor_id, .. } = record.task.kind {
                            failed_actor = Some(actor_id);
                        }
                        
                        record.retry_count += 1;
                        
                        if record.retry_count < MAX_TASK_RETRIES {
                            // Still have retries left — requeue
                            record.status = crate::types::task::TaskStatus::Pending;
                            record.assigned_to = None;
                            should_requeue = Some(record.task.clone());
                            println!("[Retry] Task {} failed (attempt {}/{}): {}", task_id, record.retry_count, MAX_TASK_RETRIES, error);
                        } else {
                            // Retry limit exceeded — fail permanently
                            record.status = crate::types::task::TaskStatus::Failed(error.clone());
                            record.assigned_to = None;
                            should_fail_job = true;
                            println!("[Failed] Task {} exceeded retry limit ({}/{}): {}", task_id, record.retry_count, MAX_TASK_RETRIES, error);
                        }
                    }

                    // Decrement worker running load
                    if let Some(w_id) = assigned_worker {
                        if let Some(load) = state.worker_load.get_mut(&w_id) {
                            *load = load.saturating_sub(1);
                        }
                    }

                    // [ACTOR] If ActorCreation failed, mark actor as Dead
                    if let Some(a_id) = failed_actor {
                        if let Some(actor_rec) = state.actors.get_mut(&a_id) {
                            actor_rec.status = crate::gcs::state::ActorStatus::Dead(error.clone());
                        }
                    }

                    // Mark Job as Failed if retry limit exceeded
                    if should_fail_job {
                        if let Some(job_record) = state.jobs.get_mut(&job_id) {
                            job_record.status = crate::types::job::JobStatus::Failed(error.clone());
                        }
                    }
                }

                if let Some(task) = should_requeue {
                    dispatcher.requeue_priority(task);
                } else if should_fail_job {
                    // Notify Driver that the Job has failed
                    if let Some(driver_tx) = driver_connections.remove(&job_id) {
                        let _ = driver_tx.1.send(Message::JobFailed {
                            job_id,
                            reason: format!("Task {} failed after {} retries: {}", task_id, MAX_TASK_RETRIES, error),
                        }).await;
                    }
                }
            }
            Message::PutObject { object_id, data } => {
                let _ = object_store.put(object_id, bytes::Bytes::from(data.clone())).await;
                
                // [PUB/SUB] Broadcast directly if anyone is waiting
                if let Some(subscribers) = pending_object_gets.remove(&object_id) {
                    for driver_tx in subscribers.1 {
                        let _ = driver_tx
                            .send(Message::ObjectData {
                                object_id,
                                chunk: data.clone(),
                                offset: 0,
                            })
                            .await;
                    }
                }
            }
            Message::DeleteObject { object_id } => {
                // Delete locally if it exists
                let _ = object_store.delete(object_id).await;

                // Remove from GCS object catalog
                {
                    let mut state = gcs.write();
                    let _ = state.object_catalog.remove(&object_id);
                }
                
                // If the catalog didn't have it, we might still want to broadcast just in case, 
                // but iterating over worker_connections is safer to ensure it's deleted everywhere.
                for mut entry in worker_connections.iter_mut() {
                    let w_tx = entry.value_mut();
                    let _ = w_tx.send(Message::DeleteObject { object_id }).await;
                }
            }
            Message::GetObject {
                object_id,
                offset,
                len,
            } => {
                if let Ok(data) = object_store.get_any(object_id).await {
                    let chunk = if offset as usize >= data.len() {
                        Vec::new()
                    } else {
                        let end = (offset as usize + len as usize).min(data.len());
                        data[offset as usize..end].to_vec()
                    };
                    let _ = tx
                        .send(Message::ObjectData {
                            object_id,
                            chunk,
                            offset,
                        })
                        .await;
                } else {
                    // Look up in GCS and ask Worker
                    let worker_opt = {
                        let state = gcs.read();
                        state
                            .object_catalog
                            .get(&object_id)
                            .and_then(|loc| loc.nodes.iter().next().cloned())
                    };

                    // [PUB/SUB] Add requester to waiting list regardless of whether object is ready
                    {
                        let mut subscribers = pending_object_gets.entry(object_id).or_insert_with(Vec::new);
                        subscribers.push(tx.clone());
                    }

                    if let Some(worker_id) = worker_opt {
                        if let Some(worker_tx) = worker_connections.get(&worker_id) {
                            let _ = worker_tx
                                .value()
                                .send(Message::GetObjectWorker { object_id })
                                .await;
                        }
                    }
                }
            }
            Message::ObjectDataWorker { object_id, data } => {
                if let Some(subscribers) = pending_object_gets.remove(&object_id) {
                    let chunk = data.unwrap_or_default(); // Empty vec if None
                    for driver_tx in subscribers.1 {
                        let _ = driver_tx
                            .send(Message::ObjectData {
                                object_id,
                                chunk: chunk.clone(),
                                offset: 0,
                            })
                            .await;
                    }
                }
            }

            Message::CancelJob { job_id } => {
                let mut tasks_to_cancel = Vec::new();
                let mut running_tasks_to_cancel = Vec::new();
                {
                    let mut state = gcs.write();
                    
                    // Phase 1: Mark job as failed and collect task IDs
                    let task_ids: Vec<crate::types::task::TaskId> = if let Some(job_record) = state.jobs.get_mut(&job_id) {
                        if job_record.status == crate::types::job::JobStatus::Running {
                            job_record.status = crate::types::job::JobStatus::Failed(
                                "Cancelled by user".to_string()
                            );
                            job_record.tasks.clone()
                        } else {
                            Vec::new()
                        }
                    } else {
                        Vec::new()
                    };
                    
                    // Phase 2: Mark individual tasks as failed and collect workers to decrement load
                    let mut workers_to_decrement = Vec::new();
                    for tid in task_ids {
                        if let Some(task_rec) = state.active_tasks.get_mut(&tid) {
                            if task_rec.status == crate::types::task::TaskStatus::Pending
                                || task_rec.status == crate::types::task::TaskStatus::Running
                            {
                                if task_rec.status == crate::types::task::TaskStatus::Running {
                                    if let Some(w_id) = task_rec.assigned_to {
                                        workers_to_decrement.push(w_id);
                                        running_tasks_to_cancel.push((w_id, tid));
                                    }
                                }
                                task_rec.status = crate::types::task::TaskStatus::Failed(
                                    "Job cancelled".to_string()
                                );
                                tasks_to_cancel.push(tid);
                            }
                        }
                    }

                    for w_id in workers_to_decrement {
                        if let Some(load) = state.worker_load.get_mut(&w_id) {
                            *load = load.saturating_sub(1);
                        }
                    }
                }

                for (w_id, task_id) in running_tasks_to_cancel {
                    if let Some(worker_tx) = worker_connections.get(&w_id) {
                        let _ = worker_tx.send(Message::CancelTask { task_id }).await;
                    }
                }

                // Notify Driver
                if let Some(driver_tx) = driver_connections.remove(&job_id) {
                    let _ = driver_tx.1.send(Message::JobFailed {
                        job_id,
                        reason: "Cancelled by user".to_string(),
                    }).await;
                }
                println!("[Cancel] Job {} cancelled, {} tasks affected", job_id, tasks_to_cancel.len());
            }

            _ => {
                // Unhandled message types
            }
        }
    }
}
