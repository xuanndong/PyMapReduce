use crate::gcs::store::Gcs;
use crate::scheduler::dispatcher::Dispatcher;
use crate::types::node::WorkerId;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::types::job::JobId;
use crate::protocol::message::Message;
use dashmap::DashMap;
use tokio::sync::mpsc::Sender;

const HEARTBEAT_TIMEOUT_SECS: u64 = 30;
const MAX_SNOOZE_COUNT: u32 = 1;

pub struct HealthMonitor {
    gcs: Arc<Gcs>,
    dispatcher: Arc<Dispatcher>,
    driver_connections: Arc<DashMap<JobId, Sender<Message>>>,
    pub last_heartbeat: HashMap<WorkerId, Instant>,
    snooze_counts: HashMap<WorkerId, u32>,
}

impl HealthMonitor {
    pub fn new(
        gcs: Arc<Gcs>,
        dispatcher: Arc<Dispatcher>,
        driver_connections: Arc<DashMap<JobId, Sender<Message>>>,
    ) -> Self {
        Self {
            gcs,
            dispatcher,
            driver_connections,
            last_heartbeat: HashMap::new(),
            snooze_counts: HashMap::new(),
        }
    }

    pub fn record_heartbeat(&mut self, worker_id: WorkerId) {
        self.last_heartbeat.insert(worker_id, Instant::now());
        self.snooze_counts.insert(worker_id, 0); // Reset snooze count
    }

    pub async fn remove_dead_worker(&mut self, worker_id: WorkerId) {
        self.last_heartbeat.remove(&worker_id);
        self.snooze_counts.remove(&worker_id);

        let mut tasks_to_requeue = Vec::new();
        let mut actor_tasks_to_fail = Vec::new();

        // Single write lock scope: remove node, collect tasks, mark dead actors, drain mailboxes
        {
            let mut state = self.gcs.write();
            state.nodes.remove(&worker_id);
            state.worker_load.remove(&worker_id);

            // Re-queue pending and running tasks that were on the dead worker
            for task_record in state.active_tasks.values_mut() {
                if task_record.assigned_to == Some(worker_id)
                    && (task_record.status == crate::types::task::TaskStatus::Running
                        || task_record.status == crate::types::task::TaskStatus::Pending)
                {
                    task_record.assigned_to = None;
                    task_record.status = crate::types::task::TaskStatus::Pending;
                    tasks_to_requeue.push(task_record.task.clone());
                }
            }

            // Lineage Recomputation: Detect lost intermediate objects and recompute producer tasks
            let mut lost_objects = Vec::new();
            for (oid, loc) in state.object_catalog.iter_mut() {
                loc.nodes.remove(&worker_id);
                if loc.nodes.is_empty() {
                    lost_objects.push(*oid);
                }
            }
            for oid in &lost_objects {
                state.object_catalog.remove(oid);
            }

            if !lost_objects.is_empty() {
                for task_record in state.active_tasks.values_mut() {
                    if let Some(oid) = task_record.output_object_id {
                        if lost_objects.contains(&oid) && task_record.status == crate::types::task::TaskStatus::Completed {
                            task_record.assigned_to = None;
                            task_record.status = crate::types::task::TaskStatus::Pending;
                            tasks_to_requeue.push(task_record.task.clone());
                        }
                    }
                }
            }
            
            // Handle actors hosted on this crashed worker
            let has_other_workers = !state.nodes.is_empty();
            let mut dead_actors = Vec::new();
            for actor_record in state.actors.values_mut() {
                if actor_record.worker_id == Some(worker_id) {
                    if has_other_workers {
                        actor_record.status = crate::gcs::state::ActorStatus::Creating;
                        actor_record.worker_id = None;
                        let recreation_task = crate::types::task::Task {
                            id: uuid::Uuid::new_v4(),
                            job_id: uuid::Uuid::nil(),
                            kind: crate::types::task::TaskKind::ActorCreation {
                                actor_id: actor_record.actor_id,
                                class_name: actor_record.class_name.clone(),
                            },
                            payload: actor_record.creation_payload.clone(),
                            runtime_env: None,
                            dependencies: Vec::new(),
                        };
                        tasks_to_requeue.push(recreation_task);
                    } else {
                        actor_record.status = crate::gcs::state::ActorStatus::Dead("Worker crashed".to_string());
                        dead_actors.push(actor_record.actor_id);
                    }
                }
            }

            // Drain mailboxes only for permanently dead actors
            for actor_id in dead_actors {
                let stuck_tasks = self.dispatcher.drain_actor_mailbox(&actor_id);
                actor_tasks_to_fail.extend(stuck_tasks);
            }
        }
        // Write lock is dropped here

        // Send failure message for stuck actor tasks to driver (no GCS lock held)
        for task in &actor_tasks_to_fail {
            if let Some(driver_tx) = self.driver_connections.get(&task.job_id) {
                let _ = driver_tx.value().send(Message::TaskFailure {
                    task_id: task.id,
                    job_id: task.job_id,
                    error: "Actor Dead (Worker crashed)".to_string(),
                }).await;
            }
        }

        // Second write lock scope: mark actor tasks as failed in GCS
        if !actor_tasks_to_fail.is_empty() {
            let mut state = self.gcs.write();
            for task in actor_tasks_to_fail {
                if let Some(record) = state.active_tasks.get_mut(&task.id) {
                    record.status = crate::types::task::TaskStatus::Failed("Actor Dead (Worker crashed)".to_string());
                }
            }
        }

        for task in tasks_to_requeue {
            self.dispatcher.requeue_priority(task);
        }
    }

    pub async fn scan_and_recover(&mut self) {
        let now = Instant::now();
        let timeout = Duration::from_secs(HEARTBEAT_TIMEOUT_SECS);
        let mut dead_workers = Vec::new();

        for (&worker_id, &last_seen) in &self.last_heartbeat {
            if now.duration_since(last_seen) > timeout {
                let snooze = self.snooze_counts.entry(worker_id).or_insert(0);
                if *snooze < MAX_SNOOZE_COUNT {
                    *snooze += 1;
                } else {
                    dead_workers.push(worker_id);
                }
            }
        }

        for worker_id in dead_workers {
            self.remove_dead_worker(worker_id).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::strategy::RoundRobinStrategy;
    use crate::types::task::{Task, TaskKind};
    use uuid::Uuid;

    #[tokio::test]
    async fn test_circuit_breaker() {
        let strategy = Box::new(RoundRobinStrategy::new());
        let dispatcher = Arc::new(Dispatcher::new(strategy));
        let gcs = Arc::new(Gcs::new());
        let driver_connections = Arc::new(DashMap::new());

        let mut monitor = HealthMonitor::new(gcs.clone(), dispatcher.clone(), driver_connections);
        let worker_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();

        monitor
            .last_heartbeat
            .insert(worker_id, Instant::now() - Duration::from_secs(40));

        let task = Task {
            id: task_id,
            job_id: Uuid::new_v4(),
            kind: TaskKind::Map,
            payload: vec![],
            runtime_env: None,
            dependencies: vec![],
        };

        {
            let mut state = gcs.write();
            state.nodes.insert(
                worker_id,
                crate::types::node::NodeInfo {
                    id: worker_id,
                    capacity: crate::types::node::NodeCapacity {
                        physical_cpus: 4,
                        total_ram_mb: 1024,
                    },
                    addr: "".to_string(),
                },
            );
            state.active_tasks.insert(
                task.id,
                crate::gcs::state::TaskRecord {
                    task: task.clone(),
                    status: crate::types::task::TaskStatus::Running,
                    assigned_to: Some(worker_id),
                    retry_count: 0,
                    output_object_id: None,
                },
            );
        }
        monitor.scan_and_recover().await;
        assert!(gcs.read().nodes.contains_key(&worker_id));

        monitor.scan_and_recover().await;
        assert!(!gcs.read().nodes.contains_key(&worker_id));

        let state = gcs.read();
        let dispatched = dispatcher.dispatch(
            &crate::types::node::NodeInfo {
                id: Uuid::new_v4(),
                capacity: crate::types::node::NodeCapacity {
                    physical_cpus: 4,
                    total_ram_mb: 1024,
                },
                addr: "".to_string(),
            },
            &*state,
        );

        assert_eq!(dispatched.unwrap().id, task.id);
    }
}
