use crate::gcs::state::{ActorStatus, GcsState, TaskRecord};
use crate::gcs::store::Gcs;
use crate::protocol::message::Message;
use crate::scheduler::dispatcher::Dispatcher;
use crate::types::job::{JobId, JobStatus};
use crate::types::node::WorkerId;
use crate::types::task::{Task, TaskKind, TaskStatus};
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Sender;
use uuid::Uuid;

const HEARTBEAT_TIMEOUT_SECS: u64 = 30;
const MAX_SNOOZE_COUNT: u32 = 1;

/// HealthMonitor tracks worker node heartbeats and handles failure recovery.
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
        self.snooze_counts.insert(worker_id, 0);
    }

    /// Identifies and returns worker IDs whose heartbeat has timed out beyond snooze threshold.
    fn detect_dead_workers(&mut self) -> Vec<WorkerId> {
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

        dead_workers
    }

    /// Resets running/pending tasks assigned to the dead worker so they can be rescheduled.
    fn recover_assigned_tasks(
        state: &mut GcsState,
        worker_id: WorkerId,
        dead_jobs: &HashSet<JobId>,
    ) -> Vec<Task> {
        state
            .active_tasks
            .values_mut()
            .filter(|rec| {
                rec.assigned_to == Some(worker_id)
                    && matches!(rec.status, TaskStatus::Running | TaskStatus::Pending)
                    && !dead_jobs.contains(&rec.task.job_id)
            })
            .map(|rec| {
                rec.assigned_to = None;
                rec.status = TaskStatus::Pending;
                rec.task.clone()
            })
            .collect()
    }

    /// Evicts objects lost when the worker died and requeues completed tasks for lineage recomputation.
    fn recover_lost_lineage(
        state: &mut GcsState,
        worker_id: WorkerId,
        dead_jobs: &HashSet<JobId>,
    ) -> Vec<Task> {
        let mut lost_objects = HashSet::new();
        state.object_catalog.retain(|oid, loc| {
            loc.nodes.remove(&worker_id);
            if loc.nodes.is_empty() {
                lost_objects.insert(*oid);
                false
            } else {
                true
            }
        });

        if lost_objects.is_empty() {
            return Vec::new();
        }

        state
            .active_tasks
            .values_mut()
            .filter(|rec| {
                rec.status == TaskStatus::Completed
                    && !dead_jobs.contains(&rec.task.job_id)
                    && rec.output_object_id.is_some_and(|oid| lost_objects.contains(&oid))
            })
            .map(|rec| {
                rec.assigned_to = None;
                rec.status = TaskStatus::Pending;
                rec.task.clone()
            })
            .collect()
    }

    /// Recovers actors assigned to the dead worker, either scheduling recreation or marking them Dead.
    fn recover_actors(
        state: &mut GcsState,
        worker_id: WorkerId,
        dispatcher: &Dispatcher,
    ) -> (Vec<Task>, Vec<Task>) {
        let has_other_workers = !state.nodes.is_empty();
        let mut dead_actors = Vec::new();
        let mut recreation_tasks = Vec::new();

        for actor_record in state.actors.values_mut() {
            if actor_record.worker_id == Some(worker_id) {
                if has_other_workers {
                    actor_record.status = ActorStatus::Creating;
                    actor_record.worker_id = None;
                    let recreation_task = Task {
                        id: Uuid::new_v4(),
                        job_id: Uuid::nil(),
                        kind: TaskKind::ActorCreation {
                            actor_id: actor_record.actor_id,
                            class_name: actor_record.class_name.clone(),
                        },
                        payload: actor_record.creation_payload.clone(),
                        runtime_env: None,
                        dependencies: Vec::new(),
                    };
                    recreation_tasks.push(recreation_task);
                } else {
                    actor_record.status = ActorStatus::Dead("Worker crashed".to_string());
                    dead_actors.push(actor_record.actor_id);
                }
            }
        }

        for recreation_task in &recreation_tasks {
            state.active_tasks.insert(
                recreation_task.id,
                TaskRecord {
                    task: recreation_task.clone(),
                    status: TaskStatus::Pending,
                    assigned_to: None,
                    retry_count: 0,
                    output_object_id: None,
                },
            );
        }

        let mut actor_tasks_to_fail = Vec::new();
        for actor_id in dead_actors {
            let stuck_tasks = dispatcher.drain_actor_mailbox(&actor_id);
            for task in &stuck_tasks {
                if let Some(record) = state.active_tasks.get_mut(&task.id) {
                    record.status = TaskStatus::Failed("Actor Dead (Worker crashed)".to_string());
                }
            }
            actor_tasks_to_fail.extend(stuck_tasks);
        }

        (recreation_tasks, actor_tasks_to_fail)
    }

    /// Notifies driver connections for actor tasks that cannot be recovered.
    async fn notify_failed_actor_tasks(&self, failed_tasks: &[Task]) {
        for task in failed_tasks {
            if let Some(driver_tx) = self.driver_connections.get(&task.job_id) {
                let _ = driver_tx
                    .value()
                    .send(Message::TaskFailure {
                        task_id: task.id,
                        job_id: task.job_id,
                        error: "Actor Dead (Worker crashed)".to_string(),
                    })
                    .await;
            }
        }
    }

    /// Unregisters a dead worker and triggers recovery for tasks, objects, and actors.
    pub async fn remove_dead_worker(&mut self, worker_id: WorkerId) {
        self.last_heartbeat.remove(&worker_id);
        self.snooze_counts.remove(&worker_id);

        let (tasks_to_requeue, actor_tasks_to_fail) = {
            let mut state = self.gcs.write();
            state.nodes.remove(&worker_id);
            state.worker_load.remove(&worker_id);

            let dead_jobs: HashSet<JobId> = state
                .jobs
                .iter()
                .filter(|(_, j)| j.status != JobStatus::Running)
                .map(|(id, _)| *id)
                .collect();

            let mut requeue = Self::recover_assigned_tasks(&mut state, worker_id, &dead_jobs);
            requeue.extend(Self::recover_lost_lineage(&mut state, worker_id, &dead_jobs));

            let (recreation_tasks, failed_actor_tasks) =
                Self::recover_actors(&mut state, worker_id, &self.dispatcher);
            requeue.extend(recreation_tasks);

            (requeue, failed_actor_tasks)
        };

        self.notify_failed_actor_tasks(&actor_tasks_to_fail).await;

        for task in tasks_to_requeue {
            self.dispatcher.requeue_priority(task);
        }
    }

    /// Scans worker heartbeats and initiates recovery for any unresponsive workers.
    pub async fn scan_and_recover(&mut self) {
        let dead_workers = self.detect_dead_workers();
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
            &state,
        );

        assert_eq!(dispatched.unwrap().id, task.id);
    }
}
