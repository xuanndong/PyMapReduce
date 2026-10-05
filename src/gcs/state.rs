use crate::protocol::message::ObjectId;
use crate::types::job::{Job, JobId, JobStatus};
use crate::types::node::{NodeInfo, WorkerId};
use crate::types::task::{Task, TaskId, TaskStatus};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobRecord {
    pub job: Job,
    pub status: JobStatus,
    pub tasks: Vec<TaskId>,
    pub results: HashMap<TaskId, Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskRecord {
    pub task: Task,
    pub status: TaskStatus,
    pub assigned_to: Option<WorkerId>,
    #[serde(default)]
    pub retry_count: u32,
    #[serde(default)]
    pub output_object_id: Option<ObjectId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ActorStatus {
    Creating,
    Alive,
    Dead(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActorRecord {
    pub actor_id: crate::types::task::ActorId,
    pub class_name: String,
    pub worker_id: Option<WorkerId>,
    pub status: ActorStatus,
    #[serde(default)]
    pub creation_payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObjectLocation {
    pub object_id: ObjectId,
    pub size_bytes: u64,
    pub nodes: HashSet<WorkerId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcsState {
    pub jobs: HashMap<JobId, JobRecord>,
    pub nodes: HashMap<WorkerId, NodeInfo>,
    pub active_tasks: HashMap<TaskId, TaskRecord>,
    pub worker_load: HashMap<WorkerId, usize>,
    pub bandwidth_ema: HashMap<WorkerId, f64>,
    pub object_catalog: HashMap<ObjectId, ObjectLocation>,
    pub object_subscribers: HashMap<ObjectId, HashSet<WorkerId>>,
    pub actors: HashMap<crate::types::task::ActorId, ActorRecord>,
    #[serde(default)]
    pub finished_jobs_queue: VecDeque<(JobId, u64)>,
}

impl Default for GcsState {
    fn default() -> Self {
        Self::new()
    }
}

impl GcsState {
    pub const DEFAULT_MAX_FINISHED_JOBS: usize = 2000;
    pub const DEFAULT_JOB_TTL_SECS: u64 = 3600;

    pub fn new() -> Self {
        Self {
            jobs: HashMap::new(),
            nodes: HashMap::new(),
            active_tasks: HashMap::new(),
            worker_load: HashMap::new(),
            bandwidth_ema: HashMap::new(),
            object_catalog: HashMap::new(),
            object_subscribers: HashMap::new(),
            actors: HashMap::new(),
            finished_jobs_queue: VecDeque::new(),
        }
    }

    fn now_secs() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    fn remove_job_if_not_running(&mut self, job_id: &JobId) {
        if self.jobs.get(job_id).is_some_and(|r| r.status != JobStatus::Running) {
            self.jobs.remove(job_id);
        }
    }

    pub fn mark_job_finished(&mut self, job_id: JobId) {
        self.finished_jobs_queue.push_back((job_id, Self::now_secs()));
        self.enforce_job_retention(Self::DEFAULT_MAX_FINISHED_JOBS, Self::DEFAULT_JOB_TTL_SECS);
    }

    pub fn enforce_job_retention(&mut self, max_finished_jobs: usize, ttl_secs: u64) {
        let now = Self::now_secs();

        while let Some(&(_, finish_time)) = self.finished_jobs_queue.front() {
            if now.saturating_sub(finish_time) >= ttl_secs {
                if let Some((job_id, _)) = self.finished_jobs_queue.pop_front() {
                    self.remove_job_if_not_running(&job_id);
                }
            } else {
                break;
            }
        }

        while self.finished_jobs_queue.len() > max_finished_jobs {
            if let Some((job_id, _)) = self.finished_jobs_queue.pop_front() {
                self.remove_job_if_not_running(&job_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn test_job_retention_capacity_cap() {
        let mut state = GcsState::new();

        let mut job_ids = Vec::new();
        for _ in 0..5 {
            let jid = Uuid::new_v4();
            job_ids.push(jid);
            state.jobs.insert(
                jid,
                JobRecord {
                    job: Job { id: jid, name: "test".to_string() },
                    status: JobStatus::Completed,
                    tasks: vec![],
                    results: HashMap::new(),
                },
            );
            state.mark_job_finished(jid);
        }

        assert_eq!(state.jobs.len(), 5);
        assert_eq!(state.finished_jobs_queue.len(), 5);

        state.enforce_job_retention(3, 3600);

        assert_eq!(state.jobs.len(), 3);
        assert_eq!(state.finished_jobs_queue.len(), 3);
        assert!(!state.jobs.contains_key(&job_ids[0]));
        assert!(!state.jobs.contains_key(&job_ids[1]));
        assert!(state.jobs.contains_key(&job_ids[2]));
        assert!(state.jobs.contains_key(&job_ids[3]));
        assert!(state.jobs.contains_key(&job_ids[4]));
    }

    #[test]
    fn test_job_retention_ttl() {
        let mut state = GcsState::new();
        let jid1 = Uuid::new_v4();
        let jid2 = Uuid::new_v4();

        state.jobs.insert(
            jid1,
            JobRecord {
                job: Job { id: jid1, name: "test1".to_string() },
                status: JobStatus::Completed,
                tasks: vec![],
                results: HashMap::new(),
            },
        );
        state.jobs.insert(
            jid2,
            JobRecord {
                job: Job { id: jid2, name: "test2".to_string() },
                status: JobStatus::Completed,
                tasks: vec![],
                results: HashMap::new(),
            },
        );

        let now = GcsState::now_secs();

        // jid1 finished 5000 seconds ago, jid2 finished just now
        state.finished_jobs_queue.push_back((jid1, now - 5000));
        state.finished_jobs_queue.push_back((jid2, now));

        state.enforce_job_retention(2000, 3600);

        assert_eq!(state.jobs.len(), 1);
        assert!(!state.jobs.contains_key(&jid1));
        assert!(state.jobs.contains_key(&jid2));
    }
}
