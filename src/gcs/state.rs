use crate::protocol::message::ObjectId;
use crate::types::job::{Job, JobId, JobStatus};
use crate::types::node::{NodeInfo, WorkerId};
use crate::types::task::{Task, TaskId, TaskStatus};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

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
}

impl Default for GcsState {
    fn default() -> Self {
        Self::new()
    }
}

impl GcsState {
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
        }
    }
}
