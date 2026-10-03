use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type TaskId = Uuid;
pub type ActorId = Uuid;
pub type ObjectId = Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TaskKind {
    Map,
    Reduce,
    ActorCreation {
        actor_id: ActorId,
        class_name: String,
    },
    ActorTask {
        actor_id: ActorId,
        method_name: String,
    },
    ActorDestruction {
        actor_id: ActorId,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Task {
    pub id: TaskId,
    pub job_id: super::job::JobId,
    pub kind: TaskKind,
    pub payload: Vec<u8>,
    pub runtime_env: Option<ObjectId>,
    pub dependencies: Vec<ObjectId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed(String),
}
