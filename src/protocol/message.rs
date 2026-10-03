use crate::types::job::JobId;
use crate::types::node::{NodeCapacity, WorkerId};
use crate::types::task::{Task, TaskId};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type ObjectId = Uuid;
pub type EncodedTask = Vec<u8>;
pub type TaskOutput = Vec<u8>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TaskReconcileStatus {
    Running,
    Completed {
        job_id: JobId,
        output: TaskOutput,
        object_id: Option<ObjectId>,
    },
    Failed(String),
    NotFound,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Message {
    // ── Driver ↔ HeadNode ─────────────────────────────────────
    SubmitJob {
        job_id: JobId,
        tasks: Vec<EncodedTask>,
        max_time_secs: Option<u64>,
    },
    AttachJob {
        job_id: JobId,
    },
    PutObject {
        object_id: ObjectId,
        data: Vec<u8>,
    },
    GetObject {
        object_id: ObjectId,
        offset: u64,
        len: u64,
    },
    GetJobStatus {
        job_id: JobId,
    },
    CancelJob {
        job_id: JobId,
    },
    Shutdown,
    DeleteObject {
        object_id: ObjectId,
    },

    JobAccepted {
        job_id: JobId,
    },
    JobProgress {
        job_id: JobId,
        completed: u32,
        total: u32,
    },
    JobComplete {
        job_id: JobId,
        results: Vec<TaskOutput>,
    },
    JobFailed {
        job_id: JobId,
        reason: String,
    },
    ObjectData {
        object_id: ObjectId,
        chunk: Vec<u8>,
        offset: u64,
    },

    // ── HeadNode ↔ WorkerNode ──────────────────────────────────
    AssignTask {
        task: Box<Task>,
    },
    CancelTask {
        task_id: TaskId,
    },
    QueryTaskStatus {
        task_id: TaskId,
        job_id: JobId,
    },
    TaskStatusResponse {
        task_id: TaskId,
        job_id: JobId,
        status: TaskReconcileStatus,
    },
    WorkerDraining {
        worker_id: WorkerId,
    },
    GetObjectWorker {
        object_id: ObjectId,
    },
    WakeUp,

    ReportReady {
        worker_id: WorkerId,
        cache_hint: Option<String>,
        capacity: NodeCapacity,
        listener_addr: Option<String>,
    },
    TaskSuccess {
        task_id: TaskId,
        job_id: JobId,
        output: Vec<u8>,
        object_id: Option<ObjectId>,
        observed_bandwidth_mbps: Option<f64>,
    },
    ObjectDataWorker {
        object_id: ObjectId,
        data: Option<Vec<u8>>,
    },
    TaskFailure {
        task_id: TaskId,
        job_id: JobId,
        error: String,
    },
    Heartbeat {
        worker_id: WorkerId,
    },

    // ── Shared ────────────────────────────────────────────────
    Pong,
    Error {
        code: u32,
        message: String,
    },
}
