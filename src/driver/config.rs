use crate::types::task::TaskKind;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct TaskInput {
    pub kind: TaskKind,
    pub payload: Vec<u8>,
    pub dependencies: Vec<crate::types::task::ObjectId>,
}

impl TaskInput {
    pub fn new(kind: TaskKind, payload: Vec<u8>) -> Self {
        Self { kind, payload, dependencies: Vec::new() }
    }
    
    pub fn with_dependencies(mut self, deps: Vec<crate::types::task::ObjectId>) -> Self {
        self.dependencies = deps;
        self
    }
}

#[derive(Debug, Clone)]
pub struct JobConfig {
    pub name: String,
    pub tasks: Vec<TaskInput>,
    pub max_time: Option<Duration>,
    pub runtime_env: Option<crate::types::task::ObjectId>,
}

impl JobConfig {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            tasks: Vec::new(),
            max_time: None,
            runtime_env: None,
        }
    }

    pub fn add_task(mut self, task: TaskInput) -> Self {
        self.tasks.push(task);
        self
    }

    pub fn with_max_time(mut self, duration: Duration) -> Self {
        self.max_time = Some(duration);
        self
    }

    pub fn with_runtime_env(mut self, runtime_env: crate::types::task::ObjectId) -> Self {
        self.runtime_env = Some(runtime_env);
        self
    }
}
