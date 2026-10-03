use thiserror::Error;

#[derive(Debug, Error)]
pub enum FrameworkError {
    #[error("I/O Error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization Error: {0}")]
    Serialization(String),

    #[error("Network Error: {0}")]
    Network(String),

    #[error("Scheduler Error: {0}")]
    Scheduler(String),

    #[error("Other Error: {0}")]
    Other(String),
}

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("Execution Failed: {0}")]
    Failed(String),
}

pub type Result<T> = std::result::Result<T, FrameworkError>;
