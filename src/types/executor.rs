pub use super::error::ExecutorError;
use super::task::Task;
use async_trait::async_trait;
use std::sync::Arc;

#[async_trait]
pub trait Executor: Send + Sync + 'static {
    async fn execute(
        &self,
        task: &Task,
        store: Arc<crate::object_store::store::ObjectStore>,
    ) -> Result<(Vec<u8>, Option<uuid::Uuid>), ExecutorError>;

    async fn cancel(&self, _task_id: uuid::Uuid) {}
}

