use crate::gcs::state::GcsState;
use crate::scheduler::queue::TaskQueue;
use crate::scheduler::strategy::{SchedulingStrategy, WorkerInfo};
use crate::types::task::Task;
use parking_lot::Mutex;
use std::sync::Arc;

pub struct Dispatcher {
    queue: Arc<Mutex<TaskQueue>>,
    strategy: Box<dyn SchedulingStrategy>,
}

impl Dispatcher {
    pub fn new(strategy: Box<dyn SchedulingStrategy>) -> Self {
        Self {
            queue: Arc::new(Mutex::new(TaskQueue::new())),
            strategy,
        }
    }

    pub fn enqueue(&self, task: Task) {
        self.queue.lock().push(task);
    }

    pub fn requeue_priority(&self, task: Task) {
        self.queue.lock().requeue_with_priority(task);
    }

    pub fn drain_actor_mailbox(&self, actor_id: &crate::types::task::ActorId) -> Vec<Task> {
        self.queue.lock().drain_actor_mailbox(actor_id)
    }

    pub fn dispatch(&self, worker: &WorkerInfo, gcs: &GcsState) -> Option<Task> {
        let mut q = self.queue.lock();
        self.strategy.poll_next_task(&mut q, worker, gcs)
    }
}
