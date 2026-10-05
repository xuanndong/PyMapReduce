use crate::types::task::Task;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, VecDeque};

pub type CacheKey = String;

#[derive(Debug, Clone)]
pub struct PriorityTask {
    pub priority: u8,
    pub seq: u64,
    pub task: Task,
}

impl PartialEq for PriorityTask {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.seq == other.seq
    }
}

impl Eq for PriorityTask {}

// Min-heap behavior: smaller priority value = higher priority.
// For same priority, smaller seq = older task = higher priority.
impl Ord for PriorityTask {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse because Rust's BinaryHeap is a max-heap
        other
            .priority
            .cmp(&self.priority)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

impl PartialOrd for PriorityTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct TaskQueue {
    pending: BinaryHeap<PriorityTask>,
    locality: HashMap<CacheKey, VecDeque<Task>>,
    pub actor_mailboxes: HashMap<crate::types::task::ActorId, VecDeque<Task>>,
    seq: u64,
}

impl Default for TaskQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskQueue {
    pub fn new() -> Self {
        Self {
            pending: BinaryHeap::new(),
            locality: HashMap::new(),
            actor_mailboxes: HashMap::new(),
            seq: 0,
        }
    }

    /// Default enqueue (priority 128)
    pub fn push(&mut self, task: Task) {
        self.push_with_priority(task, 128);
    }

    /// Fault recovery (priority 0)
    pub fn requeue_with_priority(&mut self, task: Task) {
        self.push_with_priority(task, 0);
    }
    fn push_with_priority(&mut self, task: Task, priority: u8) {
        match &task.kind {
            crate::types::task::TaskKind::ActorTask { actor_id, .. }
            | crate::types::task::TaskKind::ActorDestruction { actor_id } => {
                let mailbox = self.actor_mailboxes.entry(*actor_id).or_default();
                mailbox.push_back(task);
                return;
            }
            _ => {}
        }

        let seq = self.seq;
        self.seq += 1;
        self.pending.push(PriorityTask {
            priority,
            seq,
            task,
        });
    }

    pub fn pop(&mut self) -> Option<Task> {
        self.pending.pop().map(|pt| pt.task)
    }

    pub fn pop_for_actor(&mut self, actor_id: &crate::types::task::ActorId) -> Option<Task> {
        if let Some(queue) = self.actor_mailboxes.get_mut(actor_id) {
            let task = queue.pop_front();
            if queue.is_empty() {
                self.actor_mailboxes.remove(actor_id);
            }
            task
        } else {
            None
        }
    }

    pub fn drain_actor_mailbox(&mut self, actor_id: &crate::types::task::ActorId) -> Vec<Task> {
        if let Some(queue) = self.actor_mailboxes.remove(actor_id) {
            queue.into_iter().collect()
        } else {
            Vec::new()
        }
    }

    pub fn pop_by_locality(&mut self, cache_key: &str) -> Option<Task> {
        if let Some(queue) = self.locality.get_mut(cache_key) {
            let task = queue.pop_front();
            if queue.is_empty() {
                self.locality.remove(cache_key);
            }
            task
        } else {
            None
        }
    }

    pub fn steal_any_local(&mut self) -> Option<Task> {
        let key = self.locality.keys().next().cloned();
        if let Some(k) = key {
            self.pop_by_locality(&k)
        } else {
            None
        }
    }

    pub fn cancel_job_tasks(&mut self, job_id: uuid::Uuid) -> Vec<uuid::Uuid> {
        let mut cancelled_ids = Vec::new();

        let old_pending = std::mem::take(&mut self.pending);
        let mut kept = Vec::new();
        for pt in old_pending.into_vec() {
            if pt.task.job_id == job_id {
                cancelled_ids.push(pt.task.id);
            } else {
                kept.push(pt);
            }
        }
        self.pending = std::collections::BinaryHeap::from(kept);

        for queue in self.locality.values_mut() {
            queue.retain(|t| {
                if t.job_id == job_id {
                    cancelled_ids.push(t.id);
                    false
                } else {
                    true
                }
            });
        }
        self.locality.retain(|_, q| !q.is_empty());

        for queue in self.actor_mailboxes.values_mut() {
            queue.retain(|t| {
                if t.job_id == job_id {
                    cancelled_ids.push(t.id);
                    false
                } else {
                    true
                }
            });
        }
        self.actor_mailboxes.retain(|_, q| !q.is_empty());

        cancelled_ids
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty() 
            && self.locality.is_empty()
            && self.actor_mailboxes.values().all(|q| q.is_empty())
    }

    pub fn len(&self) -> usize {
        let local_len: usize = self.locality.values().map(|q| q.len()).sum();
        let actor_len: usize = self.actor_mailboxes.values().map(|q| q.len()).sum();
        self.pending.len() + local_len + actor_len
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::task::TaskKind;
    use uuid::Uuid;

    #[test]
    fn test_priority_queue() {
        let mut q = TaskQueue::new();
        let t1 = Task {
            id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            kind: TaskKind::Map,
            payload: Vec::new(),
            runtime_env: None,
            dependencies: vec![],
        };
        let t2 = Task {
            id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            kind: TaskKind::Map,
            payload: Vec::new(),
            runtime_env: None,
            dependencies: vec![],
        };

        q.push(t1.clone());
        q.requeue_with_priority(t2.clone()); // t2 has higher priority (0)

        let popped1 = q.pop().unwrap();
        assert_eq!(popped1.id, t2.id); // t2 should come out first

        let popped2 = q.pop().unwrap();
        assert_eq!(popped2.id, t1.id);
    }

    #[test]
    fn test_cancel_job_tasks() {
        let mut q = TaskQueue::new();
        let job1 = Uuid::new_v4();
        let job2 = Uuid::new_v4();

        let t1 = Task {
            id: Uuid::new_v4(),
            job_id: job1,
            kind: TaskKind::Map,
            payload: Vec::new(),
            runtime_env: None,
            dependencies: vec![],
        };
        let t2 = Task {
            id: Uuid::new_v4(),
            job_id: job2,
            kind: TaskKind::Map,
            payload: Vec::new(),
            runtime_env: None,
            dependencies: vec![],
        };
        let t3 = Task {
            id: Uuid::new_v4(),
            job_id: job1,
            kind: TaskKind::Map,
            payload: Vec::new(),
            runtime_env: None,
            dependencies: vec![],
        };

        q.push(t1.clone());
        q.push(t2.clone());
        q.push(t3.clone());

        assert_eq!(q.len(), 3);

        let cancelled = q.cancel_job_tasks(job1);
        assert_eq!(cancelled.len(), 2);
        assert!(cancelled.contains(&t1.id));
        assert!(cancelled.contains(&t3.id));

        assert_eq!(q.len(), 1);
        let remaining = q.pop().unwrap();
        assert_eq!(remaining.id, t2.id);
    }
}
