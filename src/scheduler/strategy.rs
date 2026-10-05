use crate::gcs::state::GcsState;
use crate::scheduler::queue::TaskQueue;
use crate::types::node::NodeInfo;
use crate::types::task::Task;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum SchedulerType {
    RoundRobin,
    WeightedCapacity,
    LeastLoad,
    LocalityFirst,
    Adaptive,
}

impl SchedulerType {
    pub fn into_strategy(self) -> Box<dyn SchedulingStrategy> {
        match self {
            Self::RoundRobin => Box::new(RoundRobinStrategy::new()),
            Self::WeightedCapacity => Box::new(WeightedCapacityStrategy),
            Self::LeastLoad => Box::new(LeastLoadStrategy),
            Self::LocalityFirst => Box::new(LocalityFirstStrategy),
            Self::Adaptive => Box::new(AdaptiveStrategy),
        }
    }
}

pub type WorkerInfo = NodeInfo;

pub trait SchedulingStrategy: Send + Sync + 'static {
    fn poll_next_task(
        &self,
        queue: &mut TaskQueue,
        worker: &WorkerInfo,
        gcs_snapshot: &GcsState,
    ) -> Option<Task>;

    fn name(&self) -> &'static str;
}

// Helper to poll pending tasks for alive actors hosted on this worker
fn poll_actor_task(queue: &mut TaskQueue, worker: &WorkerInfo, gcs: &GcsState) -> Option<Task> {
    for (actor_id, actor_rec) in &gcs.actors {
        if actor_rec.status == crate::gcs::state::ActorStatus::Alive
            && actor_rec.worker_id == Some(worker.id)
        {
            if let Some(task) = queue.pop_for_actor(actor_id) {
                return Some(task);
            }
        }
    }
    None
}

// 1. RoundRobinStrategy
pub struct RoundRobinStrategy {
    counter: AtomicUsize,
}

impl Default for RoundRobinStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl RoundRobinStrategy {
    pub fn new() -> Self {
        Self {
            counter: AtomicUsize::new(0),
        }
    }
}

impl SchedulingStrategy for RoundRobinStrategy {
    fn poll_next_task(
        &self,
        queue: &mut TaskQueue,
        worker: &WorkerInfo,
        gcs: &GcsState,
    ) -> Option<Task> {
        if let Some(task) = poll_actor_task(queue, worker, gcs) {
            return Some(task);
        }

        self.counter.fetch_add(1, Ordering::Relaxed);
        queue.pop()
    }
    fn name(&self) -> &'static str {
        "RoundRobin"
    }
}

// 2. WeightedCapacityStrategy
pub struct WeightedCapacityStrategy;
impl SchedulingStrategy for WeightedCapacityStrategy {
    fn poll_next_task(
        &self,
        queue: &mut TaskQueue,
        worker: &WorkerInfo,
        gcs: &GcsState,
    ) -> Option<Task> {
        if let Some(task) = poll_actor_task(queue, worker, gcs) {
            return Some(task);
        }

        let mut my_score = 0.0;
        let mut best_score = f64::MIN;
        
        for (w_id, info) in &gcs.nodes {
            let load = gcs.worker_load.get(w_id).copied().unwrap_or(0);
            let capacity = info.capacity.physical_cpus as f64 + (info.capacity.total_ram_mb as f64 / 1024.0);
            
            // Score = capacity / (load + 1). Higher is better.
            let score = capacity / (load as f64 + 1.0);
            
            if score > best_score {
                best_score = score;
            }
            if *w_id == worker.id {
                my_score = score;
            }
        }
        
        if my_score < best_score - 0.0001 {
            return None;
        }
        
        queue.pop()
    }
    fn name(&self) -> &'static str {
        "WeightedCapacity"
    }
}

// 3. LeastLoadStrategy
pub struct LeastLoadStrategy;
impl SchedulingStrategy for LeastLoadStrategy {
    fn poll_next_task(
        &self,
        queue: &mut TaskQueue,
        worker: &WorkerInfo,
        gcs: &GcsState,
    ) -> Option<Task> {
        if let Some(task) = poll_actor_task(queue, worker, gcs) {
            return Some(task);
        }

        let mut my_load = 0;
        let mut min_load = usize::MAX;
        
        for w_id in gcs.nodes.keys() {
            let load = gcs.worker_load.get(w_id).copied().unwrap_or(0);
            if load < min_load {
                min_load = load;
            }
            if *w_id == worker.id {
                my_load = load;
            }
        }
        
        if my_load > min_load {
            return None;
        }
        
        queue.pop()
    }
    fn name(&self) -> &'static str {
        "LeastLoad"
    }
}

// 4. LocalityFirstStrategy
pub struct LocalityFirstStrategy;
impl SchedulingStrategy for LocalityFirstStrategy {
    fn poll_next_task(
        &self,
        queue: &mut TaskQueue,
        worker: &WorkerInfo,
        gcs: &GcsState,
    ) -> Option<Task> {
        if let Some(task) = poll_actor_task(queue, worker, gcs) {
            return Some(task);
        }

        queue
            .pop_by_locality(&worker.id.to_string())
            .or_else(|| queue.pop())
    }
    fn name(&self) -> &'static str {
        "LocalityFirst"
    }
}

// 5. AdaptiveStrategy
pub struct AdaptiveStrategy;
impl SchedulingStrategy for AdaptiveStrategy {
    fn poll_next_task(
        &self,
        queue: &mut TaskQueue,
        worker: &WorkerInfo,
        gcs: &GcsState,
    ) -> Option<Task> {
        if let Some(task) = poll_actor_task(queue, worker, gcs) {
            return Some(task);
        }

        queue
            .pop_by_locality(&worker.id.to_string())
            .or_else(|| queue.pop())
            .or_else(|| queue.steal_any_local())
    }
    fn name(&self) -> &'static str {
        "Adaptive"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::node::{NodeCapacity, NodeInfo, WorkerId};
    use crate::types::task::{Task, TaskKind};
    use uuid::Uuid;

    fn create_dummy_task() -> Task {
        Task {
            id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            kind: TaskKind::Map,
            payload: vec![],
            runtime_env: None,
            dependencies: vec![],
        }
    }

    fn create_dummy_worker(id: WorkerId, cpus: usize, ram_mb: u64) -> NodeInfo {
        NodeInfo {
            id,
            capacity: NodeCapacity {
                physical_cpus: cpus,
                total_ram_mb: ram_mb,
            },
            addr: "127.0.0.1:8000".to_string(),
        }
    }

    #[test]
    fn test_least_load_strategy() {
        let strategy = LeastLoadStrategy;
        let mut queue = TaskQueue::new();
        queue.push(create_dummy_task());

        let mut gcs = GcsState::new();
        let w1_id = Uuid::new_v4();
        let w2_id = Uuid::new_v4();

        let w1 = create_dummy_worker(w1_id, 4, 8192);
        let w2 = create_dummy_worker(w2_id, 4, 8192);

        gcs.nodes.insert(w1_id, w1.clone());
        gcs.nodes.insert(w2_id, w2.clone());

        // w1 has 2 running tasks, w2 has 0
        gcs.worker_load.insert(w1_id, 2);
        gcs.worker_load.insert(w2_id, 0);

        // w1 asks for task -> should be rejected because w2 has lower load
        let task_w1 = strategy.poll_next_task(&mut queue, &w1, &gcs);
        assert!(task_w1.is_none());

        // w2 asks for task -> should succeed
        let task_w2 = strategy.poll_next_task(&mut queue, &w2, &gcs);
        assert!(task_w2.is_some());
    }

    #[test]
    fn test_weighted_capacity_strategy() {
        let strategy = WeightedCapacityStrategy;
        let mut queue = TaskQueue::new();
        queue.push(create_dummy_task());

        let mut gcs = GcsState::new();
        let w_small_id = Uuid::new_v4();
        let w_large_id = Uuid::new_v4();

        let w_small = create_dummy_worker(w_small_id, 2, 2048);
        let w_large = create_dummy_worker(w_large_id, 16, 32768);

        gcs.nodes.insert(w_small_id, w_small.clone());
        gcs.nodes.insert(w_large_id, w_large.clone());

        // Both have 0 tasks. w_large has much higher capacity score.
        gcs.worker_load.insert(w_small_id, 0);
        gcs.worker_load.insert(w_large_id, 0);

        // Small worker asks for task -> rejected in favor of large worker
        let task_small = strategy.poll_next_task(&mut queue, &w_small, &gcs);
        assert!(task_small.is_none());

        // Large worker asks for task -> granted
        let task_large = strategy.poll_next_task(&mut queue, &w_large, &gcs);
        assert!(task_large.is_some());
    }
}
