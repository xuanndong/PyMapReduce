use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type WorkerId = Uuid;
pub type NodeId = Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeCapacity {
    pub physical_cpus: usize,
    pub total_ram_mb: u64,
}

impl Default for NodeCapacity {
    fn default() -> Self {
        let mut sys = sysinfo::System::new_all();
        sys.refresh_memory();
        Self {
            physical_cpus: num_cpus::get_physical(),
            total_ram_mb: sys.total_memory() / (1024 * 1024),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeInfo {
    pub id: NodeId,
    pub capacity: NodeCapacity,
    pub addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodeStatus {
    Active,
    Draining,
    Dead,
    Offline,
}
