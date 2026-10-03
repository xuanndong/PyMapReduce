use crate::gcs::state::GcsState;
use crate::types::node::WorkerId;
use parking_lot::RwLock;
use std::sync::Arc;

pub const EMA_ALPHA: f64 = 0.3;

pub struct Gcs {
    state: Arc<RwLock<GcsState>>,
}

impl Default for Gcs {
    fn default() -> Self {
        Self::new()
    }
}

impl Gcs {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(GcsState::new())),
        }
    }

    pub fn read(&self) -> parking_lot::RwLockReadGuard<'_, GcsState> {
        self.state.read()
    }

    pub fn write(&self) -> parking_lot::RwLockWriteGuard<'_, GcsState> {
        self.state.write()
    }

    /// Update Exponential Moving Average (EMA) of Bandwidth (Algorithm 2)
    pub fn update_bandwidth_ema(&self, worker_id: WorkerId, observed_mbps: f64) {
        let mut state = self.state.write();
        let current_ema = state
            .bandwidth_ema
            .get(&worker_id)
            .copied()
            .unwrap_or(observed_mbps);

        let new_ema = (EMA_ALPHA * observed_mbps) + ((1.0 - EMA_ALPHA) * current_ema);
        state.bandwidth_ema.insert(worker_id, new_ema);
    }
}

impl Clone for Gcs {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn test_ema_update() {
        let gcs = Gcs::new();
        let worker_id = Uuid::new_v4();

        // First update initializes EMA to observed value
        gcs.update_bandwidth_ema(worker_id, 100.0);
        assert_eq!(*gcs.read().bandwidth_ema.get(&worker_id).unwrap(), 100.0);

        // Second update applies EMA
        // new_ema = 0.3 * 50.0 + 0.7 * 100.0 = 15.0 + 70.0 = 85.0
        gcs.update_bandwidth_ema(worker_id, 50.0);
        assert_eq!(*gcs.read().bandwidth_ema.get(&worker_id).unwrap(), 85.0);
    }
}
