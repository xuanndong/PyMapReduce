use super::SMALL_OBJECT_THRESHOLD_BYTES;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageTier {
    Hot,  // RAM
    Warm, // Disk
}

pub struct TierManager;

impl TierManager {
    pub fn decide_tier(size_bytes: u64) -> StorageTier {
        if size_bytes < SMALL_OBJECT_THRESHOLD_BYTES {
            StorageTier::Hot
        } else {
            StorageTier::Warm
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tier_decision() {
        assert_eq!(TierManager::decide_tier(1024), StorageTier::Hot);
        assert_eq!(
            TierManager::decide_tier(600 * 1024 * 1024),
            StorageTier::Warm
        );
        assert_eq!(
            TierManager::decide_tier(10 * 1024 * 1024 * 1024),
            StorageTier::Warm
        );
    }
}
