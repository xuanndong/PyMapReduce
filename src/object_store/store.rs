use super::object_ref::ObjectRef;
use super::tier::{StorageTier, TierManager};
use crate::protocol::message::ObjectId;
use crate::types::error::{FrameworkError, Result};
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn detect_system_hot_tier_limit() -> usize {
    let mut sys = sysinfo::System::new_all();
    sys.refresh_memory();
    let total = sys.total_memory();
    if total > 0 {
        ((total / 2) as usize).max(64 * 1024 * 1024)
    } else {
        512 * 1024 * 1024
    }
}

struct HotEntry {
    data: Bytes,
    last_accessed: AtomicU64,
}

impl HotEntry {
    fn new(data: Bytes) -> Self {
        Self {
            data,
            last_accessed: AtomicU64::new(Self::now()),
        }
    }

    fn touch(&self) {
        self.last_accessed.store(Self::now(), Ordering::Relaxed);
    }

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

pub struct ObjectStore {
    hot_tier: DashMap<ObjectId, HotEntry>,
    hot_tier_size: AtomicUsize,
    max_hot_tier_bytes: usize,
    shm_dir: std::path::PathBuf,
    warm_dir: std::path::PathBuf,
    waiters: DashMap<ObjectId, Arc<tokio::sync::Notify>>,
}

impl ObjectStore {
    pub fn new(warm_dir: std::path::PathBuf) -> Arc<Self> {
        Self::new_with_capacity(warm_dir, detect_system_hot_tier_limit())
    }

    pub fn new_with_capacity(warm_dir: std::path::PathBuf, max_hot_bytes: usize) -> Arc<Self> {
        let shm_base = std::path::Path::new("/dev/shm");
        let shm_dir = if shm_base.is_dir() {
            shm_base.join("pymapreduce")
        } else {
            std::env::temp_dir().join("pymapreduce").join("shm")
        };

        if shm_dir.exists() {
            let _ = std::fs::remove_dir_all(&shm_dir);
        }
        let _ = std::fs::create_dir_all(&shm_dir);

        if warm_dir.exists() {
            let _ = std::fs::remove_dir_all(&warm_dir);
        }
        let _ = std::fs::create_dir_all(&warm_dir);

        Arc::new(Self {
            hot_tier: DashMap::new(),
            hot_tier_size: AtomicUsize::new(0),
            max_hot_tier_bytes: max_hot_bytes,
            shm_dir,
            warm_dir,
            waiters: DashMap::new(),
        })
    }

    pub fn new_default() -> Arc<Self> {
        Self::new(std::env::temp_dir().join("pymapreduce").join("warm"))
    }

    pub async fn put(self: &Arc<Self>, id: ObjectId, data: Bytes) -> Result<ObjectRef> {
        let size = data.len() as u64;
        let tier = TierManager::decide_tier(size);

        if tier == StorageTier::Warm {
            tokio::fs::create_dir_all(&self.warm_dir)
                .await
                .map_err(FrameworkError::Io)?;
            let path = self.warm_dir.join(id.to_string());
            tokio::fs::write(path, data)
                .await
                .map_err(FrameworkError::Io)?;

            self.notify_waiters(id);
            return Ok(ObjectRef::new(id, tier, self.clone()));
        }

        // Hot Tier: Evict oldest objects if exceeding memory limit
        self.evict_if_needed(size as usize).await?;

        // Write directly to shared memory (/dev/shm) for zero-copy IPC
        if let Err(e) = self.write_shm(id, &data).await {
            tracing::warn!("Failed to write object {} to SHM: {}", id, e);
        }

        self.hot_tier.insert(id, HotEntry::new(data));
        self.hot_tier_size
            .fetch_add(size as usize, Ordering::Relaxed);

        self.notify_waiters(id);
        Ok(ObjectRef::new(id, tier, self.clone()))
    }

    pub async fn get(&self, id: ObjectId, tier: StorageTier) -> Result<Bytes> {
        if tier == StorageTier::Hot {
            if let Some(entry) = self.hot_tier.get(&id) {
                entry.touch();
                return Ok(entry.data.clone());
            }
        }

        // Fallback to warm tier on disk
        let path = self.warm_dir.join(id.to_string());
        if path.exists() {
            let data = tokio::fs::read(path).await.map_err(FrameworkError::Io)?;
            return Ok(Bytes::from(data));
        }

        Err(FrameworkError::Other(
            "Object not found in Hot or Warm tier".into(),
        ))
    }

    pub async fn get_any(&self, id: ObjectId) -> Result<Bytes> {
        if let Some(entry) = self.hot_tier.get(&id) {
            entry.touch();
            return Ok(entry.data.clone());
        }

        let path = self.warm_dir.join(id.to_string());
        if path.exists() {
            let data = tokio::fs::read(path).await.map_err(FrameworkError::Io)?;
            return Ok(Bytes::from(data));
        }

        Err(FrameworkError::Other("Object not found".into()))
    }

    /// Retrieve or create the shared memory file path for zero-copy IPC
    pub async fn ensure_shm_path(&self, id: ObjectId) -> Result<std::path::PathBuf> {
        let shm_path = self.shm_dir.join(id.to_string());
        if shm_path.exists() {
            return Ok(shm_path);
        }

        // If not in SHM, fetch data and write to SHM
        let data = self.get_any(id).await?;
        self.write_shm(id, &data).await?;
        Ok(shm_path)
    }

    pub async fn delete(&self, id: ObjectId) -> Result<()> {
        if let Some((_, entry)) = self.hot_tier.remove(&id) {
            self.hot_tier_size.fetch_sub(entry.data.len(), Ordering::Relaxed);
        }

        let shm_path = self.shm_dir.join(id.to_string());
        if shm_path.exists() {
            let _ = tokio::fs::remove_file(shm_path).await;
        }

        let warm_path = self.warm_dir.join(id.to_string());
        if warm_path.exists() {
            tokio::fs::remove_file(warm_path).await.map_err(FrameworkError::Io)?;
        }

        Ok(())
    }

    pub async fn wait_for_object(&self, id: ObjectId) {
        let notify = self
            .waiters
            .entry(id)
            .or_insert_with(|| Arc::new(tokio::sync::Notify::new()))
            .clone();

        let future = notify.notified();
        tokio::pin!(future);
        future.as_mut().enable();

        if self.get_any(id).await.is_ok() {
            return;
        }

        future.await;
    }

    // Helper functions

    async fn write_shm(&self, id: ObjectId, data: &[u8]) -> Result<()> {
        tokio::fs::create_dir_all(&self.shm_dir)
            .await
            .map_err(FrameworkError::Io)?;
        let path = self.shm_dir.join(id.to_string());
        tokio::fs::write(path, data)
            .await
            .map_err(FrameworkError::Io)?;
        Ok(())
    }

    fn notify_waiters(&self, id: ObjectId) {
        if let Some((_, notify)) = self.waiters.remove(&id) {
            notify.notify_waiters();
        }
    }

    async fn evict_if_needed(&self, needed_bytes: usize) -> Result<()> {
        while self.hot_tier_size.load(Ordering::Relaxed) + needed_bytes > self.max_hot_tier_bytes {
            let Some(id) = self.find_oldest_hot_entry() else { break; };
            let Some((_, entry)) = self.hot_tier.remove(&id) else { continue; };
            self.hot_tier_size.fetch_sub(entry.data.len(), Ordering::Relaxed);

            // Spill to warm tier on disk
            tokio::fs::create_dir_all(&self.warm_dir)
                .await
                .map_err(FrameworkError::Io)?;
            let path = self.warm_dir.join(id.to_string());
            tokio::fs::write(path, entry.data)
                .await
                .map_err(FrameworkError::Io)?;

            // Free physical tmpfs RAM by removing the shared memory file
            let shm_path = self.shm_dir.join(id.to_string());
            if shm_path.exists() {
                let _ = tokio::fs::remove_file(shm_path).await;
            }
        }
        Ok(())
    }

    fn find_oldest_hot_entry(&self) -> Option<ObjectId> {
        let mut oldest_id = None;
        let mut oldest_time = u64::MAX;

        for entry in self.hot_tier.iter() {
            let last = entry.value().last_accessed.load(Ordering::Relaxed);
            if last < oldest_time {
                oldest_time = last;
                oldest_id = Some(*entry.key());
            }
        }
        oldest_id
    }
}
