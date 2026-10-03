use super::store::ObjectStore;
use super::tier::StorageTier;
use crate::protocol::message::ObjectId;
use crate::types::error::Result;
use bytes::Bytes;
use futures::stream::Stream;
use std::sync::Arc;

#[derive(Clone)]
pub struct ObjectRef {
    id: ObjectId,
    tier: StorageTier,
    store: Arc<ObjectStore>,
}

pub enum ObjectData {
    Complete(Bytes),
    Stream(Box<dyn Stream<Item = Result<Bytes>> + Send + Unpin>),
}

impl ObjectRef {
    pub fn new(id: ObjectId, tier: StorageTier, store: Arc<ObjectStore>) -> Self {
        Self { id, tier, store }
    }

    pub async fn get(&self) -> Result<ObjectData> {
        let data = self.store.get(self.id, self.tier).await?;
        Ok(ObjectData::Complete(data))
    }
}
