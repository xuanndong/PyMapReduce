pub mod object_ref;
pub mod store;
pub mod tier;

pub const SMALL_OBJECT_THRESHOLD_BYTES: u64 = 512 * 1024 * 1024; // 512 MB
pub const DEFAULT_CHUNK_SIZE_BYTES: usize = 64 * 1024 * 1024; // 64 MB
