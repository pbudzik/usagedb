use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub db_root: PathBuf,
    pub max_memtable_size_bytes: usize,
    pub http_bind_address: String,
    /// Maximum number of event_id hashes to keep in the hot dedupe cache.
    pub dedupe_capacity: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            db_root: PathBuf::from("./data"),
            max_memtable_size_bytes: 64 * 1024 * 1024, // 64 MB
            http_bind_address: "127.0.0.1:8080".to_string(),
            dedupe_capacity: 1_000_000,
        }
    }
}
