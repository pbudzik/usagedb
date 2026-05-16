use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub db_root: PathBuf,
    pub max_memtable_size_bytes: usize,
    pub http_bind_address: String,
    /// Maximum number of event_id hashes to keep in the hot dedupe cache.
    pub dedupe_capacity: usize,
    /// Number of partitioning buckets per day. Fixed at DB creation time —
    /// stored in the manifest and never changed thereafter, since bucket
    /// assignment is `hash(account_id) % bucket_count` and changing the
    /// modulus would invalidate every existing segment's bucket label.
    /// Spec §5.2 recommends 64 for small installs, 256 for medium, 512+
    /// for large.
    pub default_bucket_count: u32,
    /// How often the background rollup worker checks for new hours to seal.
    pub rollup_tick_interval_secs: u64,
    /// Number of milliseconds to wait past an hour boundary before treating
    /// the hour as sealed for rollup. Allows in-flight events from that
    /// hour to land in raw segments first. Spec §11.3 covers the open-period
    /// query handling; this lag is the bound on "open period" duration.
    pub rollup_safety_lag_ms: i64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            db_root: PathBuf::from("./data"),
            max_memtable_size_bytes: 64 * 1024 * 1024, // 64 MB
            http_bind_address: "127.0.0.1:8080".to_string(),
            dedupe_capacity: 1_000_000,
            default_bucket_count: 64,
            rollup_tick_interval_secs: 30,
            rollup_safety_lag_ms: 60_000, // 1 minute
        }
    }
}
