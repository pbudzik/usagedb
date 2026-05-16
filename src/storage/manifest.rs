use serde::{Deserialize, Serialize};
use crate::model::ids::{AccountId, ProductId, MeterId, ModelId};
use std::collections::HashSet;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SegmentKind {
    Raw,
    Rollup,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMeta {
    pub segment_id: String,
    pub kind: SegmentKind,
    pub min_timestamp_ms: i64,
    pub max_timestamp_ms: i64,
    pub bucket: u32,
    pub row_count: u64,
    pub min_account_id: Option<AccountId>,
    pub max_account_id: Option<AccountId>,
    pub product_ids: HashSet<ProductId>,
    pub meter_ids: HashSet<MeterId>,
    pub model_ids: HashSet<ModelId>,
    pub quantity_sum: Option<i128>,
    pub checksum: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplacementRecord {
    pub old_segments: Vec<String>,
    pub new_segments: Vec<String>,
    /// When the compaction commit happened (unix ms). Used to enforce a
    /// reader grace period before old files are physically deleted —
    /// queries that snapshotted the manifest before the commit may still
    /// be holding old segment IDs in memory.
    #[serde(default)]
    pub committed_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Watermarks {
    pub hourly_rollup_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Manifest {
    pub db_version: u32,
    pub bucket_count: u32,
    pub raw_segments: Vec<SegmentMeta>,
    pub rollup_segments: Vec<SegmentMeta>,
    pub compacted_replacements: Vec<ReplacementRecord>,
    pub watermarks: Watermarks,
    /// Highest WAL file ID whose contents are durably in committed segments.
    /// WAL files with id <= this can be deleted; files with id > this must be
    /// replayed on recovery.
    #[serde(default)]
    pub last_sealed_wal_id: u64,
}

impl Manifest {
    pub fn save(&self, db_root: &std::path::Path) -> std::io::Result<()> {
        use std::io::Write;
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let manifest_path = db_root.join("manifest.json");
        let tmp_path = db_root.join("manifest.json.tmp");

        // Write + sync on a single fd before rename. Previous code used
        // fs::write (no fsync) and then re-opened the file read-only to
        // sync — that works on Linux because sync_all flushes the inode's
        // dirty pages via any fd, but the contract is shakier on other
        // platforms. This is the standard atomic-write recipe.
        {
            let mut file = std::fs::File::create(&tmp_path)?;
            file.write_all(json.as_bytes())?;
            file.sync_all()?;
            // file dropped here, closing the fd before rename
        }
        std::fs::rename(&tmp_path, &manifest_path)?;

        // Fsync the parent directory so the rename is durable (spec §14 step 7).
        let parent = std::fs::File::open(db_root)?;
        parent.sync_all()?;
        Ok(())
    }
}
