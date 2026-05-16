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
}
