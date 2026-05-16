use crate::storage::manifest::SegmentMeta;
use std::collections::HashMap;

/// One compaction job: merge `segment_ids` (all in the same bucket) into a
/// single output segment.
#[derive(Debug, Clone)]
pub struct CompactionPlan {
    pub bucket: u32,
    pub segment_ids: Vec<String>,
}

pub struct CompactionPlanner {
    pub max_small_segments: usize,
    pub max_small_size_bytes: u64,
}

impl CompactionPlanner {
    pub fn new() -> Self {
        Self {
            max_small_segments: 16,
            max_small_size_bytes: 32 * 1024 * 1024, // 32 MB
        }
    }

    /// Group raw segments by bucket; emit one plan per bucket that has more
    /// than `max_small_segments` segments. Compaction inputs share a bucket
    /// so the merge output can stay bucketed.
    pub fn plan_compaction(&self, segments: &[SegmentMeta]) -> Vec<CompactionPlan> {
        let mut by_bucket: HashMap<u32, Vec<&SegmentMeta>> = HashMap::new();
        for seg in segments {
            by_bucket.entry(seg.bucket).or_default().push(seg);
        }

        let mut plans = Vec::new();
        for (bucket, bucket_segments) in by_bucket {
            if bucket_segments.len() > self.max_small_segments {
                let segment_ids = bucket_segments.iter().map(|s| s.segment_id.clone()).collect();
                plans.push(CompactionPlan { bucket, segment_ids });
            }
        }
        plans
    }
}

impl Default for CompactionPlanner {
    fn default() -> Self {
        Self::new()
    }
}
