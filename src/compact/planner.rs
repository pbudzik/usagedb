use crate::storage::manifest::SegmentMeta;
use std::collections::HashMap;

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

    pub fn plan_compaction(&self, segments: &[SegmentMeta]) -> Vec<Vec<String>> {
        // Group segments by bucket
        let mut by_bucket: HashMap<u32, Vec<&SegmentMeta>> = HashMap::new();
        for seg in segments {
            by_bucket.entry(seg.bucket).or_default().push(seg);
        }

        let mut compaction_plans = Vec::new();
        for (_, bucket_segments) in by_bucket {
            if bucket_segments.len() > self.max_small_segments {
                let to_compact = bucket_segments.iter().map(|s| s.segment_id.clone()).collect();
                compaction_plans.push(to_compact);
            }
        }

        compaction_plans
    }
}

impl Default for CompactionPlanner {
    fn default() -> Self {
        Self::new()
    }
}
