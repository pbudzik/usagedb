use crate::storage::manifest::ReplacementRecord;
use std::io::Result as IoResult;

pub struct CompactionWorker {
}

impl CompactionWorker {
    pub fn new() -> Self {
        Self {}
    }

    pub fn run_compaction(&self, input_segment_ids: Vec<String>) -> IoResult<ReplacementRecord> {
        // 1. read and merge rows from input_segment_ids
        // 2. sort by account_id, product_id, meter_id, model_id, timestamp_ms
        // 3. dedupe exact event_id duplicates
        // 4. write output segment
        
        let output_segment_id = format!("compacted_{}", uuid::Uuid::new_v4().simple());

        // Returns replacement record to atomically update the manifest
        Ok(ReplacementRecord {
            old_segments: input_segment_ids,
            new_segments: vec![output_segment_id],
        })
    }
}

impl Default for CompactionWorker {
    fn default() -> Self {
        Self::new()
    }
}
