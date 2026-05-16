use crate::storage::manifest::{ReplacementRecord, SegmentMeta};
use crate::storage::segment_reader::RawSegmentReader;
use crate::storage::segment_writer::RawSegmentWriter;
use crate::ingest::flusher::build_segment_meta;
use crate::model::event::UsageEvent;
use std::collections::HashSet;
use std::io::Result as IoResult;
use std::path::Path;
use tracing::{info, warn};

pub struct CompactionWorker;

impl CompactionWorker {
    pub fn new() -> Self {
        Self
    }

    /// Merge `input_segment_ids` into a single output segment. Returns the
    /// new segment's metadata plus a ReplacementRecord that the manifest
    /// updater can use to atomically swap old → new and delete the inputs.
    /// Sort order matches the spec: account, product, meter, model, ts.
    /// Exact event_id duplicates are removed (cold dedupe).
    pub fn run_compaction(
        &self,
        db_root: &Path,
        input_segment_ids: &[String],
    ) -> IoResult<(SegmentMeta, ReplacementRecord)> {
        let mut events: Vec<UsageEvent> = Vec::new();
        for id in input_segment_ids {
            let path = db_root.join(format!("{}.seg", id));
            if !path.exists() {
                warn!("compaction: input segment {} missing from disk, skipping", id);
                continue;
            }
            let mut reader = RawSegmentReader::new(path)?;
            while let Some(event) = reader.read_next()? {
                events.push(event);
            }
        }

        events.sort_by(|a, b| {
            a.account_id.0.cmp(&b.account_id.0)
                .then_with(|| a.product_id.0.cmp(&b.product_id.0))
                .then_with(|| a.meter_id.0.cmp(&b.meter_id.0))
                .then_with(|| {
                    let am = a.model_id.as_ref().map(|m| m.0.as_str()).unwrap_or("");
                    let bm = b.model_id.as_ref().map(|m| m.0.as_str()).unwrap_or("");
                    am.cmp(bm)
                })
                .then_with(|| a.timestamp_ms.cmp(&b.timestamp_ms))
        });

        // Cold dedupe: drop subsequent occurrences of the same event_id.
        // First occurrence wins; spec §10.2 calls out conflict quarantine
        // but the cold dedupe path here trusts hot dedupe to have flagged
        // conflicts upstream — duplicates here are payload-identical retries.
        let mut seen: HashSet<String> = HashSet::with_capacity(events.len());
        let mut deduped = Vec::with_capacity(events.len());
        for e in events {
            if seen.insert(e.event_id.0.clone()) {
                deduped.push(e);
            }
        }

        let output_id = format!("compacted_{}", uuid::Uuid::new_v4().simple());
        let output_path = db_root.join(format!("{}.seg", output_id));
        let mut writer = RawSegmentWriter::new(output_path.clone())?;
        for e in &deduped {
            if let Err(err) = writer.write_event(e) {
                // Abort: clean up partial output.
                let _ = std::fs::remove_file(&output_path);
                return Err(err);
            }
        }
        writer.finish()?;

        info!(
            "compaction: merged {} inputs into {} ({} rows after dedupe)",
            input_segment_ids.len(),
            output_id,
            deduped.len()
        );

        let meta = build_segment_meta(&output_id, &deduped);
        Ok((
            meta,
            ReplacementRecord {
                old_segments: input_segment_ids.to_vec(),
                new_segments: vec![output_id],
            },
        ))
    }
}

impl Default for CompactionWorker {
    fn default() -> Self {
        Self::new()
    }
}
