use crate::runtime::state::{AppState, FlushMessage};
use tokio::sync::mpsc;
use tracing::{info, error};
use crate::model::event::UsageEvent;
use crate::model::ids::{AccountId, bucket_for_account};
use crate::storage::segment_writer::RawSegmentWriter;
use crate::storage::manifest::{SegmentMeta, SegmentKind};
use crate::ingest::wal::Wal;
use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;

pub struct FlusherWorker {
    state: AppState,
    receiver: mpsc::Receiver<FlushMessage>,
}

/// Returned by `attempt_flush` when something goes wrong. `events` is the
/// set of events that were not durably committed and should be retried;
/// the caller (`handle_message`) re-inserts them into the memtable.
struct FlushFailure {
    events: Vec<UsageEvent>,
    reason: String,
}

impl FlusherWorker {
    pub fn new(state: AppState, receiver: mpsc::Receiver<FlushMessage>) -> Self {
        Self { state, receiver }
    }

    pub async fn run(mut self) {
        info!("Background flusher started");
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
        info!("Background flusher stopped");
    }

    async fn handle_message(&self, msg: FlushMessage) {
        let FlushMessage { events, sealed_wal_id } = msg;
        if events.is_empty() {
            return;
        }

        // On any pre-commit failure path the flush worker re-inserts the
        // events into the memtable so they'll be retried on the next
        // flush trigger (review P1 #1). Without this they sit in the
        // sealed WAL file and are invisible to queries until restart.
        if let Err(failure) = self.attempt_flush(events, sealed_wal_id).await {
            error!(
                "Flush failed: {} — re-inserting {} events into memtable for retry",
                failure.reason,
                failure.events.len()
            );
            if !failure.events.is_empty() {
                let mut memtable = self.state.memtable.lock().await;
                for event in failure.events {
                    memtable.insert(event);
                }
            }
        }
    }

    async fn attempt_flush(
        &self,
        events: Vec<UsageEvent>,
        sealed_wal_id: u64,
    ) -> Result<(), FlushFailure> {
        let bucket_count = {
            let manifest = self.state.manifest.read().await;
            manifest.bucket_count.max(1)
        };
        let mut by_bucket: BTreeMap<u32, Vec<UsageEvent>> = BTreeMap::new();
        for event in events {
            let bucket = bucket_for_account(&event.account_id, bucket_count);
            by_bucket.entry(bucket).or_default().push(event);
        }
        info!(
            "Flushing {} bucket(s) to disk (sealed wal_id={})",
            by_bucket.len(), sealed_wal_id
        );

        let mut new_metas: Vec<SegmentMeta> = Vec::with_capacity(by_bucket.len());
        let mut written_paths: Vec<PathBuf> = Vec::with_capacity(by_bucket.len());

        let buckets: Vec<u32> = by_bucket.keys().copied().collect();
        for bucket in buckets {
            let bucket_events = by_bucket.remove(&bucket).unwrap();
            match self.write_bucket(bucket, &bucket_events) {
                Ok((meta, path)) => {
                    new_metas.push(meta);
                    written_paths.push(path);
                }
                Err(reason) => {
                    rollback_partial(&written_paths);
                    let mut remaining = bucket_events;
                    for (_, evs) in by_bucket {
                        remaining.extend(evs);
                    }
                    return Err(FlushFailure { events: remaining, reason });
                }
            }
        }

        // All bucket segments are durable. Commit the manifest atomically
        // with the WAL seal pointer.
        let save_result = {
            let mut manifest = self.state.manifest.write().await;
            for meta in &new_metas {
                manifest.raw_segments.push(meta.clone());
            }
            if sealed_wal_id > manifest.last_sealed_wal_id {
                manifest.last_sealed_wal_id = sealed_wal_id;
            }
            manifest.save(&self.state.config.db_root)
        };

        if let Err(e) = save_result {
            // Manifest save failed. The segments we wrote are orphaned;
            // remove them. The events themselves are still durable in the
            // sealed WAL file (we haven't called delete_files_through),
            // so the right thing is to surface an empty retry list —
            // recovery will replay the WAL on next start and the next
            // flusher message will re-attempt. Returning the events here
            // would risk a double-flush on restart.
            rollback_partial(&written_paths);
            return Err(FlushFailure {
                events: Vec::new(),
                reason: format!(
                    "manifest save: {} — segment files orphaned and removed; \
                     events still durable in WAL file {} for recovery to replay",
                    e, sealed_wal_id
                ),
            });
        }

        info!(
            "Flush committed: {} segments ({}), wal sealed through {}",
            new_metas.len(),
            new_metas.iter().map(|m| m.segment_id.as_str()).collect::<Vec<_>>().join(", "),
            sealed_wal_id
        );

        let wal_dir = self.state.config.db_root.join("wal");
        if let Err(e) = Wal::delete_files_through(&wal_dir, sealed_wal_id) {
            error!(
                "Failed to delete sealed WAL files <= {}: {} (recovery will retry)",
                sealed_wal_id, e
            );
        }
        Ok(())
    }

    /// Write one bucket's segment + return its metadata. Returns Err with
    /// a human-readable reason on any failure; the caller cleans up the
    /// partial file and any earlier bucket segments via `rollback_partial`.
    fn write_bucket(
        &self,
        bucket: u32,
        bucket_events: &[UsageEvent],
    ) -> Result<(SegmentMeta, PathBuf), String> {
        let segment_id = format!("raw_{}", uuid::Uuid::new_v4().simple());
        let path = self.state.config.db_root.join(format!("{}.seg", segment_id));

        let mut writer = match RawSegmentWriter::new(path.clone()) {
            Ok(w) => w,
            Err(e) => return Err(format!("create segment for bucket {}: {}", bucket, e)),
        };
        for event in bucket_events {
            if let Err(e) = writer.write_event(event) {
                let _ = std::fs::remove_file(&path);
                return Err(format!("write event to bucket {} segment {}: {}", bucket, segment_id, e));
            }
        }
        let (_row_count, checksum) = match writer.finish() {
            Ok(t) => t,
            Err(e) => {
                let _ = std::fs::remove_file(&path);
                return Err(format!("finish bucket {} segment {}: {}", bucket, segment_id, e));
            }
        };

        Ok((build_segment_meta(&segment_id, bucket_events, bucket, checksum), path))
    }
}

fn rollback_partial(paths: &[PathBuf]) {
    for path in paths {
        let _ = std::fs::remove_file(path);
    }
}

/// Build a SegmentMeta covering all events in the batch with the given bucket
/// and segment checksum (returned by `RawSegmentWriter::finish`).
pub fn build_segment_meta(segment_id: &str, batch: &[UsageEvent], bucket: u32, checksum: u64) -> SegmentMeta {
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    let mut min_account: Option<String> = None;
    let mut max_account: Option<String> = None;
    let mut product_ids = HashSet::new();
    let mut meter_ids = HashSet::new();
    let mut model_ids = HashSet::new();
    let mut quantity_sum: i128 = 0;

    for event in batch {
        if event.timestamp_ms < min_ts { min_ts = event.timestamp_ms; }
        if event.timestamp_ms > max_ts { max_ts = event.timestamp_ms; }

        let acc = &event.account_id.0;
        match &min_account {
            None => { min_account = Some(acc.clone()); }
            Some(current) if acc < current => { min_account = Some(acc.clone()); }
            _ => {}
        }
        match &max_account {
            None => { max_account = Some(acc.clone()); }
            Some(current) if acc > current => { max_account = Some(acc.clone()); }
            _ => {}
        }

        product_ids.insert(event.product_id.clone());
        meter_ids.insert(event.meter_id.clone());
        if let Some(model) = &event.model_id {
            model_ids.insert(model.clone());
        }
        quantity_sum = quantity_sum.saturating_add(event.quantity);
    }

    SegmentMeta {
        segment_id: segment_id.to_string(),
        kind: SegmentKind::Raw,
        min_timestamp_ms: if min_ts == i64::MAX { 0 } else { min_ts },
        max_timestamp_ms: if max_ts == i64::MIN { 0 } else { max_ts },
        bucket,
        row_count: batch.len() as u64,
        min_account_id: min_account.map(AccountId),
        max_account_id: max_account.map(AccountId),
        product_ids,
        meter_ids,
        model_ids,
        quantity_sum: Some(quantity_sum),
        checksum,
        // Raw segments have no inputs — they're the ground truth.
        // Compacted segments also leave this empty; their provenance is
        // tracked via Manifest.compacted_replacements.
        input_segment_ids: Vec::new(),
    }
}
