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

        // Partition events by bucket so each segment covers a single
        // (date_partition_implicit_via_ts, bucket). Mixed-account batches
        // are split into one segment per bucket they touch.
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

        // Write each bucket's segment + collect metas. We update the
        // manifest once at the end so the sealed-wal pointer advances
        // atomically with all segments from this drain.
        let mut new_metas: Vec<SegmentMeta> = Vec::with_capacity(by_bucket.len());
        let mut written_paths: Vec<PathBuf> = Vec::with_capacity(by_bucket.len());

        for (bucket, bucket_events) in by_bucket {
            let segment_id = format!("raw_{}", uuid::Uuid::new_v4().simple());
            let path = self.state.config.db_root.join(format!("{}.seg", segment_id));

            let mut writer = match RawSegmentWriter::new(path.clone()) {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to create segment writer for bucket {}: {}", bucket, e);
                    rollback_partial(&written_paths);
                    return;
                }
            };
            let mut write_failed = false;
            for event in &bucket_events {
                if let Err(e) = writer.write_event(event) {
                    error!("Failed to write event to bucket {} segment {}: {}", bucket, segment_id, e);
                    write_failed = true;
                    break;
                }
            }
            if write_failed {
                let _ = std::fs::remove_file(&path);
                rollback_partial(&written_paths);
                return;
            }
            if let Err(e) = writer.finish() {
                error!("Failed to finish bucket {} segment {}: {}", bucket, segment_id, e);
                let _ = std::fs::remove_file(&path);
                rollback_partial(&written_paths);
                return;
            }

            new_metas.push(build_segment_meta(&segment_id, &bucket_events, bucket));
            written_paths.push(path);
        }

        // All bucket segments are durable on disk. Commit the manifest
        // atomically with the WAL seal pointer.
        {
            let mut manifest = self.state.manifest.write().await;
            for meta in &new_metas {
                manifest.raw_segments.push(meta.clone());
            }
            if sealed_wal_id > manifest.last_sealed_wal_id {
                manifest.last_sealed_wal_id = sealed_wal_id;
            }
            if let Err(e) = manifest.save(&self.state.config.db_root) {
                error!(
                    "Failed to save manifest after flush: {} — segments are on disk but unreferenced; will be cleaned up by recovery",
                    e
                );
                return;
            }
        }

        info!(
            "Flush committed: {} segments ({}), wal sealed through {}",
            new_metas.len(),
            new_metas.iter().map(|m| m.segment_id.as_str()).collect::<Vec<_>>().join(", "),
            sealed_wal_id
        );

        let wal_dir = self.state.config.db_root.join("wal");
        if let Err(e) = Wal::delete_files_through(&wal_dir, sealed_wal_id) {
            error!("Failed to delete sealed WAL files <= {}: {} (recovery will retry)", sealed_wal_id, e);
        }
    }
}

fn rollback_partial(paths: &[PathBuf]) {
    for path in paths {
        let _ = std::fs::remove_file(path);
    }
}

/// Build a SegmentMeta covering all events in the batch with the given bucket.
pub fn build_segment_meta(segment_id: &str, batch: &[UsageEvent], bucket: u32) -> SegmentMeta {
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
        checksum: 0, // TODO: CRC over segment bytes (waiting on columnar format)
    }
}
