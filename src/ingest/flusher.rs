use crate::runtime::state::{AppState, FlushMessage};
use tokio::sync::mpsc;
use tracing::{info, error};
use crate::model::event::UsageEvent;
use crate::storage::segment_writer::RawSegmentWriter;
use crate::storage::manifest::{SegmentMeta, SegmentKind};
use crate::ingest::wal::Wal;
use crate::model::ids::AccountId;
use std::collections::HashSet;

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
        info!("Flushing {} events to raw segment (sealed wal_id={})", events.len(), sealed_wal_id);

        let segment_id = format!("raw_{}", uuid::Uuid::new_v4().simple());
        let path = self.state.config.db_root.join(format!("{}.seg", segment_id));

        // Write the segment file. On any write error, abort: do NOT update
        // the manifest and do NOT delete WAL files. Recovery will replay.
        let mut writer = match RawSegmentWriter::new(path.clone()) {
            Ok(w) => w,
            Err(e) => {
                error!("Failed to create segment writer: {}", e);
                return;
            }
        };
        for event in &events {
            if let Err(e) = writer.write_event(event) {
                error!("Failed to write event to segment {}: {} — aborting flush", segment_id, e);
                let _ = std::fs::remove_file(&path);
                return;
            }
        }
        if let Err(e) = writer.finish() {
            error!("Failed to finish raw segment {}: {} — aborting flush", segment_id, e);
            let _ = std::fs::remove_file(&path);
            return;
        }

        let meta = build_segment_meta(&segment_id, &events);

        // Atomically update the manifest: append segment, advance the WAL
        // seal pointer, then save. Save() does the rename+fsync dance.
        {
            let mut manifest = self.state.manifest.write().await;
            manifest.raw_segments.push(meta);
            if sealed_wal_id > manifest.last_sealed_wal_id {
                manifest.last_sealed_wal_id = sealed_wal_id;
            }
            if let Err(e) = manifest.save(&self.state.config.db_root) {
                error!("Failed to save manifest after flush: {} — segment is on disk, WAL kept", e);
                return;
            }
        }

        info!("Successfully flushed segment {}", segment_id);

        // Now that the manifest is durable, sealed WAL files can be deleted.
        // If this fails the next recovery will clean up; not critical.
        let wal_dir = self.state.config.db_root.join("wal");
        if let Err(e) = Wal::delete_files_through(&wal_dir, sealed_wal_id) {
            error!("Failed to delete sealed WAL files <= {}: {} (will retry at next recovery)", sealed_wal_id, e);
        }
    }
}

/// Build a SegmentMeta covering all events in the batch.
pub fn build_segment_meta(segment_id: &str, batch: &[UsageEvent]) -> SegmentMeta {
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
        bucket: 0, // TODO: hash(account_id) % bucket_count
        row_count: batch.len() as u64,
        min_account_id: min_account.map(AccountId),
        max_account_id: max_account.map(AccountId),
        product_ids,
        meter_ids,
        model_ids,
        quantity_sum: Some(quantity_sum),
        checksum: 0, // TODO: CRC over segment bytes
    }
}
