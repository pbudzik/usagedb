use crate::runtime::state::AppState;
use tokio::sync::mpsc;
use tracing::{info, error};
use crate::model::event::UsageEvent;
use crate::storage::segment_writer::RawSegmentWriter;
use crate::storage::manifest::{SegmentMeta, SegmentKind};
use std::collections::HashSet;

pub struct FlusherWorker {
    state: AppState,
    receiver: mpsc::Receiver<Vec<UsageEvent>>,
}

impl FlusherWorker {
    pub fn new(state: AppState, receiver: mpsc::Receiver<Vec<UsageEvent>>) -> Self {
        Self { state, receiver }
    }

    pub async fn run(mut self) {
        info!("Background flusher started");
        while let Some(batch) = self.receiver.recv().await {
            info!("Flushing {} events to raw segment", batch.len());
            
            let segment_id = format!("raw_{}", uuid::Uuid::new_v4().simple());
            let path = self.state.config.db_root.join(format!("{}.seg", segment_id));
            
            match RawSegmentWriter::new(path) {
                Ok(mut writer) => {
                    for event in &batch {
                        if let Err(e) = writer.write_event(event) {
                            error!("Failed to write event to segment: {}", e);
                        }
                    }
                    if let Err(e) = writer.finish() {
                        error!("Failed to finish raw segment: {}", e);
                    } else {
                        info!("Successfully flushed segment {}", segment_id);
                        
                        let meta = Self::build_segment_meta(&segment_id, &batch);
                        let mut manifest = self.state.manifest.write().await;
                        manifest.raw_segments.push(meta);
                        
                        if let Err(e) = manifest.save(&self.state.config.db_root) {
                            error!("Failed to save manifest after flush: {}", e);
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to create segment writer: {}", e);
                }
            }
        }
        info!("Background flusher stopped");
    }

    /// Build a complete SegmentMeta from the batch of events being flushed.
    fn build_segment_meta(segment_id: &str, batch: &[UsageEvent]) -> SegmentMeta {
        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;
        let mut min_account: Option<String> = None;
        let mut max_account: Option<String> = None;
        let mut product_ids = HashSet::new();
        let mut meter_ids = HashSet::new();
        let mut model_ids = HashSet::new();
        let mut quantity_sum: i128 = 0;

        for event in batch {
            if event.timestamp_ms < min_ts {
                min_ts = event.timestamp_ms;
            }
            if event.timestamp_ms > max_ts {
                max_ts = event.timestamp_ms;
            }

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

        use crate::model::ids::AccountId;

        SegmentMeta {
            segment_id: segment_id.to_string(),
            kind: SegmentKind::Raw,
            min_timestamp_ms: if min_ts == i64::MAX { 0 } else { min_ts },
            max_timestamp_ms: if max_ts == i64::MIN { 0 } else { max_ts },
            bucket: 0, // TODO: implement bucket assignment from hash(account_id)
            row_count: batch.len() as u64,
            min_account_id: min_account.map(AccountId),
            max_account_id: max_account.map(AccountId),
            product_ids,
            meter_ids,
            model_ids,
            quantity_sum: Some(quantity_sum),
            checksum: 0, // TODO: compute CRC/checksum over segment bytes
        }
    }
}
