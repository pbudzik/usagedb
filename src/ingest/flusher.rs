use crate::runtime::state::AppState;
use tokio::sync::mpsc;
use tracing::{info, error};
use crate::model::event::UsageEvent;
use crate::storage::segment_writer::RawSegmentWriter;

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
                        
                        let mut manifest = self.state.manifest.write().await;
                        manifest.raw_segments.push(crate::storage::manifest::SegmentMeta {
                            segment_id: segment_id.clone(),
                            kind: crate::storage::manifest::SegmentKind::Raw,
                            min_timestamp_ms: 0,
                            max_timestamp_ms: 0,
                            bucket: 0,
                            row_count: batch.len() as u64,
                            min_account_id: None,
                            max_account_id: None,
                            product_ids: std::collections::HashSet::new(),
                            meter_ids: std::collections::HashSet::new(),
                            model_ids: std::collections::HashSet::new(),
                            quantity_sum: None,
                            checksum: 0,
                        });
                        
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
}
