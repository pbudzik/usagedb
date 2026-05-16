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
                        // In a complete implementation, we'd update the Manifest here
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
