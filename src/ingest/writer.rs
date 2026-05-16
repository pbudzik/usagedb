use crate::model::event::UsageEvent;
use crate::api::http::IngestBatchResponse;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum UsageError {
    #[error("Internal storage error: {0}")]
    StorageError(String),
    #[error("Validation error: {0}")]
    ValidationError(String),
}

pub trait UsageWriter {
    fn ingest_batch(&self, batch: Vec<UsageEvent>) -> Result<IngestBatchResponse, UsageError>;
}
