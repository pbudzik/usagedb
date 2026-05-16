use serde::{Deserialize, Serialize};
use crate::model::event::UsageEvent;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageQueryRequest {
    pub source: String,
    pub account_id: String,
    pub from: String,
    pub to: String,
    pub group_by: Vec<String>,
    pub filters: Option<std::collections::HashMap<String, Vec<String>>>,
    pub metrics: Option<std::collections::HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlQueryRequest {
    pub query: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestBatchRequest {
    pub events: Vec<UsageEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestBatchResponse {
    pub accepted: usize,
    pub duplicates: usize,
    pub conflicts: usize,
    pub rejected: usize,
}
