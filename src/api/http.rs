use serde::{Deserialize, Serialize};
use crate::model::event::UsageEvent;
use crate::model::ids::{AccountId, ProductId, MeterId, ModelId};
use crate::model::query::GroupKey;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageQueryRequest {
    pub account_id: AccountId,
    pub from_ms: i64,
    pub to_ms: i64,
    pub product_id: Option<ProductId>,
    pub meter_id: Option<MeterId>,
    pub model_id: Option<ModelId>,
    pub group_by: Vec<GroupKey>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageQueryResponseLine {
    pub product_id: ProductId,
    pub meter_id: MeterId,
    pub model_id: Option<ModelId>,
    pub quantity: i128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageQueryResponse {
    pub account_id: AccountId,
    pub from_ms: i64,
    pub to_ms: i64,
    pub watermark_ms: i64,
    pub lines: Vec<UsageQueryResponseLine>,
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
