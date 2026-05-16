use serde::{Deserialize, Serialize};
use crate::model::ids::{EventId, AccountId, SubscriptionId, ProductId, MeterId, SourceId, ModelId, Unit};
use crate::model::dimensions::SmallDimensions;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventKind {
    Usage,
    Correction,
    Retraction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorrectionRef {
    pub original_event_id: EventId,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageEvent {
    pub event_id: EventId,
    pub kind: EventKind,
    pub correction_ref: Option<CorrectionRef>,
    pub account_id: AccountId,
    pub subscription_id: Option<SubscriptionId>,
    pub product_id: ProductId,
    pub meter_id: MeterId,
    pub timestamp_ms: i64,
    pub quantity: i128,
    pub unit: Unit,
    pub source: SourceId,
    pub model_id: Option<ModelId>,
    pub dimensions: SmallDimensions,
    pub ingested_at_ms: i64,
}

