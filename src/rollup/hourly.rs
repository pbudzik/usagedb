use serde::{Deserialize, Serialize};
use crate::model::ids::{AccountId, SubscriptionId, ProductId, MeterId, ModelId};
use std::collections::HashMap;

/// Aggregation key for an hourly rollup row. `dimensions_canonical` is the
/// canonical JSON of the event's `SmallDimensions` — stored verbatim in
/// the record so rollups from different segments are cross-comparable
/// (the previous u32 encoding was local to a single builder instance).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HourlyRollupKey {
    pub account_id: AccountId,
    pub subscription_id: Option<SubscriptionId>,
    pub product_id: ProductId,
    pub meter_id: MeterId,
    pub model_id: Option<ModelId>,
    pub hour_start_ms: i64,
    pub dimensions_canonical: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HourlyRollupRecord {
    pub account_id: AccountId,
    pub subscription_id: Option<SubscriptionId>,
    pub product_id: ProductId,
    pub meter_id: MeterId,
    pub model_id: Option<ModelId>,
    pub hour_start_ms: i64,
    pub dimensions_canonical: String,

    pub quantity_sum: i128,
    pub event_count: u64,
    pub first_event_ms: i64,
    pub last_event_ms: i64,
}

#[derive(Default)]
pub struct HourlyRollupState {
    pub aggregates: HashMap<HourlyRollupKey, HourlyRollupRecord>,
}

impl HourlyRollupState {
    pub fn new() -> Self {
        Self::default()
    }
}
