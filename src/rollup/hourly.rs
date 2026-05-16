use serde::{Deserialize, Serialize};
use crate::model::ids::{AccountId, SubscriptionId, ProductId, MeterId, ModelId};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HourlyRollupKey {
    pub account_id: AccountId,
    pub subscription_id: Option<SubscriptionId>,
    pub product_id: ProductId,
    pub meter_id: MeterId,
    pub model_id: Option<ModelId>,
    pub hour_start_ms: i64,
    pub dimensions_key: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HourlyRollupRecord {
    pub account_id: AccountId,
    pub subscription_id: Option<SubscriptionId>,
    pub product_id: ProductId,
    pub meter_id: MeterId,
    pub model_id: Option<ModelId>,
    pub hour_start_ms: i64,
    pub dimensions_key: u32,
    
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
