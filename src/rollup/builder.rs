use crate::model::event::UsageEvent;
use crate::rollup::hourly::{HourlyRollupKey, HourlyRollupRecord, HourlyRollupState};
use crate::storage::encoding::DictionaryEncoder;

pub struct RollupBuilder {
    state: HourlyRollupState,
    dim_encoder: DictionaryEncoder,
}

impl RollupBuilder {
    pub fn new() -> Self {
        Self {
            state: HourlyRollupState::new(),
            dim_encoder: DictionaryEncoder::new(),
        }
    }

    pub fn process_event(&mut self, event: &UsageEvent) {
        let hour_ms = (event.timestamp_ms / 3_600_000) * 3_600_000;
        
        let dim_str = serde_json::to_string(&event.dimensions).unwrap_or_default();
        let dim_key = self.dim_encoder.encode(&dim_str);

        let key = HourlyRollupKey {
            account_id: event.account_id.clone(),
            subscription_id: event.subscription_id.clone(),
            product_id: event.product_id.clone(),
            meter_id: event.meter_id.clone(),
            model_id: event.model_id.clone(),
            hour_start_ms: hour_ms,
            dimensions_key: dim_key,
        };

        let entry = self.state.aggregates.entry(key.clone()).or_insert(HourlyRollupRecord {
            account_id: key.account_id,
            subscription_id: key.subscription_id,
            product_id: key.product_id,
            meter_id: key.meter_id,
            model_id: key.model_id,
            hour_start_ms: key.hour_start_ms,
            dimensions_key: key.dimensions_key,
            quantity_sum: 0,
            event_count: 0,
            first_event_ms: event.timestamp_ms,
            last_event_ms: event.timestamp_ms,
        });

        entry.quantity_sum += event.quantity;
        entry.event_count += 1;
        if event.timestamp_ms < entry.first_event_ms {
            entry.first_event_ms = event.timestamp_ms;
        }
        if event.timestamp_ms > entry.last_event_ms {
            entry.last_event_ms = event.timestamp_ms;
        }
    }

    pub fn finalize(self) -> Vec<HourlyRollupRecord> {
        self.state.aggregates.into_values().collect()
    }
}
