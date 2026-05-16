use crate::model::event::UsageEvent;
use crate::rollup::hourly::{HourlyRollupKey, HourlyRollupRecord, HourlyRollupState};

pub struct RollupBuilder {
    state: HourlyRollupState,
}

impl RollupBuilder {
    pub fn new() -> Self {
        Self { state: HourlyRollupState::new() }
    }

    pub fn process_event(&mut self, event: &UsageEvent) {
        let hour_ms = (event.timestamp_ms / 3_600_000) * 3_600_000;

        // SmallDimensions uses BTreeMap, so serde_json output is canonical
        // by key order — same dimensions always produce the same string.
        let dim_canonical = serde_json::to_string(&event.dimensions).unwrap_or_default();

        let key = HourlyRollupKey {
            account_id: event.account_id.clone(),
            subscription_id: event.subscription_id.clone(),
            product_id: event.product_id.clone(),
            meter_id: event.meter_id.clone(),
            model_id: event.model_id.clone(),
            source: event.source.clone(),
            unit: event.unit.clone(),
            hour_start_ms: hour_ms,
            dimensions_canonical: dim_canonical.clone(),
        };

        let entry = self.state.aggregates.entry(key.clone()).or_insert(HourlyRollupRecord {
            account_id: key.account_id,
            subscription_id: key.subscription_id,
            product_id: key.product_id,
            meter_id: key.meter_id,
            model_id: key.model_id,
            source: key.source,
            unit: key.unit,
            hour_start_ms: key.hour_start_ms,
            dimensions_canonical: dim_canonical,
            quantity_sum: 0,
            event_count: 0,
            first_event_ms: event.timestamp_ms,
            last_event_ms: event.timestamp_ms,
        });

        entry.quantity_sum = entry.quantity_sum.saturating_add(event.quantity);
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

impl Default for RollupBuilder {
    fn default() -> Self {
        Self::new()
    }
}
