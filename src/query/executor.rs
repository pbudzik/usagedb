use crate::query::plan::{QueryPlan, QuerySource, AggregationFunction};
use crate::runtime::state::AppState;
use crate::model::event::{EventKind, UsageEvent};
use crate::model::dimensions::SmallDimensions;
use crate::model::ids::{
    AccountId, EventId, MeterId, ModelId, ProductId, SubscriptionId,
};
use crate::rollup::hourly::HourlyRollupRecord;
use crate::rollup::reader::RollupSegmentReader;
use crate::storage::segment_reader::RawSegmentReader;
use serde_json::{Value, Map};
use std::collections::BTreeMap;
use tracing::warn;

/// Execute a query plan. RawEvents queries scan raw segments + the live
/// memtable. RollupHourly queries scan rollup segments for the part of
/// the range at or below the rollup watermark, then fall back to raw
/// segments + memtable for the unsealed tail above the watermark. Both
/// paths feed the same aggregator, so SUM(quantity) is identical
/// regardless of source.
///
/// NOTE: COUNT semantics differ slightly for RollupHourly: each rollup row
/// counts as 1, not as the number of underlying events it aggregates.
/// Callers needing exact event counts should use the RawEvents source.
pub async fn execute_plan(state: &AppState, plan: &QueryPlan) -> Vec<Value> {
    let events = collect_events(state, plan).await;
    aggregate(plan, &events)
}

async fn collect_events(state: &AppState, plan: &QueryPlan) -> Vec<UsageEvent> {
    match plan.source {
        QuerySource::RawEvents => collect_raw_events(state, plan, plan.from_ms, plan.to_ms).await,
        QuerySource::RollupHourly => collect_rollup_then_raw_tail(state, plan).await,
    }
}

async fn collect_raw_events(
    state: &AppState,
    _plan: &QueryPlan,
    from_ms: i64,
    to_ms: i64,
) -> Vec<UsageEvent> {
    let mut events: Vec<UsageEvent> = Vec::new();

    // Snapshot the manifest under a read lock, then release before file I/O.
    // Segment files are immutable so reads outside the lock are safe.
    let segment_paths: Vec<std::path::PathBuf> = {
        let manifest = state.manifest.read().await;
        manifest
            .raw_segments
            .iter()
            .filter(|s| s.min_timestamp_ms <= to_ms && s.max_timestamp_ms >= from_ms)
            .map(|s| state.config.db_root.join(format!("{}.seg", s.segment_id)))
            .collect()
    };

    for path in segment_paths {
        if !path.exists() {
            warn!("manifest references missing segment file: {:?}", path);
            continue;
        }
        match RawSegmentReader::new(path.clone()) {
            Ok(mut reader) => loop {
                match reader.read_next() {
                    Ok(Some(e)) => {
                        // Per-event time filter: a segment can overlap the
                        // range without all its events being in range, and
                        // the rollup-fallback path passes a `from_ms`
                        // narrower than `plan.from_ms` to avoid double-
                        // counting events already in rollup segments.
                        if e.timestamp_ms >= from_ms && e.timestamp_ms <= to_ms {
                            events.push(e);
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        warn!("Error reading segment {:?}: {}", path, e);
                        break;
                    }
                }
            },
            Err(e) => warn!("Failed to open segment {:?}: {}", path, e),
        }
    }

    // Include unflushed events from the memtable within the same range.
    {
        let memtable = state.memtable.lock().await;
        for e in memtable.snapshot() {
            if e.timestamp_ms >= from_ms && e.timestamp_ms <= to_ms {
                events.push(e);
            }
        }
    }

    events
}

async fn collect_rollup_then_raw_tail(state: &AppState, plan: &QueryPlan) -> Vec<UsageEvent> {
    let (watermark, rollup_paths): (i64, Vec<(std::path::PathBuf, u64)>) = {
        let manifest = state.manifest.read().await;
        let wm = manifest.watermarks.hourly_rollup_ms;
        let rollup_upper = plan.to_ms.min(wm.saturating_sub(1)); // watermark is exclusive
        let paths = manifest
            .rollup_segments
            .iter()
            .filter(|s| s.min_timestamp_ms <= rollup_upper && s.max_timestamp_ms >= plan.from_ms)
            .map(|s| (
                state.config.db_root.join(format!("{}.rseg", s.segment_id)),
                s.checksum,
            ))
            .collect();
        (wm, paths)
    };

    let mut events: Vec<UsageEvent> = Vec::new();
    let rollup_upper = plan.to_ms.min(watermark.saturating_sub(1));

    for (path, checksum) in rollup_paths {
        if !path.exists() {
            warn!("manifest references missing rollup segment file: {:?}", path);
            continue;
        }
        match RollupSegmentReader::open(path.clone(), checksum) {
            Ok(reader) => {
                for record in reader.into_records() {
                    if record.hour_start_ms < plan.from_ms || record.hour_start_ms > rollup_upper {
                        continue;
                    }
                    events.push(rollup_record_to_event(&record));
                }
            }
            Err(e) => warn!("Failed to open rollup segment {:?}: {}", path, e),
        }
    }

    // Fall back to raw scan for the tail above the watermark, if the
    // caller asked for it.
    if plan.to_ms >= watermark {
        let tail_from = plan.from_ms.max(watermark);
        events.extend(collect_raw_events(state, plan, tail_from, plan.to_ms).await);
    }

    events
}

/// Convert a rollup row into a synthetic event so the aggregator can treat
/// rollup-derived and raw-derived rows uniformly. quantity is set to the
/// pre-aggregated sum; timestamp is the hour start so hour/day grouping
/// works. Dimensions are deserialized from the canonical JSON.
fn rollup_record_to_event(r: &HourlyRollupRecord) -> UsageEvent {
    let dimensions: SmallDimensions = serde_json::from_str(&r.dimensions_canonical)
        .unwrap_or_default();
    UsageEvent {
        event_id: EventId(format!("rollup_{}", r.hour_start_ms)), // synthetic; never used as identity
        kind: EventKind::Usage,
        correction_ref: None,
        account_id: r.account_id.clone(),
        subscription_id: r.subscription_id.clone(),
        product_id: r.product_id.clone(),
        meter_id: r.meter_id.clone(),
        timestamp_ms: r.hour_start_ms,
        quantity: r.quantity_sum,
        unit: r.unit.clone(),
        source: r.source.clone(),
        model_id: r.model_id.clone(),
        dimensions,
        ingested_at_ms: r.last_event_ms,
    }
}

fn aggregate(plan: &QueryPlan, events: &[UsageEvent]) -> Vec<Value> {
    let filtered: Vec<&UsageEvent> = events
        .iter()
        .filter(|e| matches_plan(plan, e))
        .collect();

    // No group_by + no metrics: return the event rows directly.
    if plan.group_by.is_empty() && plan.metrics.is_empty() {
        return filtered
            .iter()
            .map(|e| serde_json::to_value(e).unwrap_or(Value::Null))
            .collect();
    }

    #[derive(Default)]
    struct Agg {
        quantity_sum: i128,
        count: u64,
    }

    let mut groups: BTreeMap<Vec<String>, Agg> = BTreeMap::new();
    for e in &filtered {
        let key: Vec<String> = plan
            .group_by
            .iter()
            .map(|g| group_key_value(g, e))
            .collect();
        let entry = groups.entry(key).or_default();
        entry.quantity_sum = entry.quantity_sum.saturating_add(e.quantity);
        entry.count += 1;
    }

    let mut results = Vec::with_capacity(groups.len());
    for (key_values, agg) in groups {
        let mut obj = Map::new();
        for (i, g) in plan.group_by.iter().enumerate() {
            obj.insert(g.clone(), Value::String(key_values[i].clone()));
        }
        for (metric_name, func) in &plan.metrics {
            let v = match func {
                AggregationFunction::Sum => Value::String(agg.quantity_sum.to_string()),
                AggregationFunction::Count => Value::Number(agg.count.into()),
            };
            obj.insert(metric_name.clone(), v);
        }
        results.push(Value::Object(obj));
    }

    if plan.group_by.is_empty() && !plan.metrics.is_empty() && results.is_empty() {
        let mut obj = Map::new();
        for (metric_name, func) in &plan.metrics {
            let v = match func {
                AggregationFunction::Sum => Value::String("0".to_string()),
                AggregationFunction::Count => Value::Number(0.into()),
            };
            obj.insert(metric_name.clone(), v);
        }
        results.push(Value::Object(obj));
    }

    results
}

fn matches_plan(plan: &QueryPlan, e: &UsageEvent) -> bool {
    if e.timestamp_ms < plan.from_ms || e.timestamp_ms > plan.to_ms {
        return false;
    }
    if let Some(account) = &plan.account_id {
        if &e.account_id.0 != account {
            return false;
        }
    }
    for f in &plan.filters {
        let actual = match f.field.as_str() {
            "account_id" => Some(e.account_id.0.as_str()),
            "subscription_id" => e.subscription_id.as_ref().map(|s| s.0.as_str()),
            "product_id" => Some(e.product_id.0.as_str()),
            "meter_id" => Some(e.meter_id.0.as_str()),
            "model_id" => e.model_id.as_ref().map(|m| m.0.as_str()),
            "source" => Some(e.source.0.as_str()),
            "unit" => Some(e.unit.0.as_str()),
            other => {
                if let Some(v) = e.dimensions.inner.get(other) {
                    Some(v.as_str())
                } else {
                    return false;
                }
            }
        };
        let actual = match actual {
            Some(v) => v,
            None => return false,
        };
        if !f.values.iter().any(|v| v == actual) {
            return false;
        }
    }
    true
}

fn group_key_value(field: &str, e: &UsageEvent) -> String {
    match field {
        "account_id" => e.account_id.0.clone(),
        "subscription_id" => e
            .subscription_id
            .as_ref()
            .map(|s| s.0.clone())
            .unwrap_or_default(),
        "product_id" => e.product_id.0.clone(),
        "meter_id" => e.meter_id.0.clone(),
        "model_id" => e
            .model_id
            .as_ref()
            .map(|m| m.0.clone())
            .unwrap_or_default(),
        "source" => e.source.0.clone(),
        "unit" => e.unit.0.clone(),
        "hour_start_ms" => ((e.timestamp_ms / 3_600_000) * 3_600_000).to_string(),
        "day" => ((e.timestamp_ms / 86_400_000) * 86_400_000).to_string(),
        other => e
            .dimensions
            .inner
            .get(other)
            .cloned()
            .unwrap_or_default(),
    }
}

// Silence unused-import warnings for types referenced only via trait methods.
#[allow(dead_code)]
fn _force_account_id_used(_: &AccountId) {}
#[allow(dead_code)]
fn _force_subscription_used(_: &SubscriptionId) {}
#[allow(dead_code)]
fn _force_product_used(_: &ProductId) {}
#[allow(dead_code)]
fn _force_meter_used(_: &MeterId) {}
#[allow(dead_code)]
fn _force_model_used(_: &ModelId) {}
