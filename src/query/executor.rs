use crate::query::plan::{QueryPlan, QuerySource, AggregationFunction};
use crate::runtime::state::AppState;
use crate::model::event::UsageEvent;
use crate::storage::segment_reader::RawSegmentReader;
use serde_json::{Value, Map};
use std::collections::BTreeMap;
use tracing::warn;

/// Execute a query plan against raw segments + the live memtable. Rollup
/// segments are not yet produced by any background worker, so RollupHourly
/// queries fall back to scanning raw events with hourly grouping — same
/// answer, slower. Once the rollup builder is wired up this branch can
/// short-circuit to scanning rollup_segments.
pub async fn execute_plan(state: &AppState, plan: &QueryPlan) -> Vec<Value> {
    let events = collect_events(state, plan).await;
    aggregate(plan, &events)
}

async fn collect_events(state: &AppState, plan: &QueryPlan) -> Vec<UsageEvent> {
    let mut events: Vec<UsageEvent> = Vec::new();

    // Snapshot the manifest under a read lock, then release it before doing
    // I/O on segment files (segment files are immutable, so reading them
    // outside the manifest lock is safe).
    let segment_paths: Vec<std::path::PathBuf> = {
        let manifest = state.manifest.read().await;
        manifest
            .raw_segments
            .iter()
            .filter(|s| {
                // Skip segments that can't overlap the query time range.
                s.min_timestamp_ms <= plan.to_ms && s.max_timestamp_ms >= plan.from_ms
            })
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
                    Ok(Some(e)) => events.push(e),
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

    // Include unflushed events from the memtable so queries see freshly
    // ingested data without waiting for a flush.
    {
        let memtable = state.memtable.lock().await;
        events.extend(memtable.snapshot());
    }

    events
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

    // Group key is the tuple of group_by field values, rendered as strings
    // for use in a BTreeMap (stable ordering for deterministic output).
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
                // quantity is i128 which is JSON-unsafe past 2^53; serialize as string.
                AggregationFunction::Sum => Value::String(agg.quantity_sum.to_string()),
                AggregationFunction::Count => Value::Number(agg.count.into()),
            };
            obj.insert(metric_name.clone(), v);
        }
        // If the caller asked for metrics but no group_by, we still want a
        // single row of totals — handled above since groups has one entry
        // with an empty key.
        results.push(Value::Object(obj));
    }

    // Single-row totals path: when no group_by but metrics requested.
    if plan.group_by.is_empty() && !plan.metrics.is_empty() && results.is_empty() {
        // No events matched, but we still emit a zero row.
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

    let _ = plan.source; // both QuerySource variants currently scan raw events
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
                // Allow filtering on a dimension by name.
                if let Some(v) = e.dimensions.inner.get(other) {
                    Some(v.as_str())
                } else {
                    // Unsupported field: treat as no-match so users notice.
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
    let _ = plan.source;
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
        other => {
            // Group by a dimension by name.
            e.dimensions
                .inner
                .get(other)
                .cloned()
                .unwrap_or_default()
        }
    }
}

/// Variant of QuerySource used for telemetry; currently unused by the
/// executor since both sources resolve to a raw-events scan.
#[allow(dead_code)]
fn _force_source_used(s: &QuerySource) -> &'static str {
    match s {
        QuerySource::RawEvents => "raw",
        QuerySource::RollupHourly => "rollup",
    }
}
