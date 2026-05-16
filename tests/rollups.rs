//! Integration tests for the hourly rollup scheduler (review #9).
//!
//! Drives the worker deterministically via `tick(now_ms_override)` so
//! tests don't depend on wall-clock time. Covers:
//!   - Sealing an hour produces one rollup segment per bucket touched
//!   - Watermark advances atomically with rollup segment commit
//!   - Idempotency: a second tick over the same window is a no-op
//!   - Open period: hours past the watermark stay queryable via raw fallback
//!   - Sum across rollups equals sum across raw events

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, RwLock};
use usagedb::ingest::dedupe::HotDedupe;
use usagedb::ingest::flusher::build_segment_meta;
use usagedb::ingest::memtable::Memtable;
use usagedb::ingest::wal::Wal;
use usagedb::model::dimensions::SmallDimensions;
use usagedb::model::event::{EventKind, UsageEvent};
use usagedb::model::ids::{
    AccountId, EventId, MeterId, ModelId, ProductId, SourceId, SubscriptionId, Unit,
    bucket_for_account,
};
use usagedb::rollup::worker::RollupWorker;
use usagedb::runtime::config::Config;
use usagedb::runtime::state::{AppState, AppStateInner};
use usagedb::storage::manifest::Manifest;
use usagedb::storage::segment_writer::RawSegmentWriter;

const HOUR_MS: i64 = 3_600_000;

fn make_event(id: &str, account: &str, ts: i64, qty: i128) -> UsageEvent {
    UsageEvent {
        event_id: EventId(id.to_string()),
        kind: EventKind::Usage,
        correction_ref: None,
        account_id: AccountId(account.to_string()),
        subscription_id: Some(SubscriptionId("sub_1".into())),
        product_id: ProductId("ai_gateway".into()),
        meter_id: MeterId("tokens.input".into()),
        timestamp_ms: ts,
        quantity: qty,
        unit: Unit("token".into()),
        source: SourceId("test".into()),
        model_id: Some(ModelId("claude-sonnet-4".into())),
        dimensions: SmallDimensions::default(),
        ingested_at_ms: ts,
    }
}

fn tmp_root() -> PathBuf {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    std::mem::forget(dir);
    path
}

/// Build an AppState that points at the given DB root, with a manifest
/// matching the provided bucket_count. The flush channel is created but
/// not consumed — these tests write segments directly via the writer
/// rather than driving the flusher.
fn build_state(db_root: PathBuf, bucket_count: u32) -> AppState {
    let config = Config {
        db_root: db_root.clone(),
        default_bucket_count: bucket_count,
        rollup_safety_lag_ms: 0,
        ..Config::default()
    };
    std::fs::create_dir_all(&config.db_root).unwrap();
    let wal = Wal::open(db_root.join("wal"), 0).unwrap();
    let manifest = Manifest {
        bucket_count,
        ..Manifest::default()
    };
    let (flush_sender, _flush_receiver) = tokio::sync::mpsc::channel(4);
    Arc::new(AppStateInner {
        config,
        dedupe: Mutex::new(HotDedupe::new(1000)),
        wal: Mutex::new(wal),
        memtable: Mutex::new(Memtable::new()),
        manifest: RwLock::new(manifest),
        flush_sender,
    })
}

/// Write a raw segment for a single bucket containing the given events,
/// then register its SegmentMeta in the manifest. Mirrors what the
/// flusher does, minus the WAL rotation.
async fn commit_segment_directly(state: &AppState, events: &[UsageEvent], bucket: u32) {
    let segment_id = format!("raw_{}", uuid::Uuid::new_v4().simple());
    let path = state.config.db_root.join(format!("{}.seg", segment_id));
    let mut writer = RawSegmentWriter::new(path).unwrap();
    for e in events {
        writer.write_event(e).unwrap();
    }
    let (_rows, checksum) = writer.finish().unwrap();
    let meta = build_segment_meta(&segment_id, events, bucket, checksum);
    let mut manifest = state.manifest.write().await;
    manifest.raw_segments.push(meta);
    manifest.save(&state.config.db_root).unwrap();
}

#[tokio::test]
async fn tick_seals_completed_hours_and_advances_watermark() {
    let root = tmp_root();
    let state = build_state(root.clone(), 4);

    // Events all in hour 100 (ts in [100h, 101h)).
    let hour100 = 100 * HOUR_MS;
    let events: Vec<UsageEvent> = (0..10)
        .map(|i| make_event(&format!("evt_{i}"), &format!("acc_{}", i % 3), hour100 + i, 100 + i as i128))
        .collect();

    // Partition by bucket and write a segment per bucket.
    let bucket_count = 4u32;
    let mut by_bucket: std::collections::HashMap<u32, Vec<UsageEvent>> = std::collections::HashMap::new();
    for e in &events {
        by_bucket.entry(bucket_for_account(&e.account_id, bucket_count)).or_default().push(e.clone());
    }
    for (bucket, evs) in &by_bucket {
        commit_segment_directly(&state, evs, *bucket).await;
    }

    // Drive the worker as if "now" is well past hour 101.
    let worker = RollupWorker::new(state.clone(), 0, Duration::from_secs(30));
    let stats = worker.tick((101 * HOUR_MS) + 1).await.unwrap();

    assert!(stats.segments_written > 0, "tick must write at least one rollup segment");
    assert_eq!(stats.watermark_ms, 101 * HOUR_MS, "watermark must advance to target_hour");

    let manifest = state.manifest.read().await;
    assert_eq!(manifest.watermarks.hourly_rollup_ms, 101 * HOUR_MS);
    assert_eq!(manifest.rollup_segments.len(), by_bucket.len(),
        "one rollup segment per bucket that had events");

    let total_rollup_qty: i128 = manifest.rollup_segments.iter()
        .map(|s| s.quantity_sum.unwrap_or(0))
        .sum();
    let expected: i128 = events.iter().map(|e| e.quantity).sum();
    assert_eq!(total_rollup_qty, expected, "rollup quantity sum must equal raw event sum");
}

#[tokio::test]
async fn second_tick_is_noop_for_the_same_window() {
    let root = tmp_root();
    let state = build_state(root.clone(), 2);

    let hour = 50 * HOUR_MS;
    let events: Vec<UsageEvent> = (0..5)
        .map(|i| make_event(&format!("e{i}"), "acc_a", hour + i, 10))
        .collect();
    let bucket = bucket_for_account(&events[0].account_id, 2);
    commit_segment_directly(&state, &events, bucket).await;

    let worker = RollupWorker::new(state.clone(), 0, Duration::from_secs(30));
    let first = worker.tick((51 * HOUR_MS) + 1).await.unwrap();
    let segments_after_first = first.segments_written;
    assert!(segments_after_first > 0);

    let second = worker.tick((51 * HOUR_MS) + 1).await.unwrap();
    assert_eq!(second.segments_written, 0, "second tick must not duplicate work");
    assert_eq!(second.hours_processed, 0);

    let manifest = state.manifest.read().await;
    assert_eq!(manifest.rollup_segments.len(), segments_after_first,
        "rollup segments must not multiply on re-tick");
}

#[tokio::test]
async fn query_through_rollup_returns_same_sum_as_raw() {
    use usagedb::query::executor::execute_plan;
    use usagedb::query::plan::{AggregationFunction, QueryFilter, QueryPlan, QuerySource};
    use std::collections::HashMap;

    let root = tmp_root();
    let state = build_state(root.clone(), 2);

    let hour = 200 * HOUR_MS;
    let events: Vec<UsageEvent> = (0..20)
        .map(|i| make_event(&format!("e{i}"), "acc_q", hour + i * 1000, 7))
        .collect();
    let bucket = bucket_for_account(&events[0].account_id, 2);
    commit_segment_directly(&state, &events, bucket).await;

    let worker = RollupWorker::new(state.clone(), 0, Duration::from_secs(30));
    worker.tick((201 * HOUR_MS) + 1).await.unwrap();

    let mut metrics = HashMap::new();
    metrics.insert("quantity".to_string(), AggregationFunction::Sum);

    let plan_raw = QueryPlan {
        source: QuerySource::RawEvents,
        account_id: Some("acc_q".into()),
        from_ms: hour,
        to_ms: hour + HOUR_MS,
        filters: vec![],
        group_by: vec![],
        metrics: metrics.clone(),
        limit: None,
    };
    let raw_result = execute_plan(&state, &plan_raw).await;

    let plan_rollup = QueryPlan {
        source: QuerySource::RollupHourly,
        ..plan_raw.clone()
    };
    let rollup_result = execute_plan(&state, &plan_rollup).await;

    let raw_sum = extract_sum(&raw_result);
    let rollup_sum = extract_sum(&rollup_result);
    assert_eq!(raw_sum, rollup_sum, "SUM(quantity) must agree between RawEvents and RollupHourly");
    assert_eq!(raw_sum, 7 * 20, "expected total");

    // Silence unused-variable lints if QueryFilter is fully optimized out.
    let _ = QueryFilter { field: "x".into(), values: vec![] };
}

fn extract_sum(result: &[serde_json::Value]) -> i128 {
    result.iter()
        .filter_map(|v| v.get("quantity"))
        .filter_map(|v| v.as_str())
        .filter_map(|s| s.parse().ok())
        .next()
        .unwrap_or(0)
}

#[tokio::test]
async fn open_hour_remains_visible_via_raw_fallback() {
    use usagedb::query::executor::execute_plan;
    use usagedb::query::plan::{AggregationFunction, QueryPlan, QuerySource};
    use std::collections::HashMap;

    let root = tmp_root();
    let state = build_state(root.clone(), 2);

    // Two hours of data. Watermark will only seal the first hour because
    // the safety_lag (here 0) places `now` just past hour 11.
    let h10 = 10 * HOUR_MS;
    let h11 = 11 * HOUR_MS;
    let mut events: Vec<UsageEvent> = Vec::new();
    for i in 0..5 {
        events.push(make_event(&format!("a{i}"), "acc_z", h10 + i, 4));
    }
    for i in 0..5 {
        events.push(make_event(&format!("b{i}"), "acc_z", h11 + i, 8));
    }
    let bucket = bucket_for_account(&events[0].account_id, 2);
    commit_segment_directly(&state, &events, bucket).await;

    // Tick with now just past h11 → only h10 is sealed; h11 stays open.
    let worker = RollupWorker::new(state.clone(), 0, Duration::from_secs(30));
    let stats = worker.tick(h11 + 1).await.unwrap();
    assert_eq!(stats.watermark_ms, h11, "watermark should land on h11 (h10 sealed, h11 open)");

    // RollupHourly query over [h10, h11+1h) should see h10 from rollups
    // and h11 from raw fallback. Total = 5*4 + 5*8 = 60.
    let mut metrics = HashMap::new();
    metrics.insert("quantity".to_string(), AggregationFunction::Sum);
    let plan = QueryPlan {
        source: QuerySource::RollupHourly,
        account_id: Some("acc_z".into()),
        from_ms: h10,
        to_ms: h11 + HOUR_MS,
        filters: vec![],
        group_by: vec![],
        metrics,
        limit: None,
    };
    let result = execute_plan(&state, &plan).await;
    assert_eq!(extract_sum(&result), 60,
        "rollup query must merge sealed rollups with raw open-period events");
}
