//! Regression tests for the three P0 billing-safety bugs surfaced in
//! external review. Each test is shaped so that it would *fail* on the
//! pre-fix code:
//!
//!   P0 #1 — rollup undercount when memtable holds events past watermark
//!   P0 #2 — dedupe lost after a flush + restart, allowing double-counts
//!   P0 #3 — rollups drop `source` and `unit`, breaking grouping/filtering

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, RwLock};
use usagedb::ingest::dedupe::{DedupeResult, HotDedupe};
use usagedb::ingest::flusher::build_segment_meta;
use usagedb::ingest::memtable::Memtable;
use usagedb::ingest::wal::Wal;
use usagedb::model::dimensions::SmallDimensions;
use usagedb::model::event::{EventKind, UsageEvent};
use usagedb::model::ids::{
    AccountId, EventId, MeterId, ModelId, ProductId, SourceId, SubscriptionId, Unit,
};
use usagedb::rollup::worker::RollupWorker;
use usagedb::runtime::config::Config;
use usagedb::runtime::recovery::{compute_event_hashes, Recovery};
use usagedb::runtime::state::{AppState, AppStateInner};
use usagedb::storage::manifest::Manifest;
use usagedb::storage::segment_writer::RawSegmentWriter;

const HOUR_MS: i64 = 3_600_000;

fn tmp_root() -> PathBuf {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    std::mem::forget(dir);
    path
}

fn make_event_with_source(
    id: &str,
    account: &str,
    ts: i64,
    qty: i128,
    source: &str,
    unit: &str,
) -> UsageEvent {
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
        unit: Unit(unit.to_string()),
        source: SourceId(source.to_string()),
        model_id: Some(ModelId("claude-sonnet-4".into())),
        dimensions: SmallDimensions::default(),
        ingested_at_ms: ts,
    }
}

fn make_event(id: &str, account: &str, ts: i64, qty: i128) -> UsageEvent {
    make_event_with_source(id, account, ts, qty, "agentcore", "token")
}

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

// =========================================================================
// P0 #1 — rollup watermark must not advance past events still in memtable
// =========================================================================

#[tokio::test]
async fn rollup_watermark_bounded_by_oldest_memtable_event() {
    let root = tmp_root();
    let state = build_state(root.clone(), 2);

    // Put an event in the memtable for hour 10. Never flush it.
    let h10 = 10 * HOUR_MS;
    {
        let mut memtable = state.memtable.lock().await;
        memtable.insert(make_event("e_pending", "acc_a", h10 + 1, 99));
    }

    // Tick as if "now" is well past hour 11 — without the fix, watermark
    // would jump to 11 * HOUR_MS, sealing hour 10 with the in-memtable
    // event unaccounted for.
    let worker = RollupWorker::new(state.clone(), 0, Duration::from_secs(30), i64::MAX);
    let stats = worker.tick((11 * HOUR_MS) + 1).await.unwrap();

    assert!(
        stats.watermark_ms <= h10,
        "watermark must not advance past hour holding unflushed memtable event; got {} (expected <= {})",
        stats.watermark_ms, h10
    );
}

#[tokio::test]
async fn rollup_tick_skips_when_flush_is_in_flight() {
    let root = tmp_root();
    let state = build_state(root.clone(), 2);

    // Simulate an in-flight flush: rotate the WAL (active_id moves to 2)
    // without having the flusher commit a segment, so
    // manifest.last_sealed_wal_id stays at 0.
    {
        let mut wal = state.wal.lock().await;
        let _sealed = wal.rotate().unwrap();
        assert_eq!(wal.active_id, 2);
    }

    let h5 = 5 * HOUR_MS;
    let worker = RollupWorker::new(state.clone(), 0, Duration::from_secs(30), i64::MAX);
    let stats = worker.tick(h5 + 1).await.unwrap();

    assert!(stats.skipped_for_in_flight, "tick must skip while a flush is in flight");
    assert_eq!(stats.segments_written, 0);
}

#[tokio::test]
async fn rollup_force_drains_stale_memtable() {
    // Custom setup: keep the flush channel receiver alive for the duration
    // of the test so force_drain_memtable's `send` doesn't see a closed
    // channel. The default `build_state` helper drops the receiver because
    // most tests never trigger a flush.
    let root = tmp_root();
    std::fs::create_dir_all(&root).unwrap();
    let config = Config {
        db_root: root.clone(),
        default_bucket_count: 2,
        rollup_safety_lag_ms: 0,
        ..Config::default()
    };
    let wal = Wal::open(root.join("wal"), 0).unwrap();
    let manifest = Manifest { bucket_count: 2, ..Manifest::default() };
    let (flush_sender, mut flush_receiver) = tokio::sync::mpsc::channel(4);
    let state: AppState = Arc::new(AppStateInner {
        config,
        dedupe: Mutex::new(HotDedupe::new(1000)),
        wal: Mutex::new(wal),
        memtable: Mutex::new(Memtable::new()),
        manifest: RwLock::new(manifest),
        flush_sender,
    });

    {
        let mut memtable = state.memtable.lock().await;
        memtable.insert(make_event("stale", "acc", 100, 1));
    }

    let worker = RollupWorker::new(state.clone(), 0, Duration::from_secs(30), 1);
    tokio::time::sleep(Duration::from_millis(5)).await;
    let stats = worker.tick(usagedb_now_ms()).await.unwrap();

    assert!(stats.forced_flush, "memtable older than max_age must trigger a force-flush");

    let memtable = state.memtable.lock().await;
    assert!(memtable.is_empty(), "force-flush must drain the memtable");
    let wal = state.wal.lock().await;
    assert!(wal.active_id >= 2, "rotate must move past the previous active id");

    // Confirm the flush message actually made it into the channel.
    let msg = flush_receiver
        .try_recv()
        .expect("force-flush should have sent a FlushMessage");
    assert_eq!(msg.events.len(), 1);
    assert_eq!(msg.events[0].event_id.0, "stale");
}

fn usagedb_now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// =========================================================================
// P0 #2 — dedupe must survive a restart after the WAL has been sealed
// =========================================================================

#[tokio::test]
async fn dedupe_rebuilds_from_recent_segments_on_recovery() {
    let root = tmp_root();
    let state = build_state(root.clone(), 2);

    let now = usagedb_now_ms();
    let recent_event = make_event("evt_billed", "acc_z", now - 60_000, 100);
    commit_segment_directly(&state, &[recent_event.clone()], 0).await;

    // Tear down "process" — simulate restart by dropping state and
    // re-running recovery against the same db_root.
    drop(state);

    let recovery = Recovery::new(root.clone());
    let result = recovery.run_startup_recovery(10_000).unwrap();

    // The event should now be in the rebuilt dedupe cache — a retry
    // would be detected as a duplicate, not accepted as a new event.
    let (id_hash, payload_hash) = compute_event_hashes(&recent_event);
    let mut dedupe = result.dedupe;
    assert_eq!(
        dedupe.check_and_insert(id_hash, payload_hash),
        DedupeResult::ExactDuplicate,
        "retry after restart of a previously-committed event must NOT be accepted as new"
    );
}

#[tokio::test]
async fn dedupe_rebuild_skips_segments_older_than_ttl() {
    let root = tmp_root();
    let state = build_state(root.clone(), 2);

    // Old segment beyond the dedupe TTL window (7 days). Recovery should
    // skip scanning it — those events are too old to be retried.
    let very_old_ts = usagedb_now_ms() - 30 * 24 * 3600 * 1000; // 30 days ago
    let old_event = make_event("old", "acc_x", very_old_ts, 1);
    commit_segment_directly(&state, &[old_event.clone()], 0).await;

    drop(state);

    let recovery = Recovery::new(root.clone());
    let result = recovery.run_startup_recovery(10_000).unwrap();

    let (id_hash, payload_hash) = compute_event_hashes(&old_event);
    let mut dedupe = result.dedupe;
    // Beyond TTL, so the rebuild scan skipped it — a retry is now NewEvent.
    // That's correct behavior: TTL says the upstream pipeline shouldn't
    // be retrying events this old.
    assert_eq!(
        dedupe.check_and_insert(id_hash, payload_hash),
        DedupeResult::NewEvent
    );
}

// =========================================================================
// P0 #3 — rollups must preserve `source` and `unit`
// =========================================================================

#[tokio::test]
async fn rollups_preserve_source_and_unit() {
    use std::collections::HashMap;
    use usagedb::query::executor::execute_plan;
    use usagedb::query::plan::{AggregationFunction, QueryFilter, QueryPlan, QuerySource};

    let root = tmp_root();
    let state = build_state(root.clone(), 2);

    // Two events, same account/meter, but different source. After
    // rolling up, a query filtering by source must split them correctly.
    let h = 50 * HOUR_MS;
    let e_a = make_event_with_source("a", "acc_q", h + 1, 100, "agentcore", "token");
    let e_b = make_event_with_source("b", "acc_q", h + 2, 200, "external", "token");
    commit_segment_directly(&state, &[e_a, e_b], 0).await;

    let worker = RollupWorker::new(state.clone(), 0, Duration::from_secs(30), i64::MAX);
    worker.tick((h + HOUR_MS) + 1).await.unwrap();

    // Filter for source="agentcore" — should see only 100, not 300.
    let mut metrics = HashMap::new();
    metrics.insert("quantity".into(), AggregationFunction::Sum);

    let plan = QueryPlan {
        source: QuerySource::RollupHourly,
        account_id: Some("acc_q".into()),
        from_ms: h,
        to_ms: h + HOUR_MS,
        filters: vec![QueryFilter {
            field: "source".into(),
            values: vec!["agentcore".into()],
        }],
        group_by: vec![],
        metrics,
        limit: None,
    };
    let result = execute_plan(&state, &plan).await;
    let sum: i128 = result
        .iter()
        .filter_map(|v| v.get("quantity"))
        .filter_map(|v| v.as_str())
        .filter_map(|s| s.parse().ok())
        .next()
        .unwrap_or(0);
    assert_eq!(
        sum, 100,
        "rollup query filtered by source must return only matching events; got {}",
        sum
    );
}

#[tokio::test]
async fn rollups_group_by_source() {
    use std::collections::HashMap;
    use usagedb::query::executor::execute_plan;
    use usagedb::query::plan::{AggregationFunction, QueryPlan, QuerySource};

    let root = tmp_root();
    let state = build_state(root.clone(), 2);

    let h = 60 * HOUR_MS;
    let events = vec![
        make_event_with_source("a", "acc_g", h + 1, 100, "agentcore", "token"),
        make_event_with_source("b", "acc_g", h + 2, 50, "agentcore", "token"),
        make_event_with_source("c", "acc_g", h + 3, 200, "external", "token"),
    ];
    commit_segment_directly(&state, &events, 0).await;

    let worker = RollupWorker::new(state.clone(), 0, Duration::from_secs(30), i64::MAX);
    worker.tick((h + HOUR_MS) + 1).await.unwrap();

    let mut metrics = HashMap::new();
    metrics.insert("quantity".into(), AggregationFunction::Sum);

    let plan = QueryPlan {
        source: QuerySource::RollupHourly,
        account_id: Some("acc_g".into()),
        from_ms: h,
        to_ms: h + HOUR_MS,
        filters: vec![],
        group_by: vec!["source".into()],
        metrics,
        limit: None,
    };
    let result = execute_plan(&state, &plan).await;

    let by_source: std::collections::HashMap<String, i128> = result
        .iter()
        .map(|row| {
            let src = row.get("source").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let q = row.get("quantity").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0);
            (src, q)
        })
        .collect();

    assert_eq!(by_source.get("agentcore").copied(), Some(150),
        "agentcore should sum to 150; got {:?}", by_source);
    assert_eq!(by_source.get("external").copied(), Some(200),
        "external should sum to 200; got {:?}", by_source);
}
