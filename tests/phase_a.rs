//! Regression tests for the Phase A correctness-contract additions.
//!
//!   - Manifest generations: corrupt CURRENT-pointed generation rolls
//!     back to the previous one; legacy `manifest.json` migrates to
//!     generation 1; truly fresh DBs start without a manifest.
//!   - Rebuildable rollups: drops affected segments + rewinds watermark
//!     so a follow-up tick refills the gap.
//!   - Correction workflow: SUM(quantity) of original + correction nets
//!     out correctly; queries can filter and group by `kind`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, RwLock};
use usagedb::ingest::dedupe::HotDedupe;
use usagedb::ingest::flusher::build_segment_meta;
use usagedb::ingest::memtable::Memtable;
use usagedb::ingest::wal::Wal;
use usagedb::model::dimensions::SmallDimensions;
use usagedb::model::event::{CorrectionRef, EventKind, UsageEvent};
use usagedb::model::ids::{
    AccountId, EventId, MeterId, ModelId, ProductId, SourceId, SubscriptionId, Unit,
};
use usagedb::rollup::worker::RollupWorker;
use usagedb::runtime::config::Config;
use usagedb::runtime::recovery::Recovery;
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

fn make_event(id: &str, account: &str, ts: i64, qty: i128) -> UsageEvent {
    UsageEvent {
        event_id: EventId(id.to_string()),
        kind: EventKind::Usage,
        correction_ref: None,
        account_id: AccountId(account.to_string()),
        subscription_id: Some(SubscriptionId("sub".into())),
        product_id: ProductId("ai_gateway".into()),
        meter_id: MeterId("tokens.input".into()),
        timestamp_ms: ts,
        quantity: qty,
        unit: Unit("token".into()),
        source: SourceId("test".into()),
        model_id: Some(ModelId("m1".into())),
        dimensions: SmallDimensions::default(),
        ingested_at_ms: ts,
    }
}

fn build_state(db_root: PathBuf, bucket_count: u32) -> AppState {
    let config = Config {
        db_root: db_root.clone(),
        default_bucket_count: bucket_count,
        rollup_safety_lag_ms: 0,
        memtable_max_age_ms: i64::MAX,
        ..Config::default()
    };
    std::fs::create_dir_all(&config.db_root).unwrap();
    let wal = Wal::open(db_root.join("wal"), 0).unwrap();
    let manifest = Manifest { bucket_count, ..Manifest::default() };
    let (flush_sender, _r) = tokio::sync::mpsc::channel(4);
    Arc::new(AppStateInner {
        config,
        dedupe: Mutex::new(HotDedupe::new(1000)),
        wal: Mutex::new(wal),
        memtable: Mutex::new(Memtable::new()),
        manifest: RwLock::new(manifest),
        flush_sender,
    })
}

async fn commit_segment(state: &AppState, events: &[UsageEvent], bucket: u32) {
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
// Manifest generations
// =========================================================================

#[test]
fn save_creates_generation_files() {
    let root = tmp_root();
    std::fs::create_dir_all(&root).unwrap();
    let mut m = Manifest::default();
    m.save(&root).unwrap();
    assert!(root.join("manifest").join("CURRENT").exists());
    assert!(root.join("manifest").join("manifest-000001.json").exists());
    assert_eq!(m.current_generation, 1);

    m.save(&root).unwrap();
    assert!(root.join("manifest").join("manifest-000002.json").exists());
    assert_eq!(m.current_generation, 2);

    let contents = std::fs::read_to_string(root.join("manifest").join("CURRENT")).unwrap();
    assert_eq!(contents.trim(), "2");
}

#[test]
fn load_rolls_back_through_corrupt_generations() {
    let root = tmp_root();
    std::fs::create_dir_all(&root).unwrap();
    let mut m = Manifest::default();
    m.save(&root).unwrap(); // gen 1
    m.save(&root).unwrap(); // gen 2
    m.save(&root).unwrap(); // gen 3

    // Corrupt the current generation. Loader should walk back to gen 2.
    let gen3 = root.join("manifest").join("manifest-000003.json");
    std::fs::write(&gen3, "{ not valid json").unwrap();

    let loaded = Manifest::load(&root).unwrap().expect("must find a generation");
    assert_eq!(
        loaded.current_generation, 2,
        "rolled back to most recent valid generation"
    );
}

#[test]
fn load_fails_closed_when_no_generation_is_valid() {
    let root = tmp_root();
    std::fs::create_dir_all(&root).unwrap();
    let manifest_dir = root.join("manifest");
    std::fs::create_dir_all(&manifest_dir).unwrap();
    std::fs::write(manifest_dir.join("CURRENT"), "1\n").unwrap();
    std::fs::write(manifest_dir.join("manifest-000001.json"), "garbage").unwrap();

    let err = Manifest::load(&root).unwrap_err();
    assert!(
        err.to_string().contains("no valid manifest generation"),
        "must fail closed with a specific error: {}",
        err
    );
}

#[test]
fn load_migrates_legacy_manifest() {
    let root = tmp_root();
    std::fs::create_dir_all(&root).unwrap();
    let legacy = Manifest { bucket_count: 64, ..Manifest::default() };
    let json = serde_json::to_string_pretty(&legacy).unwrap();
    std::fs::write(root.join("manifest.json"), json).unwrap();

    let loaded = Manifest::load(&root).unwrap().expect("legacy should migrate");
    assert_eq!(loaded.bucket_count, 64);
    assert_eq!(loaded.current_generation, 1);

    // Migration moved the data into the new layout and removed the old file.
    assert!(root.join("manifest").join("CURRENT").exists());
    assert!(!root.join("manifest.json").exists());
}

#[test]
fn load_returns_none_for_fresh_db() {
    let root = tmp_root();
    std::fs::create_dir_all(&root).unwrap();
    let loaded = Manifest::load(&root).unwrap();
    assert!(loaded.is_none(), "fresh DB has no manifest");
}

#[test]
fn old_generations_get_pruned() {
    let root = tmp_root();
    std::fs::create_dir_all(&root).unwrap();
    let mut m = Manifest::default();
    // 12 saves; pruning keeps the last 10.
    for _ in 0..12 {
        m.save(&root).unwrap();
    }
    let dir = root.join("manifest");
    let kept: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok().and_then(|e| e.file_name().into_string().ok()))
        .filter(|n| n.starts_with("manifest-"))
        .collect();
    assert!(
        kept.len() <= 10,
        "expected at most 10 generation files, got {}: {:?}",
        kept.len(),
        kept
    );
    // The oldest one should be gen 3 or later (since 1, 2 got pruned).
    assert!(!dir.join("manifest-000001.json").exists());
    assert!(!dir.join("manifest-000002.json").exists());
    assert!(dir.join("manifest-000012.json").exists());
}

#[tokio::test]
async fn recovery_picks_up_generations() {
    let root = tmp_root();
    let state = build_state(root.clone(), 2);

    // Write some real data through the manifest API, then restart.
    commit_segment(&state, &[make_event("a", "acc", 100, 5)], 0).await;
    commit_segment(&state, &[make_event("b", "acc", 200, 7)], 0).await;
    drop(state);

    let recovery = Recovery::new(root.clone());
    let result = recovery.run_startup_recovery(1000).unwrap();
    assert_eq!(result.manifest.raw_segments.len(), 2);
    assert!(result.manifest.current_generation >= 2);
}

// =========================================================================
// Rebuildable rollups
// =========================================================================

#[tokio::test]
async fn rebuild_rollups_drops_segments_and_rewinds_watermark() {
    let root = tmp_root();
    let state = build_state(root.clone(), 2);

    // Build rollups for two hours.
    let h10 = 10 * HOUR_MS;
    let h11 = 11 * HOUR_MS;
    let events: Vec<UsageEvent> = (0..3)
        .flat_map(|i| {
            vec![
                make_event(&format!("a{i}"), "acc_r", h10 + i, 10),
                make_event(&format!("b{i}"), "acc_r", h11 + i, 20),
            ]
        })
        .collect();
    use usagedb::model::ids::bucket_for_account;
    let bucket = bucket_for_account(&AccountId("acc_r".into()), 2);
    commit_segment(&state, &events, bucket).await;

    let worker = RollupWorker::new(state.clone(), 0, Duration::from_secs(30), i64::MAX);
    worker.tick(h11 + HOUR_MS + 1).await.unwrap();
    let watermark_before = state.manifest.read().await.watermarks.hourly_rollup_ms;
    let rollups_before = state.manifest.read().await.rollup_segments.len();
    assert!(rollups_before >= 2, "should have rolled up both hours");
    assert_eq!(watermark_before, h11 + HOUR_MS);

    // Rebuild just hour 10's rollups.
    let dropped = worker.rebuild_rollups(h10, h11).await.unwrap();
    assert!(dropped >= 1, "should have dropped h10 rollup segment(s)");
    let watermark_after = state.manifest.read().await.watermarks.hourly_rollup_ms;
    assert_eq!(
        watermark_after, h10,
        "watermark should rewind to from_ms ({})",
        h10
    );

    // A follow-up tick should refill hour 10.
    worker.tick(h11 + HOUR_MS + 1).await.unwrap();
    let manifest = state.manifest.read().await;
    assert_eq!(manifest.watermarks.hourly_rollup_ms, h11 + HOUR_MS);
    assert!(
        manifest.rollup_segments.len() >= rollups_before,
        "rebuild + tick should restore at least the original number of rollup segments"
    );
}

// =========================================================================
// Correction event workflow
// =========================================================================

#[tokio::test]
async fn correction_event_subtracts_from_sum() {
    use std::collections::HashMap;
    use usagedb::query::executor::execute_plan;
    use usagedb::query::plan::{AggregationFunction, QueryPlan, QuerySource};

    let root = tmp_root();
    let state = build_state(root.clone(), 2);

    // Original 100, then a correction of -40. Net should be 60.
    use usagedb::model::ids::bucket_for_account;
    let bucket = bucket_for_account(&AccountId("acc_c".into()), 2);
    let original = make_event("orig", "acc_c", 100, 100);
    let mut correction = make_event("corr", "acc_c", 200, -40);
    correction.kind = EventKind::Correction;
    correction.correction_ref = Some(CorrectionRef {
        original_event_id: EventId("orig".into()),
        reason: "overcount".into(),
    });
    commit_segment(&state, &[original, correction], bucket).await;

    let mut metrics = HashMap::new();
    metrics.insert("quantity".into(), AggregationFunction::Sum);
    let plan = QueryPlan {
        source: QuerySource::RawEvents,
        account_id: Some("acc_c".into()),
        from_ms: 0,
        to_ms: 1000,
        filters: vec![],
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
    assert_eq!(sum, 60, "100 + (-40) should net to 60");
}

#[tokio::test]
async fn query_filter_by_kind_isolates_corrections() {
    use std::collections::HashMap;
    use usagedb::query::executor::execute_plan;
    use usagedb::query::plan::{AggregationFunction, QueryFilter, QueryPlan, QuerySource};

    let root = tmp_root();
    let state = build_state(root.clone(), 2);

    use usagedb::model::ids::bucket_for_account;
    let bucket = bucket_for_account(&AccountId("acc_k".into()), 2);
    let original = make_event("orig", "acc_k", 100, 100);
    let mut correction = make_event("corr", "acc_k", 200, -40);
    correction.kind = EventKind::Correction;
    correction.correction_ref = Some(CorrectionRef {
        original_event_id: EventId("orig".into()),
        reason: "overcount".into(),
    });
    commit_segment(&state, &[original, correction], bucket).await;

    let mut metrics = HashMap::new();
    metrics.insert("quantity".into(), AggregationFunction::Sum);

    // Filter by kind=Correction: should see only -40.
    let plan = QueryPlan {
        source: QuerySource::RawEvents,
        account_id: Some("acc_k".into()),
        from_ms: 0,
        to_ms: 1000,
        filters: vec![QueryFilter { field: "kind".into(), values: vec!["Correction".into()] }],
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
    assert_eq!(sum, -40, "filtering by kind=Correction should isolate the adjustment");
}

#[tokio::test]
async fn group_by_kind_splits_originals_from_corrections() {
    use std::collections::HashMap;
    use usagedb::query::executor::execute_plan;
    use usagedb::query::plan::{AggregationFunction, QueryPlan, QuerySource};

    let root = tmp_root();
    let state = build_state(root.clone(), 2);

    use usagedb::model::ids::bucket_for_account;
    let bucket = bucket_for_account(&AccountId("acc_g".into()), 2);
    let original = make_event("orig", "acc_g", 100, 100);
    let mut correction = make_event("corr", "acc_g", 200, -40);
    correction.kind = EventKind::Correction;
    correction.correction_ref = Some(CorrectionRef {
        original_event_id: EventId("orig".into()),
        reason: "overcount".into(),
    });
    commit_segment(&state, &[original, correction], bucket).await;

    let mut metrics = HashMap::new();
    metrics.insert("quantity".into(), AggregationFunction::Sum);
    let plan = QueryPlan {
        source: QuerySource::RawEvents,
        account_id: Some("acc_g".into()),
        from_ms: 0,
        to_ms: 1000,
        filters: vec![],
        group_by: vec!["kind".into()],
        metrics,
        limit: None,
    };
    let result = execute_plan(&state, &plan).await;

    let by_kind: std::collections::HashMap<String, i128> = result
        .iter()
        .map(|row| {
            let kind = row.get("kind").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let q = row.get("quantity").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0);
            (kind, q)
        })
        .collect();

    assert_eq!(by_kind.get("Usage").copied(), Some(100));
    assert_eq!(by_kind.get("Correction").copied(), Some(-40));
}
