//! Tests for the admin CLI commands. Drives the `cmd_*` functions
//! directly (the CLI binary just parses args and delegates), so we
//! don't have to spawn subprocesses.

use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::{Mutex, RwLock};

use usagedb::admin::{
    cmd_check, cmd_export_parquet, cmd_inspect_segment, cmd_rebuild_rollups, cmd_verify_period,
    open_state_for_admin,
};
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

fn tmp_root() -> PathBuf {
    let dir = tempfile::tempdir().expect("tempdir");
    let p = dir.path().to_path_buf();
    std::mem::forget(dir);
    p
}

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
        model_id: Some(ModelId("m1".into())),
        dimensions: SmallDimensions::default(),
        ingested_at_ms: ts,
    }
}

/// Build a live AppState with a manifest already on disk so the
/// admin-side `open_state_for_admin` finds something to load. Returns
/// the live AppState too so the test can commit segments through the
/// same instance.
async fn setup_db_with_segments(events: Vec<(Vec<UsageEvent>, u32)>) -> (PathBuf, AppState) {
    let root = tmp_root();
    let config = Config {
        db_root: root.clone(),
        default_bucket_count: 2,
        ..Config::default()
    };
    std::fs::create_dir_all(&config.db_root).unwrap();
    let wal = Wal::open(root.join("wal"), 0).unwrap();
    let manifest = Manifest { bucket_count: 2, ..Manifest::default() };
    let (flush_sender, _r) = tokio::sync::mpsc::channel(4);
    let state: AppState = Arc::new(AppStateInner {
        config,
        dedupe: Mutex::new(HotDedupe::new(1000)),
        wal: Mutex::new(wal),
        memtable: Mutex::new(Memtable::new()),
        manifest: RwLock::new(manifest),
        flush_sender,
    });

    // Persist the manifest so `open_state_for_admin` finds something
    // even when the test commits zero segments.
    {
        let mut m = state.manifest.write().await;
        m.save(&state.config.db_root).unwrap();
    }

    for (evts, bucket) in events {
        let segment_id = format!("raw_{}", uuid::Uuid::new_v4().simple());
        let path = state.config.db_root.join(format!("{}.seg", segment_id));
        let mut w = RawSegmentWriter::new(path).unwrap();
        for e in &evts { w.write_event(e).unwrap(); }
        let (_rows, checksum) = w.finish().unwrap();
        let meta = build_segment_meta(&segment_id, &evts, bucket, checksum);
        let mut m = state.manifest.write().await;
        m.raw_segments.push(meta);
        m.save(&state.config.db_root).unwrap();
    }

    (root, state)
}

// =========================================================================
// `check`
// =========================================================================

#[tokio::test]
async fn check_reports_manifest_summary() {
    let (root, _live) = setup_db_with_segments(vec![
        (vec![make_event("a", "acc", 1000, 10)], 0),
        (vec![make_event("b", "acc", 2000, 20)], 0),
    ])
    .await;
    drop(_live); // close the live state so admin can reopen cleanly

    let config = Config { db_root: root.clone(), ..Config::default() };
    let state = open_state_for_admin(config).unwrap();
    let out = cmd_check(state, false).await.unwrap();
    assert!(out.contains("Raw segments:         2"), "{}", out);
    assert!(out.contains("Rollup segments:      0"), "{}", out);
    assert!(out.contains("Manifest generation:"), "{}", out);
}

#[tokio::test]
async fn check_deep_passes_for_valid_segments() {
    let (root, _live) = setup_db_with_segments(vec![
        (vec![make_event("a", "acc", 1000, 10)], 0),
    ])
    .await;
    drop(_live);

    let config = Config { db_root: root.clone(), ..Config::default() };
    let state = open_state_for_admin(config).unwrap();
    let out = cmd_check(state, true).await.unwrap();
    assert!(out.contains("All segments verified"), "deep check should pass: {}", out);
}

#[tokio::test]
async fn check_deep_fails_for_corrupt_segment() {
    let (root, _live) = setup_db_with_segments(vec![
        (vec![make_event("a", "acc", 1000, 10)], 0),
    ])
    .await;
    drop(_live);

    // Corrupt the only segment file.
    let seg = std::fs::read_dir(&root)
        .unwrap()
        .find_map(|e| {
            let e = e.ok()?;
            let n = e.file_name();
            let n = n.to_str()?;
            if n.starts_with("raw_") && n.ends_with(".seg") {
                Some(e.path())
            } else {
                None
            }
        })
        .expect("segment file");
    let mut bytes = std::fs::read(&seg).unwrap();
    bytes[40] ^= 0xFF;
    std::fs::write(&seg, bytes).unwrap();

    let config = Config { db_root: root.clone(), ..Config::default() };
    let state = open_state_for_admin(config).unwrap();
    let err = cmd_check(state, true).await.unwrap_err();
    assert!(err.to_string().contains("failed verification"), "{}", err);
}

#[tokio::test]
async fn check_on_fresh_db_errors_with_clear_message() {
    let root = tmp_root();
    std::fs::create_dir_all(&root).unwrap();
    let config = Config { db_root: root, ..Config::default() };
    let err = match open_state_for_admin(config) {
        Ok(_) => panic!("expected error on a fresh DB"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("no manifest found"),
        "should give a clear error: {}",
        err
    );
}

// =========================================================================
// `inspect-segment`
// =========================================================================

#[tokio::test]
async fn inspect_segment_prints_meta_and_sample_rows() {
    let (root, live) = setup_db_with_segments(vec![
        (vec![make_event("evt_a", "acc_q", 1000, 100), make_event("evt_b", "acc_q", 2000, 200)], 0),
    ])
    .await;

    let seg_id = {
        let m = live.manifest.read().await;
        m.raw_segments[0].segment_id.clone()
    };
    drop(live);

    let config = Config { db_root: root.clone(), ..Config::default() };
    let state = open_state_for_admin(config).unwrap();
    let out = cmd_inspect_segment(state, &seg_id).await.unwrap();

    assert!(out.contains(&seg_id), "{}", out);
    assert!(out.contains("Rows: 2"), "{}", out);
    assert!(out.contains("Bucket: 0"), "{}", out);
    assert!(out.contains("evt_a"), "should show sample rows: {}", out);
    assert!(out.contains("evt_b"), "{}", out);
}

#[tokio::test]
async fn inspect_segment_errors_for_unknown_id() {
    let (root, _live) = setup_db_with_segments(vec![]).await;
    drop(_live);

    let config = Config { db_root: root.clone(), ..Config::default() };
    let state = open_state_for_admin(config).unwrap();
    let err = cmd_inspect_segment(state, "raw_bogus").await.unwrap_err();
    assert!(err.to_string().contains("not found in manifest"), "{}", err);
}

// =========================================================================
// `rebuild-rollups`
// =========================================================================

#[tokio::test]
async fn rebuild_rollups_drops_and_rewinds() {
    let bucket = bucket_for_account(&AccountId("acc_r".into()), 2);
    let h = 10 * HOUR_MS;
    let (root, live) = setup_db_with_segments(vec![
        (vec![make_event("a", "acc_r", h + 1, 10), make_event("b", "acc_r", h + 2, 20)], bucket),
    ])
    .await;

    // Seal hour 10 into a rollup via a real worker tick.
    let worker = RollupWorker::new(
        live.clone(),
        0,
        std::time::Duration::from_secs(30),
        i64::MAX,
    );
    worker.tick(h + HOUR_MS + 1).await.unwrap();
    assert_eq!(live.manifest.read().await.rollup_segments.len(), 1);
    drop(live);

    let config = Config { db_root: root.clone(), ..Config::default() };
    let state = open_state_for_admin(config).unwrap();
    let out = cmd_rebuild_rollups(
        state.clone(),
        "1970-01-01T00:00:00Z",
        "2030-01-01T00:00:00Z",
    )
    .await
    .unwrap();
    assert!(out.contains("Dropped 1 rollup segment"), "{}", out);

    let m = state.manifest.read().await;
    assert!(m.rollup_segments.is_empty());
    assert_eq!(m.watermarks.hourly_rollup_ms, 0);
}

#[tokio::test]
async fn rebuild_rollups_rejects_invalid_dates() {
    let (root, _live) = setup_db_with_segments(vec![]).await;
    drop(_live);

    let config = Config { db_root: root, ..Config::default() };
    let state = open_state_for_admin(config).unwrap();
    let err = cmd_rebuild_rollups(state, "not-a-date", "2030-01-01T00:00:00Z")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("invalid"), "{}", err);
}

// =========================================================================
// `verify-period`
// =========================================================================

#[tokio::test]
async fn verify_period_reports_match_for_sealed_rollup() {
    let bucket = bucket_for_account(&AccountId("acc_v".into()), 2);
    let h = 20 * HOUR_MS;
    let (root, live) = setup_db_with_segments(vec![
        (vec![make_event("a", "acc_v", h + 1, 30), make_event("b", "acc_v", h + 2, 40)], bucket),
    ])
    .await;
    let worker = RollupWorker::new(
        live.clone(),
        0,
        std::time::Duration::from_secs(30),
        i64::MAX,
    );
    worker.tick(h + HOUR_MS + 1).await.unwrap();
    drop(live);

    let config = Config { db_root: root, ..Config::default() };
    let state = open_state_for_admin(config).unwrap();
    let out = cmd_verify_period(
        state,
        "acc_v",
        "1970-01-01T00:00:00Z",
        "2030-01-01T00:00:00Z",
    )
    .await
    .unwrap();
    assert!(out.contains("Raw total:      70"), "{}", out);
    assert!(out.contains("Rollup total:   70"), "{}", out);
    assert!(out.contains("Drift:          0 (OK)"), "{}", out);
}

// =========================================================================
// `export-parquet`
// =========================================================================

#[tokio::test]
async fn export_parquet_writes_file() {
    let (root, _live) = setup_db_with_segments(vec![
        (vec![make_event("a", "acc", 1000, 5)], 0),
        (vec![make_event("b", "acc", 2000, 7)], 0),
    ])
    .await;
    drop(_live);

    let out_path = root.join("export.parquet");
    let config = Config { db_root: root.clone(), ..Config::default() };
    let state = open_state_for_admin(config).unwrap();
    let msg = cmd_export_parquet(state, &out_path).await.unwrap();
    assert!(msg.contains("Exported 2 events"), "{}", msg);
    assert!(msg.contains("2 segment(s)"), "{}", msg);
    assert!(out_path.exists());
    assert!(std::fs::metadata(&out_path).unwrap().len() > 0);
}
