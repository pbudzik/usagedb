//! Integration tests for the Phase D operator endpoints:
//!
//!   GET /v1/accounts/:account_id/explain  — breakdown + segment provenance
//!                                            + corrections
//!   GET /v1/accounts/:account_id/verify   — raw-vs-rollup drift check
//!
//! Drives the actual axum Router via `tower::oneshot` so the param
//! parsing and HTTP plumbing get exercised end-to-end, not just the
//! underlying executor.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tokio::sync::{Mutex, RwLock};
use tower::ServiceExt;

use usagedb::api::http_server::build_router;
use usagedb::ingest::dedupe::HotDedupe;
use usagedb::ingest::flusher::build_segment_meta;
use usagedb::ingest::memtable::Memtable;
use usagedb::ingest::wal::Wal;
use usagedb::model::dimensions::SmallDimensions;
use usagedb::model::event::{CorrectionRef, EventKind, UsageEvent};
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

async fn commit_segment(state: &AppState, events: &[UsageEvent], bucket: u32) -> String {
    let segment_id = format!("raw_{}", uuid::Uuid::new_v4().simple());
    let path = state.config.db_root.join(format!("{}.seg", segment_id));
    let mut writer = RawSegmentWriter::new(path).unwrap();
    for e in events { writer.write_event(e).unwrap(); }
    let (_rows, checksum) = writer.finish().unwrap();
    let meta = build_segment_meta(&segment_id, events, bucket, checksum);
    let mut manifest = state.manifest.write().await;
    manifest.raw_segments.push(meta);
    manifest.save(&state.config.db_root).unwrap();
    segment_id
}

/// Send a GET request through the Router and parse the response body as JSON.
async fn get_json(state: AppState, uri: &str) -> (StatusCode, serde_json::Value) {
    let app = build_router(state);
    let req = Request::builder()
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    (status, v)
}

// =========================================================================
// /v1/accounts/{id}/explain
// =========================================================================

#[tokio::test]
async fn explain_breaks_down_by_billing_dimensions() {
    let root = tmp_root();
    let state = build_state(root.clone(), 2);
    let bucket = bucket_for_account(&AccountId("acc_e".into()), 2);

    // Two events on different meters so the breakdown has two rows.
    let h = 10 * HOUR_MS;
    let e1 = make_event("a", "acc_e", h + 1, 100);
    let mut e2 = make_event("b", "acc_e", h + 2, 200);
    e2.meter_id = MeterId("tokens.output".into());
    commit_segment(&state, &[e1, e2], bucket).await;

    // Seal the hour into a rollup so the explain path exercises both
    // rollup_segments and the breakdown.
    let worker = RollupWorker::new(state.clone(), 0, Duration::from_secs(30), i64::MAX);
    worker.tick(h + HOUR_MS + 1).await.unwrap();

    let uri = format!(
        "/v1/accounts/acc_e/explain?from=1970-01-01T00:00:00Z&to=2030-01-01T00:00:00Z"
    );
    let (status, body) = get_json(state.clone(), &uri).await;
    assert_eq!(status, StatusCode::OK);

    // Lines: should have two entries, one per meter.
    let lines = body["lines"].as_array().expect("lines array");
    assert_eq!(lines.len(), 2, "expected breakdown per meter; got {:?}", lines);

    // Segments overlapping the range should be listed.
    let rollups = body["rollup_segments"].as_array().expect("rollup_segments array");
    assert!(
        !rollups.is_empty(),
        "explain should list the rollup segments that overlap the range"
    );

    // No corrections in this run.
    let corrections = body["corrections"].as_array().expect("corrections array");
    assert!(corrections.is_empty());

    // Watermark must be at or past the queried range's hour end.
    let watermark = body["watermark_ms"].as_i64().unwrap();
    assert!(watermark >= h + HOUR_MS);
}

#[tokio::test]
async fn explain_surfaces_corrections_separately() {
    let root = tmp_root();
    let state = build_state(root.clone(), 2);
    let bucket = bucket_for_account(&AccountId("acc_x".into()), 2);

    let h = 5 * HOUR_MS;
    let original = make_event("orig", "acc_x", h + 1, 100);
    let mut correction = make_event("corr", "acc_x", h + 2, -40);
    correction.kind = EventKind::Correction;
    correction.correction_ref = Some(CorrectionRef {
        original_event_id: EventId("orig".into()),
        reason: "double-counted".into(),
    });
    commit_segment(&state, &[original, correction], bucket).await;

    let uri = "/v1/accounts/acc_x/explain?from=1970-01-01T00:00:00Z&to=2030-01-01T00:00:00Z";
    let (status, body) = get_json(state.clone(), uri).await;
    assert_eq!(status, StatusCode::OK);

    // Corrections list should contain exactly the correction event.
    let corrections = body["corrections"].as_array().expect("corrections array");
    assert_eq!(corrections.len(), 1);
    let c = &corrections[0];
    assert_eq!(c["event_id"], serde_json::json!("corr"));
    assert_eq!(c["quantity"], serde_json::json!(-40));
    assert_eq!(c["kind"], serde_json::json!("Correction"));

    // The total in the breakdown should net (100 - 40 = 60). No rollups
    // exist yet (no tick), so it comes from the raw path.
    let lines = body["lines"].as_array().expect("lines array");
    let total: i128 = lines
        .iter()
        .filter_map(|r| r["quantity"].as_str())
        .filter_map(|s| s.parse::<i128>().ok())
        .sum();
    assert_eq!(total, 60);
}

#[tokio::test]
async fn explain_rejects_invalid_dates() {
    let root = tmp_root();
    let state = build_state(root.clone(), 2);
    let (status, _) = get_json(
        state,
        "/v1/accounts/acc/explain?from=not-a-date&to=2030-01-01T00:00:00Z",
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR); // wrapped into AppError
}

// =========================================================================
// /v1/accounts/{id}/verify
// =========================================================================

#[tokio::test]
async fn verify_reports_zero_drift_when_rollup_matches_raw() {
    let root = tmp_root();
    let state = build_state(root.clone(), 2);
    let bucket = bucket_for_account(&AccountId("acc_v".into()), 2);

    let h = 20 * HOUR_MS;
    let events: Vec<UsageEvent> = (0..10)
        .map(|i| make_event(&format!("e{i}"), "acc_v", h + i, 7))
        .collect();
    commit_segment(&state, &events, bucket).await;

    let worker = RollupWorker::new(state.clone(), 0, Duration::from_secs(30), i64::MAX);
    worker.tick(h + HOUR_MS + 1).await.unwrap();

    let uri = format!(
        "/v1/accounts/acc_v/verify?from=1970-01-01T00:00:00Z&to=2030-01-01T00:00:00Z"
    );
    let (status, body) = get_json(state.clone(), &uri).await;
    assert_eq!(status, StatusCode::OK);

    assert_eq!(body["raw_total"], serde_json::json!("70"));
    assert_eq!(body["rollup_total"], serde_json::json!("70"));
    assert_eq!(body["drift"], serde_json::json!("0"));
    assert_eq!(body["matches"], serde_json::json!(true));
}

#[tokio::test]
async fn verify_detects_drift_when_rollup_misses_late_event() {
    let root = tmp_root();
    let state = build_state(root.clone(), 2);
    let bucket = bucket_for_account(&AccountId("acc_d".into()), 2);

    // Seal hour 30 with one event of 100.
    let h = 30 * HOUR_MS;
    commit_segment(&state, &[make_event("a", "acc_d", h + 1, 100)], bucket).await;
    let worker = RollupWorker::new(state.clone(), 0, Duration::from_secs(30), i64::MAX);
    worker.tick(h + HOUR_MS + 1).await.unwrap();

    // Now a late event lands in hour 30 (below the watermark). The
    // rollup worker won't re-roll it without explicit rebuild_rollups,
    // so verify should see drift.
    commit_segment(&state, &[make_event("late", "acc_d", h + 5, 50)], bucket).await;

    let uri = "/v1/accounts/acc_d/verify?from=1970-01-01T00:00:00Z&to=2030-01-01T00:00:00Z";
    let (status, body) = get_json(state.clone(), uri).await;
    assert_eq!(status, StatusCode::OK);

    assert_eq!(body["raw_total"], serde_json::json!("150"));
    assert_eq!(body["rollup_total"], serde_json::json!("100"));
    assert_eq!(body["drift"], serde_json::json!("50"));
    assert_eq!(body["matches"], serde_json::json!(false));
}

#[tokio::test]
async fn verify_reports_period_sealed_status() {
    let root = tmp_root();
    let state = build_state(root.clone(), 2);
    let bucket = bucket_for_account(&AccountId("acc_p".into()), 2);

    let h = 40 * HOUR_MS;
    commit_segment(&state, &[make_event("a", "acc_p", h + 1, 10)], bucket).await;
    let worker = RollupWorker::new(state.clone(), 0, Duration::from_secs(30), i64::MAX);
    worker.tick(h + HOUR_MS + 1).await.unwrap();

    // Query a fully-sealed range: `to_ms` <= watermark. Use a literal
    // RFC 3339 with `Z` suffix (rather than to_rfc3339 which uses
    // `+00:00` and gets URL-mangled).
    let to_str = format!(
        "{}",
        chrono::DateTime::from_timestamp_millis(h + HOUR_MS).unwrap().format("%Y-%m-%dT%H:%M:%SZ")
    );
    let uri = format!(
        "/v1/accounts/acc_p/verify?from=1970-01-01T00:00:00Z&to={}",
        to_str,
    );
    let (status, body) = get_json(state.clone(), &uri).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["period_sealed"], serde_json::json!(true));
}
