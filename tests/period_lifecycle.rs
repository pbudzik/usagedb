//! Integration tests for the minimal period lifecycle (Phase D):
//!   - POST /v1/accounts/{id}/periods/{YYYY-MM}/close → Closed state
//!   - POST .../reopen → Open state
//!   - GET .../{period} → state + closed_at_ms + total_quantity
//!   - Ingest: `Usage` events in a closed period are rejected
//!   - Ingest: `Correction` / `Retraction` events are still accepted (adjustments)
//!   - Period helpers: `period_for_ts`, `parse_period`, `is_period_closed`

use std::path::PathBuf;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use tokio::sync::{Mutex, RwLock};
use tower::ServiceExt;

use usagedb::api::http::{IngestBatchRequest, IngestBatchResponse};
use usagedb::api::http_server::build_router;
use usagedb::ingest::dedupe::HotDedupe;
use usagedb::ingest::memtable::Memtable;
use usagedb::ingest::wal::Wal;
use usagedb::model::dimensions::SmallDimensions;
use usagedb::model::event::{CorrectionRef, EventKind, UsageEvent};
use usagedb::model::ids::{
    AccountId, EventId, MeterId, ModelId, ProductId, SourceId, SubscriptionId, Unit,
};
use usagedb::period::{is_period_closed, parse_period, period_for_ts};
use usagedb::runtime::config::Config;
use usagedb::runtime::state::{AppState, AppStateInner};
use usagedb::storage::manifest::{ClosedPeriod, Manifest};

fn tmp_root() -> PathBuf {
    let dir = tempfile::tempdir().expect("tempdir");
    let p = dir.path().to_path_buf();
    std::mem::forget(dir);
    p
}

fn build_state(db_root: PathBuf) -> AppState {
    let config = Config {
        db_root: db_root.clone(),
        default_bucket_count: 2,
        memtable_max_age_ms: i64::MAX,
        ..Config::default()
    };
    std::fs::create_dir_all(&config.db_root).unwrap();
    let wal = Wal::open(db_root.join("wal"), 0).unwrap();
    let manifest = Manifest { bucket_count: 2, ..Manifest::default() };
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

fn make_event(id: &str, account: &str, ts: i64, qty: i128) -> UsageEvent {
    UsageEvent {
        event_id: EventId(id.to_string()),
        kind: EventKind::Usage,
        correction_ref: None,
        account_id: AccountId(account.to_string()),
        subscription_id: Some(SubscriptionId("sub".into())),
        product_id: ProductId("prod".into()),
        meter_id: MeterId("meter".into()),
        timestamp_ms: ts,
        quantity: qty,
        unit: Unit("token".into()),
        source: SourceId("test".into()),
        model_id: Some(ModelId("m1".into())),
        dimensions: SmallDimensions::default(),
        ingested_at_ms: ts,
    }
}

/// Timestamp for the first second of a UTC (year, month, day).
fn ts_at(year: i32, month: u32, day: u32) -> i64 {
    use chrono::{NaiveDate, TimeZone, Utc};
    let dt = NaiveDate::from_ymd_opt(year, month, day)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    Utc.from_utc_datetime(&dt).timestamp_millis()
}

async fn json_post(state: AppState, uri: &str, body: serde_json::Value) -> (StatusCode, serde_json::Value) {
    let app = build_router(state);
    let req = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let v = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    (status, v)
}

async fn empty_post(state: AppState, uri: &str) -> (StatusCode, serde_json::Value) {
    let app = build_router(state);
    let req = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let v = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    (status, v)
}

async fn get_json(state: AppState, uri: &str) -> (StatusCode, serde_json::Value) {
    let app = build_router(state);
    let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
    let response = app.oneshot(req).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let v = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    (status, v)
}

// =========================================================================
// Period helpers
// =========================================================================

#[test]
fn period_for_ts_returns_utc_year_month() {
    // 2026-05-16T12:00:00Z
    let ts = ts_at(2026, 5, 16);
    assert_eq!(period_for_ts(ts), Some((2026, 5)));
    // Boundary: 2026-01-01 → (2026, 1)
    assert_eq!(period_for_ts(ts_at(2026, 1, 1)), Some((2026, 1)));
    // Negative timestamps are pre-1970; valid per chrono but unusual.
    // We don't need to assert specific behavior, just that nothing panics.
    let _ = period_for_ts(-1);
}

#[test]
fn parse_period_round_trips() {
    assert_eq!(parse_period("2026-05"), Ok((2026, 5)));
    assert_eq!(parse_period("2030-12"), Ok((2030, 12)));
    assert!(parse_period("2026").is_err());
    assert!(parse_period("2026-13").is_err());
    assert!(parse_period("2026-0").is_err());
    assert!(parse_period("abc-de").is_err());
}

#[test]
fn is_period_closed_finds_match() {
    let mut manifest = Manifest::default();
    assert!(!is_period_closed(&manifest, "acc", 2026, 5));
    manifest.closed_periods.push(ClosedPeriod {
        account_id: "acc".into(),
        year: 2026,
        month: 5,
        closed_at_ms: 1000,
        frozen_quantity: None,
        frozen_event_count: None,
        watermark_at_close_ms: None,
    });
    assert!(is_period_closed(&manifest, "acc", 2026, 5));
    assert!(!is_period_closed(&manifest, "acc", 2026, 6));
    assert!(!is_period_closed(&manifest, "different", 2026, 5));
}

// =========================================================================
// HTTP: close / reopen / get
// =========================================================================

#[tokio::test]
async fn close_period_persists_in_manifest() {
    let root = tmp_root();
    let state = build_state(root.clone());

    let (status, body) = empty_post(
        state.clone(),
        "/v1/accounts/acc_x/periods/2026-05/close",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["state"], serde_json::json!("Closed"));
    assert!(body["closed_at_ms"].as_i64().unwrap() > 0);

    // Verify persisted in manifest.
    let manifest = state.manifest.read().await;
    assert_eq!(manifest.closed_periods.len(), 1);
    assert_eq!(manifest.closed_periods[0].account_id, "acc_x");
    assert_eq!(manifest.closed_periods[0].year, 2026);
    assert_eq!(manifest.closed_periods[0].month, 5);
}

#[tokio::test]
async fn close_period_is_idempotent() {
    let root = tmp_root();
    let state = build_state(root.clone());

    let _ = empty_post(state.clone(), "/v1/accounts/acc/periods/2026-05/close").await;
    let (status, body) = empty_post(
        state.clone(),
        "/v1/accounts/acc/periods/2026-05/close",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["already_closed"], serde_json::json!(true));

    // Manifest still has exactly one entry.
    let m = state.manifest.read().await;
    assert_eq!(m.closed_periods.len(), 1);
}

#[tokio::test]
async fn reopen_period_removes_marker() {
    let root = tmp_root();
    let state = build_state(root.clone());

    let _ = empty_post(state.clone(), "/v1/accounts/acc/periods/2026-05/close").await;
    let (status, body) = empty_post(
        state.clone(),
        "/v1/accounts/acc/periods/2026-05/reopen",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["state"], serde_json::json!("Open"));
    assert_eq!(body["removed"], serde_json::json!(true));

    let m = state.manifest.read().await;
    assert!(m.closed_periods.is_empty());
}

#[tokio::test]
async fn get_period_returns_state_and_total() {
    let root = tmp_root();
    let state = build_state(root.clone());

    let (status, body) = get_json(
        state.clone(),
        "/v1/accounts/acc/periods/2026-05",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["state"], serde_json::json!("Open"));
    assert!(body["closed_at_ms"].is_null());
    assert_eq!(body["total_quantity"], serde_json::json!("0"));

    // After closing, the GET reports Closed + the close timestamp.
    let _ = empty_post(state.clone(), "/v1/accounts/acc/periods/2026-05/close").await;
    let (_status, body) = get_json(state.clone(), "/v1/accounts/acc/periods/2026-05").await;
    assert_eq!(body["state"], serde_json::json!("Closed"));
    assert!(body["closed_at_ms"].as_i64().unwrap() > 0);
}

#[tokio::test]
async fn close_period_rejects_invalid_format() {
    let root = tmp_root();
    let state = build_state(root.clone());
    let (status, _) =
        empty_post(state, "/v1/accounts/acc/periods/not-a-period/close").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

// =========================================================================
// Ingest semantics
// =========================================================================

#[tokio::test]
async fn closed_period_rejects_usage_events() {
    let root = tmp_root();
    let state = build_state(root.clone());

    // Close 2026-05 for acc_x.
    let _ = empty_post(state.clone(), "/v1/accounts/acc_x/periods/2026-05/close").await;

    // Ingest a Usage event in that period — should be rejected.
    let in_period_ts = ts_at(2026, 5, 10);
    let event = make_event("evt_blocked", "acc_x", in_period_ts, 100);
    let payload = IngestBatchRequest { events: vec![event] };

    let (status, body) = json_post(
        state.clone(),
        "/v1/usage/batch",
        serde_json::to_value(&payload).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let resp: IngestBatchResponse = serde_json::from_value(body).unwrap();
    assert_eq!(resp.accepted, 0);
    assert_eq!(resp.rejected, 1);
}

#[tokio::test]
async fn closed_period_only_rejects_target_account() {
    let root = tmp_root();
    let state = build_state(root.clone());

    let _ = empty_post(state.clone(), "/v1/accounts/acc_x/periods/2026-05/close").await;

    // Same period but a DIFFERENT account — should be accepted.
    let in_period_ts = ts_at(2026, 5, 10);
    let event = make_event("evt_other", "acc_y", in_period_ts, 100);
    let payload = IngestBatchRequest { events: vec![event] };

    let (_status, body) = json_post(
        state,
        "/v1/usage/batch",
        serde_json::to_value(&payload).unwrap(),
    )
    .await;
    let resp: IngestBatchResponse = serde_json::from_value(body).unwrap();
    assert_eq!(resp.accepted, 1);
    assert_eq!(resp.rejected, 0);
}

#[tokio::test]
async fn closed_period_only_rejects_in_period_timestamps() {
    let root = tmp_root();
    let state = build_state(root.clone());

    let _ = empty_post(state.clone(), "/v1/accounts/acc_x/periods/2026-05/close").await;

    // Same account, but a DIFFERENT period — should be accepted.
    let ts_in_april = ts_at(2026, 4, 28);
    let ts_in_june = ts_at(2026, 6, 1);
    let payload = IngestBatchRequest {
        events: vec![
            make_event("evt_apr", "acc_x", ts_in_april, 1),
            make_event("evt_jun", "acc_x", ts_in_june, 1),
        ],
    };
    let (_status, body) = json_post(
        state,
        "/v1/usage/batch",
        serde_json::to_value(&payload).unwrap(),
    )
    .await;
    let resp: IngestBatchResponse = serde_json::from_value(body).unwrap();
    assert_eq!(resp.accepted, 2, "events outside closed period must be accepted");
    assert_eq!(resp.rejected, 0);
}

// =========================================================================
// Frozen snapshot semantics
// =========================================================================

#[tokio::test]
async fn close_captures_frozen_snapshot() {
    let root = tmp_root();
    let state = build_state(root.clone());

    // Ingest two Usage events in 2026-05.
    let payload = IngestBatchRequest {
        events: vec![
            make_event("e1", "acc_s", ts_at(2026, 5, 10), 50),
            make_event("e2", "acc_s", ts_at(2026, 5, 20), 75),
        ],
    };
    let _ = json_post(state.clone(), "/v1/usage/batch", serde_json::to_value(&payload).unwrap()).await;

    // Force a flush + rollup so the rollup query at close time has
    // committed data to read. We drive the rollup worker directly.
    use usagedb::rollup::worker::RollupWorker;
    let memtable_events = {
        let mut m = state.memtable.lock().await;
        m.drain_all()
    };
    // Write a segment for the events directly so the rollup worker has
    // segments to scan.
    use usagedb::ingest::flusher::build_segment_meta;
    use usagedb::model::ids::bucket_for_account;
    use usagedb::storage::segment_writer::RawSegmentWriter;
    let bucket = bucket_for_account(&AccountId("acc_s".into()), 2);
    let segment_id = format!("raw_{}", uuid::Uuid::new_v4().simple());
    let path = state.config.db_root.join(format!("{}.seg", segment_id));
    let mut w = RawSegmentWriter::new(path).unwrap();
    for e in &memtable_events { w.write_event(e).unwrap(); }
    let (_rows, checksum) = w.finish().unwrap();
    let meta = build_segment_meta(&segment_id, &memtable_events, bucket, checksum);
    {
        let mut m = state.manifest.write().await;
        m.raw_segments.push(meta);
        m.save(&state.config.db_root).unwrap();
    }
    let worker = RollupWorker::new(
        state.clone(),
        0,
        std::time::Duration::from_secs(30),
        i64::MAX,
    );
    worker
        .tick(ts_at(2026, 6, 1) + 60_000)
        .await
        .unwrap();

    // Close the period — should snapshot 50 + 75 = 125 with 2 events.
    let (status, body) =
        empty_post(state.clone(), "/v1/accounts/acc_s/periods/2026-05/close").await;
    assert_eq!(status, StatusCode::OK);
    let frozen = &body["frozen"];
    assert!(!frozen.is_null(), "close response must include frozen snapshot");
    assert_eq!(frozen["quantity"], serde_json::json!("125"));
    assert_eq!(frozen["event_count"], serde_json::json!(2));
}

#[tokio::test]
async fn get_period_returns_frozen_snapshot_after_close() {
    let root = tmp_root();
    let state = build_state(root.clone());

    // Same setup as the snapshot test.
    let payload = IngestBatchRequest {
        events: vec![make_event("e1", "acc_g", ts_at(2026, 5, 10), 200)],
    };
    let _ = json_post(state.clone(), "/v1/usage/batch", serde_json::to_value(&payload).unwrap()).await;

    use usagedb::ingest::flusher::build_segment_meta;
    use usagedb::model::ids::bucket_for_account;
    use usagedb::rollup::worker::RollupWorker;
    use usagedb::storage::segment_writer::RawSegmentWriter;
    let events = state.memtable.lock().await.drain_all();
    let bucket = bucket_for_account(&AccountId("acc_g".into()), 2);
    let segment_id = format!("raw_{}", uuid::Uuid::new_v4().simple());
    let path = state.config.db_root.join(format!("{}.seg", segment_id));
    let mut w = RawSegmentWriter::new(path).unwrap();
    for e in &events { w.write_event(e).unwrap(); }
    let (_rows, checksum) = w.finish().unwrap();
    let meta = build_segment_meta(&segment_id, &events, bucket, checksum);
    {
        let mut m = state.manifest.write().await;
        m.raw_segments.push(meta);
        m.save(&state.config.db_root).unwrap();
    }
    let worker = RollupWorker::new(state.clone(), 0, std::time::Duration::from_secs(30), i64::MAX);
    worker.tick(ts_at(2026, 6, 1) + 60_000).await.unwrap();

    // Close.
    let _ = empty_post(state.clone(), "/v1/accounts/acc_g/periods/2026-05/close").await;

    // GET should return the frozen snapshot, not a live total.
    let (_status, body) = get_json(state.clone(), "/v1/accounts/acc_g/periods/2026-05").await;
    assert_eq!(body["state"], serde_json::json!("Closed"));
    assert_eq!(body["frozen"]["quantity"], serde_json::json!("200"));
    assert_eq!(body["frozen"]["event_count"], serde_json::json!(1));
    assert_eq!(
        body["adjustments_quantity"],
        serde_json::json!("0"),
        "no adjustments yet"
    );
    assert_eq!(body["net_total"], serde_json::json!("200"));
}

#[tokio::test]
async fn correction_after_close_surfaces_as_adjustment_not_frozen() {
    let root = tmp_root();
    let state = build_state(root.clone());

    // Setup: 1 event of 100, rolled up, then close.
    let payload = IngestBatchRequest {
        events: vec![make_event("orig", "acc_c", ts_at(2026, 5, 10), 100)],
    };
    let _ = json_post(state.clone(), "/v1/usage/batch", serde_json::to_value(&payload).unwrap()).await;

    use usagedb::ingest::flusher::build_segment_meta;
    use usagedb::model::ids::bucket_for_account;
    use usagedb::rollup::worker::RollupWorker;
    use usagedb::storage::segment_writer::RawSegmentWriter;
    let events = state.memtable.lock().await.drain_all();
    let bucket = bucket_for_account(&AccountId("acc_c".into()), 2);
    let segment_id = format!("raw_{}", uuid::Uuid::new_v4().simple());
    let path = state.config.db_root.join(format!("{}.seg", segment_id));
    let mut w = RawSegmentWriter::new(path).unwrap();
    for e in &events { w.write_event(e).unwrap(); }
    let (_rows, checksum) = w.finish().unwrap();
    let meta = build_segment_meta(&segment_id, &events, bucket, checksum);
    {
        let mut m = state.manifest.write().await;
        m.raw_segments.push(meta);
        m.save(&state.config.db_root).unwrap();
    }
    let worker = RollupWorker::new(state.clone(), 0, std::time::Duration::from_secs(30), i64::MAX);
    worker.tick(ts_at(2026, 6, 1) + 60_000).await.unwrap();
    let _ = empty_post(state.clone(), "/v1/accounts/acc_c/periods/2026-05/close").await;

    // Now land a Correction event in the closed period.
    let mut correction = make_event("corr", "acc_c", ts_at(2026, 5, 15), -40);
    correction.kind = EventKind::Correction;
    correction.correction_ref = Some(CorrectionRef {
        original_event_id: EventId("orig".into()),
        reason: "overcount".into(),
    });
    let payload = IngestBatchRequest { events: vec![correction] };
    let (_status, body) = json_post(
        state.clone(),
        "/v1/usage/batch",
        serde_json::to_value(&payload).unwrap(),
    )
    .await;
    let resp: IngestBatchResponse = serde_json::from_value(body).unwrap();
    assert_eq!(resp.accepted, 1, "correction should be accepted in closed period");

    // GET should show: frozen still 100, pending_adjustments has the
    // correction, net_total = 60.
    let (_status, body) = get_json(state.clone(), "/v1/accounts/acc_c/periods/2026-05").await;
    assert_eq!(
        body["frozen"]["quantity"],
        serde_json::json!("100"),
        "frozen snapshot must NOT change after a post-close correction"
    );
    let adjustments = body["pending_adjustments"].as_array().expect("adjustments array");
    assert_eq!(adjustments.len(), 1);
    assert_eq!(adjustments[0]["event_id"], serde_json::json!("corr"));
    assert_eq!(body["net_total"], serde_json::json!("60"));
}

#[tokio::test]
async fn closing_twice_keeps_original_snapshot() {
    let root = tmp_root();
    let state = build_state(root.clone());

    // Setup with one event + rollup.
    let payload = IngestBatchRequest {
        events: vec![make_event("e1", "acc_t", ts_at(2026, 5, 10), 100)],
    };
    let _ = json_post(state.clone(), "/v1/usage/batch", serde_json::to_value(&payload).unwrap()).await;

    use usagedb::ingest::flusher::build_segment_meta;
    use usagedb::model::ids::bucket_for_account;
    use usagedb::rollup::worker::RollupWorker;
    use usagedb::storage::segment_writer::RawSegmentWriter;
    let events = state.memtable.lock().await.drain_all();
    let bucket = bucket_for_account(&AccountId("acc_t".into()), 2);
    let segment_id = format!("raw_{}", uuid::Uuid::new_v4().simple());
    let path = state.config.db_root.join(format!("{}.seg", segment_id));
    let mut w = RawSegmentWriter::new(path).unwrap();
    for e in &events { w.write_event(e).unwrap(); }
    let (_rows, checksum) = w.finish().unwrap();
    let meta = build_segment_meta(&segment_id, &events, bucket, checksum);
    {
        let mut m = state.manifest.write().await;
        m.raw_segments.push(meta);
        m.save(&state.config.db_root).unwrap();
    }
    let worker = RollupWorker::new(state.clone(), 0, std::time::Duration::from_secs(30), i64::MAX);
    worker.tick(ts_at(2026, 6, 1) + 60_000).await.unwrap();

    let (_, first) = empty_post(state.clone(), "/v1/accounts/acc_t/periods/2026-05/close").await;
    let first_closed_at = first["closed_at_ms"].as_i64().unwrap();
    let (_, second) = empty_post(state.clone(), "/v1/accounts/acc_t/periods/2026-05/close").await;
    assert_eq!(second["already_closed"], serde_json::json!(true));
    assert_eq!(
        second["closed_at_ms"].as_i64().unwrap(),
        first_closed_at,
        "re-close must NOT overwrite the original closed_at_ms"
    );
    assert_eq!(
        second["frozen"]["quantity"],
        serde_json::json!("100"),
        "re-close must return the original snapshot value"
    );
}

#[tokio::test]
async fn reopen_then_reclose_takes_a_fresh_snapshot() {
    let root = tmp_root();
    let state = build_state(root.clone());

    // 1 event of 100.
    let payload = IngestBatchRequest {
        events: vec![make_event("e1", "acc_rr", ts_at(2026, 5, 10), 100)],
    };
    let _ = json_post(state.clone(), "/v1/usage/batch", serde_json::to_value(&payload).unwrap()).await;

    use usagedb::ingest::flusher::build_segment_meta;
    use usagedb::model::ids::bucket_for_account;
    use usagedb::rollup::worker::RollupWorker;
    use usagedb::storage::segment_writer::RawSegmentWriter;
    let events = state.memtable.lock().await.drain_all();
    let bucket = bucket_for_account(&AccountId("acc_rr".into()), 2);
    let segment_id = format!("raw_{}", uuid::Uuid::new_v4().simple());
    let path = state.config.db_root.join(format!("{}.seg", segment_id));
    let mut w = RawSegmentWriter::new(path).unwrap();
    for e in &events { w.write_event(e).unwrap(); }
    let (_rows, checksum) = w.finish().unwrap();
    let meta = build_segment_meta(&segment_id, &events, bucket, checksum);
    {
        let mut m = state.manifest.write().await;
        m.raw_segments.push(meta);
        m.save(&state.config.db_root).unwrap();
    }
    let worker = RollupWorker::new(state.clone(), 0, std::time::Duration::from_secs(30), i64::MAX);
    worker.tick(ts_at(2026, 6, 1) + 60_000).await.unwrap();

    // First close: snapshot is 100.
    let (_, first) = empty_post(state.clone(), "/v1/accounts/acc_rr/periods/2026-05/close").await;
    assert_eq!(first["frozen"]["quantity"], serde_json::json!("100"));

    // Reopen.
    let _ = empty_post(state.clone(), "/v1/accounts/acc_rr/periods/2026-05/reopen").await;

    // Ingest more.
    let payload = IngestBatchRequest {
        events: vec![make_event("e2", "acc_rr", ts_at(2026, 5, 20), 50)],
    };
    let _ = json_post(state.clone(), "/v1/usage/batch", serde_json::to_value(&payload).unwrap()).await;

    // Flush + rollup again.
    let events2 = state.memtable.lock().await.drain_all();
    let segment_id2 = format!("raw_{}", uuid::Uuid::new_v4().simple());
    let path2 = state.config.db_root.join(format!("{}.seg", segment_id2));
    let mut w2 = RawSegmentWriter::new(path2).unwrap();
    for e in &events2 { w2.write_event(e).unwrap(); }
    let (_rows, checksum2) = w2.finish().unwrap();
    let meta2 = build_segment_meta(&segment_id2, &events2, bucket, checksum2);
    {
        let mut m = state.manifest.write().await;
        m.raw_segments.push(meta2);
        // Force the rollup worker to redo hour 10 by rebuilding rollups.
        m.rollup_segments.retain(|s| s.bucket != bucket || s.min_timestamp_ms != ts_at(2026, 5, 10) / 3_600_000 * 3_600_000);
        m.watermarks.hourly_rollup_ms = 0;
        m.save(&state.config.db_root).unwrap();
    }
    let worker2 = RollupWorker::new(state.clone(), 0, std::time::Duration::from_secs(30), i64::MAX);
    worker2.tick(ts_at(2026, 6, 1) + 60_000).await.unwrap();

    // Close again — snapshot should now be 150.
    let (_, second) = empty_post(state.clone(), "/v1/accounts/acc_rr/periods/2026-05/close").await;
    assert_eq!(
        second["frozen"]["quantity"],
        serde_json::json!("150"),
        "reopen + reclose should capture a fresh snapshot"
    );
}

#[tokio::test]
async fn closed_period_accepts_corrections() {
    let root = tmp_root();
    let state = build_state(root.clone());

    let _ = empty_post(state.clone(), "/v1/accounts/acc_x/periods/2026-05/close").await;

    let in_period_ts = ts_at(2026, 5, 10);
    let mut correction = make_event("evt_correction", "acc_x", in_period_ts, -50);
    correction.kind = EventKind::Correction;
    correction.correction_ref = Some(CorrectionRef {
        original_event_id: EventId("evt_orig".into()),
        reason: "overcount".into(),
    });
    let mut retraction = make_event("evt_retraction", "acc_x", in_period_ts, 0);
    retraction.kind = EventKind::Retraction;
    retraction.correction_ref = Some(CorrectionRef {
        original_event_id: EventId("evt_orig".into()),
        reason: "retract".into(),
    });

    let payload = IngestBatchRequest { events: vec![correction, retraction] };
    let (_status, body) = json_post(
        state,
        "/v1/usage/batch",
        serde_json::to_value(&payload).unwrap(),
    )
    .await;
    let resp: IngestBatchResponse = serde_json::from_value(body).unwrap();
    assert_eq!(
        resp.accepted, 2,
        "Correction + Retraction in closed period must be accepted as adjustments"
    );
    assert_eq!(resp.rejected, 0);
}
