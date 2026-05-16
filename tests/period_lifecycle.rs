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
