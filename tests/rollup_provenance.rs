//! Regression tests for rollup → raw segment provenance (spec §19.10).
//!
//! Each rollup segment's `SegmentMeta.input_segment_ids` must list every
//! raw segment that contributed events to it. The `explain` endpoint
//! surfaces this as a `rollup_inputs: { rollup_id → [raw_id, ...] }` map
//! so an operator can drill from an invoice line back to source events.

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

/// Returns the raw segment_id so the test can assert against it.
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

async fn get_json(state: AppState, uri: &str) -> (StatusCode, serde_json::Value) {
    let app = build_router(state);
    let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
    let response = app.oneshot(req).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value =
        serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    (status, v)
}

#[tokio::test]
async fn rollup_segment_records_contributing_raw_segments() {
    let root = tmp_root();
    let state = build_state(root.clone(), 2);
    let bucket = bucket_for_account(&AccountId("acc_p".into()), 2);

    // Two raw segments for the same hour. Both should appear in the
    // rollup segment's input_segment_ids.
    let h = 30 * HOUR_MS;
    let raw_a = commit_segment(
        &state,
        &[make_event("a1", "acc_p", h + 1, 10), make_event("a2", "acc_p", h + 2, 20)],
        bucket,
    )
    .await;
    let raw_b = commit_segment(
        &state,
        &[make_event("b1", "acc_p", h + 3, 30)],
        bucket,
    )
    .await;

    // Tick the rollup worker to seal hour 30.
    let worker = RollupWorker::new(state.clone(), 0, Duration::from_secs(30), i64::MAX);
    worker.tick(h + HOUR_MS + 1).await.unwrap();

    let manifest = state.manifest.read().await;
    let rollup = manifest
        .rollup_segments
        .iter()
        .find(|s| s.min_timestamp_ms == h)
        .expect("rollup segment for hour 30 must exist");

    // Both raw segments must be listed as inputs (order isn't fixed —
    // BTreeSet sorted alphabetically inside the worker).
    assert_eq!(
        rollup.input_segment_ids.len(),
        2,
        "rollup should record both contributing raw segments, got {:?}",
        rollup.input_segment_ids
    );
    assert!(rollup.input_segment_ids.contains(&raw_a));
    assert!(rollup.input_segment_ids.contains(&raw_b));
}

#[tokio::test]
async fn rollup_input_only_lists_segments_that_actually_contributed() {
    let root = tmp_root();
    let state = build_state(root.clone(), 2);
    let bucket = bucket_for_account(&AccountId("acc_q".into()), 2);

    // Two segments, but only one falls in the hour being rolled up.
    let h = 40 * HOUR_MS;
    let _raw_in_hour = commit_segment(
        &state,
        &[make_event("inh", "acc_q", h + 1, 100)],
        bucket,
    )
    .await;
    let raw_out_of_hour = commit_segment(
        &state,
        &[make_event("out", "acc_q", h + 2 * HOUR_MS, 999)],
        bucket,
    )
    .await;

    let worker = RollupWorker::new(state.clone(), 0, Duration::from_secs(30), i64::MAX);
    worker.tick(h + HOUR_MS + 1).await.unwrap();

    let manifest = state.manifest.read().await;
    let rollup = manifest
        .rollup_segments
        .iter()
        .find(|s| s.min_timestamp_ms == h)
        .expect("rollup for hour 40");

    assert_eq!(rollup.input_segment_ids.len(), 1);
    assert!(
        !rollup.input_segment_ids.contains(&raw_out_of_hour),
        "out-of-hour segment must not be listed as input; got {:?}",
        rollup.input_segment_ids
    );
}

#[tokio::test]
async fn explain_endpoint_surfaces_rollup_inputs_map() {
    let root = tmp_root();
    let state = build_state(root.clone(), 2);
    let bucket = bucket_for_account(&AccountId("acc_e".into()), 2);

    let h = 50 * HOUR_MS;
    let raw_a = commit_segment(
        &state,
        &[make_event("a", "acc_e", h + 1, 10)],
        bucket,
    )
    .await;
    let raw_b = commit_segment(
        &state,
        &[make_event("b", "acc_e", h + 2, 20)],
        bucket,
    )
    .await;

    let worker = RollupWorker::new(state.clone(), 0, Duration::from_secs(30), i64::MAX);
    worker.tick(h + HOUR_MS + 1).await.unwrap();

    let uri = "/v1/accounts/acc_e/explain?from=1970-01-01T00:00:00Z&to=2030-01-01T00:00:00Z";
    let (status, body) = get_json(state.clone(), uri).await;
    assert_eq!(status, StatusCode::OK);

    // rollup_segments has one entry; rollup_inputs has a map from that
    // ID to the contributing raw segment IDs.
    let rollup_segments = body["rollup_segments"].as_array().unwrap();
    assert_eq!(rollup_segments.len(), 1);
    let rollup_id = rollup_segments[0].as_str().unwrap();

    let inputs_map = body["rollup_inputs"].as_object().expect("rollup_inputs map");
    let inputs = inputs_map
        .get(rollup_id)
        .expect("inputs entry for the rollup")
        .as_array()
        .unwrap();
    let input_ids: Vec<&str> = inputs.iter().filter_map(|v| v.as_str()).collect();
    assert_eq!(input_ids.len(), 2);
    assert!(input_ids.contains(&raw_a.as_str()));
    assert!(input_ids.contains(&raw_b.as_str()));
}
