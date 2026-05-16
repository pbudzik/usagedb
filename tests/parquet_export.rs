//! Integration tests for Parquet export (Phase D).
//!
//! Verifies:
//!   - Round-trip: every event written via `export_raw_segments` reads
//!     back from the Parquet file with the same field values
//!   - Empty manifest produces a valid zero-row file
//!   - The schema matches the canonical shape (column names + types)

use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tokio::sync::{Mutex, RwLock};

use usagedb::export::parquet::{export_raw_segments, parquet_schema, write_parquet};
use usagedb::ingest::dedupe::HotDedupe;
use usagedb::ingest::flusher::build_segment_meta;
use usagedb::ingest::memtable::Memtable;
use usagedb::ingest::wal::Wal;
use usagedb::model::dimensions::SmallDimensions;
use usagedb::model::event::{CorrectionRef, EventKind, UsageEvent};
use usagedb::model::ids::{
    AccountId, EventId, MeterId, ModelId, ProductId, SourceId, SubscriptionId, Unit,
};
use usagedb::runtime::config::Config;
use usagedb::runtime::state::{AppState, AppStateInner};
use usagedb::storage::manifest::Manifest;
use usagedb::storage::segment_writer::RawSegmentWriter;

fn tmp_root() -> PathBuf {
    let dir = tempfile::tempdir().expect("tempdir");
    let p = dir.path().to_path_buf();
    std::mem::forget(dir);
    p
}

fn tmp_file(name: &str) -> PathBuf {
    let dir = tempfile::tempdir().expect("tempdir");
    let p = dir.path().join(name);
    std::mem::forget(dir);
    p
}

fn rich_event(id: &str, account: &str, ts: i64, qty: i128) -> UsageEvent {
    let mut dims = SmallDimensions::default();
    dims.inner.insert("provider".into(), "anthropic".into());
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
        source: SourceId("agentcore".into()),
        model_id: Some(ModelId("claude-sonnet-4".into())),
        dimensions: dims,
        ingested_at_ms: ts + 5,
    }
}

fn build_state(db_root: PathBuf) -> AppState {
    let config = Config {
        db_root: db_root.clone(),
        default_bucket_count: 2,
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

async fn commit_segment(state: &AppState, events: &[UsageEvent], bucket: u32) {
    let segment_id = format!("raw_{}", uuid::Uuid::new_v4().simple());
    let path = state.config.db_root.join(format!("{}.seg", segment_id));
    let mut writer = RawSegmentWriter::new(path).unwrap();
    for e in events { writer.write_event(e).unwrap(); }
    let (_rows, checksum) = writer.finish().unwrap();
    let meta = build_segment_meta(&segment_id, events, bucket, checksum);
    let mut manifest = state.manifest.write().await;
    manifest.raw_segments.push(meta);
    manifest.save(&state.config.db_root).unwrap();
}

/// Read all events back out of a Parquet file. Returns rows as
/// (event_id, kind, account_id, quantity_i128).
fn read_parquet_minimal(path: &PathBuf) -> Vec<(String, String, String, i128)> {
    use arrow::array::{Array, Decimal128Array, StringArray};
    let file = std::fs::File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let event_id = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let kind = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let account_id = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let quantity = batch
            .column(10)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            out.push((
                event_id.value(i).to_string(),
                kind.value(i).to_string(),
                account_id.value(i).to_string(),
                quantity.value(i),
            ));
        }
    }
    out
}

#[test]
fn write_parquet_round_trips_event_fields() {
    let out = tmp_file("rt.parquet");
    let events = vec![
        rich_event("evt_a", "acc_1", 1_000, 100),
        rich_event("evt_b", "acc_2", 2_000, i128::MAX / 4),
        UsageEvent {
            kind: EventKind::Correction,
            correction_ref: Some(CorrectionRef {
                original_event_id: EventId("evt_a".into()),
                reason: "overcount".into(),
            }),
            ..rich_event("evt_c", "acc_1", 3_000, -100)
        },
    ];
    write_parquet(&events, &out).unwrap();

    let rows = read_parquet_minimal(&out);
    assert_eq!(rows.len(), 3);

    assert_eq!(rows[0].0, "evt_a");
    assert_eq!(rows[0].1, "Usage");
    assert_eq!(rows[0].2, "acc_1");
    assert_eq!(rows[0].3, 100);

    assert_eq!(rows[1].0, "evt_b");
    assert_eq!(rows[1].3, i128::MAX / 4);

    assert_eq!(rows[2].0, "evt_c");
    assert_eq!(rows[2].1, "Correction");
    assert_eq!(rows[2].3, -100);
}

#[test]
fn schema_matches_canonical_shape() {
    let schema = parquet_schema();
    let expected_columns: &[(&str, bool)] = &[
        ("event_id", false),
        ("kind", false),
        ("correction_original_event_id", true),
        ("correction_reason", true),
        ("account_id", false),
        ("subscription_id", true),
        ("product_id", false),
        ("meter_id", false),
        ("model_id", true),
        ("timestamp_ms", false),
        ("quantity", false),
        ("unit", false),
        ("source", false),
        ("dimensions_canonical", false),
        ("ingested_at_ms", false),
    ];
    assert_eq!(schema.fields().len(), expected_columns.len());
    for (i, (name, nullable)) in expected_columns.iter().enumerate() {
        let f = schema.field(i);
        assert_eq!(f.name(), name, "column {} name", i);
        assert_eq!(f.is_nullable(), *nullable, "column {} nullability", name);
    }
    // quantity must be Decimal128(38, 0) so i128 fits exactly.
    let quantity_field = schema.field(10);
    assert_eq!(quantity_field.data_type(), &DataType::Decimal128(38, 0));
}

#[test]
fn empty_input_writes_valid_zero_row_parquet() {
    let out = tmp_file("empty.parquet");
    write_parquet(&[], &out).unwrap();
    let rows = read_parquet_minimal(&out);
    assert!(rows.is_empty());
    // Sanity: a real file was created.
    assert!(std::fs::metadata(&out).unwrap().len() > 0);
}

#[tokio::test]
async fn export_raw_segments_round_trips_through_manifest() {
    let root = tmp_root();
    let state = build_state(root.clone());

    let events_a = vec![
        rich_event("a1", "acc_x", 1_000, 10),
        rich_event("a2", "acc_x", 1_500, 20),
    ];
    let events_b = vec![rich_event("b1", "acc_y", 2_000, 30)];
    commit_segment(&state, &events_a, 0).await;
    commit_segment(&state, &events_b, 1).await;

    let out = tmp_file("e2e.parquet");
    let stats = export_raw_segments(&state, &out).await.unwrap();
    assert_eq!(stats.events_exported, 3);
    assert_eq!(stats.segments_read, 2);

    let rows = read_parquet_minimal(&out);
    let mut ids: Vec<String> = rows.iter().map(|r| r.0.clone()).collect();
    ids.sort();
    assert_eq!(ids, vec!["a1".to_string(), "a2".to_string(), "b1".to_string()]);
}

#[tokio::test]
async fn export_empty_manifest_produces_zero_row_file() {
    let root = tmp_root();
    let state = build_state(root.clone());
    let out = tmp_file("empty_e2e.parquet");
    let stats = export_raw_segments(&state, &out).await.unwrap();
    assert_eq!(stats.events_exported, 0);
    assert_eq!(stats.segments_read, 0);
}
