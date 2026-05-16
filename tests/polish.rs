//! Tests for the operational polish bundle:
//!   - DbLock prevents concurrent access on the same db_root
//!   - Sort-on-flush writes segments in the canonical billing order
//!   - RLE for `kind` round-trips + dramatically shrinks low-cardinality columns

use std::path::PathBuf;

use usagedb::admin::open_state_for_admin;
use usagedb::ingest::flusher::sort_events_canonical;
use usagedb::model::dimensions::SmallDimensions;
use usagedb::model::event::{EventKind, UsageEvent};
use usagedb::model::ids::{
    AccountId, EventId, MeterId, ModelId, ProductId, SourceId, SubscriptionId, Unit,
};
use usagedb::runtime::config::Config;
use usagedb::runtime::lock::DbLock;
use usagedb::storage::manifest::Manifest;
use usagedb::storage::segment_reader::RawSegmentReader;
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

// =========================================================================
// DbLock
// =========================================================================

#[test]
fn db_lock_prevents_concurrent_acquisition() {
    let root = tmp_root();
    let lock_a = DbLock::acquire(&root).expect("first lock");
    let err = match DbLock::acquire(&root) {
        Ok(_) => panic!("second lock should have failed"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("holds the lock"),
        "error should mention the lock: {}",
        err
    );
    drop(lock_a);
    // After releasing, a fresh acquisition should succeed.
    let _lock_b = DbLock::acquire(&root).expect("re-acquire after drop");
}

#[tokio::test]
async fn open_state_for_admin_blocks_second_caller() {
    let root = tmp_root();
    std::fs::create_dir_all(&root).unwrap();
    // Bootstrap a manifest so open_state_for_admin gets past the
    // manifest-load step and hits the lock.
    Manifest::default().save(&root).unwrap();

    let config = Config { db_root: root.clone(), ..Config::default() };
    let (_state_a, _lock_a) = open_state_for_admin(config.clone()).expect("first");
    let err = match open_state_for_admin(config) {
        Ok(_) => panic!("concurrent open should fail"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("holds the lock"), "{}", err);
}

// =========================================================================
// Sort-on-flush
// =========================================================================

/// Verify the sort algorithm directly. The flusher's `write_bucket`
/// calls `sort_events_canonical` before writing, so a working sort
/// algorithm plus the writer test in tests/encodings.rs together
/// cover the integration.
#[test]
fn sort_events_canonical_orders_by_billing_dimensions() {
    let mut events = vec![
        // Same account, different timestamps + meters.
        make_event("e1", "acc_b", 100, 1),
        make_event("e2", "acc_a", 200, 1),
        make_event("e3", "acc_a", 100, 1),
        make_event("e4", "acc_b", 50, 1),
    ];
    sort_events_canonical(&mut events);
    let order: Vec<(String, i64)> = events
        .iter()
        .map(|e| (e.account_id.0.clone(), e.timestamp_ms))
        .collect();
    assert_eq!(
        order,
        vec![
            ("acc_a".to_string(), 100),
            ("acc_a".to_string(), 200),
            ("acc_b".to_string(), 50),
            ("acc_b".to_string(), 100),
        ],
    );
}

#[test]
fn sort_events_canonical_breaks_ties_by_meter_then_model() {
    let mut events = vec![
        UsageEvent { meter_id: MeterId("z".into()), ..make_event("a", "acc", 100, 1) },
        UsageEvent { meter_id: MeterId("a".into()), ..make_event("b", "acc", 100, 1) },
        UsageEvent {
            meter_id: MeterId("a".into()),
            model_id: Some(ModelId("z_model".into())),
            ..make_event("c", "acc", 100, 1)
        },
    ];
    sort_events_canonical(&mut events);
    let order: Vec<&str> = events.iter().map(|e| e.event_id.0.as_str()).collect();
    // meter "a" before "z"; within meter "a", model "m1" before "z_model".
    assert_eq!(order, vec!["b", "c", "a"]);
}

/// End-to-end: write events into a segment via the writer in
/// not-yet-sorted order, but use sort_events_canonical first (mirroring
/// the flusher path). The reader should see them in canonical order.
#[test]
fn writer_round_trip_preserves_sorted_order() {
    let path = tmp_file("sorted.seg");
    let mut events = vec![
        make_event("e1", "acc_b", 100, 1),
        make_event("e2", "acc_a", 200, 1),
        make_event("e3", "acc_a", 100, 1),
    ];
    sort_events_canonical(&mut events);

    let mut writer = RawSegmentWriter::new(path.clone()).unwrap();
    for e in &events {
        writer.write_event(e).unwrap();
    }
    writer.finish().unwrap();

    let mut reader = RawSegmentReader::new(path).unwrap();
    let mut read_back = Vec::new();
    while let Some(e) = reader.read_next().unwrap() {
        read_back.push((e.account_id.0, e.timestamp_ms));
    }
    assert_eq!(
        read_back,
        vec![
            ("acc_a".to_string(), 100),
            ("acc_a".to_string(), 200),
            ("acc_b".to_string(), 100),
        ],
    );
}

// =========================================================================
// RLE for `kind`
// =========================================================================

#[test]
fn rle_encoding_round_trips_mixed_kinds() {
    let out = tmp_file("rle_rt.seg");
    let mut writer = RawSegmentWriter::new(out.clone()).unwrap();
    // 100 Usage + 50 Correction + 100 Usage + 1 Retraction
    let mut events = Vec::new();
    for i in 0..100 {
        events.push(make_event(&format!("u1_{i}"), "acc", i, 10));
    }
    for i in 0..50 {
        let mut e = make_event(&format!("c_{i}"), "acc", 1000 + i, -1);
        e.kind = EventKind::Correction;
        e.correction_ref = Some(usagedb::model::event::CorrectionRef {
            original_event_id: EventId(format!("u1_{}", i)),
            reason: "test".into(),
        });
        events.push(e);
    }
    for i in 0..100 {
        events.push(make_event(&format!("u2_{i}"), "acc", 2000 + i, 10));
    }
    let mut last = make_event("r", "acc", 5000, 0);
    last.kind = EventKind::Retraction;
    last.correction_ref = Some(usagedb::model::event::CorrectionRef {
        original_event_id: EventId("u2_0".into()),
        reason: "retract".into(),
    });
    events.push(last);

    for e in &events {
        writer.write_event(e).unwrap();
    }
    writer.finish().unwrap();

    let mut reader = RawSegmentReader::new(out).unwrap();
    let mut idx = 0;
    while let Some(e) = reader.read_next().unwrap() {
        assert_eq!(e.kind, events[idx].kind, "row {}", idx);
        assert_eq!(e.event_id, events[idx].event_id);
        idx += 1;
    }
    assert_eq!(idx, events.len());
}

#[test]
fn rle_kind_round_trips_all_usage() {
    // The common case: every row is `Usage`. RLE collapses to one run.
    let out = tmp_file("rle_all_usage.seg");
    let mut writer = RawSegmentWriter::new(out.clone()).unwrap();
    for i in 0..1000 {
        writer
            .write_event(&make_event(&format!("u_{i}"), "acc", i, 1))
            .unwrap();
    }
    writer.finish().unwrap();

    let mut reader = RawSegmentReader::new(out).unwrap();
    let mut count = 0;
    while let Some(e) = reader.read_next().unwrap() {
        assert_eq!(e.kind, EventKind::Usage);
        count += 1;
    }
    assert_eq!(count, 1000);
}

#[test]
fn rle_kind_round_trips_all_corrections() {
    // Pathological: every row is Correction. RLE still works.
    let out = tmp_file("rle_all_corr.seg");
    let mut writer = RawSegmentWriter::new(out.clone()).unwrap();
    for i in 0..100 {
        let mut e = make_event(&format!("c_{i}"), "acc", i, -1);
        e.kind = EventKind::Correction;
        e.correction_ref = Some(usagedb::model::event::CorrectionRef {
            original_event_id: EventId(format!("o_{}", i)),
            reason: "x".into(),
        });
        writer.write_event(&e).unwrap();
    }
    writer.finish().unwrap();

    let mut reader = RawSegmentReader::new(out).unwrap();
    let mut count = 0;
    while let Some(e) = reader.read_next().unwrap() {
        assert_eq!(e.kind, EventKind::Correction);
        count += 1;
    }
    assert_eq!(count, 100);
}
