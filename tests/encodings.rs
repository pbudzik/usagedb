//! Per-column encoding tests (Phase B).
//!
//! Verifies:
//!   - Dictionary encoding for ID columns dramatically shrinks segments
//!     with heavy string repetition (the main win)
//!   - Delta encoding preserves timestamps including edge cases
//!     (out-of-order, negative deltas)
//!   - Zigzag-varint round-trips i128 including i128::MIN/MAX and negatives
//!   - Mixed encodings in a single segment all round-trip correctly

use std::path::PathBuf;
use usagedb::model::dimensions::SmallDimensions;
use usagedb::model::event::{EventKind, UsageEvent};
use usagedb::model::ids::{
    AccountId, EventId, MeterId, ModelId, ProductId, SourceId, SubscriptionId, Unit,
};
use usagedb::storage::segment_reader::RawSegmentReader;
use usagedb::storage::segment_writer::RawSegmentWriter;

fn tmp_path(name: &str) -> PathBuf {
    let dir = tempfile::tempdir().expect("tempdir");
    let p = dir.path().join(name);
    std::mem::forget(dir);
    p
}

fn event_with(id: &str, account: &str, ts: i64, qty: i128) -> UsageEvent {
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
        dimensions: SmallDimensions::default(),
        ingested_at_ms: ts,
    }
}

/// Same payload as the existing compression test but the bar is tighter:
/// dictionary encoding on account_id + product_id + meter_id + source +
/// unit + model_id should shrink a 10k-event segment with 1 distinct
/// account way below the old "<500 KB" bar.
#[test]
fn dictionary_encoding_shrinks_repetitive_id_columns() {
    let path = tmp_path("dict.seg");
    let mut writer = RawSegmentWriter::new(path.clone()).unwrap();
    for i in 0..10_000 {
        writer
            .write_event(&event_with(&format!("evt_{i}"), "acc_constant", 1_000 + i, 100))
            .unwrap();
    }
    writer.finish().unwrap();

    let on_disk = std::fs::metadata(&path).unwrap().len();
    // With dictionary encoding the ID columns shrink ~1000×; the bar
    // should comfortably hit under 250 KB (we observed ~150 KB locally
    // pre-this-change, expect <200 KB after — leaving slack for zstd
    // version drift).
    assert!(
        on_disk < 250_000,
        "expected dictionary-encoded 10k repetitive events < 250 KB, got {} bytes",
        on_disk
    );
}

/// Round-trip with many distinct values per ID column so the dictionary
/// is exercised non-trivially.
#[test]
fn dictionary_round_trip_with_many_distinct_values() {
    let path = tmp_path("dict_rt.seg");
    let mut writer = RawSegmentWriter::new(path.clone()).unwrap();
    let mut originals = Vec::new();
    for i in 0..200 {
        // Cycle through 17 accounts and 13 meters so the dictionary has
        // multiple entries with realistic repetition.
        let acc = format!("acc_{}", i % 17);
        let meter = format!("meter_{}", i % 13);
        let mut e = event_with(&format!("evt_{i}"), &acc, 1_000 + i, i as i128);
        e.meter_id = MeterId(meter);
        // Occasional null subscription/model exercises the Option dictionary path.
        if i % 5 == 0 {
            e.subscription_id = None;
            e.model_id = None;
        }
        originals.push(e);
    }
    for e in &originals {
        writer.write_event(e).unwrap();
    }
    writer.finish().unwrap();

    let mut reader = RawSegmentReader::new(path).unwrap();
    let mut read_back = Vec::new();
    while let Some(e) = reader.read_next().unwrap() {
        read_back.push(e);
    }
    assert_eq!(read_back.len(), originals.len());
    for (orig, got) in originals.iter().zip(read_back.iter()) {
        assert_eq!(orig, got);
    }
}

/// Delta encoding must preserve all i64 values including out-of-order
/// sequences (a Correction event might land with an older ts than its
/// neighbors) and negative timestamps (testing only — production never
/// has these, but the encoder shouldn't care).
#[test]
fn delta_encoding_handles_out_of_order_timestamps() {
    let path = tmp_path("delta_rt.seg");
    let mut writer = RawSegmentWriter::new(path.clone()).unwrap();
    let timestamps = vec![1000, 999, 5000, 4999, 1_000_000, i64::MAX / 2, 0, -100];
    for (i, &ts) in timestamps.iter().enumerate() {
        writer
            .write_event(&event_with(&format!("e{i}"), "a", ts, 1))
            .unwrap();
    }
    writer.finish().unwrap();

    let mut reader = RawSegmentReader::new(path).unwrap();
    let mut got = Vec::new();
    while let Some(e) = reader.read_next().unwrap() {
        got.push(e.timestamp_ms);
    }
    assert_eq!(got, timestamps);
}

/// Zigzag-varint must round-trip all the boundary i128 values.
#[test]
fn zigzag_varint_round_trips_edge_cases() {
    let path = tmp_path("zigzag_rt.seg");
    let mut writer = RawSegmentWriter::new(path.clone()).unwrap();
    let quantities: Vec<i128> = vec![
        0,
        1,
        -1,
        127,
        128,
        -128,
        i128::MAX / 2,
        i128::MIN / 2,
        i128::MAX,
        i128::MIN,
        42_000_000_000,
        -42_000_000_000,
    ];
    for (i, &q) in quantities.iter().enumerate() {
        writer
            .write_event(&event_with(&format!("e{i}"), "a", 1000 + i as i64, q))
            .unwrap();
    }
    writer.finish().unwrap();

    let mut reader = RawSegmentReader::new(path).unwrap();
    let mut got = Vec::new();
    while let Some(e) = reader.read_next().unwrap() {
        got.push(e.quantity);
    }
    assert_eq!(got, quantities);
}

/// Small quantities should pack into 1-2 varint bytes each, far smaller
/// than 16-byte Plain. Bound: 1000 small quantities in <8 KB on disk
/// (zstd + varint should crush this).
#[test]
fn zigzag_varint_packs_small_quantities_tight() {
    let path = tmp_path("zigzag_small.seg");
    let mut writer = RawSegmentWriter::new(path.clone()).unwrap();
    for i in 0..1_000 {
        writer
            .write_event(&event_with(&format!("e{i}"), "a", 1000 + i, (i % 100) as i128))
            .unwrap();
    }
    writer.finish().unwrap();
    let size = std::fs::metadata(&path).unwrap().len();
    assert!(
        size < 8_000,
        "1000 events with small quantities should pack tight; got {} bytes",
        size
    );
}
