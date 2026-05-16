//! Integration tests for the columnar segment format.
//!
//! Covers:
//!   - Round-trip: events written then read back match exactly
//!   - Magic / version / checksum validation on read
//!   - Empty segment (zero rows) is a valid file
//!   - zstd actually compresses a repetitive dataset (sanity check that the
//!     codec is plumbed end-to-end)

use std::path::PathBuf;
use usagedb::model::dimensions::SmallDimensions;
use usagedb::model::event::{CorrectionRef, EventKind, UsageEvent};
use usagedb::model::ids::{
    AccountId, EventId, MeterId, ModelId, ProductId, SourceId, SubscriptionId, Unit,
};
use usagedb::storage::segment_reader::RawSegmentReader;
use usagedb::storage::segment_writer::RawSegmentWriter;

fn tmp_path(name: &str) -> PathBuf {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join(name);
    std::mem::forget(dir);
    path
}

fn rich_event(id: &str, account: &str, ts: i64, qty: i128) -> UsageEvent {
    let mut dims = SmallDimensions::default();
    dims.inner.insert("provider".into(), "anthropic".into());
    dims.inner.insert("agent".into(), "support".into());

    UsageEvent {
        event_id: EventId(id.to_string()),
        kind: EventKind::Usage,
        correction_ref: None,
        account_id: AccountId(account.to_string()),
        subscription_id: Some(SubscriptionId("sub_42".into())),
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

#[test]
fn round_trip_preserves_all_fields() {
    let path = tmp_path("rt.seg");

    let events = vec![
        rich_event("evt_a", "acc_1", 1_000, 100),
        UsageEvent {
            // Exercise the Correction variant + null subscription_id / model_id.
            kind: EventKind::Correction,
            correction_ref: Some(CorrectionRef {
                original_event_id: EventId("evt_a".into()),
                reason: "double-counted".into(),
            }),
            subscription_id: None,
            model_id: None,
            ..rich_event("evt_b", "acc_2", 2_000, -50)
        },
        rich_event("evt_c", "acc_1", 3_000, i128::MAX / 2),
    ];

    let mut writer = RawSegmentWriter::new(path.clone()).unwrap();
    for e in &events {
        writer.write_event(e).unwrap();
    }
    let (rows, _checksum) = writer.finish().unwrap();
    assert_eq!(rows, events.len() as u64);

    let mut reader = RawSegmentReader::new(path).unwrap();
    assert_eq!(reader.row_count(), events.len());
    let mut read_back = Vec::new();
    while let Some(e) = reader.read_next().unwrap() {
        read_back.push(e);
    }
    assert_eq!(read_back.len(), events.len());
    for (orig, got) in events.iter().zip(read_back.iter()) {
        assert_eq!(orig, got, "round-trip event must equal original");
    }
}

#[test]
fn empty_segment_is_valid() {
    let path = tmp_path("empty.seg");
    let writer = RawSegmentWriter::new(path.clone()).unwrap();
    let (rows, _checksum) = writer.finish().unwrap();
    assert_eq!(rows, 0);

    let mut reader = RawSegmentReader::new(path).unwrap();
    assert_eq!(reader.row_count(), 0);
    assert!(reader.read_next().unwrap().is_none());
}

#[test]
fn checksum_mismatch_is_rejected() {
    let path = tmp_path("tamper.seg");
    let mut writer = RawSegmentWriter::new(path.clone()).unwrap();
    writer.write_event(&rich_event("evt_a", "acc_1", 1_000, 100)).unwrap();
    writer.finish().unwrap();

    // Flip a byte somewhere inside the body (after the start magic, well
    // before the checksum). This must invalidate either the checksum or
    // the column payload — either way `new()` should refuse to open it.
    let mut bytes = std::fs::read(&path).unwrap();
    let tamper_off = 64.min(bytes.len() / 2);
    bytes[tamper_off] ^= 0xFF;
    std::fs::write(&path, bytes).unwrap();

    let result = RawSegmentReader::new(path);
    assert!(result.is_err(), "tampered segment must be rejected");
}

#[test]
fn missing_end_magic_is_rejected() {
    let path = tmp_path("truncated.seg");
    let mut writer = RawSegmentWriter::new(path.clone()).unwrap();
    writer.write_event(&rich_event("evt", "acc", 1, 1)).unwrap();
    writer.finish().unwrap();

    // Truncate the last few bytes to remove the end magic.
    let bytes = std::fs::read(&path).unwrap();
    std::fs::write(&path, &bytes[..bytes.len() - 4]).unwrap();

    let result = RawSegmentReader::new(path);
    assert!(result.is_err(), "truncated segment must be rejected");
}

#[test]
fn compression_reduces_size_on_repetitive_data() {
    // 10k near-identical events should compress to a fraction of their raw
    // bincode size. This catches a regression where the codec byte gets
    // set to None and we silently store raw bytes.
    let path = tmp_path("compress.seg");
    let mut writer = RawSegmentWriter::new(path.clone()).unwrap();
    for i in 0..10_000 {
        writer.write_event(&rich_event(&format!("evt_{i}"), "acc_constant", 1_000 + i, 100)).unwrap();
    }
    writer.finish().unwrap();

    let on_disk = std::fs::metadata(&path).unwrap().len();
    // A row is roughly 200 bytes of payload uncompressed. 10k rows ≈ 2 MB.
    // Even modest zstd compression on repetitive strings should easily hit
    // 4× → under 500 KB. Bound is loose to survive zstd version drift.
    assert!(
        on_disk < 500_000,
        "expected zstd to compress 10k repetitive events under 500 KB, got {} bytes",
        on_disk
    );
}
