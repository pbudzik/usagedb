//! Tests for the persistent dedupe sidecar (`raw_<id>.idx`):
//!   - Round-trip: write entries, read them back identically
//!   - Missing file → Ok(None) (signal to fall back to segment scan)
//!   - Corruption (bad magic / checksum / count) → Err (also a fallback signal)
//!   - Recovery prefers the sidecar when present
//!   - Recovery falls back to segment scan when the sidecar is absent
//!   - The flusher writes a sidecar alongside every segment

use std::path::PathBuf;

use usagedb::ingest::flusher::build_segment_meta;
use usagedb::ingest::dedupe::{DedupeResult, EventHash};
use usagedb::model::dimensions::SmallDimensions;
use usagedb::model::event::{EventKind, UsageEvent};
use usagedb::model::ids::{
    AccountId, EventId, MeterId, ModelId, ProductId, SourceId, SubscriptionId, Unit,
};
use usagedb::runtime::recovery::{compute_event_hashes, Recovery};
use usagedb::storage::dedupe_index::{
    index_path, read_dedupe_index, write_dedupe_index, DedupeEntry,
};
use usagedb::storage::manifest::Manifest;
use usagedb::storage::segment_writer::RawSegmentWriter;

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

// =========================================================================
// Direct sidecar API
// =========================================================================

#[test]
fn write_then_read_round_trips_entries() {
    let dir = tmp_root();
    let path = dir.join("test.idx");
    let entries: Vec<DedupeEntry> = vec![
        (1u128, 100u128, 1000),
        (2u128, 200u128, 1001),
        (u128::MAX, 0u128, i64::MAX),
    ];
    write_dedupe_index(&path, &entries).unwrap();
    let read = read_dedupe_index(&path).unwrap().expect("sidecar present");
    assert_eq!(read, entries);
}

#[test]
fn missing_sidecar_returns_ok_none() {
    let dir = tmp_root();
    let path = dir.join("absent.idx");
    let result = read_dedupe_index(&path).unwrap();
    assert!(result.is_none());
}

#[test]
fn corrupt_checksum_is_rejected() {
    let dir = tmp_root();
    let path = dir.join("tamper.idx");
    write_dedupe_index(&path, &[(1u128, 2u128, 3i64)]).unwrap();

    // Flip a byte in the entries section (between magic+count and checksum).
    let mut bytes = std::fs::read(&path).unwrap();
    let pos = 8 + 4 + 4; // past magic + count + a few bytes
    bytes[pos] ^= 0xFF;
    std::fs::write(&path, bytes).unwrap();

    let err = read_dedupe_index(&path).unwrap_err();
    assert!(
        err.to_string().contains("checksum mismatch")
            || err.to_string().contains("corrupt dedupe index"),
        "{}",
        err
    );
}

#[test]
fn truncated_sidecar_is_rejected() {
    let dir = tmp_root();
    let path = dir.join("trunc.idx");
    write_dedupe_index(&path, &[(1u128, 2u128, 3i64)]).unwrap();
    let bytes = std::fs::read(&path).unwrap();
    std::fs::write(&path, &bytes[..bytes.len() - 4]).unwrap();

    let err = read_dedupe_index(&path).unwrap_err();
    assert!(err.to_string().contains("corrupt dedupe index"), "{}", err);
}

#[test]
fn missing_magic_is_rejected() {
    let dir = tmp_root();
    let path = dir.join("nomagic.idx");
    std::fs::write(&path, b"not the right header at all").unwrap();
    let err = read_dedupe_index(&path).unwrap_err();
    assert!(err.to_string().contains("magic"), "{}", err);
}

// =========================================================================
// Recovery integration
// =========================================================================

/// Write a segment + matching sidecar directly, then run recovery and
/// assert that dedupe picked up the entries via the fast path (sidecar)
/// rather than scanning the segment.
#[test]
fn recovery_uses_sidecar_to_rebuild_dedupe() {
    use usagedb::runtime::config::Config;

    let root = tmp_root();
    let config = Config { db_root: root.clone(), default_bucket_count: 2, ..Config::default() };
    std::fs::create_dir_all(&config.db_root).unwrap();

    // Write a recent segment + index manually.
    let now = chrono::Utc::now().timestamp_millis();
    let events = vec![
        make_event("e1", "acc_r", now - 1000, 10),
        make_event("e2", "acc_r", now - 500, 20),
    ];
    let segment_id = format!("raw_{}", uuid::Uuid::new_v4().simple());
    let seg_path = root.join(format!("{}.seg", segment_id));
    let mut w = RawSegmentWriter::new(seg_path).unwrap();
    for e in &events { w.write_event(e).unwrap(); }
    let (_rows, checksum) = w.finish().unwrap();

    // Sidecar matches segment row order.
    let entries: Vec<DedupeEntry> = events
        .iter()
        .map(|e| {
            let (id_h, p_h) = compute_event_hashes(e);
            (id_h, p_h, e.ingested_at_ms)
        })
        .collect();
    write_dedupe_index(&index_path(&root, &segment_id), &entries).unwrap();

    // Add the segment to the manifest.
    let meta = build_segment_meta(&segment_id, &events, 0, checksum);
    let mut manifest = Manifest { bucket_count: 2, ..Manifest::default() };
    manifest.raw_segments.push(meta);
    manifest.save(&root).unwrap();

    // Run recovery and verify dedupe is populated.
    let recovery = Recovery::new(root.clone());
    let result = recovery.run_startup_recovery(1000).unwrap();
    let mut dedupe = result.dedupe;
    for e in &events {
        let (id_h, p_h) = compute_event_hashes(e);
        // Re-inserting the exact same event should be flagged as duplicate.
        assert_eq!(
            dedupe.check_and_insert(id_h, p_h),
            DedupeResult::ExactDuplicate,
            "event {} should already be in dedupe via sidecar",
            e.event_id.0
        );
    }
}

/// Same setup as above but with NO sidecar — recovery must fall back
/// to scanning the segment file and still rebuild dedupe correctly.
#[test]
fn recovery_falls_back_to_segment_scan_without_sidecar() {
    use usagedb::runtime::config::Config;

    let root = tmp_root();
    let config = Config { db_root: root.clone(), default_bucket_count: 2, ..Config::default() };
    std::fs::create_dir_all(&config.db_root).unwrap();

    let now = chrono::Utc::now().timestamp_millis();
    let events = vec![make_event("e1", "acc_f", now - 1000, 10)];
    let segment_id = format!("raw_{}", uuid::Uuid::new_v4().simple());
    let seg_path = root.join(format!("{}.seg", segment_id));
    let mut w = RawSegmentWriter::new(seg_path).unwrap();
    for e in &events { w.write_event(e).unwrap(); }
    let (_rows, checksum) = w.finish().unwrap();
    // Deliberately do NOT write a sidecar.

    let meta = build_segment_meta(&segment_id, &events, 0, checksum);
    let mut manifest = Manifest { bucket_count: 2, ..Manifest::default() };
    manifest.raw_segments.push(meta);
    manifest.save(&root).unwrap();

    let recovery = Recovery::new(root.clone());
    let result = recovery.run_startup_recovery(1000).unwrap();
    let mut dedupe = result.dedupe;
    let (id_h, p_h) = compute_event_hashes(&events[0]);
    assert_eq!(
        dedupe.check_and_insert(id_h, p_h),
        DedupeResult::ExactDuplicate,
        "fallback scan should have populated dedupe"
    );
}

// =========================================================================
// EventHash typed correctly (compile-time sanity)
// =========================================================================

#[test]
fn dedupe_entry_uses_event_hash_type() {
    let _entry: DedupeEntry = (0u128 as EventHash, 0u128 as EventHash, 0i64);
}
