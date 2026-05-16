//! Integration tests for the durability + recovery contract:
//!   - WAL append-then-fsync precedes any dedupe mutation
//!   - WAL rotation seals files and ties them to a segment commit
//!   - Recovery rebuilds the memtable from unflushed WAL files
//!   - Recovery does NOT re-flush events already in committed segments

use std::path::PathBuf;
use usagedb::ingest::dedupe::HotDedupe;
use usagedb::ingest::memtable::Memtable;
use usagedb::ingest::wal::Wal;
use usagedb::model::dimensions::SmallDimensions;
use usagedb::model::event::{EventKind, UsageEvent};
use usagedb::model::ids::{
    AccountId, EventId, MeterId, ModelId, ProductId, SourceId, SubscriptionId, Unit,
};
use usagedb::runtime::recovery::{compute_event_hashes, Recovery};
use usagedb::storage::manifest::Manifest;

fn make_event(id: &str, account: &str, ts: i64, qty: i128) -> UsageEvent {
    UsageEvent {
        event_id: EventId(id.to_string()),
        kind: EventKind::Usage,
        correction_ref: None,
        account_id: AccountId(account.to_string()),
        subscription_id: Some(SubscriptionId("sub_1".to_string())),
        product_id: ProductId("ai_gateway".to_string()),
        meter_id: MeterId("tokens.input".to_string()),
        timestamp_ms: ts,
        quantity: qty,
        unit: Unit("token".to_string()),
        source: SourceId("test".to_string()),
        model_id: Some(ModelId("claude-sonnet-4".to_string())),
        dimensions: SmallDimensions::default(),
        ingested_at_ms: ts,
    }
}

fn tmp_root() -> PathBuf {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    std::mem::forget(dir); // tests are short-lived; let the OS clean up
    path
}

#[test]
fn wal_rotate_seals_active_file_and_starts_new_one() {
    let root = tmp_root();
    let wal_dir = root.join("wal");
    let mut wal = Wal::open(wal_dir.clone(), 0).unwrap();
    let starting_id = wal.active_id;

    let events = vec![make_event("evt1", "acc_1", 1000, 100)];
    wal.append_batch(&events).unwrap();
    wal.sync().unwrap();

    let sealed_id = wal.rotate().unwrap();
    assert_eq!(sealed_id, starting_id);
    assert_eq!(wal.active_id, starting_id + 1);

    // The sealed file still exists on disk until delete_files_through runs.
    let ids = Wal::list_files_after(&wal_dir, 0).unwrap();
    assert!(ids.contains(&sealed_id), "sealed file should still be on disk");
    assert!(ids.contains(&wal.active_id), "active file should be on disk");
}

#[test]
fn delete_files_through_removes_sealed_only() {
    let root = tmp_root();
    let wal_dir = root.join("wal");
    let mut wal = Wal::open(wal_dir.clone(), 0).unwrap();
    let id1 = wal.active_id;
    wal.append_batch(&[make_event("e1", "a", 1, 1)]).unwrap();
    wal.sync().unwrap();
    let sealed = wal.rotate().unwrap();
    assert_eq!(sealed, id1);
    let id2 = wal.active_id;
    wal.append_batch(&[make_event("e2", "a", 2, 1)]).unwrap();
    wal.sync().unwrap();

    Wal::delete_files_through(&wal_dir, sealed).unwrap();

    let remaining = Wal::list_files_after(&wal_dir, 0).unwrap();
    assert!(!remaining.contains(&id1), "sealed file should be deleted");
    assert!(remaining.contains(&id2), "active file must be kept");
}

#[test]
fn recovery_replays_unsealed_files_into_memtable_and_dedupe() {
    let root = tmp_root();
    let wal_dir = root.join("wal");

    // Simulate the on-disk state after some ingest + a crash before flush.
    {
        let mut wal = Wal::open(wal_dir.clone(), 0).unwrap();
        let events: Vec<UsageEvent> = (0..5)
            .map(|i| make_event(&format!("evt_{i}"), "acc_x", 1000 + i, 10))
            .collect();
        wal.append_batch(&events).unwrap();
        wal.sync().unwrap();
        // No manifest written, no rotation: simulates a crash mid-buffer.
    }

    let recovery = Recovery::new(root.clone());
    let result = recovery.run_startup_recovery(10_000).unwrap();

    // Memtable should have all 5 events back.
    assert_eq!(result.memtable.len(), 5, "all WAL events should be in memtable");

    // Dedupe should now flag retries as ExactDuplicate.
    let dup_event = make_event("evt_0", "acc_x", 1000, 10);
    let (id_hash, payload_hash) = compute_event_hashes(&dup_event);
    let mut dedupe = result.dedupe;
    assert_eq!(
        dedupe.check_and_insert(id_hash, payload_hash),
        usagedb::ingest::dedupe::DedupeResult::ExactDuplicate
    );
}

#[test]
fn recovery_skips_sealed_files_below_watermark() {
    let root = tmp_root();
    let wal_dir = root.join("wal");

    // Write events to wal-000001, rotate (sealing it), write to wal-000002.
    {
        let mut wal = Wal::open(wal_dir.clone(), 0).unwrap();
        wal.append_batch(&[make_event("sealed", "a", 1, 1)]).unwrap();
        wal.sync().unwrap();
        let _sealed = wal.rotate().unwrap();
        wal.append_batch(&[make_event("active", "a", 2, 1)]).unwrap();
        wal.sync().unwrap();
    }

    // Manifest claims wal-000001 is durable in a segment.
    let manifest = Manifest {
        last_sealed_wal_id: 1,
        ..Manifest::default()
    };
    let manifest_json = serde_json::to_string_pretty(&manifest).unwrap();
    std::fs::write(root.join("manifest.json"), manifest_json).unwrap();

    let recovery = Recovery::new(root.clone());
    let result = recovery.run_startup_recovery(10_000).unwrap();

    // Only the active file (with "active") should have replayed.
    assert_eq!(
        result.memtable.len(),
        1,
        "sealed wal-000001 contents must NOT be replayed (already in segment)"
    );

    // The sealed file should also be cleaned up off disk by recovery.
    let remaining = Wal::list_files_after(&wal_dir, 0).unwrap();
    assert!(!remaining.contains(&1));
    assert!(remaining.contains(&2));
}

#[test]
fn dedupe_classify_does_not_mutate() {
    let mut dedupe = HotDedupe::new(100);
    let (h, p) = (123u64, 456u64);
    // Classify alone should NOT register the event.
    assert_eq!(dedupe.classify(h, p), usagedb::ingest::dedupe::DedupeResult::NewEvent);
    assert_eq!(dedupe.classify(h, p), usagedb::ingest::dedupe::DedupeResult::NewEvent);

    // Only commit actually inserts.
    dedupe.commit(h, p);
    assert_eq!(
        dedupe.classify(h, p),
        usagedb::ingest::dedupe::DedupeResult::ExactDuplicate
    );
}

#[test]
fn memtable_snapshot_preserves_state() {
    let mut mt = Memtable::new();
    mt.insert(make_event("e1", "a", 1, 10));
    mt.insert(make_event("e2", "a", 2, 20));

    let snap = mt.snapshot();
    assert_eq!(snap.len(), 2);
    // Snapshot is a clone, so the memtable still has events.
    assert_eq!(mt.len(), 2);
}
