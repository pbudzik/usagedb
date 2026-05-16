//! Integration tests for the compaction scheduler.
//!
//! Drives `tick(now_ms)` directly so tests don't depend on wall clock.
//! Covers:
//!   - tick merges per-bucket plans into a single output segment
//!   - manifest swap is atomic (raw_segments loses inputs, gains output;
//!     compacted_replacements grows)
//!   - re-tick on the post-compaction manifest is a no-op
//!   - old files survive the grace window, then get deleted on the next
//!     post-grace tick
//!   - the compacted segment is fully readable and contains every event
//!     from the inputs (event_id dedup applied)

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, RwLock};
use usagedb::compact::worker::CompactionWorker;
use usagedb::ingest::dedupe::HotDedupe;
use usagedb::ingest::flusher::build_segment_meta;
use usagedb::ingest::memtable::Memtable;
use usagedb::ingest::wal::Wal;
use usagedb::model::dimensions::SmallDimensions;
use usagedb::model::event::{EventKind, UsageEvent};
use usagedb::model::ids::{
    AccountId, EventId, MeterId, ModelId, ProductId, SourceId, SubscriptionId, Unit,
};
use usagedb::runtime::config::Config;
use usagedb::runtime::state::{AppState, AppStateInner};
use usagedb::storage::manifest::Manifest;
use usagedb::storage::segment_reader::RawSegmentReader;
use usagedb::storage::segment_writer::RawSegmentWriter;

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
        model_id: Some(ModelId("claude-sonnet-4".into())),
        dimensions: SmallDimensions::default(),
        ingested_at_ms: ts,
    }
}

fn tmp_root() -> PathBuf {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    std::mem::forget(dir);
    path
}

fn build_state(db_root: PathBuf, bucket_count: u32) -> AppState {
    let config = Config {
        db_root: db_root.clone(),
        default_bucket_count: bucket_count,
        ..Config::default()
    };
    std::fs::create_dir_all(&config.db_root).unwrap();
    let wal = Wal::open(db_root.join("wal"), 0).unwrap();
    let manifest = Manifest {
        bucket_count,
        ..Manifest::default()
    };
    let (flush_sender, _flush_receiver) = tokio::sync::mpsc::channel(4);
    Arc::new(AppStateInner {
        config,
        dedupe: Mutex::new(HotDedupe::new(1000)),
        wal: Mutex::new(wal),
        memtable: Mutex::new(Memtable::new()),
        manifest: RwLock::new(manifest),
        flush_sender,
    })
}

/// Write a raw segment with the given events at the given bucket, and add
/// its meta to the manifest. Returns the segment ID.
async fn commit_segment(state: &AppState, events: &[UsageEvent], bucket: u32) -> String {
    let segment_id = format!("raw_{}", uuid::Uuid::new_v4().simple());
    let path = state.config.db_root.join(format!("{}.seg", segment_id));
    let mut writer = RawSegmentWriter::new(path).unwrap();
    for e in events {
        writer.write_event(e).unwrap();
    }
    let (_rows, checksum) = writer.finish().unwrap();
    let meta = build_segment_meta(&segment_id, events, bucket, checksum);
    let mut manifest = state.manifest.write().await;
    manifest.raw_segments.push(meta);
    manifest.save(&state.config.db_root).unwrap();
    segment_id
}

#[tokio::test]
async fn tick_merges_small_segments_in_a_bucket() {
    let root = tmp_root();
    let state = build_state(root.clone(), 4);

    // Threshold is "> max_small_segments", so 3 segments with max=2 trips it.
    let mut original_ids = Vec::new();
    for i in 0..3 {
        let event = make_event(&format!("e_{i}"), "acc_a", 1000 + i, 10);
        let id = commit_segment(&state, &[event], 1).await;
        original_ids.push(id);
    }

    let worker = CompactionWorker::new(state.clone(), 2, 30_000, Duration::from_secs(60));
    let stats = worker.tick(1_000_000).await.unwrap();
    assert_eq!(stats.compactions_committed, 1);

    let manifest = state.manifest.read().await;
    assert_eq!(manifest.raw_segments.len(), 1, "3 inputs → 1 output");
    let output = &manifest.raw_segments[0];
    assert!(output.segment_id.starts_with("compacted_"));
    assert_eq!(output.bucket, 1);
    assert_eq!(output.row_count, 3);

    assert_eq!(manifest.compacted_replacements.len(), 1);
    let rec = &manifest.compacted_replacements[0];
    assert_eq!(rec.old_segments.len(), 3);
    for id in &original_ids {
        assert!(rec.old_segments.contains(id));
    }
    assert!(rec.committed_at_ms > 0);
}

#[tokio::test]
async fn output_segment_contains_all_input_events() {
    let root = tmp_root();
    let state = build_state(root.clone(), 2);

    let events_per_input = 4;
    let input_count = 3;
    let mut all_event_ids: Vec<String> = Vec::new();
    for seg_i in 0..input_count {
        let events: Vec<UsageEvent> = (0..events_per_input)
            .map(|i| {
                let id = format!("e_{seg_i}_{i}");
                all_event_ids.push(id.clone());
                make_event(&id, "acc_q", (seg_i * 100 + i) as i64, 7)
            })
            .collect();
        commit_segment(&state, &events, 0).await;
    }

    let worker = CompactionWorker::new(state.clone(), 2, 30_000, Duration::from_secs(60));
    worker.tick(1_000_000).await.unwrap();

    let output_id = {
        let manifest = state.manifest.read().await;
        manifest.raw_segments[0].segment_id.clone()
    };
    let path = state.config.db_root.join(format!("{}.seg", output_id));
    let mut reader = RawSegmentReader::new(path).unwrap();
    let mut read_ids = Vec::new();
    while let Some(e) = reader.read_next().unwrap() {
        read_ids.push(e.event_id.0);
    }
    read_ids.sort();
    all_event_ids.sort();
    assert_eq!(read_ids, all_event_ids,
        "every input event must be present in the compacted output (no loss, no dup)");
}

#[tokio::test]
async fn second_tick_is_noop_after_compaction() {
    let root = tmp_root();
    let state = build_state(root.clone(), 2);
    for i in 0..3 {
        commit_segment(&state, &[make_event(&format!("e{i}"), "acc_a", i, 1)], 0).await;
    }

    let worker = CompactionWorker::new(state.clone(), 2, 30_000, Duration::from_secs(60));
    let first = worker.tick(1_000_000).await.unwrap();
    assert_eq!(first.compactions_committed, 1);

    let second = worker.tick(1_000_000).await.unwrap();
    assert_eq!(second.compactions_committed, 0,
        "single output segment is below threshold; no further compaction");
    assert_eq!(second.replacements_finalized, 0,
        "now=1_000_000, committed=1_000_000, grace=30_000 ⇒ not yet finalizable");
}

#[tokio::test]
async fn old_files_deleted_after_grace_window() {
    let root = tmp_root();
    let state = build_state(root.clone(), 2);

    let mut old_ids = Vec::new();
    for i in 0..3 {
        let id = commit_segment(&state, &[make_event(&format!("e{i}"), "acc_g", i, 1)], 0).await;
        old_ids.push(id);
    }

    // Commit time = 1_000_000; grace = 30_000.
    let worker = CompactionWorker::new(state.clone(), 2, 30_000, Duration::from_secs(60));
    worker.tick(1_000_000).await.unwrap();

    // Old files still on disk just after compaction.
    for id in &old_ids {
        let path = state.config.db_root.join(format!("{}.seg", id));
        assert!(path.exists(), "old segment {} must survive until grace expires", id);
    }

    // A tick at now=1_000_029 is still inside the grace window.
    let mid = worker.tick(1_000_000 + 29_999).await.unwrap();
    assert_eq!(mid.replacements_finalized, 0);
    for id in &old_ids {
        assert!(state.config.db_root.join(format!("{}.seg", id)).exists(),
            "still in grace window");
    }

    // Tick past the grace window: files get deleted, record drops out of
    // compacted_replacements.
    let post = worker.tick(1_000_000 + 30_001).await.unwrap();
    assert_eq!(post.replacements_finalized, 1);
    for id in &old_ids {
        assert!(!state.config.db_root.join(format!("{}.seg", id)).exists(),
            "old segment {} should be deleted after grace", id);
    }
    let manifest = state.manifest.read().await;
    assert!(manifest.compacted_replacements.is_empty(),
        "replacement record should be removed after files are deleted");
}

#[tokio::test]
async fn plans_are_per_bucket() {
    let root = tmp_root();
    let state = build_state(root.clone(), 4);
    // Bucket 0: 3 segments → triggers (threshold > 2).
    for i in 0..3 {
        commit_segment(&state, &[make_event(&format!("a{i}"), "acc_0", i, 1)], 0).await;
    }
    // Bucket 1: 2 segments → below threshold.
    for i in 0..2 {
        commit_segment(&state, &[make_event(&format!("b{i}"), "acc_1", i, 1)], 1).await;
    }
    // Bucket 2: 4 segments → triggers.
    for i in 0..4 {
        commit_segment(&state, &[make_event(&format!("c{i}"), "acc_2", i, 1)], 2).await;
    }

    let worker = CompactionWorker::new(state.clone(), 2, 30_000, Duration::from_secs(60));
    let stats = worker.tick(1_000_000).await.unwrap();
    assert_eq!(stats.compactions_committed, 2,
        "buckets 0 and 2 trigger compaction; bucket 1 stays put");

    let manifest = state.manifest.read().await;
    // 1 compacted (bucket 0) + 2 untouched (bucket 1) + 1 compacted (bucket 2) = 4 segments.
    assert_eq!(manifest.raw_segments.len(), 4);
    let buckets: std::collections::HashSet<u32> = manifest
        .raw_segments
        .iter()
        .map(|s| s.bucket)
        .collect();
    assert!(buckets.contains(&0));
    assert!(buckets.contains(&1));
    assert!(buckets.contains(&2));
}
