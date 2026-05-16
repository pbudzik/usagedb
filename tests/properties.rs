//! proptest-driven property tests for the spec §19 billing-safety
//! invariants. Each test generates randomized event sequences within a
//! bounded space and checks an invariant after applying the same
//! sequence the production code path would take (ingest → optional
//! flush → optional rollup/compaction → query).
//!
//! Invariants exercised:
//!   §19.1  acknowledged events are recoverable
//!   §19.4  a raw segment is never counted twice in rollups
//!   §19.5  duplicate event_id with same payload is not double-counted
//!   §19.6  duplicate event_id with different payload is visible as conflict
//!   §19.8  rollup totals reconcile with raw totals
//!   §19.9  compaction does not change logical results
//!
//! NOTE on shape: each property test spawns a fresh tempdir-backed DB,
//! drives a few async steps via a single-threaded tokio runtime, and
//! compares two derived values for equality. The state-machine driver
//! that randomly interleaves ingest/flush/compact/rollup/restart/query
//! over many steps is a follow-up (deterministic simulation testing).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use proptest::prelude::*;
use tokio::runtime::Runtime;
use tokio::sync::{Mutex, RwLock};

use usagedb::compact::worker::CompactionWorker;
use usagedb::ingest::dedupe::{DedupeResult, HotDedupe};
use usagedb::ingest::flusher::build_segment_meta;
use usagedb::ingest::memtable::Memtable;
use usagedb::ingest::wal::Wal;
use usagedb::model::dimensions::SmallDimensions;
use usagedb::model::event::{EventKind, UsageEvent};
use usagedb::model::ids::{
    AccountId, EventId, MeterId, ModelId, ProductId, SourceId, SubscriptionId, Unit,
    bucket_for_account,
};
use usagedb::query::executor::execute_plan;
use usagedb::query::plan::{AggregationFunction, QueryPlan, QuerySource};
use usagedb::rollup::worker::RollupWorker;
use usagedb::runtime::config::Config;
use usagedb::runtime::recovery::{compute_event_hashes, Recovery};
use usagedb::runtime::state::{AppState, AppStateInner};
use usagedb::storage::manifest::Manifest;
use usagedb::storage::segment_writer::RawSegmentWriter;

const HOUR_MS: i64 = 3_600_000;
const BUCKET_COUNT: u32 = 4;
const ACCOUNTS: &[&str] = &["acc_a", "acc_b", "acc_c", "acc_d"];
const METERS: &[&str] = &["tokens.input", "tokens.output", "tokens.embedding"];

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Strategy that produces a syntactically valid `UsageEvent` within a
/// bounded space. Timestamps stay inside a single hour so the rollup
/// path is deterministic; quantities are small positive integers so
/// SUMs fit in i128 comfortably.
fn arb_event() -> impl Strategy<Value = UsageEvent> {
    (
        "[a-z]{2,8}",                      // event_id (small alphabet keeps collisions interesting)
        prop::sample::select(ACCOUNTS),
        prop::sample::select(METERS),
        1i64..HOUR_MS,                     // ts in hour 0
        1i128..1000i128,
    )
        .prop_map(|(id, account, meter, ts, qty)| UsageEvent {
            event_id: EventId(id),
            kind: EventKind::Usage,
            correction_ref: None,
            account_id: AccountId(account.to_string()),
            subscription_id: Some(SubscriptionId("sub_1".into())),
            product_id: ProductId("ai_gateway".into()),
            meter_id: MeterId(meter.to_string()),
            timestamp_ms: ts,
            quantity: qty,
            unit: Unit("token".into()),
            source: SourceId("test".into()),
            model_id: Some(ModelId("m1".into())),
            dimensions: SmallDimensions::default(),
            ingested_at_ms: 0, // stamped by ingest
        })
}

fn arb_event_batch() -> impl Strategy<Value = Vec<UsageEvent>> {
    prop::collection::vec(arb_event(), 1..30)
}

/// In-process driver that exercises the same primitives the production
/// pipeline does, but synchronously and without HTTP/tokio-task
/// scheduling. The flush path bypasses the channel + flusher worker
/// (writes segments directly + updates manifest); this loses some
/// realism but gains determinism — enough for property tests.
struct Harness {
    state: AppState,
    _tmp_dir: tempfile::TempDir,
}

impl Harness {
    fn new() -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db_root = tmp.path().to_path_buf();
        let config = Config {
            db_root: db_root.clone(),
            default_bucket_count: BUCKET_COUNT,
            // Tests drive ticks directly; default tick intervals would just
            // burn CPU. memtable_max_age=MAX disables the force-drain path
            // since the property tests handle flushing explicitly.
            memtable_max_age_ms: i64::MAX,
            ..Config::default()
        };
        std::fs::create_dir_all(&db_root).expect("create db_root");
        let wal = Wal::open(db_root.join("wal"), 0).expect("open wal");
        let manifest = Manifest { bucket_count: BUCKET_COUNT, ..Manifest::default() };
        let (flush_sender, _flush_receiver) = tokio::sync::mpsc::channel(4);
        let state = Arc::new(AppStateInner {
            config,
            dedupe: Mutex::new(HotDedupe::new(100_000)),
            wal: Mutex::new(wal),
            memtable: Mutex::new(Memtable::new()),
            manifest: RwLock::new(manifest),
            flush_sender,
        });
        Self { state, _tmp_dir: tmp }
    }

    /// Mimic `handle_ingest`'s critical section: classify → WAL+sync →
    /// commit dedupe + memtable. Returns per-status counts so tests can
    /// assert on the dedup result distribution.
    async fn ingest(&self, events: Vec<UsageEvent>) -> IngestStats {
        let mut accepted = 0usize;
        let mut duplicates = 0usize;
        let mut conflicts = 0usize;

        let mut dedupe = self.state.dedupe.lock().await;
        let mut wal = self.state.wal.lock().await;
        let mut memtable = self.state.memtable.lock().await;
        let mut seen_in_batch: HashMap<u128, u128> = HashMap::new();

        for mut event in events {
            event.ingested_at_ms = now_ms();
            let (id_hash, payload_hash) = compute_event_hashes(&event);

            if let Some(&prior) = seen_in_batch.get(&id_hash) {
                if prior == payload_hash { duplicates += 1; } else { conflicts += 1; }
                continue;
            }
            match dedupe.classify(id_hash, payload_hash) {
                DedupeResult::NewEvent => {
                    seen_in_batch.insert(id_hash, payload_hash);
                    wal.append_batch([&event]).expect("wal append");
                    wal.sync().expect("wal sync");
                    dedupe.commit(id_hash, payload_hash);
                    memtable.insert(event);
                    accepted += 1;
                }
                DedupeResult::ExactDuplicate => duplicates += 1,
                DedupeResult::PayloadConflict => conflicts += 1,
            }
        }

        IngestStats { accepted, duplicates, conflicts }
    }

    /// Drain memtable + rotate WAL + write one segment per bucket +
    /// update manifest (segments + `last_sealed_wal_id`) + delete the
    /// now-sealed WAL files. Bypasses the channel + flusher worker but
    /// otherwise mirrors what the production flusher does. Skipping the
    /// WAL rotation here was a bug — without it, after restart, recovery
    /// replays the WAL into the memtable AND scans the segment, so the
    /// same event contributes twice to a SUM query.
    async fn force_flush(&self) {
        let (drained, sealed_wal_id) = {
            let mut wal = self.state.wal.lock().await;
            let mut memtable = self.state.memtable.lock().await;
            if memtable.is_empty() {
                return;
            }
            let drained = memtable.drain_all();
            let sealed = wal.rotate().expect("wal rotate");
            (drained, sealed)
        };

        let bucket_count = self.state.manifest.read().await.bucket_count.max(1);
        let mut by_bucket: BTreeMap<u32, Vec<UsageEvent>> = BTreeMap::new();
        for event in drained {
            let b = bucket_for_account(&event.account_id, bucket_count);
            by_bucket.entry(b).or_default().push(event);
        }

        let mut new_metas = Vec::new();
        for (bucket, events) in by_bucket {
            let seg_id = format!("raw_{}", uuid::Uuid::new_v4().simple());
            let path = self.state.config.db_root.join(format!("{}.seg", seg_id));
            let mut writer = RawSegmentWriter::new(path).expect("create segment");
            for e in &events { writer.write_event(e).expect("write event"); }
            let (_rows, checksum) = writer.finish().expect("finish segment");
            new_metas.push(build_segment_meta(&seg_id, &events, bucket, checksum));
        }

        {
            let mut manifest = self.state.manifest.write().await;
            for m in new_metas { manifest.raw_segments.push(m); }
            if sealed_wal_id > manifest.last_sealed_wal_id {
                manifest.last_sealed_wal_id = sealed_wal_id;
            }
            manifest.save(&self.state.config.db_root).expect("manifest save");
        }

        let wal_dir = self.state.config.db_root.join("wal");
        Wal::delete_files_through(&wal_dir, sealed_wal_id).expect("wal cleanup");
    }

    async fn tick_rollup(&self, now: i64) {
        // memtable_max_age=MAX so force-drain never triggers.
        let worker = RollupWorker::new(
            self.state.clone(),
            0,
            Duration::from_secs(60),
            i64::MAX,
        );
        worker.tick(now).await.expect("rollup tick");
    }

    async fn tick_compaction(&self, now: i64) {
        // max_small_segments=1 so compaction triggers when any bucket has 2+
        // segments — used by the compaction-preserves-sum property.
        let worker = CompactionWorker::new(
            self.state.clone(),
            1,
            0,
            Duration::from_secs(60),
        );
        worker.tick(now).await.expect("compaction tick");
    }

    /// Compute SUM(quantity) for an account from a given source.
    async fn query_sum(&self, source: QuerySource, account: &str) -> i128 {
        let mut metrics = HashMap::new();
        metrics.insert("quantity".into(), AggregationFunction::Sum);
        let plan = QueryPlan {
            source,
            account_id: Some(account.into()),
            from_ms: 0,
            to_ms: i64::MAX,
            filters: vec![],
            group_by: vec![],
            metrics,
            limit: None,
        };
        let result = execute_plan(&self.state, &plan).await;
        result
            .iter()
            .filter_map(|v| v.get("quantity"))
            .filter_map(|v| v.as_str())
            .filter_map(|s| s.parse().ok())
            .next()
            .unwrap_or(0)
    }

    /// Compute SUM per account across all `ACCOUNTS` from the given source.
    async fn query_sums_per_account(&self, source: QuerySource) -> HashMap<&'static str, i128> {
        let mut sums = HashMap::new();
        for acc in ACCOUNTS {
            sums.insert(*acc, self.query_sum(source.clone(), acc).await);
        }
        sums
    }

    /// Simulate a restart: drop the in-process state and rebuild via
    /// `Recovery::run_startup_recovery`. Returns a new Harness reusing
    /// the same on-disk db_root.
    fn restart(self) -> Self {
        let Self { state, _tmp_dir: tmp } = self;
        let db_root = state.config.db_root.clone();
        let bucket_count = state.config.default_bucket_count;
        let max_memtable = state.config.max_memtable_size_bytes;
        drop(state);

        let recovery = Recovery::new(db_root.clone());
        let recovery_result = recovery.run_startup_recovery(100_000).expect("recovery");

        let config = Config {
            db_root: db_root.clone(),
            default_bucket_count: bucket_count,
            max_memtable_size_bytes: max_memtable,
            memtable_max_age_ms: i64::MAX,
            ..Config::default()
        };
        let wal = Wal::open(
            db_root.join("wal"),
            recovery_result.manifest.last_sealed_wal_id,
        )
        .expect("reopen wal");
        let (flush_sender, _flush_receiver) = tokio::sync::mpsc::channel(4);
        let new_state = Arc::new(AppStateInner {
            config,
            dedupe: Mutex::new(recovery_result.dedupe),
            wal: Mutex::new(wal),
            memtable: Mutex::new(recovery_result.memtable),
            manifest: RwLock::new(recovery_result.manifest),
            flush_sender,
        });
        Self { state: new_state, _tmp_dir: tmp }
    }
}

#[derive(Debug, Default)]
struct IngestStats {
    accepted: usize,
    duplicates: usize,
    conflicts: usize,
}

fn rt() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

proptest! {
    // 32 cases keeps CI quick. Each case spins up a tempdir, ingest path,
    // rollup tick, etc. — non-trivial fixed cost per case.
    #![proptest_config(ProptestConfig {
        cases: 32,
        .. ProptestConfig::default()
    })]

    // §19.8 — rollup totals reconcile with raw totals.
    #[test]
    fn raw_sum_equals_rollup_sum(events in arb_event_batch()) {
        let rt = rt();
        let (raw_sums, rollup_sums) = rt.block_on(async {
            let harness = Harness::new();
            harness.ingest(events).await;
            harness.force_flush().await;
            // Tick the rollup worker with `now` far enough past the hour
            // that the safety_lag has elapsed and the watermark advances
            // to HOUR_MS (sealing hour 0).
            harness.tick_rollup(HOUR_MS + 120_000).await;

            let raw = harness.query_sums_per_account(QuerySource::RawEvents).await;
            let rollup = harness.query_sums_per_account(QuerySource::RollupHourly).await;
            (raw, rollup)
        });

        for acc in ACCOUNTS {
            prop_assert_eq!(
                raw_sums[acc], rollup_sums[acc],
                "SUM(quantity) for {} disagrees: raw={}, rollup={}",
                acc, raw_sums[acc], rollup_sums[acc]
            );
        }
    }

    // §19.5 — retrying an ingest batch must not change any sum.
    #[test]
    fn duplicate_ingest_is_idempotent(events in arb_event_batch()) {
        let rt = rt();
        let (first_sums, second_sums, second_accepted, second_dups_plus_conflicts) = rt.block_on(async {
            let harness = Harness::new();
            harness.ingest(events.clone()).await;
            harness.force_flush().await;
            let first = harness.query_sums_per_account(QuerySource::RawEvents).await;

            // Ingest the exact same batch again. Every event should be
            // detected as a duplicate; sums unchanged.
            let stats = harness.ingest(events.clone()).await;
            harness.force_flush().await;
            let second = harness.query_sums_per_account(QuerySource::RawEvents).await;

            (first, second, stats.accepted, stats.duplicates + stats.conflicts)
        });

        for acc in ACCOUNTS {
            prop_assert_eq!(
                first_sums[acc], second_sums[acc],
                "retry changed SUM for {}: {} → {}", acc, first_sums[acc], second_sums[acc]
            );
        }
        prop_assert_eq!(second_accepted, 0, "no events should be re-accepted on retry");
        // Every event on the retry should be classified as duplicate or conflict.
        prop_assert!(
            second_dups_plus_conflicts >= events.len() / 2,
            "expected retries to be flagged as dup/conflict; got {} for {} events",
            second_dups_plus_conflicts, events.len()
        );
    }

    // §19.9 — compaction preserves logical results.
    #[test]
    fn compaction_preserves_sum(
        batches in prop::collection::vec(arb_event_batch(), 2..6),
    ) {
        let rt = rt();
        let (before, after) = rt.block_on(async {
            let harness = Harness::new();
            // Multiple ingest+flush cycles so each bucket accumulates
            // several segments — gives compaction something to merge.
            for batch in batches {
                harness.ingest(batch).await;
                harness.force_flush().await;
            }
            let before = harness.query_sums_per_account(QuerySource::RawEvents).await;
            harness.tick_compaction(1_000_000).await;
            let after = harness.query_sums_per_account(QuerySource::RawEvents).await;
            (before, after)
        });

        for acc in ACCOUNTS {
            prop_assert_eq!(
                before[acc], after[acc],
                "compaction changed SUM for {}: {} → {}", acc, before[acc], after[acc]
            );
        }
    }

    // §19.1 — acknowledged events are recoverable across restart.
    #[test]
    fn recovery_preserves_sum(events in arb_event_batch()) {
        let rt = rt();
        let (before, after) = rt.block_on(async {
            let harness = Harness::new();
            harness.ingest(events).await;
            harness.force_flush().await;
            let before = harness.query_sums_per_account(QuerySource::RawEvents).await;
            let harness = harness.restart();
            let after = harness.query_sums_per_account(QuerySource::RawEvents).await;
            (before, after)
        });

        for acc in ACCOUNTS {
            prop_assert_eq!(
                before[acc], after[acc],
                "restart lost data for {}: {} → {}", acc, before[acc], after[acc]
            );
        }
    }

    // §19.1 (variant): events still in memtable at restart-time are
    // recovered via WAL replay into the memtable, not lost.
    #[test]
    fn recovery_preserves_sum_without_flush(events in arb_event_batch()) {
        let rt = rt();
        let (before, after) = rt.block_on(async {
            let harness = Harness::new();
            harness.ingest(events).await;
            // No flush — events sit in memtable + sealed-WAL is empty.
            let before = harness.query_sums_per_account(QuerySource::RawEvents).await;
            let harness = harness.restart();
            let after = harness.query_sums_per_account(QuerySource::RawEvents).await;
            (before, after)
        });

        for acc in ACCOUNTS {
            prop_assert_eq!(
                before[acc], after[acc],
                "restart lost unflushed data for {}: {} → {}",
                acc, before[acc], after[acc]
            );
        }
    }

    // §19.4 — a raw segment's events are never double-rolled-up.
    #[test]
    fn rollup_tick_is_idempotent(events in arb_event_batch()) {
        let rt = rt();
        let (after_first, after_second) = rt.block_on(async {
            let harness = Harness::new();
            harness.ingest(events).await;
            harness.force_flush().await;
            harness.tick_rollup(HOUR_MS + 120_000).await;
            let first = harness.query_sums_per_account(QuerySource::RollupHourly).await;
            // Second tick at the same `now`: the worker should observe
            // target_hour <= current_watermark and emit no new rollup
            // segments. Sums must not change.
            harness.tick_rollup(HOUR_MS + 120_000).await;
            let second = harness.query_sums_per_account(QuerySource::RollupHourly).await;
            (first, second)
        });

        for acc in ACCOUNTS {
            prop_assert_eq!(
                after_first[acc], after_second[acc],
                "second rollup tick changed SUM for {}: {} → {}",
                acc, after_first[acc], after_second[acc]
            );
        }
    }
}

// §19.6 — duplicate event_id with different payload is visible as conflict.
// Not in proptest! because the strategy needs to guarantee a payload
// difference, which is easier to express as a unit-style randomized test.
#[test]
fn conflict_when_same_id_different_payload() {
    let rt = rt();
    rt.block_on(async {
        let harness = Harness::new();
        let base = UsageEvent {
            event_id: EventId("evt_x".into()),
            kind: EventKind::Usage,
            correction_ref: None,
            account_id: AccountId("acc_a".into()),
            subscription_id: Some(SubscriptionId("sub_1".into())),
            product_id: ProductId("ai_gateway".into()),
            meter_id: MeterId("tokens.input".into()),
            timestamp_ms: 1000,
            quantity: 100,
            unit: Unit("token".into()),
            source: SourceId("test".into()),
            model_id: Some(ModelId("m1".into())),
            dimensions: SmallDimensions::default(),
            ingested_at_ms: 0,
        };
        let s1 = harness.ingest(vec![base.clone()]).await;
        assert_eq!(s1.accepted, 1);

        // Same event_id, different quantity → payload hash differs.
        let mut variant = base.clone();
        variant.quantity = 200;
        let s2 = harness.ingest(vec![variant]).await;
        assert_eq!(s2.accepted, 0);
        assert_eq!(s2.conflicts, 1);
        assert_eq!(s2.duplicates, 0);
    });
}
