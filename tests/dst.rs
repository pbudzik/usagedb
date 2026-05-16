//! Deterministic Simulation Testing (DST) — Phase E part 2.
//!
//! Drives randomized sequences of operations against a fresh DB and a
//! parallel reference model, asserting §19 billing-safety invariants
//! after each step. Test cases are reproducible via proptest seed;
//! failures shrink to a minimal trace of ops, which makes regressions
//! immediately bisectable.
//!
//! What this catches that the single-property tests in `properties.rs`
//! do not:
//!   - bugs that only manifest after a specific interleaving of
//!     ingest / flush / rollup / compact / restart / close (e.g. a
//!     dedupe entry lost across restart and double-counted on retry)
//!   - bugs where a sequence preserves "raw == rollup" momentarily but
//!     leaves the system in a state that breaks the next operation
//!
//! Invariants checked after every step:
//!   §19.5  raw SUM(quantity) per account matches the reference model
//!          (duplicate event_ids never double-count, no events lost)
//!   §19.1  acknowledged events survive restart
//!   §19.9  compaction does not change logical results
//!   §19.21 closed periods reject new Usage events (model + SUT agree)
//!
//! Invariants checked at end of sequence (after a final flush + rollup):
//!   §19.8  raw SUM == rollup SUM per account
//!
//! The reference model deliberately stays simple — it tracks acked
//! `event_id` hashes (so duplicate detection matches), per-account
//! quantity totals, and the set of closed `(account, year, month)`
//! tuples. It does not model rollup watermarks, segment layout, or
//! compaction internals — the SUT's raw query reflects all of those.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{NaiveDate, TimeZone, Utc};
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
use usagedb::period::period_for_ts;
use usagedb::query::executor::execute_plan;
use usagedb::query::plan::{AggregationFunction, QueryPlan, QuerySource};
use usagedb::rollup::worker::RollupWorker;
use usagedb::runtime::config::Config;
use usagedb::runtime::recovery::{Recovery, compute_event_hashes};
use usagedb::runtime::state::{AppState, AppStateInner};
use usagedb::storage::manifest::{ClosedPeriod, Manifest};
use usagedb::storage::segment_writer::RawSegmentWriter;

const HOUR_MS: i64 = 3_600_000;
const BUCKET_COUNT: u32 = 4;
const ACCOUNTS: &[&str] = &["acc_a", "acc_b", "acc_c"];
const METERS: &[&str] = &["tokens.input", "tokens.output"];
const TARGET_YEAR: u16 = 2026;
const TARGET_MONTH: u8 = 1;

/// Epoch-millis of 2026-01-01T00:00:00Z. All generated events fall in
/// hour 0 of that period, so ClosePeriod uses (2026, 1) and the rollup
/// watermark advances cleanly when `tick_rollup` is invoked at
/// `base_ts() + HOUR_MS + 120_000`.
fn base_ts() -> i64 {
    NaiveDate::from_ymd_opt(TARGET_YEAR as i32, TARGET_MONTH as u32, 1)
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|dt| Utc.from_utc_datetime(&dt).timestamp_millis())
        .expect("base_ts construction")
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[derive(Debug, Clone)]
enum Op {
    /// A small batch of randomly-generated events.
    Ingest(Vec<UsageEvent>),
    /// Force-flush the memtable to one segment per bucket.
    Flush,
    /// Advance the rollup watermark past hour 0 if the memtable allows.
    RollupTick,
    /// Trigger compaction (with the threshold dialed down so any bucket
    /// with 2+ segments gets merged).
    CompactTick,
    /// Drop in-process state and rebuild from disk.
    Restart,
    /// Close the (account, 2026-01) period.
    ClosePeriod { account: String },
}

fn arb_event() -> impl Strategy<Value = UsageEvent> {
    (
        "[a-z]{2,8}",
        prop::sample::select(ACCOUNTS),
        prop::sample::select(METERS),
        0i64..HOUR_MS,
        1i128..1000i128,
    )
        .prop_map(|(id, account, meter, ts_offset, qty)| UsageEvent {
            event_id: EventId(id),
            kind: EventKind::Usage,
            correction_ref: None,
            account_id: AccountId(account.to_string()),
            subscription_id: Some(SubscriptionId("sub_1".into())),
            product_id: ProductId("ai_gateway".into()),
            meter_id: MeterId(meter.to_string()),
            timestamp_ms: base_ts() + ts_offset,
            quantity: qty,
            unit: Unit("token".into()),
            source: SourceId("test".into()),
            model_id: Some(ModelId("m1".into())),
            dimensions: SmallDimensions::default(),
            ingested_at_ms: 0,
        })
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        // Weighted to favor data-producing ops over administrative ones,
        // so a sequence of length N actually accumulates events.
        5 => prop::collection::vec(arb_event(), 1..6).prop_map(Op::Ingest),
        2 => Just(Op::Flush),
        2 => Just(Op::RollupTick),
        2 => Just(Op::CompactTick),
        1 => Just(Op::Restart),
        1 => prop::sample::select(ACCOUNTS)
            .prop_map(|a| Op::ClosePeriod { account: a.to_string() }),
    ]
}

fn arb_sequence() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(arb_op(), 5..30)
}

/// In-memory parallel model. Tracks just enough state to predict what
/// every observable SUT query should return after each op:
///   - `acked`: event_id hash → payload hash for every acknowledged
///     event (used so duplicate ingests are not double-counted).
///   - `sum`:  net SUM(quantity) per account.
///   - `closed`: (account, year, month) tuples that are closed (so a
///     model Ingest reproduces the SUT's period-rejection behavior).
#[derive(Debug, Default)]
struct Model {
    acked: HashMap<u128, u128>,
    sum: HashMap<String, i128>,
    closed: HashSet<(String, u16, u8)>,
}

impl Model {
    fn ingest(&mut self, events: &[UsageEvent]) {
        let mut seen_in_batch: HashSet<u128> = HashSet::new();
        for ev in events {
            if matches!(ev.kind, EventKind::Usage) {
                if let Some((y, m)) = period_for_ts(ev.timestamp_ms) {
                    if self.closed.contains(&(ev.account_id.0.clone(), y, m)) {
                        continue;
                    }
                }
            }
            let (id_hash, payload_hash) = compute_event_hashes(ev);
            if seen_in_batch.contains(&id_hash) {
                continue;
            }
            if self.acked.contains_key(&id_hash) {
                continue;
            }
            seen_in_batch.insert(id_hash);
            self.acked.insert(id_hash, payload_hash);
            *self.sum.entry(ev.account_id.0.clone()).or_insert(0) += ev.quantity;
        }
    }

    fn close(&mut self, account: &str, year: u16, month: u8) {
        self.closed.insert((account.to_string(), year, month));
    }

    fn sum_for(&self, account: &str) -> i128 {
        self.sum.get(account).copied().unwrap_or(0)
    }
}

/// Single-process harness that drives ingest / flush / rollup /
/// compaction / restart / close-period directly against the production
/// primitives. Bypasses the HTTP layer + flusher channel for
/// determinism — same shortcut as `properties.rs`.
struct DstHarness {
    state: AppState,
    _tmp: tempfile::TempDir,
}

impl DstHarness {
    fn new() -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db_root = tmp.path().to_path_buf();
        let config = Config {
            db_root: db_root.clone(),
            default_bucket_count: BUCKET_COUNT,
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
        Self { state, _tmp: tmp }
    }

    /// Mirror `handle_ingest` + `ingest_critical_section`. Snapshot the
    /// closed-periods list, reject Usage events in closed periods, then
    /// classify against the dedupe cache and commit accepted events.
    async fn ingest(&self, events: Vec<UsageEvent>) {
        let closed: Vec<ClosedPeriod> = {
            let manifest = self.state.manifest.read().await;
            manifest.closed_periods.clone()
        };

        let mut classified: Vec<(UsageEvent, u128, u128)> = Vec::new();
        for mut event in events {
            if matches!(event.kind, EventKind::Usage) {
                if let Some((y, m)) = period_for_ts(event.timestamp_ms) {
                    let is_closed = closed.iter().any(|p| {
                        p.account_id == event.account_id.0 && p.year == y && p.month == m
                    });
                    if is_closed {
                        continue;
                    }
                }
            }
            event.ingested_at_ms = now_ms();
            let (id_hash, payload_hash) = compute_event_hashes(&event);
            classified.push((event, id_hash, payload_hash));
        }

        let mut dedupe = self.state.dedupe.lock().await;
        let mut wal = self.state.wal.lock().await;
        let mut memtable = self.state.memtable.lock().await;
        let mut seen_in_batch: HashMap<u128, u128> = HashMap::new();

        for (event, id_hash, payload_hash) in classified {
            if seen_in_batch.contains_key(&id_hash) {
                continue;
            }
            match dedupe.classify(id_hash, payload_hash) {
                DedupeResult::NewEvent => {
                    seen_in_batch.insert(id_hash, payload_hash);
                    wal.append_batch([&event]).expect("wal append");
                    wal.sync().expect("wal sync");
                    dedupe.commit(id_hash, payload_hash);
                    memtable.insert(event);
                }
                DedupeResult::ExactDuplicate | DedupeResult::PayloadConflict => {}
            }
        }
    }

    /// Drain memtable, rotate WAL, write one segment per bucket, update
    /// the manifest, and delete the now-sealed WAL files. Same shortcut
    /// the property tests use — bypasses the flusher channel but follows
    /// the same on-disk contract.
    async fn flush(&self) {
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
            for e in &events {
                writer.write_event(e).expect("write event");
            }
            let (_rows, checksum) = writer.finish().expect("finish segment");
            new_metas.push(build_segment_meta(&seg_id, &events, bucket, checksum));
        }

        {
            let mut manifest = self.state.manifest.write().await;
            for m in new_metas {
                manifest.raw_segments.push(m);
            }
            if sealed_wal_id > manifest.last_sealed_wal_id {
                manifest.last_sealed_wal_id = sealed_wal_id;
            }
            manifest.save(&self.state.config.db_root).expect("manifest save");
        }

        let wal_dir = self.state.config.db_root.join("wal");
        Wal::delete_files_through(&wal_dir, sealed_wal_id).expect("wal cleanup");
    }

    async fn rebuild_rollups(&self, from_ms: i64, to_ms: i64) {
        let worker = RollupWorker::new(
            self.state.clone(),
            0,
            Duration::from_secs(60),
            i64::MAX,
        );
        worker.rebuild_rollups(from_ms, to_ms).await.expect("rebuild_rollups");
    }

    async fn rollup_tick(&self) {
        let worker = RollupWorker::new(
            self.state.clone(),
            0,
            Duration::from_secs(60),
            i64::MAX,
        );
        worker
            .tick(base_ts() + HOUR_MS + 120_000)
            .await
            .expect("rollup tick");
    }

    async fn compact_tick(&self) {
        // max_small_segments = 1 → any bucket with 2+ small segments
        // becomes a compaction target. reader_grace = 0 → the new
        // segment immediately replaces the inputs (no deletion lag).
        let worker = CompactionWorker::new(
            self.state.clone(),
            1,
            0,
            Duration::from_secs(60),
        );
        worker
            .tick(base_ts() + HOUR_MS + 120_000)
            .await
            .expect("compaction tick");
    }

    /// Replicate the close-period logic from `handle_close_period`
    /// without going through the HTTP layer: re-check under the write
    /// lock, snapshot the current rollup+raw total, push a
    /// `ClosedPeriod` with the frozen quantity, persist the manifest.
    async fn close_period(&self, account: &str, year: u16, month: u8) {
        {
            let manifest = self.state.manifest.read().await;
            let already = manifest.closed_periods.iter().any(|p| {
                p.account_id == account && p.year == year && p.month == month
            });
            if already {
                return;
            }
        }

        let (from_ms, to_ms) = period_bounds(year, month);
        let frozen_quantity = self
            .query_sum(QuerySource::RollupHourly, account, from_ms, to_ms)
            .await;

        let mut manifest = self.state.manifest.write().await;
        if manifest.closed_periods.iter().any(|p| {
            p.account_id == account && p.year == year && p.month == month
        }) {
            return;
        }
        let entry = ClosedPeriod {
            account_id: account.to_string(),
            year,
            month,
            closed_at_ms: now_ms(),
            frozen_quantity: Some(frozen_quantity),
            frozen_event_count: Some(0),
            watermark_at_close_ms: Some(manifest.watermarks.hourly_rollup_ms),
        };
        manifest.closed_periods.push(entry);
        manifest
            .save(&self.state.config.db_root)
            .expect("manifest save");
    }

    /// Rebuild via the production startup recovery path on the same
    /// db_root, exactly like a process restart.
    fn restart(self) -> Self {
        let Self { state, _tmp: tmp } = self;
        let db_root = state.config.db_root.clone();
        let bucket_count = state.config.default_bucket_count;
        let max_memtable = state.config.max_memtable_size_bytes;
        drop(state);

        let recovery = Recovery::new(db_root.clone());
        let recovery_result = recovery
            .run_startup_recovery(100_000)
            .expect("recovery");

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
        Self { state: new_state, _tmp: tmp }
    }

    async fn query_sum(
        &self,
        source: QuerySource,
        account: &str,
        from_ms: i64,
        to_ms: i64,
    ) -> i128 {
        let mut metrics = HashMap::new();
        metrics.insert("quantity".into(), AggregationFunction::Sum);
        let plan = QueryPlan {
            source,
            account_id: Some(account.into()),
            from_ms,
            to_ms,
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

    async fn raw_sums(&self) -> HashMap<String, i128> {
        let mut out = HashMap::new();
        for acc in ACCOUNTS {
            out.insert(
                acc.to_string(),
                self.query_sum(QuerySource::RawEvents, acc, 0, i64::MAX).await,
            );
        }
        out
    }

    async fn rollup_sums(&self) -> HashMap<String, i128> {
        let mut out = HashMap::new();
        for acc in ACCOUNTS {
            out.insert(
                acc.to_string(),
                self.query_sum(QuerySource::RollupHourly, acc, 0, i64::MAX).await,
            );
        }
        out
    }
}

fn period_bounds(year: u16, month: u8) -> (i64, i64) {
    let from = NaiveDate::from_ymd_opt(year as i32, month as u32, 1)
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|dt| Utc.from_utc_datetime(&dt).timestamp_millis())
        .expect("period from_ms");
    let to = if month == 12 {
        NaiveDate::from_ymd_opt(year as i32 + 1, 1, 1)
    } else {
        NaiveDate::from_ymd_opt(year as i32, month as u32 + 1, 1)
    }
    .and_then(|d| d.and_hms_opt(0, 0, 0))
    .map(|dt| Utc.from_utc_datetime(&dt).timestamp_millis())
    .expect("period to_ms");
    (from, to)
}

fn rt() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

/// Run an operation against both the model and the harness. Returns
/// the (possibly-restarted) harness so the caller can keep driving.
async fn step(mut harness: DstHarness, model: &mut Model, op: Op) -> DstHarness {
    match op {
        Op::Ingest(events) => {
            model.ingest(&events);
            harness.ingest(events).await;
        }
        Op::Flush => harness.flush().await,
        Op::RollupTick => harness.rollup_tick().await,
        Op::CompactTick => harness.compact_tick().await,
        Op::Restart => harness = harness.restart(),
        Op::ClosePeriod { account } => {
            model.close(&account, TARGET_YEAR, TARGET_MONTH);
            harness.close_period(&account, TARGET_YEAR, TARGET_MONTH).await;
        }
    }
    harness
}

/// Assert §19.5: SUT raw sum == model sum, per account, after every step.
async fn assert_raw_matches_model(
    harness: &DstHarness,
    model: &Model,
    step_idx: usize,
    op_label: &str,
) -> Result<(), TestCaseError> {
    let raw = harness.raw_sums().await;
    for acc in ACCOUNTS {
        let expected = model.sum_for(acc);
        let actual = *raw.get(*acc).unwrap_or(&0);
        prop_assert_eq!(
            expected,
            actual,
            "step {} ({}): account {} raw sum diverged from model (expected={}, actual={})",
            step_idx,
            op_label,
            acc,
            expected,
            actual,
        );
    }
    Ok(())
}

fn op_label(op: &Op) -> &'static str {
    match op {
        Op::Ingest(_) => "Ingest",
        Op::Flush => "Flush",
        Op::RollupTick => "RollupTick",
        Op::CompactTick => "CompactTick",
        Op::Restart => "Restart",
        Op::ClosePeriod { .. } => "ClosePeriod",
    }
}

proptest! {
    // 64 sequences ≈ 1–2 seconds locally; each step does real disk I/O
    // (WAL fsync, segment write, manifest rename). The shrinker
    // collapses failures to a minimum-length op trace, so the cost of a
    // bigger run is small when nothing breaks.
    #![proptest_config(ProptestConfig {
        cases: 64,
        .. ProptestConfig::default()
    })]

    /// Drive a random op sequence; after each step assert §19.5
    /// (raw sum matches the reference model). At the end, do a final
    /// flush + rollup + compaction and assert §19.8 (raw == rollup).
    #[test]
    fn dst_state_machine(ops in arb_sequence()) {
        let rt = rt();
        rt.block_on(async {
            let mut harness = DstHarness::new();
            let mut model = Model::default();

            for (idx, op) in ops.iter().cloned().enumerate() {
                let label = op_label(&op);
                harness = step(harness, &mut model, op).await;
                assert_raw_matches_model(&harness, &model, idx, label).await?;
            }

            // Final reconciliation: flush everything, *rebuild* the
            // rollups, advance the watermark, run a compaction pass,
            // then assert raw == rollup for every account. §19.8.
            //
            // The rebuild_rollups call exists to cover the case where
            // the random trace did a `RollupTick` on an empty DB —
            // that advances the watermark past hour 0, so subsequent
            // ingests can't be rolled up by a vanilla tick. In
            // production that's handled by `rebuild_rollups`; in DST
            // we do the same to keep the §19.8 invariant honest.
            harness.flush().await;
            harness.rebuild_rollups(0, i64::MAX).await;
            harness.rollup_tick().await;
            harness.compact_tick().await;

            // raw still matches model after the wrap-up ops.
            assert_raw_matches_model(&harness, &model, ops.len(), "Finalize").await?;

            let raw = harness.raw_sums().await;
            let rollup = harness.rollup_sums().await;
            for acc in ACCOUNTS {
                let r = *raw.get(*acc).unwrap_or(&0);
                let g = *rollup.get(*acc).unwrap_or(&0);
                prop_assert_eq!(
                    r,
                    g,
                    "post-finalize: account {} raw={} rollup={} (§19.8 violation)",
                    acc, r, g,
                );
            }
            Ok(())
        })?;
    }
}
