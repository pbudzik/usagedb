//! Background worker that converts sealed raw segments into hourly rollups.
//!
//! Watermark contract:
//!   - The manifest holds `watermarks.hourly_rollup_ms` — the upper bound
//!     (exclusive) below which every hour has been rolled up.
//!   - Each tick advances the watermark, but **only** to a target that
//!     respects three safety bounds (review P0 #1):
//!       a. `time_target = floor((now - safety_lag) / 1h) * 1h`
//!       b. if a previous flush is in flight (a sealed WAL file hasn't
//!          been committed to a raw segment yet), skip this tick
//!       c. if the memtable holds events, the watermark can advance only
//!          up to the floor-of-hour of the oldest such event — never past
//!          unflushed data
//!   - Stalls prevented: if the memtable's oldest event has been pending
//!     longer than `memtable_max_age_ms`, the worker force-drains it
//!     (drain + WAL rotate + queue flush) and skips this tick. The next
//!     tick sees the flushed state.
//!   - For each hour in `[current_watermark, target)` we scan all raw
//!     segments that overlap the hour, aggregate events whose timestamp
//!     falls inside the hour (grouped by bucket from `account_id`), and
//!     write one rollup segment per bucket.
//!   - The manifest update (rollup segments + new watermark) is one
//!     atomic save, so a crash mid-tick leaves the previous watermark
//!     in place and the next tick re-does the work.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::Notify;
use tracing::{error, info, warn};

use crate::model::event::UsageEvent;
use crate::model::ids::{AccountId, bucket_for_account};
use crate::rollup::builder::RollupBuilder;
use crate::rollup::hourly::HourlyRollupRecord;
use crate::rollup::writer::RollupSegmentWriter;
use crate::runtime::state::{AppState, FlushMessage};
use crate::storage::manifest::{SegmentKind, SegmentMeta};
use crate::storage::segment_reader::RawSegmentReader;

const HOUR_MS: i64 = 3_600_000;

pub struct RollupWorker {
    state: AppState,
    safety_lag_ms: i64,
    tick_interval: Duration,
    memtable_max_age_ms: i64,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct RollupTickStats {
    pub segments_written: usize,
    pub watermark_ms: i64,
    pub hours_processed: usize,
    /// Set when this tick force-drained the memtable instead of advancing
    /// the watermark. The next tick should pick up the flushed state.
    pub forced_flush: bool,
    /// Set when this tick saw an in-flight flush and waited.
    pub skipped_for_in_flight: bool,
}

impl RollupWorker {
    pub fn new(
        state: AppState,
        safety_lag_ms: i64,
        tick_interval: Duration,
        memtable_max_age_ms: i64,
    ) -> Self {
        Self { state, safety_lag_ms, tick_interval, memtable_max_age_ms }
    }

    pub async fn run(self, shutdown: Arc<Notify>) {
        info!(
            "Rollup worker started (interval={:?}, safety_lag={}ms, memtable_max_age={}ms)",
            self.tick_interval, self.safety_lag_ms, self.memtable_max_age_ms
        );
        let mut interval = tokio::time::interval(self.tick_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    match self.tick(now_ms()).await {
                        Ok(stats) if stats.segments_written > 0 => {
                            info!(
                                "Rollup tick: wrote {} segments across {} hour(s); watermark={}",
                                stats.segments_written, stats.hours_processed, stats.watermark_ms
                            );
                        }
                        Ok(stats) if stats.forced_flush => {
                            info!("Rollup tick: force-drained stale memtable");
                        }
                        Ok(_) => {}
                        Err(e) => error!("Rollup tick failed: {}", e),
                    }
                }
                _ = shutdown.notified() => {
                    info!("Rollup worker stopping");
                    break;
                }
            }
        }
    }

    /// Do one rollup pass. `now_ms_override` controls the "current time"
    /// used to compute the target watermark and the memtable-staleness
    /// check; tests pass an explicit value instead of waiting on the clock.
    pub async fn tick(&self, now_ms_override: i64) -> anyhow::Result<RollupTickStats> {
        // Snapshot the bits we need from the manifest + WAL.
        let (current_watermark, raw_segments, rollup_segments, bucket_count, last_sealed_wal_id) = {
            let manifest = self.state.manifest.read().await;
            (
                manifest.watermarks.hourly_rollup_ms,
                manifest.raw_segments.clone(),
                manifest.rollup_segments.clone(),
                manifest.bucket_count.max(1),
                manifest.last_sealed_wal_id,
            )
        };
        let active_wal_id = {
            let wal = self.state.wal.lock().await;
            wal.active_id
        };

        // Safety bound (b): if there are sealed WAL files that haven't yet
        // been committed to raw segments, those events would be missed by
        // a watermark advance. Skip and let the flusher catch up.
        if active_wal_id > last_sealed_wal_id + 1 {
            return Ok(RollupTickStats {
                segments_written: 0,
                watermark_ms: current_watermark,
                hours_processed: 0,
                forced_flush: false,
                skipped_for_in_flight: true,
            });
        }

        // Stall prevention: memtable too old → force flush, skip tick.
        let needs_force_drain = {
            let memtable = self.state.memtable.lock().await;
            match memtable.oldest_insert_at_ms() {
                Some(at) => (now_ms_override - at) >= self.memtable_max_age_ms,
                None => false,
            }
        };
        if needs_force_drain {
            self.force_drain_memtable().await?;
            return Ok(RollupTickStats {
                segments_written: 0,
                watermark_ms: current_watermark,
                hours_processed: 0,
                forced_flush: true,
                skipped_for_in_flight: false,
            });
        }

        // Safety bound (c): never advance past unflushed memtable data.
        let memtable_min_ts = {
            let memtable = self.state.memtable.lock().await;
            memtable.min_event_timestamp_ms()
        };
        let time_target = ((now_ms_override - self.safety_lag_ms) / HOUR_MS) * HOUR_MS;
        let target_hour = match memtable_min_ts {
            Some(ts) => {
                let oldest_hour = (ts / HOUR_MS) * HOUR_MS;
                time_target.min(oldest_hour)
            }
            None => time_target,
        };

        // Initial-watermark clamp: a fresh DB starts with watermark=0;
        // skip the empty hours up to where data actually starts.
        let mut current_watermark = current_watermark;
        if current_watermark == 0 && !raw_segments.is_empty() {
            let earliest = raw_segments.iter().map(|s| s.min_timestamp_ms).min().unwrap_or(0);
            current_watermark = (earliest / HOUR_MS) * HOUR_MS;
        }

        if target_hour <= current_watermark {
            return Ok(RollupTickStats {
                segments_written: 0,
                watermark_ms: current_watermark,
                hours_processed: 0,
                forced_flush: false,
                skipped_for_in_flight: false,
            });
        }

        // Per-(hour, bucket) tuples already rolled up — don't double up.
        let already_rolled: std::collections::HashSet<(i64, u32)> = rollup_segments
            .iter()
            .map(|s| (s.min_timestamp_ms, s.bucket))
            .collect();

        let mut new_segments: Vec<(SegmentMeta, std::path::PathBuf)> = Vec::new();
        let mut hour = current_watermark;
        let mut hours_processed = 0usize;

        while hour < target_hour {
            let hour_end = hour + HOUR_MS;

            let mut by_bucket: HashMap<u32, RollupBuilder> = HashMap::new();
            // Track per-bucket provenance: which raw segment IDs
            // contributed events to each output rollup segment. Used to
            // populate `SegmentMeta.input_segment_ids` so invoice
            // lineage is auditable (spec §19.10).
            let mut inputs_by_bucket: HashMap<u32, std::collections::BTreeSet<String>> =
                HashMap::new();

            for seg in &raw_segments {
                if seg.max_timestamp_ms < hour || seg.min_timestamp_ms >= hour_end {
                    continue;
                }
                let path = self
                    .state
                    .config
                    .db_root
                    .join(format!("{}.seg", seg.segment_id));
                if !path.exists() {
                    warn!("rollup: raw segment {} referenced by manifest is missing", seg.segment_id);
                    continue;
                }
                let mut reader = RawSegmentReader::new(path)?;
                while let Some(event) = reader.read_next()? {
                    if event.timestamp_ms < hour || event.timestamp_ms >= hour_end {
                        continue;
                    }
                    let bucket = bucket_for_account(&event.account_id, bucket_count);
                    if already_rolled.contains(&(hour, bucket)) {
                        continue;
                    }
                    by_bucket.entry(bucket).or_default().process_event(&event);
                    inputs_by_bucket
                        .entry(bucket)
                        .or_default()
                        .insert(seg.segment_id.clone());
                }
            }

            for (bucket, builder) in by_bucket {
                let records = builder.finalize();
                if records.is_empty() {
                    continue;
                }
                let inputs: Vec<String> = inputs_by_bucket
                    .remove(&bucket)
                    .map(|set| set.into_iter().collect())
                    .unwrap_or_default();
                let (meta, path) =
                    self.write_rollup_segment(records, bucket, hour, hour_end, inputs)?;
                new_segments.push((meta, path));
            }

            hour = hour_end;
            hours_processed += 1;
        }

        // Atomically commit: append rollup segments + advance watermark.
        let segments_written = new_segments.len();
        if segments_written > 0 || target_hour > current_watermark {
            let mut manifest = self.state.manifest.write().await;
            for (meta, _path) in &new_segments {
                manifest.rollup_segments.push(meta.clone());
            }
            manifest.watermarks.hourly_rollup_ms = target_hour;
            if let Err(e) = manifest.save(&self.state.config.db_root) {
                error!("rollup: manifest save failed: {} — cleaning up {} segment files", e, new_segments.len());
                for (_meta, path) in &new_segments {
                    let _ = std::fs::remove_file(path);
                }
                return Err(anyhow::anyhow!("manifest save failed: {}", e));
            }
        }

        Ok(RollupTickStats {
            segments_written,
            watermark_ms: target_hour,
            hours_processed,
            forced_flush: false,
            skipped_for_in_flight: false,
        })
    }

    /// Operator-facing: drop any rollup segments overlapping `[from_ms, to_ms)`
    /// and rewind the watermark to `from_ms`. The next tick refills the
    /// gap from raw segments (review Phase A — rebuildable rollups).
    ///
    /// Use cases:
    ///   - a bug in the rollup builder was fixed and the cached rollups
    ///     need to be regenerated
    ///   - late events arrived for a period that was already sealed and
    ///     the rollups undercounted
    ///   - operator just wants to verify rollup = raw after the next tick
    ///
    /// The rollup segment *files* are not deleted immediately — they're
    /// just removed from `manifest.rollup_segments`, so any in-flight
    /// query that snapshotted the manifest can still open them. Cleanup
    /// of orphaned rollup files is a future operability task.
    pub async fn rebuild_rollups(&self, from_ms: i64, to_ms: i64) -> anyhow::Result<usize> {
        let mut manifest = self.state.manifest.write().await;
        let before = manifest.rollup_segments.len();
        manifest.rollup_segments.retain(|s| {
            // Keep segments that don't overlap [from_ms, to_ms).
            !(s.min_timestamp_ms < to_ms && s.max_timestamp_ms >= from_ms)
        });
        let dropped = before - manifest.rollup_segments.len();

        if manifest.watermarks.hourly_rollup_ms > from_ms {
            manifest.watermarks.hourly_rollup_ms = from_ms;
        }
        manifest.save(&self.state.config.db_root)?;
        info!(
            "Rebuild scheduled: dropped {} rollup segment(s) in [{}, {}); watermark rewound to {}",
            dropped, from_ms, to_ms, manifest.watermarks.hourly_rollup_ms
        );
        Ok(dropped)
    }

    /// Drain the memtable, rotate the WAL, and queue the resulting batch
    /// for the flusher. Same primitives the ingest path uses when the
    /// size threshold trips. Called from `tick` when the memtable has
    /// been sitting too long.
    async fn force_drain_memtable(&self) -> anyhow::Result<()> {
        let drained = {
            let mut wal = self.state.wal.lock().await;
            let mut memtable = self.state.memtable.lock().await;
            if memtable.is_empty() {
                return Ok(());
            }
            let events = memtable.drain_all();
            let sealed_id = wal.rotate()?;
            FlushMessage { events, sealed_wal_id: sealed_id }
        };
        self.state
            .flush_sender
            .send(drained)
            .await
            .map_err(|e| anyhow::anyhow!("rollup: force-flush channel send failed: {}", e))?;
        Ok(())
    }

    fn write_rollup_segment(
        &self,
        records: Vec<HourlyRollupRecord>,
        bucket: u32,
        hour_start: i64,
        hour_end: i64,
        input_segment_ids: Vec<String>,
    ) -> anyhow::Result<(SegmentMeta, std::path::PathBuf)> {
        let segment_id = format!("rollup_{}", uuid::Uuid::new_v4().simple());
        let path = self
            .state
            .config
            .db_root
            .join(format!("{}.rseg", segment_id));

        let mut writer = RollupSegmentWriter::new(path.clone())?;
        for r in &records {
            writer.write_record(r)?;
        }
        let (row_count, checksum) = writer.finish()?;

        let meta = build_rollup_segment_meta(
            &segment_id,
            &records,
            bucket,
            checksum,
            hour_start,
            hour_end - 1,
            row_count,
            input_segment_ids,
        );
        Ok((meta, path))
    }
}

fn build_rollup_segment_meta(
    segment_id: &str,
    records: &[HourlyRollupRecord],
    bucket: u32,
    checksum: u64,
    min_ts: i64,
    max_ts: i64,
    row_count: u64,
    input_segment_ids: Vec<String>,
) -> SegmentMeta {
    let mut product_ids = std::collections::HashSet::new();
    let mut meter_ids = std::collections::HashSet::new();
    let mut model_ids = std::collections::HashSet::new();
    let mut min_account: Option<String> = None;
    let mut max_account: Option<String> = None;
    let mut quantity_sum: i128 = 0;

    for r in records {
        product_ids.insert(r.product_id.clone());
        meter_ids.insert(r.meter_id.clone());
        if let Some(m) = &r.model_id {
            model_ids.insert(m.clone());
        }
        let acc = &r.account_id.0;
        match &min_account {
            None => min_account = Some(acc.clone()),
            Some(c) if acc < c => min_account = Some(acc.clone()),
            _ => {}
        }
        match &max_account {
            None => max_account = Some(acc.clone()),
            Some(c) if acc > c => max_account = Some(acc.clone()),
            _ => {}
        }
        quantity_sum = quantity_sum.saturating_add(r.quantity_sum);
    }

    SegmentMeta {
        segment_id: segment_id.to_string(),
        kind: SegmentKind::Rollup,
        min_timestamp_ms: min_ts,
        max_timestamp_ms: max_ts,
        bucket,
        row_count,
        min_account_id: min_account.map(AccountId),
        max_account_id: max_account.map(AccountId),
        product_ids,
        meter_ids,
        model_ids,
        quantity_sum: Some(quantity_sum),
        checksum,
        input_segment_ids,
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// Silence unused-import lint when no callers reference UsageEvent directly.
#[allow(dead_code)]
fn _force_usage_event_import_used(_e: &UsageEvent) {}
