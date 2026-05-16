//! Background worker that converts sealed raw segments into hourly rollups.
//!
//! Watermark contract:
//!   - The manifest holds `watermarks.hourly_rollup_ms` — the upper bound
//!     (exclusive) below which every hour has been rolled up.
//!   - Each tick advances the watermark to `target = floor((now - safety_lag) / 1h) * 1h`.
//!     Hours strictly below `target` are eligible for rollup.
//!   - For each hour in `[current_watermark, target)` we scan all raw
//!     segments that overlap the hour, aggregate events whose timestamp
//!     falls inside the hour, and write one rollup segment per bucket.
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
use crate::runtime::state::AppState;
use crate::storage::manifest::{SegmentKind, SegmentMeta};
use crate::storage::segment_reader::RawSegmentReader;

const HOUR_MS: i64 = 3_600_000;

pub struct RollupWorker {
    state: AppState,
    safety_lag_ms: i64,
    tick_interval: Duration,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct RollupTickStats {
    pub segments_written: usize,
    pub watermark_ms: i64,
    pub hours_processed: usize,
}

impl RollupWorker {
    pub fn new(state: AppState, safety_lag_ms: i64, tick_interval: Duration) -> Self {
        Self { state, safety_lag_ms, tick_interval }
    }

    /// Loop until `shutdown` is notified. Errors from individual ticks are
    /// logged and the loop continues — the next tick retries any work that
    /// wasn't committed.
    pub async fn run(self, shutdown: Arc<Notify>) {
        info!(
            "Rollup worker started (interval={:?}, safety_lag={}ms)",
            self.tick_interval, self.safety_lag_ms
        );
        let mut interval = tokio::time::interval(self.tick_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // First tick fires immediately; consume it so we wait a full
        // interval before doing real work.
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
                        Ok(_) => { /* nothing to do */ }
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

    /// Do one rollup pass. Public so tests can drive the worker deterministically
    /// instead of waiting on the interval timer. `now_ms_override` controls the
    /// "current time" used to compute the target watermark, so tests can drive
    /// the worker forward without sleeping.
    pub async fn tick(&self, now_ms_override: i64) -> anyhow::Result<RollupTickStats> {
        let target_hour = ((now_ms_override - self.safety_lag_ms) / HOUR_MS) * HOUR_MS;

        // Snapshot manifest under the read lock.
        let (mut current_watermark, raw_segments, rollup_segments, bucket_count) = {
            let manifest = self.state.manifest.read().await;
            (
                manifest.watermarks.hourly_rollup_ms,
                manifest.raw_segments.clone(),
                manifest.rollup_segments.clone(),
                manifest.bucket_count.max(1),
            )
        };

        // If the watermark is at the epoch but there's actual data, clamp it
        // forward to the earliest hour any raw segment touches — otherwise
        // a fresh DB would iterate billions of empty hours.
        if current_watermark == 0 && !raw_segments.is_empty() {
            let earliest = raw_segments.iter().map(|s| s.min_timestamp_ms).min().unwrap_or(0);
            current_watermark = (earliest / HOUR_MS) * HOUR_MS;
        }

        if target_hour <= current_watermark {
            return Ok(RollupTickStats {
                segments_written: 0,
                watermark_ms: current_watermark,
                hours_processed: 0,
            });
        }

        // Build the set of (hour_start, bucket) tuples we've already rolled up,
        // so a crash + restart can't double-count even within the same
        // watermark window.
        let already_rolled: std::collections::HashSet<(i64, u32)> = rollup_segments
            .iter()
            .map(|s| (s.min_timestamp_ms, s.bucket))
            .collect();

        let mut new_segments: Vec<(SegmentMeta, std::path::PathBuf)> = Vec::new();
        let mut hour = current_watermark;
        let mut hours_processed = 0usize;

        while hour < target_hour {
            let hour_end = hour + HOUR_MS;

            // Group records by bucket. We re-derive the bucket from the
            // event's account_id rather than trusting the segment's bucket
            // metadata, so that any future compaction that mixes buckets
            // can't poison the rollup.
            let mut by_bucket: HashMap<u32, RollupBuilder> = HashMap::new();
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
                }
            }

            for (bucket, builder) in by_bucket {
                let records = builder.finalize();
                if records.is_empty() {
                    continue;
                }
                let (meta, path) = self.write_rollup_segment(records, bucket, hour, hour_end)?;
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
                // Manifest save failed: clean up the segment files we
                // wrote, since they're now unreferenced.
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
        })
    }

    fn write_rollup_segment(
        &self,
        records: Vec<HourlyRollupRecord>,
        bucket: u32,
        hour_start: i64,
        hour_end: i64,
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
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// Silence unused-import lint when no callers reference UsageEvent in this
// file directly (the type is reached via RawSegmentReader::read_next).
#[allow(dead_code)]
fn _force_usage_event_import_used(_e: &UsageEvent) {}
