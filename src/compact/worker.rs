//! Background compaction worker.
//!
//! Each tick:
//!   1. Sweep pending deletions: for every `compacted_replacements` entry
//!      whose age exceeds the grace window, delete the old segment files
//!      and remove the record from the manifest.
//!   2. Run the planner. For each per-bucket plan, merge the input
//!      segments into one new output (sort + cold-dedupe), then atomically
//!      swap old → new in the manifest with a `ReplacementRecord`. Old
//!      files stay on disk until phase 1 of a future tick deletes them.
//!
//! The grace window is configurable (`Config.compaction_grace_secs`) and
//! exists because a query that snapshotted the manifest just before a
//! compaction commit may still try to open old segment IDs. The grace
//! window must be longer than any reasonable query duration.

use std::collections::HashSet;
use std::io::Result as IoResult;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::Notify;
use tracing::{error, info, warn};

use crate::compact::planner::{CompactionPlan, CompactionPlanner};
use crate::ingest::flusher::build_segment_meta;
use crate::model::event::UsageEvent;
use crate::runtime::recovery::compute_event_hashes;
use crate::runtime::state::AppState;
use crate::storage::dedupe_index::{index_path, write_dedupe_index, DedupeEntry};
use crate::storage::manifest::ReplacementRecord;
use crate::storage::segment_reader::RawSegmentReader;
use crate::storage::segment_writer::RawSegmentWriter;

pub struct CompactionWorker {
    state: AppState,
    max_small_segments: usize,
    grace_ms: i64,
    tick_interval: Duration,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct CompactionTickStats {
    /// Number of new compacted segments written this tick.
    pub compactions_committed: usize,
    /// Number of replacement records whose old files were deleted this tick.
    pub replacements_finalized: usize,
}

impl CompactionWorker {
    pub fn new(
        state: AppState,
        max_small_segments: usize,
        grace_ms: i64,
        tick_interval: Duration,
    ) -> Self {
        Self { state, max_small_segments, grace_ms, tick_interval }
    }

    pub async fn run(self, shutdown: Arc<Notify>) {
        info!(
            "Compaction worker started (interval={:?}, grace={}ms, threshold>{} segments/bucket)",
            self.tick_interval, self.grace_ms, self.max_small_segments
        );
        let mut interval = tokio::time::interval(self.tick_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await; // skip immediate first tick

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    match self.tick(now_ms()).await {
                        Ok(stats) if stats.compactions_committed > 0 || stats.replacements_finalized > 0 => {
                            info!(
                                "Compaction tick: {} compactions committed, {} replacements finalized",
                                stats.compactions_committed, stats.replacements_finalized
                            );
                        }
                        Ok(_) => {}
                        Err(e) => error!("Compaction tick failed: {}", e),
                    }
                }
                _ = shutdown.notified() => {
                    info!("Compaction worker stopping");
                    break;
                }
            }
        }
    }

    /// One compaction pass. `now_ms_override` controls the "current time"
    /// used for the deletion grace check; tests pass an explicit value
    /// instead of waiting on wall clock.
    pub async fn tick(&self, now_ms_override: i64) -> anyhow::Result<CompactionTickStats> {
        let replacements_finalized = self.sweep_pending_deletions(now_ms_override).await?;

        let plans: Vec<CompactionPlan> = {
            let manifest = self.state.manifest.read().await;
            let planner = CompactionPlanner {
                max_small_segments: self.max_small_segments,
                max_small_size_bytes: 32 * 1024 * 1024,
            };
            planner.plan_compaction(&manifest.raw_segments)
        };

        let mut compactions_committed = 0usize;
        for plan in plans {
            match self.run_one_plan(&plan, now_ms_override).await {
                Ok(true) => compactions_committed += 1,
                Ok(false) => {} // plan invalidated by concurrent state change
                Err(e) => error!("Compaction job for bucket {} failed: {}", plan.bucket, e),
            }
        }

        Ok(CompactionTickStats { compactions_committed, replacements_finalized })
    }

    /// Execute one compaction plan: read inputs, merge+sort+dedupe, write
    /// the output, atomically swap in the manifest. Returns Ok(false) if
    /// the plan's inputs are no longer all present in the manifest (a
    /// previous tick or concurrent compaction already handled them).
    async fn run_one_plan(&self, plan: &CompactionPlan, now_ms: i64) -> anyhow::Result<bool> {
        let db_root = &self.state.config.db_root;

        // Read events from inputs. Done outside the manifest write lock —
        // segment files are immutable.
        let events = read_segments(db_root, &plan.segment_ids)?;
        let deduped = sort_and_dedupe(events);

        if deduped.is_empty() {
            // Nothing to write (inputs all missing / empty). Skip.
            return Ok(false);
        }

        // Write the output segment.
        let output_id = format!("compacted_{}", uuid::Uuid::new_v4().simple());
        let output_path = db_root.join(format!("{}.seg", output_id));
        let mut writer = RawSegmentWriter::new(output_path.clone())?;
        for e in &deduped {
            if let Err(err) = writer.write_event(e) {
                let _ = std::fs::remove_file(&output_path);
                return Err(err.into());
            }
        }
        let (_rows, checksum) = match writer.finish() {
            Ok(t) => t,
            Err(e) => {
                let _ = std::fs::remove_file(&output_path);
                return Err(e.into());
            }
        };
        let new_meta = build_segment_meta(&output_id, &deduped, plan.bucket, checksum);

        // Write the dedupe sidecar for the compacted output too — same
        // recovery speedup applies. Non-fatal on failure.
        let idx_entries: Vec<DedupeEntry> = deduped
            .iter()
            .map(|e| {
                let (id_h, p_h) = compute_event_hashes(e);
                (id_h, p_h, e.ingested_at_ms)
            })
            .collect();
        let idx = index_path(db_root, &output_id);
        if let Err(e) = write_dedupe_index(&idx, &idx_entries) {
            warn!("compaction: dedupe sidecar write for {} failed: {}", output_id, e);
            let _ = std::fs::remove_file(&idx);
        }

        // Atomically swap old → new in the manifest.
        let committed = {
            let mut manifest = self.state.manifest.write().await;

            // Defensive: every input must still be in raw_segments. If a
            // concurrent worker (none today, but future-proofing) already
            // removed any, abort this plan and clean up the output.
            let input_set: HashSet<&String> = plan.segment_ids.iter().collect();
            let still_present = manifest
                .raw_segments
                .iter()
                .filter(|s| input_set.contains(&s.segment_id))
                .count();
            if still_present != plan.segment_ids.len() {
                warn!(
                    "Compaction plan inputs no longer all present in manifest ({}/{}); aborting",
                    still_present, plan.segment_ids.len()
                );
                drop(manifest);
                let _ = std::fs::remove_file(&output_path);
                return Ok(false);
            }

            manifest.raw_segments.retain(|s| !input_set.contains(&s.segment_id));
            manifest.raw_segments.push(new_meta);
            manifest.compacted_replacements.push(ReplacementRecord {
                old_segments: plan.segment_ids.clone(),
                new_segments: vec![output_id.clone()],
                committed_at_ms: now_ms,
            });

            match manifest.save(&self.state.config.db_root) {
                Ok(()) => true,
                Err(e) => {
                    error!("Compaction manifest save failed: {} — cleaning up output", e);
                    // Roll back the in-memory mutation by re-reading from disk
                    // would be ideal; for now we surface the error and let
                    // the operator deal with it. The output file we just
                    // wrote is unreferenced; remove it.
                    drop(manifest);
                    let _ = std::fs::remove_file(&output_path);
                    return Err(anyhow::anyhow!("manifest save failed: {}", e));
                }
            }
        };

        if committed {
            info!(
                "Compacted {} inputs in bucket {} into {} ({} rows after dedupe)",
                plan.segment_ids.len(),
                plan.bucket,
                output_id,
                deduped.len()
            );
        }
        Ok(committed)
    }

    /// Walk `compacted_replacements`. Any record whose age exceeds the
    /// grace window has its old files deleted and is removed from the
    /// manifest. Returns the number of replacements finalized.
    async fn sweep_pending_deletions(&self, now_ms: i64) -> anyhow::Result<usize> {
        let cutoff = now_ms - self.grace_ms;
        let to_finalize: Vec<ReplacementRecord> = {
            let manifest = self.state.manifest.read().await;
            manifest
                .compacted_replacements
                .iter()
                .filter(|r| r.committed_at_ms != 0 && r.committed_at_ms <= cutoff)
                .cloned()
                .collect()
        };

        if to_finalize.is_empty() {
            return Ok(0);
        }

        let db_root = &self.state.config.db_root;
        for record in &to_finalize {
            for old_id in &record.old_segments {
                let path = db_root.join(format!("{}.seg", old_id));
                if path.exists() {
                    if let Err(e) = std::fs::remove_file(&path) {
                        warn!("Failed to delete old segment {}: {}", old_id, e);
                    }
                }
            }
        }

        // Update the manifest to drop the finalized records. We compare by
        // (committed_at_ms, old_segments) — adequately unique since each
        // tick generates a distinct ms timestamp per record.
        let mut manifest = self.state.manifest.write().await;
        let finalized_keys: HashSet<(i64, Vec<String>)> = to_finalize
            .iter()
            .map(|r| (r.committed_at_ms, r.old_segments.clone()))
            .collect();
        manifest
            .compacted_replacements
            .retain(|r| !finalized_keys.contains(&(r.committed_at_ms, r.old_segments.clone())));
        if let Err(e) = manifest.save(&self.state.config.db_root) {
            error!("Failed to persist replacement cleanup: {}", e);
            return Err(anyhow::anyhow!("manifest save failed: {}", e));
        }

        Ok(to_finalize.len())
    }
}

fn read_segments(db_root: &Path, segment_ids: &[String]) -> IoResult<Vec<UsageEvent>> {
    let mut events = Vec::new();
    for id in segment_ids {
        let path = db_root.join(format!("{}.seg", id));
        if !path.exists() {
            warn!("compaction: input segment {} missing from disk", id);
            continue;
        }
        let mut reader = RawSegmentReader::new(path)?;
        while let Some(event) = reader.read_next()? {
            events.push(event);
        }
    }
    Ok(events)
}

fn sort_and_dedupe(mut events: Vec<UsageEvent>) -> Vec<UsageEvent> {
    events.sort_by(|a, b| {
        a.account_id.0.cmp(&b.account_id.0)
            .then_with(|| a.product_id.0.cmp(&b.product_id.0))
            .then_with(|| a.meter_id.0.cmp(&b.meter_id.0))
            .then_with(|| {
                let am = a.model_id.as_ref().map(|m| m.0.as_str()).unwrap_or("");
                let bm = b.model_id.as_ref().map(|m| m.0.as_str()).unwrap_or("");
                am.cmp(bm)
            })
            .then_with(|| a.timestamp_ms.cmp(&b.timestamp_ms))
    });
    let mut seen: HashSet<String> = HashSet::with_capacity(events.len());
    events.retain(|e| seen.insert(e.event_id.0.clone()));
    events
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[allow(dead_code)]
fn _retain_path_use(_: &PathBuf) {}
