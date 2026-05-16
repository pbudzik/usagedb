//! Admin CLI commands. Each `cmd_*` function returns a formatted string
//! (or writes side effects + returns a short status). The binary entry
//! point in `src/main.rs` parses CLI args, calls one of these, and
//! prints the returned string. Splitting it this way keeps the
//! commands testable without spawning a subprocess.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use chrono::DateTime;
use tokio::sync::{Mutex, RwLock};

use crate::export::parquet::export_raw_segments;
use crate::ingest::dedupe::HotDedupe;
use crate::ingest::memtable::Memtable;
use crate::ingest::wal::Wal;
use crate::rollup::worker::RollupWorker;
use crate::runtime::config::Config;
use crate::runtime::lock::DbLock;
use crate::runtime::state::{AppState, AppStateInner};
use crate::storage::manifest::{Manifest, SegmentKind};
use crate::storage::segment_reader::RawSegmentReader;

/// Build a read-only AppState by loading the manifest directly. Acquires
/// the DB process lock at the same time and returns it as a separate
/// guard the caller must hold for the duration of the admin operation.
/// Does NOT run the full Recovery flow (no segment-scan dedupe rebuild,
/// no WAL replay) — admin commands operate on the on-disk state as-is.
///
/// Returning `(AppState, DbLock)` rather than embedding the lock in the
/// state keeps AppStateInner test-friendly: tests that build state
/// manually (each with a fresh tempdir) don't need to also fake a lock.
pub fn open_state_for_admin(config: Config) -> anyhow::Result<(AppState, DbLock)> {
    let lock = DbLock::acquire(&config.db_root)?;
    let manifest = Manifest::load(&config.db_root)?.ok_or_else(|| {
        anyhow::anyhow!(
            "no manifest found at {:?} — run the server at least once to initialize the DB",
            config.db_root
        )
    })?;
    let wal_dir = config.db_root.join("wal");
    std::fs::create_dir_all(&wal_dir)?;
    let wal = Wal::open(wal_dir, manifest.last_sealed_wal_id)?;
    let (flush_sender, _r) = tokio::sync::mpsc::channel(4);
    let state = Arc::new(AppStateInner {
        config,
        // Minimal dedupe — admin commands don't ingest.
        dedupe: Mutex::new(HotDedupe::new(1)),
        wal: Mutex::new(wal),
        memtable: Mutex::new(Memtable::new()),
        manifest: RwLock::new(manifest),
        flush_sender,
    });
    Ok((state, lock))
}

/// `usagedb check [--deep]` — print manifest summary; with --deep, also
/// open every segment to verify checksum + format.
pub async fn cmd_check(state: AppState, deep: bool) -> anyhow::Result<String> {
    let manifest = state.manifest.read().await;
    let mut out = String::new();
    out.push_str(&format!("Database root:        {:?}\n", state.config.db_root));
    out.push_str(&format!("Manifest generation:  {}\n", manifest.current_generation));
    out.push_str(&format!("Bucket count:         {}\n", manifest.bucket_count));
    out.push_str(&format!("Raw segments:         {}\n", manifest.raw_segments.len()));
    out.push_str(&format!("Rollup segments:      {}\n", manifest.rollup_segments.len()));
    out.push_str(&format!(
        "Watermark:            {} ms ({})\n",
        manifest.watermarks.hourly_rollup_ms,
        format_ms(manifest.watermarks.hourly_rollup_ms),
    ));
    out.push_str(&format!("Last sealed WAL:      {}\n", manifest.last_sealed_wal_id));
    out.push_str(&format!(
        "Pending replacements: {}\n",
        manifest.compacted_replacements.len()
    ));

    if deep {
        out.push_str("\nVerifying segment files...\n");
        let mut errors = 0usize;
        for meta in manifest.raw_segments.iter().chain(manifest.rollup_segments.iter()) {
            let ext = match meta.kind {
                SegmentKind::Raw => "seg",
                SegmentKind::Rollup => "rseg",
            };
            let path = state.config.db_root.join(format!("{}.{}", meta.segment_id, ext));
            let result = match meta.kind {
                SegmentKind::Raw => verify_raw_segment(&path),
                SegmentKind::Rollup => verify_rollup_segment(&path, meta.checksum),
            };
            match result {
                Ok(()) => out.push_str(&format!("  {} OK\n", meta.segment_id)),
                Err(e) => {
                    errors += 1;
                    out.push_str(&format!("  {} ERROR: {}\n", meta.segment_id, e));
                }
            }
        }
        if errors > 0 {
            out.push_str(&format!("\n{} segment(s) failed verification.\n", errors));
            return Err(anyhow::anyhow!("{} segment(s) failed verification", errors));
        }
        out.push_str("\nAll segments verified.\n");
    }
    Ok(out)
}

fn verify_raw_segment(path: &Path) -> anyhow::Result<()> {
    // RawSegmentReader::new validates magic + checksum + end magic + version
    // + decodes every column. If it returns Ok, the segment is structurally
    // sound.
    RawSegmentReader::new(path.to_path_buf())?;
    Ok(())
}

fn verify_rollup_segment(path: &Path, expected_checksum: u64) -> anyhow::Result<()> {
    use crate::rollup::reader::RollupSegmentReader;
    RollupSegmentReader::open(path.to_path_buf(), expected_checksum)?;
    Ok(())
}

/// `usagedb rebuild-rollups --from --to` — drop affected rollups + rewind
/// watermark; next server tick refills.
pub async fn cmd_rebuild_rollups(
    state: AppState,
    from: &str,
    to: &str,
) -> anyhow::Result<String> {
    let from_ms = parse_rfc3339_ms(from, "from")?;
    let to_ms = parse_rfc3339_ms(to, "to")?;

    let worker = RollupWorker::new(
        state.clone(),
        state.config.rollup_safety_lag_ms,
        Duration::from_secs(60),
        i64::MAX, // disable force-drain — admin command, no background ticks
    );
    let dropped = worker.rebuild_rollups(from_ms, to_ms).await?;
    Ok(format!(
        "Dropped {} rollup segment(s) overlapping [{}, {}).\n\
         Watermark rewound to {}.\n\
         Run the server (or wait for its rollup worker) to refill the gap from raw segments.\n",
        dropped, from, to, from
    ))
}

/// `usagedb inspect-segment <id>` — print segment metadata + a sample of rows.
pub async fn cmd_inspect_segment(state: AppState, segment_id: &str) -> anyhow::Result<String> {
    let manifest = state.manifest.read().await;
    let raw_match = manifest.raw_segments.iter().find(|s| s.segment_id == segment_id);
    let rollup_match = manifest.rollup_segments.iter().find(|s| s.segment_id == segment_id);

    let meta = raw_match.or(rollup_match).ok_or_else(|| {
        anyhow::anyhow!("segment {} not found in manifest", segment_id)
    })?;
    let is_raw = matches!(meta.kind, SegmentKind::Raw);

    let mut out = String::new();
    out.push_str(&format!("Segment: {}\n", meta.segment_id));
    out.push_str(&format!("  Kind: {:?}\n", meta.kind));
    out.push_str(&format!("  Bucket: {}\n", meta.bucket));
    out.push_str(&format!("  Rows: {}\n", meta.row_count));
    out.push_str(&format!(
        "  Timestamp range: [{}, {}]\n",
        format_ms(meta.min_timestamp_ms),
        format_ms(meta.max_timestamp_ms),
    ));
    if let Some(min) = &meta.min_account_id {
        out.push_str(&format!("  Account range: {}", min.0));
        if let Some(max) = &meta.max_account_id {
            out.push_str(&format!(" .. {}", max.0));
        }
        out.push('\n');
    }
    out.push_str(&format!(
        "  Products: {}\n",
        meta.product_ids
            .iter()
            .map(|p| p.0.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    ));
    out.push_str(&format!(
        "  Meters: {}\n",
        meta.meter_ids
            .iter()
            .map(|m| m.0.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    ));
    out.push_str(&format!(
        "  Models: {}\n",
        meta.model_ids
            .iter()
            .map(|m| m.0.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    ));
    out.push_str(&format!(
        "  Quantity sum: {}\n",
        meta.quantity_sum.map(|q| q.to_string()).unwrap_or_else(|| "(none)".into())
    ));
    out.push_str(&format!("  Checksum: {:#018x}\n", meta.checksum));
    if !meta.input_segment_ids.is_empty() {
        out.push_str("  Input segments (rollup provenance):\n");
        for id in &meta.input_segment_ids {
            out.push_str(&format!("    - {}\n", id));
        }
    }

    if is_raw {
        let path = state.config.db_root.join(format!("{}.seg", segment_id));
        let mut reader = RawSegmentReader::new(path)?;
        out.push_str("\n  First few rows:\n");
        let mut shown = 0;
        while let Some(e) = reader.read_next()? {
            if shown >= 5 {
                break;
            }
            out.push_str(&format!(
                "    {} {:?} acc={} product={} meter={} model={:?} ts={} qty={}\n",
                e.event_id.0,
                e.kind,
                e.account_id.0,
                e.product_id.0,
                e.meter_id.0,
                e.model_id.as_ref().map(|m| m.0.as_str()),
                e.timestamp_ms,
                e.quantity,
            ));
            shown += 1;
        }
        if shown == 0 {
            out.push_str("    (empty segment)\n");
        }
    }
    Ok(out)
}

/// `usagedb verify-period --account --from --to` — call the same logic
/// as the /verify endpoint and pretty-print the result.
pub async fn cmd_verify_period(
    state: AppState,
    account: &str,
    from: &str,
    to: &str,
) -> anyhow::Result<String> {
    use crate::query::executor::execute_plan;
    use crate::query::plan::{AggregationFunction, QueryPlan, QuerySource};
    use std::collections::HashMap;

    let from_ms = parse_rfc3339_ms(from, "from")?;
    let to_ms = parse_rfc3339_ms(to, "to")?;

    let mut metrics = HashMap::new();
    metrics.insert("quantity".to_string(), AggregationFunction::Sum);
    let plan_raw = QueryPlan {
        source: QuerySource::RawEvents,
        account_id: Some(account.to_string()),
        from_ms,
        to_ms,
        filters: vec![],
        group_by: vec![],
        metrics: metrics.clone(),
        limit: None,
    };
    let plan_rollup = QueryPlan {
        source: QuerySource::RollupHourly,
        ..plan_raw.clone()
    };
    let raw_total = extract_sum(&execute_plan(&state, &plan_raw).await);
    let rollup_total = extract_sum(&execute_plan(&state, &plan_rollup).await);
    let drift = raw_total.saturating_sub(rollup_total);
    let watermark_ms = state.manifest.read().await.watermarks.hourly_rollup_ms;
    let period_sealed = to_ms <= watermark_ms;

    let mut out = String::new();
    out.push_str(&format!("Account:        {}\n", account));
    out.push_str(&format!(
        "Range:          [{}, {})\n",
        from, to
    ));
    out.push_str(&format!(
        "Watermark:      {} ({})\n",
        watermark_ms,
        format_ms(watermark_ms),
    ));
    out.push_str(&format!("Period sealed:  {}\n", period_sealed));
    out.push_str(&format!("Raw total:      {}\n", raw_total));
    out.push_str(&format!("Rollup total:   {}\n", rollup_total));
    out.push_str(&format!(
        "Drift:          {} {}\n",
        drift,
        if drift == 0 { "(OK)" } else { "(MISMATCH)" }
    ));
    Ok(out)
}

/// `usagedb export-parquet <output>` — dump every raw segment in the
/// manifest to one Parquet file.
pub async fn cmd_export_parquet(state: AppState, output: &Path) -> anyhow::Result<String> {
    let stats = export_raw_segments(&state, output).await?;
    Ok(format!(
        "Exported {} events from {} segment(s) to {:?}\n",
        stats.events_exported, stats.segments_read, output
    ))
}

fn parse_rfc3339_ms(s: &str, field: &str) -> anyhow::Result<i64> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.timestamp_millis())
        .map_err(|e| anyhow::anyhow!("invalid `{}` (not RFC 3339): {}", field, e))
}

fn extract_sum(result: &[serde_json::Value]) -> i128 {
    result
        .iter()
        .filter_map(|v| v.get("quantity"))
        .filter_map(|v| v.as_str())
        .filter_map(|s| s.parse().ok())
        .next()
        .unwrap_or(0)
}

fn format_ms(ms: i64) -> String {
    DateTime::from_timestamp_millis(ms)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_else(|| "<invalid>".into())
}

#[allow(dead_code)]
fn _force_pathbuf_use(_: &PathBuf) {}
