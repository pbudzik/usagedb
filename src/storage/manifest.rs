use serde::{Deserialize, Serialize};
use crate::model::ids::{AccountId, ProductId, MeterId, ModelId};
use std::collections::HashSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SegmentKind {
    Raw,
    Rollup,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMeta {
    pub segment_id: String,
    pub kind: SegmentKind,
    pub min_timestamp_ms: i64,
    pub max_timestamp_ms: i64,
    pub bucket: u32,
    pub row_count: u64,
    pub min_account_id: Option<AccountId>,
    pub max_account_id: Option<AccountId>,
    pub product_ids: HashSet<ProductId>,
    pub meter_ids: HashSet<MeterId>,
    pub model_ids: HashSet<ModelId>,
    pub quantity_sum: Option<i128>,
    pub checksum: u64,
    /// For rollup segments: the raw segment IDs whose events were
    /// aggregated to produce this rollup. Empty for raw and compacted
    /// segments (compacted segments' provenance lives in
    /// `Manifest.compacted_replacements`).
    ///
    /// Spec §19.10: invoice snapshots must reference a watermark + the
    /// source segment set. This is the per-rollup half of that — given
    /// a rollup segment, you can name every raw segment that
    /// contributed to it, so an invoice line's lineage is auditable.
    #[serde(default)]
    pub input_segment_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplacementRecord {
    pub old_segments: Vec<String>,
    pub new_segments: Vec<String>,
    /// When the compaction commit happened (unix ms). Used to enforce a
    /// reader grace period before old files are physically deleted —
    /// queries that snapshotted the manifest before the commit may still
    /// be holding old segment IDs in memory.
    #[serde(default)]
    pub committed_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Watermarks {
    pub hourly_rollup_ms: i64,
}

/// A finalized billing period for one account. New `Usage` events with a
/// timestamp inside this period are rejected at ingest. `Correction` and
/// `Retraction` events are still accepted (they become post-close
/// adjustments, per spec §13). An operator-driven `reopen_period`
/// removes the entry from `closed_periods`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClosedPeriod {
    pub account_id: String,
    pub year: u16,
    pub month: u8,
    /// Wall-clock when the close happened (unix ms). Surfaced in the
    /// GET endpoint so operators can correlate with their billing run.
    pub closed_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Manifest {
    pub db_version: u32,
    pub bucket_count: u32,
    pub raw_segments: Vec<SegmentMeta>,
    pub rollup_segments: Vec<SegmentMeta>,
    pub compacted_replacements: Vec<ReplacementRecord>,
    pub watermarks: Watermarks,
    /// Highest WAL file ID whose contents are durably in committed segments.
    /// WAL files with id <= this can be deleted; files with id > this must be
    /// replayed on recovery.
    #[serde(default)]
    pub last_sealed_wal_id: u64,

    /// Per-account billing periods that have been closed. `Usage` events
    /// timestamped inside any of these are rejected at ingest.
    /// `#[serde(default)]` so manifests written before period lifecycle
    /// deserialize with an empty list.
    #[serde(default)]
    pub closed_periods: Vec<ClosedPeriod>,

    /// Generation number — bumps on every save. Used by the manifest-
    /// generation recovery to walk backwards through historical versions
    /// when the current one is corrupt. `#[serde(skip)]` because it's
    /// tracked by file name (`manifest-NNNNNN.json`), not embedded in
    /// the JSON itself.
    #[serde(skip)]
    pub current_generation: u64,
}

/// Number of historical generations to keep on disk. Each save creates a
/// new file; older generations beyond this count are pruned. Bounded so
/// disk usage doesn't grow without limit on long-running databases.
const KEEP_GENERATIONS: u64 = 10;

/// Per-generation manifest directory layout (review Phase A — Manifest
/// generations). Replaces the single `manifest.json` with:
///
/// ```text
/// manifest/
///   CURRENT                    (text file: latest valid generation u64)
///   manifest-000001.json
///   manifest-000002.json
///   ...
/// ```
///
/// Recovery reads CURRENT and loads that generation. If it's corrupt, the
/// loader walks backwards until it finds a generation that parses cleanly
/// — billing data isn't orphaned by a single corrupt write. If no
/// generation parses, recovery fails closed.
///
/// Legacy `manifest.json` from pre-generation DBs is auto-migrated on
/// first load.
impl Manifest {
    /// Save a new generation. Increments `current_generation`, writes the
    /// new file, atomically updates CURRENT, and prunes old generations.
    /// `&mut self` because the generation counter bumps.
    pub fn save(&mut self, db_root: &Path) -> std::io::Result<()> {
        let manifest_dir = db_root.join("manifest");
        std::fs::create_dir_all(&manifest_dir)?;

        let next_gen = self.current_generation + 1;
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        // Write the new generation file with an atomic tmp+rename.
        let gen_path = manifest_dir.join(generation_filename(next_gen));
        let gen_tmp = manifest_dir.join(format!("{}.tmp", generation_filename(next_gen)));
        {
            let mut file = std::fs::File::create(&gen_tmp)?;
            file.write_all(json.as_bytes())?;
            file.sync_all()?;
        }
        std::fs::rename(&gen_tmp, &gen_path)?;

        // Atomically advance CURRENT to point at the new generation.
        let current_path = manifest_dir.join("CURRENT");
        let current_tmp = manifest_dir.join("CURRENT.tmp");
        {
            let mut file = std::fs::File::create(&current_tmp)?;
            file.write_all(next_gen.to_string().as_bytes())?;
            file.sync_all()?;
        }
        std::fs::rename(&current_tmp, &current_path)?;

        // Fsync the directory so the renames are durable.
        let dir_handle = std::fs::File::open(&manifest_dir)?;
        dir_handle.sync_all()?;

        self.current_generation = next_gen;

        // Prune old generations. Failure here is non-fatal — disk just
        // accumulates a few extra files.
        if let Err(e) = prune_old_generations(&manifest_dir, next_gen) {
            warn!("manifest: failed to prune old generations: {}", e);
        }

        Ok(())
    }

    /// Load the most recent valid manifest from disk.
    ///
    /// Search order:
    ///   1. `manifest/CURRENT` → load that generation. On corruption,
    ///      walk backwards through earlier generations until one parses.
    ///   2. Legacy `manifest.json` → migrate it to generation 1 and
    ///      delete the old file.
    ///   3. No manifest at all → return Ok(None), caller initializes a
    ///      fresh DB.
    ///
    /// Returns `Err` only when a manifest file exists but no generation
    /// parses cleanly — the operator must intervene rather than silently
    /// starting with an empty DB.
    pub fn load(db_root: &Path) -> std::io::Result<Option<Self>> {
        let manifest_dir = db_root.join("manifest");
        let current_path = manifest_dir.join("CURRENT");

        if current_path.exists() {
            return Self::load_from_generations(&manifest_dir, &current_path)
                .map(Some);
        }

        // Legacy single-file path. Migrate it to generation 1.
        let legacy_path = db_root.join("manifest.json");
        if legacy_path.exists() {
            return Self::migrate_legacy(db_root, &legacy_path).map(Some);
        }

        Ok(None)
    }

    fn load_from_generations(manifest_dir: &Path, current_path: &Path) -> std::io::Result<Self> {
        let current_str = std::fs::read_to_string(current_path)?;
        let current_gen: u64 = current_str.trim().parse().map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("CURRENT file corrupt: {}", e),
            )
        })?;

        // Try CURRENT first, then walk backwards.
        for g in (1..=current_gen).rev() {
            let gen_path = manifest_dir.join(generation_filename(g));
            if !gen_path.exists() {
                continue;
            }
            let data = match std::fs::read_to_string(&gen_path) {
                Ok(d) => d,
                Err(e) => {
                    warn!("manifest: cannot read generation {}: {}", g, e);
                    continue;
                }
            };
            match serde_json::from_str::<Manifest>(&data) {
                Ok(mut m) => {
                    m.current_generation = g;
                    if g < current_gen {
                        warn!(
                            "manifest: CURRENT pointed at corrupt generation {}; rolled back to {}",
                            current_gen, g
                        );
                    }
                    info!(
                        "Loaded manifest generation {} with {} raw segments",
                        g,
                        m.raw_segments.len()
                    );
                    return Ok(m);
                }
                Err(e) => {
                    warn!("manifest: generation {} corrupt ({}); trying older", g, e);
                }
            }
        }

        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "no valid manifest generation found under {:?} — CURRENT={}; \
                 inspect the directory manually before retrying",
                manifest_dir, current_gen
            ),
        ))
    }

    fn migrate_legacy(db_root: &Path, legacy_path: &Path) -> std::io::Result<Self> {
        let data = std::fs::read_to_string(legacy_path)?;
        let mut manifest: Manifest = serde_json::from_str(&data).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Corrupt manifest: legacy {:?} won't parse: {}. \
                     Refusing to start; no generations to fall back to. \
                     Back up the file and inspect manually.",
                    legacy_path, e
                ),
            )
        })?;

        info!("Migrating legacy manifest.json to generation 1");
        // current_generation starts at 0; save() bumps to 1.
        manifest.save(db_root)?;
        let _ = std::fs::remove_file(legacy_path);

        Ok(manifest)
    }
}

fn generation_filename(g: u64) -> String {
    format!("manifest-{:06}.json", g)
}

fn parse_generation(name: &str) -> Option<u64> {
    let stem = name.strip_prefix("manifest-")?.strip_suffix(".json")?;
    stem.parse().ok()
}

fn prune_old_generations(manifest_dir: &Path, current_gen: u64) -> std::io::Result<()> {
    if current_gen <= KEEP_GENERATIONS {
        return Ok(());
    }
    // Keep the last KEEP_GENERATIONS generations: [current_gen-KEEP+1 ..= current_gen].
    // Anything strictly older than that floor gets deleted.
    let floor = current_gen - KEEP_GENERATIONS + 1;
    let mut deleted = 0usize;
    for entry in std::fs::read_dir(manifest_dir)? {
        let entry = entry?;
        let name = match entry.file_name().into_string() {
            Ok(n) => n,
            Err(_) => continue,
        };
        if let Some(g) = parse_generation(&name) {
            if g < floor {
                let _ = std::fs::remove_file(entry.path());
                deleted += 1;
            }
        }
    }
    if deleted > 0 {
        info!("Pruned {} old manifest generation(s)", deleted);
    }
    Ok(())
}

#[allow(dead_code)]
fn _force_pathbuf_use(_p: &PathBuf) {}
