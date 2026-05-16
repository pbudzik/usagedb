use std::path::PathBuf;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::storage::manifest::Manifest;
use crate::ingest::wal::Wal;
use crate::ingest::dedupe::{HotDedupe, DEFAULT_TTL_MS};
use crate::ingest::memtable::Memtable;
use crate::model::event::UsageEvent;
use crate::storage::segment_reader::RawSegmentReader;
use std::io::Result as IoResult;
use tracing::{info, warn};

pub struct RecoveryResult {
    pub manifest: Manifest,
    pub dedupe: HotDedupe,
    pub memtable: Memtable,
}

pub struct Recovery {
    pub db_root: PathBuf,
}

impl Recovery {
    pub fn new(db_root: PathBuf) -> Self {
        Self { db_root }
    }

    pub fn run_startup_recovery(&self, dedupe_capacity: usize) -> IoResult<RecoveryResult> {
        info!("Starting recovery...");

        // 1. Load manifest
        let manifest_path = self.db_root.join("manifest.json");
        let manifest: Manifest = if manifest_path.exists() {
            let data = fs::read_to_string(&manifest_path)?;
            match serde_json::from_str(&data) {
                Ok(m) => {
                    info!("Loaded manifest with {} raw segments",
                        { let m: &Manifest = &m; m.raw_segments.len() });
                    m
                }
                Err(e) => {
                    warn!("Corrupt manifest, starting fresh: {}", e);
                    Manifest::default()
                }
            }
        } else {
            info!("No manifest found, starting fresh");
            Manifest::default()
        };

        let last_sealed_wal_id = manifest.last_sealed_wal_id;

        // 2. Remove tmp files
        let tmp_dir = self.db_root.join("tmp");
        if tmp_dir.exists() {
            for entry in fs::read_dir(&tmp_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_file() {
                    info!("Removing tmp file: {:?}", path);
                    let _ = fs::remove_file(path);
                }
            }
        }

        // 3. Clean up WAL files <= last_sealed_wal_id (their contents are
        //    already durable in segments). Leftover from a crash between
        //    segment commit and WAL deletion.
        let wal_dir = self.db_root.join("wal");
        if wal_dir.exists() && last_sealed_wal_id > 0 {
            Wal::delete_files_through(&wal_dir, last_sealed_wal_id)?;
        }

        // 4. Replay WAL files > last_sealed_wal_id: rebuild dedupe AND
        //    refill the memtable so events are visible and will be
        //    re-flushed on the next memtable rotation.
        let mut dedupe = HotDedupe::new(dedupe_capacity);
        let mut memtable = Memtable::new();

        if wal_dir.exists() {
            let file_ids = Wal::list_files_after(&wal_dir, last_sealed_wal_id)?;
            let mut total_events = 0usize;
            for id in &file_ids {
                let events = Wal::replay_file(&wal_dir, *id)?;
                for event in events {
                    let (event_id_hash, payload_hash) = compute_event_hashes(&event);
                    dedupe.insert_known(event_id_hash, payload_hash, event.ingested_at_ms);
                    memtable.insert(event);
                    total_events += 1;
                }
            }
            if !file_ids.is_empty() {
                info!(
                    "WAL replay: {} unflushed events recovered into memtable across {} file(s)",
                    total_events, file_ids.len()
                );
            }
        }

        // 5. Rebuild dedupe from recent raw segments (review P0 #2).
        //    The hot cache normally tracks every ack'd event for TTL
        //    minutes/days. Across a restart, sealed-and-deleted WAL files
        //    means dedupe entries vanish — so a retry of an event we
        //    already accepted before the crash would re-bill. Scan
        //    segments whose max timestamp is within the TTL window and
        //    re-register their events.
        let cutoff = now_ms_recovery().saturating_sub(DEFAULT_TTL_MS);
        let mut segments_scanned = 0usize;
        let mut events_registered = 0usize;
        for seg in &manifest.raw_segments {
            if seg.max_timestamp_ms < cutoff {
                continue;
            }
            let path = self.db_root.join(format!("{}.seg", seg.segment_id));
            if !path.exists() {
                warn!(
                    "recovery: manifest references missing raw segment {} — dedupe will be incomplete for its events",
                    seg.segment_id
                );
                continue;
            }
            match RawSegmentReader::new(path) {
                Ok(mut reader) => {
                    loop {
                        match reader.read_next() {
                            Ok(Some(event)) => {
                                let (id_hash, payload_hash) = compute_event_hashes(&event);
                                dedupe.insert_known(id_hash, payload_hash, event.ingested_at_ms);
                                events_registered += 1;
                            }
                            Ok(None) => break,
                            Err(e) => {
                                warn!("recovery: error reading segment {}: {}", seg.segment_id, e);
                                break;
                            }
                        }
                    }
                    segments_scanned += 1;
                }
                Err(e) => warn!("recovery: failed to open segment {}: {}", seg.segment_id, e),
            }
        }
        if segments_scanned > 0 {
            info!(
                "Dedupe rebuild: scanned {} recent segments, registered {} events",
                segments_scanned, events_registered
            );
        }

        info!(
            "Recovery complete: manifest loaded ({} segments), dedupe rebuilt ({} entries), memtable size {} bytes",
            manifest.raw_segments.len(),
            dedupe.len(),
            memtable.size_bytes(),
        );

        Ok(RecoveryResult {
            manifest,
            dedupe,
            memtable,
        })
    }
}

/// Hash an event for dedupe purposes. Uses blake3 to produce stable 128-bit
/// identities; collision probability at 10^9 events is ~2^-67. The payload
/// hash zeroes `ingested_at_ms` so that retries with a different ingest
/// timestamp are still recognized as the same payload.
pub fn compute_event_hashes(event: &UsageEvent) -> (crate::ingest::dedupe::EventHash, crate::ingest::dedupe::EventHash) {
    let event_id_hash = blake3_u128(event.event_id.0.as_bytes());

    let mut ev_clone = event.clone();
    ev_clone.ingested_at_ms = 0;
    let payload_hash = match bincode::serialize(&ev_clone) {
        Ok(bytes) => blake3_u128(&bytes),
        // bincode failure on a well-formed UsageEvent is effectively
        // impossible (no non-serializable fields), but if it does happen
        // we degrade to a zero hash so the event is treated as conflict-
        // prone rather than silently deduped.
        Err(_) => 0,
    };

    (event_id_hash, payload_hash)
}

fn now_ms_recovery() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn blake3_u128(data: &[u8]) -> u128 {
    let hash = blake3::hash(data);
    let bytes = hash.as_bytes();
    let mut buf = [0u8; 16];
    buf.copy_from_slice(&bytes[..16]);
    u128::from_le_bytes(buf)
}
