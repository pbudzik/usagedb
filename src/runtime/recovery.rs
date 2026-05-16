use std::path::PathBuf;
use std::fs;
use crate::storage::manifest::Manifest;
use crate::ingest::wal::Wal;
use crate::ingest::dedupe::HotDedupe;
use crate::ingest::memtable::Memtable;
use crate::model::event::UsageEvent;
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

fn blake3_u128(data: &[u8]) -> u128 {
    let hash = blake3::hash(data);
    let bytes = hash.as_bytes();
    let mut buf = [0u8; 16];
    buf.copy_from_slice(&bytes[..16]);
    u128::from_le_bytes(buf)
}
