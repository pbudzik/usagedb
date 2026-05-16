use std::path::PathBuf;
use std::fs;
use crate::storage::manifest::Manifest;
use crate::ingest::wal::Wal;
use crate::ingest::dedupe::HotDedupe;
use crate::ingest::memtable::Memtable;
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
        let manifest = if manifest_path.exists() {
            let data = fs::read_to_string(&manifest_path)?;
            match serde_json::from_str(&data) {
                Ok(m) => {
                    info!("Loaded manifest with {} raw segments", {
                        let m: &Manifest = &m;
                        m.raw_segments.len()
                    });
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

        // 3. Collect committed segment IDs for WAL filtering
        let committed_segment_ids: std::collections::HashSet<String> = manifest
            .raw_segments
            .iter()
            .map(|s| s.segment_id.clone())
            .collect();

        // 4. Replay WAL to rebuild dedupe cache and recover unflushed events
        let wal_path = self.db_root.join("wal.jsonl");
        let wal_events = Wal::replay(&wal_path)?;

        let mut dedupe = HotDedupe::new(dedupe_capacity);
        let memtable = Memtable::new();

        // Rebuild dedupe from all WAL events (they represent the recent history).
        // Events that are already in committed segments don't need to go to the memtable,
        // but we still register them in dedupe for conflict detection.
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        for event in &wal_events {
            let mut s1 = DefaultHasher::new();
            event.event_id.hash(&mut s1);
            let event_id_hash = s1.finish();

            let mut s2 = DefaultHasher::new();
            let mut ev_clone = event.clone();
            ev_clone.ingested_at_ms = 0;
            if let Ok(bytes) = bincode::serialize(&ev_clone) {
                std::hash::Hash::hash_slice(&bytes, &mut s2);
            }
            let payload_hash = s2.finish();

            dedupe.insert_known(event_id_hash, payload_hash, event.ingested_at_ms);
        }

        info!(
            "Recovery complete: manifest loaded ({} segments), dedupe rebuilt ({} entries)",
            committed_segment_ids.len(),
            dedupe.len()
        );

        Ok(RecoveryResult {
            manifest,
            dedupe,
            memtable,
        })
    }
}
