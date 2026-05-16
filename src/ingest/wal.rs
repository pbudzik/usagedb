use std::path::{Path, PathBuf};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write, Result as IoResult};
use crate::model::event::UsageEvent;
use tracing::{info, warn};

/// Append-only write-ahead log split across numbered files. The active file
/// (highest id) receives new appends through a BufWriter — writes are
/// coalesced in a userspace buffer (8 KiB by default) so the kernel sees
/// one bulk write per batch instead of one per event. `sync` flushes the
/// buffer before fsync to maintain the durability contract.
///
/// On `rotate`, the buffer is flushed, the current file closed, and a new
/// one with id+1 opened; the returned id is the now-sealed file. After the
/// flusher commits a segment containing those events, sealed files can be
/// deleted via `delete_files_through`.
pub struct Wal {
    pub dir: PathBuf,
    pub active_id: u64,
    file: BufWriter<File>,
}

fn wal_filename(id: u64) -> String {
    format!("wal-{:06}.log", id)
}

fn parse_wal_id(name: &str) -> Option<u64> {
    let stem = name.strip_suffix(".log")?;
    let num = stem.strip_prefix("wal-")?;
    num.parse().ok()
}

impl Wal {
    /// Open the WAL directory and select the active file. If files exist with
    /// id > last_sealed_id, the highest such id becomes active (so we append
    /// to it). Otherwise we start fresh at last_sealed_id + 1.
    pub fn open(dir: PathBuf, last_sealed_id: u64) -> IoResult<Self> {
        std::fs::create_dir_all(&dir)?;

        let mut highest_unsealed: Option<u64> = None;
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = match name.to_str() {
                Some(s) => s,
                None => continue,
            };
            if let Some(id) = parse_wal_id(name) {
                if id > last_sealed_id {
                    highest_unsealed = Some(highest_unsealed.map_or(id, |h| h.max(id)));
                }
            }
        }

        let active_id = highest_unsealed.unwrap_or(last_sealed_id + 1);
        let path = dir.join(wal_filename(active_id));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        Ok(Self { dir, active_id, file: BufWriter::new(file) })
    }

    /// Append a batch of events. Writes go through the BufWriter, so they
    /// are not durable until `sync` (Strict mode) returns. In Fast mode
    /// the caller skips sync and relies on OS buffering.
    pub fn append_batch<'a, I>(&mut self, events: I) -> IoResult<()>
    where
        I: IntoIterator<Item = &'a UsageEvent>,
    {
        for event in events {
            let json = serde_json::to_string(event)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            writeln!(self.file, "{}", json)?;
        }
        Ok(())
    }

    /// Flush the userspace buffer to the kernel, then fsync the file.
    /// Required for Strict durability before acking the batch.
    pub fn sync(&mut self) -> IoResult<()> {
        self.file.flush()?;
        self.file.get_ref().sync_data()
    }

    /// Flush the userspace buffer without an fsync. Used by Fast mode so
    /// the bytes reach the kernel page cache before we ack, but we skip
    /// the disk round-trip.
    pub fn flush_buffer(&mut self) -> IoResult<()> {
        self.file.flush()
    }

    /// Seal the current active file and open the next one. Returns the id of
    /// the sealed file. Caller must guarantee the events in the sealed file
    /// are about to be flushed to a segment; only after segment+manifest
    /// commit should `delete_files_through(sealed_id)` be called.
    pub fn rotate(&mut self) -> IoResult<u64> {
        // Drain the buffer to the sealed file before closing it, otherwise
        // unflushed events would be dropped when the BufWriter is replaced.
        self.file.flush()?;
        self.file.get_ref().sync_data()?;
        let sealed_id = self.active_id;
        self.active_id += 1;
        let new_path = self.dir.join(wal_filename(self.active_id));
        let new_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&new_path)?;
        self.file = BufWriter::new(new_file);
        // Fsync the directory so the new file's directory entry is durable.
        if let Ok(dir_handle) = File::open(&self.dir) {
            let _ = dir_handle.sync_all();
        }
        Ok(sealed_id)
    }

    /// List WAL file IDs strictly greater than `last_sealed_id`, sorted.
    pub fn list_files_after(dir: &Path, last_sealed_id: u64) -> IoResult<Vec<u64>> {
        let mut ids = Vec::new();
        if !dir.exists() {
            return Ok(ids);
        }
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            if let Some(s) = name.to_str() {
                if let Some(id) = parse_wal_id(s) {
                    if id > last_sealed_id {
                        ids.push(id);
                    }
                }
            }
        }
        ids.sort();
        Ok(ids)
    }

    /// Replay events from a single WAL file. Tolerates corrupt trailing lines.
    pub fn replay_file(dir: &Path, id: u64) -> IoResult<Vec<UsageEvent>> {
        let path = dir.join(wal_filename(id));
        if !path.exists() {
            return Ok(Vec::new());
        }
        let file = File::open(&path)?;
        let reader = BufReader::new(file);
        let mut events = Vec::new();
        let mut line_num = 0u64;
        let mut skipped = 0u64;

        for line_result in reader.lines() {
            line_num += 1;
            let line = match line_result {
                Ok(l) => l,
                Err(e) => {
                    warn!("WAL replay {}: I/O error at line {}: {}", id, line_num, e);
                    skipped += 1;
                    continue;
                }
            };
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<UsageEvent>(&line) {
                Ok(event) => events.push(event),
                Err(e) => {
                    warn!("WAL replay {}: corrupt entry at line {}: {}", id, line_num, e);
                    skipped += 1;
                }
            }
        }

        if skipped > 0 {
            info!("WAL file {}: {} events recovered, {} lines skipped", id, events.len(), skipped);
        }
        Ok(events)
    }

    /// Delete all WAL files with id <= up_to_id. Used after a segment commit
    /// where those files' events are now in durable segments.
    pub fn delete_files_through(dir: &Path, up_to_id: u64) -> IoResult<()> {
        if !dir.exists() {
            return Ok(());
        }
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            if let Some(s) = name.to_str() {
                if let Some(id) = parse_wal_id(s) {
                    if id <= up_to_id {
                        let _ = std::fs::remove_file(entry.path());
                    }
                }
            }
        }
        Ok(())
    }
}
