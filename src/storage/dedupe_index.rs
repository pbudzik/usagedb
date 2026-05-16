//! Per-segment sidecar dedupe index.
//!
//! Each raw segment `raw_<uuid>.seg` gets a companion `raw_<uuid>.idx`
//! file holding `Vec<(event_id_hash, payload_hash, ingested_at_ms)>` in
//! the same row order. Recovery uses this to rebuild the hot dedupe
//! cache without decompressing + bincode-decoding the full event
//! column — a 10–100× speedup on segments with rich payloads.
//!
//! The sidecar is an *optimization*: if missing or corrupt, recovery
//! falls back to scanning the segment file with `RawSegmentReader` and
//! recomputing hashes. So the format change is non-breaking — old
//! segments without an `.idx` still recover correctly, just slower.
//!
//! File layout (the entire file):
//!   - magic       b"UDBIDX01"  (8 bytes)
//!   - count       u32 LE       (number of entries)
//!   - entries     `Vec<(u128, u128, i64)>` bincode-serialized
//!   - checksum    u64 LE       (low 8 bytes of blake3 over above)
//!
//! Corruption is detected on read; a checksum mismatch causes the
//! caller to fall back to the segment scan.

use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::ingest::dedupe::EventHash;
use crate::storage::segment_format::checksum;

pub const MAGIC: &[u8; 8] = b"UDBIDX01";

/// `(event_id_hash, payload_hash, ingested_at_ms)` for one event in
/// segment row order.
pub type DedupeEntry = (EventHash, EventHash, i64);

/// Sidecar `.idx` path for a segment.
pub fn index_path(db_root: &Path, segment_id: &str) -> PathBuf {
    db_root.join(format!("{}.idx", segment_id))
}

/// Write the index next to the segment file. Caller is responsible for
/// ordering this relative to the manifest commit — typically: write
/// segment → write index → commit manifest. If the manifest commit
/// fails, both files are orphaned; recovery cleans them up by ignoring
/// segments not in the manifest.
pub fn write_dedupe_index(path: &Path, entries: &[DedupeEntry]) -> std::io::Result<()> {
    let body = bincode::serialize(entries)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    let mut buf: Vec<u8> = Vec::with_capacity(MAGIC.len() + 4 + body.len() + 8);
    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    buf.extend_from_slice(&body);
    let cs = checksum(&buf);
    buf.extend_from_slice(&cs.to_le_bytes());

    let mut file = File::create(path)?;
    file.write_all(&buf)?;
    file.sync_all()?;
    Ok(())
}

/// Read the sidecar. Returns `Ok(None)` if the file is missing (callers
/// fall back to segment scanning). Returns `Err` for corruption /
/// truncation / checksum mismatch — also a signal to fall back.
pub fn read_dedupe_index(path: &Path) -> std::io::Result<Option<Vec<DedupeEntry>>> {
    if !path.exists() {
        return Ok(None);
    }
    let mut file = File::open(path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;

    const MIN_LEN: usize = 8 + 4 + 8; // magic + count + checksum
    if bytes.len() < MIN_LEN {
        return Err(corrupt("file too small"));
    }
    if &bytes[..MAGIC.len()] != MAGIC {
        return Err(corrupt("missing magic"));
    }
    let cs_off = bytes.len() - 8;
    let stored_cs = u64::from_le_bytes([
        bytes[cs_off],
        bytes[cs_off + 1],
        bytes[cs_off + 2],
        bytes[cs_off + 3],
        bytes[cs_off + 4],
        bytes[cs_off + 5],
        bytes[cs_off + 6],
        bytes[cs_off + 7],
    ]);
    let body = &bytes[..cs_off];
    let computed_cs = checksum(body);
    if computed_cs != stored_cs {
        return Err(corrupt(&format!(
            "checksum mismatch (stored {:#x}, computed {:#x})",
            stored_cs, computed_cs
        )));
    }

    let count = u32::from_le_bytes([
        bytes[MAGIC.len()],
        bytes[MAGIC.len() + 1],
        bytes[MAGIC.len() + 2],
        bytes[MAGIC.len() + 3],
    ]) as usize;

    let entries_bytes = &bytes[MAGIC.len() + 4..cs_off];
    let entries: Vec<DedupeEntry> = bincode::deserialize(entries_bytes)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    if entries.len() != count {
        return Err(corrupt(&format!(
            "entry count mismatch (header={}, decoded={})",
            count,
            entries.len()
        )));
    }
    Ok(Some(entries))
}

fn corrupt(msg: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("corrupt dedupe index: {}", msg),
    )
}
