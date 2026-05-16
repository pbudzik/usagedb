use std::fs::File;
use std::io::{Read, Result as IoResult};
use std::path::PathBuf;
use crate::rollup::hourly::HourlyRollupRecord;
use crate::storage::segment_format::checksum;

/// Read all rollup records from a `.rseg` file. On open, the entire file is
/// loaded into memory, its checksum is verified against the provided
/// `expected_checksum` (sourced from the manifest's SegmentMeta), and
/// records are decoded in order.
pub struct RollupSegmentReader {
    records: Vec<HourlyRollupRecord>,
    cursor: usize,
}

impl RollupSegmentReader {
    /// Open a rollup segment and verify it matches the manifest's checksum.
    /// Returns InvalidData if the file is truncated or has been tampered.
    pub fn open(path: PathBuf, expected_checksum: u64) -> IoResult<Self> {
        let mut file = File::open(&path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;

        let computed = checksum(&bytes);
        if computed != expected_checksum {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "rollup segment checksum mismatch (stored {:#x}, computed {:#x})",
                    expected_checksum, computed
                ),
            ));
        }

        // Decode the length-prefixed bincode stream.
        let mut records = Vec::new();
        let mut off = 0usize;
        while off < bytes.len() {
            if off + 4 > bytes.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "rollup segment: truncated length header",
                ));
            }
            let len = u32::from_le_bytes([bytes[off], bytes[off + 1], bytes[off + 2], bytes[off + 3]]) as usize;
            off += 4;
            if off + len > bytes.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "rollup segment: truncated record payload",
                ));
            }
            let record: HourlyRollupRecord = bincode::deserialize(&bytes[off..off + len])
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            records.push(record);
            off += len;
        }

        Ok(Self { records, cursor: 0 })
    }

    pub fn row_count(&self) -> usize {
        self.records.len()
    }

    pub fn read_next(&mut self) -> Option<HourlyRollupRecord> {
        if self.cursor >= self.records.len() {
            return None;
        }
        let r = self.records[self.cursor].clone();
        self.cursor += 1;
        Some(r)
    }

    /// Consume the reader and return all records.
    pub fn into_records(self) -> Vec<HourlyRollupRecord> {
        self.records
    }
}
