use std::fs::File;
use std::io::{BufWriter, Write, Result as IoResult};
use std::path::PathBuf;
use crate::rollup::hourly::HourlyRollupRecord;
use crate::storage::segment_format::checksum;

/// Simple length-prefixed bincode writer for hourly rollup records.
/// Rollup segments are far smaller than raw segments (one row per
/// hour×bucket×dimension tuple, not per event), so the columnar format
/// the raw segments use isn't a meaningful win here yet. A future PR
/// can switch this to the same columnar layout once it matters.
pub struct RollupSegmentWriter {
    pub path: PathBuf,
    writer: BufWriter<File>,
    row_count: u64,
}

impl RollupSegmentWriter {
    pub fn new(path: PathBuf) -> IoResult<Self> {
        let file = File::create(&path)?;
        Ok(Self {
            path,
            writer: BufWriter::new(file),
            row_count: 0,
        })
    }

    pub fn write_record(&mut self, record: &HourlyRollupRecord) -> IoResult<()> {
        let data = bincode::serialize(record)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let len = data.len() as u32;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&data)?;
        self.row_count += 1;
        Ok(())
    }

    /// Flush, fsync, then re-read the file to compute a blake3 checksum.
    /// Returns (row_count, checksum). The checksum is stored in
    /// SegmentMeta so the manifest carries an integrity hash for every
    /// rollup segment, matching the raw-segment guarantee.
    pub fn finish(mut self) -> IoResult<(u64, u64)> {
        self.writer.flush()?;
        self.writer.into_inner()?.sync_all()?;
        let bytes = std::fs::read(&self.path)?;
        let cs = checksum(&bytes);
        Ok((self.row_count, cs))
    }
}
