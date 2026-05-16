use std::fs::File;
use std::io::{BufWriter, Write, Result as IoResult};
use std::path::PathBuf;
use crate::rollup::hourly::HourlyRollupRecord;
use bincode;

pub struct RollupSegmentWriter {
    pub path: PathBuf,
    writer: BufWriter<File>,
    event_count: u64,
}

impl RollupSegmentWriter {
    pub fn new(path: PathBuf) -> IoResult<Self> {
        let file = File::create(&path)?;
        let writer = BufWriter::new(file);
        Ok(Self {
            path,
            writer,
            event_count: 0,
        })
    }

    pub fn write_record(&mut self, record: &HourlyRollupRecord) -> IoResult<()> {
        let data = bincode::serialize(record).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let len = data.len() as u32;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&data)?;
        self.event_count += 1;
        Ok(())
    }

    pub fn finish(mut self) -> IoResult<u64> {
        self.writer.flush()?;
        Ok(self.event_count)
    }
}
