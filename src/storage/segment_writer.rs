use std::fs::File;
use std::io::{BufWriter, Write, Result as IoResult};
use std::path::PathBuf;
use crate::model::event::UsageEvent;
use bincode;

pub struct RawSegmentWriter {
    pub path: PathBuf,
    writer: BufWriter<File>,
    event_count: u64,
}

impl RawSegmentWriter {
    pub fn new(path: PathBuf) -> IoResult<Self> {
        let file = File::create(&path)?;
        let writer = BufWriter::new(file);
        Ok(Self {
            path,
            writer,
            event_count: 0,
        })
    }

    pub fn write_event(&mut self, event: &UsageEvent) -> IoResult<()> {
        let data = bincode::serialize(event).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let len = data.len() as u32;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&data)?;
        self.event_count += 1;
        Ok(())
    }

    pub fn finish(mut self) -> IoResult<u64> {
        self.writer.flush()?;
        self.writer.into_inner()?.sync_all()?;
        Ok(self.event_count)
    }
}
