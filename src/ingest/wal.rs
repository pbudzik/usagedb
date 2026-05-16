use std::path::PathBuf;
use std::fs::{File, OpenOptions};
use std::io::{Write, Result as IoResult};
use crate::model::event::UsageEvent;

pub struct Wal {
    pub path: PathBuf,
    pub file: File,
}

impl Wal {
    pub fn new(path: PathBuf) -> IoResult<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        
        Ok(Self { path, file })
    }

    pub fn append_batch(&mut self, events: &[UsageEvent]) -> IoResult<()> {
        for event in events {
            let json = serde_json::to_string(event).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            writeln!(self.file, "{}", json)?;
        }
        Ok(())
    }

    pub fn sync(&self) -> IoResult<()> {
        self.file.sync_data()
    }
}
