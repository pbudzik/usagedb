use std::path::PathBuf;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write, Result as IoResult};
use crate::model::event::UsageEvent;
use tracing::{info, warn};

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

    /// Replay all events from the WAL file. Tolerates corrupt trailing lines
    /// (e.g. from a crash mid-write) by logging warnings and skipping them.
    pub fn replay(path: &PathBuf) -> IoResult<Vec<UsageEvent>> {
        if !path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut events = Vec::new();
        let mut line_num = 0u64;
        let mut skipped = 0u64;

        for line_result in reader.lines() {
            line_num += 1;
            let line = match line_result {
                Ok(l) => l,
                Err(e) => {
                    warn!("WAL replay: I/O error at line {}: {}", line_num, e);
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
                    warn!("WAL replay: corrupt entry at line {}: {}", line_num, e);
                    skipped += 1;
                }
            }
        }

        info!(
            "WAL replay complete: {} events recovered, {} lines skipped",
            events.len(),
            skipped
        );

        Ok(events)
    }
}
