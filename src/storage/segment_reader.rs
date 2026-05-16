use std::fs::File;
use std::io::Result as IoResult;
use std::path::PathBuf;
use crate::model::event::UsageEvent;
use crate::model::ids::AccountId;
use bincode;
use memmap2::Mmap;

pub struct RawSegmentReader {
    _file: File, // Keep file alive
    mmap: Mmap,
    cursor: usize,
}

impl RawSegmentReader {
    pub fn new(path: PathBuf) -> IoResult<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(Self { _file: file, mmap, cursor: 0 })
    }

    pub fn read_next(&mut self) -> IoResult<Option<UsageEvent>> {
        if self.cursor >= self.mmap.len() {
            return Ok(None);
        }

        if self.cursor + 4 > self.mmap.len() {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Incomplete length header"));
        }

        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&self.mmap[self.cursor..self.cursor + 4]);
        let len = u32::from_le_bytes(len_bytes) as usize;
        self.cursor += 4;

        if self.cursor + len > self.mmap.len() {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Incomplete data payload"));
        }

        let data = &self.mmap[self.cursor..self.cursor + len];
        self.cursor += len;
        
        let event: UsageEvent = bincode::deserialize(data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            
        Ok(Some(event))
    }

    pub fn scan_by_account(mut self, target_account: &AccountId) -> IoResult<Vec<UsageEvent>> {
        let mut results = Vec::new();
        while let Some(event) = self.read_next()? {
            if &event.account_id == target_account {
                results.push(event);
            }
        }
        Ok(results)
    }
}
