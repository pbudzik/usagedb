use std::fs::File;
use std::io::{BufReader, Read, Result as IoResult};
use std::path::PathBuf;
use crate::model::event::UsageEvent;
use crate::model::ids::AccountId;
use bincode;

pub struct RawSegmentReader {
    reader: BufReader<File>,
}

impl RawSegmentReader {
    pub fn new(path: PathBuf) -> IoResult<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        Ok(Self { reader })
    }

    pub fn read_next(&mut self) -> IoResult<Option<UsageEvent>> {
        let mut len_bytes = [0u8; 4];
        match self.reader.read_exact(&mut len_bytes) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
        
        let len = u32::from_le_bytes(len_bytes) as usize;
        let mut data = vec![0u8; len];
        self.reader.read_exact(&mut data)?;
        
        let event: UsageEvent = bincode::deserialize(&data)
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
