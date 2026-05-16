use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct DedupeEntry {
    pub payload_hash: u64,
    pub first_seen_ms: i64,
}

#[derive(Default)]
pub struct HotDedupe {
    cache: HashMap<u64, DedupeEntry>,
}

pub enum DedupeResult {
    NewEvent,
    ExactDuplicate,
    PayloadConflict,
}

impl HotDedupe {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn check_and_insert(&mut self, event_id_hash: u64, payload_hash: u64) -> DedupeResult {
        if let Some(existing) = self.cache.get(&event_id_hash) {
            if existing.payload_hash == payload_hash {
                DedupeResult::ExactDuplicate
            } else {
                DedupeResult::PayloadConflict
            }
        } else {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            self.cache.insert(
                event_id_hash,
                DedupeEntry {
                    payload_hash,
                    first_seen_ms: now,
                },
            );
            DedupeResult::NewEvent
        }
    }
}
