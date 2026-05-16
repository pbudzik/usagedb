use std::collections::{HashMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct DedupeEntry {
    pub payload_hash: u64,
    pub first_seen_ms: i64,
}

pub struct HotDedupe {
    cache: HashMap<u64, DedupeEntry>,
    order: VecDeque<u64>,
    max_capacity: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DedupeResult {
    NewEvent,
    ExactDuplicate,
    PayloadConflict,
}

impl HotDedupe {
    pub fn new(max_capacity: usize) -> Self {
        Self {
            cache: HashMap::new(),
            order: VecDeque::new(),
            max_capacity,
        }
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
            self.order.push_back(event_id_hash);
            
            if self.cache.len() > self.max_capacity {
                if let Some(oldest) = self.order.pop_front() {
                    self.cache.remove(&oldest);
                }
            }
            
            DedupeResult::NewEvent
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hot_dedupe() {
        let mut dedupe = HotDedupe::new(100);
        
        // First insertion should be NewEvent
        let res1 = dedupe.check_and_insert(1234, 5678);
        assert_eq!(res1, DedupeResult::NewEvent);

        // Exact duplicate (same event_id, same payload)
        let res2 = dedupe.check_and_insert(1234, 5678);
        assert_eq!(res2, DedupeResult::ExactDuplicate);

        // Payload conflict (same event_id, different payload)
        let res3 = dedupe.check_and_insert(1234, 9999);
        assert_eq!(res3, DedupeResult::PayloadConflict);

        // Another new event
        let res4 = dedupe.check_and_insert(4321, 5678);
        assert_eq!(res4, DedupeResult::NewEvent);
    }
}

