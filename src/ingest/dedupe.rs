use std::collections::{HashMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

/// Default TTL of 7 days in milliseconds, covering typical retry windows.
/// Public so the recovery path can use the same window when rebuilding
/// dedupe from recent raw segments (review P0 #2).
pub const DEFAULT_TTL_MS: i64 = 7 * 24 * 3600 * 1000;

/// Event identity hash: 128 bits derived from blake3, stable across Rust
/// versions and large enough that birthday collisions are negligible at
/// billing scale (~10^9 events ⇒ collision probability ≈ 2^-67).
pub type EventHash = u128;

#[derive(Debug, Clone)]
pub struct DedupeEntry {
    pub payload_hash: EventHash,
    pub first_seen_ms: i64,
}

pub struct HotDedupe {
    cache: HashMap<EventHash, DedupeEntry>,
    /// Insertion order for FIFO eviction when capacity is exceeded.
    order: VecDeque<(EventHash, i64)>, // (event_id_hash, inserted_at_ms)
    max_capacity: usize,
    ttl_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DedupeResult {
    NewEvent,
    ExactDuplicate,
    PayloadConflict,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

impl HotDedupe {
    pub fn new(max_capacity: usize) -> Self {
        Self {
            cache: HashMap::new(),
            order: VecDeque::new(),
            max_capacity,
            ttl_ms: DEFAULT_TTL_MS,
        }
    }

    pub fn with_ttl(mut self, ttl_ms: i64) -> Self {
        self.ttl_ms = ttl_ms;
        self
    }

    /// Classify an event against the cache without mutating state. Used by
    /// the ingest hot path to decide whether to WAL-append before committing
    /// the dedupe entry.
    pub fn classify(&self, event_id_hash: EventHash, payload_hash: EventHash) -> DedupeResult {
        if let Some(existing) = self.cache.get(&event_id_hash) {
            if existing.payload_hash == payload_hash {
                DedupeResult::ExactDuplicate
            } else {
                DedupeResult::PayloadConflict
            }
        } else {
            DedupeResult::NewEvent
        }
    }

    /// Commit a previously-classified `NewEvent` into the cache. Safe to call
    /// after WAL durability has been established. No-op if a concurrent
    /// caller already inserted the same hash.
    pub fn commit(&mut self, event_id_hash: EventHash, payload_hash: EventHash) {
        self.evict_expired();
        if self.cache.contains_key(&event_id_hash) {
            return;
        }
        let now = now_ms();
        self.cache.insert(
            event_id_hash,
            DedupeEntry {
                payload_hash,
                first_seen_ms: now,
            },
        );
        self.order.push_back((event_id_hash, now));

        while self.cache.len() > self.max_capacity {
            if let Some((oldest_hash, _)) = self.order.pop_front() {
                self.cache.remove(&oldest_hash);
            } else {
                break;
            }
        }
    }

    /// Convenience wrapper that classifies and (if NewEvent) commits in one
    /// step. Used by tests; the hot path should call classify + commit
    /// separately around the WAL append.
    pub fn check_and_insert(&mut self, event_id_hash: EventHash, payload_hash: EventHash) -> DedupeResult {
        let result = self.classify(event_id_hash, payload_hash);
        if result == DedupeResult::NewEvent {
            self.commit(event_id_hash, payload_hash);
        }
        result
    }

    /// Insert a known event during WAL replay without returning a dedupe result.
    /// This rebuilds the hot cache from durable state.
    pub fn insert_known(&mut self, event_id_hash: EventHash, payload_hash: EventHash, first_seen_ms: i64) {
        if self.cache.contains_key(&event_id_hash) {
            return;
        }
        self.cache.insert(
            event_id_hash,
            DedupeEntry {
                payload_hash,
                first_seen_ms,
            },
        );
        self.order.push_back((event_id_hash, first_seen_ms));
    }

    /// Remove entries older than `ttl_ms` from the front of the insertion
    /// queue. The cache only inserts (never updates), so an entry in
    /// `order` whose timestamp is past the cutoff has the same first_seen
    /// in `cache` — drop both unconditionally. `order` is monotonically
    /// increasing so we stop at the first non-stale entry.
    fn evict_expired(&mut self) {
        let cutoff = now_ms() - self.ttl_ms;
        while let Some(&(hash, inserted_at)) = self.order.front() {
            if inserted_at >= cutoff {
                break;
            }
            self.order.pop_front();
            self.cache.remove(&hash);
        }
    }

    pub fn len(&self) -> usize {
        self.cache.len()
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

    #[test]
    fn test_capacity_eviction() {
        let mut dedupe = HotDedupe::new(3);

        dedupe.check_and_insert(1, 100);
        dedupe.check_and_insert(2, 200);
        dedupe.check_and_insert(3, 300);
        assert_eq!(dedupe.len(), 3);

        // 4th insert should evict the oldest (1)
        dedupe.check_and_insert(4, 400);
        assert_eq!(dedupe.len(), 3);

        // Event 1 was evicted, so re-inserting is a NewEvent, not a duplicate
        let res = dedupe.check_and_insert(1, 100);
        assert_eq!(res, DedupeResult::NewEvent);
    }

    #[test]
    fn test_insert_known_for_replay() {
        let mut dedupe = HotDedupe::new(100);

        let recent = now_ms() - 1000; // 1 second ago, well within TTL
        dedupe.insert_known(1234, 5678, recent);
        assert_eq!(dedupe.len(), 1);

        // Should detect as duplicate
        let res = dedupe.check_and_insert(1234, 5678);
        assert_eq!(res, DedupeResult::ExactDuplicate);

        // Should detect conflict
        let res = dedupe.check_and_insert(1234, 9999);
        assert_eq!(res, DedupeResult::PayloadConflict);
    }
}
