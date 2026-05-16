use crate::model::event::UsageEvent;
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Memtable {
    events: VecDeque<UsageEvent>,
    approx_size_bytes: usize,
    /// Wall-clock timestamp when the oldest currently-buffered event was
    /// inserted. None when the memtable is empty. Used by the rollup
    /// worker to detect a memtable that's holding events long enough that
    /// the watermark would otherwise advance past them (review P0 #1).
    oldest_insert_at_ms: Option<i64>,
}

impl Memtable {
    pub fn new() -> Self {
        Self {
            events: VecDeque::new(),
            approx_size_bytes: 0,
            oldest_insert_at_ms: None,
        }
    }

    pub fn insert(&mut self, event: UsageEvent) {
        self.approx_size_bytes += Self::estimate_event_size(&event);
        if self.events.is_empty() {
            self.oldest_insert_at_ms = Some(now_ms());
        }
        self.events.push_back(event);
    }

    /// Estimate the total heap + stack size of a UsageEvent.
    /// Accounts for all String fields and the BTreeMap in dimensions.
    fn estimate_event_size(event: &UsageEvent) -> usize {
        let base = std::mem::size_of::<UsageEvent>();

        let strings = event.event_id.0.len()
            + event.account_id.0.len()
            + event.subscription_id.as_ref().map(|s| s.0.len()).unwrap_or(0)
            + event.product_id.0.len()
            + event.meter_id.0.len()
            + event.source.0.len()
            + event.unit.0.len()
            + event.model_id.as_ref().map(|m| m.0.len()).unwrap_or(0)
            + event.correction_ref.as_ref().map(|c| c.original_event_id.0.len() + c.reason.len()).unwrap_or(0);

        // BTreeMap overhead: ~64 bytes per entry for node overhead + key/value heap
        let dims: usize = event.dimensions.inner.iter()
            .map(|(k, v)| k.len() + v.len() + 64)
            .sum();

        base + strings + dims
    }

    pub fn size_bytes(&self) -> usize {
        self.approx_size_bytes
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Wall-clock timestamp at which the oldest currently-buffered event
    /// was inserted. Used to detect a memtable that's been sitting too
    /// long without a flush.
    pub fn oldest_insert_at_ms(&self) -> Option<i64> {
        self.oldest_insert_at_ms
    }

    /// Smallest `timestamp_ms` across buffered events. None when empty.
    /// Used by the rollup worker to keep the watermark from advancing
    /// past unflushed data.
    pub fn min_event_timestamp_ms(&self) -> Option<i64> {
        self.events.iter().map(|e| e.timestamp_ms).min()
    }

    pub fn drain_all(&mut self) -> Vec<UsageEvent> {
        let events = self.events.drain(..).collect();
        self.approx_size_bytes = 0;
        self.oldest_insert_at_ms = None;
        events
    }

    /// Clone the current contents for read-only access (e.g. queries scanning
    /// unflushed data).
    pub fn snapshot(&self) -> Vec<UsageEvent> {
        self.events.iter().cloned().collect()
    }
}

impl Default for Memtable {
    fn default() -> Self {
        Self::new()
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
