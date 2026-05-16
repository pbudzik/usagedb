use crate::model::event::UsageEvent;
use std::collections::VecDeque;

pub struct Memtable {
    events: VecDeque<UsageEvent>,
    approx_size_bytes: usize,
}

impl Memtable {
    pub fn new() -> Self {
        Self {
            events: VecDeque::new(),
            approx_size_bytes: 0,
        }
    }

    pub fn insert(&mut self, event: UsageEvent) {
        self.approx_size_bytes += Self::estimate_event_size(&event);
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

    pub fn drain_all(&mut self) -> Vec<UsageEvent> {
        let events = self.events.drain(..).collect();
        self.approx_size_bytes = 0;
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
