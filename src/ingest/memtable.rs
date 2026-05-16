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
        // Approximate size: static fields
        self.approx_size_bytes += std::mem::size_of::<UsageEvent>();
        self.events.push_back(event);
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
}

impl Default for Memtable {
    fn default() -> Self {
        Self::new()
    }
}
