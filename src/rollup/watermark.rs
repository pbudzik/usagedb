use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Default)]
pub struct WatermarkTracker {
    watermarks: Arc<RwLock<HashMap<String, i64>>>,
}

impl WatermarkTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update_watermark(&self, partition: &str, watermark_ms: i64) {
        let mut w = self.watermarks.write().unwrap();
        w.insert(partition.to_string(), watermark_ms);
    }

    pub fn get_watermark(&self, partition: &str) -> Option<i64> {
        let w = self.watermarks.read().unwrap();
        w.get(partition).copied()
    }
}
