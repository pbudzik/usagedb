use crate::model::ids::{AccountId, ProductId, MeterId};
use std::collections::HashSet;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockMeta {
    pub row_start: u32,
    pub row_count: u32,
    pub min_timestamp_ms: i64,
    pub max_timestamp_ms: i64,
    pub min_account_id: AccountId,
    pub max_account_id: AccountId,
    pub product_ids: HashSet<ProductId>,
    pub meter_ids: HashSet<MeterId>,
    pub offset: u64,
    pub len: u32,
}

impl BlockMeta {
    pub fn might_contain(&self, account_id: Option<&AccountId>, product_id: Option<&ProductId>, min_time: i64, max_time: i64) -> bool {
        if let Some(acc) = account_id {
            if acc.0 < self.min_account_id.0 || acc.0 > self.max_account_id.0 {
                return false;
            }
        }
        if let Some(prod) = product_id {
            if !self.product_ids.contains(prod) {
                return false;
            }
        }
        if min_time > self.max_timestamp_ms || max_time < self.min_timestamp_ms {
            return false;
        }
        true
    }
}
