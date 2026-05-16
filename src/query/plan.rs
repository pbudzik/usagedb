use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum QuerySource {
    RawEvents,
    RollupHourly,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryFilter {
    pub field: String,
    pub values: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AggregationFunction {
    Sum,
    Count,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPlan {
    pub source: QuerySource,
    pub account_id: Option<String>,
    pub from_ms: i64,
    pub to_ms: i64,
    pub filters: Vec<QueryFilter>,
    pub group_by: Vec<String>,
    pub metrics: HashMap<String, AggregationFunction>,
    pub limit: Option<usize>,
}
