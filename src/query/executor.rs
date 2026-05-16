use crate::query::plan::QueryPlan;
use serde_json::Value;

pub fn execute_plan(plan: &QueryPlan) -> Vec<Value> {
    // Basic mock implementation for MVP
    // In a real system, this would:
    // 1. Scan manifest to find overlapping segments
    // 2. Open RawSegmentReader or RollupSegmentReader
    // 3. Filter using BlockMeta
    // 4. Perform group_by aggregations in memory using HashMaps
    // 5. Return JSON rows
    
    vec![serde_json::json!({
        "account_id": plan.account_id,
        "metrics": "mock_data",
        "plan_source": format!("{:?}", plan.source)
    })]
}
