use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AccountId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SubscriptionId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProductId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MeterId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourceId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ModelId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Unit(pub String);

/// Map an account to its bucket within a given bucket_count. Uses blake3 so
/// the assignment is stable across Rust versions and builds. The bucket
/// count is fixed at DB creation time (stored in the manifest) — changing
/// it would invalidate every existing segment's bucket label.
pub fn bucket_for_account(account_id: &AccountId, bucket_count: u32) -> u32 {
    if bucket_count == 0 {
        return 0;
    }
    let hash = blake3::hash(account_id.0.as_bytes());
    let bytes = hash.as_bytes();
    let prefix = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    prefix % bucket_count
}
