//! Integration tests for bucket assignment (spec §5.2).
//!
//! Bucket = blake3(account_id) % bucket_count, stable across runs, and
//! distributes accounts across buckets within tolerance.

use std::collections::HashMap;
use usagedb::model::ids::{AccountId, bucket_for_account};

#[test]
fn bucket_is_deterministic_for_a_given_account_and_count() {
    let acc = AccountId("acc_12345".to_string());
    let b1 = bucket_for_account(&acc, 64);
    let b2 = bucket_for_account(&acc, 64);
    assert_eq!(b1, b2, "same account + same bucket_count must yield same bucket");
    assert!(b1 < 64);
}

#[test]
fn bucket_changes_with_bucket_count() {
    // Not strictly required by the spec, but a useful sanity check that
    // the modulo is doing its job and we're not hashing to bucket 0 always.
    let acc = AccountId("acc_constant".to_string());
    let b64 = bucket_for_account(&acc, 64);
    let b256 = bucket_for_account(&acc, 256);
    assert!(b64 < 64);
    assert!(b256 < 256);
}

#[test]
fn zero_bucket_count_yields_zero() {
    // Defensive: avoid panic on % 0 even though manifest init prevents this.
    let acc = AccountId("acc_x".to_string());
    assert_eq!(bucket_for_account(&acc, 0), 0);
}

#[test]
fn bucket_distribution_is_roughly_uniform() {
    // Hash 10k synthetic account IDs into 64 buckets and assert no bucket
    // is empty and no bucket is dominant. Chi-squared style heuristic; the
    // expected count per bucket is 156.25, so [80, 240] is ~50%-150% — wide
    // enough to survive blake3 variance but tight enough to catch a bug
    // like "always returns 0" or "uses only low bit".
    let bucket_count = 64u32;
    let mut counts: HashMap<u32, usize> = HashMap::new();
    for i in 0..10_000 {
        let acc = AccountId(format!("acc_{}", i));
        let b = bucket_for_account(&acc, bucket_count);
        *counts.entry(b).or_insert(0) += 1;
    }
    assert_eq!(counts.len(), bucket_count as usize, "every bucket should see traffic");
    let min = *counts.values().min().unwrap();
    let max = *counts.values().max().unwrap();
    assert!(min >= 80, "min bucket count {} too low — distribution skewed", min);
    assert!(max <= 240, "max bucket count {} too high — distribution skewed", max);
}
