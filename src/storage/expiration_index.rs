//! Expiration index for efficient active expiration
//!
//! Redis 6.0+ uses a radix tree to track keys with expiration sorted by expire time.
//! This allows O(1) sampling of keys with TTL and efficient range queries for expired keys.
//!
//! Implementation:
//! - BTreeMap<i64, DashSet<Bytes>>: expiration_ms -> set of keys
//! - Allows fast sampling of random keys that have expiration
//! - Supports efficient deletion of expired keys in batches

use bytes::Bytes;
use dashmap::DashSet;
use parking_lot::RwLock;
use std::collections::BTreeMap;

/// Expiration index tracking keys with TTL sorted by expiration time
///
/// This structure enables the Redis-style active expiration algorithm:
/// 1. Sample 20 random keys that have expiration
/// 2. Delete expired keys
/// 3. If >25% expired, repeat (adaptive)
pub struct ExpirationIndex {
    /// Map from expiration time (ms) to set of keys expiring at that time
    /// Using BTreeMap for sorted access (range queries)
    /// Using DashSet for concurrent key additions/removals at same expiration time
    index: RwLock<BTreeMap<i64, DashSet<Bytes>>>,
}

impl ExpirationIndex {
    /// Create a new expiration index
    pub fn new() -> Self {
        Self {
            index: RwLock::new(BTreeMap::new()),
        }
    }

    /// Add a key with expiration time
    /// Called when setting TTL on a key
    #[inline]
    pub fn add(&self, key: Bytes, expire_at_ms: i64) {
        let mut index = self.index.write();
        index
            .entry(expire_at_ms)
            .or_insert_with(DashSet::new)
            .insert(key);
    }

    /// Remove a key from the index
    /// Called when:
    /// - Key is deleted
    /// - Key's TTL is removed (PERSIST)
    /// - Key's TTL is changed (remove old, add new)
    #[inline]
    pub fn remove(&self, key: &Bytes, expire_at_ms: i64) {
        let mut index = self.index.write();
        if let Some(keys) = index.get(&expire_at_ms) {
            keys.remove(key);
            // Clean up empty buckets to prevent memory leak
            if keys.is_empty() {
                index.remove(&expire_at_ms);
            }
        }
    }

    /// Update a key's expiration time (remove from old, add to new)
    #[inline]
    pub fn update(&self, key: Bytes, old_expire_at_ms: Option<i64>, new_expire_at_ms: i64) {
        if let Some(old_time) = old_expire_at_ms {
            if old_time != new_expire_at_ms {
                self.remove(&key, old_time);
            }
        }
        self.add(key, new_expire_at_ms);
    }

    /// Sample random keys with expiration
    /// Returns up to `count` keys that have expiration set
    ///
    /// Algorithm:
    /// 1. Get random expiration buckets
    /// 2. Sample random keys from those buckets
    /// 3. Return keys for expiration check
    pub fn sample_random(&self, count: usize) -> Vec<Bytes> {
        let index = self.index.read();
        if index.is_empty() {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(count);
        let total_buckets = index.len();

        // Sample from random buckets
        for _ in 0..count {
            // Pick a random bucket
            let bucket_idx = fastrand::usize(0..total_buckets);
            if let Some((_, keys)) = index.iter().nth(bucket_idx) {
                // Pick a random key from the bucket
                if !keys.is_empty() {
                    // DashSet doesn't have efficient random access, so we iterate
                    // This is acceptable since buckets are typically small
                    let key_count = keys.len();
                    if key_count > 0 {
                        let key_idx = fastrand::usize(0..key_count);
                        if let Some(key) = keys.iter().nth(key_idx) {
                            result.push(key.key().clone());
                        }
                    }
                }
            }

            if result.len() >= count {
                break;
            }
        }

        result
    }

    /// Get all expired keys up to current time
    /// Used for batch deletion of expired keys
    #[inline]
    #[allow(dead_code)]
    pub fn get_expired_keys(&self, now_ms: i64, limit: usize) -> Vec<(i64, Vec<Bytes>)> {
        let index = self.index.read();
        let mut result = Vec::new();
        let mut total = 0;

        // Get all buckets with expiration time <= now
        for (&expire_at, keys) in index.range(..=now_ms) {
            let keys_vec: Vec<Bytes> = keys.iter().map(|k| k.key().clone()).collect();
            total += keys_vec.len();
            result.push((expire_at, keys_vec));

            if total >= limit {
                break;
            }
        }

        result
    }

    /// Get approximate count of keys with expiration
    #[inline]
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        let index = self.index.read();
        index.values().map(|keys| keys.len()).sum()
    }

    /// Check if index is empty
    #[inline]
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.index.read().is_empty()
    }

    /// Clear all entries (used for FLUSHDB/FLUSHALL)
    #[inline]
    pub fn clear(&self) {
        self.index.write().clear();
    }

    /// Get number of expiration buckets
    #[inline]
    #[allow(dead_code)]
    pub fn bucket_count(&self) -> usize {
        self.index.read().len()
    }
}

impl Default for ExpirationIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_remove() {
        let idx = ExpirationIndex::new();
        let key = Bytes::from_static(b"key1");

        idx.add(key.clone(), 1000);
        assert_eq!(idx.len(), 1);

        idx.remove(&key, 1000);
        assert_eq!(idx.len(), 0);
        assert!(idx.is_empty());
    }

    #[test]
    fn test_update() {
        let idx = ExpirationIndex::new();
        let key = Bytes::from_static(b"key1");

        idx.add(key.clone(), 1000);
        idx.update(key.clone(), Some(1000), 2000);

        assert_eq!(idx.len(), 1);
        assert_eq!(idx.bucket_count(), 1);
    }

    #[test]
    fn test_sample_random() {
        let idx = ExpirationIndex::new();

        for i in 0..100 {
            let key = Bytes::from(format!("key{}", i));
            idx.add(key, 1000 + i);
        }

        let samples = idx.sample_random(20);
        assert!(samples.len() <= 20);
    }

    #[test]
    fn test_get_expired_keys() {
        let idx = ExpirationIndex::new();

        // Add keys with different expiration times
        idx.add(Bytes::from_static(b"key1"), 1000);
        idx.add(Bytes::from_static(b"key2"), 2000);
        idx.add(Bytes::from_static(b"key3"), 3000);
        idx.add(Bytes::from_static(b"key4"), 1000); // Same as key1

        let expired = idx.get_expired_keys(2500, 100);

        // Should get buckets at 1000 and 2000
        assert_eq!(expired.len(), 2);

        let total_keys: usize = expired.iter().map(|(_, keys)| keys.len()).sum();
        assert_eq!(total_keys, 3); // key1, key2, key4
    }

    #[test]
    fn test_empty_bucket_cleanup() {
        let idx = ExpirationIndex::new();
        let key = Bytes::from_static(b"key1");

        idx.add(key.clone(), 1000);
        assert_eq!(idx.bucket_count(), 1);

        idx.remove(&key, 1000);
        assert_eq!(idx.bucket_count(), 0); // Bucket should be removed
    }
}
