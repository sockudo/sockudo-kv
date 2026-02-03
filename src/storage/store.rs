use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};

use super::dashtable::{DashTable, calculate_hash};
use super::expiration_index::ExpirationIndex;
use super::ops::search_ops::SearchIndex;
use super::types::Entry;
use parking_lot::RwLock;
use std::sync::Arc;

/// Encoding configuration thresholds (mirrors Redis/Dragonfly config options)
/// Controls when to use compact encodings vs full data structures
#[derive(Debug, Clone)]
pub struct EncodingConfig {
    /// Max entries in hash before using full DashTable (hash-max-listpack-entries)
    pub hash_max_listpack_entries: usize,
    /// Max value size in hash for listpack (hash-max-listpack-value)
    pub hash_max_listpack_value: usize,
    /// Max list size for single listpack node (list-max-listpack-size)
    /// Negative values = element count limit, positive = byte size limit
    pub list_max_listpack_size: i32,
    /// List compression depth - how many nodes at ends stay uncompressed (list-compress-depth)
    pub list_compress_depth: u32,
    /// Max entries in set for intset encoding (set-max-intset-entries)
    pub set_max_intset_entries: usize,
    /// Max entries in set for listpack (set-max-listpack-entries)
    pub set_max_listpack_entries: usize,
    /// Max value size for listpack in set (set-max-listpack-value)
    pub set_max_listpack_value: usize,
    /// Max entries in zset for listpack (zset-max-listpack-entries)
    pub zset_max_listpack_entries: usize,
    /// Max value size for listpack in zset (zset-max-listpack-value)
    pub zset_max_listpack_value: usize,
    /// Max bytes for HyperLogLog sparse encoding (hll-sparse-max-bytes)
    pub hll_sparse_max_bytes: usize,
    /// Max bytes per stream radix tree node (stream-node-max-bytes)
    pub stream_node_max_bytes: usize,
    /// Max entries per stream radix tree node (stream-node-max-entries)
    pub stream_node_max_entries: usize,
}

impl Default for EncodingConfig {
    fn default() -> Self {
        // Redis defaults
        Self {
            hash_max_listpack_entries: 512,
            hash_max_listpack_value: 64,
            list_max_listpack_size: -2, // Special: -2 = 8KB per node
            list_compress_depth: 0,     // No compression
            set_max_intset_entries: 512,
            set_max_listpack_entries: 128,
            set_max_listpack_value: 64,
            zset_max_listpack_entries: 128,
            zset_max_listpack_value: 64,
            hll_sparse_max_bytes: 3000,
            stream_node_max_bytes: 4096,
            stream_node_max_entries: 100,
        }
    }
}

/// Lock-free key-value store backed by DashTable.
/// Provides O(1) concurrent access with minimal contention.
///
/// Type-specific operations are implemented in separate modules under `ops/`
pub struct Store {
    /// The main data store - sharded concurrent hash table
    pub(crate) data: DashTable<(Bytes, Entry)>,
    /// Track approximate key count for INFO command
    pub(crate) key_count: AtomicU64,
    /// Expiration index for efficient active expiration (Redis 6.0+ style)
    /// Tracks keys with TTL sorted by expiration time
    pub(crate) expiration_index: ExpirationIndex,
    /// Search indexes: index_name -> SearchIndex
    pub(crate) search_indexes: DashTable<(Bytes, SearchIndex)>,
    /// Search aliases: alias -> index_name
    pub(crate) search_aliases: DashTable<(Bytes, Bytes)>,
    /// Encoding thresholds for data structure optimization
    /// Encoding thresholds for data structure optimization
    pub encoding: Arc<RwLock<EncodingConfig>>,
}

impl Store {
    // ==================== Core Generic Operations ====================

    /// Create a new store with default capacity
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Create a new store with specified capacity
    /// Dragonfly-style: one shard per CPU core for optimal concurrency under load
    /// Uses lazy allocation (capacity=0) for memory efficiency at cold start
    pub fn with_capacity(capacity: usize) -> Self {
        // Dragonfly approach: shard count = CPU cores for minimal lock contention
        let num_cpus = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4);
        // Clamp to reasonable range: min 4, max 64 shards
        let shard_count = num_cpus.clamp(4, 64).next_power_of_two();

        Self {
            // Dynamic shards based on CPU count for optimal concurrency
            data: DashTable::with_capacity_and_shard_amount(capacity, shard_count),
            key_count: AtomicU64::new(0),
            expiration_index: ExpirationIndex::new(),
            // Metadata maps rarely used, minimal 2 shards
            search_indexes: DashTable::with_shard_amount(2),
            search_aliases: DashTable::with_shard_amount(2),
            encoding: Arc::new(RwLock::new(EncodingConfig::default())),
        }
    }

    /// Create a new store with specified encoding config
    pub fn with_encoding(encoding: EncodingConfig) -> Self {
        let mut store = Self::with_capacity(0);
        store.encoding = Arc::new(RwLock::new(encoding));
        store
    }

    /// Create a new QuickList with the current list-max-listpack-size config
    #[inline]
    pub fn new_quicklist(&self) -> crate::storage::quicklist::QuickList {
        let fill = self.encoding.read().list_max_listpack_size;
        crate::storage::quicklist::QuickList::with_fill(fill)
    }

    // ==================== Core Generic Operations ====================

    /// Check if key exists (and not expired)
    #[inline]
    pub fn exists(&self, key: &[u8]) -> bool {
        self.exists_with_lazy_expire(key).0
    }

    /// Check if key exists (and not expired), returning (exists, was_lazily_expired)
    /// This is used to propagate DEL to replicas when lazy expiration occurs.
    #[inline]
    pub fn exists_with_lazy_expire(&self, key: &[u8]) -> (bool, bool) {
        let h = calculate_hash(key);
        match self
            .data
            .entry(h, |kv| kv.0 == key, |kv| calculate_hash(&kv.0))
        {
            crate::storage::dashtable::Entry::Occupied(e) => {
                if e.get().1.is_expired() {
                    e.remove();
                    (false, true) // Key was lazily expired
                } else {
                    (true, false)
                }
            }
            crate::storage::dashtable::Entry::Vacant(_) => (false, false),
        }
    }

    /// Delete a key
    #[inline]
    pub fn del(&self, key: &[u8]) -> bool {
        if let Some((key_bytes, entry)) = self.data_remove(key) {
            self.key_count.fetch_sub(1, Ordering::Relaxed);

            // Remove from expiration index if it has expiration
            if let Some(expire_ms) = entry.expire_at_ms() {
                self.expiration_index.remove(&key_bytes, expire_ms);
            }

            true
        } else {
            false
        }
    }

    /// Set expiration (milliseconds from now)
    #[inline]
    pub fn expire(&self, key: &[u8], ms: i64) -> bool {
        let h = calculate_hash(key);
        match self
            .data
            .entry(h, |kv| kv.0 == key, |kv| calculate_hash(&kv.0))
        {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    false
                } else {
                    let old_expire = entry.expire_at_ms();
                    let new_expire = super::value::now_ms() + ms;
                    entry.set_expire_in(ms);

                    // Update expiration index
                    let key_bytes = e.get().0.clone();
                    self.expiration_index
                        .update(key_bytes, old_expire, new_expire);

                    true
                }
            }
            crate::storage::dashtable::Entry::Vacant(_) => false,
        }
    }

    /// Get TTL in milliseconds (-2 if not exists, -1 if no expiration)
    #[inline]
    pub fn pttl(&self, key: &[u8]) -> i64 {
        let h = calculate_hash(key);
        match self
            .data
            .entry(h, |kv| kv.0 == key, |kv| calculate_hash(&kv.0))
        {
            crate::storage::dashtable::Entry::Occupied(e) => {
                if e.get().1.is_expired() {
                    -2
                } else {
                    e.get().1.ttl_ms().unwrap_or(-1)
                }
            }
            crate::storage::dashtable::Entry::Vacant(_) => -2,
        }
    }

    /// Get approximate number of keys
    #[inline]
    pub fn len(&self) -> usize {
        self.key_count.load(Ordering::Relaxed) as usize
    }

    /// Check if store is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get key version for WATCH (returns 0 if key doesn't exist)
    #[inline]
    pub fn get_version(&self, key: &[u8]) -> Option<u64> {
        let h = calculate_hash(key);
        match self
            .data
            .entry(h, |kv| kv.0 == key, |kv| calculate_hash(&kv.0))
        {
            crate::storage::dashtable::Entry::Occupied(e) => {
                if e.get().1.is_expired() {
                    None
                } else {
                    Some(e.get().1.version())
                }
            }
            crate::storage::dashtable::Entry::Vacant(_) => None,
        }
    }

    /// Iterate over all keys (for cluster slot operations)
    /// The callback receives a reference to each non-expired key
    #[inline]
    pub fn for_each_key<F>(&self, mut f: F)
    where
        F: FnMut(&[u8]),
    {
        for kv in self.data.iter() {
            if !kv.1.is_expired() {
                f(&kv.0);
            }
        }
    }

    /// Redis-style active expiration cycle
    ///
    /// Algorithm (matches Redis 6.0+):
    /// 1. Sample 20 random keys that have expiration set
    /// 2. Delete all expired keys found
    /// 3. If >25% expired, repeat (adaptive)
    /// 4. Time-bound to prevent blocking
    ///
    /// Returns the total count of expired keys that were deleted
    pub fn expire_random_keys(&self, samples: usize, use_lazy: bool) -> usize {
        const REDIS_SAMPLE_SIZE: usize = 20;
        const ADAPTIVE_THRESHOLD_PERCENT: usize = 25;
        const MAX_ITERATIONS: usize = 16; // Safety limit

        let sample_size = if samples > 0 {
            samples
        } else {
            REDIS_SAMPLE_SIZE
        };
        let mut total_expired = 0;
        let mut iteration = 0;

        loop {
            iteration += 1;

            // Sample random keys that have expiration
            let keys = self.expiration_index.sample_random(sample_size);
            if keys.is_empty() {
                break;
            }

            let sampled = keys.len();
            let mut expired_count = 0;

            // Check each sampled key and delete if expired
            for key in keys {
                if self.is_key_expired(&key) {
                    if use_lazy {
                        self.lazy_del(&key);
                    } else {
                        self.del(&key);
                    }
                    expired_count += 1;
                }
            }

            total_expired += expired_count;

            // Redis adaptive algorithm: if >25% expired, repeat
            let expired_percent = (expired_count * 100) / sampled.max(1);
            if expired_percent <= ADAPTIVE_THRESHOLD_PERCENT {
                break; // <25% expired, we're done
            }

            // Safety: don't loop forever
            if iteration >= MAX_ITERATIONS {
                break;
            }
        }

        total_expired
    }

    /// Check if a key is expired (helper for expiration cycle)
    #[inline]
    fn is_key_expired(&self, key: &[u8]) -> bool {
        let h = calculate_hash(key);
        match self
            .data
            .entry(h, |kv| kv.0 == key, |kv| calculate_hash(&kv.0))
        {
            crate::storage::dashtable::Entry::Occupied(e) => e.get().1.is_expired(),
            crate::storage::dashtable::Entry::Vacant(_) => false,
        }
    }

    // ==================== DashTable Helpers ====================

    #[inline]
    pub(crate) fn data_get(
        &self,
        key: &[u8],
    ) -> Option<super::dashtable::ReadOnlyRef<'_, (Bytes, Entry)>> {
        let h = calculate_hash(key);
        self.data.get(h, |kv| kv.0 == key)
    }

    #[inline]
    pub(crate) fn data_entry<'a>(
        &'a self,
        key: &[u8],
    ) -> super::dashtable::Entry<'a, (Bytes, Entry)> {
        let h = calculate_hash(key);
        self.data
            .entry(h, |kv| kv.0 == key, |kv| calculate_hash(&kv.0))
    }

    #[inline]
    pub(crate) fn data_remove(&self, key: &[u8]) -> Option<(Bytes, Entry)> {
        let h = calculate_hash(key);
        self.data
            .remove(h, |kv| kv.0 == key, |kv| calculate_hash(&kv.0))
    }

    #[inline]
    pub(crate) fn data_insert(&self, key: Bytes, entry: Entry) -> Option<(Bytes, Entry)> {
        let h = calculate_hash(&key);
        self.data.insert(
            h,
            (key.clone(), entry),
            |kv| kv.0 == key,
            |kv| calculate_hash(&kv.0),
        )
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::DataType;

    #[test]
    fn test_redis_style_expiration_cycle() {
        let store = Store::new();

        // Add 100 keys, 30 of which are already expired
        for i in 0..100 {
            let key = Bytes::from(format!("key{}", i));
            let value = Bytes::from(format!("value{}", i));

            if i < 30 {
                // These keys are already expired (1ms in the past)
                let entry =
                    Entry::with_expire(DataType::String(value), super::super::value::now_ms() - 1);
                store.data_insert(key.clone(), entry);
                store
                    .expiration_index
                    .add(key, super::super::value::now_ms() - 1);
            } else {
                // These keys expire in 1 hour
                let expire_at = super::super::value::now_ms() + 3600000;
                let entry = Entry::with_expire(DataType::String(value), expire_at);
                store.data_insert(key.clone(), entry);
                store.expiration_index.add(key, expire_at);
            }
            store.key_count.fetch_add(1, Ordering::Relaxed);
        }

        assert_eq!(store.len(), 100);

        // Run the Redis-style expiration cycle with sample size 20
        // Since 30% of keys are expired (>25% threshold), it should repeat multiple times
        let expired = store.expire_random_keys(20, false);

        // Should have deleted some expired keys (likely all 30, but at least some)
        assert!(expired > 0, "Should have expired at least some keys");
        assert!(expired <= 30, "Should not expire more than 30 keys");

        // The adaptive algorithm should eventually clear all expired keys
        // Run it a few more times to be sure
        let mut total_expired = expired;
        for _ in 0..5 {
            let expired = store.expire_random_keys(20, false);
            total_expired += expired;
            if expired == 0 {
                break; // No more expired keys
            }
        }

        // All 30 expired keys should eventually be deleted
        assert_eq!(total_expired, 30, "Should have expired all 30 expired keys");
        assert_eq!(store.len(), 70, "Should have 70 keys remaining");
    }

    #[test]
    fn test_expiration_index_maintained() {
        let store = Store::new();
        let key = Bytes::from("testkey");
        let value = Bytes::from("testvalue");

        // Set key with expiration
        let expire_at = super::super::value::now_ms() + 1000;
        store.set_ex(key.clone(), value.clone(), 1000);

        // Check expiration index has the key
        assert!(!store.expiration_index.is_empty());

        // Update expiration
        store.expire_at(&key, expire_at + 1000, false, false, false, false);

        // Delete the key
        assert!(store.del(&key));

        // Expiration index should be cleaned up
        // Note: The index is cleaned up when the key is removed
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_persist_removes_from_index() {
        let store = Store::new();
        let key = Bytes::from("testkey");
        let value = Bytes::from("testvalue");

        // Set key with expiration
        store.set_ex(key.clone(), value, 10000);

        // Verify key has expiration
        assert!(store.pttl(&key) > 0);

        // Persist the key (remove expiration)
        assert!(store.persist(&key));

        // Key should no longer have expiration
        assert_eq!(store.pttl(&key), -1);

        // Key should still exist
        assert!(store.exists(&key));
    }

    #[test]
    fn test_flush_clears_expiration_index() {
        let store = Store::new();

        // Add multiple keys with expiration
        for i in 0..10 {
            let key = Bytes::from(format!("key{}", i));
            let value = Bytes::from(format!("value{}", i));
            store.set_ex(key, value, 10000);
        }

        assert_eq!(store.len(), 10);

        // Flush the store
        store.flush();

        assert_eq!(store.len(), 0);
        assert!(store.expiration_index.is_empty());
    }

    #[test]
    fn test_adaptive_expiration_stops_at_threshold() {
        let store = Store::new();

        // Add 100 keys, only 5 expired (5% < 25% threshold)
        for i in 0..100 {
            let key = Bytes::from(format!("key{}", i));
            let value = Bytes::from(format!("value{}", i));

            if i < 5 {
                // Expired
                let entry =
                    Entry::with_expire(DataType::String(value), super::super::value::now_ms() - 1);
                store.data_insert(key.clone(), entry);
                store
                    .expiration_index
                    .add(key, super::super::value::now_ms() - 1);
            } else {
                // Not expired
                let expire_at = super::super::value::now_ms() + 3600000;
                let entry = Entry::with_expire(DataType::String(value), expire_at);
                store.data_insert(key.clone(), entry);
                store.expiration_index.add(key, expire_at);
            }
            store.key_count.fetch_add(1, Ordering::Relaxed);
        }

        // Run expiration cycle with sample size 20
        // Should find ~1 expired key (5% of 20), which is < 25% threshold
        // So it should stop after first iteration
        let expired = store.expire_random_keys(20, false);

        // Should have expired some keys but not necessarily all
        assert!(expired <= 5, "Should not exceed total expired keys");
    }
}
