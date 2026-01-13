use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};

use super::dashtable::{DashTable, calculate_hash};
use super::ops::search_ops::SearchIndex;
use super::types::Entry;

/// Lock-free key-value store backed by DashTable.
/// Provides O(1) concurrent access with minimal contention.
///
/// Type-specific operations are implemented in separate modules under `ops/`
pub struct Store {
    /// The main data store - sharded concurrent hash table
    pub(crate) data: DashTable<(Bytes, Entry)>,
    /// Track approximate key count for INFO command
    pub(crate) key_count: AtomicU64,
    /// Search indexes: index_name -> SearchIndex
    pub(crate) search_indexes: DashTable<(Bytes, SearchIndex)>,
    /// Search aliases: alias -> index_name
    pub(crate) search_aliases: DashTable<(Bytes, Bytes)>,
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
            // Metadata maps rarely used, minimal 2 shards
            search_indexes: DashTable::with_shard_amount(2),
            search_aliases: DashTable::with_shard_amount(2),
        }
    }

    // ==================== Core Generic Operations ====================

    /// Check if key exists (and not expired)
    #[inline]
    pub fn exists(&self, key: &[u8]) -> bool {
        let h = calculate_hash(key);
        match self
            .data
            .entry(h, |kv| kv.0 == key, |kv| calculate_hash(&kv.0))
        {
            crate::storage::dashtable::Entry::Occupied(e) => {
                if e.get().1.is_expired() {
                    e.remove();
                    false
                } else {
                    true
                }
            }
            crate::storage::dashtable::Entry::Vacant(_) => false,
        }
    }

    /// Delete a key
    #[inline]
    pub fn del(&self, key: &[u8]) -> bool {
        if self.data_remove(key).is_some() {
            self.key_count.fetch_sub(1, Ordering::Relaxed);
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
                    entry.set_expire_in(ms);
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
        self.data.remove(h, |kv| kv.0 == key)
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
