use bytes::Bytes;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use super::ops::search_ops::SearchIndex;
use super::types::Entry;

/// Lock-free key-value store backed by DashMap.
/// Provides O(1) concurrent access with minimal contention.
///
/// Type-specific operations are implemented in separate modules under `ops/`
pub struct Store {
    /// The main data store - sharded concurrent hashmap
    pub(crate) data: DashMap<Bytes, Entry>,
    /// Track approximate key count for INFO command
    pub(crate) key_count: AtomicU64,
    /// Search indexes: index_name -> SearchIndex
    pub(crate) search_indexes: DashMap<Bytes, SearchIndex>,
    /// Search aliases: alias -> index_name
    pub(crate) search_aliases: DashMap<Bytes, Bytes>,
}

impl Store {
    /// Create a new store with default capacity
    pub fn new() -> Self {
        Self {
            data: DashMap::new(),
            key_count: AtomicU64::new(0),
            search_indexes: DashMap::new(),
            search_aliases: DashMap::new(),
        }
    }

    /// Create a new store with specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: DashMap::with_capacity(capacity),
            key_count: AtomicU64::new(0),
            search_indexes: DashMap::new(),
            search_aliases: DashMap::new(),
        }
    }

    // ==================== Core Generic Operations ====================

    /// Check if key exists (and not expired)
    #[inline]
    pub fn exists(&self, key: &[u8]) -> bool {
        match self.data.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    drop(entry);
                    self.del(key);
                    false
                } else {
                    true
                }
            }
            None => false,
        }
    }

    /// Delete a key
    #[inline]
    pub fn del(&self, key: &[u8]) -> bool {
        if self.data.remove(key).is_some() {
            self.key_count.fetch_sub(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Get key type
    #[inline]
    pub fn key_type(&self, key: &[u8]) -> Option<&'static str> {
        self.data.get(key).and_then(|e| {
            if e.is_expired() {
                None
            } else {
                Some(e.data.type_name())
            }
        })
    }

    /// Set expiration (milliseconds from now)
    #[inline]
    pub fn expire(&self, key: &[u8], ms: i64) -> bool {
        match self.data.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    false
                } else {
                    entry.set_expire_in(ms);
                    true
                }
            }
            None => false,
        }
    }

    /// Remove expiration
    #[inline]
    pub fn persist(&self, key: &[u8]) -> bool {
        match self.data.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    false
                } else {
                    entry.persist();
                    true
                }
            }
            None => false,
        }
    }

    /// Get TTL in milliseconds (-2 if not exists, -1 if no expiration)
    #[inline]
    pub fn pttl(&self, key: &[u8]) -> i64 {
        match self.data.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    -2
                } else {
                    entry.ttl_ms().unwrap_or(-1)
                }
            }
            None => -2,
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

    /// Clear all keys
    pub fn flush(&self) {
        self.data.clear();
        self.key_count.store(0, Ordering::Relaxed);
    }

    /// Get key version for WATCH (returns 0 if key doesn't exist)
    #[inline]
    pub fn get_version(&self, key: &[u8]) -> Option<u64> {
        self.data.get(key).and_then(|entry| {
            if entry.is_expired() {
                None
            } else {
                Some(entry.version())
            }
        })
    }

    /// Iterate over all keys (for cluster slot operations)
    /// The callback receives a reference to each non-expired key
    #[inline]
    pub fn for_each_key<F>(&self, mut f: F)
    where
        F: FnMut(&[u8]),
    {
        for entry in self.data.iter() {
            if !entry.value().is_expired() {
                f(entry.key());
            }
        }
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}
