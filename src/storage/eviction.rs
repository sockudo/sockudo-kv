//! Memory eviction module
//!
//! Implements Redis-compatible eviction policies using sampling-based algorithms.
//! Supports: noeviction, volatile-lru, allkeys-lru, volatile-lfu, allkeys-lfu,
//! volatile-random, allkeys-random, volatile-ttl

use bytes::Bytes;
use std::sync::Arc;

use crate::config::MaxMemoryPolicy;
use crate::storage::Store;

/// Result of an eviction attempt
#[derive(Debug)]
pub struct EvictionResult {
    /// Number of keys evicted
    pub evicted: usize,
    /// Bytes freed (approximate)
    pub bytes_freed: usize,
}

/// Evict keys from the store based on the configured policy
///
/// Returns the number of keys evicted, or None if policy is noeviction
pub fn evict_keys(
    store: &Arc<Store>,
    policy: &MaxMemoryPolicy,
    samples: usize,
    count: usize,
) -> Option<EvictionResult> {
    // Public API uses sync deletion for backwards compatibility
    match policy {
        MaxMemoryPolicy::NoEviction => None,
        MaxMemoryPolicy::VolatileLru => Some(evict_volatile_lru(store, samples, count, false)),
        MaxMemoryPolicy::AllKeysLru => Some(evict_allkeys_lru(store, samples, count, false)),
        MaxMemoryPolicy::VolatileLfu => Some(evict_volatile_lfu(store, samples, count, false)),
        MaxMemoryPolicy::AllKeysLfu => Some(evict_allkeys_lfu(store, samples, count, false)),
        MaxMemoryPolicy::VolatileRandom => Some(evict_volatile_random(store, count, false)),
        MaxMemoryPolicy::AllKeysRandom => Some(evict_allkeys_random(store, count, false)),
        MaxMemoryPolicy::VolatileTtl => Some(evict_volatile_ttl(store, samples, count, false)),
    }
}

/// Check memory limit and evict keys if necessary
///
/// Uses maxmemory_samples and maxmemory_eviction_tenacity from ServerState.
/// Respects replica_ignore_maxmemory setting.
/// Uses lazy deletion when lazyfree_lazy_eviction is enabled.
///
/// Returns true if operation can proceed, false if OOM and policy is noeviction
pub fn maybe_evict(store: &Arc<Store>, server: &crate::server_state::ServerState) -> bool {
    use std::sync::atomic::Ordering;

    let maxmemory = server.maxmemory.load(Ordering::Relaxed);
    if maxmemory == 0 {
        return true; // No memory limit
    }

    // Check if this is a replica that should ignore maxmemory
    let config = server.config.read();
    if config.replica_ignore_maxmemory && config.replicaof.is_some() {
        return true; // Replicas don't evict when replica_ignore_maxmemory is set
    }
    let policy = config.maxmemory_policy;
    drop(config);

    // Get memory usage estimate (key count * approximate size)
    // Real implementation would use allocator stats
    let estimated_memory = store.len() as u64 * 256; // Rough estimate

    if estimated_memory <= maxmemory {
        return true; // Under limit
    }

    // Need to evict
    if matches!(policy, MaxMemoryPolicy::NoEviction) {
        return false; // OOM error
    }

    let samples = server.maxmemory_samples.load(Ordering::Relaxed);
    let tenacity = server.maxmemory_eviction_tenacity.load(Ordering::Relaxed);
    let use_lazy = server.lazyfree_lazy_eviction.load(Ordering::Relaxed);

    // Eviction count based on tenacity (0-100 scale, 10 = 1 key per cycle)
    let count = ((tenacity as usize) / 10).max(1);

    if let Some(result) = evict_keys_internal(store, &policy, samples, count, use_lazy) {
        server
            .evicted_keys
            .fetch_add(result.evicted as u64, Ordering::Relaxed);
    }

    true
}

/// Internal evict function that accepts use_lazy parameter
fn evict_keys_internal(
    store: &Arc<Store>,
    policy: &MaxMemoryPolicy,
    samples: usize,
    count: usize,
    use_lazy: bool,
) -> Option<EvictionResult> {
    match policy {
        MaxMemoryPolicy::NoEviction => None,
        MaxMemoryPolicy::VolatileLru => Some(evict_volatile_lru(store, samples, count, use_lazy)),
        MaxMemoryPolicy::AllKeysLru => Some(evict_allkeys_lru(store, samples, count, use_lazy)),
        MaxMemoryPolicy::VolatileLfu => Some(evict_volatile_lfu(store, samples, count, use_lazy)),
        MaxMemoryPolicy::AllKeysLfu => Some(evict_allkeys_lfu(store, samples, count, use_lazy)),
        MaxMemoryPolicy::VolatileRandom => Some(evict_volatile_random(store, count, use_lazy)),
        MaxMemoryPolicy::AllKeysRandom => Some(evict_allkeys_random(store, count, use_lazy)),
        MaxMemoryPolicy::VolatileTtl => Some(evict_volatile_ttl(store, samples, count, use_lazy)),
    }
}

/// Helper function to delete a key with or without lazy freeing
#[inline]
fn del_key(store: &Arc<Store>, key: &[u8], use_lazy: bool) -> bool {
    if use_lazy {
        store.lazy_del(key)
    } else {
        store.del(key)
    }
}

/// Evict keys with expiration set using LRU (samples random keys, evicts least recently used)
fn evict_volatile_lru(
    store: &Arc<Store>,
    samples: usize,
    count: usize,
    use_lazy: bool,
) -> EvictionResult {
    let mut evicted = 0;

    for _ in 0..count {
        let candidates = sample_volatile_keys(store, samples);
        if candidates.is_empty() {
            break;
        }

        // Find key with oldest (smallest) LRU time
        if let Some((key, _)) = candidates
            .into_iter()
            .min_by_key(|(_, idle_time)| std::cmp::Reverse(*idle_time))
            && del_key(store, &key, use_lazy)
        {
            evicted += 1;
        }
    }

    EvictionResult {
        evicted,
        bytes_freed: 0, // Approximate, not tracked precisely
    }
}

/// Evict any keys using LRU
fn evict_allkeys_lru(
    store: &Arc<Store>,
    samples: usize,
    count: usize,
    use_lazy: bool,
) -> EvictionResult {
    let mut evicted = 0;

    for _ in 0..count {
        let candidates = sample_all_keys(store, samples);
        if candidates.is_empty() {
            break;
        }

        if let Some((key, _)) = candidates
            .into_iter()
            .min_by_key(|(_, idle_time)| std::cmp::Reverse(*idle_time))
            && del_key(store, &key, use_lazy)
        {
            evicted += 1;
        }
    }

    EvictionResult {
        evicted,
        bytes_freed: 0,
    }
}

/// Evict keys with expiration using LFU (least frequently used)
fn evict_volatile_lfu(
    store: &Arc<Store>,
    samples: usize,
    count: usize,
    use_lazy: bool,
) -> EvictionResult {
    let mut evicted = 0;

    for _ in 0..count {
        let candidates = sample_volatile_keys_lfu(store, samples);
        if candidates.is_empty() {
            break;
        }

        // Find key with lowest LFU counter
        if let Some((key, _)) = candidates.into_iter().min_by_key(|(_, lfu)| *lfu)
            && del_key(store, &key, use_lazy)
        {
            evicted += 1;
        }
    }

    EvictionResult {
        evicted,
        bytes_freed: 0,
    }
}

/// Evict any keys using LFU
fn evict_allkeys_lfu(
    store: &Arc<Store>,
    samples: usize,
    count: usize,
    use_lazy: bool,
) -> EvictionResult {
    let mut evicted = 0;

    for _ in 0..count {
        let candidates = sample_all_keys_lfu(store, samples);
        if candidates.is_empty() {
            break;
        }

        if let Some((key, _)) = candidates.into_iter().min_by_key(|(_, lfu)| *lfu)
            && del_key(store, &key, use_lazy)
        {
            evicted += 1;
        }
    }

    EvictionResult {
        evicted,
        bytes_freed: 0,
    }
}

/// Evict random keys with expiration
fn evict_volatile_random(store: &Arc<Store>, count: usize, use_lazy: bool) -> EvictionResult {
    let mut evicted = 0;
    let candidates = sample_volatile_keys(store, count * 2);

    for (key, _) in candidates.into_iter().take(count) {
        if del_key(store, &key, use_lazy) {
            evicted += 1;
        }
    }

    EvictionResult {
        evicted,
        bytes_freed: 0,
    }
}

/// Evict random keys
fn evict_allkeys_random(store: &Arc<Store>, count: usize, use_lazy: bool) -> EvictionResult {
    let mut evicted = 0;
    let candidates = sample_all_keys(store, count * 2);

    for (key, _) in candidates.into_iter().take(count) {
        if del_key(store, &key, use_lazy) {
            evicted += 1;
        }
    }

    EvictionResult {
        evicted,
        bytes_freed: 0,
    }
}

/// Evict keys with shortest TTL
fn evict_volatile_ttl(
    store: &Arc<Store>,
    samples: usize,
    count: usize,
    use_lazy: bool,
) -> EvictionResult {
    let mut evicted = 0;

    for _ in 0..count {
        let candidates = sample_volatile_keys_ttl(store, samples);
        if candidates.is_empty() {
            break;
        }

        // Find key with smallest TTL
        if let Some((key, _)) = candidates.into_iter().min_by_key(|(_, ttl)| *ttl)
            && del_key(store, &key, use_lazy)
        {
            evicted += 1;
        }
    }

    EvictionResult {
        evicted,
        bytes_freed: 0,
    }
}

// ==================== Sampling Helpers ====================

/// Sample random keys with expiration set, returning (key, idle_time)
fn sample_volatile_keys(store: &Arc<Store>, samples: usize) -> Vec<(Bytes, i64)> {
    let mut result = Vec::with_capacity(samples);
    let len = store.data.len();
    if len == 0 {
        return result;
    }

    // Random sampling using skip iterator
    for _ in 0..samples {
        let skip = fastrand::usize(0..len.max(1));
        if let Some(entry) = store.data.iter().nth(skip) {
            let e = &entry.1;
            if !e.is_expired() && e.has_expire() {
                result.push((entry.0.clone(), e.idle_time()));
            }
        }
        if result.len() >= samples {
            break;
        }
    }

    result
}

/// Sample random keys (any), returning (key, idle_time)
fn sample_all_keys(store: &Arc<Store>, samples: usize) -> Vec<(Bytes, i64)> {
    let mut result = Vec::with_capacity(samples);
    let len = store.data.len();
    if len == 0 {
        return result;
    }

    for _ in 0..samples {
        let skip = fastrand::usize(0..len.max(1));
        if let Some(entry) = store.data.iter().nth(skip) {
            let e = &entry.1;
            if !e.is_expired() {
                result.push((entry.0.clone(), e.idle_time()));
            }
        }
        if result.len() >= samples {
            break;
        }
    }

    result
}

/// Sample keys with expiration for LFU, returning (key, lfu_counter)
fn sample_volatile_keys_lfu(store: &Arc<Store>, samples: usize) -> Vec<(Bytes, u8)> {
    let mut result = Vec::with_capacity(samples);
    let len = store.data.len();
    if len == 0 {
        return result;
    }

    for _ in 0..samples {
        let skip = fastrand::usize(0..len.max(1));
        if let Some(entry) = store.data.iter().nth(skip) {
            let e = &entry.1;
            if !e.is_expired() && e.has_expire() {
                result.push((entry.0.clone(), e.lfu_counter()));
            }
        }
        if result.len() >= samples {
            break;
        }
    }

    result
}

/// Sample any keys for LFU
fn sample_all_keys_lfu(store: &Arc<Store>, samples: usize) -> Vec<(Bytes, u8)> {
    let mut result = Vec::with_capacity(samples);
    let len = store.data.len();
    if len == 0 {
        return result;
    }

    for _ in 0..samples {
        let skip = fastrand::usize(0..len.max(1));
        if let Some(entry) = store.data.iter().nth(skip) {
            let e = &entry.1;
            if !e.is_expired() {
                result.push((entry.0.clone(), e.lfu_counter()));
            }
        }
        if result.len() >= samples {
            break;
        }
    }

    result
}

/// Sample keys with expiration for TTL eviction, returning (key, ttl_ms)
fn sample_volatile_keys_ttl(store: &Arc<Store>, samples: usize) -> Vec<(Bytes, i64)> {
    let mut result = Vec::with_capacity(samples);
    let len = store.data.len();
    if len == 0 {
        return result;
    }

    for _ in 0..samples {
        let skip = fastrand::usize(0..len.max(1));
        if let Some(entry) = store.data.iter().nth(skip) {
            let e = &entry.1;
            if !e.is_expired()
                && let Some(ttl) = e.ttl_ms()
            {
                result.push((entry.0.clone(), ttl));
            }
        }
        if result.len() >= samples {
            break;
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eviction_result() {
        let result = EvictionResult {
            evicted: 5,
            bytes_freed: 1024,
        };
        assert_eq!(result.evicted, 5);
    }
}
