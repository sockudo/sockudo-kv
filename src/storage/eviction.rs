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
    match policy {
        MaxMemoryPolicy::NoEviction => None,
        MaxMemoryPolicy::VolatileLru => Some(evict_volatile_lru(store, samples, count)),
        MaxMemoryPolicy::AllKeysLru => Some(evict_allkeys_lru(store, samples, count)),
        MaxMemoryPolicy::VolatileLfu => Some(evict_volatile_lfu(store, samples, count)),
        MaxMemoryPolicy::AllKeysLfu => Some(evict_allkeys_lfu(store, samples, count)),
        MaxMemoryPolicy::VolatileRandom => Some(evict_volatile_random(store, count)),
        MaxMemoryPolicy::AllKeysRandom => Some(evict_allkeys_random(store, count)),
        MaxMemoryPolicy::VolatileTtl => Some(evict_volatile_ttl(store, samples, count)),
    }
}

/// Evict keys with expiration set using LRU (samples random keys, evicts least recently used)
fn evict_volatile_lru(store: &Arc<Store>, samples: usize, count: usize) -> EvictionResult {
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
            && store.del(&key)
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
fn evict_allkeys_lru(store: &Arc<Store>, samples: usize, count: usize) -> EvictionResult {
    let mut evicted = 0;

    for _ in 0..count {
        let candidates = sample_all_keys(store, samples);
        if candidates.is_empty() {
            break;
        }

        if let Some((key, _)) = candidates
            .into_iter()
            .min_by_key(|(_, idle_time)| std::cmp::Reverse(*idle_time))
            && store.del(&key)
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
fn evict_volatile_lfu(store: &Arc<Store>, samples: usize, count: usize) -> EvictionResult {
    let mut evicted = 0;

    for _ in 0..count {
        let candidates = sample_volatile_keys_lfu(store, samples);
        if candidates.is_empty() {
            break;
        }

        // Find key with lowest LFU counter
        if let Some((key, _)) = candidates.into_iter().min_by_key(|(_, lfu)| *lfu)
            && store.del(&key)
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
fn evict_allkeys_lfu(store: &Arc<Store>, samples: usize, count: usize) -> EvictionResult {
    let mut evicted = 0;

    for _ in 0..count {
        let candidates = sample_all_keys_lfu(store, samples);
        if candidates.is_empty() {
            break;
        }

        if let Some((key, _)) = candidates.into_iter().min_by_key(|(_, lfu)| *lfu)
            && store.del(&key)
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
fn evict_volatile_random(store: &Arc<Store>, count: usize) -> EvictionResult {
    let mut evicted = 0;
    let candidates = sample_volatile_keys(store, count * 2);

    for (key, _) in candidates.into_iter().take(count) {
        if store.del(&key) {
            evicted += 1;
        }
    }

    EvictionResult {
        evicted,
        bytes_freed: 0,
    }
}

/// Evict random keys
fn evict_allkeys_random(store: &Arc<Store>, count: usize) -> EvictionResult {
    let mut evicted = 0;
    let candidates = sample_all_keys(store, count * 2);

    for (key, _) in candidates.into_iter().take(count) {
        if store.del(&key) {
            evicted += 1;
        }
    }

    EvictionResult {
        evicted,
        bytes_freed: 0,
    }
}

/// Evict keys with shortest TTL
fn evict_volatile_ttl(store: &Arc<Store>, samples: usize, count: usize) -> EvictionResult {
    let mut evicted = 0;

    for _ in 0..count {
        let candidates = sample_volatile_keys_ttl(store, samples);
        if candidates.is_empty() {
            break;
        }

        // Find key with smallest TTL
        if let Some((key, _)) = candidates.into_iter().min_by_key(|(_, ttl)| *ttl)
            && store.del(&key)
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
