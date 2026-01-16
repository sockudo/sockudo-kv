//! Ultra-high-performance Bloom Filter implementation
//!
//! Features:
//! - MurmurHash64A with double hashing for k hash functions
//! - Scalable Bloom filters with sub-filter stacking
//! - Cache-friendly 64-bit aligned bit array
//! - O(k) add/check operations
//!
//! Based on Redis Bloom filter design for full compatibility.

use std::f64::consts::LN_2;

// ============================================================================
// MurmurHash64A Implementation
// ============================================================================

/// MurmurHash64A - fast, high-quality 64-bit hash
/// Used by Redis for Bloom filters and hash tables
#[inline]
pub fn murmurhash64a(data: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4a7935bd1e995;
    const R: i32 = 47;

    let len = data.len();
    let mut h: u64 = seed ^ ((len as u64).wrapping_mul(M));

    // Process 8-byte chunks
    let chunks = len / 8;
    for i in 0..chunks {
        let offset = i * 8;
        let mut k = u64::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ]);

        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);

        h ^= k;
        h = h.wrapping_mul(M);
    }

    // Handle remaining bytes
    let remaining = &data[chunks * 8..];
    match remaining.len() {
        7 => {
            h ^= (remaining[6] as u64) << 48;
            h ^= (remaining[5] as u64) << 40;
            h ^= (remaining[4] as u64) << 32;
            h ^= (remaining[3] as u64) << 24;
            h ^= (remaining[2] as u64) << 16;
            h ^= (remaining[1] as u64) << 8;
            h ^= remaining[0] as u64;
            h = h.wrapping_mul(M);
        }
        6 => {
            h ^= (remaining[5] as u64) << 40;
            h ^= (remaining[4] as u64) << 32;
            h ^= (remaining[3] as u64) << 24;
            h ^= (remaining[2] as u64) << 16;
            h ^= (remaining[1] as u64) << 8;
            h ^= remaining[0] as u64;
            h = h.wrapping_mul(M);
        }
        5 => {
            h ^= (remaining[4] as u64) << 32;
            h ^= (remaining[3] as u64) << 24;
            h ^= (remaining[2] as u64) << 16;
            h ^= (remaining[1] as u64) << 8;
            h ^= remaining[0] as u64;
            h = h.wrapping_mul(M);
        }
        4 => {
            h ^= (remaining[3] as u64) << 24;
            h ^= (remaining[2] as u64) << 16;
            h ^= (remaining[1] as u64) << 8;
            h ^= remaining[0] as u64;
            h = h.wrapping_mul(M);
        }
        3 => {
            h ^= (remaining[2] as u64) << 16;
            h ^= (remaining[1] as u64) << 8;
            h ^= remaining[0] as u64;
            h = h.wrapping_mul(M);
        }
        2 => {
            h ^= (remaining[1] as u64) << 8;
            h ^= remaining[0] as u64;
            h = h.wrapping_mul(M);
        }
        1 => {
            h ^= remaining[0] as u64;
            h = h.wrapping_mul(M);
        }
        _ => {}
    }

    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;

    h
}

// ============================================================================
// Bloom Filter Configuration
// ============================================================================

/// Configuration for a Bloom filter
#[derive(Debug, Clone)]
pub struct BloomFilterConfig {
    /// Target false positive rate (e.g., 0.01 = 1%)
    pub error_rate: f64,
    /// Expected number of items
    pub capacity: usize,
    /// Expansion factor for scaling (default: 2)
    pub expansion: u32,
    /// If true, don't create new sub-filters when full
    pub nonscaling: bool,
}

impl Default for BloomFilterConfig {
    fn default() -> Self {
        Self {
            error_rate: 0.01,
            capacity: 100,
            expansion: 2,
            nonscaling: false,
        }
    }
}

impl BloomFilterConfig {
    /// Calculate optimal number of hash functions
    #[inline]
    pub fn optimal_hash_count(error_rate: f64) -> u32 {
        ((-error_rate.ln() / LN_2).ceil() as u32).max(1)
    }

    /// Calculate optimal number of bits per item
    #[inline]
    pub fn bits_per_item(error_rate: f64) -> f64 {
        -error_rate.ln() / (LN_2 * LN_2)
    }

    /// Calculate required bit array size
    #[inline]
    pub fn required_bits(capacity: usize, error_rate: f64) -> usize {
        let bits_per_item = Self::bits_per_item(error_rate);
        ((capacity as f64 * bits_per_item).ceil() as usize).max(64)
    }
}

// ============================================================================
// Single Bloom Filter (Non-scaling)
// ============================================================================

/// A single Bloom filter with fixed capacity
#[derive(Debug, Clone)]
pub struct BloomFilter {
    /// Bit array stored as u64s for cache efficiency
    bits: Vec<u64>,
    /// Total number of bits
    num_bits: usize,
    /// Number of hash functions
    num_hashes: u32,
    /// Number of items added
    items_added: usize,
    /// Configured capacity
    capacity: usize,
    /// Error rate for this filter
    error_rate: f64,
}

impl BloomFilter {
    /// Create a new Bloom filter with given capacity and error rate
    pub fn new(capacity: usize, error_rate: f64) -> Self {
        let num_bits = BloomFilterConfig::required_bits(capacity, error_rate);
        let num_hashes = BloomFilterConfig::optimal_hash_count(error_rate);

        // Round up to next u64 boundary
        let num_u64s = (num_bits + 63) / 64;
        let actual_bits = num_u64s * 64;

        Self {
            bits: vec![0u64; num_u64s],
            num_bits: actual_bits,
            num_hashes,
            items_added: 0,
            capacity,
            error_rate,
        }
    }

    /// Add an item to the filter. Returns true if the item might be new.
    #[inline]
    pub fn add(&mut self, item: &[u8]) -> bool {
        let hash = murmurhash64a(item, 0);
        let h1 = (hash >> 32) as u32;
        let h2 = hash as u32;

        let mut possibly_new = false;

        for i in 0..self.num_hashes {
            // Double hashing: h(i) = h1 + i * h2
            let combined = (h1 as u64).wrapping_add((i as u64).wrapping_mul(h2 as u64));
            let bit_index = (combined % self.num_bits as u64) as usize;

            let word_index = bit_index / 64;
            let bit_offset = bit_index % 64;
            let mask = 1u64 << bit_offset;

            if self.bits[word_index] & mask == 0 {
                possibly_new = true;
            }
            self.bits[word_index] |= mask;
        }

        if possibly_new {
            self.items_added += 1;
        }

        possibly_new
    }

    /// Check if an item might exist in the filter
    #[inline]
    pub fn exists(&self, item: &[u8]) -> bool {
        let hash = murmurhash64a(item, 0);
        let h1 = (hash >> 32) as u32;
        let h2 = hash as u32;

        for i in 0..self.num_hashes {
            let combined = (h1 as u64).wrapping_add((i as u64).wrapping_mul(h2 as u64));
            let bit_index = (combined % self.num_bits as u64) as usize;

            let word_index = bit_index / 64;
            let bit_offset = bit_index % 64;
            let mask = 1u64 << bit_offset;

            if self.bits[word_index] & mask == 0 {
                return false;
            }
        }

        true
    }

    /// Check if the filter should be scaled (capacity exceeded)
    #[inline]
    pub fn should_scale(&self) -> bool {
        self.items_added >= self.capacity
    }

    /// Get the number of items added
    #[inline]
    pub fn len(&self) -> usize {
        self.items_added
    }

    /// Check if filter is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.items_added == 0
    }

    /// Get capacity
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get the size in bytes
    #[inline]
    pub fn size_bytes(&self) -> usize {
        self.bits.len() * 8
    }

    /// Get number of bits
    #[inline]
    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    /// Get number of hash functions
    #[inline]
    pub fn num_hashes(&self) -> u32 {
        self.num_hashes
    }

    /// Get error rate
    #[inline]
    pub fn error_rate(&self) -> f64 {
        self.error_rate
    }

    /// Serialize the filter for RDB/replication
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(32 + self.bits.len() * 8);

        // Header: num_bits (8) + num_hashes (4) + items_added (8) + capacity (8) + error_rate (8)
        result.extend_from_slice(&(self.num_bits as u64).to_le_bytes());
        result.extend_from_slice(&self.num_hashes.to_le_bytes());
        result.extend_from_slice(&(self.items_added as u64).to_le_bytes());
        result.extend_from_slice(&(self.capacity as u64).to_le_bytes());
        result.extend_from_slice(&self.error_rate.to_le_bytes());

        // Bit array
        for word in &self.bits {
            result.extend_from_slice(&word.to_le_bytes());
        }

        result
    }

    /// Deserialize the filter
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 36 {
            return None;
        }

        let num_bits = u64::from_le_bytes(data[0..8].try_into().ok()?) as usize;
        let num_hashes = u32::from_le_bytes(data[8..12].try_into().ok()?);
        let items_added = u64::from_le_bytes(data[12..20].try_into().ok()?) as usize;
        let capacity = u64::from_le_bytes(data[20..28].try_into().ok()?) as usize;
        let error_rate = f64::from_le_bytes(data[28..36].try_into().ok()?);

        let num_u64s = (num_bits + 63) / 64;
        let expected_len = 36 + num_u64s * 8;
        if data.len() < expected_len {
            return None;
        }

        let mut bits = Vec::with_capacity(num_u64s);
        for i in 0..num_u64s {
            let offset = 36 + i * 8;
            let word = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
            bits.push(word);
        }

        Some(Self {
            bits,
            num_bits,
            num_hashes,
            items_added,
            capacity,
            error_rate,
        })
    }
}

// ============================================================================
// Scalable Bloom Filter
// ============================================================================

/// A scalable Bloom filter that grows by adding sub-filters
#[derive(Debug, Clone)]
pub struct ScalableBloomFilter {
    /// Stack of filters (newest last)
    filters: Vec<BloomFilter>,
    /// Configuration
    pub config: BloomFilterConfig,
    /// Total items added across all filters
    total_items: usize,
}

impl ScalableBloomFilter {
    /// Create a new scalable Bloom filter
    pub fn new(config: BloomFilterConfig) -> Self {
        let initial_filter = BloomFilter::new(config.capacity, config.error_rate);
        Self {
            filters: vec![initial_filter],
            config,
            total_items: 0,
        }
    }

    /// Create with specific error rate and capacity
    pub fn with_capacity(capacity: usize, error_rate: f64) -> Self {
        Self::new(BloomFilterConfig {
            capacity,
            error_rate,
            ..Default::default()
        })
    }

    /// Add an item. Returns true if the item might be new.
    pub fn add(&mut self, item: &[u8]) -> bool {
        // First check if item already exists
        if self.exists(item) {
            return false;
        }

        // Get the current (last) filter
        let current = self.filters.last_mut().unwrap();

        // Check if we need to scale
        if current.should_scale() && !self.config.nonscaling {
            // Create a new filter with expanded capacity
            let new_capacity = current.capacity() * self.config.expansion as usize;
            // Use tighter error rate for new filters to maintain overall error rate
            let new_error_rate = current.error_rate() * 0.5;
            let new_filter = BloomFilter::new(new_capacity, new_error_rate);
            self.filters.push(new_filter);
        }

        // Add to the current (potentially new) filter
        let current = self.filters.last_mut().unwrap();
        let added = current.add(item);
        if added {
            self.total_items += 1;
        }
        added
    }

    /// Check if an item might exist
    pub fn exists(&self, item: &[u8]) -> bool {
        // Check all filters from newest to oldest
        for filter in self.filters.iter().rev() {
            if filter.exists(item) {
                return true;
            }
        }
        false
    }

    /// Add multiple items. Returns count of newly added items.
    pub fn add_many(&mut self, items: &[&[u8]]) -> usize {
        let mut added = 0;
        for item in items {
            if self.add(item) {
                added += 1;
            }
        }
        added
    }

    /// Check multiple items. Returns a vector of bools.
    pub fn exists_many(&self, items: &[&[u8]]) -> Vec<bool> {
        items.iter().map(|item| self.exists(item)).collect()
    }

    /// Get total items added
    #[inline]
    pub fn len(&self) -> usize {
        self.total_items
    }

    /// Check if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.total_items == 0
    }

    /// Get configured capacity (initial)
    #[inline]
    pub fn capacity(&self) -> usize {
        self.config.capacity
    }

    /// Get number of sub-filters
    #[inline]
    pub fn num_filters(&self) -> usize {
        self.filters.len()
    }

    /// Get total size in bytes
    pub fn size_bytes(&self) -> usize {
        self.filters.iter().map(|f| f.size_bytes()).sum()
    }

    /// Get expansion factor
    #[inline]
    pub fn expansion(&self) -> u32 {
        self.config.expansion
    }

    /// Get error rate
    #[inline]
    pub fn error_rate(&self) -> f64 {
        self.config.error_rate
    }

    /// Estimate cardinality using bit population
    /// Uses the formula: -m * ln(1 - X/m) where m is bits and X is set bits
    pub fn estimate_cardinality(&self) -> usize {
        let mut total_estimate = 0.0;

        for filter in &self.filters {
            let m = filter.num_bits() as f64;
            let k = filter.num_hashes() as f64;

            // Count set bits
            let set_bits: usize = filter.bits.iter().map(|w| w.count_ones() as usize).sum();
            let x = set_bits as f64;

            if x >= m {
                // All bits set, use items_added as estimate
                total_estimate += filter.len() as f64;
            } else {
                // Estimate using the formula
                let ratio = 1.0 - (x / m);
                if ratio > 0.0 {
                    let estimate = -(m / k) * ratio.ln();
                    total_estimate += estimate;
                }
            }
        }

        total_estimate.round() as usize
    }

    /// Serialize for BF.SCANDUMP
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::new();

        // Header: version (1) + num_filters (4) + config
        result.push(1); // version
        result.extend_from_slice(&(self.filters.len() as u32).to_le_bytes());
        result.extend_from_slice(&self.config.error_rate.to_le_bytes());
        result.extend_from_slice(&(self.config.capacity as u64).to_le_bytes());
        result.extend_from_slice(&self.config.expansion.to_le_bytes());
        result.push(if self.config.nonscaling { 1 } else { 0 });
        result.extend_from_slice(&(self.total_items as u64).to_le_bytes());

        // Each filter with length prefix
        for filter in &self.filters {
            let filter_bytes = filter.to_bytes();
            result.extend_from_slice(&(filter_bytes.len() as u32).to_le_bytes());
            result.extend_from_slice(&filter_bytes);
        }

        result
    }

    /// Deserialize from BF.LOADCHUNK
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 30 {
            return None;
        }

        let version = data[0];
        if version != 1 {
            return None;
        }

        let num_filters = u32::from_le_bytes(data[1..5].try_into().ok()?) as usize;
        let error_rate = f64::from_le_bytes(data[5..13].try_into().ok()?);
        let capacity = u64::from_le_bytes(data[13..21].try_into().ok()?) as usize;
        let expansion = u32::from_le_bytes(data[21..25].try_into().ok()?);
        let nonscaling = data[25] != 0;
        let total_items = u64::from_le_bytes(data[26..34].try_into().ok()?) as usize;

        let config = BloomFilterConfig {
            error_rate,
            capacity,
            expansion,
            nonscaling,
        };

        let mut filters = Vec::with_capacity(num_filters);
        let mut offset = 34;

        for _ in 0..num_filters {
            if offset + 4 > data.len() {
                return None;
            }
            let filter_len = u32::from_le_bytes(data[offset..offset + 4].try_into().ok()?) as usize;
            offset += 4;
            if offset + filter_len > data.len() {
                return None;
            }
            let filter = BloomFilter::from_bytes(&data[offset..offset + filter_len])?;
            filters.push(filter);
            offset += filter_len;
        }

        Some(Self {
            filters,
            config,
            total_items,
        })
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_murmurhash64a() {
        // Basic sanity check
        let hash1 = murmurhash64a(b"hello", 0);
        let hash2 = murmurhash64a(b"hello", 0);
        let hash3 = murmurhash64a(b"world", 0);

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_bloom_filter_basic() {
        let mut bf = BloomFilter::new(1000, 0.01);

        assert!(!bf.exists(b"test"));
        bf.add(b"test");
        assert!(bf.exists(b"test"));
        assert!(!bf.exists(b"not_added"));
    }

    #[test]
    fn test_bloom_filter_false_positive_rate() {
        let mut bf = BloomFilter::new(10000, 0.01);

        // Add items
        for i in 0..10000 {
            bf.add(format!("item{}", i).as_bytes());
        }

        // Check false positives
        let mut false_positives = 0;
        for i in 10000..20000 {
            if bf.exists(format!("item{}", i).as_bytes()) {
                false_positives += 1;
            }
        }

        let fp_rate = false_positives as f64 / 10000.0;
        // Allow some margin (should be close to 1%)
        assert!(fp_rate < 0.02, "False positive rate too high: {}", fp_rate);
    }

    #[test]
    fn test_scalable_bloom_filter() {
        let config = BloomFilterConfig {
            capacity: 100,
            error_rate: 0.01,
            expansion: 2,
            nonscaling: false,
        };
        let mut sbf = ScalableBloomFilter::new(config);

        // Add items past capacity
        for i in 0..500 {
            sbf.add(format!("item{}", i).as_bytes());
        }

        // Should have scaled
        assert!(sbf.num_filters() > 1);

        // All items should exist
        for i in 0..500 {
            assert!(sbf.exists(format!("item{}", i).as_bytes()));
        }
    }

    #[test]
    fn test_serialization() {
        let mut sbf = ScalableBloomFilter::with_capacity(100, 0.01);

        for i in 0..50 {
            sbf.add(format!("test{}", i).as_bytes());
        }

        let bytes = sbf.to_bytes();
        let restored = ScalableBloomFilter::from_bytes(&bytes).unwrap();

        assert_eq!(sbf.len(), restored.len());
        assert_eq!(sbf.num_filters(), restored.num_filters());

        // Check items exist in restored
        for i in 0..50 {
            assert!(restored.exists(format!("test{}", i).as_bytes()));
        }
    }
}
