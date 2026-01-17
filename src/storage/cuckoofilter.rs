//! Ultra-high-performance Cuckoo Filter implementation
//!
//! Features:
//! - O(1) lookups, insertions, and deletions
//! - Supports deletion (unlike Bloom filters)
//! - Configurable bucket size and fingerprint size
//! - Automatic expansion with sub-filters
//!
//! Based on "Cuckoo Filter: Practically Better Than Bloom" by Fan et al.

// ============================================================================
// High-performance hash functions
// ============================================================================

/// MurmurHash3 finalizer - excellent mixing for fingerprints
#[inline(always)]
fn murmur_mix(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
    h ^= h >> 33;
    h
}

/// Primary hash function for items - uses MurmurHash64A-style processing
#[inline]
pub fn item_hash(data: &[u8]) -> u64 {
    const M: u64 = 0xc6a4a7935bd1e995;
    const R: i32 = 47;
    const SEED: u64 = 0x5bd1e995;

    let len = data.len();
    let mut h: u64 = SEED ^ ((len as u64).wrapping_mul(M));

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
    if !remaining.is_empty() {
        for (i, &byte) in remaining.iter().enumerate() {
            h ^= (byte as u64) << (i * 8);
        }
        h = h.wrapping_mul(M);
    }

    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;

    h
}

// ============================================================================
// Fingerprint handling
// ============================================================================

/// Extract fingerprint from hash - ensures non-zero fingerprint
#[inline(always)]
fn fingerprint_from_hash(hash: u64, fp_size_bits: u8) -> u32 {
    let mask = (1u64 << fp_size_bits) - 1;
    let fp = (hash & mask) as u32;
    // Ensure non-zero (zero would cause issues with empty slot detection)
    if fp == 0 { 1 } else { fp }
}

/// Calculate alternate bucket index using partial-key cuckoo hashing
/// i2 = i1 XOR hash(fingerprint)
/// The XOR ensures that alt_index(alt_index(i, fp), fp) == i
#[inline(always)]
fn alt_index(index: usize, fingerprint: u32, num_buckets: usize) -> usize {
    let fp_hash = murmur_mix(fingerprint as u64) as usize;
    // Ensure fp_hash is within bucket range before XOR to maintain symmetry
    let fp_index = fp_hash % num_buckets;
    // Handle the case where fp_index is 0 to avoid i XOR 0 = i
    let fp_index = if fp_index == 0 { 1 } else { fp_index };
    (index ^ fp_index) % num_buckets
}

// ============================================================================
// Bucket implementation
// ============================================================================

/// A bucket holding multiple fingerprints
#[derive(Clone)]
pub struct Bucket {
    /// Fingerprints stored in this bucket (0 = empty slot)
    fingerprints: Vec<u32>,
}

impl std::fmt::Debug for Bucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bucket")
            .field("fingerprints", &self.fingerprints)
            .finish()
    }
}

impl Bucket {
    /// Create a new bucket with given size
    #[inline]
    fn new(size: usize) -> Self {
        Self {
            fingerprints: vec![0u32; size],
        }
    }

    /// Check if bucket contains the fingerprint
    #[inline]
    fn contains(&self, fp: u32) -> bool {
        self.fingerprints.iter().any(|&f| f == fp)
    }

    /// Count occurrences of fingerprint in bucket
    #[inline]
    fn count(&self, fp: u32) -> usize {
        self.fingerprints.iter().filter(|&&f| f == fp).count()
    }

    /// Insert fingerprint if there's an empty slot
    /// Returns true if inserted
    #[inline]
    fn insert(&mut self, fp: u32) -> bool {
        for slot in &mut self.fingerprints {
            if *slot == 0 {
                *slot = fp;
                return true;
            }
        }
        false
    }

    /// Delete one occurrence of the fingerprint
    /// Returns true if deleted
    #[inline]
    fn delete(&mut self, fp: u32) -> bool {
        for slot in &mut self.fingerprints {
            if *slot == fp {
                *slot = 0;
                return true;
            }
        }
        false
    }

    /// Get fingerprint at specific index
    #[inline]
    fn get(&self, idx: usize) -> u32 {
        self.fingerprints[idx]
    }

    /// Set fingerprint at specific index
    #[inline]
    fn set(&mut self, idx: usize, fp: u32) {
        self.fingerprints[idx] = fp;
    }
}

// ============================================================================
// Cuckoo Filter Configuration
// ============================================================================

/// Configuration for Cuckoo Filter
#[derive(Debug, Clone)]
pub struct CuckooFilterConfig {
    /// Initial capacity (will be rounded to power of 2)
    pub capacity: usize,
    /// Number of entries per bucket (default: 4)
    pub bucket_size: usize,
    /// Maximum number of kicks before declaring filter full (default: 500)
    pub max_iterations: usize,
    /// Expansion factor when creating new sub-filters (default: 1 = same size)
    pub expansion: u32,
    /// Fingerprint size in bits (8, 12, 16, or 32)
    pub fingerprint_bits: u8,
}

impl Default for CuckooFilterConfig {
    /// Redis-compatible defaults following RedisBloom CF module conventions
    fn default() -> Self {
        Self {
            capacity: 1024,
            // RedisBloom uses bucket_size=2 by default for better space efficiency
            // at the cost of slightly more relocations. The paper recommends 4.
            bucket_size: 2,
            // Maximum cuckoo kicks before declaring filter full
            // RedisBloom uses 500 which provides good balance
            max_iterations: 500,
            // Expansion factor when creating new sub-filters
            // 1 = same size as original, 2 = double size
            expansion: 1,
            // 16-bit fingerprints provide false positive rate of ~0.03% with b=2
            // or ~0.0001% with b=4
            fingerprint_bits: 16,
        }
    }
}

impl CuckooFilterConfig {
    /// Calculate number of buckets needed for capacity
    /// Returns power of 2 for efficient modulo via bitwise AND
    #[inline]
    fn num_buckets(capacity: usize, bucket_size: usize) -> usize {
        let min_buckets = (capacity + bucket_size - 1) / bucket_size;
        // Round up to power of 2
        min_buckets.next_power_of_two()
    }
}

// ============================================================================
// Single Cuckoo Filter
// ============================================================================

/// A single Cuckoo Filter with fixed capacity
#[derive(Debug, Clone)]
pub struct CuckooFilter {
    /// Array of buckets
    buckets: Vec<Bucket>,
    /// Number of buckets (power of 2)
    num_buckets: usize,
    /// Bucket size
    bucket_size: usize,
    /// Max kicks during insertion
    max_iterations: usize,
    /// Fingerprint size in bits
    fingerprint_bits: u8,
    /// Number of items stored
    count: usize,
    /// Original capacity
    capacity: usize,
}

impl CuckooFilter {
    /// Create a new Cuckoo Filter with given configuration
    pub fn new(config: &CuckooFilterConfig) -> Self {
        let num_buckets = CuckooFilterConfig::num_buckets(config.capacity, config.bucket_size);
        let buckets: Vec<Bucket> = (0..num_buckets)
            .map(|_| Bucket::new(config.bucket_size))
            .collect();

        Self {
            buckets,
            num_buckets,
            bucket_size: config.bucket_size,
            max_iterations: config.max_iterations,
            fingerprint_bits: config.fingerprint_bits,
            count: 0,
            capacity: config.capacity,
        }
    }

    /// Create with default configuration
    pub fn with_capacity(capacity: usize) -> Self {
        let config = CuckooFilterConfig {
            capacity,
            ..Default::default()
        };
        Self::new(&config)
    }

    /// Get bucket indices for an item
    #[inline]
    fn get_indices(&self, item: &[u8]) -> (usize, usize, u32) {
        let hash = item_hash(item);
        let fp = fingerprint_from_hash(hash >> 32, self.fingerprint_bits);
        let i1 = (hash as usize) % self.num_buckets;
        let i2 = alt_index(i1, fp, self.num_buckets);
        (i1, i2, fp)
    }

    /// Add an item to the filter
    /// Returns true if added successfully, false if filter is full
    pub fn add(&mut self, item: &[u8]) -> bool {
        let (i1, i2, fp) = self.get_indices(item);

        // Try to insert into bucket 1
        if self.buckets[i1].insert(fp) {
            self.count += 1;
            return true;
        }

        // Try to insert into bucket 2
        if self.buckets[i2].insert(fp) {
            self.count += 1;
            return true;
        }

        // Both buckets full - need to kick
        self.cuckoo_insert(i1, fp)
    }

    /// Add item only if it doesn't exist
    /// Returns true if added (item was new), false if already exists or filter full
    pub fn add_nx(&mut self, item: &[u8]) -> bool {
        if self.exists(item) {
            return false;
        }
        self.add(item)
    }

    /// Cuckoo insertion with deterministic kicks (Redis-style)
    /// Uses rotating victim slot selection to allow rollback on failure
    fn cuckoo_insert(&mut self, start_index: usize, start_fp: u32) -> bool {
        let mut index = start_index;
        let mut fp = start_fp;
        let mut victim_slot = 0usize;

        // Forward phase: try to find a home for our fingerprint
        for _ in 0..self.max_iterations {
            // Swap our fingerprint with the victim slot
            let old_fp = self.buckets[index].get(victim_slot);
            self.buckets[index].set(victim_slot, fp);
            fp = old_fp;

            // Calculate alternate index for the displaced fingerprint
            index = alt_index(index, fp, self.num_buckets);

            // Try to insert the displaced fingerprint in an empty slot
            if self.buckets[index].insert(fp) {
                self.count += 1;
                return true;
            }

            // Rotate victim slot deterministically for rollback capability
            victim_slot = (victim_slot + 1) % self.bucket_size;
        }

        // Failed to find space - rollback all swaps to restore filter integrity
        // Run the same sequence in reverse
        for _ in 0..self.max_iterations {
            // Reverse the victim slot rotation
            victim_slot = (victim_slot + self.bucket_size - 1) % self.bucket_size;

            // Calculate the previous bucket (alt_index is symmetric: alt(alt(i, fp), fp) = i)
            index = alt_index(index, fp, self.num_buckets);

            // Swap back
            let old_fp = self.buckets[index].get(victim_slot);
            self.buckets[index].set(victim_slot, fp);
            fp = old_fp;
        }

        false
    }

    /// Check if item might exist in the filter
    #[inline]
    pub fn exists(&self, item: &[u8]) -> bool {
        let (i1, i2, fp) = self.get_indices(item);
        self.buckets[i1].contains(fp) || self.buckets[i2].contains(fp)
    }

    /// Count estimated occurrences of item
    /// Note: Due to fingerprint collisions, this may overcount
    #[inline]
    pub fn count_item(&self, item: &[u8]) -> usize {
        let (i1, i2, fp) = self.get_indices(item);
        if i1 == i2 {
            self.buckets[i1].count(fp)
        } else {
            self.buckets[i1].count(fp) + self.buckets[i2].count(fp)
        }
    }

    /// Delete one occurrence of an item
    /// Returns true if deleted, false if not found
    /// WARNING: Only delete items you're sure exist to avoid corrupting the filter
    pub fn delete(&mut self, item: &[u8]) -> bool {
        let (i1, i2, fp) = self.get_indices(item);

        if self.buckets[i1].delete(fp) {
            self.count = self.count.saturating_sub(1);
            return true;
        }

        if self.buckets[i2].delete(fp) {
            self.count = self.count.saturating_sub(1);
            return true;
        }

        false
    }

    /// Get number of items stored
    #[inline]
    pub fn len(&self) -> usize {
        self.count
    }

    /// Check if filter is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Get configured capacity
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get actual maximum capacity (buckets * bucket_size)
    #[inline]
    pub fn max_capacity(&self) -> usize {
        self.num_buckets * self.bucket_size
    }

    /// Get number of buckets
    #[inline]
    pub fn num_buckets(&self) -> usize {
        self.num_buckets
    }

    /// Get bucket size
    #[inline]
    pub fn bucket_size(&self) -> usize {
        self.bucket_size
    }

    /// Get max iterations
    #[inline]
    pub fn max_iterations(&self) -> usize {
        self.max_iterations
    }

    /// Calculate memory usage in bytes
    pub fn size_bytes(&self) -> usize {
        let bucket_mem = self.num_buckets * self.bucket_size * 4;
        let overhead =
            self.num_buckets * std::mem::size_of::<Bucket>() + std::mem::size_of::<Self>();
        bucket_mem + overhead
    }

    /// Serialize to bytes for persistence
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(32 + self.num_buckets * self.bucket_size * 4);

        // Header
        result.push(1); // version
        result.extend_from_slice(&(self.num_buckets as u64).to_le_bytes());
        result.extend_from_slice(&(self.bucket_size as u32).to_le_bytes());
        result.extend_from_slice(&(self.max_iterations as u32).to_le_bytes());
        result.push(self.fingerprint_bits);
        result.extend_from_slice(&(self.count as u64).to_le_bytes());
        result.extend_from_slice(&(self.capacity as u64).to_le_bytes());

        // Buckets
        for bucket in &self.buckets {
            for &fp in &bucket.fingerprints {
                result.extend_from_slice(&fp.to_le_bytes());
            }
        }

        result
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 38 {
            return None;
        }

        let version = data[0];
        if version != 1 {
            return None;
        }

        let num_buckets = u64::from_le_bytes(data[1..9].try_into().ok()?) as usize;
        let bucket_size = u32::from_le_bytes(data[9..13].try_into().ok()?) as usize;
        let max_iterations = u32::from_le_bytes(data[13..17].try_into().ok()?) as usize;
        let fingerprint_bits = data[17];
        let count = u64::from_le_bytes(data[18..26].try_into().ok()?) as usize;
        let capacity = u64::from_le_bytes(data[26..34].try_into().ok()?) as usize;

        let expected_len = 34 + num_buckets * bucket_size * 4;
        if data.len() < expected_len {
            return None;
        }

        let mut buckets = Vec::with_capacity(num_buckets);
        let mut offset = 34;

        for _ in 0..num_buckets {
            let mut bucket = Bucket::new(bucket_size);
            for j in 0..bucket_size {
                bucket.fingerprints[j] =
                    u32::from_le_bytes(data[offset..offset + 4].try_into().ok()?);
                offset += 4;
            }
            buckets.push(bucket);
        }

        Some(Self {
            buckets,
            num_buckets,
            bucket_size,
            max_iterations,
            fingerprint_bits,
            count,
            capacity,
        })
    }
}

// ============================================================================
// Scalable Cuckoo Filter (with expansion)
// ============================================================================

/// A scalable Cuckoo Filter that grows by adding sub-filters
#[derive(Debug, Clone)]
pub struct ScalableCuckooFilter {
    /// Stack of filters (newest last)
    filters: Vec<CuckooFilter>,
    /// Configuration
    pub config: CuckooFilterConfig,
    /// Total items across all filters
    total_count: usize,
}

impl ScalableCuckooFilter {
    /// Create a new scalable Cuckoo Filter
    pub fn new(config: CuckooFilterConfig) -> Self {
        let initial_filter = CuckooFilter::new(&config);
        Self {
            filters: vec![initial_filter],
            config,
            total_count: 0,
        }
    }

    /// Create with default config
    pub fn with_capacity(capacity: usize) -> Self {
        let config = CuckooFilterConfig {
            capacity,
            ..Default::default()
        };
        Self::new(config)
    }

    /// Add an item
    /// Returns true if added successfully
    pub fn add(&mut self, item: &[u8]) -> bool {
        // Try to add to current (last) filter
        let current = self.filters.last_mut().unwrap();

        if current.add(item) {
            self.total_count += 1;
            return true;
        }

        // Current filter is full - try to expand
        if self.config.expansion > 0 {
            let new_capacity = current.capacity() * self.config.expansion as usize;
            let new_config = CuckooFilterConfig {
                capacity: new_capacity.max(self.config.capacity), // At least original capacity
                ..self.config.clone()
            };
            let mut new_filter = CuckooFilter::new(&new_config);

            if new_filter.add(item) {
                self.filters.push(new_filter);
                self.total_count += 1;
                return true;
            }
        }

        false
    }

    /// Add item only if it doesn't exist
    pub fn add_nx(&mut self, item: &[u8]) -> bool {
        if self.exists(item) {
            return false;
        }
        self.add(item)
    }

    /// Check if item might exist
    pub fn exists(&self, item: &[u8]) -> bool {
        for filter in self.filters.iter().rev() {
            if filter.exists(item) {
                return true;
            }
        }
        false
    }

    /// Count estimated occurrences across all filters
    pub fn count_item(&self, item: &[u8]) -> usize {
        self.filters.iter().map(|f| f.count_item(item)).sum()
    }

    /// Delete one occurrence of an item
    pub fn delete(&mut self, item: &[u8]) -> bool {
        for filter in self.filters.iter_mut().rev() {
            if filter.delete(item) {
                self.total_count = self.total_count.saturating_sub(1);
                return true;
            }
        }
        false
    }

    /// Get total item count
    #[inline]
    pub fn len(&self) -> usize {
        self.total_count
    }

    /// Check if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.total_count == 0
    }

    /// Get configured initial capacity
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

    /// Get bucket size
    #[inline]
    pub fn bucket_size(&self) -> usize {
        self.config.bucket_size
    }

    /// Get max iterations
    #[inline]
    pub fn max_iterations(&self) -> usize {
        self.config.max_iterations
    }

    /// Get expansion rate
    #[inline]
    pub fn expansion(&self) -> u32 {
        self.config.expansion
    }

    /// Get total number of buckets across all filters
    pub fn total_buckets(&self) -> usize {
        self.filters.iter().map(|f| f.num_buckets()).sum()
    }

    /// Check multiple items at once
    pub fn exists_many(&self, items: &[&[u8]]) -> Vec<bool> {
        items.iter().map(|item| self.exists(item)).collect()
    }

    /// Serialize for persistence
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::new();

        // Header
        result.push(1); // version
        result.extend_from_slice(&(self.filters.len() as u32).to_le_bytes());
        result.extend_from_slice(&(self.config.capacity as u64).to_le_bytes());
        result.extend_from_slice(&(self.config.bucket_size as u32).to_le_bytes());
        result.extend_from_slice(&(self.config.max_iterations as u32).to_le_bytes());
        result.extend_from_slice(&self.config.expansion.to_le_bytes());
        result.push(self.config.fingerprint_bits);
        result.extend_from_slice(&(self.total_count as u64).to_le_bytes());

        // Each filter with length prefix
        for filter in &self.filters {
            let filter_bytes = filter.to_bytes();
            result.extend_from_slice(&(filter_bytes.len() as u32).to_le_bytes());
            result.extend_from_slice(&filter_bytes);
        }

        result
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 34 {
            return None;
        }

        let version = data[0];
        if version != 1 {
            return None;
        }

        let num_filters = u32::from_le_bytes(data[1..5].try_into().ok()?) as usize;
        let capacity = u64::from_le_bytes(data[5..13].try_into().ok()?) as usize;
        let bucket_size = u32::from_le_bytes(data[13..17].try_into().ok()?) as usize;
        let max_iterations = u32::from_le_bytes(data[17..21].try_into().ok()?) as usize;
        let expansion = u32::from_le_bytes(data[21..25].try_into().ok()?);
        let fingerprint_bits = data[25];
        let total_count = u64::from_le_bytes(data[26..34].try_into().ok()?) as usize;

        let config = CuckooFilterConfig {
            capacity,
            bucket_size,
            max_iterations,
            expansion,
            fingerprint_bits,
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
            let filter = CuckooFilter::from_bytes(&data[offset..offset + filter_len])?;
            filters.push(filter);
            offset += filter_len;
        }

        Some(Self {
            filters,
            config,
            total_count,
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
    fn test_cuckoo_filter_basic() {
        let mut cf = CuckooFilter::with_capacity(1000);

        assert!(!cf.exists(b"test"));
        assert!(cf.add(b"test"));
        assert!(cf.exists(b"test"));
        assert!(!cf.exists(b"not_added"));
    }

    #[test]
    fn test_cuckoo_filter_delete() {
        let mut cf = CuckooFilter::with_capacity(1000);

        cf.add(b"item1");
        cf.add(b"item2");
        cf.add(b"item3");

        assert_eq!(cf.len(), 3);
        assert!(cf.exists(b"item2"));

        assert!(cf.delete(b"item2"));
        assert!(!cf.exists(b"item2"));
        assert_eq!(cf.len(), 2);

        // Delete non-existent
        assert!(!cf.delete(b"item2"));
        assert_eq!(cf.len(), 2);
    }

    #[test]
    fn test_cuckoo_filter_add_nx() {
        let mut cf = CuckooFilter::with_capacity(1000);

        assert!(cf.add_nx(b"item1"));
        assert!(!cf.add_nx(b"item1")); // Should fail, already exists
        assert_eq!(cf.len(), 1);
    }

    #[test]
    fn test_cuckoo_filter_count() {
        let mut cf = CuckooFilter::with_capacity(1000);

        cf.add(b"item1");
        cf.add(b"item1");
        cf.add(b"item1");

        // Count may be >= 3 due to fingerprint collisions
        assert!(cf.count_item(b"item1") >= 3);
        assert_eq!(cf.len(), 3);
    }

    #[test]
    fn test_cuckoo_filter_fill() {
        let config = CuckooFilterConfig {
            capacity: 100,
            bucket_size: 4,
            max_iterations: 500,
            expansion: 0,
            fingerprint_bits: 16,
        };
        let mut cf = CuckooFilter::new(&config);

        // Add items and verify each one immediately
        let mut successful_adds = 0;
        let mut failed_verifications = Vec::new();

        for i in 0..200 {
            let item = format!("item{}", i);
            let added = cf.add(item.as_bytes());
            if added {
                successful_adds += 1;
                // Immediately verify it exists
                if !cf.exists(item.as_bytes()) {
                    failed_verifications.push(i);
                }
            }
        }

        assert!(
            failed_verifications.is_empty(),
            "Items added but not found: {:?}",
            failed_verifications
        );

        // Should add close to capacity before failures start
        assert!(
            successful_adds >= 80,
            "Expected ~100 successful adds, got {}",
            successful_adds
        );
    }

    #[test]
    fn test_cuckoo_filter_serialization() {
        let mut cf = CuckooFilter::with_capacity(100);

        for i in 0..50 {
            cf.add(format!("test{}", i).as_bytes());
        }

        let bytes = cf.to_bytes();
        let restored = CuckooFilter::from_bytes(&bytes).unwrap();

        assert_eq!(cf.len(), restored.len());
        assert_eq!(cf.num_buckets(), restored.num_buckets());

        for i in 0..50 {
            assert!(restored.exists(format!("test{}", i).as_bytes()));
        }
    }

    #[test]
    fn test_scalable_cuckoo_filter() {
        let config = CuckooFilterConfig {
            capacity: 500,
            bucket_size: 4,
            max_iterations: 500,
            expansion: 2,
            fingerprint_bits: 16,
        };
        let mut scf = ScalableCuckooFilter::new(config);

        // Add items - most should succeed
        for i in 0..400 {
            scf.add(format!("item{}", i).as_bytes());
        }

        // Should have added items
        assert!(
            scf.len() > 300,
            "Should have added many items, got {}",
            scf.len()
        );

        // All early items should exist
        for i in 0..100 {
            assert!(
                scf.exists(format!("item{}", i).as_bytes()),
                "Item {} not found",
                i
            );
        }
    }

    #[test]
    fn test_scalable_cuckoo_filter_delete() {
        let mut scf = ScalableCuckooFilter::with_capacity(1000);

        for i in 0..50 {
            scf.add(format!("item{}", i).as_bytes());
        }

        assert_eq!(scf.len(), 50);

        // Delete some items
        for i in 0..25 {
            assert!(scf.delete(format!("item{}", i).as_bytes()));
        }

        assert_eq!(scf.len(), 25);

        // Deleted items should not exist
        for i in 0..25 {
            assert!(!scf.exists(format!("item{}", i).as_bytes()));
        }

        // Remaining items should exist
        for i in 25..50 {
            assert!(scf.exists(format!("item{}", i).as_bytes()));
        }
    }

    #[test]
    fn test_scalable_serialization() {
        let mut scf = ScalableCuckooFilter::with_capacity(1000);

        for i in 0..75 {
            scf.add(format!("test{}", i).as_bytes());
        }

        let bytes = scf.to_bytes();
        let restored = ScalableCuckooFilter::from_bytes(&bytes).unwrap();

        assert_eq!(scf.len(), restored.len());
        assert_eq!(scf.num_filters(), restored.num_filters());

        for i in 0..75 {
            assert!(restored.exists(format!("test{}", i).as_bytes()));
        }
    }

    #[test]
    fn test_false_positive_rate() {
        let mut cf = CuckooFilter::with_capacity(10000);

        // Add 10000 items
        for i in 0..10000 {
            cf.add(format!("item{}", i).as_bytes());
        }

        // Check false positives on different items
        let mut false_positives = 0;
        for i in 10000..20000 {
            if cf.exists(format!("item{}", i).as_bytes()) {
                false_positives += 1;
            }
        }

        let fp_rate = false_positives as f64 / 10000.0;
        // With 16-bit fingerprints and bucket_size=4, FP rate should be low
        assert!(
            fp_rate < 0.02,
            "False positive rate too high: {} ({} FPs)",
            fp_rate,
            false_positives
        );
    }
}
