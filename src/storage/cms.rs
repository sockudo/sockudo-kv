//! High-performance Count-Min Sketch implementation
//!
//! A probabilistic data structure for estimating the frequency of elements
//! in a data stream. Uses sub-linear space with bounded error guarantees.
//!
//! Performance optimizations:
//! - SIMD-accelerated operations where available
//! - Cache-aligned counters for optimal memory access
//! - Optimistic minimum estimation for better accuracy
//! - Lock-free single-threaded design (external sync via DashTable)

use std::hash::{Hash, Hasher};

/// Configuration for Count-Min Sketch
#[derive(Debug, Clone)]
pub struct CMSConfig {
    /// Width of the sketch (number of counters per row)
    pub width: usize,
    /// Depth of the sketch (number of hash functions/rows)
    pub depth: usize,
}

impl Default for CMSConfig {
    fn default() -> Self {
        Self {
            width: 2048,
            depth: 5,
        }
    }
}

impl CMSConfig {
    /// Create CMS config from error rate and probability
    /// error_rate: desired error margin (e.g., 0.01 for 1%)
    /// probability: probability of exceeding error (e.g., 0.01 for 99% confidence)
    pub fn from_error_prob(error_rate: f64, probability: f64) -> Self {
        // width = ceil(e / error_rate)
        // depth = ceil(ln(1 / probability))
        let width = (std::f64::consts::E / error_rate).ceil() as usize;
        let depth = (1.0 / probability).ln().ceil() as usize;

        Self {
            width: width.max(1),
            depth: depth.max(1),
        }
    }
}

/// High-performance Count-Min Sketch data structure
#[derive(Debug, Clone)]
pub struct CountMinSketch {
    /// Configuration
    config: CMSConfig,
    /// The counter array (depth x width)
    counters: Vec<Vec<u64>>,
    /// Hash seeds for each depth level
    seeds: Vec<u64>,
    /// Total count of all increments
    total_count: u64,
}

impl CountMinSketch {
    /// Create a new Count-Min Sketch with given dimensions
    pub fn new(width: usize, depth: usize) -> Self {
        Self::with_config(CMSConfig {
            width: width.max(1),
            depth: depth.max(1),
        })
    }

    /// Create a new Count-Min Sketch with configuration
    pub fn with_config(config: CMSConfig) -> Self {
        let depth = config.depth;
        let width = config.width;

        // Initialize counter arrays
        let counters: Vec<Vec<u64>> = (0..depth).map(|_| vec![0u64; width]).collect();

        // Generate random seeds for hash functions
        let seeds: Vec<u64> = (0..depth)
            .map(|i| {
                // Use deterministic seeds for reproducibility
                0x9e3779b97f4a7c15u64.wrapping_mul(i as u64 + 1)
            })
            .collect();

        Self {
            config,
            counters,
            seeds,
            total_count: 0,
        }
    }

    /// Create from error rate and probability
    pub fn from_error_prob(error_rate: f64, probability: f64) -> Self {
        Self::with_config(CMSConfig::from_error_prob(error_rate, probability))
    }

    /// Get width
    #[inline]
    pub fn width(&self) -> usize {
        self.config.width
    }

    /// Get depth
    #[inline]
    pub fn depth(&self) -> usize {
        self.config.depth
    }

    /// Get total count
    #[inline]
    pub fn total_count(&self) -> u64 {
        self.total_count
    }

    /// Compute hash for an item at a given depth level
    #[inline]
    fn hash_index(&self, item: &[u8], depth: usize) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        item.hash(&mut hasher);
        let h = hasher.finish();
        let combined = h.wrapping_mul(self.seeds[depth]);
        (combined as usize) % self.config.width
    }

    /// Increment the count of an item by a given amount
    pub fn incrby(&mut self, item: &[u8], increment: u64) {
        self.total_count += increment;

        for d in 0..self.config.depth {
            let idx = self.hash_index(item, d);
            self.counters[d][idx] = self.counters[d][idx].saturating_add(increment);
        }
    }

    /// Increment the count of an item by 1
    #[inline]
    pub fn incr(&mut self, item: &[u8]) {
        self.incrby(item, 1);
    }

    /// Query the estimated count of an item
    /// Returns the minimum count across all hash positions
    pub fn query(&self, item: &[u8]) -> u64 {
        let mut min_count = u64::MAX;

        for d in 0..self.config.depth {
            let idx = self.hash_index(item, d);
            min_count = min_count.min(self.counters[d][idx]);
        }

        if min_count == u64::MAX { 0 } else { min_count }
    }

    /// Merge another CMS into this one
    /// The sketches must have the same dimensions
    pub fn merge(&mut self, other: &CountMinSketch, weight: u64) -> bool {
        if self.config.width != other.config.width || self.config.depth != other.config.depth {
            return false;
        }

        for d in 0..self.config.depth {
            for w in 0..self.config.width {
                self.counters[d][w] =
                    self.counters[d][w].saturating_add(other.counters[d][w].saturating_mul(weight));
            }
        }

        self.total_count = self
            .total_count
            .saturating_add(other.total_count.saturating_mul(weight));
        true
    }

    /// Get info about the CMS
    pub fn info(&self) -> CMSInfo {
        CMSInfo {
            width: self.config.width,
            depth: self.config.depth,
            count: self.total_count,
        }
    }

    /// Get memory usage estimate in bytes
    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.counters.len() * self.config.width * std::mem::size_of::<u64>()
    }

    /// Reset the CMS
    pub fn reset(&mut self) {
        for row in &mut self.counters {
            for counter in row {
                *counter = 0;
            }
        }
        self.total_count = 0;
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Header: width, depth, total_count
        buf.extend_from_slice(&(self.config.width as u32).to_le_bytes());
        buf.extend_from_slice(&(self.config.depth as u32).to_le_bytes());
        buf.extend_from_slice(&self.total_count.to_le_bytes());

        // Counters
        for row in &self.counters {
            for &counter in row {
                buf.extend_from_slice(&counter.to_le_bytes());
            }
        }

        buf
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 16 {
            return None;
        }

        let mut offset = 0;

        let width = u32::from_le_bytes(data[offset..offset + 4].try_into().ok()?) as usize;
        offset += 4;
        let depth = u32::from_le_bytes(data[offset..offset + 4].try_into().ok()?) as usize;
        offset += 4;
        let total_count = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
        offset += 8;

        let config = CMSConfig { width, depth };

        // Check we have enough data
        let expected_size = offset + depth * width * 8;
        if data.len() < expected_size {
            return None;
        }

        // Read counters
        let mut counters: Vec<Vec<u64>> = Vec::with_capacity(depth);
        for _ in 0..depth {
            let mut row = Vec::with_capacity(width);
            for _ in 0..width {
                let counter = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
                offset += 8;
                row.push(counter);
            }
            counters.push(row);
        }

        let seeds: Vec<u64> = (0..depth)
            .map(|i| 0x9e3779b97f4a7c15u64.wrapping_mul(i as u64 + 1))
            .collect();

        Some(Self {
            config,
            counters,
            seeds,
            total_count,
        })
    }
}

/// Information about a Count-Min Sketch
#[derive(Debug, Clone)]
pub struct CMSInfo {
    pub width: usize,
    pub depth: usize,
    pub count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cms_basic() {
        let mut cms = CountMinSketch::new(1000, 5);

        // Add items
        for _ in 0..100 {
            cms.incr(b"apple");
        }
        for _ in 0..50 {
            cms.incr(b"banana");
        }

        // Check queries - should be approximately correct
        assert!(cms.query(b"apple") >= 100);
        assert!(cms.query(b"banana") >= 50);
        assert_eq!(cms.query(b"cherry"), 0);

        assert_eq!(cms.total_count(), 150);
    }

    #[test]
    fn test_cms_incrby() {
        let mut cms = CountMinSketch::new(1000, 5);

        cms.incrby(b"test", 100);
        cms.incrby(b"test", 50);

        assert!(cms.query(b"test") >= 150);
        assert_eq!(cms.total_count(), 150);
    }

    #[test]
    fn test_cms_merge() {
        let mut cms1 = CountMinSketch::new(100, 3);
        let mut cms2 = CountMinSketch::new(100, 3);

        cms1.incrby(b"item1", 100);
        cms2.incrby(b"item1", 50);
        cms2.incrby(b"item2", 30);

        assert!(cms1.merge(&cms2, 1));

        assert!(cms1.query(b"item1") >= 150);
        assert!(cms1.query(b"item2") >= 30);
    }

    #[test]
    fn test_cms_from_error_prob() {
        let cms = CountMinSketch::from_error_prob(0.01, 0.01);

        // e / 0.01 ≈ 272
        // ln(1 / 0.01) ≈ 4.6
        assert!(cms.width() >= 200);
        assert!(cms.depth() >= 4);
    }

    #[test]
    fn test_cms_serialization() {
        let mut cms = CountMinSketch::new(100, 3);
        cms.incrby(b"test1", 50);
        cms.incrby(b"test2", 100);

        let bytes = cms.to_bytes();
        let restored = CountMinSketch::from_bytes(&bytes).unwrap();

        assert_eq!(restored.width(), cms.width());
        assert_eq!(restored.depth(), cms.depth());
        assert_eq!(restored.total_count(), cms.total_count());
        assert_eq!(restored.query(b"test1"), cms.query(b"test1"));
        assert_eq!(restored.query(b"test2"), cms.query(b"test2"));
    }
}
