//! Ultra-high-performance T-Digest implementation
//!
//! T-Digest is a probabilistic data structure for accurate estimation of quantiles
//! and cumulative distribution functions (CDFs) from streaming data.
//!
//! Features:
//! - O(1) amortized insertion
//! - Accurate quantile estimation (especially at extremes)
//! - Mergeable sketches
//! - Configurable compression for accuracy/memory tradeoff
//!
//! Based on "Computing Extremely Accurate Quantiles Using t-Digests" by Ted Dunning

use std::cmp::Ordering;

// ============================================================================
// Centroid - a weighted point representing multiple observations
// ============================================================================

/// A centroid is a weighted mean representing a cluster of observations
#[derive(Debug, Clone, Copy)]
pub struct Centroid {
    /// Mean value of this centroid
    pub mean: f64,
    /// Weight (count of observations merged into this centroid)
    pub weight: f64,
}

impl Centroid {
    #[inline]
    pub fn new(mean: f64, weight: f64) -> Self {
        Self { mean, weight }
    }

    /// Merge another centroid into this one
    #[inline]
    pub fn add(&mut self, other: &Centroid) {
        let total_weight = self.weight + other.weight;
        self.mean = (self.mean * self.weight + other.mean * other.weight) / total_weight;
        self.weight = total_weight;
    }

    /// Add a single value with weight 1
    #[inline]
    pub fn add_value(&mut self, value: f64) {
        let total_weight = self.weight + 1.0;
        self.mean = (self.mean * self.weight + value) / total_weight;
        self.weight = total_weight;
    }
}

impl PartialEq for Centroid {
    fn eq(&self, other: &Self) -> bool {
        self.mean == other.mean
    }
}

impl PartialOrd for Centroid {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.mean.partial_cmp(&other.mean)
    }
}

// ============================================================================
// T-Digest implementation
// ============================================================================

/// T-Digest data structure for quantile estimation
#[derive(Debug, Clone)]
pub struct TDigest {
    /// Merged centroids (sorted by mean)
    centroids: Vec<Centroid>,
    /// Unmerged buffer (pending observations)
    buffer: Vec<f64>,
    /// Compression parameter (higher = more accurate, more memory)
    compression: f64,
    /// Maximum buffer size before auto-merge
    buffer_capacity: usize,
    /// Total weight of merged centroids
    merged_weight: f64,
    /// Total weight of unmerged buffer
    unmerged_weight: f64,
    /// Minimum observed value
    min: f64,
    /// Maximum observed value
    max: f64,
    /// Total number of observations added
    total_observations: u64,
    /// Number of compressions performed
    total_compressions: u64,
}

impl Default for TDigest {
    fn default() -> Self {
        Self::new(100.0)
    }
}

impl TDigest {
    /// Create a new T-Digest with the given compression factor
    /// Compression of 100 is typical, 1000 is very accurate
    pub fn new(compression: f64) -> Self {
        let compression = compression.max(10.0); // Minimum compression
        // Buffer capacity based on compression - allows efficient batch processing
        let buffer_capacity = (compression * 5.0) as usize;
        // Centroid capacity based on compression
        let centroid_capacity = (compression * 6.0) as usize;

        Self {
            centroids: Vec::with_capacity(centroid_capacity),
            buffer: Vec::with_capacity(buffer_capacity),
            compression,
            buffer_capacity,
            merged_weight: 0.0,
            unmerged_weight: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            total_observations: 0,
            total_compressions: 0,
        }
    }

    /// Add a single observation
    #[inline]
    pub fn add(&mut self, value: f64) {
        if value.is_nan() {
            return;
        }

        // Update min/max
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }

        self.buffer.push(value);
        self.unmerged_weight += 1.0;
        self.total_observations += 1;

        // Auto-merge when buffer is full
        if self.buffer.len() >= self.buffer_capacity {
            self.compress();
        }
    }

    /// Add multiple observations
    pub fn add_many(&mut self, values: &[f64]) {
        for &value in values {
            self.add(value);
        }
    }

    /// Compress the buffer into centroids
    pub fn compress(&mut self) {
        if self.buffer.is_empty() && self.centroids.is_empty() {
            return;
        }

        // Sort buffer
        self.buffer
            .sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));

        // Create centroids from buffer
        let mut new_centroids: Vec<Centroid> =
            self.buffer.iter().map(|&v| Centroid::new(v, 1.0)).collect();

        // Merge with existing centroids
        new_centroids.extend(self.centroids.iter().cloned());

        // Sort all centroids by mean
        new_centroids.sort_by(|a, b| a.mean.partial_cmp(&b.mean).unwrap_or(Ordering::Equal));

        // Compress centroids using the scaling function
        self.centroids = self.compress_centroids(new_centroids);

        // Update weights
        self.merged_weight += self.unmerged_weight;
        self.unmerged_weight = 0.0;
        self.buffer.clear();
        self.total_compressions += 1;
    }

    /// Compress centroids using the k-size constraint
    fn compress_centroids(&self, centroids: Vec<Centroid>) -> Vec<Centroid> {
        if centroids.is_empty() {
            return Vec::new();
        }

        let total_weight: f64 = centroids.iter().map(|c| c.weight).sum();
        if total_weight == 0.0 {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(centroids.len());
        let mut current = centroids[0];
        let mut weight_so_far = 0.0;

        for centroid in centroids.into_iter().skip(1) {
            let projected_weight = weight_so_far + current.weight + centroid.weight;
            let q0 = weight_so_far / total_weight;
            let q1 = projected_weight / total_weight;

            // k-size constraint: maximum size of centroid at quantile q
            let k_limit = self.k_size(q0, q1, total_weight);

            if current.weight + centroid.weight <= k_limit {
                // Merge centroids
                current.add(&centroid);
            } else {
                // Start new centroid
                weight_so_far += current.weight;
                result.push(current);
                current = centroid;
            }
        }

        result.push(current);
        result
    }

    /// Calculate the maximum size for a centroid at quantile range [q0, q1]
    /// Uses the k2 scaling function for better accuracy at the extremes
    #[inline]
    fn k_size(&self, q0: f64, q1: f64, total_weight: f64) -> f64 {
        // k2 scaling function: k(q) = compression * (asin(2q-1)/pi + 0.5)
        // This gives more resolution at the extremes (q near 0 or 1)
        let k0 = self.scale_function(q0);
        let k1 = self.scale_function(q1);
        let dk = k1 - k0;

        // Maximum weight that can be assigned
        total_weight * dk.abs().max(1.0 / self.compression)
    }

    /// k2 scale function: maps quantile to k-space
    #[inline]
    fn scale_function(&self, q: f64) -> f64 {
        let q = q.clamp(0.0, 1.0);
        self.compression * (((2.0 * q - 1.0).asin() / std::f64::consts::PI) + 0.5)
    }

    /// Estimate the quantile value (0.0 to 1.0)
    pub fn quantile(&mut self, q: f64) -> f64 {
        if q.is_nan() || q < 0.0 || q > 1.0 {
            return f64::NAN;
        }

        // Ensure data is merged
        if !self.buffer.is_empty() {
            self.compress();
        }

        if self.centroids.is_empty() {
            return f64::NAN;
        }

        // Special cases
        if q == 0.0 {
            return self.min;
        }
        if q == 1.0 {
            return self.max;
        }

        let total = self.merged_weight;
        let target = q * total;

        // Binary search for the centroid containing the target weight
        let mut weight_sum = 0.0;

        for i in 0..self.centroids.len() {
            let c = &self.centroids[i];
            let next_weight = weight_sum + c.weight;

            if target <= next_weight {
                // Interpolate within this centroid
                let prev_mean = if i == 0 {
                    self.min
                } else {
                    (self.centroids[i - 1].mean + c.mean) / 2.0
                };
                let next_mean = if i == self.centroids.len() - 1 {
                    self.max
                } else {
                    (c.mean + self.centroids[i + 1].mean) / 2.0
                };

                let centroid_start = weight_sum;
                let centroid_end = next_weight;

                if centroid_end == centroid_start {
                    return c.mean;
                }

                let t = (target - centroid_start) / (centroid_end - centroid_start);
                return prev_mean + t * (next_mean - prev_mean);
            }

            weight_sum = next_weight;
        }

        self.max
    }

    /// Estimate multiple quantiles at once
    pub fn quantiles(&mut self, qs: &[f64]) -> Vec<f64> {
        qs.iter().map(|&q| self.quantile(q)).collect()
    }

    /// Estimate the CDF (cumulative distribution function) for a value
    /// Returns the fraction of observations <= value
    pub fn cdf(&mut self, value: f64) -> f64 {
        if value.is_nan() {
            return f64::NAN;
        }

        // Ensure data is merged
        if !self.buffer.is_empty() {
            self.compress();
        }

        if self.centroids.is_empty() {
            return f64::NAN;
        }

        if value <= self.min {
            return 0.0;
        }
        if value >= self.max {
            return 1.0;
        }

        let total = self.merged_weight;
        let mut weight_sum = 0.0;

        for i in 0..self.centroids.len() {
            let c = &self.centroids[i];

            if c.mean >= value {
                // Interpolate
                let prev_mean = if i == 0 {
                    self.min
                } else {
                    (self.centroids[i - 1].mean + c.mean) / 2.0
                };

                if c.mean == prev_mean {
                    return weight_sum / total;
                }

                let t = (value - prev_mean) / (c.mean - prev_mean);
                let interpolated = weight_sum + t * c.weight / 2.0;
                return interpolated / total;
            }

            weight_sum += c.weight;
        }

        1.0
    }

    /// Estimate CDFs for multiple values
    pub fn cdfs(&mut self, values: &[f64]) -> Vec<f64> {
        values.iter().map(|&v| self.cdf(v)).collect()
    }

    /// Estimate the rank of a value (number of observations <= value)
    pub fn rank(&mut self, value: f64) -> i64 {
        if value.is_nan() {
            return -2;
        }

        // Ensure data is merged
        if !self.buffer.is_empty() {
            self.compress();
        }

        if self.centroids.is_empty() {
            return -2;
        }

        if value < self.min {
            return -1;
        }

        let total = self.merged_weight;
        let cdf = self.cdf(value);

        if cdf.is_nan() {
            return -2;
        }

        (cdf * total).round() as i64
    }

    /// Estimate ranks for multiple values
    pub fn ranks(&mut self, values: &[f64]) -> Vec<i64> {
        values.iter().map(|&v| self.rank(v)).collect()
    }

    /// Estimate the reverse rank (number of observations >= value)
    pub fn rev_rank(&mut self, value: f64) -> i64 {
        if value.is_nan() {
            return -2;
        }

        // Ensure data is merged
        if !self.buffer.is_empty() {
            self.compress();
        }

        if self.centroids.is_empty() {
            return -2;
        }

        if value > self.max {
            return -1;
        }

        let total = self.merged_weight as i64;
        let rank = self.rank(value);

        if rank < 0 {
            return total;
        }

        total - rank
    }

    /// Estimate reverse ranks for multiple values
    pub fn rev_ranks(&mut self, values: &[f64]) -> Vec<i64> {
        values.iter().map(|&v| self.rev_rank(v)).collect()
    }

    /// Get value at a specific rank (0 to n-1)
    pub fn by_rank(&mut self, rank: i64) -> f64 {
        if rank < 0 {
            return f64::NEG_INFINITY;
        }

        // Ensure data is merged
        if !self.buffer.is_empty() {
            self.compress();
        }

        if self.centroids.is_empty() {
            return f64::NAN;
        }

        let total = self.merged_weight as i64;

        if rank >= total {
            return f64::INFINITY;
        }

        let q = (rank as f64 + 0.5) / self.merged_weight;
        self.quantile(q)
    }

    /// Get values at specific ranks
    pub fn by_ranks(&mut self, ranks: &[i64]) -> Vec<f64> {
        ranks.iter().map(|&r| self.by_rank(r)).collect()
    }

    /// Get value at a specific reverse rank
    pub fn by_rev_rank(&mut self, rev_rank: i64) -> f64 {
        if rev_rank < 0 {
            return f64::INFINITY;
        }

        // Ensure data is merged
        if !self.buffer.is_empty() {
            self.compress();
        }

        if self.centroids.is_empty() {
            return f64::NAN;
        }

        let total = self.merged_weight as i64;

        if rev_rank >= total {
            return f64::NEG_INFINITY;
        }

        let rank = total - rev_rank - 1;
        self.by_rank(rank)
    }

    /// Get values at specific reverse ranks
    pub fn by_rev_ranks(&mut self, rev_ranks: &[i64]) -> Vec<f64> {
        rev_ranks.iter().map(|&r| self.by_rev_rank(r)).collect()
    }

    /// Get minimum value
    #[inline]
    pub fn min(&self) -> f64 {
        if self.total_observations == 0 {
            f64::NAN
        } else {
            self.min
        }
    }

    /// Get maximum value
    #[inline]
    pub fn max(&self) -> f64 {
        if self.total_observations == 0 {
            f64::NAN
        } else {
            self.max
        }
    }

    /// Calculate trimmed mean between two quantiles
    pub fn trimmed_mean(&mut self, low_quantile: f64, high_quantile: f64) -> f64 {
        if low_quantile >= high_quantile || low_quantile < 0.0 || high_quantile > 1.0 {
            return f64::NAN;
        }

        // Ensure data is merged
        if !self.buffer.is_empty() {
            self.compress();
        }

        if self.centroids.is_empty() {
            return f64::NAN;
        }

        let total = self.merged_weight;
        let low_weight = low_quantile * total;
        let high_weight = high_quantile * total;

        let mut sum = 0.0;
        let mut weight_sum = 0.0;
        let mut included_weight = 0.0;

        for c in &self.centroids {
            let centroid_start = weight_sum;
            let centroid_end = weight_sum + c.weight;

            // Calculate overlap with [low_weight, high_weight]
            let overlap_start = centroid_start.max(low_weight);
            let overlap_end = centroid_end.min(high_weight);

            if overlap_end > overlap_start {
                let overlap_weight = overlap_end - overlap_start;
                sum += c.mean * overlap_weight;
                included_weight += overlap_weight;
            }

            weight_sum = centroid_end;
        }

        if included_weight == 0.0 {
            f64::NAN
        } else {
            sum / included_weight
        }
    }

    /// Reset the sketch, keeping compression setting
    pub fn reset(&mut self) {
        self.centroids.clear();
        self.buffer.clear();
        self.merged_weight = 0.0;
        self.unmerged_weight = 0.0;
        self.min = f64::INFINITY;
        self.max = f64::NEG_INFINITY;
        self.total_observations = 0;
        self.total_compressions = 0;
    }

    /// Merge another T-Digest into this one
    pub fn merge(&mut self, other: &TDigest) {
        // Add other's buffer values
        for &v in &other.buffer {
            self.add(v);
        }

        // Merge centroids
        for c in &other.centroids {
            // Add as weighted values
            self.buffer.push(c.mean);
            self.unmerged_weight += c.weight - 1.0; // -1 because add() adds 1
        }

        // Update min/max
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }

        // Compress if needed
        if self.buffer.len() >= self.buffer_capacity {
            self.compress();
        }
    }

    /// Get compression parameter
    #[inline]
    pub fn compression(&self) -> f64 {
        self.compression
    }

    /// Get number of centroids
    #[inline]
    pub fn num_centroids(&self) -> usize {
        self.centroids.len()
    }

    /// Get number of unmerged observations
    #[inline]
    pub fn num_unmerged(&self) -> usize {
        self.buffer.len()
    }

    /// Get total observations count
    #[inline]
    pub fn total_observations(&self) -> u64 {
        self.total_observations
    }

    /// Get merged weight
    #[inline]
    pub fn merged_weight(&self) -> f64 {
        self.merged_weight
    }

    /// Get unmerged weight
    #[inline]
    pub fn unmerged_weight(&self) -> f64 {
        self.unmerged_weight
    }

    /// Get total compressions
    #[inline]
    pub fn total_compressions(&self) -> u64 {
        self.total_compressions
    }

    /// Get capacity (max centroids)
    #[inline]
    pub fn capacity(&self) -> usize {
        (self.compression * 6.0) as usize
    }

    /// Estimate memory usage in bytes
    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.centroids.capacity() * std::mem::size_of::<Centroid>()
            + self.buffer.capacity() * std::mem::size_of::<f64>()
    }

    /// Check if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.total_observations == 0
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::new();

        // Version
        result.push(1);

        // Compression
        result.extend_from_slice(&self.compression.to_le_bytes());

        // Stats
        result.extend_from_slice(&self.min.to_le_bytes());
        result.extend_from_slice(&self.max.to_le_bytes());
        result.extend_from_slice(&self.merged_weight.to_le_bytes());
        result.extend_from_slice(&self.total_observations.to_le_bytes());
        result.extend_from_slice(&self.total_compressions.to_le_bytes());

        // Centroids
        result.extend_from_slice(&(self.centroids.len() as u32).to_le_bytes());
        for c in &self.centroids {
            result.extend_from_slice(&c.mean.to_le_bytes());
            result.extend_from_slice(&c.weight.to_le_bytes());
        }

        // Buffer
        result.extend_from_slice(&(self.buffer.len() as u32).to_le_bytes());
        for &v in &self.buffer {
            result.extend_from_slice(&v.to_le_bytes());
        }

        result
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 53 {
            return None;
        }

        let version = data[0];
        if version != 1 {
            return None;
        }

        let compression = f64::from_le_bytes(data[1..9].try_into().ok()?);
        let min = f64::from_le_bytes(data[9..17].try_into().ok()?);
        let max = f64::from_le_bytes(data[17..25].try_into().ok()?);
        let merged_weight = f64::from_le_bytes(data[25..33].try_into().ok()?);
        let total_observations = u64::from_le_bytes(data[33..41].try_into().ok()?);
        let total_compressions = u64::from_le_bytes(data[41..49].try_into().ok()?);

        let num_centroids = u32::from_le_bytes(data[49..53].try_into().ok()?) as usize;

        let mut offset = 53;
        let mut centroids = Vec::with_capacity(num_centroids);
        for _ in 0..num_centroids {
            if offset + 16 > data.len() {
                return None;
            }
            let mean = f64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
            let weight = f64::from_le_bytes(data[offset + 8..offset + 16].try_into().ok()?);
            centroids.push(Centroid::new(mean, weight));
            offset += 16;
        }

        if offset + 4 > data.len() {
            return None;
        }
        let num_buffer = u32::from_le_bytes(data[offset..offset + 4].try_into().ok()?) as usize;
        offset += 4;

        let mut buffer = Vec::with_capacity(num_buffer);
        for _ in 0..num_buffer {
            if offset + 8 > data.len() {
                return None;
            }
            let v = f64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
            buffer.push(v);
            offset += 8;
        }

        let unmerged_weight = buffer.len() as f64;
        let buffer_capacity = (compression * 5.0) as usize;

        Some(Self {
            centroids,
            buffer,
            compression,
            buffer_capacity,
            merged_weight,
            unmerged_weight,
            min,
            max,
            total_observations,
            total_compressions,
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
    fn test_basic_operations() {
        let mut td = TDigest::new(100.0);

        // Add some values
        td.add(1.0);
        td.add(2.0);
        td.add(3.0);
        td.add(4.0);
        td.add(5.0);

        assert_eq!(td.total_observations(), 5);
        assert_eq!(td.min(), 1.0);
        assert_eq!(td.max(), 5.0);
    }

    #[test]
    fn test_quantiles() {
        let mut td = TDigest::new(1000.0);

        // Add values 1-100
        for i in 1..=100 {
            td.add(i as f64);
        }

        // Test quantiles
        let q0 = td.quantile(0.0);
        let q50 = td.quantile(0.5);
        let q100 = td.quantile(1.0);

        assert_eq!(q0, 1.0);
        assert_eq!(q100, 100.0);
        // Median should be close to 50
        assert!((q50 - 50.0).abs() < 5.0, "Median was {}", q50);
    }

    #[test]
    fn test_cdf() {
        let mut td = TDigest::new(1000.0);

        for i in 1..=100 {
            td.add(i as f64);
        }

        let cdf_0 = td.cdf(0.0);
        let cdf_50 = td.cdf(50.0);
        let cdf_100 = td.cdf(100.0);
        let cdf_200 = td.cdf(200.0);

        assert_eq!(cdf_0, 0.0);
        assert_eq!(cdf_100, 1.0);
        assert_eq!(cdf_200, 1.0);
        // CDF at 50 should be close to 0.5
        assert!((cdf_50 - 0.5).abs() < 0.1, "CDF at 50 was {}", cdf_50);
    }

    #[test]
    fn test_rank() {
        let mut td = TDigest::new(1000.0);

        td.add(10.0);
        td.add(20.0);
        td.add(30.0);
        td.add(40.0);
        td.add(50.0);
        td.add(60.0);

        assert_eq!(td.rank(0.0), -1); // Below min
        assert!(td.rank(10.0) >= 0);
        assert!(td.rank(60.0) >= 5);
    }

    #[test]
    fn test_trimmed_mean() {
        let mut td = TDigest::new(1000.0);

        for i in 1..=10 {
            td.add(i as f64);
        }

        // Full mean should be 5.5
        let full_mean = td.trimmed_mean(0.0, 1.0);
        assert!((full_mean - 5.5).abs() < 0.5, "Full mean was {}", full_mean);
    }

    #[test]
    fn test_merge() {
        let mut td1 = TDigest::new(100.0);
        let mut td2 = TDigest::new(100.0);

        for i in 1..=50 {
            td1.add(i as f64);
        }
        for i in 51..=100 {
            td2.add(i as f64);
        }

        td1.merge(&td2);

        assert_eq!(td1.min(), 1.0);
        assert_eq!(td1.max(), 100.0);
    }

    #[test]
    fn test_serialization() {
        let mut td = TDigest::new(100.0);

        for i in 1..=50 {
            td.add(i as f64);
        }
        td.compress();

        let bytes = td.to_bytes();
        let restored = TDigest::from_bytes(&bytes).unwrap();

        assert_eq!(td.compression(), restored.compression());
        assert_eq!(td.min(), restored.min());
        assert_eq!(td.max(), restored.max());
        assert_eq!(td.total_observations(), restored.total_observations());
    }

    #[test]
    fn test_empty() {
        let td = TDigest::new(100.0);

        assert!(td.is_empty());
        assert!(td.min().is_nan());
        assert!(td.max().is_nan());
    }

    #[test]
    fn test_reset() {
        let mut td = TDigest::new(100.0);

        for i in 1..=100 {
            td.add(i as f64);
        }

        td.reset();

        assert!(td.is_empty());
        assert_eq!(td.total_observations(), 0);
    }
}
