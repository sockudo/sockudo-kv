use roaring::RoaringTreemap;

/// Memory-efficient integer set using compressed bitmaps (Roaring Bitmaps).
/// Supports 64-bit integers and SIMD optimizations.
/// Replacing the legacy array-based IntSet.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct IntSet {
    pub(crate) inner: RoaringTreemap,
}

impl IntSet {
    pub fn new() -> Self {
        Self {
            inner: RoaringTreemap::new(),
        }
    }

    /// Check if set contains value
    pub fn contains(&self, value: i64) -> bool {
        self.inner.contains(value as u64)
    }

    /// Add value to set. Returns true if added.
    pub fn insert(&mut self, value: i64) -> bool {
        self.inner.insert(value as u64)
    }

    /// Remove value from set. Returns true if removed.
    pub fn remove(&mut self, value: i64) -> bool {
        self.inner.remove(value as u64)
    }

    pub fn len(&self) -> usize {
        self.inner.len() as usize
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Iterate over values
    pub fn iter(&self) -> impl Iterator<Item = i64> + '_ {
        self.inner.iter().map(|v| v as i64)
    }
}
