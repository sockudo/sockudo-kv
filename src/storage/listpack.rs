//! Listpack - Memory-efficient compact list of key-value pairs
//!
//! Listpack is a memory-efficient data structure for storing small collections of
//! key-value pairs (for hashes) or member-score pairs (for sorted sets).
//! It uses varint encoding to minimize memory overhead.
//!
//! Format: [count: u16] [entry1] [entry2] ... [entryN]
//! Entry format: [key_len: varint] [key_data] [value_len: varint] [value_data]
//!
//! For sorted sets, the value is an 8-byte f64 score encoded as little-endian bytes.

use bytes::Bytes;

/// Maximum number of entries before upgrading to a full data structure.
/// - Hash: 512 entries (1024 fields+values)
/// - SortedSet: 128 entries (member+score pairs)
pub const LISTPACK_HASH_MAX_ENTRIES: usize = 512;
pub const LISTPACK_ZSET_MAX_ENTRIES: usize = 128;

/// Maximum entry size in bytes before forcing upgrade
pub const LISTPACK_MAX_ENTRY_SIZE: usize = 64;

/// A memory-efficient packed list of key-value pairs
#[derive(Clone, Debug, Default)]
pub struct Listpack {
    /// Packed data: [count: u16] [entries...]
    data: Vec<u8>,
    /// Number of entries (cached for O(1) access)
    count: u16,
}

impl Listpack {
    /// Create a new empty listpack
    #[inline]
    pub fn new() -> Self {
        let mut data = Vec::with_capacity(64);
        // Reserve 2 bytes for count
        data.extend_from_slice(&0u16.to_le_bytes());
        Self { data, count: 0 }
    }

    /// Create with pre-allocated capacity
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        let mut data = Vec::with_capacity(2 + capacity * 20); // Estimate 20 bytes per entry
        data.extend_from_slice(&0u16.to_le_bytes());
        Self { data, count: 0 }
    }

    /// Number of entries
    #[inline]
    pub fn len(&self) -> usize {
        self.count as usize
    }

    /// Check if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Get memory usage in bytes
    #[inline]
    pub fn memory_usage(&self) -> usize {
        self.data.capacity() + std::mem::size_of::<Self>()
    }

    /// Check if an entry would be too large for listpack
    #[inline]
    pub fn entry_fits(key: &[u8], value: &[u8]) -> bool {
        key.len() <= LISTPACK_MAX_ENTRY_SIZE && value.len() <= LISTPACK_MAX_ENTRY_SIZE
    }

    /// Check if listpack is at capacity for hash usage
    #[inline]
    pub fn hash_at_capacity(&self) -> bool {
        self.count as usize >= LISTPACK_HASH_MAX_ENTRIES
    }

    /// Check if listpack is at capacity for sorted set usage
    #[inline]
    pub fn zset_at_capacity(&self) -> bool {
        self.count as usize >= LISTPACK_ZSET_MAX_ENTRIES
    }

    // ==================== Hash Operations ====================

    /// Find a key and return its position and the position after the value.
    /// Returns (key_start, value_start, value_end) or None if not found.
    fn find_key(&self, key: &[u8]) -> Option<(usize, usize, usize)> {
        let mut pos = 2; // Skip count header

        for _ in 0..self.count {
            let key_start = pos;

            // Read key length
            let (key_len, key_data_start) = read_varint(&self.data, pos)?;
            let key_data_end = key_data_start + key_len as usize;

            // Check if key matches using memchr for first byte optimization
            let found = if key.is_empty() {
                key_len == 0
            } else if key_len as usize == key.len() {
                // Fast path: use memchr to find first byte, then compare
                &self.data[key_data_start..key_data_end] == key
            } else {
                false
            };

            // Read value length to skip it
            let (value_len, value_data_start) = read_varint(&self.data, key_data_end)?;
            let value_end = value_data_start + value_len as usize;

            if found {
                return Some((key_start, value_data_start, value_end));
            }

            pos = value_end;
        }

        None
    }

    /// Get a value by key
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let (_, value_start, value_end) = self.find_key(key)?;
        Some(Bytes::copy_from_slice(&self.data[value_start..value_end]))
    }

    /// Check if key exists
    #[inline]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.find_key(key).is_some()
    }

    /// Insert or update a key-value pair.
    /// Returns true if this was a new key, false if updated.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> bool {
        if let Some((key_start, old_value_start, old_value_end)) = self.find_key(key) {
            // Update existing entry
            let old_value_len = old_value_end - old_value_start;
            let new_value_len = value.len();

            if old_value_len == new_value_len {
                // Same size: in-place update
                self.data[old_value_start..old_value_end].copy_from_slice(value);
            } else {
                // Different size: reconstruct
                // Find where the value length varint starts
                let (key_len, key_data_start) = read_varint(&self.data, key_start).unwrap();
                let key_data_end = key_data_start + key_len as usize;

                let mut new_data = Vec::with_capacity(self.data.len() + new_value_len);
                new_data.extend_from_slice(&self.data[..key_data_end]);
                write_varint(&mut new_data, new_value_len as u64);
                new_data.extend_from_slice(value);
                new_data.extend_from_slice(&self.data[old_value_end..]);

                self.data = new_data;
            }
            false
        } else {
            // New entry: append
            write_varint(&mut self.data, key.len() as u64);
            self.data.extend_from_slice(key);
            write_varint(&mut self.data, value.len() as u64);
            self.data.extend_from_slice(value);

            self.count += 1;
            self.update_count();
            true
        }
    }

    /// Remove a key. Returns the old value if it existed.
    pub fn remove(&mut self, key: &[u8]) -> Option<Bytes> {
        let (key_start, value_start, value_end) = self.find_key(key)?;

        // Extract value before removing
        let value = Bytes::copy_from_slice(&self.data[value_start..value_end]);

        // Remove the entire entry
        let remaining = self.data.len() - value_end;
        self.data.copy_within(value_end.., key_start);
        self.data.truncate(key_start + remaining);

        self.count -= 1;
        self.update_count();

        // Shrink if we freed a lot of space
        if self.data.capacity() > 256 && self.data.len() < self.data.capacity() / 4 {
            self.data.shrink_to_fit();
        }

        Some(value)
    }

    /// Iterate over all key-value pairs
    #[inline]
    pub fn iter(&self) -> ListpackIter<'_> {
        ListpackIter {
            data: &self.data,
            pos: 2,
            remaining: self.count,
        }
    }

    /// Get all keys
    #[inline]
    pub fn keys(&self) -> impl Iterator<Item = Bytes> + '_ {
        self.iter().map(|(k, _)| k)
    }

    /// Get all values
    #[inline]
    pub fn values(&self) -> impl Iterator<Item = Bytes> + '_ {
        self.iter().map(|(_, v)| v)
    }

    fn update_count(&mut self) {
        let bytes = self.count.to_le_bytes();
        self.data[0] = bytes[0];
        self.data[1] = bytes[1];
    }

    // ==================== Sorted Set Operations ====================

    /// Insert a member with score (for sorted sets).
    /// Score is stored as 8-byte little-endian f64.
    /// Returns true if new member, false if updated.
    pub fn zinsert(&mut self, member: &[u8], score: f64) -> bool {
        let score_bytes = score.to_le_bytes();

        if let Some((_key_start, value_start, _value_end)) = self.find_key(member) {
            // Update existing score (always 8 bytes)
            self.data[value_start..value_start + 8].copy_from_slice(&score_bytes);
            false
        } else {
            // New member: append in sorted order
            // For simplicity, we append at end. zrange will sort.
            write_varint(&mut self.data, member.len() as u64);
            self.data.extend_from_slice(member);
            write_varint(&mut self.data, 8); // Score is always 8 bytes
            self.data.extend_from_slice(&score_bytes);

            self.count += 1;
            self.update_count();
            true
        }
    }

    /// Get score for a member
    #[inline]
    pub fn zscore(&self, member: &[u8]) -> Option<f64> {
        let (_, value_start, _) = self.find_key(member)?;
        let score_bytes: [u8; 8] = self.data[value_start..value_start + 8].try_into().ok()?;
        Some(f64::from_le_bytes(score_bytes))
    }

    /// Remove a member (returns score if existed)
    #[inline]
    pub fn zremove(&mut self, member: &[u8]) -> Option<f64> {
        let value = self.remove(member)?;
        if value.len() == 8 {
            let score_bytes: [u8; 8] = value.as_ref().try_into().ok()?;
            Some(f64::from_le_bytes(score_bytes))
        } else {
            None
        }
    }

    /// Iterate over all member-score pairs
    #[inline]
    pub fn ziter(&self) -> impl Iterator<Item = (Bytes, f64)> + '_ {
        self.iter().filter_map(|(member, value)| {
            if value.len() == 8 {
                let score_bytes: [u8; 8] = value.as_ref().try_into().ok()?;
                Some((member, f64::from_le_bytes(score_bytes)))
            } else {
                None
            }
        })
    }

    /// Get all members sorted by score
    pub fn zrange_sorted(&self) -> Vec<(Bytes, f64)> {
        let mut entries: Vec<(Bytes, f64)> = self.ziter().collect();
        entries.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        entries
    }

    /// Get rank of a member (0-indexed, sorted by score ascending)
    pub fn zrank(&self, member: &[u8]) -> Option<usize> {
        let target_score = self.zscore(member)?;
        let target_member = member;

        let sorted = self.zrange_sorted();
        sorted.iter().position(|(m, s)| {
            (*s == target_score || (*s - target_score).abs() < f64::EPSILON)
                && m.as_ref() == target_member
        })
    }

    // ==================== Conversion Methods ====================

    /// Convert to a Vec of key-value pairs (for upgrading to DashTable)
    #[inline]
    pub fn to_hash_vec(&self) -> Vec<(Bytes, Bytes)> {
        self.iter().collect()
    }

    /// Convert to a Vec of member-score pairs (for upgrading to SortedSetData)
    #[inline]
    pub fn to_zset_vec(&self) -> Vec<(Bytes, f64)> {
        self.ziter().collect()
    }

    /// Create from iterator of key-value pairs
    pub fn from_hash_iter<I: IntoIterator<Item = (Bytes, Bytes)>>(iter: I) -> Self {
        let mut lp = Self::new();
        for (key, value) in iter {
            lp.insert(&key, &value);
        }
        lp
    }

    /// Create from iterator of member-score pairs
    pub fn from_zset_iter<I: IntoIterator<Item = (Bytes, f64)>>(iter: I) -> Self {
        let mut lp = Self::new();
        for (member, score) in iter {
            lp.zinsert(&member, score);
        }
        lp
    }

    /// Clear all entries
    pub fn clear(&mut self) {
        self.data.truncate(2);
        self.data[0] = 0;
        self.data[1] = 0;
        self.count = 0;
    }
}

/// Iterator over listpack entries
pub struct ListpackIter<'a> {
    data: &'a [u8],
    pos: usize,
    remaining: u16,
}

impl<'a> Iterator for ListpackIter<'a> {
    type Item = (Bytes, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        // Read key
        let (key_len, key_start) = read_varint(self.data, self.pos)?;
        let key_end = key_start + key_len as usize;
        let key = Bytes::copy_from_slice(&self.data[key_start..key_end]);

        // Read value
        let (value_len, value_start) = read_varint(self.data, key_end)?;
        let value_end = value_start + value_len as usize;
        let value = Bytes::copy_from_slice(&self.data[value_start..value_end]);

        self.pos = value_end;
        self.remaining -= 1;

        Some((key, value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.remaining as usize;
        (remaining, Some(remaining))
    }
}

impl<'a> ExactSizeIterator for ListpackIter<'a> {}

// ==================== Varint Utilities ====================

/// Write a varint to a buffer
#[inline]
fn write_varint(buf: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        buf.push((n as u8) | 0x80);
        n >>= 7;
    }
    buf.push(n as u8);
}

/// Read a varint from data at position. Returns (value, position_after_varint).
#[inline]
fn read_varint(data: &[u8], mut pos: usize) -> Option<(u64, usize)> {
    let mut result = 0u64;
    let mut shift = 0;

    while pos < data.len() {
        let byte = data[pos];
        pos += 1;
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some((result, pos));
        }
        shift += 7;
        if shift >= 64 {
            return None; // Overflow protection
        }
    }
    None
}

// ==================== Tests ====================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_listpack_basic_operations() {
        let mut lp = Listpack::new();

        // Insert
        assert!(lp.insert(b"key1", b"value1"));
        assert!(lp.insert(b"key2", b"value2"));
        assert_eq!(lp.len(), 2);

        // Get
        assert_eq!(lp.get(b"key1"), Some(Bytes::from("value1")));
        assert_eq!(lp.get(b"key2"), Some(Bytes::from("value2")));
        assert_eq!(lp.get(b"nonexistent"), None);

        // Update
        assert!(!lp.insert(b"key1", b"newvalue"));
        assert_eq!(lp.get(b"key1"), Some(Bytes::from("newvalue")));
        assert_eq!(lp.len(), 2);

        // Contains
        assert!(lp.contains_key(b"key1"));
        assert!(!lp.contains_key(b"nonexistent"));

        // Remove
        assert_eq!(lp.remove(b"key1"), Some(Bytes::from("newvalue")));
        assert_eq!(lp.len(), 1);
        assert_eq!(lp.get(b"key1"), None);
    }

    #[test]
    fn test_listpack_iteration() {
        let mut lp = Listpack::new();
        lp.insert(b"a", b"1");
        lp.insert(b"b", b"2");
        lp.insert(b"c", b"3");

        let items: Vec<_> = lp.iter().collect();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0], (Bytes::from("a"), Bytes::from("1")));
        assert_eq!(items[1], (Bytes::from("b"), Bytes::from("2")));
        assert_eq!(items[2], (Bytes::from("c"), Bytes::from("3")));
    }

    #[test]
    fn test_listpack_sorted_set() {
        let mut lp = Listpack::new();

        // Insert members with scores
        assert!(lp.zinsert(b"member1", 1.5));
        assert!(lp.zinsert(b"member2", 2.5));
        assert!(lp.zinsert(b"member3", 0.5));
        assert_eq!(lp.len(), 3);

        // Get scores
        assert_eq!(lp.zscore(b"member1"), Some(1.5));
        assert_eq!(lp.zscore(b"member2"), Some(2.5));
        assert_eq!(lp.zscore(b"nonexistent"), None);

        // Update score
        assert!(!lp.zinsert(b"member1", 3.0));
        assert_eq!(lp.zscore(b"member1"), Some(3.0));

        // Sorted range
        let sorted = lp.zrange_sorted();
        assert_eq!(sorted.len(), 3);
        assert_eq!(sorted[0].0, Bytes::from("member3")); // 0.5
        assert_eq!(sorted[1].0, Bytes::from("member2")); // 2.5
        assert_eq!(sorted[2].0, Bytes::from("member1")); // 3.0

        // Rank
        assert_eq!(lp.zrank(b"member3"), Some(0)); // lowest score
        assert_eq!(lp.zrank(b"member2"), Some(1));
        assert_eq!(lp.zrank(b"member1"), Some(2)); // highest score

        // Remove
        assert_eq!(lp.zremove(b"member2"), Some(2.5));
        assert_eq!(lp.len(), 2);
    }

    #[test]
    fn test_listpack_empty_keys_values() {
        let mut lp = Listpack::new();

        // Empty key
        assert!(lp.insert(b"", b"empty_key_value"));
        assert_eq!(lp.get(b""), Some(Bytes::from("empty_key_value")));

        // Empty value
        assert!(lp.insert(b"empty_value", b""));
        assert_eq!(lp.get(b"empty_value"), Some(Bytes::from("")));

        // Both empty
        lp.clear();
        assert!(lp.insert(b"", b""));
        assert_eq!(lp.get(b""), Some(Bytes::from("")));
    }

    #[test]
    fn test_listpack_capacity_checks() {
        let lp = Listpack::new();
        assert!(!lp.hash_at_capacity());
        assert!(!lp.zset_at_capacity());

        // Check entry size limits
        assert!(Listpack::entry_fits(b"small", b"value"));
        let large = vec![b'x'; LISTPACK_MAX_ENTRY_SIZE + 1];
        assert!(!Listpack::entry_fits(&large, b"value"));
    }

    #[test]
    fn test_listpack_conversion() {
        let mut lp = Listpack::new();
        lp.insert(b"a", b"1");
        lp.insert(b"b", b"2");

        let vec = lp.to_hash_vec();
        assert_eq!(vec.len(), 2);

        let lp2 = Listpack::from_hash_iter(vec);
        assert_eq!(lp2.len(), 2);
        assert_eq!(lp2.get(b"a"), Some(Bytes::from("1")));
    }

    #[test]
    fn test_varint_encoding() {
        // Test various sizes
        let test_values = [0u64, 1, 127, 128, 255, 256, 16383, 16384, u32::MAX as u64];

        for &val in &test_values {
            let mut buf = Vec::new();
            write_varint(&mut buf, val);
            let (decoded, _) = read_varint(&buf, 0).unwrap();
            assert_eq!(decoded, val, "Failed for value {}", val);
        }
    }
}
