//! Generic storage operations
//!
//! Implements COPY, DUMP/RESTORE, KEYS pattern matching, SCAN cursor iteration,
//! RENAME, and other generic key operations.

use bytes::Bytes;
use dashmap::DashMap;
use dashmap::DashSet;
use std::collections::VecDeque;
use std::sync::atomic::Ordering;

use crate::storage::Store;
use crate::storage::types::{
    DataType, Entry, HyperLogLogData, SortedSetData, StreamData, TimeSeriesData, VectorSetData,
};
use crate::storage::value::now_ms;

// ======================== Pattern Matching ========================

/// Match a glob pattern against a string
/// Supports: * (any sequence), ? (single char), [abc] (char class), [^abc] (negated), \ (escape)
#[inline]
pub fn match_pattern(pattern: &[u8], key: &[u8]) -> bool {
    match_pattern_impl(pattern, key, 0, 0)
}

fn match_pattern_impl(pattern: &[u8], key: &[u8], mut pi: usize, mut ki: usize) -> bool {
    while pi < pattern.len() {
        match pattern[pi] {
            b'*' => {
                // Skip consecutive stars
                while pi < pattern.len() && pattern[pi] == b'*' {
                    pi += 1;
                }
                if pi == pattern.len() {
                    return true; // Trailing * matches everything
                }
                // Try matching rest from each position
                while ki <= key.len() {
                    if match_pattern_impl(pattern, key, pi, ki) {
                        return true;
                    }
                    ki += 1;
                }
                return false;
            }
            b'?' => {
                if ki >= key.len() {
                    return false;
                }
                pi += 1;
                ki += 1;
            }
            b'[' => {
                if ki >= key.len() {
                    return false;
                }
                pi += 1;
                let negated = pi < pattern.len() && pattern[pi] == b'^';
                if negated {
                    pi += 1;
                }
                let mut matched = false;
                let c = key[ki];
                while pi < pattern.len() && pattern[pi] != b']' {
                    if pi + 2 < pattern.len() && pattern[pi + 1] == b'-' {
                        // Range like [a-z]
                        if c >= pattern[pi] && c <= pattern[pi + 2] {
                            matched = true;
                        }
                        pi += 3;
                    } else {
                        if pattern[pi] == c {
                            matched = true;
                        }
                        pi += 1;
                    }
                }
                if pi < pattern.len() {
                    pi += 1; // Skip ]
                }
                if matched == negated {
                    return false;
                }
                ki += 1;
            }
            b'\\' => {
                // Escape next character
                pi += 1;
                if pi >= pattern.len() || ki >= key.len() || pattern[pi] != key[ki] {
                    return false;
                }
                pi += 1;
                ki += 1;
            }
            c => {
                if ki >= key.len() || key[ki] != c {
                    return false;
                }
                pi += 1;
                ki += 1;
            }
        }
    }
    ki == key.len()
}

// ======================== Store Extension Trait ========================

impl Store {
    // ==================== COPY ====================

    /// Copy a key to a new key. Returns:
    /// - Ok(true) if copied successfully
    /// - Ok(false) if source doesn't exist or dest exists and replace=false
    /// - Err if wrong type (shouldn't happen)
    pub fn copy_key(&self, source: &[u8], dest: &[u8], replace: bool) -> bool {
        // Get source entry
        let source_entry = match self.data.get(source) {
            Some(e) if !e.is_expired() => e,
            _ => return false,
        };

        // Check if dest exists when replace=false
        if !replace && self.exists(dest) {
            return false;
        }

        // Clone the data type
        let cloned_data = match &source_entry.data {
            DataType::String(s) => DataType::String(s.clone()),
            DataType::List(l) => DataType::List(l.clone()),
            DataType::Set(s) => {
                let new_set = DashSet::new();
                for item in s.iter() {
                    new_set.insert(item.clone());
                }
                DataType::Set(new_set)
            }
            DataType::Hash(h) => {
                let new_hash = DashMap::new();
                for item in h.iter() {
                    new_hash.insert(item.key().clone(), item.value().clone());
                }
                DataType::Hash(new_hash)
            }
            DataType::SortedSet(zs) => {
                let mut new_zs = SortedSetData::new();
                // Use scores HashMap for cloning (safe iteration)
                for (member, &score) in zs.scores.iter() {
                    new_zs.insert(member.clone(), score);
                }
                DataType::SortedSet(new_zs)
            }
            DataType::Stream(st) => {
                // StreamData has groups that don't implement Clone, so we do a shallow copy
                // Groups will need manual handling if deep copy is needed
                let new_stream = StreamData {
                    entries: st.entries.clone(),
                    last_id: st.last_id,
                    first_id: st.first_id,
                    entries_added: st.entries_added,
                    max_deleted_id: st.max_deleted_id,
                    groups: std::collections::HashMap::new(), // Empty groups for copy
                };
                DataType::Stream(new_stream)
            }
            DataType::HyperLogLog(hll) => DataType::HyperLogLog(HyperLogLogData {
                registers: hll.registers,
            }),
            DataType::Json(j) => DataType::Json(j.clone()),
            DataType::TimeSeries(ts) => {
                let new_ts = TimeSeriesData {
                    samples: ts.samples.clone(),
                    labels: ts.labels.clone(),
                    retention: ts.retention,
                    chunk_size: ts.chunk_size,
                    duplicate_policy: ts.duplicate_policy,
                    ignore_max_time_diff: ts.ignore_max_time_diff,
                    ignore_max_val_diff: ts.ignore_max_val_diff,
                    rules: ts.rules.clone(),
                    total_samples: ts.total_samples,
                    first_timestamp: ts.first_timestamp,
                    last_timestamp: ts.last_timestamp,
                };
                DataType::TimeSeries(Box::new(new_ts))
            }
            DataType::VectorSet(vs) => {
                // Deep clone vector set
                let new_vs = VectorSetData {
                    dim: vs.dim,
                    reduced_dim: vs.reduced_dim,
                    quant: vs.quant,
                    nodes: vs
                        .nodes
                        .iter()
                        .map(|n| crate::storage::types::VectorNode {
                            element: n.element.clone(),
                            vector: n.vector.clone(),
                            vector_q8: n.vector_q8.clone(),
                            vector_bin: n.vector_bin.clone(),
                            attributes: n.attributes.clone(),
                            connections: n.connections.clone(),
                            level: n.level,
                        })
                        .collect(),
                    element_index: vs.element_index.clone(),
                    entry_point: vs.entry_point,
                    max_level: vs.max_level,
                    m: vs.m,
                    m0: vs.m0,
                    ef_construction: vs.ef_construction,
                    level_mult: vs.level_mult,
                };
                DataType::VectorSet(Box::new(new_vs))
            }
        };

        // Copy TTL
        let expire = source_entry.expire_at_ms();
        drop(source_entry);

        // Remove existing if replace
        if replace {
            self.del(dest);
        }

        // Insert new entry
        let new_entry = match expire {
            Some(exp) => Entry::with_expire(cloned_data, exp),
            None => Entry::new(cloned_data),
        };

        self.data.insert(Bytes::copy_from_slice(dest), new_entry);
        self.key_count.fetch_add(1, Ordering::Relaxed);
        true
    }

    // ==================== EXPIREAT ====================

    /// Set absolute expiration time (milliseconds since epoch)
    /// Options: NX (only if no TTL), XX (only if has TTL), GT (only if new > current), LT (only if new < current)
    pub fn expire_at(
        &self,
        key: &[u8],
        timestamp_ms: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> bool {
        match self.data.get(key) {
            Some(entry) if !entry.is_expired() => {
                let current = entry.expire_at_ms();

                // NX: only set if no current expiration
                if nx && current.is_some() {
                    return false;
                }
                // XX: only set if has current expiration
                if xx && current.is_none() {
                    return false;
                }
                // GT: only set if new > current (or current is None)
                if gt
                    && let Some(curr) = current
                        && timestamp_ms <= curr {
                            return false;
                        }
                // LT: only set if new < current (or current is None)
                if lt
                    && let Some(curr) = current
                        && timestamp_ms >= curr {
                            return false;
                        }

                entry.set_expire_at(timestamp_ms);
                true
            }
            _ => false,
        }
    }

    /// Get absolute expiration time in milliseconds
    /// Returns -1 if no expiration, -2 if key doesn't exist
    pub fn expire_time_ms(&self, key: &[u8]) -> i64 {
        match self.data.get(key) {
            Some(entry) if !entry.is_expired() => entry.expire_at_ms().unwrap_or(-1),
            _ => -2,
        }
    }

    // ==================== KEYS ====================

    /// Get all keys matching a pattern
    /// WARNING: O(N) operation
    pub fn keys_pattern(&self, pattern: &[u8]) -> Vec<Bytes> {
        let mut result = Vec::new();

        // Fast path for "*" pattern
        if pattern == b"*" {
            for entry in self.data.iter() {
                if !entry.is_expired() {
                    result.push(entry.key().clone());
                }
            }
            return result;
        }

        for entry in self.data.iter() {
            if !entry.is_expired() && match_pattern(pattern, entry.key()) {
                result.push(entry.key().clone());
            }
        }
        result
    }

    // ==================== SCAN ====================

    /// Scan keys with cursor-based iteration
    /// Returns (next_cursor, keys)
    /// cursor=0 starts new iteration, returned cursor=0 means iteration complete
    pub fn scan(
        &self,
        cursor: u64,
        pattern: Option<&[u8]>,
        count: usize,
        type_filter: Option<&[u8]>,
    ) -> (u64, Vec<Bytes>) {
        let mut result = Vec::new();
        let count = count.max(10); // Minimum 10 keys per iteration

        // Simple cursor implementation: cursor = items_seen
        let mut seen = 0u64;
        let mut items_returned = 0usize;
        let mut new_cursor = 0u64;

        for entry in self.data.iter() {
            if entry.is_expired() {
                continue;
            }

            seen += 1;
            if seen <= cursor {
                continue;
            }

            // Type filter
            if let Some(type_name) = type_filter {
                let key_type = entry.data.type_name().as_bytes();
                if !key_type.eq_ignore_ascii_case(type_name) {
                    continue;
                }
            }

            // Pattern filter
            if let Some(pat) = pattern
                && !match_pattern(pat, entry.key()) {
                    continue;
                }

            result.push(entry.key().clone());
            items_returned += 1;

            if items_returned >= count {
                new_cursor = seen;
                break;
            }
        }

        // If we went through all items without hitting count, cursor = 0 (done)
        if items_returned < count {
            new_cursor = 0;
        }

        (new_cursor, result)
    }

    // ==================== RANDOMKEY ====================

    /// Get a random key from the store
    pub fn random_key(&self) -> Option<Bytes> {
        let len = self.data.len();
        if len == 0 {
            return None;
        }

        // Pick random index and iterate to find it
        let target = fastrand::usize(..len);
        let mut i = 0;
        for entry in self.data.iter() {
            if !entry.is_expired() {
                if i == target {
                    return Some(entry.key().clone());
                }
                i += 1;
            }
        }

        // Fallback: return first non-expired key
        for entry in self.data.iter() {
            if !entry.is_expired() {
                return Some(entry.key().clone());
            }
        }
        None
    }

    // ==================== RENAME ====================

    /// Rename a key (overwrites destination)
    pub fn rename(&self, key: &[u8], newkey: &[u8]) -> bool {
        if key == newkey {
            return self.exists(key);
        }

        // Remove source and get its entry
        let (_, entry) = match self.data.remove(key) {
            Some(pair) => pair,
            None => return false,
        };

        if entry.is_expired() {
            return false;
        }

        self.key_count.fetch_sub(1, Ordering::Relaxed);

        // Remove destination if exists
        if self.data.remove(newkey).is_some() {
            self.key_count.fetch_sub(1, Ordering::Relaxed);
        }

        // Insert with new key
        self.data.insert(Bytes::copy_from_slice(newkey), entry);
        self.key_count.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Rename only if destination doesn't exist
    pub fn rename_nx(&self, key: &[u8], newkey: &[u8]) -> i64 {
        if key == newkey {
            return if self.exists(key) { 0 } else { -1 };
        }

        // Check if dest exists
        if self.exists(newkey) {
            return 0;
        }

        // Remove source
        let (_, entry) = match self.data.remove(key) {
            Some(pair) => pair,
            None => return -1, // Source doesn't exist
        };

        if entry.is_expired() {
            return -1;
        }

        self.key_count.fetch_sub(1, Ordering::Relaxed);

        // Insert with new key
        self.data.insert(Bytes::copy_from_slice(newkey), entry);
        self.key_count.fetch_add(1, Ordering::Relaxed);
        1
    }

    // ==================== TOUCH ====================

    /// Touch keys (update last access time for LRU policies)
    /// Returns count of existing keys
    pub fn touch(&self, keys: &[Bytes]) -> i64 {
        let mut count = 0i64;
        for key in keys {
            if let Some(entry) = self.data.get(key.as_ref())
                && !entry.is_expired() {
                    // Just accessing the entry updates LRU in most implementations
                    // We don't have explicit LRU tracking, so this is a no-op
                    count += 1;
                }
        }
        count
    }

    // ==================== OBJECT ====================

    /// Get object encoding (internal representation)
    pub fn object_encoding(&self, key: &[u8]) -> Option<&'static str> {
        self.data.get(key).and_then(|e| {
            if e.is_expired() {
                None
            } else {
                Some(match &e.data {
                    DataType::String(s) => {
                        // Check if it's a small integer
                        if s.len() <= 20
                            && let Ok(s_str) = std::str::from_utf8(s)
                                && s_str.parse::<i64>().is_ok() {
                                    return Some("int");
                                }
                        if s.len() <= 44 { "embstr" } else { "raw" }
                    }
                    DataType::List(l) => {
                        if l.len() <= 512 {
                            "listpack"
                        } else {
                            "quicklist"
                        }
                    }
                    DataType::Set(s) => {
                        if s.len() <= 512 {
                            "listpack"
                        } else {
                            "hashtable"
                        }
                    }
                    DataType::Hash(h) => {
                        if h.len() <= 512 {
                            "listpack"
                        } else {
                            "hashtable"
                        }
                    }
                    DataType::SortedSet(zs) => {
                        if zs.len() <= 128 {
                            "listpack"
                        } else {
                            "skiplist"
                        }
                    }
                    DataType::Stream(_) => "stream",
                    DataType::HyperLogLog(_) => "raw",
                    DataType::Json(_) => "raw",
                    DataType::TimeSeries(_) => "raw",
                    DataType::VectorSet(_) => "raw",
                })
            }
        })
    }

    // ==================== DUMP/RESTORE ====================

    /// Serialize a key value for DUMP command
    /// Returns a compact binary format
    pub fn dump_key(&self, key: &[u8]) -> Option<Vec<u8>> {
        let entry = self.data.get(key)?;
        if entry.is_expired() {
            return None;
        }

        let mut buf = Vec::new();

        // Type byte
        let type_byte = match &entry.data {
            DataType::String(_) => 0u8,
            DataType::List(_) => 1,
            DataType::Set(_) => 2,
            DataType::Hash(_) => 3,
            DataType::SortedSet(_) => 4,
            DataType::Stream(_) => 5,
            DataType::HyperLogLog(_) => 6,
            DataType::Json(_) => 7,
            DataType::TimeSeries(_) => 8,
            DataType::VectorSet(_) => 9,
        };
        buf.push(type_byte);

        // Serialize based on type
        match &entry.data {
            DataType::String(s) => {
                write_varint(&mut buf, s.len() as u64);
                buf.extend_from_slice(s);
            }
            DataType::List(l) => {
                write_varint(&mut buf, l.len() as u64);
                for item in l.iter() {
                    write_varint(&mut buf, item.len() as u64);
                    buf.extend_from_slice(item);
                }
            }
            DataType::Set(s) => {
                write_varint(&mut buf, s.len() as u64);
                for item in s.iter() {
                    write_varint(&mut buf, item.len() as u64);
                    buf.extend_from_slice(&item);
                }
            }
            DataType::Hash(h) => {
                write_varint(&mut buf, h.len() as u64);
                for item in h.iter() {
                    write_varint(&mut buf, item.key().len() as u64);
                    buf.extend_from_slice(item.key());
                    write_varint(&mut buf, item.value().len() as u64);
                    buf.extend_from_slice(item.value());
                }
            }
            DataType::SortedSet(zs) => {
                write_varint(&mut buf, zs.len() as u64);
                // Use scores HashMap for stable iteration
                for (member, &score) in zs.scores.iter() {
                    write_varint(&mut buf, member.len() as u64);
                    buf.extend_from_slice(member);
                    buf.extend_from_slice(&score.to_le_bytes());
                }
            }
            _ => {
                // For complex types, just store type marker
                // Full serialization would require more work
            }
        }

        // Add 8-byte CRC placeholder (simple checksum)
        let checksum = simple_crc64(&buf);
        buf.extend_from_slice(&checksum.to_le_bytes());

        Some(buf)
    }

    /// Restore a key from serialized data
    pub fn restore_key(
        &self,
        key: &[u8],
        ttl_ms: i64,
        data: &[u8],
        replace: bool,
        absttl: bool,
    ) -> Result<(), &'static str> {
        if data.len() < 9 {
            return Err("ERR DUMP payload version or checksum are wrong");
        }

        // Verify checksum
        let payload = &data[..data.len() - 8];
        let stored_crc = u64::from_le_bytes(data[data.len() - 8..].try_into().unwrap());
        let computed_crc = simple_crc64(payload);
        if stored_crc != computed_crc {
            return Err("ERR DUMP payload version or checksum are wrong");
        }

        // Check if key exists
        if !replace && self.exists(key) {
            return Err("BUSYKEY Target key name already exists");
        }

        // Parse type and data
        let type_byte = payload[0];
        let mut pos = 1;

        let data_type = match type_byte {
            0 => {
                // String
                let (len, new_pos) = read_varint(payload, pos)?;
                pos = new_pos;
                if pos + len as usize > payload.len() {
                    return Err("ERR DUMP payload version or checksum are wrong");
                }
                DataType::String(Bytes::copy_from_slice(&payload[pos..pos + len as usize]))
            }
            1 => {
                // List
                let (count, new_pos) = read_varint(payload, pos)?;
                pos = new_pos;
                let mut list = VecDeque::with_capacity(count as usize);
                for _ in 0..count {
                    let (len, new_pos) = read_varint(payload, pos)?;
                    pos = new_pos;
                    if pos + len as usize > payload.len() {
                        return Err("ERR DUMP payload version or checksum are wrong");
                    }
                    list.push_back(Bytes::copy_from_slice(&payload[pos..pos + len as usize]));
                    pos += len as usize;
                }
                DataType::List(list)
            }
            2 => {
                // Set
                let (count, new_pos) = read_varint(payload, pos)?;
                pos = new_pos;
                let set = DashSet::new();
                for _ in 0..count {
                    let (len, new_pos) = read_varint(payload, pos)?;
                    pos = new_pos;
                    if pos + len as usize > payload.len() {
                        return Err("ERR DUMP payload version or checksum are wrong");
                    }
                    set.insert(Bytes::copy_from_slice(&payload[pos..pos + len as usize]));
                    pos += len as usize;
                }
                DataType::Set(set)
            }
            3 => {
                // Hash
                let (count, new_pos) = read_varint(payload, pos)?;
                pos = new_pos;
                let hash = DashMap::new();
                for _ in 0..count {
                    let (klen, new_pos) = read_varint(payload, pos)?;
                    pos = new_pos;
                    if pos + klen as usize > payload.len() {
                        return Err("ERR DUMP payload version or checksum are wrong");
                    }
                    let k = Bytes::copy_from_slice(&payload[pos..pos + klen as usize]);
                    pos += klen as usize;

                    let (vlen, new_pos) = read_varint(payload, pos)?;
                    pos = new_pos;
                    if pos + vlen as usize > payload.len() {
                        return Err("ERR DUMP payload version or checksum are wrong");
                    }
                    let v = Bytes::copy_from_slice(&payload[pos..pos + vlen as usize]);
                    pos += vlen as usize;

                    hash.insert(k, v);
                }
                DataType::Hash(hash)
            }
            4 => {
                // SortedSet
                let (count, new_pos) = read_varint(payload, pos)?;
                pos = new_pos;
                let mut zset = SortedSetData::new();
                for _ in 0..count {
                    let (len, new_pos) = read_varint(payload, pos)?;
                    pos = new_pos;
                    if pos + len as usize + 8 > payload.len() {
                        return Err("ERR DUMP payload version or checksum are wrong");
                    }
                    let member = Bytes::copy_from_slice(&payload[pos..pos + len as usize]);
                    pos += len as usize;
                    let score = f64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
                    pos += 8;
                    zset.insert(member, score);
                }
                DataType::SortedSet(zset)
            }
            _ => return Err("ERR DUMP payload version or checksum are wrong"),
        };

        // Remove existing if replace
        if replace {
            self.del(key);
        }

        // Calculate expiration
        let entry = if ttl_ms == 0 {
            Entry::new(data_type)
        } else if absttl {
            Entry::with_expire(data_type, ttl_ms)
        } else {
            Entry::with_expire(data_type, now_ms() + ttl_ms)
        };

        self.data.insert(Bytes::copy_from_slice(key), entry);
        self.key_count.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }
}

// ==================== Helper Functions ====================

fn write_varint(buf: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        buf.push((n as u8) | 0x80);
        n >>= 7;
    }
    buf.push(n as u8);
}

fn read_varint(data: &[u8], mut pos: usize) -> Result<(u64, usize), &'static str> {
    let mut result = 0u64;
    let mut shift = 0;
    loop {
        if pos >= data.len() {
            return Err("ERR DUMP payload version or checksum are wrong");
        }
        let byte = data[pos];
        pos += 1;
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 64 {
            return Err("ERR DUMP payload version or checksum are wrong");
        }
    }
    Ok((result, pos))
}

fn simple_crc64(data: &[u8]) -> u64 {
    // Simple CRC-64 implementation
    let mut crc: u64 = 0;
    for &byte in data {
        crc ^= (byte as u64) << 56;
        for _ in 0..8 {
            if crc & 0x8000_0000_0000_0000 != 0 {
                crc = (crc << 1) ^ 0x42F0E1EBA9EA3693;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_matching() {
        assert!(match_pattern(b"*", b"anything"));
        assert!(match_pattern(b"hello*", b"hello world"));
        assert!(match_pattern(b"*world", b"hello world"));
        assert!(match_pattern(b"h?llo", b"hello"));
        assert!(match_pattern(b"h[ae]llo", b"hello"));
        assert!(match_pattern(b"h[^i]llo", b"hello"));
        assert!(!match_pattern(b"h[^e]llo", b"hello"));
        assert!(match_pattern(b"h\\*llo", b"h*llo"));
        assert!(!match_pattern(b"hello", b"world"));
    }
}
