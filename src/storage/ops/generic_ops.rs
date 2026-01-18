//! Generic storage operations
//!
//! Implements COPY, DUMP/RESTORE, KEYS pattern matching, SCAN cursor iteration,
//! RENAME, and other generic key operations.
use bytes::Bytes;
use dashmap::DashSet;
use std::sync::atomic::Ordering;

use crate::storage::Store;
use crate::storage::dashtable::{DashTable, calculate_hash};
use crate::storage::types::{DataType, Entry, SortedSetData, StreamData, VectorSetData};

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
        let source_entry = match self.data_get(source) {
            Some(e) if !e.1.is_expired() => e,
            _ => return false,
        };

        // Check if dest exists when replace=false
        if !replace && self.exists(dest) {
            return false;
        }

        // Clone the data type
        let cloned_data = match &source_entry.1.data {
            DataType::String(s) => DataType::String(s.clone()),
            DataType::RawString(s) => DataType::RawString(s.clone()),
            DataType::List(l) => DataType::List(l.clone()),
            DataType::Set(s) => DataType::Set(s.clone()),
            DataType::IntSet(s) => DataType::IntSet(s.clone()),
            DataType::Hash(h) => {
                let new_hash = DashTable::with_shard_amount(h.shards_len());
                for item in h.iter() {
                    let h_val = calculate_hash(&item.0);
                    new_hash.insert_unique(h_val, (item.0.clone(), item.1.clone()), |kv| {
                        calculate_hash(&kv.0)
                    });
                }
                DataType::Hash(new_hash)
            }
            DataType::SortedSet(zs) => {
                let mut new_zs = SortedSetData::new();
                for (member, &score) in zs.scores.iter() {
                    new_zs.insert(member.clone(), score);
                }
                DataType::SortedSet(new_zs)
            }
            DataType::Stream(st) => {
                let new_stream = StreamData {
                    entries: st.entries.clone(),
                    last_id: st.last_id,
                    first_id: st.first_id,
                    entries_added: st.entries_added,
                    max_deleted_id: st.max_deleted_id,
                    groups: std::collections::HashMap::new(),
                };
                DataType::Stream(new_stream)
            }
            DataType::HyperLogLog(hll) => DataType::HyperLogLog(hll.clone()),
            DataType::Json(j) => DataType::Json(j.clone()),
            DataType::TimeSeries(ts) => DataType::TimeSeries(ts.clone()),
            DataType::VectorSet(vs) => {
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
            DataType::HashPacked(lp) => DataType::HashPacked(lp.clone()),
            DataType::SortedSetPacked(lp) => DataType::SortedSetPacked(lp.clone()),
            DataType::SetPacked(lp) => DataType::SetPacked(lp.clone()),
            #[cfg(feature = "bloom")]
            DataType::BloomFilter(bf) => DataType::BloomFilter(bf.clone()),
            #[cfg(feature = "bloom")]
            DataType::CuckooFilter(cf) => DataType::CuckooFilter(cf.clone()),
            #[cfg(feature = "bloom")]
            DataType::TDigest(td) => DataType::TDigest(td.clone()),
            #[cfg(feature = "bloom")]
            DataType::TopK(tk) => DataType::TopK(tk.clone()),
            #[cfg(feature = "bloom")]
            DataType::CountMinSketch(cms) => DataType::CountMinSketch(cms.clone()),
        };

        let new_entry = Entry::new(cloned_data);
        let expire_ms = source_entry.1.expire_at_ms();
        if let Some(expire) = expire_ms {
            new_entry.set_expire_at(expire);
        }

        if replace {
            self.data_remove(dest);
        }

        let dest_bytes = Bytes::copy_from_slice(dest);
        self.data_insert(dest_bytes.clone(), new_entry);
        self.key_count.fetch_add(1, Ordering::Relaxed);

        // Add to expiration index if copied key has expiration
        if let Some(expire) = expire_ms {
            self.expiration_index.add(dest_bytes, expire);
        }

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
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return false;
                }

                if nx && entry.expire_at_ms().is_some() {
                    return false;
                }

                let old_expire = entry.expire_at_ms();
                let should_update = if xx || gt || lt {
                    let current = old_expire;
                    xx || (gt && current.map_or(true, |c| timestamp_ms > c))
                        || (lt && current.map_or(false, |c| timestamp_ms < c))
                } else {
                    !nx
                };

                if should_update {
                    entry.set_expire_at(timestamp_ms);

                    // Update expiration index
                    let key_bytes = e.get().0.clone();
                    self.expiration_index
                        .update(key_bytes, old_expire, timestamp_ms);

                    true
                } else {
                    false
                }
            }
            crate::storage::dashtable::Entry::Vacant(_) => false,
        }
    }

    /// Get absolute expiration time in milliseconds
    /// Returns -1 if no expiration, -2 if key doesn't exist
    pub fn expire_time_ms(&self, key: &[u8]) -> i64 {
        match self.data_get(key) {
            Some(entry_ref) if !entry_ref.1.is_expired() => {
                entry_ref.1.expire_at_ms().unwrap_or(-1)
            }
            _ => -2,
        }
    }

    /// Remove the expiration from a key.
    /// Returns true if the key existed and expiration was removed, false otherwise.
    pub fn persist(&self, key: &[u8]) -> bool {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    false
                } else {
                    let old_expire = entry.expire_at_ms();
                    let had_expire = entry.persist();

                    // Remove from expiration index if it had an expiration
                    if had_expire {
                        if let Some(expire_ms) = old_expire {
                            let key_bytes = e.get().0.clone();
                            self.expiration_index.remove(&key_bytes, expire_ms);
                        }
                    }

                    had_expire
                }
            }
            crate::storage::dashtable::Entry::Vacant(_) => false,
        }
    }

    // ==================== KEYS ====================

    /// Get all keys matching a pattern
    pub fn keys_pattern(&self, pattern: &[u8]) -> Vec<Bytes> {
        let mut keys = Vec::new();
        self.data.for_each(|kv| {
            if !kv.1.is_expired() && match_pattern(pattern, &kv.0) {
                keys.push(kv.0.clone());
            }
        });
        keys
    }

    /// SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
    ///
    /// This implementation uses the cursor as a shard index.
    /// It returns all keys in the current shard and moves to the next shard.
    /// This is a simple implementation that respects the stateless nature of SCAN.
    pub fn scan(
        &self,
        cursor: u64,
        pattern: Option<&[u8]>,
        _count: usize, // We ignore count and return one shard at a time
        type_filter: Option<&[u8]>,
    ) -> (u64, Vec<Bytes>) {
        let mut keys = Vec::new();
        let mut expired_keys = Vec::new();
        let shard_idx = cursor as usize;
        let shards_len = self.data.shards_len();

        if shard_idx >= shards_len {
            return (0, keys);
        }

        self.data.for_each_in_shard(shard_idx, |kv| {
            // Pattern filter comes first - keys not matching pattern are skipped entirely
            // and do NOT trigger passive expiration (Redis behavior)
            let matches_pattern = pattern.map_or(true, |p| match_pattern(p, &kv.0));
            if !matches_pattern {
                return;
            }

            // Expiration check comes after pattern but before type filter
            // Expired keys matching pattern trigger passive expiration
            if kv.1.is_expired() {
                expired_keys.push(kv.0.clone());
                return;
            }

            // Type filter comes last
            let matches_type = type_filter.map_or(true, |tf| {
                let type_name = match &kv.1.data {
                    DataType::String(_) => "string",
                    DataType::List(_) => "list",
                    DataType::Set(_) => "set",
                    DataType::Hash(_) => "hash",
                    DataType::SortedSet(_) => "zset",
                    _ => "unknown",
                };
                type_name.as_bytes() == tf
            });

            if matches_type {
                keys.push(kv.0.clone());
            }
        });

        // Passive expiration: delete expired keys encountered during scan
        for key in expired_keys {
            self.del(&key);
        }

        let next_cursor = if shard_idx + 1 < shards_len {
            (shard_idx + 1) as u64
        } else {
            0
        };

        (next_cursor, keys)
    }

    /// Get count of keys
    pub fn dbsize(&self) -> usize {
        self.data.len()
    }

    /// Randomly pick a key
    pub fn random_key(&self) -> Option<Bytes> {
        let len = self.data.len();
        if len == 0 {
            return None;
        }
        self.data
            .iter()
            .nth(fastrand::usize(0..len))
            .filter(|entry| !entry.1.is_expired())
            .map(|entry| entry.0.clone())
    }

    /// RENAME key newkey
    pub fn rename(&self, key: &[u8], newkey: &[u8]) -> bool {
        if let Some((_, entry)) = self.data_remove(key) {
            self.data_remove(newkey);
            self.data_insert(Bytes::copy_from_slice(newkey), entry);
            true
        } else {
            false
        }
    }

    /// RENAMENX key newkey
    /// Returns 1 if renamed, 0 if newkey exists, -1 if key doesn't exist
    pub fn rename_nx(&self, key: &[u8], newkey: &[u8]) -> i64 {
        if self.exists(newkey) {
            return 0;
        }
        if self.rename(key, newkey) { 1 } else { -1 }
    }

    /// TOUCH keys - update access time and return count of existing keys
    pub fn touch(&self, keys: &[Bytes]) -> i64 {
        let mut count = 0;
        for key in keys {
            if self.exists(key) {
                count += 1;
            }
        }
        count
    }

    /// Get object encoding info
    pub fn object_encoding(&self, key: &[u8]) -> Option<&'static str> {
        let entry_ref = self.data_get(key)?;
        if entry_ref.1.is_expired() {
            return None;
        }
        Some(match &entry_ref.1.data {
            DataType::String(s) => {
                // Check if string can be represented as an integer
                if std::str::from_utf8(s)
                    .ok()
                    .and_then(|str_val| str_val.parse::<i64>().ok())
                    .is_some()
                {
                    "int"
                } else if s.len() <= 44 {
                    "embstr"
                } else {
                    "raw"
                }
            }
            DataType::RawString(_) => {
                // RawString is always "raw" encoding (modified by SETRANGE/APPEND)
                "raw"
            }
            DataType::List(list) => {
                // Redis 7.0+: use is_listpack_encoding() to properly determine encoding
                // based on node count AND whether the single node fits within limits
                if list.is_listpack_encoding() {
                    "listpack"
                } else {
                    "quicklist"
                }
            }
            DataType::Set(_) => "hashtable",
            DataType::SetPacked(_) => "listpack",
            DataType::IntSet(_) => "intset",
            DataType::Hash(_) => "hashtable",
            DataType::HashPacked(_) => "listpack",
            DataType::SortedSet(_) => "skiplist",
            DataType::SortedSetPacked(_) => "listpack",
            DataType::Stream(_) => "stream",
            _ => "unknown",
        })
    }

    /// Get Type Name
    pub fn key_type(&self, key: &[u8]) -> Option<&'static str> {
        let entry_ref = self.data_get(key)?;
        if entry_ref.1.is_expired() {
            return None;
        }
        Some(entry_ref.1.data.type_name())
    }

    /// Estimate memory usage for a key (like Redis MEMORY USAGE)
    /// Returns memory in bytes including key overhead, value, and internal structures
    /// This aims to match Redis's MEMORY USAGE output for compatibility
    pub fn memory_usage(&self, key: &[u8], samples: usize) -> Option<usize> {
        let entry_ref = self.data_get(key)?;
        if entry_ref.1.is_expired() {
            return None;
        }

        // Redis uses a kvobj (16 bytes on 64-bit) + SDS strings for key and value
        // We estimate to be compatible with Redis's MEMORY USAGE output
        const KVOBJ_OVERHEAD: usize = 16; // Redis kvobj header size on 64-bit

        // SDS header size depends on string length:
        // sdshdr5: 1 byte for strings up to 31 bytes
        // sdshdr8: 3 bytes for strings up to 255 bytes
        // sdshdr16: 5 bytes for strings up to 65535 bytes
        fn sds_overhead(len: usize) -> usize {
            if len < 32 {
                1
            } else if len < 256 {
                3
            } else {
                5
            }
        }

        // Key size: SDS header + key bytes + null terminator
        let key_size = sds_overhead(key.len()) + key.len() + 1;

        // Data size varies by type
        let data_size = match &entry_ref.1.data {
            DataType::String(s) | DataType::RawString(s) => {
                // SDS header + value bytes + null terminator
                sds_overhead(s.len()) + s.len() + 1
            }
            DataType::List(l) => {
                // QuickList: each node has overhead + data
                let mut total = 64; // QuickList base overhead
                let len = l.len();
                if len > 0 {
                    let sample_count = samples.min(len);
                    let mut sample_size = 0;
                    for (i, item) in l.iter().enumerate() {
                        if i >= sample_count {
                            break;
                        }
                        sample_size += sds_overhead(item.len()) + item.len() + 1;
                    }
                    total += (sample_size * len) / sample_count;
                    total += len * 32; // Node overhead
                }
                total
            }
            DataType::Set(s) => {
                // Set: hash table overhead + elements
                let len = s.len();
                let mut total = 64;
                if len > 0 {
                    let sample_count = samples.min(len);
                    let mut sample_size = 0;
                    for (i, item) in s.iter().enumerate() {
                        if i >= sample_count {
                            break;
                        }
                        sample_size += sds_overhead(item.len()) + item.len() + 1 + 8;
                    }
                    total += (sample_size * len) / sample_count;
                }
                total
            }
            DataType::IntSet(s) => {
                // IntSet: compact integer storage
                let len = s.len();
                16 + len * 8
            }
            DataType::Hash(h) => {
                // Hash: hash table overhead + field-value pairs
                let len = h.len();
                let mut total = 64;
                if len > 0 {
                    let sample_count = samples.min(len);
                    let mut sample_size = 0;
                    let mut counted = 0;
                    h.for_each(|kv| {
                        if counted >= sample_count {
                            return;
                        }
                        sample_size += sds_overhead(kv.0.len()) + kv.0.len() + 1;
                        sample_size += sds_overhead(kv.1.len()) + kv.1.len() + 1;
                        sample_size += 8; // Hash entry overhead
                        counted += 1;
                    });
                    if counted > 0 {
                        total += (sample_size * len) / counted;
                    }
                }
                total
            }
            DataType::HashPacked(lp) => {
                // Listpack: compact storage
                32 + lp.len() * 10
            }
            DataType::SortedSet(zs) => {
                // SortedSet: skiplist + hash
                let len = zs.len();
                let mut total = 64;
                if len > 0 {
                    let sample_count = samples.min(len);
                    let mut sample_size = 0;
                    for (i, (member, _)) in zs.scores.iter().enumerate() {
                        if i >= sample_count {
                            break;
                        }
                        sample_size += sds_overhead(member.len()) + member.len() + 1 + 8 + 32;
                    }
                    total += (sample_size * len) / sample_count;
                }
                total
            }
            DataType::SortedSetPacked(lp) => {
                // Listpack: compact storage
                32 + lp.len() * 12
            }
            DataType::SetPacked(lp) => {
                // Listpack: compact storage for small sets
                32 + lp.len() * 10
            }
            DataType::Stream(st) => {
                // Stream: radix tree + consumer groups
                let entries_len = st.entries.len();
                let mut total = 64;
                if entries_len > 0 {
                    let sample_count = samples.min(entries_len);
                    let mut sample_size = 0;
                    for (i, (_, fields)) in st.entries.iter().enumerate() {
                        if i >= sample_count {
                            break;
                        }
                        sample_size += 16 + 16; // StreamId + listpack header
                        for (k, v) in fields {
                            sample_size += sds_overhead(k.len()) + k.len() + 1;
                            sample_size += sds_overhead(v.len()) + v.len() + 1;
                        }
                    }
                    total += (sample_size * entries_len) / sample_count;
                }
                total += st.groups.len() * 128;
                total
            }
            DataType::HyperLogLog(_) => {
                // HLL: fixed size registers
                12304
            }
            DataType::Json(j) => {
                // JSON: estimate based on string representation
                let json_str = sonic_rs::to_string(j.as_ref()).unwrap_or_default();
                32 + json_str.len()
            }
            DataType::TimeSeries(ts) => ts.memory_usage(),
            DataType::VectorSet(vs) => {
                // VectorSet: HNSW graph
                64 + vs.len() * (vs.dim * 4 + 64)
            }
            #[cfg(feature = "bloom")]
            DataType::BloomFilter(bf) => bf.size_bytes() + 32,
            #[cfg(feature = "bloom")]
            DataType::CuckooFilter(cf) => cf.size_bytes() + 32,
            #[cfg(feature = "bloom")]
            DataType::TDigest(td) => td.memory_usage(),
            #[cfg(feature = "bloom")]
            DataType::TopK(tk) => tk.memory_usage(),
            #[cfg(feature = "bloom")]
            DataType::CountMinSketch(cms) => cms.memory_usage(),
        };

        Some(KVOBJ_OVERHEAD + key_size + data_size)
    }

    /// Serialize a key value for DUMP command
    pub fn dump_key(&self, key: &[u8]) -> Option<Vec<u8>> {
        let entry_ref = self.data_get(key)?;
        if entry_ref.1.is_expired() {
            return None;
        }

        let mut data = Vec::new();
        data.push(1); // Version

        let expire = entry_ref.1.expire_at_ms().unwrap_or(0);
        data.extend_from_slice(&expire.to_le_bytes());

        let type_byte = match &entry_ref.1.data {
            DataType::String(_) | DataType::RawString(_) => 0u8,
            DataType::List(_) => 1,
            DataType::Set(_) => 2,
            DataType::IntSet(_) => 2,    // Dump as regular set
            DataType::SetPacked(_) => 2, // Dump as regular set
            DataType::Hash(_) | DataType::HashPacked(_) => 3,
            DataType::SortedSet(_) | DataType::SortedSetPacked(_) => 4,
            DataType::Stream(_) => 5,
            DataType::HyperLogLog(_) => 6,
            DataType::Json(_) => 7,
            DataType::TimeSeries(_) => 8,
            DataType::VectorSet(_) => 9,
            #[cfg(feature = "bloom")]
            DataType::BloomFilter(_) => 10,
            #[cfg(feature = "bloom")]
            DataType::CuckooFilter(_) => 11,
            #[cfg(feature = "bloom")]
            DataType::TDigest(_) => 12,
            #[cfg(feature = "bloom")]
            DataType::TopK(_) => 13,
            #[cfg(feature = "bloom")]
            DataType::CountMinSketch(_) => 14,
        };
        data.push(type_byte);

        match &entry_ref.1.data {
            DataType::String(s) | DataType::RawString(s) => {
                write_varint(&mut data, s.len() as u64);
                data.extend_from_slice(s);
            }
            DataType::List(l) => {
                write_varint(&mut data, l.len() as u64);
                for item in l.iter() {
                    write_varint(&mut data, item.len() as u64);
                    data.extend_from_slice(&item);
                }
            }
            DataType::Set(s) => {
                write_varint(&mut data, s.len() as u64);
                for item in s.iter() {
                    write_varint(&mut data, item.len() as u64);
                    data.extend_from_slice(&item);
                }
            }
            DataType::IntSet(s) => {
                // Dump as regular set
                write_varint(&mut data, s.len() as u64);

                // Manual iteration over IntSet
                for item in s.iter() {
                    let s_val = item.to_string();
                    write_varint(&mut data, s_val.len() as u64);
                    data.extend_from_slice(s_val.as_bytes());
                }
            }
            DataType::Hash(h) => {
                write_varint(&mut data, h.len() as u64);
                h.for_each(|kv| {
                    write_varint(&mut data, kv.0.len() as u64);
                    data.extend_from_slice(&kv.0);
                    write_varint(&mut data, kv.1.len() as u64);
                    data.extend_from_slice(&kv.1);
                });
            }
            DataType::HashPacked(lp) => {
                write_varint(&mut data, lp.len() as u64);
                for (k, v) in lp.iter() {
                    write_varint(&mut data, k.len() as u64);
                    data.extend_from_slice(&k);
                    write_varint(&mut data, v.len() as u64);
                    data.extend_from_slice(&v);
                }
            }
            DataType::SortedSet(zs) => {
                write_varint(&mut data, zs.len() as u64);
                for (member, &score) in zs.scores.iter() {
                    write_varint(&mut data, member.len() as u64);
                    data.extend_from_slice(member);
                    data.extend_from_slice(&score.to_le_bytes());
                }
            }
            DataType::SortedSetPacked(lp) => {
                write_varint(&mut data, lp.len() as u64);
                for (member, score) in lp.ziter() {
                    write_varint(&mut data, member.len() as u64);
                    data.extend_from_slice(&member);
                    data.extend_from_slice(&score.to_le_bytes());
                }
            }
            #[cfg(feature = "bloom")]
            DataType::BloomFilter(bf) => {
                let bytes = bf.to_bytes();
                write_varint(&mut data, bytes.len() as u64);
                data.extend_from_slice(&bytes);
            }
            #[cfg(feature = "bloom")]
            DataType::CuckooFilter(cf) => {
                let bytes = cf.to_bytes();
                write_varint(&mut data, bytes.len() as u64);
                data.extend_from_slice(&bytes);
            }
            #[cfg(feature = "bloom")]
            DataType::TDigest(td) => {
                let bytes = td.to_bytes();
                write_varint(&mut data, bytes.len() as u64);
                data.extend_from_slice(&bytes);
            }
            _ => {}
        }

        let checksum = simple_crc64(&data);
        data.extend_from_slice(&checksum.to_le_bytes());

        Some(data)
    }

    /// Restore a key from serialized data
    pub fn restore_key(
        &self,
        key: &[u8],
        ttl_ms: i64,
        data: &[u8],
        replace: bool,
        absttl: bool,
        _sanitize: bool,
    ) -> std::result::Result<(), String> {
        let data_len = data.len();
        if data_len < 10 {
            return Err("ERR invalid dump data".into());
        }

        if !replace && self.exists(key) {
            return Err("BUSYKEY Target key name already exists.".into());
        }

        let data_len = data.len();
        let payload = &data[..data_len - 8];
        let expected_crc = u64::from_le_bytes(data[data_len - 8..].try_into().unwrap());
        if simple_crc64(payload) != expected_crc {
            return Err("ERR invalid checksum".into());
        }

        if data[0] != 1 {
            return Err("ERR unknown serialization version".into());
        }
        let expire_at_meta = i64::from_le_bytes(data[1..9].try_into().unwrap());

        let type_byte = data[9];
        let mut pos = 10;

        let data_type = match type_byte {
            0 => {
                let (len, new_pos) = read_varint(data, pos).map_err(|e| e.to_string())?;
                pos = new_pos;
                if pos + len as usize > data_len - 8 {
                    return Err("ERR invalid string length".into());
                }
                let s = &data[pos..pos + len as usize];
                DataType::String(Bytes::copy_from_slice(s))
            }
            1 => {
                let (count, new_pos) = read_varint(data, pos).map_err(|e| e.to_string())?;
                pos = new_pos;
                let mut list = self.new_quicklist();
                for _ in 0..count {
                    let (len, next_pos) = read_varint(data, pos).map_err(|e| e.to_string())?;
                    pos = next_pos;
                    if pos + len as usize > data_len - 8 {
                        return Err("ERR invalid list item length".into());
                    }
                    list.push_back(Bytes::copy_from_slice(&data[pos..pos + len as usize]));
                    pos += len as usize;
                }
                DataType::List(list)
            }
            2 => {
                let (count, new_pos) = read_varint(data, pos).map_err(|e| e.to_string())?;
                pos = new_pos;
                let set = DashSet::new();
                for _ in 0..count {
                    let (len, next_pos) = read_varint(data, pos).map_err(|e| e.to_string())?;
                    pos = next_pos;
                    if pos + len as usize > data_len - 8 {
                        return Err("ERR invalid set item length".into());
                    }
                    set.insert(Bytes::copy_from_slice(&data[pos..pos + len as usize]));
                    pos += len as usize;
                }
                DataType::Set(set)
            }
            3 => {
                let (count, new_pos) = read_varint(data, pos).map_err(|e| e.to_string())?;
                pos = new_pos;
                let hash = DashTable::new();
                for _ in 0..count {
                    let (k_len, next_k_pos) = read_varint(data, pos).map_err(|e| e.to_string())?;
                    pos = next_k_pos;
                    if pos + k_len as usize > data_len - 8 {
                        return Err("ERR invalid hash key length".into());
                    }
                    let k = Bytes::copy_from_slice(&data[pos..pos + k_len as usize]);
                    pos += k_len as usize;

                    let (v_len, next_v_pos) = read_varint(data, pos).map_err(|e| e.to_string())?;
                    pos = next_v_pos;
                    if pos + v_len as usize > data_len - 8 {
                        return Err("ERR invalid hash value length".into());
                    }
                    let v = Bytes::copy_from_slice(&data[pos..pos + v_len as usize]);
                    pos += v_len as usize;

                    let h = calculate_hash(&k);
                    hash.insert_unique(h, (k, v), |kv| calculate_hash(&kv.0));
                }
                DataType::Hash(hash)
            }
            #[cfg(feature = "bloom")]
            10 => {
                // Bloom Filter
                let (len, new_pos) = read_varint(data, pos).map_err(|e| e.to_string())?;
                pos = new_pos;
                if pos + len as usize > data_len - 8 {
                    return Err("ERR invalid bloom filter length".into());
                }
                let bf_data = &data[pos..pos + len as usize];
                let bf = crate::storage::bloomfilter::ScalableBloomFilter::from_bytes(bf_data)
                    .ok_or("ERR invalid bloom filter data")?;
                DataType::BloomFilter(Box::new(bf))
            }
            #[cfg(feature = "bloom")]
            11 => {
                // Cuckoo Filter
                let (len, new_pos) = read_varint(data, pos).map_err(|e| e.to_string())?;
                pos = new_pos;
                if pos + len as usize > data_len - 8 {
                    return Err("ERR invalid cuckoo filter length".into());
                }
                let cf_data = &data[pos..pos + len as usize];
                let cf = crate::storage::cuckoofilter::ScalableCuckooFilter::from_bytes(cf_data)
                    .ok_or("ERR invalid cuckoo filter data")?;
                DataType::CuckooFilter(Box::new(cf))
            }
            _ => return Err("ERR unsupported data type for restore".into()),
        };

        let final_expire = if absttl {
            if ttl_ms > 0 { Some(ttl_ms) } else { None }
        } else if ttl_ms > 0 {
            Some(crate::storage::value::now_ms() + ttl_ms)
        } else if expire_at_meta > 0 {
            Some(expire_at_meta)
        } else {
            None
        };

        let entry = Entry::new(data_type);
        if let Some(exp) = final_expire {
            entry.set_expire_at(exp);
        }

        if replace {
            self.data_remove(key);
        }

        let key_bytes = Bytes::copy_from_slice(key);
        self.data_insert(key_bytes.clone(), entry);
        self.key_count.fetch_add(1, Ordering::Relaxed);

        // Add to expiration index if key has expiration
        if let Some(exp) = final_expire {
            self.expiration_index.add(key_bytes, exp);
        }

        Ok(())
    }

    pub fn flush(&self) {
        self.data.clear();
        self.search_indexes.clear();
        self.search_aliases.clear();
        self.expiration_index.clear();
        self.key_count.store(0, Ordering::Relaxed);
    }

    /// Lazy (async) delete - moves the value to a background task for deallocation
    /// Returns true if key was deleted, false if it didn't exist
    /// The actual memory deallocation happens in the background
    #[inline]
    pub fn lazy_del(&self, key: &[u8]) -> bool {
        if let Some((key_bytes, entry)) = self.data_remove(key) {
            self.key_count.fetch_sub(1, Ordering::Relaxed);

            // Remove from expiration index if it has expiration
            if let Some(expire_ms) = entry.expire_at_ms() {
                self.expiration_index.remove(&key_bytes, expire_ms);
            }

            // Spawn background task to drop the value
            // This moves the owned value to a task that will deallocate it
            std::thread::spawn(move || {
                drop((key_bytes, entry));
            });
            true
        } else {
            false
        }
    }

    /// Lazy (async) flush - clears data structures
    /// Note: For true async deallocation, Entry would need to implement Clone.
    /// Currently this behaves the same as flush() but is provided for API consistency.
    /// The actual memory freeing happens via Drop when entries are removed.
    pub fn lazy_flush(&self) {
        self.data.clear();
        self.search_indexes.clear();
        self.search_aliases.clear();
        self.expiration_index.clear();
        self.key_count.store(0, Ordering::Relaxed);
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
