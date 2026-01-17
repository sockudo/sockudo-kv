//! RDB serialization for replication
//!
//! Full RDB format implementation for generating snapshots and loading data.

use bytes::Bytes;

use crate::storage::dashtable::{DashTable, calculate_hash};
use crate::storage::{DataType, Entry, SortedSetData, now_ms};

/// RDB opcodes
pub const RDB_OPCODE_AUX: u8 = 0xFA;
pub const RDB_OPCODE_SELECTDB: u8 = 0xFE;
pub const RDB_OPCODE_RESIZEDB: u8 = 0xFB;
pub const RDB_OPCODE_EXPIRETIME_MS: u8 = 0xFC;
pub const RDB_OPCODE_EOF: u8 = 0xFF;

/// Value type encodings
pub const RDB_TYPE_STRING: u8 = 0;
pub const RDB_TYPE_LIST: u8 = 1;
pub const RDB_TYPE_SET: u8 = 2;
pub const RDB_TYPE_ZSET: u8 = 5; // ZSET with scores as strings
pub const RDB_TYPE_HASH: u8 = 4;
pub const RDB_TYPE_LIST_QUICKLIST: u8 = 14;
pub const RDB_TYPE_HASH_LISTPACK: u8 = 16;
pub const RDB_TYPE_SET_LISTPACK: u8 = 20;
pub const RDB_TYPE_ZSET_LISTPACK: u8 = 17;

/// Custom type encodings for extended data types (using module-style encoding)
/// These use high values to avoid collision with Redis RDB types
pub const RDB_TYPE_STREAM: u8 = 19; // Redis 5.0 stream type
pub const RDB_TYPE_MODULE_AUX: u8 = 247; // Module auxiliary data
pub const RDB_TYPE_MODULE_2: u8 = 7; // Module type with opcode

/// Module type IDs (64-bit encoded as string for compatibility)
pub const MODULE_TYPE_TIMESERIES: &[u8] = b"TSDB-TYPE";
pub const MODULE_TYPE_VECTORSET: &[u8] = b"vectorset";
pub const MODULE_TYPE_BLOOMFILTER: &[u8] = b"MBbloom--";
pub const MODULE_TYPE_CUCKOOFILTER: &[u8] = b"MBcuckoo-";
pub const MODULE_TYPE_TDIGEST: &[u8] = b"TDIS-TYPE";
pub const MODULE_TYPE_TOPK: &[u8] = b"MBtopk---";
pub const MODULE_TYPE_CMS: &[u8] = b"MBcms----";

/// LZF compression encoding
pub const RDB_ENC_LZF: u8 = 3;

/// RDB configuration options
#[derive(Debug, Clone, Copy)]
pub struct RdbConfig {
    /// Enable LZF compression for strings (default: true)
    pub compression: bool,
    /// Append CRC64 checksum to RDB file (default: true)
    pub checksum: bool,
}

impl Default for RdbConfig {
    fn default() -> Self {
        Self {
            compression: true,
            checksum: true,
        }
    }
}

/// Generate RDB snapshot of store with default settings
pub fn generate_rdb(multi_store: &crate::storage::MultiStore) -> Bytes {
    generate_rdb_with_config(multi_store, RdbConfig::default())
}

/// Generate RDB snapshot of store with custom configuration
pub fn generate_rdb_with_config(
    multi_store: &crate::storage::MultiStore,
    config: RdbConfig,
) -> Bytes {
    let mut rdb = Vec::with_capacity(1024 * 1024); // 1MB initial capacity

    // Magic number and version
    rdb.extend_from_slice(b"REDIS0011");

    // AUX fields
    write_aux(&mut rdb, "redis-ver", "7.0.0");
    write_aux_int(&mut rdb, "redis-bits", 64);
    write_aux_int(
        &mut rdb,
        "ctime",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64,
    );
    write_aux_int(&mut rdb, "used-mem", 0);
    write_aux(&mut rdb, "aof-base", "0");

    for db_index in 0..multi_store.db_count() {
        let store = multi_store.db(db_index);

        // Skip empty databases
        if store.data.is_empty() {
            continue;
        }

        // Select DB
        rdb.push(RDB_OPCODE_SELECTDB);
        write_length(&mut rdb, db_index);

        // Count keys and expires
        let mut key_count = 0usize;
        let mut expire_count = 0usize;
        for entry in store.data.iter() {
            if !entry.1.is_expired() {
                key_count += 1;
                if entry.1.ttl_ms().is_some() {
                    expire_count += 1;
                }
            }
        }

        // Resize DB
        rdb.push(RDB_OPCODE_RESIZEDB);
        write_length(&mut rdb, key_count);
        write_length(&mut rdb, expire_count);

        // Write all keys
        for entry in store.data.iter() {
            let key = &entry.0;
            let value = &entry.1;

            // Skip expired
            if value.is_expired() {
                continue;
            }

            // Write expiration if present
            if let Some(ttl) = value.ttl_ms()
                && ttl > 0
            {
                let expire_at = now_ms() + ttl;
                rdb.push(RDB_OPCODE_EXPIRETIME_MS);
                rdb.extend_from_slice(&expire_at.to_le_bytes());
            }

            // Write value based on type
            write_value(&mut rdb, Some(key), &value.data, config.compression);
        }
    }

    // EOF
    rdb.push(RDB_OPCODE_EOF);

    // CRC64 checksum (write 0 if disabled)
    let checksum = if config.checksum { crc64(&rdb) } else { 0 };
    rdb.extend_from_slice(&checksum.to_le_bytes());

    Bytes::from(rdb)
}

/// Write a key-value pair to RDB
pub(crate) fn write_value(rdb: &mut Vec<u8>, key: Option<&Bytes>, data: &DataType, compress: bool) {
    match data {
        DataType::String(s) | DataType::RawString(s) => {
            rdb.push(RDB_TYPE_STRING);
            if let Some(k) = key {
                write_string(rdb, k);
            }
            write_string_compressed(rdb, s, compress);
        }
        DataType::List(list) => {
            rdb.push(RDB_TYPE_LIST);
            if let Some(k) = key {
                write_string(rdb, k);
            }
            write_length(rdb, list.len());
            for item in list.iter() {
                write_string(rdb, &item);
            }
        }
        DataType::Set(set) => {
            rdb.push(RDB_TYPE_SET);
            if let Some(k) = key {
                write_string(rdb, k);
            }
            write_length(rdb, set.len());
            for item in set.iter() {
                write_string(rdb, item.key());
            }
        }
        DataType::IntSet(set) => {
            // Serialize as regular SET of strings for compatibility
            rdb.push(RDB_TYPE_SET);
            if let Some(k) = key {
                write_string(rdb, k);
            }
            write_length(rdb, set.len());
            for item in set.iter() {
                let s = item.to_string();
                write_string(rdb, &Bytes::from(s));
            }
        }
        DataType::SetPacked(lp) => {
            // Serialize as regular SET for compatibility
            rdb.push(RDB_TYPE_SET);
            if let Some(k) = key {
                write_string(rdb, k);
            }
            write_length(rdb, lp.len());
            for (member, _) in lp.iter() {
                write_string(rdb, &member);
            }
        }
        DataType::Hash(hash) => {
            rdb.push(RDB_TYPE_HASH);
            if let Some(k) = key {
                write_string(rdb, k);
            }
            write_length(rdb, hash.len());
            for item in hash.iter() {
                write_string(rdb, &item.0);
                write_string(rdb, &item.1);
            }
        }
        DataType::HashPacked(lp) => {
            rdb.push(RDB_TYPE_HASH);
            if let Some(k) = key {
                write_string(rdb, k);
            }
            write_length(rdb, lp.len());
            for (k, v) in lp.iter() {
                write_string(rdb, &k);
                write_string(rdb, &v);
            }
        }
        DataType::SortedSet(zset) => {
            rdb.push(RDB_TYPE_ZSET);
            if let Some(k) = key {
                write_string(rdb, k);
            }
            write_length(rdb, zset.len());
            for (member, score) in &zset.scores {
                write_string(rdb, member);
                // Write score as double
                write_double(rdb, *score);
            }
        }
        DataType::SortedSetPacked(lp) => {
            rdb.push(RDB_TYPE_ZSET);
            if let Some(k) = key {
                write_string(rdb, k);
            }
            write_length(rdb, lp.len());
            for (member, score) in lp.ziter() {
                write_string(rdb, &member);
                // Write score as double
                write_double(rdb, score);
            }
        }
        DataType::HyperLogLog(hll) => {
            // Store as string (Redis stores HLL as special string)
            rdb.push(RDB_TYPE_STRING);
            if let Some(k) = key {
                write_string(rdb, k);
            }
            let regs = hll.get_registers();
            write_string(rdb, &Bytes::copy_from_slice(&regs));
        }
        DataType::Json(json) => {
            // Serialize JSON as string
            rdb.push(RDB_TYPE_STRING);
            if let Some(k) = key {
                write_string(rdb, k);
            }
            let json_str = sonic_rs::to_string(json.as_ref()).unwrap_or_default();
            write_string(rdb, &Bytes::from(json_str));
        }
        // Stream - serialize with full structure
        DataType::Stream(stream) => {
            rdb.push(RDB_TYPE_STREAM);
            if let Some(k) = key {
                write_string(rdb, k);
            }
            write_stream(rdb, stream);
        }
        // TimeSeries - serialize as module type with binary data
        DataType::TimeSeries(ts) => {
            rdb.push(RDB_TYPE_MODULE_2);
            if let Some(k) = key {
                write_string(rdb, k);
            }
            write_module_type(rdb, MODULE_TYPE_TIMESERIES);
            write_timeseries(rdb, ts);
        }
        // VectorSet - serialize as module type with binary data
        DataType::VectorSet(vs) => {
            rdb.push(RDB_TYPE_MODULE_2);
            if let Some(k) = key {
                write_string(rdb, k);
            }
            write_module_type(rdb, MODULE_TYPE_VECTORSET);
            write_vectorset(rdb, vs);
        }
        // RedisBloom-like probabilistic data structures (feature-gated)
        #[cfg(feature = "bloom")]
        DataType::BloomFilter(bf) => {
            rdb.push(RDB_TYPE_MODULE_2);
            if let Some(k) = key {
                write_string(rdb, k);
            }
            write_module_type(rdb, MODULE_TYPE_BLOOMFILTER);
            let bf_bytes = bf.to_bytes();
            write_string(rdb, &Bytes::from(bf_bytes));
        }
        #[cfg(feature = "bloom")]
        DataType::CuckooFilter(cf) => {
            rdb.push(RDB_TYPE_MODULE_2);
            if let Some(k) = key {
                write_string(rdb, k);
            }
            write_module_type(rdb, MODULE_TYPE_CUCKOOFILTER);
            let cf_bytes = cf.to_bytes();
            write_string(rdb, &Bytes::from(cf_bytes));
        }
        #[cfg(feature = "bloom")]
        DataType::TDigest(td) => {
            rdb.push(RDB_TYPE_MODULE_2);
            if let Some(k) = key {
                write_string(rdb, k);
            }
            write_module_type(rdb, MODULE_TYPE_TDIGEST);
            let td_bytes = td.to_bytes();
            write_string(rdb, &Bytes::from(td_bytes));
        }
        #[cfg(feature = "bloom")]
        DataType::TopK(tk) => {
            rdb.push(RDB_TYPE_MODULE_2);
            if let Some(k) = key {
                write_string(rdb, k);
            }
            write_module_type(rdb, MODULE_TYPE_TOPK);
            let tk_bytes = tk.to_bytes();
            write_string(rdb, &Bytes::from(tk_bytes));
        }
        #[cfg(feature = "bloom")]
        DataType::CountMinSketch(cms) => {
            rdb.push(RDB_TYPE_MODULE_2);
            if let Some(k) = key {
                write_string(rdb, k);
            }
            write_module_type(rdb, MODULE_TYPE_CMS);
            let cms_bytes = cms.to_bytes();
            write_string(rdb, &Bytes::from(cms_bytes));
        }
    }
}

/// Write double in RDB format
fn write_double(rdb: &mut Vec<u8>, value: f64) {
    if value.is_nan() {
        rdb.push(253);
    } else if value.is_infinite() {
        if value > 0.0 {
            rdb.push(254);
        } else {
            rdb.push(255);
        }
    } else {
        rdb.push(0);
        rdb.extend_from_slice(&value.to_le_bytes());
    }
}

/// Write AUX field
fn write_aux(rdb: &mut Vec<u8>, key: &str, value: &str) {
    rdb.push(RDB_OPCODE_AUX);
    write_string(rdb, &Bytes::copy_from_slice(key.as_bytes()));
    write_string(rdb, &Bytes::copy_from_slice(value.as_bytes()));
}

/// Write AUX field with integer value
fn write_aux_int(rdb: &mut Vec<u8>, key: &str, value: i64) {
    rdb.push(RDB_OPCODE_AUX);
    write_string(rdb, &Bytes::copy_from_slice(key.as_bytes()));
    write_int(rdb, value);
}

/// Write length-prefixed string (no compression, used for keys and aux)
fn write_string(rdb: &mut Vec<u8>, s: &Bytes) {
    write_length(rdb, s.len());
    rdb.extend_from_slice(s);
}

/// Write string with optional LZF compression (for values)
/// Compresses if enabled and string is > 20 bytes and compression saves space
fn write_string_compressed(rdb: &mut Vec<u8>, s: &Bytes, compress: bool) {
    // Only attempt compression for strings over 20 bytes
    if compress && s.len() > 20 {
        if let Some(compressed) = crate::lzf::compress(s) {
            // Write LZF encoding: 0xC3 (RDB_ENC_LZF), compressed_len, orig_len, data
            rdb.push(RDB_ENC_LZF | 0xC0); // 0xC3
            write_length(rdb, compressed.len());
            write_length(rdb, s.len());
            rdb.extend_from_slice(&compressed);
            return;
        }
    }
    // Fallback to uncompressed
    write_string(rdb, s);
}

/// Write integer in RDB encoding
fn write_int(rdb: &mut Vec<u8>, value: i64) {
    if (-128..=127).contains(&value) {
        // RDB_ENC_INT8 - covers both positive 0-127 and negative -128..-1
        rdb.push(0xC0);
        rdb.push(value as u8);
    } else if (-32768..=32767).contains(&value) {
        rdb.push(0xC1);
        rdb.extend_from_slice(&(value as i16).to_le_bytes());
    } else if (-2147483648..=2147483647).contains(&value) {
        rdb.push(0xC2);
        rdb.extend_from_slice(&(value as i32).to_le_bytes());
    } else {
        let s = value.to_string();
        write_string(rdb, &Bytes::copy_from_slice(s.as_bytes()));
    }
}

/// Write length encoding
fn write_length(rdb: &mut Vec<u8>, len: usize) {
    if len < 64 {
        rdb.push(len as u8);
    } else if len < 16384 {
        rdb.push(0x40 | ((len >> 8) as u8));
        rdb.push(len as u8);
    } else {
        rdb.push(0x80);
        rdb.extend_from_slice(&(len as u32).to_be_bytes());
    }
}

/// Write module type identifier (9 bytes)
fn write_module_type(rdb: &mut Vec<u8>, module_type: &[u8]) {
    // Module type is a 9-byte identifier
    write_string(rdb, &Bytes::copy_from_slice(module_type));
}

/// Write Stream data structure
fn write_stream(rdb: &mut Vec<u8>, stream: &crate::storage::StreamData) {
    // Write number of entries
    write_length(rdb, stream.entries.len());

    // Write each entry
    for (id, fields) in &stream.entries {
        // Write stream ID (ms + seq)
        rdb.extend_from_slice(&id.ms.to_le_bytes());
        rdb.extend_from_slice(&id.seq.to_le_bytes());

        // Write field count
        write_length(rdb, fields.len());

        // Write each field-value pair
        for (field, value) in fields {
            write_string(rdb, field);
            write_string(rdb, value);
        }
    }

    // Write last_id
    rdb.extend_from_slice(&stream.last_id.ms.to_le_bytes());
    rdb.extend_from_slice(&stream.last_id.seq.to_le_bytes());

    // Write entries_added
    rdb.extend_from_slice(&stream.entries_added.to_le_bytes());

    // Write max_deleted_id
    rdb.extend_from_slice(&stream.max_deleted_id.ms.to_le_bytes());
    rdb.extend_from_slice(&stream.max_deleted_id.seq.to_le_bytes());

    // Write first_id (optional)
    match stream.first_id {
        Some(id) => {
            rdb.push(1);
            rdb.extend_from_slice(&id.ms.to_le_bytes());
            rdb.extend_from_slice(&id.seq.to_le_bytes());
        }
        None => {
            rdb.push(0);
        }
    }

    // Write consumer groups
    write_length(rdb, stream.groups.len());
    for (group_name, group) in &stream.groups {
        write_string(rdb, group_name);

        // Write last_delivered_id
        rdb.extend_from_slice(&group.last_delivered_id.ms.to_le_bytes());
        rdb.extend_from_slice(&group.last_delivered_id.seq.to_le_bytes());

        // Write entries_read
        match group.entries_read {
            Some(count) => {
                rdb.push(1);
                rdb.extend_from_slice(&count.to_le_bytes());
            }
            None => {
                rdb.push(0);
            }
        }

        // Write pending entries
        write_length(rdb, group.pending.len());
        for (id, pending) in &group.pending {
            rdb.extend_from_slice(&id.ms.to_le_bytes());
            rdb.extend_from_slice(&id.seq.to_le_bytes());
            write_string(rdb, &pending.consumer);
            rdb.extend_from_slice(&pending.delivery_time.to_le_bytes());
            rdb.extend_from_slice(&pending.delivery_count.to_le_bytes());
        }

        // Write consumers
        write_length(rdb, group.consumers.len());
        for (consumer_name, consumer) in &group.consumers {
            write_string(rdb, consumer_name);
            rdb.extend_from_slice(&consumer.last_seen.to_le_bytes());

            // Write consumer pending IDs
            write_length(rdb, consumer.pending.len());
            for id in &consumer.pending {
                rdb.extend_from_slice(&id.ms.to_le_bytes());
                rdb.extend_from_slice(&id.seq.to_le_bytes());
            }
        }
    }
}

/// Write TimeSeries data structure
fn write_timeseries(rdb: &mut Vec<u8>, ts: &crate::storage::timeseries::TimeSeries) {
    // Version marker
    rdb.push(1);

    // Write configuration
    rdb.extend_from_slice(&ts.retention_ms.to_le_bytes());
    rdb.extend_from_slice(&ts.chunk_duration_ms.to_le_bytes());
    rdb.push(ts.duplicate_policy as u8);

    // Write first/last timestamps and last value
    rdb.extend_from_slice(&ts.first_timestamp.to_le_bytes());
    rdb.extend_from_slice(&ts.last_timestamp.to_le_bytes());
    rdb.extend_from_slice(&ts.last_value.to_le_bytes());

    // Write labels
    write_length(rdb, ts.labels.len());
    for (key, value) in &ts.labels {
        write_string(rdb, &Bytes::copy_from_slice(key.as_bytes()));
        write_string(rdb, &Bytes::copy_from_slice(value.as_bytes()));
    }

    // Write compaction rules
    write_length(rdb, ts.rules.len());
    for rule in &ts.rules {
        write_string(rdb, &rule.dest_key);
        rdb.push(rule.aggregation as u8);
        rdb.extend_from_slice(&rule.bucket_duration.to_le_bytes());
        rdb.extend_from_slice(&rule.align_timestamp.to_le_bytes());
    }

    // Get all samples by iterating through a full range
    let samples = ts.range(i64::MIN, i64::MAX);
    write_length(rdb, samples.len());
    for (timestamp, value) in samples {
        rdb.extend_from_slice(&timestamp.to_le_bytes());
        rdb.extend_from_slice(&value.to_le_bytes());
    }
}

/// Write VectorSet data structure
fn write_vectorset(rdb: &mut Vec<u8>, vs: &crate::storage::VectorSetData) {
    // Version marker
    rdb.push(1);

    // Write configuration
    write_length(rdb, vs.dim);
    match vs.reduced_dim {
        Some(d) => {
            rdb.push(1);
            write_length(rdb, d);
        }
        None => {
            rdb.push(0);
        }
    }
    rdb.push(vs.quant as u8);
    write_length(rdb, vs.m);
    write_length(rdb, vs.m0);
    write_length(rdb, vs.ef_construction);
    rdb.push(vs.max_level);
    rdb.extend_from_slice(&vs.level_mult.to_le_bytes());

    // Write entry point
    match vs.entry_point {
        Some(ep) => {
            rdb.push(1);
            rdb.extend_from_slice(&ep.to_le_bytes());
        }
        None => {
            rdb.push(0);
        }
    }

    // Write nodes
    write_length(rdb, vs.nodes.len());
    for node in &vs.nodes {
        // Write element name
        write_string(rdb, &node.element);

        // Write vector
        write_length(rdb, node.vector.len());
        for &v in &node.vector {
            rdb.extend_from_slice(&v.to_le_bytes());
        }

        // Write level
        rdb.push(node.level);

        // Write connections per layer
        write_length(rdb, node.connections.len());
        for layer_conns in &node.connections {
            write_length(rdb, layer_conns.len());
            for &conn in layer_conns {
                rdb.extend_from_slice(&conn.to_le_bytes());
            }
        }

        // Write attributes (as JSON string if present)
        match &node.attributes {
            Some(attrs) => {
                rdb.push(1);
                let json_str = sonic_rs::to_string(attrs.as_ref()).unwrap_or_default();
                write_string(rdb, &Bytes::from(json_str));
            }
            None => {
                rdb.push(0);
            }
        }
    }

    // Write element_index for quick reconstruction
    write_length(rdb, vs.element_index.len());
    for (name, &idx) in &vs.element_index {
        write_string(rdb, name);
        rdb.extend_from_slice(&idx.to_le_bytes());
    }
}

// ==================== RDB Loading ====================

/// Load RDB data into store
pub fn load_rdb(data: &[u8], multi_store: &crate::storage::MultiStore) -> Result<(), String> {
    // Verify magic
    if data.len() < 9 || &data[0..5] != b"REDIS" {
        return Err("Invalid RDB magic".to_string());
    }
    let mut cursor = 9; // Skip REDIS + version

    let mut current_expire: Option<i64> = None;
    let mut current_db_index = 0;

    // Get initial store
    let mut store = multi_store.db(current_db_index);

    while cursor < data.len() {
        let opcode = data[cursor];
        cursor += 1;

        match opcode {
            RDB_OPCODE_EOF => {
                // Verify checksum if present
                if cursor + 8 <= data.len() {
                    let checksum = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                    if checksum != 0 {
                        // Calculate checksum of data loaded so far (including EOF marker)
                        let calculated = crc64(&data[0..cursor]);
                        if calculated != checksum {
                            return Err(format!(
                                "RDB checksum mismatch: expected {}, got {}",
                                checksum, calculated
                            ));
                        }
                    }
                }
                break;
            }
            RDB_OPCODE_AUX => {
                // Skip aux field
                let (_, len1) = read_string(&data[cursor..])?;
                cursor += len1;
                let (_, len2) = read_string(&data[cursor..])?;
                cursor += len2;
            }
            RDB_OPCODE_SELECTDB => {
                let (db_index, len) = read_length(&data[cursor..])?;
                cursor += len;

                // Switch database
                if db_index < multi_store.db_count() {
                    current_db_index = db_index;
                    store = multi_store.db(current_db_index);
                } else {
                    return Err(format!("Database index out of range: {}", db_index));
                }
            }
            RDB_OPCODE_RESIZEDB => {
                let (_, len1) = read_length(&data[cursor..])?;
                cursor += len1;
                let (_, len2) = read_length(&data[cursor..])?;
                cursor += len2;
            }
            RDB_OPCODE_EXPIRETIME_MS => {
                if cursor + 8 > data.len() {
                    return Err("Truncated expire time".to_string());
                }
                let expire = i64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                current_expire = Some(expire);
                cursor += 8;
            }
            RDB_TYPE_STRING => {
                let (key, klen) = read_string(&data[cursor..])?;
                cursor += klen;
                let (value, vlen) = read_string(&data[cursor..])?;
                cursor += vlen;

                store.set(key.clone(), value);
                if let Some(expire) = current_expire.take() {
                    let ttl = expire - now_ms();
                    if ttl > 0 {
                        store.expire(&key, ttl);
                    }
                }
            }
            RDB_TYPE_LIST => {
                let (key, klen) = read_string(&data[cursor..])?;
                cursor += klen;
                let (list_len, llen) = read_length(&data[cursor..])?;
                cursor += llen;

                let mut list = crate::storage::quicklist::QuickList::new();
                for _ in 0..list_len {
                    let (item, ilen) = read_string(&data[cursor..])?;
                    cursor += ilen;
                    list.push_back(item);
                }

                store.data_insert(key.clone(), Entry::new(DataType::List(list)));
                store
                    .key_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            RDB_TYPE_SET => {
                let (key, klen) = read_string(&data[cursor..])?;
                cursor += klen;
                let (set_len, slen) = read_length(&data[cursor..])?;
                cursor += slen;

                let set = dashmap::DashSet::new();
                for _ in 0..set_len {
                    let (member, mlen) = read_string(&data[cursor..])?;
                    cursor += mlen;
                    set.insert(member);
                }

                store.data_insert(key.clone(), Entry::new(DataType::Set(set)));
                store
                    .key_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            RDB_TYPE_HASH => {
                let (key, klen) = read_string(&data[cursor..])?;
                cursor += klen;
                let (hash_len, hlen) = read_length(&data[cursor..])?;
                cursor += hlen;

                let hash = DashTable::new();
                for _ in 0..hash_len {
                    let (field, flen) = read_string(&data[cursor..])?;
                    cursor += flen;
                    let (value, vlen) = read_string(&data[cursor..])?;
                    cursor += vlen;
                    let h = calculate_hash(&field);
                    hash.insert_unique(h, (field, value), |kv| calculate_hash(&kv.0));
                }

                store.data_insert(key.clone(), Entry::new(DataType::Hash(hash)));
                store
                    .key_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            RDB_TYPE_ZSET => {
                let (key, klen) = read_string(&data[cursor..])?;
                cursor += klen;
                let (zset_len, zlen) = read_length(&data[cursor..])?;
                cursor += zlen;

                let mut zset = SortedSetData::new();
                for _ in 0..zset_len {
                    let (member, mlen) = read_string(&data[cursor..])?;
                    cursor += mlen;
                    let (score, slen) = read_double(&data[cursor..])?;
                    cursor += slen;
                    zset.insert(member, score);
                }

                store.data_insert(key.clone(), Entry::new(DataType::SortedSet(zset)));
                store
                    .key_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            RDB_TYPE_STREAM => {
                let (key, klen) = read_string(&data[cursor..])?;
                cursor += klen;
                let (stream, slen) = read_stream(&data[cursor..])?;
                cursor += slen;

                store.data_insert(key.clone(), Entry::new(DataType::Stream(stream)));
                store
                    .key_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                if let Some(expire) = current_expire.take() {
                    let ttl = expire - now_ms();
                    if ttl > 0 {
                        store.expire(&key, ttl);
                    }
                }
            }
            RDB_TYPE_MODULE_2 => {
                let (key, klen) = read_string(&data[cursor..])?;
                cursor += klen;
                let (module_type, mlen) = read_string(&data[cursor..])?;
                cursor += mlen;

                // Dispatch based on module type
                match module_type.as_ref() {
                    MODULE_TYPE_TIMESERIES => {
                        let (ts, tlen) = read_timeseries(&data[cursor..])?;
                        cursor += tlen;
                        store.data_insert(
                            key.clone(),
                            Entry::new(DataType::TimeSeries(Box::new(ts))),
                        );
                        store
                            .key_count
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    MODULE_TYPE_VECTORSET => {
                        let (vs, vlen) = read_vectorset(&data[cursor..])?;
                        cursor += vlen;
                        store.data_insert(
                            key.clone(),
                            Entry::new(DataType::VectorSet(Box::new(vs))),
                        );
                        store
                            .key_count
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    #[cfg(feature = "bloom")]
                    MODULE_TYPE_BLOOMFILTER => {
                        let (bf_data, blen) = read_string(&data[cursor..])?;
                        cursor += blen;
                        if let Some(bf) =
                            crate::storage::bloomfilter::ScalableBloomFilter::from_bytes(&bf_data)
                        {
                            store.data_insert(
                                key.clone(),
                                Entry::new(DataType::BloomFilter(Box::new(bf))),
                            );
                            store
                                .key_count
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        } else {
                            return Err("Failed to deserialize BloomFilter".to_string());
                        }
                    }
                    #[cfg(feature = "bloom")]
                    MODULE_TYPE_CUCKOOFILTER => {
                        let (cf_data, clen) = read_string(&data[cursor..])?;
                        cursor += clen;
                        if let Some(cf) =
                            crate::storage::cuckoofilter::ScalableCuckooFilter::from_bytes(&cf_data)
                        {
                            store.data_insert(
                                key.clone(),
                                Entry::new(DataType::CuckooFilter(Box::new(cf))),
                            );
                            store
                                .key_count
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        } else {
                            return Err("Failed to deserialize CuckooFilter".to_string());
                        }
                    }
                    #[cfg(feature = "bloom")]
                    MODULE_TYPE_TDIGEST => {
                        let (td_data, tlen) = read_string(&data[cursor..])?;
                        cursor += tlen;
                        if let Some(td) = crate::storage::tdigest::TDigest::from_bytes(&td_data) {
                            store.data_insert(
                                key.clone(),
                                Entry::new(DataType::TDigest(Box::new(td))),
                            );
                            store
                                .key_count
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        } else {
                            return Err("Failed to deserialize TDigest".to_string());
                        }
                    }
                    #[cfg(feature = "bloom")]
                    MODULE_TYPE_TOPK => {
                        let (tk_data, tklen) = read_string(&data[cursor..])?;
                        cursor += tklen;
                        if let Some(tk) = crate::storage::topk::TopK::from_bytes(&tk_data) {
                            store
                                .data_insert(key.clone(), Entry::new(DataType::TopK(Box::new(tk))));
                            store
                                .key_count
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        } else {
                            return Err("Failed to deserialize TopK".to_string());
                        }
                    }
                    #[cfg(feature = "bloom")]
                    MODULE_TYPE_CMS => {
                        let (cms_data, cmslen) = read_string(&data[cursor..])?;
                        cursor += cmslen;
                        if let Some(cms) =
                            crate::storage::cms::CountMinSketch::from_bytes(&cms_data)
                        {
                            store.data_insert(
                                key.clone(),
                                Entry::new(DataType::CountMinSketch(Box::new(cms))),
                            );
                            store
                                .key_count
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        } else {
                            return Err("Failed to deserialize CountMinSketch".to_string());
                        }
                    }
                    _ => {
                        return Err(format!("Unknown module type: {:?}", module_type));
                    }
                }

                if let Some(expire) = current_expire.take() {
                    let ttl = expire - now_ms();
                    if ttl > 0 {
                        store.expire(&key, ttl);
                    }
                }
            }
            _ => {
                // Skip unknown type
                return Err(format!("Unknown RDB type: {}", opcode));
            }
        }
    }

    Ok(())
}

/// Read length-encoded value
fn read_length(data: &[u8]) -> Result<(usize, usize), String> {
    if data.is_empty() {
        return Err("Truncated length".to_string());
    }

    let first = data[0];
    let encoding = (first & 0xC0) >> 6;

    match encoding {
        0 => Ok((first as usize, 1)),
        1 => {
            if data.len() < 2 {
                return Err("Truncated length".to_string());
            }
            let len = (((first & 0x3F) as usize) << 8) | (data[1] as usize);
            Ok((len, 2))
        }
        2 => {
            if data.len() < 5 {
                return Err("Truncated length".to_string());
            }
            let len = u32::from_be_bytes(data[1..5].try_into().unwrap()) as usize;
            Ok((len, 5))
        }
        3 => {
            // Special encoding (integer)
            let special = first & 0x3F;
            match special {
                0 => Ok((data.get(1).copied().unwrap_or(0) as usize, 2)),
                1 => {
                    if data.len() < 3 {
                        return Err("Truncated int16".to_string());
                    }
                    let val = i16::from_le_bytes(data[1..3].try_into().unwrap());
                    Ok((val as usize, 3))
                }
                2 => {
                    if data.len() < 5 {
                        return Err("Truncated int32".to_string());
                    }
                    let val = i32::from_le_bytes(data[1..5].try_into().unwrap());
                    Ok((val as usize, 5))
                }
                _ => Err(format!("Unknown special encoding: {}", special)),
            }
        }
        _ => unreachable!(),
    }
}

/// Read string from RDB
fn read_string(data: &[u8]) -> Result<(Bytes, usize), String> {
    if data.is_empty() {
        return Err("Truncated string header".to_string());
    }

    // Check for LZF encoding (0xC3)
    if (data[0] & 0xC0) == 0xC0 && (data[0] & 0x3F) == RDB_ENC_LZF {
        let mut cursor = 1;
        // Read compressed length
        let (clen, clen_size) = read_length(&data[cursor..])?;
        cursor += clen_size;
        // Read original length
        let (len, len_size) = read_length(&data[cursor..])?;
        cursor += len_size;

        if data.len() < cursor + clen {
            return Err("Truncated LZF data".to_string());
        }

        let compressed = &data[cursor..cursor + clen];
        let decompressed =
            crate::lzf::decompress(compressed, len).map_err(|e| format!("LZF error: {}", e))?;

        return Ok((Bytes::from(decompressed), cursor + clen));
    }

    let (len, len_size) = read_length(data)?;

    // Check for special encodings
    if data[0] & 0xC0 == 0xC0 {
        let special = data[0] & 0x3F;
        match special {
            0 => {
                let val = data.get(1).copied().unwrap_or(0);
                return Ok((Bytes::from(val.to_string()), 2));
            }
            1 => {
                if data.len() < 3 {
                    return Err("Truncated".to_string());
                }
                let val = i16::from_le_bytes(data[1..3].try_into().unwrap());
                return Ok((Bytes::from(val.to_string()), 3));
            }
            2 => {
                if data.len() < 5 {
                    return Err("Truncated".to_string());
                }
                let val = i32::from_le_bytes(data[1..5].try_into().unwrap());
                return Ok((Bytes::from(val.to_string()), 5));
            }
            // RDB_ENC_LZF is handled above
            _ => {}
        }
    }

    let total_len = len_size + len;
    if data.len() < total_len {
        return Err("Truncated string".to_string());
    }

    Ok((
        Bytes::copy_from_slice(&data[len_size..total_len]),
        total_len,
    ))
}

/// Read double from RDB
fn read_double(data: &[u8]) -> Result<(f64, usize), String> {
    if data.is_empty() {
        return Err("Truncated double".to_string());
    }

    match data[0] {
        253 => Ok((f64::NAN, 1)),
        254 => Ok((f64::INFINITY, 1)),
        255 => Ok((f64::NEG_INFINITY, 1)),
        0 => {
            if data.len() < 9 {
                return Err("Truncated double".to_string());
            }
            let val = f64::from_le_bytes(data[1..9].try_into().unwrap());
            Ok((val, 9))
        }
        _ => {
            // String encoded
            let (s, len) = read_string(data)?;
            let val: f64 = std::str::from_utf8(&s)
                .map_err(|_| "Invalid UTF-8")?
                .parse()
                .map_err(|_| "Invalid float")?;
            Ok((val, len))
        }
    }
}

/// CRC64 checksum (ECMA polynomial)
fn crc64(data: &[u8]) -> u64 {
    const POLY: u64 = 0xC96C5795D7870F42;
    let mut crc: u64 = 0;

    for &byte in data {
        crc ^= byte as u64;
        for _ in 0..8 {
            if crc & 1 == 1 {
                crc = (crc >> 1) ^ POLY;
            } else {
                crc >>= 1;
            }
        }
    }

    crc
}

// ==================== Complex Type Reading ====================

use crate::storage::{
    Consumer, ConsumerGroup, PendingEntry, StreamData, StreamId, VectorNode, VectorQuantization,
    VectorSetData,
};

/// Read Stream data structure
fn read_stream(data: &[u8]) -> Result<(StreamData, usize), String> {
    let mut cursor = 0;

    // Read number of entries
    let (entry_count, elen) = read_length(&data[cursor..])?;
    cursor += elen;

    let mut stream = StreamData::new();

    // Read each entry
    for _ in 0..entry_count {
        if cursor + 16 > data.len() {
            return Err("Truncated stream entry ID".to_string());
        }
        let ms = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
        let seq = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
        let id = StreamId::new(ms, seq);

        // Read field count
        let (field_count, flen) = read_length(&data[cursor..])?;
        cursor += flen;

        let mut fields = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            let (field, fl) = read_string(&data[cursor..])?;
            cursor += fl;
            let (value, vl) = read_string(&data[cursor..])?;
            cursor += vl;
            fields.push((field, value));
        }

        stream.entries.insert(id, fields);
    }

    // Read last_id
    if cursor + 16 > data.len() {
        return Err("Truncated stream last_id".to_string());
    }
    stream.last_id.ms = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
    cursor += 8;
    stream.last_id.seq = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
    cursor += 8;

    // Read entries_added
    if cursor + 8 > data.len() {
        return Err("Truncated stream entries_added".to_string());
    }
    stream.entries_added = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
    cursor += 8;

    // Read max_deleted_id
    if cursor + 16 > data.len() {
        return Err("Truncated stream max_deleted_id".to_string());
    }
    stream.max_deleted_id.ms = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
    cursor += 8;
    stream.max_deleted_id.seq = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
    cursor += 8;

    // Read first_id (optional)
    if cursor >= data.len() {
        return Err("Truncated stream first_id marker".to_string());
    }
    let has_first_id = data[cursor];
    cursor += 1;
    if has_first_id == 1 {
        if cursor + 16 > data.len() {
            return Err("Truncated stream first_id".to_string());
        }
        let ms = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
        let seq = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
        stream.first_id = Some(StreamId::new(ms, seq));
    }

    // Read consumer groups
    let (group_count, glen) = read_length(&data[cursor..])?;
    cursor += glen;

    for _ in 0..group_count {
        let (group_name, gnlen) = read_string(&data[cursor..])?;
        cursor += gnlen;

        // Read last_delivered_id
        if cursor + 16 > data.len() {
            return Err("Truncated group last_delivered_id".to_string());
        }
        let ms = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
        let seq = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;

        let mut group = ConsumerGroup::new(StreamId::new(ms, seq));

        // Read entries_read
        if cursor >= data.len() {
            return Err("Truncated group entries_read marker".to_string());
        }
        let has_entries_read = data[cursor];
        cursor += 1;
        if has_entries_read == 1 {
            if cursor + 8 > data.len() {
                return Err("Truncated group entries_read".to_string());
            }
            group.entries_read = Some(u64::from_le_bytes(
                data[cursor..cursor + 8].try_into().unwrap(),
            ));
            cursor += 8;
        } else {
            group.entries_read = None;
        }

        // Read pending entries
        let (pending_count, plen) = read_length(&data[cursor..])?;
        cursor += plen;

        for _ in 0..pending_count {
            if cursor + 16 > data.len() {
                return Err("Truncated pending entry ID".to_string());
            }
            let ms = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
            cursor += 8;
            let seq = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
            cursor += 8;
            let id = StreamId::new(ms, seq);

            let (consumer_name, cnlen) = read_string(&data[cursor..])?;
            cursor += cnlen;

            if cursor + 12 > data.len() {
                return Err("Truncated pending entry times".to_string());
            }
            let delivery_time = i64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
            cursor += 8;
            let delivery_count = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());
            cursor += 4;

            group.pending.insert(
                id,
                PendingEntry {
                    consumer: consumer_name,
                    delivery_time,
                    delivery_count,
                },
            );
        }

        // Read consumers
        let (consumer_count, clen) = read_length(&data[cursor..])?;
        cursor += clen;

        for _ in 0..consumer_count {
            let (consumer_name, cnlen) = read_string(&data[cursor..])?;
            cursor += cnlen;

            if cursor + 8 > data.len() {
                return Err("Truncated consumer last_seen".to_string());
            }
            let last_seen = i64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
            cursor += 8;

            let mut consumer = Consumer {
                pending: std::collections::HashSet::new(),
                last_seen,
            };

            // Read consumer pending IDs
            let (pending_id_count, pidlen) = read_length(&data[cursor..])?;
            cursor += pidlen;

            for _ in 0..pending_id_count {
                if cursor + 16 > data.len() {
                    return Err("Truncated consumer pending ID".to_string());
                }
                let ms = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;
                let seq = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;
                consumer.pending.insert(StreamId::new(ms, seq));
            }

            group.consumers.insert(consumer_name, consumer);
        }

        stream.groups.insert(group_name, group);
    }

    Ok((stream, cursor))
}

/// Read TimeSeries data structure
fn read_timeseries(data: &[u8]) -> Result<(crate::storage::timeseries::TimeSeries, usize), String> {
    use crate::storage::{Aggregation, CompactionRule, DuplicatePolicy};

    let mut cursor = 0;

    // Version marker
    if cursor >= data.len() {
        return Err("Truncated timeseries version".to_string());
    }
    let version = data[cursor];
    cursor += 1;
    if version != 1 {
        return Err(format!("Unknown timeseries version: {}", version));
    }

    // Read configuration
    if cursor + 17 > data.len() {
        return Err("Truncated timeseries config".to_string());
    }
    let retention_ms = i64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
    cursor += 8;
    let chunk_duration_ms = i64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
    cursor += 8;
    let duplicate_policy_byte = data[cursor];
    cursor += 1;

    let duplicate_policy = match duplicate_policy_byte {
        0 => DuplicatePolicy::Block,
        1 => DuplicatePolicy::First,
        2 => DuplicatePolicy::Last,
        3 => DuplicatePolicy::Min,
        4 => DuplicatePolicy::Max,
        5 => DuplicatePolicy::Sum,
        _ => DuplicatePolicy::Block,
    };

    // Read first/last timestamps and last value
    if cursor + 24 > data.len() {
        return Err("Truncated timeseries timestamps".to_string());
    }
    let _first_timestamp = i64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
    cursor += 8;
    let _last_timestamp = i64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
    cursor += 8;
    let _last_value = f64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
    cursor += 8;

    // Read labels
    let (label_count, llen) = read_length(&data[cursor..])?;
    cursor += llen;

    let mut labels = std::collections::HashMap::new();
    for _ in 0..label_count {
        let (key, klen) = read_string(&data[cursor..])?;
        cursor += klen;
        let (value, vlen) = read_string(&data[cursor..])?;
        cursor += vlen;
        labels.insert(
            String::from_utf8_lossy(&key).to_string(),
            String::from_utf8_lossy(&value).to_string(),
        );
    }

    // Read compaction rules
    let (rule_count, rlen) = read_length(&data[cursor..])?;
    cursor += rlen;

    let mut rules = Vec::with_capacity(rule_count);
    for _ in 0..rule_count {
        let (dest_key, dlen) = read_string(&data[cursor..])?;
        cursor += dlen;

        if cursor + 17 > data.len() {
            return Err("Truncated compaction rule".to_string());
        }
        let agg_byte = data[cursor];
        cursor += 1;
        let bucket_duration = i64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
        let align_timestamp = i64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;

        let aggregation = match agg_byte {
            0 => Aggregation::Avg,
            1 => Aggregation::First,
            2 => Aggregation::Last,
            3 => Aggregation::Min,
            4 => Aggregation::Max,
            5 => Aggregation::Sum,
            6 => Aggregation::Range,
            7 => Aggregation::Count,
            8 => Aggregation::StdP,
            9 => Aggregation::StdS,
            10 => Aggregation::VarP,
            11 => Aggregation::VarS,
            12 => Aggregation::Twa,
            _ => Aggregation::Avg,
        };

        rules.push(CompactionRule {
            dest_key,
            aggregation,
            bucket_duration,
            align_timestamp,
        });
    }

    // Create TimeSeries with options
    let mut ts = crate::storage::timeseries::TimeSeries::with_options(
        retention_ms,
        chunk_duration_ms,
        duplicate_policy,
    );
    ts.labels = labels;
    ts.rules = rules;

    // Read samples
    let (sample_count, slen) = read_length(&data[cursor..])?;
    cursor += slen;

    for _ in 0..sample_count {
        if cursor + 16 > data.len() {
            return Err("Truncated timeseries sample".to_string());
        }
        let timestamp = i64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
        let value = f64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;

        // Ignore errors during add (e.g., duplicate timestamps)
        let _ = ts.add(timestamp, value);
    }

    Ok((ts, cursor))
}

/// Read VectorSet data structure
fn read_vectorset(data: &[u8]) -> Result<(VectorSetData, usize), String> {
    let mut cursor = 0;

    // Version marker
    if cursor >= data.len() {
        return Err("Truncated vectorset version".to_string());
    }
    let version = data[cursor];
    cursor += 1;
    if version != 1 {
        return Err(format!("Unknown vectorset version: {}", version));
    }

    // Read configuration
    let (dim, dlen) = read_length(&data[cursor..])?;
    cursor += dlen;

    // Read reduced_dim
    if cursor >= data.len() {
        return Err("Truncated vectorset reduced_dim marker".to_string());
    }
    let has_reduced_dim = data[cursor];
    cursor += 1;
    let reduced_dim = if has_reduced_dim == 1 {
        let (rd, rdlen) = read_length(&data[cursor..])?;
        cursor += rdlen;
        Some(rd)
    } else {
        None
    };

    // Read quant
    if cursor >= data.len() {
        return Err("Truncated vectorset quant".to_string());
    }
    let quant_byte = data[cursor];
    cursor += 1;
    let quant = match quant_byte {
        0 => VectorQuantization::NoQuant,
        1 => VectorQuantization::Q8,
        2 => VectorQuantization::Binary,
        _ => VectorQuantization::NoQuant,
    };

    // Read m, m0, ef_construction
    let (m, mlen) = read_length(&data[cursor..])?;
    cursor += mlen;
    let (m0, m0len) = read_length(&data[cursor..])?;
    cursor += m0len;
    let (ef_construction, eflen) = read_length(&data[cursor..])?;
    cursor += eflen;

    // Read max_level
    if cursor >= data.len() {
        return Err("Truncated vectorset max_level".to_string());
    }
    let max_level = data[cursor];
    cursor += 1;

    // Read level_mult
    if cursor + 8 > data.len() {
        return Err("Truncated vectorset level_mult".to_string());
    }
    let level_mult = f64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
    cursor += 8;

    // Read entry point
    if cursor >= data.len() {
        return Err("Truncated vectorset entry_point marker".to_string());
    }
    let has_entry_point = data[cursor];
    cursor += 1;
    let entry_point = if has_entry_point == 1 {
        if cursor + 4 > data.len() {
            return Err("Truncated vectorset entry_point".to_string());
        }
        let ep = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());
        cursor += 4;
        Some(ep)
    } else {
        None
    };

    // Create VectorSetData
    let mut vs = VectorSetData {
        dim,
        reduced_dim,
        quant,
        nodes: Vec::new(),
        element_index: std::collections::HashMap::new(),
        entry_point,
        max_level,
        m,
        m0,
        ef_construction,
        level_mult,
    };

    // Read nodes
    let (node_count, nlen) = read_length(&data[cursor..])?;
    cursor += nlen;

    for _ in 0..node_count {
        // Read element name
        let (element, elen) = read_string(&data[cursor..])?;
        cursor += elen;

        // Read vector
        let (vec_len, vlen) = read_length(&data[cursor..])?;
        cursor += vlen;

        let mut vector = Vec::with_capacity(vec_len);
        for _ in 0..vec_len {
            if cursor + 4 > data.len() {
                return Err("Truncated vectorset vector element".to_string());
            }
            let v = f32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());
            cursor += 4;
            vector.push(v);
        }

        // Read level
        if cursor >= data.len() {
            return Err("Truncated vectorset node level".to_string());
        }
        let level = data[cursor];
        cursor += 1;

        // Read connections per layer
        let (conn_layer_count, cllen) = read_length(&data[cursor..])?;
        cursor += cllen;

        let mut connections = Vec::with_capacity(conn_layer_count);
        for _ in 0..conn_layer_count {
            let (conn_count, cclen) = read_length(&data[cursor..])?;
            cursor += cclen;

            let mut layer_conns = Vec::with_capacity(conn_count);
            for _ in 0..conn_count {
                if cursor + 4 > data.len() {
                    return Err("Truncated vectorset connection".to_string());
                }
                let conn = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());
                cursor += 4;
                layer_conns.push(conn);
            }
            connections.push(layer_conns);
        }

        // Read attributes
        if cursor >= data.len() {
            return Err("Truncated vectorset attributes marker".to_string());
        }
        let has_attrs = data[cursor];
        cursor += 1;
        let attributes = if has_attrs == 1 {
            let (json_str, jlen) = read_string(&data[cursor..])?;
            cursor += jlen;
            match sonic_rs::from_slice(&json_str) {
                Ok(val) => Some(Box::new(val)),
                Err(_) => None,
            }
        } else {
            None
        };

        // Create VectorNode
        let node = VectorNode {
            element: element.clone(),
            vector,
            vector_q8: None,  // Will be regenerated if needed
            vector_bin: None, // Will be regenerated if needed
            attributes,
            connections,
            level,
        };

        vs.nodes.push(node);
    }

    // Read element_index
    let (index_count, ilen) = read_length(&data[cursor..])?;
    cursor += ilen;

    for _ in 0..index_count {
        let (name, nlen) = read_string(&data[cursor..])?;
        cursor += nlen;

        if cursor + 4 > data.len() {
            return Err("Truncated vectorset element_index value".to_string());
        }
        let idx = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());
        cursor += 4;

        vs.element_index.insert(name, idx);
    }

    Ok((vs, cursor))
}
