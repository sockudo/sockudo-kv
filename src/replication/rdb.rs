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
            write_value(&mut rdb, key, &value.data, config.compression);
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
fn write_value(rdb: &mut Vec<u8>, key: &Bytes, data: &DataType, compress: bool) {
    match data {
        DataType::String(s) => {
            rdb.push(RDB_TYPE_STRING);
            write_string(rdb, key);
            write_string_compressed(rdb, s, compress);
        }
        DataType::List(list) => {
            rdb.push(RDB_TYPE_LIST);
            write_string(rdb, key);
            write_length(rdb, list.len());
            for item in list.iter() {
                write_string(rdb, &item);
            }
        }
        DataType::Set(set) => {
            rdb.push(RDB_TYPE_SET);
            write_string(rdb, key);
            write_length(rdb, set.len());
            for item in set.iter() {
                write_string(rdb, item.key());
            }
        }
        DataType::IntSet(set) => {
            // Serialize as regular SET of strings for compatibility
            rdb.push(RDB_TYPE_SET);
            write_string(rdb, key);
            write_length(rdb, set.len());
            for item in set.iter() {
                let s = item.to_string();
                write_string(rdb, &Bytes::from(s));
            }
        }
        DataType::Hash(hash) => {
            rdb.push(RDB_TYPE_HASH);
            write_string(rdb, key);
            write_length(rdb, hash.len());
            for item in hash.iter() {
                write_string(rdb, &item.0);
                write_string(rdb, &item.1);
            }
        }
        DataType::HashPacked(lp) => {
            rdb.push(RDB_TYPE_HASH);
            write_string(rdb, key);
            write_length(rdb, lp.len());
            for (k, v) in lp.iter() {
                write_string(rdb, &k);
                write_string(rdb, &v);
            }
        }
        DataType::SortedSet(zset) => {
            rdb.push(RDB_TYPE_ZSET);
            write_string(rdb, key);
            write_length(rdb, zset.len());
            for (member, score) in &zset.scores {
                write_string(rdb, member);
                // Write score as double
                write_double(rdb, *score);
            }
        }
        DataType::SortedSetPacked(lp) => {
            rdb.push(RDB_TYPE_ZSET);
            write_string(rdb, key);
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
            write_string(rdb, key);
            let regs = hll.get_registers();
            write_string(rdb, &Bytes::copy_from_slice(&regs));
        }
        DataType::Json(json) => {
            // Serialize JSON as string
            rdb.push(RDB_TYPE_STRING);
            write_string(rdb, key);
            let json_str = sonic_rs::to_string(json.as_ref()).unwrap_or_default();
            write_string(rdb, &Bytes::from(json_str));
        }
        // Stream, TimeSeries, VectorSet - serialize as minimal representation
        DataType::Stream(_)
        | DataType::TimeSeries(_)
        | DataType::VectorSet(_)
        | DataType::BloomFilter(_) => {
            // Skip complex types for now - they need special handling
            // or implement simplified representation
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
