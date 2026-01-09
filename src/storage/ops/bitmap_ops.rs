use bytes::Bytes;
use dashmap::mapref::entry::Entry as DashEntry;

use crate::error::{Error, Result};
use crate::storage::Store;
use crate::storage::types::{DataType, Entry};

use std::sync::atomic::Ordering;

/// Bitmap operations for the Store
/// Redis treats strings as arrays of bits - these operations work on that model
impl Store {
    // ==================== Bitmap operations ====================

    /// SETBIT - Sets or clears the bit at offset. Returns the original bit value.
    /// Auto-extends the string if needed, fills with zero bytes.
    #[inline]
    pub fn setbit(&self, key: Bytes, offset: usize, value: bool) -> Result<u8> {
        let byte_offset = offset / 8;
        let bit_offset = 7 - (offset % 8); // Redis uses big-endian bit ordering

        match self.data.entry(key) {
            DashEntry::Occupied(mut e) => {
                let entry = e.get_mut();
                if entry.is_expired() {
                    // Create new string with the bit set
                    let mut bytes = vec![0u8; byte_offset + 1];
                    if value {
                        bytes[byte_offset] |= 1 << bit_offset;
                    }
                    entry.data = DataType::String(Bytes::from(bytes));
                    entry.persist();
                    entry.bump_version();
                    return Ok(0);
                }

                match &mut entry.data {
                    DataType::String(s) => {
                        let mut bytes = s.to_vec();

                        // Extend if needed
                        if byte_offset >= bytes.len() {
                            bytes.resize(byte_offset + 1, 0);
                        }

                        // Get old bit value
                        let old_bit = (bytes[byte_offset] >> bit_offset) & 1;

                        // Set new bit value
                        if value {
                            bytes[byte_offset] |= 1 << bit_offset;
                        } else {
                            bytes[byte_offset] &= !(1 << bit_offset);
                        }

                        *s = Bytes::from(bytes);
                        entry.bump_version();
                        Ok(old_bit)
                    }
                    _ => Err(Error::WrongType),
                }
            }
            DashEntry::Vacant(e) => {
                let mut bytes = vec![0u8; byte_offset + 1];
                if value {
                    bytes[byte_offset] |= 1 << bit_offset;
                }
                e.insert(Entry::new(DataType::String(Bytes::from(bytes))));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(0)
            }
        }
    }

    /// GETBIT - Returns the bit value at offset. Returns 0 if key doesn't exist or offset is out of range.
    #[inline]
    pub fn getbit(&self, key: &[u8], offset: usize) -> Result<u8> {
        let byte_offset = offset / 8;
        let bit_offset = 7 - (offset % 8);

        match self.data.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    return Ok(0);
                }
                match &entry.data {
                    DataType::String(s) => {
                        if byte_offset >= s.len() {
                            Ok(0)
                        } else {
                            Ok((s[byte_offset] >> bit_offset) & 1)
                        }
                    }
                    _ => Err(Error::WrongType),
                }
            }
            None => Ok(0),
        }
    }

    /// BITCOUNT - Count the number of set bits in a string
    /// If use_bit_index is true, start/end are bit offsets; otherwise byte offsets
    #[inline]
    pub fn bitcount(
        &self,
        key: &[u8],
        start: Option<i64>,
        end: Option<i64>,
        use_bit_index: bool,
    ) -> Result<usize> {
        match self.data.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    return Ok(0);
                }
                match &entry.data {
                    DataType::String(s) => {
                        if s.is_empty() {
                            return Ok(0);
                        }

                        let len = s.len() as i64;

                        if use_bit_index {
                            // BIT mode: start/end are bit offsets
                            let bit_len = len * 8;
                            let start = normalize_bit_index(start.unwrap_or(0), bit_len);
                            let end = normalize_bit_index(end.unwrap_or(bit_len - 1), bit_len);

                            if start > end || start >= bit_len {
                                return Ok(0);
                            }

                            let start = start as usize;
                            let end = (end as usize).min((bit_len - 1) as usize);

                            // Count bits in range
                            let mut count = 0usize;
                            for bit_pos in start..=end {
                                let byte_idx = bit_pos / 8;
                                let bit_idx = 7 - (bit_pos % 8);
                                if (s[byte_idx] >> bit_idx) & 1 == 1 {
                                    count += 1;
                                }
                            }
                            Ok(count)
                        } else {
                            // BYTE mode (default): start/end are byte offsets
                            let start = normalize_index(start.unwrap_or(0), len) as usize;
                            let end = normalize_index(end.unwrap_or(len - 1), len) as usize;

                            if start > end || start >= s.len() {
                                return Ok(0);
                            }

                            let end = end.min(s.len() - 1);

                            // Use popcount - this compiles to POPCNT instruction
                            let count: u32 = s[start..=end].iter().map(|b| b.count_ones()).sum();
                            Ok(count as usize)
                        }
                    }
                    _ => Err(Error::WrongType),
                }
            }
            None => Ok(0),
        }
    }

    /// BITPOS - Find the first bit set to 0 or 1 in a string
    /// Returns -1 if bit not found
    #[inline]
    pub fn bitpos(
        &self,
        key: &[u8],
        bit: u8,
        start: Option<i64>,
        end: Option<i64>,
        use_bit_index: bool,
    ) -> Result<i64> {
        match self.data.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    // Empty string behavior: looking for 0 returns 0, looking for 1 returns -1
                    return Ok(if bit == 0 { 0 } else { -1 });
                }
                match &entry.data {
                    DataType::String(s) => {
                        if s.is_empty() {
                            return Ok(if bit == 0 { 0 } else { -1 });
                        }

                        let len = s.len() as i64;

                        if use_bit_index {
                            // BIT mode
                            let bit_len = len * 8;
                            let start_bit =
                                normalize_bit_index(start.unwrap_or(0), bit_len) as usize;
                            let end_bit =
                                normalize_bit_index(end.unwrap_or(bit_len - 1), bit_len) as usize;

                            if start_bit > end_bit {
                                return Ok(-1);
                            }

                            let end_bit = end_bit.min((bit_len - 1) as usize);

                            for pos in start_bit..=end_bit {
                                let byte_idx = pos / 8;
                                let bit_idx = 7 - (pos % 8);
                                let current_bit = (s[byte_idx] >> bit_idx) & 1;
                                if current_bit == bit {
                                    return Ok(pos as i64);
                                }
                            }
                            Ok(-1)
                        } else {
                            // BYTE mode
                            let start_byte = normalize_index(start.unwrap_or(0), len) as usize;
                            let end_byte = if let Some(e) = end {
                                normalize_index(e, len) as usize
                            } else {
                                s.len() - 1
                            };

                            if start_byte > end_byte || start_byte >= s.len() {
                                return Ok(-1);
                            }

                            let end_byte = end_byte.min(s.len() - 1);
                            let has_explicit_end = end.is_some();

                            // Search for the bit
                            for byte_idx in start_byte..=end_byte {
                                let byte = s[byte_idx];
                                if bit == 1 {
                                    // Looking for first 1
                                    if byte != 0 {
                                        // Find the first set bit
                                        for bit_idx in 0..8 {
                                            if (byte >> (7 - bit_idx)) & 1 == 1 {
                                                return Ok((byte_idx * 8 + bit_idx) as i64);
                                            }
                                        }
                                    }
                                } else {
                                    // Looking for first 0
                                    if byte != 0xFF {
                                        // Find the first clear bit
                                        for bit_idx in 0..8 {
                                            if (byte >> (7 - bit_idx)) & 1 == 0 {
                                                return Ok((byte_idx * 8 + bit_idx) as i64);
                                            }
                                        }
                                    }
                                }
                            }

                            // Not found in range
                            if bit == 0 && !has_explicit_end {
                                // Special case: if looking for 0 without explicit end,
                                // return the position just after the string
                                Ok((s.len() * 8) as i64)
                            } else {
                                Ok(-1)
                            }
                        }
                    }
                    _ => Err(Error::WrongType),
                }
            }
            None => {
                // Empty key: looking for 0 returns 0, looking for 1 returns -1
                Ok(if bit == 0 { 0 } else { -1 })
            }
        }
    }

    /// BITOP - Perform bitwise operations between strings and store result in destkey
    /// Returns the size of the resulting string
    #[inline]
    pub fn bitop(&self, op: &[u8], destkey: Bytes, keys: &[Bytes]) -> Result<usize> {
        if keys.is_empty() {
            return Err(Error::WrongArity("BITOP"));
        }

        // Get all source strings
        let mut strings: Vec<Vec<u8>> = Vec::with_capacity(keys.len());
        let mut max_len = 0usize;

        for key in keys {
            match self.data.get(key.as_ref()) {
                Some(entry) => {
                    if entry.is_expired() {
                        strings.push(Vec::new());
                    } else {
                        match &entry.data {
                            DataType::String(s) => {
                                max_len = max_len.max(s.len());
                                strings.push(s.to_vec());
                            }
                            _ => return Err(Error::WrongType),
                        }
                    }
                }
                None => strings.push(Vec::new()),
            }
        }

        // Handle NOT specially (single operand)
        let op_upper = op.to_ascii_uppercase();
        let result = match op_upper.as_slice() {
            b"NOT" => {
                if keys.len() != 1 {
                    return Err(Error::Syntax);
                }
                let src = &strings[0];
                src.iter().map(|b| !b).collect::<Vec<u8>>()
            }
            b"AND" => {
                if max_len == 0 {
                    Vec::new()
                } else {
                    let mut result = vec![0xFF; max_len];
                    for s in &strings {
                        for (i, r) in result.iter_mut().enumerate() {
                            let byte = s.get(i).copied().unwrap_or(0);
                            *r &= byte;
                        }
                    }
                    result
                }
            }
            b"OR" => {
                let mut result = vec![0u8; max_len];
                for s in &strings {
                    for (i, &byte) in s.iter().enumerate() {
                        result[i] |= byte;
                    }
                }
                result
            }
            b"XOR" => {
                let mut result = vec![0u8; max_len];
                for s in &strings {
                    for (i, &byte) in s.iter().enumerate() {
                        result[i] ^= byte;
                    }
                }
                result
            }
            b"DIFF" => {
                // DIFF: first AND NOT(OR of rest) - bits in first but not in any other
                if strings.is_empty() {
                    Vec::new()
                } else {
                    let first = &strings[0];
                    let mut result = first.clone();
                    result.resize(max_len, 0);

                    for s in strings.iter().skip(1) {
                        for (i, &byte) in s.iter().enumerate() {
                            result[i] &= !byte;
                        }
                    }
                    result
                }
            }
            b"DIFF1" => {
                // DIFF1: XOR between first two only (like diff for exactly 2 args)
                if strings.len() < 2 {
                    strings.first().cloned().unwrap_or_default()
                } else {
                    let mut result = vec![0u8; max_len];
                    let s1 = &strings[0];
                    let s2 = &strings[1];
                    for (i, r) in result.iter_mut().enumerate() {
                        let b1 = s1.get(i).copied().unwrap_or(0);
                        let b2 = s2.get(i).copied().unwrap_or(0);
                        *r = b1 ^ b2;
                    }
                    result
                }
            }
            b"ANDOR" => {
                // ANDOR: AND of first two, then OR with rest
                if strings.len() < 2 {
                    strings.first().cloned().unwrap_or_default()
                } else {
                    let mut result = vec![0u8; max_len];
                    let s1 = &strings[0];
                    let s2 = &strings[1];
                    // First AND
                    for (i, r) in result.iter_mut().enumerate() {
                        let b1 = s1.get(i).copied().unwrap_or(0);
                        let b2 = s2.get(i).copied().unwrap_or(0);
                        *r = b1 & b2;
                    }
                    // Then OR with rest
                    for s in strings.iter().skip(2) {
                        for (i, &byte) in s.iter().enumerate() {
                            result[i] |= byte;
                        }
                    }
                    result
                }
            }
            b"ONE" => {
                // ONE: bits that are set in exactly one source
                let mut counts = vec![0u8; max_len * 8];
                for s in &strings {
                    for (byte_idx, &byte) in s.iter().enumerate() {
                        for bit_idx in 0..8 {
                            if (byte >> (7 - bit_idx)) & 1 == 1 {
                                counts[byte_idx * 8 + bit_idx] =
                                    counts[byte_idx * 8 + bit_idx].saturating_add(1);
                            }
                        }
                    }
                }
                let mut result = vec![0u8; max_len];
                for (bit_pos, &count) in counts.iter().enumerate() {
                    if count == 1 {
                        let byte_idx = bit_pos / 8;
                        let bit_idx = 7 - (bit_pos % 8);
                        result[byte_idx] |= 1 << bit_idx;
                    }
                }
                result
            }
            _ => return Err(Error::Syntax),
        };

        let result_len = result.len();

        // Store result
        if result.is_empty() {
            self.del(destkey.as_ref());
        } else {
            match self.data.entry(destkey) {
                DashEntry::Occupied(mut e) => {
                    let entry = e.get_mut();
                    entry.data = DataType::String(Bytes::from(result));
                    entry.persist();
                    entry.bump_version();
                }
                DashEntry::Vacant(e) => {
                    e.insert(Entry::new(DataType::String(Bytes::from(result))));
                    self.key_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        Ok(result_len)
    }

    /// BITFIELD - Perform arbitrary bitfield integer operations
    /// Returns a vector of results for each GET/SET/INCRBY subcommand
    #[inline]
    pub fn bitfield(&self, key: Bytes, ops: Vec<BitfieldOp>) -> Result<Vec<Option<i64>>> {
        let mut results = Vec::with_capacity(ops.len());

        match self.data.entry(key) {
            DashEntry::Occupied(mut e) => {
                let entry = e.get_mut();
                if entry.is_expired() {
                    entry.data = DataType::String(Bytes::new());
                    entry.persist();
                }

                match &mut entry.data {
                    DataType::String(s) => {
                        let mut bytes = s.to_vec();
                        let mut overflow_mode = OverflowMode::Wrap;

                        for op in ops {
                            match op {
                                BitfieldOp::Overflow(mode) => {
                                    overflow_mode = mode;
                                }
                                BitfieldOp::Get { encoding, offset } => {
                                    let value = bitfield_get(&bytes, encoding, offset);
                                    results.push(Some(value));
                                }
                                BitfieldOp::Set {
                                    encoding,
                                    offset,
                                    value,
                                } => {
                                    ensure_bytes_size(&mut bytes, offset, encoding.bits);
                                    let old = bitfield_get(&bytes, encoding, offset);
                                    bitfield_set(&mut bytes, encoding, offset, value);
                                    results.push(Some(old));
                                }
                                BitfieldOp::IncrBy {
                                    encoding,
                                    offset,
                                    increment,
                                } => {
                                    ensure_bytes_size(&mut bytes, offset, encoding.bits);
                                    let old = bitfield_get(&bytes, encoding, offset);
                                    let new_value =
                                        match overflow_mode.apply(old, increment, encoding) {
                                            Some(v) => v,
                                            None => {
                                                results.push(None);
                                                continue;
                                            }
                                        };
                                    bitfield_set(&mut bytes, encoding, offset, new_value);
                                    results.push(Some(new_value));
                                }
                            }
                        }

                        *s = Bytes::from(bytes);
                        entry.bump_version();
                    }
                    _ => return Err(Error::WrongType),
                }
            }
            DashEntry::Vacant(e) => {
                let mut bytes = Vec::new();
                let mut overflow_mode = OverflowMode::Wrap;

                for op in ops {
                    match op {
                        BitfieldOp::Overflow(mode) => {
                            overflow_mode = mode;
                        }
                        BitfieldOp::Get { encoding, offset } => {
                            let value = bitfield_get(&bytes, encoding, offset);
                            results.push(Some(value));
                        }
                        BitfieldOp::Set {
                            encoding,
                            offset,
                            value,
                        } => {
                            ensure_bytes_size(&mut bytes, offset, encoding.bits);
                            let old = bitfield_get(&bytes, encoding, offset);
                            bitfield_set(&mut bytes, encoding, offset, value);
                            results.push(Some(old));
                        }
                        BitfieldOp::IncrBy {
                            encoding,
                            offset,
                            increment,
                        } => {
                            ensure_bytes_size(&mut bytes, offset, encoding.bits);
                            let old = bitfield_get(&bytes, encoding, offset);
                            let new_value = match overflow_mode.apply(old, increment, encoding) {
                                Some(v) => v,
                                None => {
                                    results.push(None);
                                    continue;
                                }
                            };
                            bitfield_set(&mut bytes, encoding, offset, new_value);
                            results.push(Some(new_value));
                        }
                    }
                }

                if !bytes.is_empty() {
                    e.insert(Entry::new(DataType::String(Bytes::from(bytes))));
                    self.key_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        Ok(results)
    }

    /// BITFIELD_RO - Read-only bitfield operations (GET only)
    #[inline]
    pub fn bitfield_ro(&self, key: &[u8], ops: Vec<BitfieldOp>) -> Result<Vec<Option<i64>>> {
        let mut results = Vec::with_capacity(ops.len());

        let bytes = match self.data.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    Vec::new()
                } else {
                    match &entry.data {
                        DataType::String(s) => s.to_vec(),
                        _ => return Err(Error::WrongType),
                    }
                }
            }
            None => Vec::new(),
        };

        for op in ops {
            match op {
                BitfieldOp::Get { encoding, offset } => {
                    let value = bitfield_get(&bytes, encoding, offset);
                    results.push(Some(value));
                }
                _ => return Err(Error::Syntax), // RO only allows GET
            }
        }

        Ok(results)
    }
}

// ==================== Bitfield Helper Types ====================

/// Encoding for bitfield operations
#[derive(Debug, Clone, Copy)]
pub struct BitfieldEncoding {
    pub signed: bool,
    pub bits: u8,
}

impl BitfieldEncoding {
    pub fn parse(s: &[u8]) -> Option<Self> {
        if s.is_empty() {
            return None;
        }

        let signed = s[0] == b'i' || s[0] == b'I';
        let unsigned = s[0] == b'u' || s[0] == b'U';

        if !signed && !unsigned {
            return None;
        }

        let bits: u8 = std::str::from_utf8(&s[1..]).ok()?.parse().ok()?;

        // Valid ranges: u1-u63, i1-i64
        if signed {
            if !(1..=64).contains(&bits) {
                return None;
            }
        } else if !(1..=63).contains(&bits) {
            return None;
        }

        Some(Self { signed, bits })
    }

    pub fn max_value(self) -> i64 {
        if self.signed {
            (1i64 << (self.bits - 1)) - 1
        } else if self.bits >= 63 {
            i64::MAX
        } else {
            (1i64 << self.bits) - 1
        }
    }

    pub fn min_value(self) -> i64 {
        if self.signed {
            -(1i64 << (self.bits - 1))
        } else {
            0
        }
    }
}

/// Overflow handling mode
#[derive(Debug, Clone, Copy, Default)]
pub enum OverflowMode {
    #[default]
    Wrap,
    Sat,
    Fail,
}

impl OverflowMode {
    pub fn from_bytes(s: &[u8]) -> Option<Self> {
        match s.to_ascii_uppercase().as_slice() {
            b"WRAP" => Some(Self::Wrap),
            b"SAT" => Some(Self::Sat),
            b"FAIL" => Some(Self::Fail),
            _ => None,
        }
    }

    /// Apply overflow handling to an increment operation
    /// Returns None if FAIL mode and overflow would occur
    pub fn apply(self, old: i64, increment: i64, encoding: BitfieldEncoding) -> Option<i64> {
        let min = encoding.min_value();
        let max = encoding.max_value();

        let (new_val, overflow) = old.overflowing_add(increment);
        let in_range = new_val >= min && new_val <= max;

        match self {
            OverflowMode::Wrap => {
                if encoding.signed {
                    // Signed wrap
                    let range = 1i128 << encoding.bits;
                    let mut v = (old as i128 + increment as i128) % range;
                    if v > max as i128 {
                        v -= range;
                    } else if v < min as i128 {
                        v += range;
                    }
                    Some(v as i64)
                } else {
                    // Unsigned wrap
                    let mask = if encoding.bits >= 64 {
                        u64::MAX
                    } else {
                        (1u64 << encoding.bits) - 1
                    };
                    let v = ((old as u64).wrapping_add(increment as u64)) & mask;
                    Some(v as i64)
                }
            }
            OverflowMode::Sat => {
                if overflow || !in_range {
                    if increment > 0 { Some(max) } else { Some(min) }
                } else {
                    Some(new_val)
                }
            }
            OverflowMode::Fail => {
                if overflow || !in_range {
                    None
                } else {
                    Some(new_val)
                }
            }
        }
    }
}

/// Bitfield operation
#[derive(Debug)]
pub enum BitfieldOp {
    Get {
        encoding: BitfieldEncoding,
        offset: usize,
    },
    Set {
        encoding: BitfieldEncoding,
        offset: usize,
        value: i64,
    },
    IncrBy {
        encoding: BitfieldEncoding,
        offset: usize,
        increment: i64,
    },
    Overflow(OverflowMode),
}

// ==================== Bitfield Helper Functions ====================

/// Ensure bytes vector is large enough for the given bit operation
fn ensure_bytes_size(bytes: &mut Vec<u8>, bit_offset: usize, bits: u8) {
    let needed_bytes = (bit_offset + bits as usize).div_ceil(8);
    if bytes.len() < needed_bytes {
        bytes.resize(needed_bytes, 0);
    }
}

/// Get a value from the bitfield
fn bitfield_get(bytes: &[u8], encoding: BitfieldEncoding, bit_offset: usize) -> i64 {
    let bits = encoding.bits as usize;
    let mut value: u64 = 0;

    for i in 0..bits {
        let pos = bit_offset + i;
        let byte_idx = pos / 8;
        let bit_idx = 7 - (pos % 8);

        if byte_idx < bytes.len() {
            let bit = (bytes[byte_idx] >> bit_idx) & 1;
            value = (value << 1) | bit as u64;
        } else {
            value <<= 1;
        }
    }

    if encoding.signed && bits > 0 {
        // Sign extend
        let sign_bit = 1u64 << (bits - 1);
        if value & sign_bit != 0 {
            let mask = !((1u64 << bits) - 1);
            value |= mask;
        }
    }

    value as i64
}

/// Set a value in the bitfield
fn bitfield_set(bytes: &mut [u8], encoding: BitfieldEncoding, bit_offset: usize, value: i64) {
    let bits = encoding.bits as usize;
    let value = value as u64;

    for i in 0..bits {
        let pos = bit_offset + i;
        let byte_idx = pos / 8;
        let bit_idx = 7 - (pos % 8);

        if byte_idx < bytes.len() {
            let bit = (value >> (bits - 1 - i)) & 1;
            if bit == 1 {
                bytes[byte_idx] |= 1 << bit_idx;
            } else {
                bytes[byte_idx] &= !(1 << bit_idx);
            }
        }
    }
}

// ==================== Index Helpers ====================

/// Normalize index for byte operations (supports negative indices)
fn normalize_index(idx: i64, len: i64) -> i64 {
    if idx < 0 {
        (len + idx).max(0)
    } else {
        idx.min(len - 1).max(0)
    }
}

/// Normalize index for bit operations
fn normalize_bit_index(idx: i64, bit_len: i64) -> i64 {
    if idx < 0 {
        (bit_len + idx).max(0)
    } else {
        idx.min(bit_len - 1).max(0)
    }
}
