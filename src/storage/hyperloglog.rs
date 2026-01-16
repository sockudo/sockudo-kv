//! Ultra-high-performance HyperLogLog implementation
//!
//! Features:
//! - Sparse encoding for memory efficiency (small cardinalities)
//! - Dense encoding with 6-bit packed registers
//! - SIMD-accelerated counting using `wide` crate
//! - Cardinality caching to avoid recomputation
//! - Fast wyhash-based hashing
//! - Automatic sparse-to-dense promotion with proper thresholds

use std::sync::atomic::{AtomicU64, Ordering};
use wide::f64x4;

// ============================================================================
// Constants
// ============================================================================

/// HyperLogLog precision (14 bits = 16384 registers)
pub const HLL_P: usize = 14;
/// Number of registers (2^14 = 16384)
pub const HLL_REGISTERS: usize = 1 << HLL_P;
/// Bits per register in dense encoding
pub const HLL_BITS: usize = 6;
/// Maximum value a register can hold (2^6 - 1 = 63)
pub const HLL_MAX_VALUE: u8 = 63;
/// Dense encoding size in bytes (16384 * 6 / 8 = 12288)
pub const HLL_DENSE_SIZE: usize = (HLL_REGISTERS * HLL_BITS).div_ceil(8);
/// Alpha constant for bias correction (for m=16384)
const HLL_ALPHA: f64 = 0.7213 / (1.0 + 1.079 / HLL_REGISTERS as f64);
/// Invalid cache marker (MSB set)
const HLL_CACHE_INVALID: u64 = 1 << 63;

/// Maximum sparse representation size before promoting to dense
/// Redis uses ~3000 bytes, we use similar threshold
const HLL_SPARSE_MAX_BYTES: usize = 3000;

/// Maximum register value representable in sparse VAL opcode (5 bits = 31, +1 = 32)
const HLL_SPARSE_VAL_MAX: u8 = 32;

// ============================================================================
// Sparse Encoding Opcodes
// ============================================================================
//
// Redis-compatible sparse encoding uses three opcodes:
//
// ZERO:  00xxxxxx           - (x+1) consecutive zero registers (1-64)
// XZERO: 01xxxxxx yyyyyyyy  - (xy+1) consecutive zero registers (1-16384)
// VAL:   1vvvvvcc           - value (v+1) repeated (c+1) times (val 1-32, count 1-4)

/// Check if byte is ZERO opcode (00xxxxxx)
#[inline(always)]
fn is_zero(b: u8) -> bool {
    b & 0xC0 == 0x00
}

/// Check if byte is XZERO opcode (01xxxxxx)
#[inline(always)]
fn is_xzero(b: u8) -> bool {
    b & 0xC0 == 0x40
}

/// Decode ZERO opcode run length (1-64)
#[inline(always)]
fn zero_len(b: u8) -> usize {
    (b & 0x3F) as usize + 1
}

/// Decode XZERO opcode run length (1-16384)
#[inline(always)]
fn xzero_len(b1: u8, b2: u8) -> usize {
    (((b1 & 0x3F) as usize) << 8 | b2 as usize) + 1
}

/// Decode VAL opcode value (1-32)
#[inline(always)]
fn val_value(b: u8) -> u8 {
    ((b >> 2) & 0x1F) + 1
}

/// Decode VAL opcode run length (1-4)
#[inline(always)]
fn val_len(b: u8) -> usize {
    (b & 0x03) as usize + 1
}

/// Encode ZERO opcode (len 1-64)
#[inline(always)]
fn encode_zero(len: usize) -> u8 {
    debug_assert!((1..=64).contains(&len));
    (len - 1) as u8
}

/// Encode XZERO opcode (len 1-16384)
#[inline(always)]
fn encode_xzero(len: usize) -> [u8; 2] {
    debug_assert!((1..=16384).contains(&len));
    let v = (len - 1) as u16;
    [0x40 | ((v >> 8) as u8), (v & 0xFF) as u8]
}

/// Encode VAL opcode (value 1-32, len 1-4)
#[inline(always)]
fn encode_val(value: u8, len: usize) -> u8 {
    debug_assert!((1..=32).contains(&value));
    debug_assert!((1..=4).contains(&len));
    0x80 | ((value - 1) << 2) | ((len - 1) as u8)
}

// ============================================================================
// HyperLogLog Encoding
// ============================================================================

/// HyperLogLog encoding type
#[derive(Debug, Clone)]
pub enum HllEncoding {
    /// Sparse run-length encoding for small cardinalities
    Sparse(Vec<u8>),
    /// Dense 6-bit packed registers
    Dense(Box<[u8; HLL_DENSE_SIZE]>),
}

// ============================================================================
// HyperLogLog Main Structure
// ============================================================================

/// Ultra-high-performance HyperLogLog
#[derive(Debug)]
pub struct HyperLogLog {
    /// Encoding (sparse or dense)
    encoding: HllEncoding,
    /// Cached cardinality (MSB = invalid flag)
    cached_card: AtomicU64,
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for HyperLogLog {
    fn clone(&self) -> Self {
        Self {
            encoding: self.encoding.clone(),
            cached_card: AtomicU64::new(self.cached_card.load(Ordering::Relaxed)),
        }
    }
}

impl HyperLogLog {
    /// Create a new empty HyperLogLog with sparse encoding
    #[inline]
    pub fn new() -> Self {
        // Start with sparse encoding: single XZERO opcode representing 16384 zeros
        let xzero = encode_xzero(HLL_REGISTERS);
        Self {
            encoding: HllEncoding::Sparse(vec![xzero[0], xzero[1]]),
            cached_card: AtomicU64::new(0), // Empty HLL has cardinality 0
        }
    }

    /// Create a new HyperLogLog with dense encoding (pre-allocated)
    #[inline]
    pub fn new_dense() -> Self {
        Self {
            encoding: HllEncoding::Dense(Box::new([0u8; HLL_DENSE_SIZE])),
            cached_card: AtomicU64::new(0),
        }
    }

    /// Check if using sparse encoding
    #[inline]
    pub fn is_sparse(&self) -> bool {
        matches!(self.encoding, HllEncoding::Sparse(_))
    }

    /// Get encoding name
    #[inline]
    pub fn encoding_name(&self) -> &'static str {
        match &self.encoding {
            HllEncoding::Sparse(_) => "sparse",
            HllEncoding::Dense(_) => "dense",
        }
    }

    /// Add an element, returns true if the cardinality estimate might have changed
    #[inline]
    pub fn add(&mut self, data: &[u8]) -> bool {
        let hash = wyhash(data);
        self.add_hash(hash)
    }

    /// Add a pre-computed hash
    pub fn add_hash(&mut self, hash: u64) -> bool {
        // Extract register index (top P bits)
        let index = (hash >> (64 - HLL_P)) as usize;
        // Count leading zeros in remaining bits + 1
        let remaining = (hash << HLL_P) | (1 << (HLL_P - 1));
        let count = (remaining.leading_zeros() as u8 + 1).min(HLL_MAX_VALUE);

        let changed = match &mut self.encoding {
            HllEncoding::Sparse(sparse) => {
                let result = sparse_set(sparse, index, count);
                // Check if we should promote to dense
                if sparse.len() > HLL_SPARSE_MAX_BYTES || count > HLL_SPARSE_VAL_MAX {
                    self.promote_to_dense();
                }
                result
            }
            HllEncoding::Dense(dense) => dense_set(dense, index, count),
        };

        if changed {
            self.invalidate_cache();
        }
        changed
    }

    /// Promote sparse encoding to dense
    pub fn promote_to_dense(&mut self) {
        if let HllEncoding::Sparse(sparse) = &self.encoding {
            let mut dense = Box::new([0u8; HLL_DENSE_SIZE]);

            // Decode sparse into dense
            let mut reg_idx = 0usize;
            let mut i = 0;
            while i < sparse.len() && reg_idx < HLL_REGISTERS {
                let byte = sparse[i];
                if is_zero(byte) {
                    // ZERO: skip (len) registers (they're already 0)
                    reg_idx += zero_len(byte);
                    i += 1;
                } else if is_xzero(byte) {
                    // XZERO: skip (len) registers
                    if i + 1 >= sparse.len() {
                        break;
                    }
                    reg_idx += xzero_len(byte, sparse[i + 1]);
                    i += 2;
                } else {
                    // VAL: set (len) registers to (value)
                    let val = val_value(byte);
                    let len = val_len(byte);
                    for _ in 0..len {
                        if reg_idx < HLL_REGISTERS {
                            dense_set_unchecked(&mut dense, reg_idx, val);
                            reg_idx += 1;
                        }
                    }
                    i += 1;
                }
            }

            self.encoding = HllEncoding::Dense(dense);
        }
    }

    /// Get the cardinality estimate
    #[inline]
    pub fn count(&self) -> u64 {
        // Check cache first
        let cached = self.cached_card.load(Ordering::Relaxed);
        if cached & HLL_CACHE_INVALID == 0 {
            return cached;
        }

        let card = match &self.encoding {
            HllEncoding::Sparse(sparse) => count_sparse(sparse),
            HllEncoding::Dense(dense) => count_dense_simd(dense),
        };

        // Cache the result
        self.cached_card.store(card, Ordering::Relaxed);
        card
    }

    /// Invalidate the cached cardinality
    #[inline]
    fn invalidate_cache(&self) {
        self.cached_card
            .fetch_or(HLL_CACHE_INVALID, Ordering::Relaxed);
    }

    /// Merge another HyperLogLog into this one
    pub fn merge(&mut self, other: &HyperLogLog) {
        // Ensure both are dense for efficient merging
        self.promote_to_dense();

        let other_registers = other.get_registers();

        if let HllEncoding::Dense(dense) = &mut self.encoding {
            // SIMD merge: take max of each register
            for (i, &val) in other_registers.iter().enumerate() {
                if val > 0 {
                    let current = dense_get(dense, i);
                    if val > current {
                        dense_set_unchecked(dense, i, val);
                    }
                }
            }
            self.invalidate_cache();
        }
    }

    /// Get all register values
    pub fn get_registers(&self) -> Vec<u8> {
        match &self.encoding {
            HllEncoding::Dense(dense) => {
                let mut registers = vec![0u8; HLL_REGISTERS];
                for (i, val) in registers.iter_mut().enumerate() {
                    *val = dense_get(dense, i);
                }
                registers
            }
            HllEncoding::Sparse(sparse) => {
                let mut registers = vec![0u8; HLL_REGISTERS];
                let mut reg_idx = 0usize;
                let mut i = 0;

                while i < sparse.len() && reg_idx < HLL_REGISTERS {
                    let byte = sparse[i];
                    if is_zero(byte) {
                        reg_idx += zero_len(byte);
                        i += 1;
                    } else if is_xzero(byte) {
                        if i + 1 >= sparse.len() {
                            break;
                        }
                        reg_idx += xzero_len(byte, sparse[i + 1]);
                        i += 2;
                    } else {
                        let val = val_value(byte);
                        let len = val_len(byte);
                        for _ in 0..len {
                            if reg_idx < HLL_REGISTERS {
                                registers[reg_idx] = val;
                                reg_idx += 1;
                            }
                        }
                        i += 1;
                    }
                }
                registers
            }
        }
    }

    /// Get sparse representation for PFDEBUG DECODE
    pub fn decode_sparse(&self) -> Option<String> {
        match &self.encoding {
            HllEncoding::Sparse(sparse) => {
                let mut result = String::new();
                let mut i = 0;
                while i < sparse.len() {
                    let byte = sparse[i];
                    if is_zero(byte) {
                        let len = zero_len(byte);
                        result.push_str(&format!("z:{} ", len));
                        i += 1;
                    } else if is_xzero(byte) {
                        if i + 1 >= sparse.len() {
                            break;
                        }
                        let len = xzero_len(byte, sparse[i + 1]);
                        result.push_str(&format!("Z:{} ", len));
                        i += 2;
                    } else {
                        let val = val_value(byte);
                        let len = val_len(byte);
                        result.push_str(&format!("v:{},{} ", val, len));
                        i += 1;
                    }
                }
                Some(result.trim_end().to_string())
            }
            HllEncoding::Dense(_) => None,
        }
    }
}

// ============================================================================
// Sparse Encoding Operations
// ============================================================================

/// Set a register in sparse encoding, returns true if value changed
fn sparse_set(sparse: &mut Vec<u8>, index: usize, count: u8) -> bool {
    if count == 0 {
        return false;
    }

    // Find the opcode spanning the target index
    let mut reg_idx = 0usize;
    let mut i = 0;

    while i < sparse.len() && reg_idx <= index {
        let byte = sparse[i];

        if is_zero(byte) {
            let len = zero_len(byte);
            let end_idx = reg_idx + len;

            if index < end_idx {
                // Target is within this ZERO run
                return sparse_set_in_zero(sparse, i, reg_idx, len, index, count);
            }
            reg_idx = end_idx;
            i += 1;
        } else if is_xzero(byte) {
            if i + 1 >= sparse.len() {
                break;
            }
            let len = xzero_len(byte, sparse[i + 1]);
            let end_idx = reg_idx + len;

            if index < end_idx {
                // Target is within this XZERO run
                return sparse_set_in_xzero(sparse, i, reg_idx, len, index, count);
            }
            reg_idx = end_idx;
            i += 2;
        } else {
            // VAL opcode
            let val = val_value(byte);
            let len = val_len(byte);
            let end_idx = reg_idx + len;

            if index < end_idx {
                // Target is within this VAL run
                if count <= val {
                    return false; // Current value is already >= count
                }
                return sparse_set_in_val(sparse, i, reg_idx, val, len, index, count);
            }
            reg_idx = end_idx;
            i += 1;
        }
    }

    false // Index not found (shouldn't happen with valid sparse encoding)
}

/// Set register within a ZERO opcode
fn sparse_set_in_zero(
    sparse: &mut Vec<u8>,
    pos: usize,
    start: usize,
    len: usize,
    index: usize,
    count: u8,
) -> bool {
    let offset = index - start;
    let before = offset;
    let after = len - offset - 1;

    // Build replacement opcodes
    let mut replacement = Vec::with_capacity(5);

    // Zeros before
    if before > 0 {
        append_zeros(&mut replacement, before);
    }

    // The new value
    replacement.push(encode_val(count, 1));

    // Zeros after
    if after > 0 {
        append_zeros(&mut replacement, after);
    }

    // Replace the ZERO opcode
    sparse.splice(pos..pos + 1, replacement);
    true
}

/// Set register within an XZERO opcode
fn sparse_set_in_xzero(
    sparse: &mut Vec<u8>,
    pos: usize,
    start: usize,
    len: usize,
    index: usize,
    count: u8,
) -> bool {
    let offset = index - start;
    let before = offset;
    let after = len - offset - 1;

    // Build replacement opcodes
    let mut replacement = Vec::with_capacity(6);

    // Zeros before
    if before > 0 {
        append_zeros(&mut replacement, before);
    }

    // The new value
    replacement.push(encode_val(count, 1));

    // Zeros after
    if after > 0 {
        append_zeros(&mut replacement, after);
    }

    // Replace the XZERO opcode (2 bytes)
    sparse.splice(pos..pos + 2, replacement);
    true
}

/// Set register within a VAL opcode
fn sparse_set_in_val(
    sparse: &mut Vec<u8>,
    pos: usize,
    start: usize,
    old_val: u8,
    len: usize,
    index: usize,
    count: u8,
) -> bool {
    let offset = index - start;
    let before = offset;
    let after = len - offset - 1;

    // Build replacement opcodes
    let mut replacement = Vec::with_capacity(3);

    // Values before (same old value)
    if before > 0 {
        append_vals(&mut replacement, old_val, before);
    }

    // The new value
    replacement.push(encode_val(count, 1));

    // Values after (same old value)
    if after > 0 {
        append_vals(&mut replacement, old_val, after);
    }

    // Replace the VAL opcode
    sparse.splice(pos..pos + 1, replacement);
    true
}

/// Append zero opcodes for `count` registers
fn append_zeros(buf: &mut Vec<u8>, mut count: usize) {
    while count > 0 {
        if count > 64 {
            // Use XZERO for large runs
            let run = count.min(16384);
            let xzero = encode_xzero(run);
            buf.push(xzero[0]);
            buf.push(xzero[1]);
            count -= run;
        } else {
            // Use ZERO for small runs
            buf.push(encode_zero(count));
            count = 0;
        }
    }
}

/// Append VAL opcodes for `count` registers with `value`
fn append_vals(buf: &mut Vec<u8>, value: u8, mut count: usize) {
    while count > 0 {
        let run = count.min(4); // VAL can encode 1-4 repetitions
        buf.push(encode_val(value, run));
        count -= run;
    }
}

/// Count cardinality for sparse encoding
fn count_sparse(sparse: &[u8]) -> u64 {
    let mut sum = 0.0f64;
    let mut zeros = 0usize;
    let mut total_regs = 0usize;

    let mut i = 0;
    while i < sparse.len() {
        let byte = sparse[i];
        if is_zero(byte) {
            let len = zero_len(byte);
            zeros += len;
            sum += len as f64; // 2^(-0) = 1
            total_regs += len;
            i += 1;
        } else if is_xzero(byte) {
            if i + 1 >= sparse.len() {
                break;
            }
            let len = xzero_len(byte, sparse[i + 1]);
            zeros += len;
            sum += len as f64;
            total_regs += len;
            i += 2;
        } else {
            let val = val_value(byte);
            let len = val_len(byte);
            sum += (len as f64) * 2.0f64.powi(-(val as i32));
            total_regs += len;
            i += 1;
        }
    }

    // Fill remaining with zeros if sparse is incomplete
    if total_regs < HLL_REGISTERS {
        let remaining = HLL_REGISTERS - total_regs;
        zeros += remaining;
        sum += remaining as f64;
    }

    compute_cardinality(sum, zeros)
}

// ============================================================================
// Dense Encoding Operations
// ============================================================================

/// Set a register in dense encoding, returns true if value changed
#[inline]
fn dense_set(dense: &mut [u8; HLL_DENSE_SIZE], index: usize, value: u8) -> bool {
    let current = dense_get(dense, index);
    if value <= current {
        return false;
    }
    dense_set_unchecked(dense, index, value);
    true
}

/// Set a register without checking current value
#[inline]
fn dense_set_unchecked(dense: &mut [u8; HLL_DENSE_SIZE], index: usize, value: u8) {
    let bit_pos = index * HLL_BITS;
    let byte_pos = bit_pos / 8;
    let bit_offset = bit_pos % 8;

    if bit_offset <= 2 {
        // Value fits in single byte (bits 0-5, 1-6, or 2-7)
        let mask = 0x3F << bit_offset;
        dense[byte_pos] = (dense[byte_pos] & !mask) | ((value & 0x3F) << bit_offset);
    } else {
        // Value spans two bytes
        let bits_in_first = 8 - bit_offset;
        let bits_in_second = HLL_BITS - bits_in_first;

        // Clear and set bits in first byte
        let mask1 = (1u8 << bits_in_first) - 1;
        dense[byte_pos] =
            (dense[byte_pos] & !(mask1 << bit_offset)) | ((value & mask1) << bit_offset);

        // Clear and set bits in second byte
        if byte_pos + 1 < HLL_DENSE_SIZE {
            let mask2 = (1u8 << bits_in_second) - 1;
            dense[byte_pos + 1] =
                (dense[byte_pos + 1] & !mask2) | ((value >> bits_in_first) & mask2);
        }
    }
}

/// Get a register from dense encoding
#[inline]
fn dense_get(dense: &[u8; HLL_DENSE_SIZE], index: usize) -> u8 {
    let bit_pos = index * HLL_BITS;
    let byte_pos = bit_pos / 8;
    let bit_offset = bit_pos % 8;

    if bit_offset <= 2 {
        // Value fits in single byte
        (dense[byte_pos] >> bit_offset) & 0x3F
    } else {
        // Value spans two bytes
        let bits_in_first = 8 - bit_offset;
        let low = dense[byte_pos] >> bit_offset;
        let high = if byte_pos + 1 < HLL_DENSE_SIZE {
            dense[byte_pos + 1] << bits_in_first
        } else {
            0
        };
        (low | high) & 0x3F
    }
}

/// Count cardinality from dense encoding using SIMD
fn count_dense_simd(dense: &[u8; HLL_DENSE_SIZE]) -> u64 {
    // First extract all registers into a flat array
    let mut registers = [0u8; HLL_REGISTERS];
    for (i, val) in registers.iter_mut().enumerate() {
        *val = dense_get(dense, i);
    }

    // SIMD-accelerated sum using wide crate
    // Process 4 registers at a time with f64x4
    let mut sum = f64x4::ZERO;
    let mut zeros = 0usize;

    // Precompute powers of 2^(-n) for n = 0..63
    // This avoids calling powi in the hot loop
    const POW_TABLE: [f64; 64] = {
        let mut table = [0.0; 64];
        let mut i = 0;
        while i < 64 {
            table[i] = 1.0 / ((1u64 << i) as f64);
            i += 1;
        }
        table
    };

    let mut i = 0;
    while i + 4 <= HLL_REGISTERS {
        let r0 = registers[i] as usize;
        let r1 = registers[i + 1] as usize;
        let r2 = registers[i + 2] as usize;
        let r3 = registers[i + 3] as usize;

        // Use lookup table for 2^(-r)
        let v = f64x4::new([POW_TABLE[r0], POW_TABLE[r1], POW_TABLE[r2], POW_TABLE[r3]]);
        sum += v;

        // Count zeros
        zeros += (r0 == 0) as usize + (r1 == 0) as usize + (r2 == 0) as usize + (r3 == 0) as usize;

        i += 4;
    }

    // Handle remaining registers (should be 0 for 16384 which is divisible by 4)
    let mut scalar_sum = 0.0f64;
    while i < HLL_REGISTERS {
        let r = registers[i] as usize;
        scalar_sum += POW_TABLE[r];
        zeros += (r == 0) as usize;
        i += 1;
    }

    let total_sum = sum.reduce_add() + scalar_sum;
    compute_cardinality(total_sum, zeros)
}

/// Compute cardinality from harmonic sum and zero count
#[inline]
fn compute_cardinality(sum: f64, zeros: usize) -> u64 {
    let m = HLL_REGISTERS as f64;
    let estimate = HLL_ALPHA * m * m / sum;

    // Small range correction (linear counting)
    if estimate <= 2.5 * m && zeros > 0 {
        return (m * (m / zeros as f64).ln()) as u64;
    }

    // Large range correction (for very large cardinalities)
    let two_pow_32 = (1u64 << 32) as f64;
    if estimate > two_pow_32 / 30.0 {
        return (-two_pow_32 * (1.0 - estimate / two_pow_32).ln()) as u64;
    }

    estimate as u64
}

// ============================================================================
// Fast Hash Function (wyhash)
// ============================================================================

/// Ultra-fast wyhash implementation (v4)
#[inline]
pub fn wyhash(data: &[u8]) -> u64 {
    const P0: u64 = 0xa076_1d64_78bd_642f;
    const P1: u64 = 0xe703_7ed1_a0b4_28db;
    const P2: u64 = 0x8ebc_6af0_9c88_c6e3;
    const P3: u64 = 0x5899_65cc_7537_4cc3;

    let len = data.len();
    let mut seed = P0;

    if len <= 16 {
        if len >= 4 {
            let a = read32(data) as u64;
            let b = read32(&data[((len >> 3) << 2)..]) as u64;
            let c = read32(&data[len - 4..]) as u64;
            let d = read32(&data[len - 4 - ((len >> 3) << 2)..]) as u64;
            return wymix(wymix(a ^ P0, b ^ P1), wymix(c ^ P2, d ^ P3)) ^ (len as u64);
        } else if len > 0 {
            let a = data[0] as u64;
            let b = data[len >> 1] as u64;
            let c = data[len - 1] as u64;
            return wymix(a ^ P0, (b << 8 | c) ^ seed) ^ (len as u64);
        } else {
            return wymix(P0, P1);
        }
    }

    let mut p = data;
    let mut remaining = len;

    if remaining > 48 {
        let mut s1 = seed;
        let mut s2 = seed;
        while remaining > 48 {
            seed = wymix(read64(p) ^ P0, read64(&p[8..]) ^ seed);
            s1 = wymix(read64(&p[16..]) ^ P1, read64(&p[24..]) ^ s1);
            s2 = wymix(read64(&p[32..]) ^ P2, read64(&p[40..]) ^ s2);
            p = &p[48..];
            remaining -= 48;
        }
        seed ^= s1 ^ s2;
    }

    while remaining > 16 {
        seed = wymix(read64(p) ^ P0, read64(&p[8..]) ^ seed);
        p = &p[16..];
        remaining -= 16;
    }

    let a = read64(&p[remaining.saturating_sub(16)..]);
    let b = read64(&p[remaining.saturating_sub(8)..]);
    wymix(a ^ P0, b ^ seed) ^ (len as u64)
}

#[inline(always)]
fn wymix(a: u64, b: u64) -> u64 {
    let r = (a as u128).wrapping_mul(b as u128);
    (r as u64) ^ (r >> 64) as u64
}

#[inline(always)]
fn read64(data: &[u8]) -> u64 {
    if data.len() >= 8 {
        u64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ])
    } else {
        let mut buf = [0u8; 8];
        buf[..data.len()].copy_from_slice(data);
        u64::from_le_bytes(buf)
    }
}

#[inline(always)]
fn read32(data: &[u8]) -> u32 {
    if data.len() >= 4 {
        u32::from_le_bytes([data[0], data[1], data[2], data[3]])
    } else {
        let mut buf = [0u8; 4];
        buf[..data.len()].copy_from_slice(data);
        u32::from_le_bytes(buf)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_hll() {
        let hll = HyperLogLog::new();
        assert!(hll.is_sparse());
        assert_eq!(hll.count(), 0);
    }

    #[test]
    fn test_add_single() {
        let mut hll = HyperLogLog::new();
        assert!(hll.add(b"hello"));
        assert!(hll.count() >= 1);
    }

    #[test]
    fn test_sparse_encoding() {
        let mut hll = HyperLogLog::new();
        // Add a few elements - should stay sparse
        for i in 0..10 {
            hll.add(format!("elem{}", i).as_bytes());
        }
        assert!(hll.is_sparse());
        let count = hll.count();
        assert!(count >= 8 && count <= 12, "count was {}", count);
    }

    #[test]
    fn test_sparse_decode() {
        let hll = HyperLogLog::new();
        let decoded = hll.decode_sparse().unwrap();
        assert!(decoded.contains("Z:16384"), "decoded: {}", decoded);
    }

    #[test]
    fn test_promote_to_dense() {
        let mut hll = HyperLogLog::new();
        assert!(hll.is_sparse());
        hll.promote_to_dense();
        assert!(!hll.is_sparse());
        assert_eq!(hll.encoding_name(), "dense");
    }

    #[test]
    fn test_add_many_promotes() {
        let mut hll = HyperLogLog::new();
        // Add enough elements to exceed sparse threshold
        for i in 0..5000 {
            hll.add(format!("element{}", i).as_bytes());
        }
        // Should have promoted to dense
        assert!(!hll.is_sparse());
    }

    #[test]
    fn test_add_many_accuracy() {
        let mut hll = HyperLogLog::new();
        for i in 0..10000 {
            hll.add(format!("element{}", i).as_bytes());
        }
        let count = hll.count();
        // HLL should estimate within ~2% of actual count
        assert!(count >= 9500 && count <= 10500, "count was {}", count);
    }

    #[test]
    fn test_merge() {
        let mut hll1 = HyperLogLog::new();
        let mut hll2 = HyperLogLog::new();

        for i in 0..5000 {
            hll1.add(format!("a{}", i).as_bytes());
        }
        for i in 0..5000 {
            hll2.add(format!("b{}", i).as_bytes());
        }

        hll1.merge(&hll2);
        let count = hll1.count();
        // Should be approximately 10000
        assert!(count >= 9500 && count <= 10500, "count was {}", count);
    }

    #[test]
    fn test_cache() {
        let mut hll = HyperLogLog::new();
        for i in 0..1000 {
            hll.add(format!("elem{}", i).as_bytes());
        }

        let count1 = hll.count();
        let count2 = hll.count(); // Should use cache
        assert_eq!(count1, count2);
    }

    #[test]
    fn test_dense_encoding_direct() {
        let mut hll = HyperLogLog::new_dense();
        assert!(!hll.is_sparse());

        for i in 0..1000 {
            hll.add(format!("item{}", i).as_bytes());
        }

        let count = hll.count();
        assert!(count >= 950 && count <= 1050, "count was {}", count);
    }

    #[test]
    fn test_dense_bit_packing() {
        // Test all register positions and values
        let mut dense = [0u8; HLL_DENSE_SIZE];

        // Test setting and getting various positions
        for i in [0, 1, 2, 3, 100, 1000, 8191, 8192, 16383] {
            for val in [1, 31, 32, 63] {
                dense_set_unchecked(&mut dense, i, val);
                let got = dense_get(&dense, i);
                assert_eq!(got, val, "index={}, expected={}, got={}", i, val, got);
            }
        }
    }

    #[test]
    fn test_wyhash_consistency() {
        let h1 = wyhash(b"hello");
        let h2 = wyhash(b"hello");
        assert_eq!(h1, h2);

        let h3 = wyhash(b"world");
        assert_ne!(h1, h3);
    }

    #[test]
    fn test_registers() {
        let mut hll = HyperLogLog::new_dense();
        hll.add(b"test");

        let regs = hll.get_registers();
        assert_eq!(regs.len(), HLL_REGISTERS);

        // At least one register should be non-zero
        assert!(regs.iter().any(|&r| r > 0));
    }

    #[test]
    fn test_zero_opcode() {
        assert!(is_zero(0b00_000000));
        assert!(is_zero(0b00_111111));
        assert!(!is_zero(0b01_000000));
        assert!(!is_zero(0b10_000000));

        assert_eq!(zero_len(0b00_000000), 1);
        assert_eq!(zero_len(0b00_111111), 64);
    }

    #[test]
    fn test_xzero_opcode() {
        assert!(is_xzero(0b01_000000));
        assert!(is_xzero(0b01_111111));
        assert!(!is_xzero(0b00_000000));
        assert!(!is_xzero(0b10_000000));

        assert_eq!(xzero_len(0b01_000000, 0), 1);
        assert_eq!(xzero_len(0b01_111111, 0xFF), 16384);
    }

    #[test]
    fn test_val_opcode() {
        assert!(0b10_000000 & 0x80 == 0x80);
        assert!(0b11_111111 & 0x80 == 0x80);
        assert!(!(0b00_000000 & 0x80 == 0x80));
        assert!(!(0b01_000000 & 0x80 == 0x80));

        // 1vvvvvcc where v=0 (value 1), c=0 (count 1)
        assert_eq!(val_value(0b10_000000), 1);
        assert_eq!(val_len(0b10_000000), 1);

        // v=31 (value 32), c=3 (count 4)
        assert_eq!(val_value(0b11_111111), 32);
        assert_eq!(val_len(0b11_111111), 4);
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        // ZERO
        for len in 1..=64 {
            let encoded = encode_zero(len);
            assert!(is_zero(encoded));
            assert_eq!(zero_len(encoded), len);
        }

        // XZERO
        for len in [1, 64, 65, 100, 1000, 16384] {
            let [b1, b2] = encode_xzero(len);
            assert!(is_xzero(b1));
            assert_eq!(xzero_len(b1, b2), len);
        }

        // VAL
        for val in 1..=32 {
            for len in 1..=4 {
                let encoded = encode_val(val, len);
                assert!(encoded & 0x80 == 0x80);
                assert_eq!(val_value(encoded), val);
                assert_eq!(val_len(encoded), len);
            }
        }
    }
}
