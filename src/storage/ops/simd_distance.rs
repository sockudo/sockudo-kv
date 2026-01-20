//! SIMD-optimized distance computation for vector similarity search
//!
//! This module provides highly optimized distance functions using:
//! - AVX-512 (512-bit) on supported x86_64 CPUs
//! - AVX2 (256-bit) on most modern x86_64 CPUs
//! - NEON (128-bit) on ARM64 (Apple Silicon, AWS Graviton, etc.)
//! - Scalar fallback for any architecture
//!
//! The module uses runtime feature detection to select the best implementation.

// Allow unsafe operations inside unsafe functions (Rust 2024 compatibility)
#![allow(unsafe_op_in_unsafe_fn)]

use std::sync::OnceLock;

// ==================== Runtime CPU Feature Detection ====================

/// CPU feature detection result (cached on first use)
#[derive(Clone, Copy, Debug)]
pub struct SimdCapabilities {
    #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
    pub avx512f: bool,
    #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
    pub avx2: bool,
    #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
    pub fma: bool,
    #[cfg(target_arch = "aarch64")]
    pub neon: bool,
}

impl SimdCapabilities {
    fn detect() -> Self {
        Self {
            #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
            avx512f: std::arch::is_x86_feature_detected!("avx512f"),
            #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
            avx2: std::arch::is_x86_feature_detected!("avx2"),
            #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
            fma: std::arch::is_x86_feature_detected!("fma"),
            #[cfg(target_arch = "aarch64")]
            neon: std::arch::is_aarch64_feature_detected!("neon"),
        }
    }
}

/// Get cached SIMD capabilities
pub fn simd_caps() -> &'static SimdCapabilities {
    static CAPS: OnceLock<SimdCapabilities> = OnceLock::new();
    CAPS.get_or_init(SimdCapabilities::detect)
}

// ==================== Precomputed Norm Structure ====================

/// Precomputed vector metadata for fast distance computation
#[derive(Debug, Clone, Copy)]
pub struct VectorMeta {
    /// L2 norm (magnitude) of the vector
    pub norm: f32,
    /// 1.0 / norm (for fast normalization)
    pub inv_norm: f32,
    /// Sum of squares (for euclidean distance)
    pub sum_sq: f32,
}

impl VectorMeta {
    /// Compute metadata for a vector
    #[inline]
    pub fn compute(vector: &[f32]) -> Self {
        let sum_sq = dot_product_auto(vector, vector);
        let norm = sum_sq.sqrt();
        let inv_norm = if norm > 1e-10 { 1.0 / norm } else { 0.0 };
        Self {
            norm,
            inv_norm,
            sum_sq,
        }
    }

    /// Check if this is a zero/near-zero vector
    #[inline]
    pub fn is_zero(&self) -> bool {
        self.norm < 1e-10
    }
}

// ==================== Dispatch Functions ====================

/// Compute dot product using the best available SIMD instructions
#[inline]
pub fn dot_product_auto(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());

    #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
    {
        let caps = simd_caps();
        if caps.avx512f && a.len() >= 16 {
            return unsafe { dot_product_avx512(a, b) };
        }
        if caps.avx2 && a.len() >= 8 {
            return unsafe { dot_product_avx2(a, b) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        let caps = simd_caps();
        if caps.neon && a.len() >= 4 {
            return unsafe { dot_product_neon(a, b) };
        }
    }

    dot_product_scalar(a, b)
}

/// Compute squared L2 distance using the best available SIMD instructions
#[inline]
pub fn l2_distance_sq_auto(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());

    #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
    {
        let caps = simd_caps();
        if caps.avx512f && a.len() >= 16 {
            return unsafe { l2_distance_sq_avx512(a, b) };
        }
        if caps.avx2 && a.len() >= 8 {
            return unsafe { l2_distance_sq_avx2(a, b) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        let caps = simd_caps();
        if caps.neon && a.len() >= 4 {
            return unsafe { l2_distance_sq_neon(a, b) };
        }
    }

    l2_distance_sq_scalar(a, b)
}

/// Compute cosine distance: 1 - (a · b) / (||a|| * ||b||)
/// Range: [0, 2] where 0 = identical, 1 = orthogonal, 2 = opposite
#[inline]
pub fn cosine_distance_auto(a: &[f32], b: &[f32]) -> f32 {
    let dot = dot_product_auto(a, b);
    let norm_a = dot_product_auto(a, a).sqrt();
    let norm_b = dot_product_auto(b, b).sqrt();

    if norm_a < 1e-10 || norm_b < 1e-10 {
        return 1.0; // Treat zero vectors as orthogonal
    }

    1.0 - (dot / (norm_a * norm_b))
}

/// Compute cosine distance using precomputed norms (much faster for repeated queries)
#[inline]
pub fn cosine_distance_with_norms(
    a: &[f32],
    meta_a: &VectorMeta,
    b: &[f32],
    meta_b: &VectorMeta,
) -> f32 {
    if meta_a.is_zero() || meta_b.is_zero() {
        return 1.0;
    }

    let dot = dot_product_auto(a, b);
    // Use precomputed inverse norms: (a · b) / (||a|| * ||b||) = (a · b) * inv_norm_a * inv_norm_b
    let similarity = dot * meta_a.inv_norm * meta_b.inv_norm;
    1.0 - similarity.clamp(-1.0, 1.0)
}

/// Compute sum of squares (for norm computation)
#[inline]
pub fn sum_of_squares_auto(v: &[f32]) -> f32 {
    dot_product_auto(v, v)
}

// ==================== Scalar Implementations ====================

#[inline]
fn dot_product_scalar(a: &[f32], b: &[f32]) -> f32 {
    // Process in chunks of 4 for better instruction-level parallelism
    let chunks = a.len() / 4;
    let mut sum0 = 0.0f32;
    let mut sum1 = 0.0f32;
    let mut sum2 = 0.0f32;
    let mut sum3 = 0.0f32;

    for i in 0..chunks {
        let base = i * 4;
        sum0 += a[base] * b[base];
        sum1 += a[base + 1] * b[base + 1];
        sum2 += a[base + 2] * b[base + 2];
        sum3 += a[base + 3] * b[base + 3];
    }

    let mut total = sum0 + sum1 + sum2 + sum3;

    // Handle remaining elements
    for i in (chunks * 4)..a.len() {
        total += a[i] * b[i];
    }

    total
}

#[inline]
fn l2_distance_sq_scalar(a: &[f32], b: &[f32]) -> f32 {
    let chunks = a.len() / 4;
    let mut sum0 = 0.0f32;
    let mut sum1 = 0.0f32;
    let mut sum2 = 0.0f32;
    let mut sum3 = 0.0f32;

    for i in 0..chunks {
        let base = i * 4;
        let d0 = a[base] - b[base];
        let d1 = a[base + 1] - b[base + 1];
        let d2 = a[base + 2] - b[base + 2];
        let d3 = a[base + 3] - b[base + 3];
        sum0 += d0 * d0;
        sum1 += d1 * d1;
        sum2 += d2 * d2;
        sum3 += d3 * d3;
    }

    let mut total = sum0 + sum1 + sum2 + sum3;

    for i in (chunks * 4)..a.len() {
        let d = a[i] - b[i];
        total += d * d;
    }

    total
}

// ==================== AVX-512 Implementations (x86_64) ====================

#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
#[target_feature(enable = "avx512f")]
unsafe fn dot_product_avx512(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::x86_64::*;

    let n = a.len();
    let chunks = n / 16;

    let mut sum = _mm512_setzero_ps();

    for i in 0..chunks {
        let base = i * 16;
        let va = _mm512_loadu_ps(a.as_ptr().add(base));
        let vb = _mm512_loadu_ps(b.as_ptr().add(base));
        sum = _mm512_fmadd_ps(va, vb, sum);
    }

    // Horizontal sum of 512-bit register
    let mut total = _mm512_reduce_add_ps(sum);

    // Handle remaining elements
    for i in (chunks * 16)..n {
        total += a[i] * b[i];
    }

    total
}

#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
#[target_feature(enable = "avx512f")]
unsafe fn l2_distance_sq_avx512(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::x86_64::*;

    let n = a.len();
    let chunks = n / 16;

    let mut sum = _mm512_setzero_ps();

    for i in 0..chunks {
        let base = i * 16;
        let va = _mm512_loadu_ps(a.as_ptr().add(base));
        let vb = _mm512_loadu_ps(b.as_ptr().add(base));
        let diff = _mm512_sub_ps(va, vb);
        sum = _mm512_fmadd_ps(diff, diff, sum);
    }

    let mut total = _mm512_reduce_add_ps(sum);

    for i in (chunks * 16)..n {
        let d = a[i] - b[i];
        total += d * d;
    }

    total
}

// ==================== AVX2 Implementations (x86_64) ====================

#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
#[target_feature(enable = "avx2", enable = "fma")]
unsafe fn dot_product_avx2(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::x86_64::*;

    let n = a.len();
    let chunks = n / 8;

    // Use 4 accumulators to hide FMA latency (4 cycles on modern CPUs)
    let mut sum0 = _mm256_setzero_ps();
    let mut sum1 = _mm256_setzero_ps();
    let mut sum2 = _mm256_setzero_ps();
    let mut sum3 = _mm256_setzero_ps();

    let unroll_chunks = chunks / 4;

    for i in 0..unroll_chunks {
        let base = i * 32;

        let va0 = _mm256_loadu_ps(a.as_ptr().add(base));
        let vb0 = _mm256_loadu_ps(b.as_ptr().add(base));
        sum0 = _mm256_fmadd_ps(va0, vb0, sum0);

        let va1 = _mm256_loadu_ps(a.as_ptr().add(base + 8));
        let vb1 = _mm256_loadu_ps(b.as_ptr().add(base + 8));
        sum1 = _mm256_fmadd_ps(va1, vb1, sum1);

        let va2 = _mm256_loadu_ps(a.as_ptr().add(base + 16));
        let vb2 = _mm256_loadu_ps(b.as_ptr().add(base + 16));
        sum2 = _mm256_fmadd_ps(va2, vb2, sum2);

        let va3 = _mm256_loadu_ps(a.as_ptr().add(base + 24));
        let vb3 = _mm256_loadu_ps(b.as_ptr().add(base + 24));
        sum3 = _mm256_fmadd_ps(va3, vb3, sum3);
    }

    // Handle remaining full 8-element chunks
    for i in (unroll_chunks * 4)..chunks {
        let base = i * 8;
        let va = _mm256_loadu_ps(a.as_ptr().add(base));
        let vb = _mm256_loadu_ps(b.as_ptr().add(base));
        sum0 = _mm256_fmadd_ps(va, vb, sum0);
    }

    // Combine accumulators
    let sum01 = _mm256_add_ps(sum0, sum1);
    let sum23 = _mm256_add_ps(sum2, sum3);
    let sum = _mm256_add_ps(sum01, sum23);

    // Horizontal sum of 256-bit register
    let hi = _mm256_extractf128_ps(sum, 1);
    let lo = _mm256_castps256_ps128(sum);
    let sum128 = _mm_add_ps(lo, hi);
    let sum64 = _mm_add_ps(sum128, _mm_movehl_ps(sum128, sum128));
    let sum32 = _mm_add_ss(sum64, _mm_shuffle_ps(sum64, sum64, 1));
    let mut total = _mm_cvtss_f32(sum32);

    // Handle remaining elements
    for i in (chunks * 8)..n {
        total += a[i] * b[i];
    }

    total
}

#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
#[target_feature(enable = "avx2", enable = "fma")]
unsafe fn l2_distance_sq_avx2(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::x86_64::*;

    let n = a.len();
    let chunks = n / 8;

    let mut sum0 = _mm256_setzero_ps();
    let mut sum1 = _mm256_setzero_ps();
    let mut sum2 = _mm256_setzero_ps();
    let mut sum3 = _mm256_setzero_ps();

    let unroll_chunks = chunks / 4;

    for i in 0..unroll_chunks {
        let base = i * 32;

        let va0 = _mm256_loadu_ps(a.as_ptr().add(base));
        let vb0 = _mm256_loadu_ps(b.as_ptr().add(base));
        let diff0 = _mm256_sub_ps(va0, vb0);
        sum0 = _mm256_fmadd_ps(diff0, diff0, sum0);

        let va1 = _mm256_loadu_ps(a.as_ptr().add(base + 8));
        let vb1 = _mm256_loadu_ps(b.as_ptr().add(base + 8));
        let diff1 = _mm256_sub_ps(va1, vb1);
        sum1 = _mm256_fmadd_ps(diff1, diff1, sum1);

        let va2 = _mm256_loadu_ps(a.as_ptr().add(base + 16));
        let vb2 = _mm256_loadu_ps(b.as_ptr().add(base + 16));
        let diff2 = _mm256_sub_ps(va2, vb2);
        sum2 = _mm256_fmadd_ps(diff2, diff2, sum2);

        let va3 = _mm256_loadu_ps(a.as_ptr().add(base + 24));
        let vb3 = _mm256_loadu_ps(b.as_ptr().add(base + 24));
        let diff3 = _mm256_sub_ps(va3, vb3);
        sum3 = _mm256_fmadd_ps(diff3, diff3, sum3);
    }

    for i in (unroll_chunks * 4)..chunks {
        let base = i * 8;
        let va = _mm256_loadu_ps(a.as_ptr().add(base));
        let vb = _mm256_loadu_ps(b.as_ptr().add(base));
        let diff = _mm256_sub_ps(va, vb);
        sum0 = _mm256_fmadd_ps(diff, diff, sum0);
    }

    let sum01 = _mm256_add_ps(sum0, sum1);
    let sum23 = _mm256_add_ps(sum2, sum3);
    let sum = _mm256_add_ps(sum01, sum23);

    let hi = _mm256_extractf128_ps(sum, 1);
    let lo = _mm256_castps256_ps128(sum);
    let sum128 = _mm_add_ps(lo, hi);
    let sum64 = _mm_add_ps(sum128, _mm_movehl_ps(sum128, sum128));
    let sum32 = _mm_add_ss(sum64, _mm_shuffle_ps(sum64, sum64, 1));
    let mut total = _mm_cvtss_f32(sum32);

    for i in (chunks * 8)..n {
        let d = a[i] - b[i];
        total += d * d;
    }

    total
}

// ==================== NEON Implementations (ARM64) ====================

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn dot_product_neon(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::aarch64::*;

    let n = a.len();
    let chunks = n / 4;

    // Use 4 accumulators for better pipelining
    let mut sum0 = vdupq_n_f32(0.0);
    let mut sum1 = vdupq_n_f32(0.0);
    let mut sum2 = vdupq_n_f32(0.0);
    let mut sum3 = vdupq_n_f32(0.0);

    let unroll_chunks = chunks / 4;

    for i in 0..unroll_chunks {
        let base = i * 16;

        let va0 = vld1q_f32(a.as_ptr().add(base));
        let vb0 = vld1q_f32(b.as_ptr().add(base));
        sum0 = vfmaq_f32(sum0, va0, vb0);

        let va1 = vld1q_f32(a.as_ptr().add(base + 4));
        let vb1 = vld1q_f32(b.as_ptr().add(base + 4));
        sum1 = vfmaq_f32(sum1, va1, vb1);

        let va2 = vld1q_f32(a.as_ptr().add(base + 8));
        let vb2 = vld1q_f32(b.as_ptr().add(base + 8));
        sum2 = vfmaq_f32(sum2, va2, vb2);

        let va3 = vld1q_f32(a.as_ptr().add(base + 12));
        let vb3 = vld1q_f32(b.as_ptr().add(base + 12));
        sum3 = vfmaq_f32(sum3, va3, vb3);
    }

    for i in (unroll_chunks * 4)..chunks {
        let base = i * 4;
        let va = vld1q_f32(a.as_ptr().add(base));
        let vb = vld1q_f32(b.as_ptr().add(base));
        sum0 = vfmaq_f32(sum0, va, vb);
    }

    // Combine accumulators
    let sum01 = vaddq_f32(sum0, sum1);
    let sum23 = vaddq_f32(sum2, sum3);
    let sum = vaddq_f32(sum01, sum23);

    // Horizontal sum
    let mut total = vaddvq_f32(sum);

    for i in (chunks * 4)..n {
        total += a[i] * b[i];
    }

    total
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn l2_distance_sq_neon(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::aarch64::*;

    let n = a.len();
    let chunks = n / 4;

    let mut sum0 = vdupq_n_f32(0.0);
    let mut sum1 = vdupq_n_f32(0.0);
    let mut sum2 = vdupq_n_f32(0.0);
    let mut sum3 = vdupq_n_f32(0.0);

    let unroll_chunks = chunks / 4;

    for i in 0..unroll_chunks {
        let base = i * 16;

        let va0 = vld1q_f32(a.as_ptr().add(base));
        let vb0 = vld1q_f32(b.as_ptr().add(base));
        let diff0 = vsubq_f32(va0, vb0);
        sum0 = vfmaq_f32(sum0, diff0, diff0);

        let va1 = vld1q_f32(a.as_ptr().add(base + 4));
        let vb1 = vld1q_f32(b.as_ptr().add(base + 4));
        let diff1 = vsubq_f32(va1, vb1);
        sum1 = vfmaq_f32(sum1, diff1, diff1);

        let va2 = vld1q_f32(a.as_ptr().add(base + 8));
        let vb2 = vld1q_f32(b.as_ptr().add(base + 8));
        let diff2 = vsubq_f32(va2, vb2);
        sum2 = vfmaq_f32(sum2, diff2, diff2);

        let va3 = vld1q_f32(a.as_ptr().add(base + 12));
        let vb3 = vld1q_f32(b.as_ptr().add(base + 12));
        let diff3 = vsubq_f32(va3, vb3);
        sum3 = vfmaq_f32(sum3, diff3, diff3);
    }

    for i in (unroll_chunks * 4)..chunks {
        let base = i * 4;
        let va = vld1q_f32(a.as_ptr().add(base));
        let vb = vld1q_f32(b.as_ptr().add(base));
        let diff = vsubq_f32(va, vb);
        sum0 = vfmaq_f32(sum0, diff, diff);
    }

    let sum01 = vaddq_f32(sum0, sum1);
    let sum23 = vaddq_f32(sum2, sum3);
    let sum = vaddq_f32(sum01, sum23);

    let mut total = vaddvq_f32(sum);

    for i in (chunks * 4)..n {
        let d = a[i] - b[i];
        total += d * d;
    }

    total
}

// ==================== Binary/Hamming Distance (SIMD) ====================

/// Compute Hamming distance between two binary vectors (stored as u64 arrays)
/// Uses POPCNT instruction for fast bit counting
#[inline]
pub fn hamming_distance_auto(a: &[u64], b: &[u64]) -> u32 {
    debug_assert_eq!(a.len(), b.len());

    #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
    {
        if std::arch::is_x86_feature_detected!("popcnt") {
            return unsafe { hamming_distance_popcnt(a, b) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        return unsafe { hamming_distance_neon(a, b) };
    }

    #[allow(unreachable_code)]
    hamming_distance_scalar(a, b)
}

#[inline]
fn hamming_distance_scalar(a: &[u64], b: &[u64]) -> u32 {
    a.iter()
        .zip(b.iter())
        .map(|(&x, &y)| (x ^ y).count_ones())
        .sum()
}

#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
#[target_feature(enable = "popcnt")]
unsafe fn hamming_distance_popcnt(a: &[u64], b: &[u64]) -> u32 {
    use std::arch::x86_64::*;

    let mut total = 0u32;
    for (&x, &y) in a.iter().zip(b.iter()) {
        total += _popcnt64((x ^ y) as i64) as u32;
    }
    total
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn hamming_distance_neon(a: &[u64], b: &[u64]) -> u32 {
    use std::arch::aarch64::*;

    let mut total = 0u32;

    // Process pairs of u64 (128 bits at a time)
    let chunks = a.len() / 2;
    for i in 0..chunks {
        let base = i * 2;
        let va = vld1q_u64(a.as_ptr().add(base));
        let vb = vld1q_u64(b.as_ptr().add(base));
        let xor = veorq_u64(va, vb);

        // Cast to u8 for popcount
        let xor_u8 = vreinterpretq_u8_u64(xor);
        let cnt = vcntq_u8(xor_u8);

        // Horizontal sum
        total += vaddlvq_u8(cnt) as u32;
    }

    // Handle remaining
    for i in (chunks * 2)..a.len() {
        total += (a[i] ^ b[i]).count_ones();
    }

    total
}

// ==================== Q8 Quantized Distance ====================

/// Compute dot product of Q8 quantized vectors
/// Uses SIMD for processing 32 i8 values at a time
#[inline]
pub fn dot_product_q8_auto(a: &[i8], b: &[i8]) -> i32 {
    debug_assert_eq!(a.len(), b.len());

    #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
    {
        let caps = simd_caps();
        if caps.avx2 && a.len() >= 32 {
            return unsafe { dot_product_q8_avx2(a, b) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        let caps = simd_caps();
        if caps.neon && a.len() >= 16 {
            return unsafe { dot_product_q8_neon(a, b) };
        }
    }

    dot_product_q8_scalar(a, b)
}

#[inline]
fn dot_product_q8_scalar(a: &[i8], b: &[i8]) -> i32 {
    a.iter()
        .zip(b.iter())
        .map(|(&x, &y)| (x as i32) * (y as i32))
        .sum()
}

#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
#[target_feature(enable = "avx2")]
unsafe fn dot_product_q8_avx2(a: &[i8], b: &[i8]) -> i32 {
    use std::arch::x86_64::*;

    let n = a.len();
    let chunks = n / 32;

    let mut sum = _mm256_setzero_si256();

    for i in 0..chunks {
        let base = i * 32;
        let va = _mm256_loadu_si256(a.as_ptr().add(base) as *const __m256i);
        let vb = _mm256_loadu_si256(b.as_ptr().add(base) as *const __m256i);

        // Multiply adjacent pairs and add: i8*i8 -> i16, then pairs of i16 -> i32
        let prod = _mm256_maddubs_epi16(
            _mm256_xor_si256(va, _mm256_set1_epi8(-128i8)), // Convert signed to unsigned
            _mm256_xor_si256(vb, _mm256_set1_epi8(-128i8)),
        );

        // Horizontal add pairs of i16 to i32
        let prod32 = _mm256_madd_epi16(prod, _mm256_set1_epi16(1));
        sum = _mm256_add_epi32(sum, prod32);
    }

    // Horizontal sum
    let hi = _mm256_extracti128_si256(sum, 1);
    let lo = _mm256_castsi256_si128(sum);
    let sum128 = _mm_add_epi32(lo, hi);
    let sum64 = _mm_add_epi32(sum128, _mm_srli_si128(sum128, 8));
    let sum32 = _mm_add_epi32(sum64, _mm_srli_si128(sum64, 4));
    let mut total = _mm_cvtsi128_si32(sum32);

    // Handle remaining
    for i in (chunks * 32)..n {
        total += (a[i] as i32) * (b[i] as i32);
    }

    // Adjust for signed->unsigned conversion offset
    // Each element was shifted by 128, so we need to subtract the accumulated offset
    let offset = (128 * 128 * 2) as i32 * chunks as i32 * 16; // 16 pairs per 256-bit
    total - offset
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn dot_product_q8_neon(a: &[i8], b: &[i8]) -> i32 {
    use std::arch::aarch64::*;

    let n = a.len();
    let chunks = n / 16;

    let mut sum = vdupq_n_s32(0);

    for i in 0..chunks {
        let base = i * 16;
        let va = vld1q_s8(a.as_ptr().add(base));
        let vb = vld1q_s8(b.as_ptr().add(base));

        // Widening multiply: i8 * i8 -> i16
        let prod_lo = vmull_s8(vget_low_s8(va), vget_low_s8(vb));
        let prod_hi = vmull_s8(vget_high_s8(va), vget_high_s8(vb));

        // Widen to i32 and accumulate
        sum = vpadalq_s16(sum, prod_lo);
        sum = vpadalq_s16(sum, prod_hi);
    }

    let mut total = vaddvq_s32(sum);

    for i in (chunks * 16)..n {
        total += (a[i] as i32) * (b[i] as i32);
    }

    total
}

// ==================== Batch Distance Computation ====================

/// Compute distances from a query to multiple vectors
/// Much more efficient than computing one at a time due to better cache utilization
#[inline]
pub fn batch_cosine_distances(
    query: &[f32],
    query_meta: &VectorMeta,
    vectors: &[&[f32]],
    metas: &[VectorMeta],
    results: &mut [f32],
) {
    debug_assert_eq!(vectors.len(), metas.len());
    debug_assert_eq!(vectors.len(), results.len());

    for (i, (v, m)) in vectors.iter().zip(metas.iter()).enumerate() {
        results[i] = cosine_distance_with_norms(query, query_meta, v, m);
    }
}

/// Compute distances from a query to multiple vectors, returning indices sorted by distance
#[inline]
pub fn batch_cosine_distances_sorted(
    query: &[f32],
    vectors: &[&[f32]],
    limit: usize,
) -> Vec<(usize, f32)> {
    let query_meta = VectorMeta::compute(query);

    let mut results: Vec<(usize, f32)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let meta = VectorMeta::compute(v);
            (i, cosine_distance_with_norms(query, &query_meta, v, &meta))
        })
        .collect();

    // Partial sort for top-k
    if limit < results.len() {
        results.select_nth_unstable_by(limit, |a, b| {
            a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(limit);
    }

    results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    results
}

// ==================== Tests ====================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dot_product_scalar() {
        let a = vec![1.0, 2.0, 3.0, 4.0];
        let b = vec![5.0, 6.0, 7.0, 8.0];
        let result = dot_product_scalar(&a, &b);
        assert!((result - 70.0).abs() < 1e-6);
    }

    #[test]
    fn test_dot_product_auto() {
        let a: Vec<f32> = (0..128).map(|i| i as f32).collect();
        let b: Vec<f32> = (0..128).map(|i| (i * 2) as f32).collect();

        let scalar = dot_product_scalar(&a, &b);
        let auto = dot_product_auto(&a, &b);

        assert!(
            (scalar - auto).abs() < 1e-3,
            "scalar={} auto={}",
            scalar,
            auto
        );
    }

    #[test]
    fn test_l2_distance() {
        let a = vec![1.0, 2.0, 3.0, 4.0];
        let b = vec![5.0, 6.0, 7.0, 8.0];
        let result = l2_distance_sq_auto(&a, &b);
        // (5-1)^2 + (6-2)^2 + (7-3)^2 + (8-4)^2 = 16 + 16 + 16 + 16 = 64
        assert!((result - 64.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_distance() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        let distance = cosine_distance_auto(&a, &b);
        // Orthogonal vectors should have distance 1
        assert!((distance - 1.0).abs() < 1e-6);

        let c = vec![1.0, 0.0, 0.0];
        let d = vec![1.0, 0.0, 0.0];
        let distance2 = cosine_distance_auto(&c, &d);
        // Identical vectors should have distance 0
        assert!(distance2.abs() < 1e-6);
    }

    #[test]
    fn test_cosine_distance_with_norms() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];

        let meta_a = VectorMeta::compute(&a);
        let meta_b = VectorMeta::compute(&b);

        let dist1 = cosine_distance_auto(&a, &b);
        let dist2 = cosine_distance_with_norms(&a, &meta_a, &b, &meta_b);

        assert!((dist1 - dist2).abs() < 1e-6);
    }

    #[test]
    fn test_hamming_distance() {
        let a = vec![0b1010_1010u64, 0b1111_0000u64];
        let b = vec![0b0101_0101u64, 0b0000_1111u64];
        // XOR gives all 1s for both words = 16 bits each = 32 total
        let dist = hamming_distance_auto(&a, &b);
        assert_eq!(dist, 16);
    }

    #[test]
    fn test_q8_dot_product() {
        let a: Vec<i8> = (0..16).map(|i| i as i8).collect();
        let b: Vec<i8> = (0..16).map(|i| (i * 2) as i8).collect();

        let scalar = dot_product_q8_scalar(&a, &b);
        let auto = dot_product_q8_auto(&a, &b);

        assert_eq!(scalar, auto);
    }

    #[test]
    fn test_vector_meta() {
        let v = vec![3.0, 4.0]; // 3-4-5 triangle
        let meta = VectorMeta::compute(&v);

        assert!((meta.norm - 5.0).abs() < 1e-6);
        assert!((meta.inv_norm - 0.2).abs() < 1e-6);
        assert!((meta.sum_sq - 25.0).abs() < 1e-6);
    }
}
