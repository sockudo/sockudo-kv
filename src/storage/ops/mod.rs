//! Storage operations organized by data type
//!
//! This module splits the large Store implementation into manageable,
//! type-specific operation modules. Each module contains all operations
//! for a specific Redis data type.

pub mod bitmap_ops;
pub mod generic_ops;
pub mod geo_ops;
pub mod hash_ops;
pub mod hyperloglog_ops;
pub mod list_ops;
pub mod set_ops;
pub mod sorted_set_ops;
pub mod stream_ops;
pub mod string_ops;

// Optional modules (feature-gated)
#[cfg(feature = "json")]
pub mod json_ops;

#[cfg(feature = "search")]
pub mod search_ops;

#[cfg(feature = "timeseries")]
pub mod timeseries_ops;

#[cfg(feature = "vector")]
pub mod vector_ops;

// RedisBloom-like probabilistic data structures
#[cfg(feature = "bloom")]
pub mod bloom_ops;

#[cfg(feature = "bloom")]
pub mod cuckoo_ops;

#[cfg(feature = "bloom")]
pub mod tdigest_ops;

#[cfg(feature = "bloom")]
pub mod topk_ops;

#[cfg(feature = "bloom")]
pub mod cms_ops;
