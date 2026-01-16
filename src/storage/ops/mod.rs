//! Storage operations organized by data type
//!
//! This module splits the large Store implementation into manageable,
//! type-specific operation modules. Each module contains all operations
//! for a specific Redis data type.

pub mod bitmap_ops;
pub mod bloom_ops;
pub mod generic_ops;
pub mod geo_ops;
pub mod hash_ops;
pub mod hyperloglog_ops;
pub mod json_ops;
pub mod list_ops;
pub mod search_ops;
pub mod set_ops;
pub mod sorted_set_ops;
pub mod stream_ops;
pub mod string_ops;
pub mod timeseries_ops;
pub mod vector_ops;
