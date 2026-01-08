//! Storage layer module
//!
//! This module provides the core Store implementation and all type-specific operations.
//! Operations are organized into separate modules by data type to keep the codebase
//! maintainable as new commands are added.

mod multi_store;
mod store;
mod types;
mod value;

// Import all operation modules - each adds methods to Store via impl blocks
pub mod ops;

// Re-export the main Store struct
pub use multi_store::{DEFAULT_DB_COUNT, MultiStore};
pub use store::Store;

// Re-export commonly used types
pub use types::{
    Aggregation, CompactionRule, DataType, DuplicatePolicy, Entry, HyperLogLogData, SortedSetData,
    StreamData, StreamId, TimeSeriesData, VectorQuantization,
};
pub use value::now_ms;

// Re-export geo utilities
pub use ops::geo_ops::{GeoResult, from_meters, geohash_to_string};
