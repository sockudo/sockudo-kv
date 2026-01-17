//! Storage layer module
//!
//! This module provides the core Store implementation and all type-specific operations.
//! Operations are organized into separate modules by data type to keep the codebase
//! maintainable as new commands are added.

pub mod dashtable;
pub use dashtable::{DashTable, calculate_hash};
mod multi_store;
mod store;
mod types;
mod value;

// High-performance HyperLogLog implementation
pub mod hyperloglog;

// High-performance B+Tree inspired by Dragonfly
pub mod bench_ops;
pub mod bptree;
pub mod intset;
pub mod listpack;
pub mod quicklist;

// Ultra-high-performance TimeSeries with Gorilla compression
pub mod timeseries;

// Ultra-high-performance Bloom Filter
pub mod bloomfilter;

// Ultra-high-performance Cuckoo Filter
pub mod cuckoofilter;

// Ultra-high-performance T-Digest
pub mod tdigest;

// Ultra-high-performance Top-K Heavy Hitters
pub mod topk;

// Ultra-high-performance Count-Min Sketch
pub mod cms;

// Import all operation modules - each adds methods to Store via impl blocks
pub mod eviction;
pub mod ops;

#[cfg(test)]
mod bptree_bench;

// Re-export the main Store struct
pub use multi_store::{DEFAULT_DB_COUNT, MultiStore};
pub use store::{EncodingConfig, Store};

// Re-export commonly used types
pub use types::{
    Aggregation, CompactionRule, DataType, DuplicatePolicy, Entry, HyperLogLogData, SortedSetData,
    StreamData, StreamId, TimeSeriesInfo, VectorQuantization,
};
pub use value::now_ms;

// Re-export high-performance HyperLogLog
pub use hyperloglog::HyperLogLog;

// Re-export high-performance TimeSeries (Gorilla compression)
pub use timeseries::TimeSeries as CompressedTimeSeries;

// Re-export high-performance B+Tree
pub use bptree::BPTree;

// Re-export geo utilities
pub use ops::geo_ops::{GeoResult, from_meters, geohash_to_string};
