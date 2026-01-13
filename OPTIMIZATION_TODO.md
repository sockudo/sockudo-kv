# Optimization TODO

This document tracks remaining performance optimization opportunities for Sockudo-KV.

## Current State (as of latest benchmarks)

### BPTree (used for SortedSets)

| Operation | BPTree | BTreeSet | Ratio | Status |
|-----------|--------|----------|-------|--------|
| ZADD (Insert) | 186ms (Random), 63ms (Seq) | 114ms (Random), 52ms (Seq) | ~1.6x slower (Random) | ⚠️ Needs optimization |
| ZRANK | 9.7ms (Seq Get equivalent) | 37ms | **3.8x faster** | ✅ Excellent |
| ZRANGE by index | 9.7ms (Seq Get equivalent) | 37ms | **3.8x faster** | ✅ Excellent |
| ZRANGEBYSCORE | 9.7ms (Seq Get equivalent) | 37ms | **3.8x faster** | ✅ Excellent |
| ZREM (Remove) | 158ms | 125ms | 1.2x slower | ✅ Good enough |

### BTreeMap (used for Streams, TimeSeries)

Currently using Rust's highly optimized `std::collections::BTreeMap` - no custom optimizations needed.

---

## High Priority Optimizations

### 1. BPTree Insert Performance (ZADD)

**Current:** 1.9x slower than BTreeSet  
**Target:** Match or beat BTreeSet

#### Ideas:
- [ ] **Bulk insert optimization**: When inserting multiple items, sort them first and insert in order
- [ ] **Lazy splitting**: Delay node splits until absolutely necessary
- [ ] **SIMD binary search**: Use SIMD instructions for searching within nodes (requires `packed_simd` or `std::simd`)
- [ ] **Prefetching**: Add prefetch hints when traversing tree levels
- [ ] **Node pooling**: Pre-allocate nodes in chunks to reduce allocation overhead

### 2. BPTree Remove Performance (ZREM)

**Current:** 5.9x slower than BTreeSet  
**Target:** Within 2x of BTreeSet

#### Ideas:
- [ ] **Lazy rebalancing**: Mark nodes as "soft deleted" and rebalance in batches during idle time
- [ ] **Skip rebalancing for small trees**: Trees with < 1000 items may not benefit from strict balancing
- [ ] **Tombstone markers**: For high-churn workloads, use tombstones instead of immediate removal
- [ ] **Batch remove API**: `remove_range()` that removes multiple items efficiently
- [ ] **Merge on read**: Defer merging until the next read operation touches the affected nodes

### 3. First/Last Access

**Current:** 101µs (with caching)  
**Target:** < 1µs (true O(1))

#### Ideas:
- [ ] **Store first/last key-value directly**: Cache the actual K,V not just leaf index
- [ ] **Update cache on every mutation**: Keep cache always valid, avoid any traversal

---

## Medium Priority Optimizations

### 4. Memory Layout Improvements

- [ ] **Arena allocation**: Allocate all nodes from a contiguous arena for better cache locality
- [ ] **Node size tuning**: Benchmark different node sizes (currently 256 bytes, try 512, 1024)
- [ ] **Separate key/value storage**: Store keys inline, values in separate Vec (better cache for searches)
- [ ] **Compressed pointers**: Use u32 indices instead of usize on 64-bit systems

### 5. Iterator Optimizations

- [ ] **Leaf-linked iteration**: Add next/prev pointers between leaf nodes for O(1) sequential access
- [ ] **Cursor API**: Stateful cursor that can be reused across multiple iterations
- [ ] **Parallel iteration**: `par_iter()` using rayon for large range queries

### 6. Concurrency Improvements

- [ ] **Lock-free reads**: Allow concurrent reads without locking (copy-on-write or epoch-based)
- [ ] **Fine-grained locking**: Lock individual nodes instead of entire tree
- [ ] **Read-write lock per level**: Different locks for different tree levels

---

## Low Priority / Future Optimizations

### 7. Specialized Variants

- [ ] **BPTreeSet**: Variant without values for pure set operations (saves memory)
- [ ] **IntBPTree**: Specialized version for integer keys with faster comparison
- [ ] **StringBPTree**: Specialized version for string keys with prefix compression

### 8. Persistence Optimizations

- [ ] **Memory-mapped nodes**: mmap-based storage for persistence without serialization
- [ ] **Copy-on-write snapshots**: Efficient snapshots for RDB saves
- [ ] **Write-ahead log integration**: WAL-friendly mutation batching

### 9. Monitoring & Profiling

- [ ] **Tree statistics**: Track tree height, fill factor, rebalance frequency
- [ ] **Hot path identification**: Instrument to find most accessed nodes
- [ ] **Automatic tuning**: Adjust node size based on workload characteristics

---

## Benchmarking Notes

### Running Benchmarks

```bash
# SortedSet (ZSET) operations
cargo test bench_bptree_vs_btreeset_sorted_sets --release -- --nocapture

# Stream operations  
cargo test bench_bptree_vs_btreemap_streams --release -- --nocapture

# TimeSeries operations
cargo test bench_chunkindex_performance --release -- --nocapture
```

### Key Metrics to Track

1. **Throughput**: Operations per second
2. **Latency**: P50, P99, P999 latencies
3. **Memory**: Bytes per entry, fragmentation ratio
4. **Scalability**: Performance at 1K, 100K, 10M entries

---

## References

- [Dragonfly BPTree Implementation](https://github.com/dragonflydb/dragonfly/blob/main/src/core/bptree_set.h)
- [Facebook's F14 Map](https://github.com/facebook/folly/blob/main/folly/container/F14Map.h) - SIMD hash map techniques
- [TLX B+ Tree](https://github.com/tlx/tlx/blob/master/tlx/container/btree.hpp) - High-performance C++ B+ tree
- [Rust std BTreeMap](https://doc.rust-lang.org/src/alloc/collections/btree/map.rs.html) - Reference implementation

---

## Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2024-XX-XX | Use BPTree for SortedSets | 891x faster ZRANK, 1563x faster ZRANGE by index |
| 2024-XX-XX | Use BTreeMap for Streams | 2-5x faster for all Stream operations |
| 2024-XX-XX | Use BTreeMap for TimeSeries | 4.3x faster range queries than BPTree |
| 2024-XX-XX | Add first/last leaf caching | 8x improvement in first()/last() |