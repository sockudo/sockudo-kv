# Sockudo-KV

> [!WARNING]
> **Early Development Status**: This project is currently in an early experimental stage of development. Features may be unstable, APIs might change, and it is not yet recommended for production use.

A high-performance, Redis-compatible in-memory database written in Rust with ultra-low latency and lock-free data structures.

## Features

### Core Data Types (Redis-compatible)
- **Strings** - Raw bytes with optional encoding optimizations
  - Commands: GET, SET, APPEND, INCR/DECR, GETEX, SETEX, SETNX, MGET, MSET
  - String operations, bit manipulation, numeric operations
  
- **Lists** - Ordered collections (QuickList with compressed nodes)
  - Commands: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LINDEX, LLEN, LMOVE, LPOS, BLPOP, BRPOP
  - Doubly-linked list with listpack compression for memory efficiency
  
- **Hashes** - Field-value maps with automatic encoding optimization
  - Commands: HSET, HGET, HMSET, HGETALL, HINCRBY, HDEL, HEXISTS, HSCAN
  - Switches between listpack (small) and DashTable (large) automatically
  
- **Sets** - Unordered unique elements with automatic int optimization
  - Commands: SADD, SREM, SMEMBERS, SISMEMBER, SINTER, SUNION, SDIFF, SCARD, SPOP
  - IntSet encoding for integer-only sets, DashSet for generic sets
  
- **Sorted Sets** - Unique elements with scores (B+Tree + HashMap)
  - Commands: ZADD, ZRANGE, ZRANGEBYSCORE, ZRANK, ZINCRBY, ZREM, ZSCORE, ZPOPMIN, ZPOPMAX
  - Listpack for small sets, full B+Tree implementation for large sets
  
- **Bitmaps** - Bit-level operations on strings
  - Commands: SETBIT, GETBIT, BITCOUNT, BITOP, BITPOS, BITFIELD
  - Efficient bit manipulation with SIMD optimizations

### Advanced Data Types

- **Streams** - Append-only log with consumer groups (Redis 5.0+)
  - Commands: XADD, XREAD, XRANGE, XGROUP, XREADGROUP, XACK, XCLAIM, XPENDING, XTRIM
  - Full consumer group support with PEL tracking
  
- **HyperLogLog** - Probabilistic cardinality estimation
  - Commands: PFADD, PFCOUNT, PFMERGE
  - Sparse/dense encoding with 0.81% standard error
  
- **JSON** - RedisJSON-compatible JSON document storage
  - Commands: JSON.SET, JSON.GET, JSON.DEL, JSON.ARRAPPEND, JSON.OBJKEYS, JSON.NUMINCRBY
  - Uses sonic-rs for ultra-fast JSON parsing and manipulation
  
- **TimeSeries** - RedisTimeSeries-compatible time-series data
  - Commands: TS.CREATE, TS.ADD, TS.RANGE, TS.MRANGE, TS.GET, TS.INFO
  - Gorilla compression algorithm with B+Tree indexing
  - SIMD-accelerated aggregations (avg, sum, min, max, count)
  
- **Vector Sets** - HNSW-based similarity search (Redis 8.0 compatible)
  - Commands: VADD, VSIM, VREM, VCARD, VDIM, VINFO, VGETATTR, VSETATTR
  - Hierarchical Navigable Small World graphs for k-NN search
  - Cosine, Euclidean, and dot product similarity metrics
  - Optional quantization (Q8, Binary) for reduced memory

### Probabilistic Data Structures (RedisBloom-compatible, feature-gated)

- **Bloom Filter** - Scalable probabilistic set membership
  - Commands: BF.ADD, BF.MADD, BF.EXISTS, BF.MEXISTS, BF.RESERVE, BF.INFO
  - Auto-scaling with configurable false positive rate
  
- **Cuckoo Filter** - Probabilistic set with deletion support
  - Commands: CF.ADD, CF.ADDNX, CF.COUNT, CF.DEL, CF.EXISTS, CF.INFO
  - Better space efficiency than Bloom for deletion use-cases
  
- **T-Digest** - Streaming quantile estimation
  - Commands: TDIGEST.CREATE, TDIGEST.ADD, TDIGEST.QUANTILE, TDIGEST.CDF, TDIGEST.MERGE
  - Accurate percentile queries on streaming data
  
- **Top-K** - Heavy hitters tracking
  - Commands: TOPK.ADD, TOPK.QUERY, TOPK.COUNT, TOPK.LIST, TOPK.INFO
  - Count-Min Sketch based frequency tracking
  
- **Count-Min Sketch** - Frequency estimation
  - Commands: CMS.INCRBY, CMS.QUERY, CMS.INFO, CMS.MERGE
  - Space-efficient approximate counting

### Full-Text Search (RediSearch-compatible)

- **Search Indexes** - Full-text and secondary indexing
  - Commands: FT.CREATE, FT.SEARCH, FT.AGGREGATE, FT.DROPINDEX, FT.INFO
  - Text, numeric, tag, and geo fields
  - Boolean queries with AND/OR/NOT operators
  - Aggregations with groupby, reduce, and apply

### Server Features
- **Pub/Sub** - SUBSCRIBE, PUBLISH, PSUBSCRIBE, PUNSUBSCRIBE
- **Sharded Pub/Sub** - SSUBSCRIBE, SUNSUBSCRIBE, SPUBLISH (Redis 7.0+)
- **Transactions** - MULTI, EXEC, DISCARD, WATCH, UNWATCH
- **Scripting** - EVAL, EVALSHA, SCRIPT LOAD, FUNCTION LOAD, FCALL
- **Cluster Mode** - CLUSTER NODES, CLUSTER SLOTS, CLUSTER INFO, CLUSTER KEYSLOT
- **Persistence** - RDB snapshots, AOF (append-only file)
- **Replication** - Master-replica setup with SLAVEOF/REPLICAOF
- **ACL** - User management, authentication, permission control

## Performance

Sockudo-KV is engineered for maximum performance:

- **Lock-free data structures** using DashMap/DashSet for concurrent access
- **Zero-copy parsing** with Bytes for minimal allocations
- **SIMD-friendly** auto-vectorized distance calculations for vector search
- **Tokio async runtime** for efficient I/O multiplexing
- **Connection pooling** and efficient buffer management
- **Redis 6.0+ expiration algorithm** with adaptive sampling and expiration index for O(1) key expiration

## Quick Start

### Build from Source

```bash
# Clone the repository
git clone https://github.com/sockudo/sockudo-kv.git
cd sockudo-kv

# Build with optimizations
cargo build --release

# Run the server
./target/release/sockudo-kv
```

### Command Line Options

```
sockudo-kv [OPTIONS]

Options:
  -h, --host <HOST>           Host to bind to [default: 0.0.0.0]
  -p, --port <PORT>           Port to listen on [default: 6379]
  -d, --databases <NUM>       Number of databases [default: 16]
  -r, --requirepass <PASS>    Require password for connections
      --replicaof <HOST PORT> Connect as replica to master
      --cluster-enabled       Enable cluster mode
      --cluster-config-file   Cluster configuration file
```

### Using with redis-cli

```bash
# Connect to server
redis-cli -p 6379

# Basic operations
SET mykey "Hello, World!"
GET mykey
HSET myhash field1 value1 field2 value2
HGETALL myhash

# Vector similarity search
VADD myvectors VALUES 3 0.1 0.2 0.3 element1
VSIM myvectors VALUES 3 0.1 0.2 0.3 COUNT 10

# TimeSeries
TS.CREATE temperature RETENTION 3600000
TS.ADD temperature * 25.5
TS.RANGE temperature - + COUNT 10

# Pub/Sub
SUBSCRIBE mychannel
PUBLISH mychannel "Hello subscribers!"
```

## Configuration

Create a `redis.conf` file for advanced configuration:

```conf
# Network
bind 0.0.0.0
port 6379
tcp-keepalive 300

# Security
requirepass mypassword
protected-mode yes

# Persistence
appendonly yes
appendfilename "appendonly.aof"
save 3600 1
save 300 100

# Replication
replicaof 192.168.1.100 6379
masterauth replicapassword

# Memory
maxmemory 4gb
maxmemory-policy allkeys-lru
```

## Configuration Features

### ✅ Fully Implemented

| Category | Options |
|----------|---------|
| **Network** | `bind`, `port`, `tcp-backlog`, `tcp-keepalive`, `unixsocket`, `timeout` |
| **Security** | `requirepass`, `protected-mode`, `acl-file`, `rename-command` |
| **Persistence** | `save`, `appendonly`, `appendfilename`, `appendfsync`, `rdbcompression`, `rdbchecksum`, `aof-rewrite-incremental-fsync` |
| **Replication** | `replicaof`, `masterauth`, `replica-read-only`, `repl-backlog-size`, `repl-timeout` |
| **Memory** | `maxmemory`, `maxmemory-policy`, `maxmemory-samples`, `active-expire-effort` |
| **Lazy Free** | `lazyfree-lazy-eviction`, `lazyfree-lazy-expire`, `lazyfree-lazy-server-del` |
| **Limits** | `maxclients`, `client-query-buffer-limit`, `proto-max-bulk-len` |
| **LFU Eviction** | `lfu-log-factor`, `lfu-decay-time` |
| **Background Tasks** | `hz`, `dynamic-hz`, `activerehashing` |
| **Latency** | `latency-tracking`, `latency-tracking-info-percentiles`, `slowlog-*` |
| **Defrag** | `activedefrag`, `active-defrag-*` thresholds |
| **TLS** | `tls-port`, `tls-cert-file`, `tls-key-file`, `tls-ca-cert-file`, `tls-auth-clients` |
| **Cluster** | `cluster-enabled`, `cluster-config-file`, `cluster-node-timeout`, `cluster-require-full-coverage` |
| **Pub/Sub** | `notify-keyspace-events` |
| **Scripting** | `lua-time-limit` |
| **Shutdown** | `shutdown-timeout` |
| **Process Mgmt** | `daemonize` (Unix), `supervised` (systemd/upstart/auto) |
| **Connection Rate** | `max-new-connections-per-cycle` |
| **Encoding** | `hash-max-listpack-*`, `list-max-listpack-*`, `set-max-*`, `zset-max-*` |

### ✅ Linux-Specific (under `#[cfg(target_os = "linux")]`)

| Option | Description |
|--------|-------------|
| `oom-score-adj` | OOM killer score adjustment (`no`, `yes`, `relative`, `absolute`) |
| `oom-score-adj-values` | OOM score values for master/replica/bgsave |
| `disable-thp` | Disable Transparent Huge Pages via sysfs + prctl |
| `socket-mark-id` | SO_MARK for outgoing connections |

### ⚠️ Parsed but No-Op (Compatibility)

| Option | Reason |
|--------|--------|
| `io-threads` | Tokio async runtime manages threading |
| `io-threads-do-reads` | Tokio manages I/O |
| `server-cpulist` | Tokio manages CPU affinity |
| `bio-cpulist` | Background I/O handled by Tokio |
| `jemalloc-bg-thread` | Uses mimalloc, not jemalloc |
| `ignore-warnings` | Parsed for compatibility |

### ❌ Not Implemented

| Option | Reason |
|--------|--------|
| `loadmodule` | Module system API not implemented |
| `debug` | Command partially implemented (some subcommands missing) |


## Architecture

```
sockudo-kv/
├── src/
│   ├── main.rs              # Server entry point, TCP handling
│   ├── lib.rs               # Library exports
│   ├── protocol.rs          # RESP2/RESP3 parser
│   ├── error.rs             # Error types
│   ├── cli.rs               # Command line parsing
│   ├── config.rs            # Configuration management
│   ├── pubsub.rs            # Pub/Sub infrastructure
│   ├── lua_engine.rs        # Lua scripting (EVAL, FCALL)
│   ├── cluster_state.rs     # Redis Cluster state
│   ├── client_manager.rs    # Client connection tracking
│   ├── cron.rs              # Background tasks (expiration, LFU decay)
│   ├── storage/
│   │   ├── mod.rs           # Storage layer entry
│   │   ├── store.rs         # Core key-value store
│   │   ├── types.rs         # Data type definitions
│   │   ├── expiration_index.rs  # Redis 6.0+ style expiration tracking
│   │   ├── eviction.rs      # Memory eviction policies (LRU, LFU)
│   │   └── ops/             # Operations by data type
│   │       ├── string_ops.rs
│   │       ├── list_ops.rs
│   │       ├── hash_ops.rs
│   │       ├── set_ops.rs
│   │       ├── sorted_set_ops.rs
│   │       ├── stream_ops.rs
│   │       ├── json_ops.rs
│   │       ├── timeseries_ops.rs
│   │       ├── vector_ops.rs
│   │       ├── search_ops.rs
│   │       └── ...
│   ├── commands/            # Command handlers
│   │   ├── string.rs
│   │   ├── list.rs
│   │   ├── hash.rs
│   │   ├── set.rs
│   │   ├── sorted_set.rs
│   │   ├── stream.rs
│   │   ├── pubsub.rs
│   │   ├── cluster.rs
│   │   └── ...
│   └── replication/
│       ├── rdb.rs           # RDB serialization
│       └── replica.rs       # Replication client
```

## Key Expiration (Redis 6.0+ Compatible)

Sockudo-KV implements the **exact same key expiration algorithm** as Redis 6.0+ and Dragonfly:

### Hybrid Expiration Strategy

1. **Passive Expiration (Lazy Deletion)**
   - When a key is accessed (GET, SET, etc.), check if expired
   - If expired, delete immediately and return null
   - Zero overhead for keys without expiration

2. **Active Expiration (Background Sampling)**
   - Runs 10 times per second (configurable via `hz`)
   - **Algorithm:**
     - Sample 20 random keys that have TTL set
     - Delete all expired keys found
     - **If >25% expired, repeat immediately** (adaptive)
     - Maximum 16 iterations per cycle (safety limit)
   - Time-bounded to prevent blocking

### Expiration Index

Unlike Redis <6.0 which sampled from all keys, Sockudo-KV uses an **expiration index**:

```
BTreeMap<i64, DashSet<Bytes>>
   │         └─ Keys expiring at this time
   └─ Expiration timestamp (ms)
```

**Benefits:**
- **O(1) sampling** - only samples keys with expiration
- **Sorted by time** - enables efficient range queries
- **Concurrent access** - thread-safe with RwLock
- **Auto-cleanup** - empty buckets removed automatically

### Configuration

```conf
# Active expiration effort (1-10, default 1)
# Higher = more CPU time spent on expiration
active-expire-effort 1

# Background task frequency (default 10 Hz)
hz 10

# Dynamic Hz scaling based on client count
dynamic-hz yes

# Lazy freeing on expiration (async deletion)
lazyfree-lazy-expire no
```

### Performance Characteristics

| Metric | Value |
|--------|-------|
| Sampling complexity | O(1) per key |
| Keys sampled per cycle | 20 (Redis default) |
| Adaptive threshold | 25% expired keys |
| Max iterations | 16 per cycle |
| Frequency | 10 Hz (100ms interval) |
| Index overhead | ~24 bytes per key with TTL |

### Expiration Commands

All Redis expiration commands fully supported:

```bash
# Relative expiration
EXPIRE key 60              # Expire in 60 seconds
PEXPIRE key 60000          # Expire in 60000 milliseconds

# Absolute expiration
EXPIREAT key 1735689600    # Expire at Unix timestamp
PEXPIREAT key 1735689600000  # Expire at Unix timestamp (ms)

# Conditional expiration (Redis 7.0+)
EXPIRE key 60 NX           # Set only if no expiration
EXPIRE key 60 XX           # Set only if has expiration
EXPIRE key 60 GT           # Set only if new > current
EXPIRE key 60 LT           # Set only if new < current

# Query expiration
TTL key                    # Get TTL in seconds (-1 = no expiry, -2 = not exists)
PTTL key                   # Get TTL in milliseconds
EXPIRETIME key             # Get absolute expiration time (seconds)
PEXPIRETIME key            # Get absolute expiration time (ms)

# Remove expiration
PERSIST key                # Remove TTL from key
```

### String Operations with Expiration

```bash
# SET with expiration options
SET key value EX 60        # Expire in 60 seconds
SET key value PX 60000     # Expire in 60000 ms
SET key value EXAT 1735689600    # Expire at timestamp
SET key value PXAT 1735689600000 # Expire at timestamp (ms)
SET key value KEEPTTL      # Keep existing TTL

# GETEX - Get with expiration modification
GETEX key EX 60            # Get and set new expiration
GETEX key EXAT 1735689600  # Get and set absolute expiration
GETEX key PERSIST          # Get and remove expiration
```

### Implementation Details

The expiration index is maintained across all operations:

- **SET/HSET/etc with TTL** → Add to index
- **EXPIRE/EXPIREAT** → Update index (remove old, add new)
- **PERSIST** → Remove from index
- **DEL/UNLINK** → Remove from index
- **FLUSHDB/FLUSHALL** → Clear index
- **Passive expiration** → Check on access, remove if expired
- **Active expiration** → Sample from index, adaptive repeat

This ensures O(1) access to keys with expiration and matches Redis 6.0+ behavior exactly.

## Data Type Implementations

Sockudo-KV implements **all 18 data types** with automatic encoding optimizations for memory efficiency:

### String
- **Storage:** `Bytes` - zero-copy byte buffer
- **Encoding:** Raw bytes, automatically converted for numeric operations
- **Use case:** Caching, counters, sessions, binary data

### List
- **Storage:** `QuickList` - doubly-linked list of listpack nodes
- **Encoding:** Compressed listpack nodes (configurable via `list-max-listpack-size`)
- **Complexity:** O(1) push/pop at ends, O(n) for index access
- **Use case:** Queues, activity feeds, recent items

### Hash  
- **Storage:** Dual encoding - `Listpack` (small) or `DashTable` (large)
- **Threshold:** Switches at 512 entries (configurable via `hash-max-listpack-entries`)
- **Complexity:** O(1) field access with DashTable, O(n) with listpack
- **Use case:** Objects, session data, user profiles

### Set
- **Storage:** Dual encoding - `IntSet` (integers) or `DashSet` (generic)
- **Threshold:** IntSet for up to 512 integer-only members
- **Complexity:** O(1) add/remove/check membership
- **Use case:** Tags, unique visitors, relationships

### Sorted Set
- **Storage:** Dual encoding - `Listpack` (small) or `B+Tree + HashMap` (large)
- **Threshold:** Switches at 128 entries (configurable via `zset-max-listpack-entries`)
- **Structure (large):** B+Tree for range queries + HashMap for O(1) score lookup
- **Complexity:** O(log n) insert/delete, O(1) score lookup, O(log n + k) range queries
- **Use case:** Leaderboards, priority queues, time-series indexes

### Stream
- **Storage:** `BTreeMap<StreamId, Vec<(field, value)>>` with consumer group metadata
- **Features:** Auto-generated IDs, consumer groups, pending entries list (PEL)
- **Complexity:** O(log n) insert, O(log n + k) range queries
- **Use case:** Event logs, message queues, audit trails

### HyperLogLog
- **Storage:** Sparse (set of registers) or Dense (16KB register array)
- **Encoding:** Automatically promotes sparse → dense at 3000 bytes
- **Accuracy:** 0.81% standard error with 16,384 registers
- **Complexity:** O(1) add, O(n) count (n = number of HLLs merged)
- **Use case:** Unique visitor counting, cardinality estimation

### Bitmap (String-based)
- **Storage:** Byte array with bit-level operations
- **SIMD:** Auto-vectorized for BITCOUNT, BITPOS
- **Complexity:** O(1) getbit/setbit, O(n) for count/operations
- **Use case:** User permissions, feature flags, bloom filters

### JSON
- **Storage:** `sonic_rs::Value` - ultra-fast JSON parser
- **Features:** JSONPath queries, nested updates, array operations
- **Complexity:** O(1) for top-level keys, O(depth) for nested paths
- **Use case:** Document storage, configuration, complex objects

### TimeSeries
- **Storage:** Gorilla-compressed chunks in B+Tree
- **Compression:** Gorilla algorithm (XOR + delta-of-delta encoding)
- **Indexing:** B+Tree for fast range queries
- **Aggregations:** SIMD-accelerated (avg, sum, min, max, count)
- **Complexity:** O(log n) insert, O(log n + k/c) range (c = compression ratio)
- **Use case:** Metrics, monitoring, IoT sensor data

### VectorSet
- **Storage:** HNSW graph + vector embeddings
- **Index:** Hierarchical Navigable Small World for approximate k-NN
- **Metrics:** Cosine similarity, Euclidean distance, dot product
- **Quantization:** Optional Q8 or binary for 4x-32x memory savings
- **Complexity:** O(log n) search with high recall, O(n) exact search
- **Use case:** Semantic search, recommendation systems, image similarity

### Bloom Filter (feature-gated)
- **Storage:** Scalable bloom filter with multiple layers
- **Features:** Auto-scaling, configurable false positive rate
- **Complexity:** O(k) operations (k = number of hash functions)
- **Use case:** Duplicate detection, cache filtering, spell checking

### Cuckoo Filter (feature-gated)
- **Storage:** Cuckoo hash table with fingerprints
- **Features:** Deletion support, better space efficiency than Bloom
- **Complexity:** O(1) expected add/delete/check
- **Use case:** Set membership with deletions, counting filters

### T-Digest (feature-gated)
- **Storage:** Weighted centroids with merging buffer
- **Features:** Streaming quantile estimation, accurate at extremes
- **Complexity:** O(1) amortized insert, O(log n) quantile query
- **Use case:** Percentile monitoring, latency tracking, SLA monitoring

### Top-K (feature-gated)
- **Storage:** Min-heap + Count-Min Sketch
- **Features:** Heavy hitters tracking with approximate counts
- **Complexity:** O(log k) add, O(k) list
- **Use case:** Trending items, hot keys, abuse detection

### Count-Min Sketch (feature-gated)
- **Storage:** 2D array of counters (width × depth)
- **Features:** Frequency estimation with bounded error
- **Complexity:** O(d) operations (d = depth)
- **Use case:** Frequency counting, rate limiting, analytics

### Search Index (RediSearch)
- **Storage:** Inverted indexes per field type
- **Text:** Tokenized inverted index with position tracking
- **Numeric:** Range tree for numeric fields
- **Tag:** Hash-based exact match index
- **Geo:** Geohash-based spatial index
- **Complexity:** O(log n) for numeric/geo, O(m) for text (m = matching docs)
- **Use case:** Full-text search, faceted search, geo-queries

### IntSet (Internal)
- **Storage:** Sorted array of integers (16/32/64-bit)
- **Encoding:** Automatically upgrades int16 → int32 → int64
- **Complexity:** O(log n) search, O(n) insert (maintains sorted order)
- **Use case:** Internal optimization for integer-only sets

### Listpack (Internal)
- **Storage:** Compact variable-length encoding
- **Features:** Backward traversal, length prefixes
- **Complexity:** O(n) for operations (but very cache-friendly)
- **Use case:** Internal optimization for small hashes/sets/sorted sets

## Supported Commands

### Strings
`GET`, `SET`, `SETNX`, `SETEX`, `PSETEX`, `MGET`, `MSET`, `MSETNX`, `INCR`, `INCRBY`, `INCRBYFLOAT`, `DECR`, `DECRBY`, `APPEND`, `STRLEN`, `GETRANGE`, `SETRANGE`, `GETEX`, `GETDEL`, `GETSET`

### Lists
`LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `LINDEX`, `LSET`, `LINSERT`, `LREM`, `LTRIM`, `LPOS`, `LMOVE`, `BLPOP`, `BRPOP`

### Hashes
`HSET`, `HGET`, `HMSET`, `HMGET`, `HGETALL`, `HDEL`, `HEXISTS`, `HLEN`, `HKEYS`, `HVALS`, `HINCRBY`, `HINCRBYFLOAT`, `HSETNX`, `HSCAN`, `HRANDFIELD`

### Sets
`SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SMISMEMBER`, `SCARD`, `SPOP`, `SRANDMEMBER`, `SINTER`, `SINTERSTORE`, `SUNION`, `SUNIONSTORE`, `SDIFF`, `SDIFFSTORE`, `SMOVE`, `SSCAN`

### Sorted Sets
`ZADD`, `ZREM`, `ZSCORE`, `ZRANK`, `ZREVRANK`, `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`, `ZRANGEBYLEX`, `ZCARD`, `ZCOUNT`, `ZINCRBY`, `ZMPOP`, `ZPOPMIN`, `ZPOPMAX`, `ZRANGESTORE`, `ZUNIONSTORE`, `ZINTERSTORE`, `ZSCAN`, `ZMSCORE`, `ZRANDMEMBER`

### Streams
`XADD`, `XREAD`, `XRANGE`, `XREVRANGE`, `XLEN`, `XINFO`, `XTRIM`, `XDEL`, `XGROUP`, `XREADGROUP`, `XACK`, `XCLAIM`, `XAUTOCLAIM`, `XPENDING`, `XSETID`

### Keys
`DEL`, `EXISTS`, `EXPIRE`, `EXPIREAT`, `EXPIRETIME`, `TTL`, `PTTL`, `PERSIST`, `TYPE`, `KEYS`, `SCAN`, `RENAME`, `RENAMENX`, `COPY`, `DUMP`, `RESTORE`, `OBJECT`, `TOUCH`, `UNLINK`, `SORT`

### Server
`PING`, `ECHO`, `INFO`, `DBSIZE`, `FLUSHDB`, `FLUSHALL`, `SAVE`, `BGSAVE`, `LASTSAVE`, `TIME`, `DEBUG`, `SLOWLOG`, `MEMORY`, `CONFIG`, `ACL`, `CLIENT`, `COMMAND`, `MODULE`

### Connection
`AUTH`, `SELECT`, `QUIT`, `RESET`, `CLIENT ID`, `CLIENT INFO`, `CLIENT SETNAME`, `CLIENT GETNAME`, `CLIENT LIST`, `CLIENT KILL`

### Pub/Sub
`SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBLISH`, `PUBSUB`, `SSUBSCRIBE`, `SUNSUBSCRIBE`, `SPUBLISH`

### Cluster
`CLUSTER NODES`, `CLUSTER SLOTS`, `CLUSTER INFO`, `CLUSTER KEYSLOT`, `CLUSTER MYID`, `CLUSTER MEET`, `CLUSTER ADDSLOTS`, `CLUSTER DELSLOTS`, `CLUSTER SETSLOT`, `CLUSTER REPLICATE`, `CLUSTER FAILOVER`, `CLUSTER BUMPEPOCH`

### Scripting
`EVAL`, `EVALSHA`, `SCRIPT LOAD`, `SCRIPT EXISTS`, `SCRIPT FLUSH`, `FUNCTION LOAD`, `FUNCTION LIST`, `FUNCTION DELETE`, `FUNCTION FLUSH`, `FCALL`, `FCALL_RO`

### Transactions
`MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`

### Geospatial
`GEOADD`, `GEOPOS`, `GEODIST`, `GEOHASH`, `GEORADIUS`, `GEORADIUSBYMEMBER`, `GEOSEARCH`, `GEOSEARCHSTORE`

### HyperLogLog
`PFADD`, `PFCOUNT`, `PFMERGE`

### JSON (RedisJSON compatible)
`JSON.SET`, `JSON.GET`, `JSON.DEL`, `JSON.MGET`, `JSON.TYPE`, `JSON.STRLEN`, `JSON.STRAPPEND`, `JSON.NUMINCRBY`, `JSON.NUMMULTBY`, `JSON.ARRAPPEND`, `JSON.ARRINDEX`, `JSON.ARRINSERT`, `JSON.ARRLEN`, `JSON.ARRPOP`, `JSON.ARRTRIM`, `JSON.OBJKEYS`, `JSON.OBJLEN`, `JSON.TOGGLE`, `JSON.RESP`

### TimeSeries (RedisTimeSeries compatible)
`TS.CREATE`, `TS.ADD`, `TS.MADD`, `TS.DEL`, `TS.GET`, `TS.MGET`, `TS.RANGE`, `TS.MRANGE`, `TS.REVRANGE`, `TS.INFO`, `TS.ALTER`, `TS.CREATERULE`, `TS.DELETERULE`

### Vector (Redis 8.0 Vector Sets)
`VADD`, `VCARD`, `VDEL`, `VDIM`, `VEMB`, `VGETATTR`, `VINFO`, `VISMEMBER`, `VLINKS`, `VRANDMEMBER`, `VRANGE`, `VREM`, `VSETATTR`, `VSIM`

### Search (RediSearch compatible)
`FT.CREATE`, `FT.SEARCH`, `FT.AGGREGATE`, `FT.DROPINDEX`, `FT.INFO`, `FT._LIST`, `FT.ALIASADD`, `FT.ALIASDEL`, `FT.ALIASUPDATE`, `FT.ALTER`, `FT.TAGVALS`, `FT.SYNUPDATE`, `FT.SYNDUMP`

## Dependencies

- **tokio** - Async runtime
- **bytes** - Zero-copy byte buffers
- **dashmap** - Lock-free concurrent hash maps
- **sonic-rs** - High-performance JSON parsing
- **mlua** - Lua scripting engine
- **roaring** - Compressed bitmaps for search
- **fastrand** - Fast random number generation

## Benchmarking

```bash
# Run redis-benchmark against sockudo-kv
redis-benchmark -p 6379 -q -n 100000 -c 50
```

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.
