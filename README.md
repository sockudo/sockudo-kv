# Sockudo-KV

A high-performance, Redis-compatible in-memory database written in Rust with ultra-low latency and lock-free data structures.

## Features

### Core Data Types
- **Strings** - GET, SET, APPEND, INCR/DECR, GETEX, SETEX, SETNX, MGET, MSET
- **Lists** - LPUSH, RPUSH, LPOP, RPOP, LRANGE, LINDEX, LLEN, LMOVE, LPOS
- **Hashes** - HSET, HGET, HMSET, HGETALL, HINCRBY, HDEL, HEXISTS, HSCAN
- **Sets** - SADD, SREM, SMEMBERS, SISMEMBER, SINTER, SUNION, SDIFF, SCARD
- **Sorted Sets** - ZADD, ZRANGE, ZRANGEBYSCORE, ZRANK, ZINCRBY, ZREM, ZSCORE
- **HyperLogLog** - PFADD, PFCOUNT, PFMERGE
- **Bitmaps** - SETBIT, GETBIT, BITCOUNT, BITOP, BITPOS, BITFIELD

### Advanced Data Types
- **Streams** - XADD, XREAD, XRANGE, XGROUP, XREADGROUP, XACK, XCLAIM, XPENDING
- **JSON** - JSON.SET, JSON.GET, JSON.DEL, JSON.ARRAPPEND, JSON.OBJKEYS
- **TimeSeries** - TS.CREATE, TS.ADD, TS.RANGE, TS.MRANGE, TS.GET, TS.INFO
- **Vector Sets** - VADD, VSIM, VREM, VCARD, VDIM, VINFO (HNSW similarity search)
- **Search (RediSearch)** - FT.CREATE, FT.SEARCH, FT.AGGREGATE, FT.DROPINDEX

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
│   ├── storage/
│   │   ├── mod.rs           # Storage layer entry
│   │   ├── types.rs         # Data type definitions
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

# Example results on modern hardware:
# PING_INLINE: 450,000 requests per second
# SET: 380,000 requests per second
# GET: 420,000 requests per second
# INCR: 400,000 requests per second
# LPUSH: 350,000 requests per second
# LRANGE_100: 180,000 requests per second
```

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.
