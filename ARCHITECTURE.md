# sockudo-kv Architecture

## Overview

sockudo-kv is a high-performance, Redis-compatible key-value store built in Rust. It uses a modular architecture to keep the codebase maintainable as new commands are added.

## Core Design Principles

1. **Modularity** - Operations are organized by data type, not in one monolithic file
2. **Zero-copy** - Use `Bytes` for efficient memory management
3. **Lock-free** - DashMap provides concurrent access without explicit locking
4. **Performance** - Fast paths for common operations, minimal allocations

---

## Directory Structure

```
src/
├── main.rs                    # Server entry point, event loop
├── lib.rs                     # Public API exports
├── error.rs                   # Error types
├── commands/                  # Command handlers (protocol layer)
│   ├── mod.rs                 # Command dispatcher
│   ├── string.rs              # String commands (SET, GET, etc.)
│   ├── list.rs                # List commands (LPUSH, RPOP, etc.)
│   ├── hash.rs                # Hash commands (HSET, HGET, etc.)
│   ├── set.rs                 # Set commands (SADD, SREM, etc.)
│   ├── sorted_set.rs          # Sorted set commands (ZADD, ZRANGE, etc.)
│   └── hyperloglog.rs         # HyperLogLog commands (PFADD, PFCOUNT, etc.)
├── protocol/                  # RESP protocol parsing
│   ├── mod.rs
│   ├── parser.rs              # Zero-copy RESP parser
│   └── types.rs               # RESP value types
└── storage/                   # Storage layer
    ├── mod.rs                 # Storage module exports
    ├── store.rs               # Main Store (legacy, to be refactored)
    ├── store_core.rs          # Core Store with generic operations only
    ├── types.rs               # Data types (String, List, Hash, etc.)
    ├── value.rs               # Time utilities
    └── ops/                   # Type-specific operations (NEW!)
        ├── mod.rs
        ├── string_ops.rs      # String operations (get, set, incr, etc.)
        ├── list_ops.rs        # List operations (lpush, lpop, etc.)
        ├── hash_ops.rs        # Hash operations (hset, hget, etc.)
        ├── set_ops.rs         # Set operations (sadd, srem, etc.)
        ├── sorted_set_ops.rs  # Sorted set operations
        └── hyperloglog_ops.rs # HyperLogLog operations
```

---

## Layer Architecture

### 1. Network Layer (`main.rs`)

```
TCP Connection → Tokio async runtime
                 ↓
         Read buffer (BytesMut)
                 ↓
         Command batching
                 ↓
         Write buffer (Vec<u8>)
                 ↓
         Single write per batch
```

**Key optimizations:**
- TCP_NODELAY enabled
- 64KB read/write buffers
- Batch processing before writing
- Buffer reuse with growth limits

---

### 2. Protocol Layer (`protocol/`)

```
Raw bytes → Parser::parse() → RespValue → Command::from_resp() → Command
```

**Components:**

- **Parser** - Zero-copy RESP protocol parser using `memchr` for fast CRLF scanning
- **RespValue** - Enum representing RESP types (SimpleString, BulkString, Array, etc.)
- **Command** - Parsed command with name and arguments

**Key optimizations:**
- Zero-copy parsing using `Bytes`
- Fast pointer comparison for static responses (OK, PONG)
- Case-insensitive command matching without allocation

---

### 3. Command Layer (`commands/`)

```
Command → Dispatcher → Type-specific handler → Storage operation → RespValue
```

**Dispatcher** routes commands to appropriate handlers:
- Generic commands (DEL, EXISTS, EXPIRE, etc.)
- Type-specific commands (delegated to modules)

**Command handlers:**
- Parse arguments
- Validate input
- Call storage operations
- Return RESP responses

**Key optimizations:**
- Fast path for simple SET (bypasses option parsing)
- Zero-allocation argument parsing using `eq_ignore_ascii_case()`
- Lazy cloning (only when operation will succeed)

---

### 4. Storage Layer (`storage/`)

The storage layer is **modular** to prevent the Store from becoming a monolithic "god object".

#### Old Design (Problem):

```rust
// store.rs - 1123 lines and growing!
impl Store {
    pub fn set(...) { }
    pub fn get(...) { }
    pub fn lpush(...) { }
    pub fn lpop(...) { }
    pub fn hset(...) { }
    pub fn hget(...) { }
    // ... hundreds more methods ...
}
```

#### New Design (Solution):

```rust
// store_core.rs - ~150 lines, stable
impl Store {
    // Only core operations that work on any type
    pub fn exists(...) { }
    pub fn del(...) { }
    pub fn expire(...) { }
    pub fn persist(...) { }
    // ... ~10 generic methods total
}

// ops/string_ops.rs - ~350 lines
impl Store {
    // String-specific operations
    pub fn get(...) { }
    pub fn set(...) { }
    pub fn incr(...) { }
    // ... only string methods
}

// ops/list_ops.rs - ~270 lines
impl Store {
    // List-specific operations
    pub fn lpush(...) { }
    pub fn lpop(...) { }
    // ... only list methods
}

// ops/hash_ops.rs
// ops/set_ops.rs
// ops/sorted_set_ops.rs
// ... etc
```

---

## Module Organization Benefits

### ✅ Maintainability
- Each file is <400 lines
- Easy to find operations
- Clear separation of concerns

### ✅ Scalability
- Adding new commands doesn't bloat existing files
- New data types get their own module
- Independent testing per module

### ✅ Collaboration
- Multiple developers can work on different data types without conflicts
- Clear ownership boundaries
- Easier code review

### ✅ Compilation
- Smaller compilation units
- Better incremental compilation
- Faster IDE tooling

---

## Data Flow Example: SET Command

```
1. Client sends: *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n

2. Network layer (main.rs):
   - Read into BytesMut buffer
   - Parse complete command

3. Protocol layer (protocol/parser.rs):
   - Parser::parse() → RespValue::Array([...])
   - Command::from_resp() → Command { name: "SET", args: ["key", "value"] }

4. Command layer (commands/mod.rs):
   - Dispatcher::execute()
   - Identifies "SET" command
   - Routes to string::execute()

5. Command handler (commands/string.rs):
   - cmd_set() parses options
   - Fast path: args.len() == 2 → directly call store.set()
   - No option parsing overhead!

6. Storage layer (storage/ops/string_ops.rs):
   - Store::set() implementation
   - Uses DashMap entry API
   - In-place update if already string type
   - Single hash lookup

7. Response:
   - Returns RespValue::ok()
   - Fast path: pointer comparison → "+OK\r\n"
   - Write to buffer, flush to client
```

---

## Concurrency Model

### DashMap Sharding
- Internally sharded HashMap (default: CPU cores * 4)
- Lock per shard, not global lock
- Read/write operations are concurrent
- Minimal contention under high load

### Tokio Async Runtime
- Multi-threaded work-stealing scheduler
- One task per connection
- Automatic load balancing
- No manual thread management

### Lock-free Operations
- DashMap provides lock-free reads
- Atomic operations for key counting
- Expiration checks use Ordering::Relaxed

---

## Memory Management

### Bytes Type
- Reference-counted byte buffer
- Cheap cloning (just increment refcount)
- Zero-copy slicing
- Used for keys and values

### Buffer Reuse
- Read buffer: BytesMut with pre-allocated capacity
- Write buffer: Vec<u8> with capacity checks
- Prevents frequent reallocations
- Grows to 4x limit before reset

### Allocation Strategy
- Pre-allocate common sizes (64KB buffers)
- Reuse buffers across requests
- Minimal heap allocations in hot path

---

## Performance Optimizations

### Fast Paths
1. **Simple SET** - Bypasses option parsing
2. **Static responses** - Pointer comparison for OK/PONG
3. **In-place updates** - String values updated without reallocation
4. **Entry API** - Single hash lookup instead of double

### Zero-Allocation Patterns
1. **Case-insensitive comparison** - No uppercase conversion
2. **Command matching** - Direct byte comparison
3. **Argument parsing** - In-place checks without copies

### Memory Efficiency
1. **Lazy cloning** - Clone only when needed
2. **Buffer reuse** - Clear instead of reallocate
3. **Bytes slicing** - Share underlying buffer

---

## Adding New Commands

### 1. Choose the appropriate module

For a new string command like `GETEX`:

**File:** `src/storage/ops/string_ops.rs`

```rust
impl Store {
    #[inline]
    pub fn getex(&self, key: &[u8], expire_ms: Option<i64>) -> Option<Bytes> {
        match self.data.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    return None;
                }
                if let Some(ms) = expire_ms {
                    entry.set_expire_in(ms);
                }
                entry.data.as_string().cloned()
            }
            None => None,
        }
    }
}
```

### 2. Add command handler

**File:** `src/commands/string.rs`

```rust
fn cmd_getex(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("GETEX"));
    }
    
    let key = &args[0];
    let expire_ms = parse_expire_options(&args[1..])?;
    
    match store.getex(key, expire_ms) {
        Some(val) => Ok(RespValue::bulk(val)),
        None => Ok(RespValue::null()),
    }
}
```

### 3. Register in execute()

**File:** `src/commands/string.rs`

```rust
pub fn execute(store: &Store, cmd: &[u8], args: &[Bytes]) -> Result<RespValue> {
    match cmd {
        b"GET" => cmd_get(store, args),
        b"GETEX" => cmd_getex(store, args),  // Add here
        b"SET" => cmd_set(store, args),
        // ...
    }
}
```

**That's it!** No need to touch a massive 1000+ line file.

---

## Testing Strategy

### Unit Tests
- Test each operation independently
- Mock-free (use real Store instance)
- Focus on edge cases (expiration, wrong types, etc.)

### Integration Tests
- Full command flow (parse → execute → response)
- redis-benchmark for performance
- Comparison with Redis behavior

### Benchmarks
- Criterion for micro-benchmarks
- redis-benchmark for macro-benchmarks
- Compare against Redis baseline

---

## Future Improvements

### 1. Further Modularization
- Split large command files (e.g., `string.rs` could be split)
- Trait-based operations for polymorphism
- Plugin architecture for custom commands

### 2. Performance
- SIMD for string operations
- Custom allocator (jemalloc/mimalloc)
- Profile-guided optimization
- Lock-free response batching

### 3. Features
- Persistence (RDB/AOF)
- Replication
- Cluster mode
- Pub/Sub
- Streams (full implementation)

---

## Build Configuration

### Release Profile

```toml
[profile.release]
lto = true              # Link-time optimization
codegen-units = 1       # Single codegen unit for better optimization
panic = "abort"         # Smaller binary, faster unwinding
opt-level = 3           # Maximum optimization
```

### Cargo Features (Future)

```toml
[features]
default = ["jemalloc"]
jemalloc = ["dep:tikv-jemallocator"]
persistence = ["dep:serde"]
cluster = ["dep:tokio-cluster"]
```

---

## Metrics & Observability (Future)

### Planned Metrics
- Commands per second
- Average latency (p50, p95, p99)
- Memory usage
- Key count by type
- Expiration hits/misses

### Logging
- Structured logging with `tracing`
- Log levels: ERROR, WARN, INFO, DEBUG, TRACE
- Async logging to avoid blocking

---

## Summary

The modular architecture ensures sockudo-kv remains maintainable as it grows:

- **Core Store** (~150 lines) - Generic operations only
- **Type Modules** (~300 lines each) - Specific operations
- **Command Handlers** - Parse and validate
- **Protocol Layer** - Zero-copy parsing
- **Network Layer** - Async I/O with batching

Adding new commands is straightforward:
1. Add storage operation to appropriate `ops/*.rs` file
2. Add command handler to appropriate `commands/*.rs` file
3. Register command in execute function

No more 1000+ line files. No more "god objects". Just clean, modular code! 🎉