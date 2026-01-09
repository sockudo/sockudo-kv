# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Sockudo-KV is a high-performance, Redis-compatible in-memory database written in Rust. It implements 100+ Redis commands across 10+ data types using lock-free data structures and zero-copy parsing.

## Build & Run Commands

```bash
# Build
cargo build --release

# Run server (default port 6379)
./target/release/sockudo-kv
cargo run --release

# Run with options
./target/release/sockudo-kv --port 6380 --requirepass mypassword

# Run tests
cargo test
cargo test --release

# Benchmark (requires redis-benchmark tool)
redis-benchmark -p 6379 -q -n 100000 -c 50
```

## Architecture

### Layered Design

```
Network (main.rs) → Protocol (protocol/) → Commands (commands/) → Storage (storage/)
```

1. **Network Layer** (`main.rs`): TCP listener with Tokio async runtime, 64KB buffers, command batching
2. **Protocol Layer** (`protocol/`): Zero-copy RESP2/RESP3 parser using `Bytes` and `memchr`
3. **Command Layer** (`commands/`): Dispatcher routes to type-specific handlers
4. **Storage Layer** (`storage/`): DashMap-backed store with modular ops files

### Key Modules

| Path | Purpose |
|------|---------|
| `src/commands/mod.rs` | Command dispatcher - routes commands to handlers |
| `src/storage/store.rs` | Core DashMap store with generic operations |
| `src/storage/types.rs` | Entry and DataType definitions |
| `src/storage/ops/*.rs` | Type-specific operations (one file per data type) |
| `src/protocol/parser.rs` | Zero-copy RESP parser |

### Adding New Commands

1. Add storage operation to `src/storage/ops/<type>_ops.rs`
2. Add command handler to `src/commands/<type>.rs`
3. Register in the `execute()` function of that module

No monolithic files to modify - each data type has isolated modules.

## Key Design Patterns

- **Zero-copy**: Use `Bytes` for keys/values, avoid cloning until necessary
- **Lock-free**: DashMap sharding (CPU cores × 4) provides concurrent access
- **Entry API**: Single hash lookup instead of get-then-set
- **Fast paths**: Simple SET/GET bypass option parsing
- **Lazy cloning**: Clone only after validation passes

## Concurrency Model

- DashMap with per-shard locks (not global lock)
- Tokio multi-threaded work-stealing scheduler
- One async task per connection
- Atomic operations use `Ordering::Relaxed` for non-critical counters

## Dependencies

Key crates to understand:
- `dashmap` - Lock-free concurrent HashMap
- `bytes` - Zero-copy byte buffers
- `sonic-rs` - High-performance JSON
- `mlua` - Lua scripting (EVAL/FCALL commands)
- `mimalloc` - Global allocator

## Clippy Allowances

The codebase intentionally allows in `lib.rs`:
- `too_many_arguments` - Redis API design requires many parameters
- `type_complexity` - Redis data structures are inherently complex
- `large_enum_variant` - DataType enum variants vary in size by design

## Testing

Tests are inline with `#[test]` attribute. Key test locations:
- `src/storage/ops/generic_ops.rs` - Generic operation tests
- `src/storage/eviction.rs` - Eviction policy tests
- `src/commands/connection.rs` - Connection handling tests

Run a specific test:
```bash
cargo test test_name
cargo test --release test_name
```

## Release Profile

```toml
lto = true           # Link-time optimization
codegen-units = 1    # Single codegen unit
panic = "abort"      # No unwinding
opt-level = 3        # Maximum optimization
```

## Connect with redis-cli

```bash
redis-cli -p 6379
SET key value
GET key
```
