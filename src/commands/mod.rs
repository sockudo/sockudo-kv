//! Redis command implementations
//!
//! This module contains all Redis-compatible command implementations organized by category.

pub mod bitmap;
pub mod blocking;
pub mod cluster;
pub mod connection;
pub mod generic;
pub mod geo;
pub mod hash;
pub mod hyperloglog;
pub mod list;
pub mod pubsub;
pub mod scripting;
pub mod server;
pub mod set;
pub mod sorted_set;
pub mod stream;
pub mod string;
pub mod transaction;

// Optional modules (enabled via Cargo features)
#[cfg(feature = "json")]
pub mod json;

#[cfg(feature = "search")]
pub mod search;

#[cfg(feature = "timeseries")]
pub mod timeseries;

#[cfg(feature = "vector")]
pub mod vector;

// RedisBloom-like probabilistic data structures (feature-gated)
#[cfg(feature = "bloom")]
pub mod bloom;

#[cfg(feature = "bloom")]
pub mod cms;

#[cfg(feature = "bloom")]
pub mod cuckoo;

#[cfg(feature = "bloom")]
pub mod tdigest;

#[cfg(feature = "bloom")]
pub mod topk;

use crate::error::{Error, Result};
use crate::protocol::{Command, RespValue};
use crate::replication::ReplicationManager;
use crate::server_state::ServerState;
use crate::storage::{MultiStore, Store};
use bytes::Bytes;
use std::sync::Arc;

// FFI for mimalloc memory stats
unsafe extern "C" {
    fn mi_process_info(
        elapsed_msecs: *mut usize,
        user_msecs: *mut usize,
        system_msecs: *mut usize,
        current_rss: *mut usize,
        peak_rss: *mut usize,
        current_commit: *mut usize,
        peak_commit: *mut usize,
        page_faults: *mut usize,
    );
}

/// Get memory statistics from mimalloc allocator
/// Returns (used_memory, peak_rss, current_rss)
fn get_mimalloc_memory_stats() -> (usize, usize, usize) {
    let mut elapsed_msecs: usize = 0;
    let mut user_msecs: usize = 0;
    let mut system_msecs: usize = 0;
    let mut current_rss: usize = 0;
    let mut peak_rss: usize = 0;
    let mut current_commit: usize = 0;
    let mut peak_commit: usize = 0;
    let mut page_faults: usize = 0;

    unsafe {
        mi_process_info(
            &mut elapsed_msecs,
            &mut user_msecs,
            &mut system_msecs,
            &mut current_rss,
            &mut peak_rss,
            &mut current_commit,
            &mut peak_commit,
            &mut page_faults,
        );
    }

    // current_commit is the total bytes allocated (used_memory equivalent)
    // current_rss is the resident set size
    // peak_rss is the peak resident set size
    (current_commit, peak_rss, current_rss)
}

/// Format memory size in human-readable format
fn format_memory_human(bytes: usize) -> String {
    const KB: usize = 1024;
    const MB: usize = KB * 1024;
    const GB: usize = MB * 1024;

    if bytes >= GB {
        format!("{:.2}G", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2}M", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2}K", bytes as f64 / KB as f64)
    } else {
        format!("{}B", bytes)
    }
}

/// Command dispatcher - routes commands to appropriate handlers
pub struct Dispatcher;

impl Dispatcher {
    /// Execute a command with full server state (for external commands)
    #[inline]
    pub fn execute(
        multi_store: &Arc<MultiStore>,
        store: &Store,
        server: &Arc<ServerState>,
        replication: Option<&Arc<ReplicationManager>>,
        client: Option<&crate::client::ClientState>,
        cmd: Command,
    ) -> RespValue {
        match Self::execute_inner(
            Some(multi_store),
            store,
            Some(server),
            replication,
            client,
            &cmd,
        ) {
            Ok(resp) => resp,
            Err(e) => RespValue::error(&e.to_string()),
        }
    }

    /// Execute a command without server state (for Lua scripts and transactions)
    /// Server-specific commands will return an error in this mode
    #[inline]
    pub fn execute_basic(store: &Store, cmd: Command) -> RespValue {
        match Self::execute_inner(None, store, None, None, None, &cmd) {
            Ok(resp) => resp,
            Err(e) => RespValue::error(&e.to_string()),
        }
    }

    fn execute_inner(
        multi_store: Option<&Arc<MultiStore>>,
        store: &Store,
        server: Option<&Arc<ServerState>>,
        replication: Option<&Arc<ReplicationManager>>,
        client: Option<&crate::client::ClientState>,
        cmd: &Command,
    ) -> Result<RespValue> {
        let args = &cmd.args;
        let cmd_name = cmd.name();

        // Cluster Redirection Logic (if enabled)
        if let Some(state) = server
            && state.config.read().cluster_enabled
            && !cmd.args.is_empty()
        {
            // Check if command is key-based.
            // For MVP, we assume first arg is key if it's not a server/cluster/script command.
            // In production, we should look up command table.

            // Skip if command is in a strict allowlist for random nodes (e.g. INFO, PING, etc)
            // or if it's already handled elsewhere (like CLUSTER command).
            let skip = is_server_command(cmd_name)
                || is_cluster_protocol_command(cmd_name)
                || cmd.is_command(b"INFO")
                || cmd.is_command(b"DBSIZE")
                || cmd.is_command(b"KEYS");

            if !skip {
                // Determine if this is a write command or pubsub shard command
                let is_write = is_write_command(cmd_name);
                let is_pubsub_shard = cmd.is_command(b"SPUBLISH")
                    || cmd.is_command(b"SSUBSCRIBE")
                    || cmd.is_command(b"SUNSUBSCRIBE");

                // Read cluster config once
                let (require_full_coverage, allow_reads_when_down, allow_pubsubshard_when_down) = {
                    let config = state.config.read();
                    (
                        config.cluster_require_full_coverage,
                        config.cluster_allow_reads_when_down,
                        config.cluster_allow_pubsubshard_when_down,
                    )
                };

                // Check if cluster is healthy and command is allowed
                if let Err(err_msg) = state.cluster.should_allow_command(
                    is_write,
                    is_pubsub_shard,
                    require_full_coverage,
                    allow_reads_when_down,
                    allow_pubsubshard_when_down,
                ) {
                    return Ok(RespValue::error(err_msg));
                }

                // Calculate hashing slot for first key
                // Note: MGET/MSET etc might have multiple. We check first for now.
                let key = &args[0];
                let slot = crate::cluster::message::key_slot(key);

                // Cross-slot validation for multi-key commands
                let is_all_keys =
                    cmd.is_command(b"MGET") || cmd.is_command(b"DEL") || cmd.is_command(b"EXISTS");
                let is_pair_keys = cmd.is_command(b"MSET") || cmd.is_command(b"MSETNX");

                if is_all_keys {
                    for arg in args.iter().skip(1) {
                        if crate::cluster::message::key_slot(arg) != slot {
                            return Ok(RespValue::error(
                                "CROSSSLOT Keys in request don't hash to the same slot",
                            ));
                        }
                    }
                } else if is_pair_keys {
                    for i in (2..args.len()).step_by(2) {
                        if crate::cluster::message::key_slot(&args[i]) != slot {
                            return Ok(RespValue::error(
                                "CROSSSLOT Keys in request don't hash to the same slot",
                            ));
                        }
                    }
                }

                if let Some(target) = state.cluster.get_slot_owner(slot) {
                    // If target is not me
                    if target != state.cluster.my_id {
                        // Check for ASKING (implement later if needed, assume MOVED for now)

                        // Find IP:Port of target
                        if let Some(node) = state.cluster.nodes.get(&target) {
                            let ip = &node.ip;
                            let port = node.port;
                            let err_msg = format!("MOVED {} {}:{}", slot, ip, port);
                            return Ok(RespValue::error(&err_msg));
                        } else {
                            // Unknown node?
                            // Fallthrough or error?
                            // Maybe it's us but ID mismatch? Unlikely.
                        }
                    }
                } else {
                    // Slot unassigned
                    return Ok(RespValue::error("CLUSTERDOWN Hash slot not served"));
                }
            }
        }

        // Server commands (need ServerState) - includes CONFIG now
        if is_server_command(cmd_name) {
            return match server {
                Some(srv) => server::execute(multi_store, store, srv, cmd_name, args),
                None => Err(Error::Custom(
                    "ERR server commands not available in this context".to_string(),
                )),
            };
        }

        // Info command
        if cmd.is_command(b"INFO") {
            return Ok(cmd_info(store, args, server));
        }

        // Generic commands
        if cmd.is_command(b"DEL") {
            // Check if lazyfree_lazy_user_del is enabled
            let use_lazy = server
                .map(|s| {
                    s.lazyfree_lazy_user_del
                        .load(std::sync::atomic::Ordering::Relaxed)
                })
                .unwrap_or(false);
            let result = cmd_del(store, args, use_lazy);
            // Propagate DEL to replicas
            if let Ok(ref resp) = result {
                if !matches!(resp, RespValue::Error(_)) {
                    if let Some(repl) = replication {
                        let mut parts = Vec::with_capacity(1 + args.len());
                        parts.push(Bytes::from_static(b"DEL"));
                        parts.extend_from_slice(args);
                        repl.propagate(&parts);
                    }
                }
            }
            return result;
        }
        if cmd.is_command(b"EXISTS") {
            return cmd_exists(store, args);
        }
        if cmd.is_command(b"EXPIRE") {
            let result = cmd_expire(store, args);
            // Propagate EXPIRE (convert to PEXPIREAT for consistency)
            if let Ok(RespValue::Integer(1)) = result {
                if let Some(repl) = replication {
                    // Convert EXPIRE to PEXPIREAT with absolute timestamp
                    if let Ok(seconds) = std::str::from_utf8(&args[1])
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
                        .ok_or(())
                    {
                        let expire_at = crate::storage::now_ms() + seconds * 1000;
                        let parts = vec![
                            Bytes::from_static(b"PEXPIREAT"),
                            args[0].clone(),
                            Bytes::from(expire_at.to_string()),
                        ];
                        repl.propagate(&parts);
                    }
                }
            }
            return result;
        }
        if cmd.is_command(b"PEXPIRE") {
            let result = cmd_pexpire(store, args);
            // Propagate PEXPIRE (convert to PEXPIREAT for consistency)
            if let Ok(RespValue::Integer(1)) = result {
                if let Some(repl) = replication {
                    if let Ok(ms) = std::str::from_utf8(&args[1])
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
                        .ok_or(())
                    {
                        let expire_at = crate::storage::now_ms() + ms;
                        let parts = vec![
                            Bytes::from_static(b"PEXPIREAT"),
                            args[0].clone(),
                            Bytes::from(expire_at.to_string()),
                        ];
                        repl.propagate(&parts);
                    }
                }
            }
            return result;
        }
        if cmd.is_command(b"TTL") {
            return cmd_ttl(store, args);
        }
        if cmd.is_command(b"PTTL") {
            return cmd_pttl(store, args);
        }
        if cmd.is_command(b"PERSIST") {
            let result = cmd_persist(store, args);
            // Propagate PERSIST
            if let Ok(RespValue::Integer(1)) = result {
                if let Some(repl) = replication {
                    let parts = vec![Bytes::from_static(b"PERSIST"), args[0].clone()];
                    repl.propagate(&parts);
                }
            }
            return result;
        }
        if cmd.is_command(b"TYPE") {
            return cmd_type(store, args);
        }
        if cmd.is_command(b"DBSIZE") {
            return Ok(RespValue::integer(store.len() as i64));
        }
        if cmd.is_command(b"FLUSHDB") || cmd.is_command(b"FLUSHALL") {
            // Check if lazyfree_lazy_user_flush is enabled
            let use_lazy = server
                .map(|s| {
                    s.lazyfree_lazy_user_flush
                        .load(std::sync::atomic::Ordering::Relaxed)
                })
                .unwrap_or(false);
            if use_lazy {
                store.lazy_flush();
            } else {
                store.flush();
            }
            // Propagate FLUSHDB/FLUSHALL
            if let Some(repl) = replication {
                let cmd_bytes = if cmd.is_command(b"FLUSHALL") {
                    Bytes::from_static(b"FLUSHALL")
                } else {
                    Bytes::from_static(b"FLUSHDB")
                };
                repl.propagate(&[cmd_bytes]);
            }
            return Ok(RespValue::ok());
        }

        // New generic commands (COPY, DUMP, RESTORE, KEYS, SCAN, RENAME, etc.)
        match generic::execute(store, server, cmd_name, args) {
            Ok(resp) => return Ok(resp),
            Err(Error::UnknownCommand(_)) => {}
            Err(e) => return Err(e),
        }

        // Try each command module
        // Only continue to next module if it returns UnknownCommand
        // Other errors (WrongArity, Syntax, etc.) should be returned immediately
        // Normalize command name to uppercase for module dispatch
        let raw_name = cmd.name();
        let cmd_name_owned;
        let cmd_name = if raw_name.iter().all(|b| !b.is_ascii_lowercase()) {
            raw_name
        } else {
            cmd_name_owned = raw_name.to_ascii_uppercase();
            &cmd_name_owned
        };

        // Helper to execute specific modules
        let execute_module = || -> Result<RespValue> {
            match string::execute(store, cmd_name, args) {
                Ok(resp) => return Ok(resp),
                Err(Error::UnknownCommand(_)) => {}
                Err(e) => return Err(e),
            }
            match list::execute(store, cmd_name, args) {
                Ok(resp) => return Ok(resp),
                Err(Error::UnknownCommand(_)) => {}
                Err(e) => return Err(e),
            }
            match hash::execute(store, cmd_name, args, client, replication) {
                Ok(resp) => return Ok(resp),
                Err(Error::UnknownCommand(_)) => {}
                Err(e) => return Err(e),
            }
            match set::execute(store, cmd_name, args, server, replication) {
                Ok(resp) => return Ok(resp),
                Err(Error::UnknownCommand(_)) => {}
                Err(e) => return Err(e),
            }
            match sorted_set::execute(store, cmd_name, args) {
                Ok(resp) => return Ok(resp),
                Err(Error::UnknownCommand(_)) => {}
                Err(e) => return Err(e),
            }
            match hyperloglog::execute(store, cmd_name, args) {
                Ok(resp) => return Ok(resp),
                Err(Error::UnknownCommand(_)) => {}
                Err(e) => return Err(e),
            }

            // Bitmap commands
            if cmd_name.len() >= 3 && cmd_name[..3].eq_ignore_ascii_case(b"BIT")
                || cmd_name.eq_ignore_ascii_case(b"SETBIT")
                || cmd_name.eq_ignore_ascii_case(b"GETBIT")
            {
                return bitmap::execute(store, cmd_name, args);
            }

            // JSON commands
            #[cfg(feature = "json")]
            if cmd_name.len() > 5 && cmd_name[..5].eq_ignore_ascii_case(b"JSON.") {
                return json::execute(store, cmd_name, args);
            }

            // TimeSeries commands
            #[cfg(feature = "timeseries")]
            if cmd_name.len() > 3 && cmd_name[..3].eq_ignore_ascii_case(b"TS.") {
                return timeseries::execute(store, cmd_name, args);
            }

            // Search commands
            #[cfg(feature = "search")]
            if cmd_name.len() > 3 && cmd_name[..3].eq_ignore_ascii_case(b"FT.") {
                return search::execute(store, cmd_name, args);
            }

            // Geo commands
            if cmd_name.len() >= 3 && cmd_name[..3].eq_ignore_ascii_case(b"GEO") {
                return geo::execute(store, cmd_name, args);
            }

            // Stream commands
            if !cmd_name.is_empty() && (cmd_name[0] == b'X' || cmd_name[0] == b'x') {
                return stream::execute(store, cmd_name, args);
            }

            // Scripting commands (EVAL, EVALSHA, SCRIPT, FUNCTION, FCALL)
            if cmd_name.eq_ignore_ascii_case(b"EVAL")
                || cmd_name.eq_ignore_ascii_case(b"EVAL_RO")
                || cmd_name.eq_ignore_ascii_case(b"EVALSHA")
                || cmd_name.eq_ignore_ascii_case(b"EVALSHA_RO")
                || cmd_name.eq_ignore_ascii_case(b"SCRIPT")
                || cmd_name.eq_ignore_ascii_case(b"FUNCTION")
                || cmd_name.eq_ignore_ascii_case(b"FCALL")
                || cmd_name.eq_ignore_ascii_case(b"FCALL_RO")
            {
                return scripting::execute(store, cmd_name, args);
            }

            // Vector commands
            #[cfg(feature = "vector")]
            if !cmd_name.is_empty() && (cmd_name[0] == b'V' || cmd_name[0] == b'v') {
                match vector::execute(store, cmd_name, args) {
                    Ok(resp) => return Ok(resp),
                    Err(Error::UnknownCommand(_)) => {}
                    Err(e) => return Err(e),
                }
            }

            // Bloom
            #[cfg(feature = "bloom")]
            {
                if cmd_name.len() >= 3 && cmd_name[..3].eq_ignore_ascii_case(b"BF.") {
                    return bloom::execute(store, cmd_name, args);
                }
                if cmd_name.len() >= 3 && cmd_name[..3].eq_ignore_ascii_case(b"CF.") {
                    return cuckoo::execute(store, cmd_name, args);
                }
                if cmd_name.len() >= 8 && cmd_name[..8].eq_ignore_ascii_case(b"TDIGEST.") {
                    return tdigest::execute(store, cmd_name, args);
                }
                if cmd_name.len() >= 5 && cmd_name[..5].eq_ignore_ascii_case(b"TOPK.") {
                    return topk::execute(store, cmd_name, args);
                }
                if cmd_name.len() >= 4 && cmd_name[..4].eq_ignore_ascii_case(b"CMS.") {
                    return cms::execute(store, cmd_name, args);
                }
            }

            Err(Error::UnknownCommand(
                String::from_utf8_lossy(raw_name).into_owned(),
            ))
        };

        let result = execute_module();

        // Propagation logic
        if let Ok(ref resp) = result {
            // Only propagate if successful (not error response per se, but RespValue::Error is valid response)
            // But usually we don't propagate errors.
            // Helper: is_error() on RespValue
            let is_error = matches!(resp, RespValue::Error(_));

            if !is_error {
                if let Some(repl) = replication {
                    let mut should_propagate = is_write_command(cmd_name);

                    // Special handling for GETEX: only propagate if it has write options
                    if cmd.is_command(b"GETEX") && args.len() > 1 {
                        should_propagate = true;
                    }

                    if should_propagate {
                        // Special rewrite logic for certain commands
                        if cmd.is_command(b"GETDEL") {
                            // GETDEL key -> DEL key
                            let parts = vec![Bytes::from_static(b"DEL"), args[0].clone()];
                            repl.propagate(&parts);
                        } else if cmd.is_command(b"DELEX") {
                            // DELEX key [condition] -> DEL key (when successful deletion)
                            // Only propagate if deletion actually happened (result is integer 1)
                            if matches!(result, Ok(RespValue::Integer(1))) {
                                let parts = vec![Bytes::from_static(b"DEL"), args[0].clone()];
                                repl.propagate(&parts);
                            }
                        } else if cmd.is_command(b"HGETDEL")
                            || cmd.is_command(b"HSET")
                            || cmd.is_command(b"HDEL")
                            || cmd.is_command(b"HMSET")
                            || cmd.is_command(b"HSETNX")
                            || cmd.is_command(b"HINCRBY")
                            || cmd.is_command(b"HINCRBYFLOAT")
                        {
                            // Hash commands handle their own propagation in hash.rs
                            // Skip standard propagation to avoid double propagation
                        } else if cmd.is_command(b"SPOP") {
                            // SPOP handles its own propagation in set.rs
                            // When key is deleted, it propagates DEL or UNLINK instead
                        } else if cmd.is_command(b"GETEX") {
                            // Rewrite GETEX based on options
                            // GETEX key [EX seconds | PX ms | EXAT | PXAT | PERSIST]
                            if args.len() > 1 {
                                let key = &args[0];
                                let opt = args[1].to_ascii_uppercase();
                                match opt.as_slice() {
                                    b"PERSIST" => {
                                        let parts =
                                            vec![Bytes::from_static(b"PERSIST"), key.clone()];
                                        repl.propagate(&parts);
                                    }
                                    b"EX" | b"PX" | b"EXAT" | b"PXAT" => {
                                        if args.len() >= 3 {
                                            // We normalize everything to PEXPIREAT for simplicity in propagation
                                            // provided we can calculate the absolute time.
                                            // But for now, let's just propagate the command that was used equivalent?
                                            // Redis propagates as PEXPIREAT for EX/PX/EXAT/PXAT.
                                            // We need to calculate the timestamp if relative.

                                            use crate::storage::now_ms;

                                            // We can read what was set or recalculate.
                                            // Easier to just recalculate based on input.
                                            let val_str =
                                                std::str::from_utf8(&args[2]).unwrap_or("0");
                                            let val = val_str.parse::<i64>().unwrap_or(0);

                                            let expire_at_ms = match opt.as_slice() {
                                                b"EX" => now_ms() + val * 1000,
                                                b"PX" => now_ms() + val,
                                                b"EXAT" => val * 1000,
                                                b"PXAT" => val,
                                                _ => 0,
                                            };

                                            if expire_at_ms > 0 {
                                                let parts = vec![
                                                    Bytes::from_static(b"PEXPIREAT"),
                                                    key.clone(),
                                                    Bytes::from(expire_at_ms.to_string()),
                                                ];
                                                repl.propagate(&parts);
                                            }
                                        }
                                    }
                                    _ => {} // Should not happen if validation passed
                                }
                            }
                        } else {
                            // Standard propagation
                            let mut parts = Vec::with_capacity(1 + args.len());
                            parts.push(Bytes::copy_from_slice(cmd.name()));
                            parts.extend_from_slice(args);
                            repl.propagate(&parts);
                        }
                    }
                }
            }
        }
        result
    }
}

fn cmd_del(store: &Store, args: &[Bytes], use_lazy: bool) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("DEL"));
    }
    let count: i64 = if use_lazy {
        args.iter()
            .map(|k| if store.lazy_del(k) { 1 } else { 0 })
            .sum()
    } else {
        args.iter().map(|k| if store.del(k) { 1 } else { 0 }).sum()
    };
    Ok(RespValue::integer(count))
}

fn cmd_exists(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("EXISTS"));
    }
    let count: i64 = args
        .iter()
        .map(|k| if store.exists(k) { 1 } else { 0 })
        .sum();
    Ok(RespValue::integer(count))
}

fn cmd_expire(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("EXPIRE"));
    }
    let seconds: i64 = std::str::from_utf8(&args[1])
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)?;
    let result = store.expire(&args[0], seconds * 1000);
    Ok(RespValue::integer(if result { 1 } else { 0 }))
}

fn cmd_pexpire(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("PEXPIRE"));
    }
    let ms: i64 = std::str::from_utf8(&args[1])
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)?;
    let result = store.expire(&args[0], ms);
    Ok(RespValue::integer(if result { 1 } else { 0 }))
}

fn cmd_ttl(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("TTL"));
    }
    let pttl = store.pttl(&args[0]);
    let ttl = if pttl >= 0 { pttl / 1000 } else { pttl };
    Ok(RespValue::integer(ttl))
}

fn cmd_pttl(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("PTTL"));
    }
    Ok(RespValue::integer(store.pttl(&args[0])))
}

fn cmd_persist(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("PERSIST"));
    }
    let result = store.persist(&args[0]);
    Ok(RespValue::integer(if result { 1 } else { 0 }))
}

fn cmd_type(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("TYPE"));
    }
    let type_name = store.key_type(&args[0]).unwrap_or("none");
    Ok(RespValue::SimpleString(Bytes::copy_from_slice(
        type_name.as_bytes(),
    )))
}

fn cmd_info(store: &Store, args: &[Bytes], server: Option<&Arc<ServerState>>) -> RespValue {
    // Return minimal INFO response for redis-benchmark compatibility
    let section = if args.is_empty() {
        b"all"
    } else {
        args[0].as_ref()
    };

    let mut info = String::new();

    let is_all =
        section.eq_ignore_ascii_case(b"all") || section.eq_ignore_ascii_case(b"everything");
    let is_default = section.eq_ignore_ascii_case(b"default");

    if section.eq_ignore_ascii_case(b"server") || is_all || is_default {
        info.push_str("# Server\r\n");
        info.push_str("redis_version:7.0.0\r\n");
        info.push_str("redis_mode:standalone\r\n");
        info.push_str("os:Linux\r\n");
        info.push_str("arch_bits:64\r\n");
        info.push_str(&format!("process_id:{}\r\n", std::process::id()));
    }

    if section.eq_ignore_ascii_case(b"memory") || is_all || is_default {
        if !info.is_empty() && !info.ends_with("\r\n\r\n") {
            info.push_str("\r\n");
        }
        info.push_str("# Memory\r\n");

        // Get memory stats from mimalloc
        let (used_memory, peak_rss, current_rss) = get_mimalloc_memory_stats();

        info.push_str(&format!("used_memory:{}\r\n", used_memory));
        info.push_str(&format!(
            "used_memory_human:{}\r\n",
            format_memory_human(used_memory)
        ));
        info.push_str(&format!("used_memory_rss:{}\r\n", current_rss));
        info.push_str(&format!(
            "used_memory_rss_human:{}\r\n",
            format_memory_human(current_rss)
        ));
        info.push_str(&format!("used_memory_peak:{}\r\n", peak_rss));
        info.push_str(&format!(
            "used_memory_peak_human:{}\r\n",
            format_memory_human(peak_rss)
        ));
        info.push_str("used_memory_overhead:0\r\n");
        info.push_str("used_memory_startup:0\r\n");
        info.push_str(&format!("used_memory_dataset:{}\r\n", used_memory));
        info.push_str("used_memory_dataset_perc:100.00%\r\n");
        info.push_str("total_system_memory:0\r\n");
        info.push_str("total_system_memory_human:0B\r\n");
        info.push_str("used_memory_lua:0\r\n");
        info.push_str("used_memory_lua_human:0B\r\n");
        info.push_str("maxmemory:0\r\n");
        info.push_str("maxmemory_human:0B\r\n");
        info.push_str("maxmemory_policy:noeviction\r\n");
        info.push_str("mem_fragmentation_ratio:1.00\r\n");
        info.push_str("mem_fragmentation_bytes:0\r\n");
        info.push_str("mem_allocator:mimalloc\r\n");
    }

    if section.eq_ignore_ascii_case(b"persistence") || is_all || is_default {
        if !info.is_empty() && !info.ends_with("\r\n\r\n") {
            info.push_str("\r\n");
        }
        info.push_str("# Persistence\r\n");
        info.push_str("loading:0\r\n");
        info.push_str("async_loading:0\r\n");
        info.push_str("current_cow_peak:0\r\n");
        info.push_str("current_cow_size:0\r\n");
        info.push_str("current_cow_size_age:0\r\n");
        info.push_str("current_fork_perc:0.00\r\n");
        info.push_str("current_save_keys_processed:0\r\n");
        info.push_str("current_save_keys_total:0\r\n");
        info.push_str("rdb_changes_since_last_save:0\r\n");

        // Get rdb_bgsave_in_progress from server state
        let bgsave_in_progress = server
            .map(|s| {
                s.rdb_bgsave_in_progress
                    .load(std::sync::atomic::Ordering::Relaxed)
            })
            .unwrap_or(false);
        info.push_str(&format!(
            "rdb_bgsave_in_progress:{}\r\n",
            if bgsave_in_progress { 1 } else { 0 }
        ));

        info.push_str("rdb_last_save_time:0\r\n");
        info.push_str("rdb_last_bgsave_status:ok\r\n");
        info.push_str("rdb_last_bgsave_time_sec:-1\r\n");
        info.push_str("rdb_current_bgsave_time_sec:-1\r\n");
        info.push_str("rdb_saves:0\r\n");
        info.push_str("rdb_last_cow_size:0\r\n");
        info.push_str("rdb_last_load_keys_expired:0\r\n");
        info.push_str("rdb_last_load_keys_loaded:0\r\n");
        info.push_str("aof_enabled:0\r\n");

        // Get aof_rewrite_in_progress from server state
        let aof_rewrite_in_progress = server
            .map(|s| {
                s.aof_rewrite_in_progress
                    .load(std::sync::atomic::Ordering::Relaxed)
            })
            .unwrap_or(false);
        info.push_str(&format!(
            "aof_rewrite_in_progress:{}\r\n",
            if aof_rewrite_in_progress { 1 } else { 0 }
        ));

        info.push_str("aof_rewrite_scheduled:0\r\n");
        info.push_str("aof_last_rewrite_time_sec:-1\r\n");
        info.push_str("aof_current_rewrite_time_sec:-1\r\n");
        info.push_str("aof_last_bgrewrite_status:ok\r\n");
        info.push_str("aof_rewrites:0\r\n");
        info.push_str("aof_last_write_status:ok\r\n");
        info.push_str("aof_last_cow_size:0\r\n");
        info.push_str("module_fork_in_progress:0\r\n");
        info.push_str("module_fork_last_cow_size:0\r\n");
    }

    if section.eq_ignore_ascii_case(b"replication") || is_all || is_default {
        if !info.is_empty() && !info.ends_with("\r\n\r\n") {
            info.push_str("\r\n");
        }
        info.push_str("# Replication\r\nrole:master\r\nconnected_slaves:0\r\n");
    }

    if section.eq_ignore_ascii_case(b"clients") || is_all || is_default {
        if !info.is_empty() && !info.ends_with("\r\n\r\n") {
            info.push_str("\r\n");
        }
        info.push_str("# Clients\r\n");
        info.push_str("connected_clients:0\r\n");
        let blocked_clients = server
            .map(|s| s.blocking.blocked_client_count())
            .unwrap_or(0);
        info.push_str(&format!("blocked_clients:{}\r\n", blocked_clients));
        info.push_str("tracking_clients:0\r\n");
        info.push_str("clients_in_timeout_table:0\r\n");
    }

    if section.eq_ignore_ascii_case(b"stats") || is_all || is_default {
        if !info.is_empty() && !info.ends_with("\r\n\r\n") {
            info.push_str("\r\n");
        }
        info.push_str("# Stats\r\ntotal_connections_received:0\r\ntotal_commands_processed:0\r\n");
    }

    if section.eq_ignore_ascii_case(b"keyspace") || is_all || is_default {
        if !info.is_empty() && !info.ends_with("\r\n\r\n") {
            info.push_str("\r\n");
        }
        info.push_str("# Keyspace\r\n");

        // Get key count and expiring keys count
        let keys = store.len();
        if keys > 0 {
            // Count keys with TTL
            let expires = store.expiration_index.len();
            info.push_str(&format!(
                "db0:keys={},expires={},avg_ttl=0\r\n",
                keys, expires
            ));
        }
    }

    RespValue::bulk(Bytes::copy_from_slice(info.as_bytes()))
}

/// Check if command is a server management command
#[inline]
fn is_server_command(cmd: &[u8]) -> bool {
    cmd.eq_ignore_ascii_case(b"ACL")
        || cmd.eq_ignore_ascii_case(b"COMMAND")
        || cmd.eq_ignore_ascii_case(b"CONFIG")
        || cmd.eq_ignore_ascii_case(b"DEBUG")
        || cmd.eq_ignore_ascii_case(b"MEMORY")
        || cmd.eq_ignore_ascii_case(b"LATENCY")
        || cmd.eq_ignore_ascii_case(b"SLOWLOG")
        || cmd.eq_ignore_ascii_case(b"BGREWRITEAOF")
        || cmd.eq_ignore_ascii_case(b"BGSAVE")
        || cmd.eq_ignore_ascii_case(b"LASTSAVE")
        || cmd.eq_ignore_ascii_case(b"LOLWUT")
        || cmd.eq_ignore_ascii_case(b"SAVE")
        || cmd.eq_ignore_ascii_case(b"SHUTDOWN")
        || cmd.eq_ignore_ascii_case(b"TIME")
        || cmd.eq_ignore_ascii_case(b"FAILOVER")
        || cmd.eq_ignore_ascii_case(b"PSYNC")
        || cmd.eq_ignore_ascii_case(b"REPLCONF")
        || cmd.eq_ignore_ascii_case(b"REPLICAOF")
        || cmd.eq_ignore_ascii_case(b"ROLE")
        || cmd.eq_ignore_ascii_case(b"SLAVEOF")
        || cmd.eq_ignore_ascii_case(b"SYNC")
        || cmd.eq_ignore_ascii_case(b"MODULE")
        || cmd.eq_ignore_ascii_case(b"SWAPDB")
}

fn is_cluster_protocol_command(cmd: &[u8]) -> bool {
    cmd.eq_ignore_ascii_case(b"CLUSTER")
        || cmd.eq_ignore_ascii_case(b"ASKING")
        || cmd.eq_ignore_ascii_case(b"READONLY")
        || cmd.eq_ignore_ascii_case(b"READWRITE")
}

const READ_ONLY_COMMANDS: &[&str] = &[
    "GET",
    "MGET",
    "STRLEN",
    "GETRANGE",
    "EXISTS",
    "TTL",
    "PTTL",
    "TYPE",
    "GETBIT",
    "BITCOUNT",
    "BITPOS",
    "SUBSTR",
    "DUMP",
    "OBJECT",
    "KEYS",
    "SCAN",
    "RANDOMKEY",
    "DBSIZE",
    "LLEN",
    "LINDEX",
    "LRANGE",
    "LPOS",
    "SCARD",
    "SISMEMBER",
    "SMISMEMBER",
    "SRANDMEMBER",
    "SINTER",
    "SUNION",
    "SDIFF",
    "SSCAN",
    "HLEN",
    "HKEYS",
    "HVALS",
    "HGET",
    "HMGET",
    "HEXISTS",
    "HSTRLEN",
    "HGETALL",
    "HSCAN",
    "HRANDFIELD",
    "ZCARD",
    "ZCOUNT",
    "ZLEXCOUNT",
    "ZSCORE",
    "ZRANK",
    "ZREVRANK",
    "ZRANGE",
    "ZRANGEBYLEX",
    "ZRANGEBYSCORE",
    "ZREVRANGE",
    "ZREVRANGEBYLEX",
    "ZREVRANGEBYSCORE",
    "ZMPOP",
    "ZINTER",
    "ZUNION",
    "ZDIFF",
    "ZSCAN",
    "PFCOUNT",
    "XREAD",
    "XREADGROUP",
    "XRANGE",
    "XREVRANGE",
    "XLEN",
    "XPENDING",
    "XINFO",
    "GEOHASH",
    "GEOPOS",
    "GEODIST",
    "GEORADIUS_RO",
    "GEORADIUSBYMEMBER_RO",
    "GEOSEARCH",
    "EVAL_RO",
    "EVALSHA_RO",
    "FCALL_RO",
    "FUNCTION",
    "COMMAND",
    "ECHO",
    "PING",
    "TIME",
    "ROLE",
    "LASTSAVE",
    "LOLWUT",
    "MEMORY",
    "WAIT",
    "WAITAOF",
    "AUTH",
    "HELLO",
    "ACL",
    "BGREWRITEAOF",
    "BGSAVE",
    "SAVE",
    "SHUTDOWN",
    "SLAVEOF",
    "REPLICAOF",
    "SYNC",
    "PSYNC",
    "MONITOR",
    "DEBUG",
    "CONFIG",
    "CLIENT",
    "CLUSTER",
    "READONLY",
    "READWRITE",
    "ASKING",
    "SELECT",
    "SUBSCRIBE",
    "PSUBSCRIBE",
    "UNSUBSCRIBE",
    "PUNSUBSCRIBE",
    "SSUBSCRIBE",
    "SUNSUBSCRIBE",
    "PUBSUB",
    "WATCH",
    "UNWATCH",
    "MULTI",
    "EXEC",
    "DISCARD",
    "QUIT",
    "RESET",
    "FAILOVER",
    "REPLCONF",
    "MODULE",
    "LATENCY",
    "SLOWLOG",
    "SWAPDB",
    "INFO",
];

/// Check if a command is a write command (used for cluster read/write checks and replication)
fn is_write_command(cmd: &[u8]) -> bool {
    let c = std::str::from_utf8(cmd).unwrap_or("").to_uppercase();
    !READ_ONLY_COMMANDS.contains(&c.as_str())
}
