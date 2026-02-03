//! Transaction command handlers
//!
//! MULTI, EXEC, DISCARD, WATCH, UNWATCH for atomic command execution.

use bytes::Bytes;
use std::sync::Arc;

use crate::client::ClientState;
use crate::commands::Dispatcher;
use crate::error::{Error, Result};
use crate::protocol::{Command, RespValue};
use crate::replication::ReplicationManager;
use crate::server_state::ServerState;
use crate::storage::{self, MultiStore, Store};

/// Result of transaction command execution
pub enum TransactionResult {
    /// Normal response
    Response(RespValue),
    /// Normal response with keys to signal for blocked clients (after EXEC)
    ResponseWithSignal(RespValue, Vec<Bytes>),
    /// Queue command and return QUEUED
    Queued,
}

/// Check if command is a transaction command
#[inline]
pub fn is_transaction_command(cmd: &[u8]) -> bool {
    cmd.eq_ignore_ascii_case(b"MULTI")
        || cmd.eq_ignore_ascii_case(b"EXEC")
        || cmd.eq_ignore_ascii_case(b"DISCARD")
        || cmd.eq_ignore_ascii_case(b"WATCH")
        || cmd.eq_ignore_ascii_case(b"UNWATCH")
}

/// Commands that cannot be used inside MULTI
#[inline]
pub fn is_forbidden_in_multi(cmd: &[u8]) -> bool {
    cmd.eq_ignore_ascii_case(b"WATCH")
        || cmd.eq_ignore_ascii_case(b"SUBSCRIBE")
        || cmd.eq_ignore_ascii_case(b"PSUBSCRIBE")
        || cmd.eq_ignore_ascii_case(b"UNSUBSCRIBE")
        || cmd.eq_ignore_ascii_case(b"PUNSUBSCRIBE")
}

/// Execute a transaction command
pub fn execute(
    client: &Arc<ClientState>,
    multi_store: &Arc<MultiStore>,
    store: &Arc<Store>,
    server_state: &Arc<ServerState>,
    replication: Option<&Arc<ReplicationManager>>,
    cmd: &[u8],
    args: &[Bytes],
) -> Result<TransactionResult> {
    match cmd.to_ascii_uppercase().as_slice() {
        b"MULTI" => cmd_multi(client),
        b"EXEC" => cmd_exec(client, multi_store, store, server_state, replication),
        b"DISCARD" => cmd_discard(client),
        b"WATCH" => cmd_watch(client, store, args),
        b"UNWATCH" => cmd_unwatch(client),
        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd).into_owned(),
        )),
    }
}

/// MULTI - Start a transaction
fn cmd_multi(client: &Arc<ClientState>) -> Result<TransactionResult> {
    if client.in_multi() {
        return Err(Error::Custom(
            "ERR MULTI calls can not be nested".to_string(),
        ));
    }
    client.start_multi();
    Ok(TransactionResult::Response(RespValue::ok()))
}

/// Check if command is a list push command that could wake blocked clients
#[inline]
/// Convert time-relative commands (EXPIRE, PEXPIRE, SETEX, PSETEX) to absolute time
/// for replication consistency (EXPIREAT, PEXPIREAT, SET with PXAT)
fn convert_command_for_replication(cmd_name: &Bytes, cmd_args: &[Bytes]) -> Vec<Bytes> {
    let cmd_upper = cmd_name.to_ascii_uppercase();
    let now_ms = storage::now_ms();

    match cmd_upper.as_slice() {
        b"EXPIRE" if cmd_args.len() >= 2 => {
            // EXPIRE key seconds -> PEXPIREAT key timestamp_ms
            if let Ok(seconds) = std::str::from_utf8(&cmd_args[1])
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .ok_or(())
            {
                let expire_at_ms = now_ms + seconds * 1000;
                let mut parts = vec![
                    Bytes::from_static(b"PEXPIREAT"),
                    cmd_args[0].clone(),
                    Bytes::from(expire_at_ms.to_string()),
                ];
                // Preserve additional flags (NX, XX, GT, LT) if present
                if cmd_args.len() > 2 {
                    parts.extend(cmd_args[2..].iter().cloned());
                }
                return parts;
            }
        }
        b"PEXPIRE" if cmd_args.len() >= 2 => {
            // PEXPIRE key milliseconds -> PEXPIREAT key timestamp_ms
            if let Ok(ms) = std::str::from_utf8(&cmd_args[1])
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .ok_or(())
            {
                let expire_at_ms = now_ms + ms;
                let mut parts = vec![
                    Bytes::from_static(b"PEXPIREAT"),
                    cmd_args[0].clone(),
                    Bytes::from(expire_at_ms.to_string()),
                ];
                // Preserve additional flags (NX, XX, GT, LT) if present
                if cmd_args.len() > 2 {
                    parts.extend(cmd_args[2..].iter().cloned());
                }
                return parts;
            }
        }
        b"SETEX" if cmd_args.len() >= 3 => {
            // SETEX key seconds value -> SET key value PXAT timestamp_ms
            if let Ok(seconds) = std::str::from_utf8(&cmd_args[1])
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .ok_or(())
            {
                let expire_at_ms = now_ms + seconds * 1000;
                return vec![
                    Bytes::from_static(b"SET"),
                    cmd_args[0].clone(),
                    cmd_args[2].clone(),
                    Bytes::from_static(b"PXAT"),
                    Bytes::from(expire_at_ms.to_string()),
                ];
            }
        }
        b"PSETEX" if cmd_args.len() >= 3 => {
            // PSETEX key milliseconds value -> SET key value PXAT timestamp_ms
            if let Ok(ms) = std::str::from_utf8(&cmd_args[1])
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .ok_or(())
            {
                let expire_at_ms = now_ms + ms;
                return vec![
                    Bytes::from_static(b"SET"),
                    cmd_args[0].clone(),
                    cmd_args[2].clone(),
                    Bytes::from_static(b"PXAT"),
                    Bytes::from(expire_at_ms.to_string()),
                ];
            }
        }
        _ => {}
    }

    // Default: return the command unchanged
    let mut parts = Vec::with_capacity(1 + cmd_args.len());
    parts.push(cmd_name.clone());
    parts.extend(cmd_args.iter().cloned());
    parts
}

fn is_list_push_command(cmd: &[u8]) -> bool {
    cmd.eq_ignore_ascii_case(b"LPUSH")
        || cmd.eq_ignore_ascii_case(b"RPUSH")
        || cmd.eq_ignore_ascii_case(b"LPUSHX")
        || cmd.eq_ignore_ascii_case(b"RPUSHX")
        || cmd.eq_ignore_ascii_case(b"LMOVE")
        || cmd.eq_ignore_ascii_case(b"RPOPLPUSH")
}

/// EXEC - Execute all queued commands
fn cmd_exec(
    client: &Arc<ClientState>,
    multi_store: &Arc<MultiStore>,
    store: &Arc<Store>,
    server_state: &Arc<ServerState>,
    replication: Option<&Arc<ReplicationManager>>,
) -> Result<TransactionResult> {
    // Take the multi state
    let multi_state = match client.take_multi() {
        Some(state) => state,
        None => {
            return Err(Error::Custom("ERR EXEC without MULTI".to_string()));
        }
    };

    // Clear watched keys after EXEC (regardless of success/failure)
    let watched_keys = client.get_watched_keys();
    client.unwatch();

    // Check if any watched keys were modified
    for (key, version) in watched_keys {
        if let Some(current_version) = store.get_version(&key) {
            if current_version != version {
                // Key was modified, abort transaction
                return Ok(TransactionResult::Response(RespValue::Null));
            }
        } else if version != 0 {
            // Key was deleted
            return Ok(TransactionResult::Response(RespValue::Null));
        }
    }

    // Check for queued errors
    if multi_state.has_errors() {
        return Err(Error::Custom(
            "EXECABORT Transaction discarded because of previous errors".to_string(),
        ));
    }

    // Execute all queued commands, tracking keys to signal for blocked clients
    let mut results = Vec::with_capacity(multi_state.commands.len());
    let mut keys_to_signal: Vec<Bytes> = Vec::new();

    // Collect commands for replication (we'll propagate after successful execution)
    let mut commands_for_replication: Vec<(Bytes, Vec<Bytes>)> = Vec::new();

    for queued in multi_state.commands {
        // Track list push commands to signal blocked clients after EXEC
        let push_key = if is_list_push_command(&queued.name) && !queued.args.is_empty() {
            // For LMOVE/RPOPLPUSH, the destination key is args[1]
            if queued.name.eq_ignore_ascii_case(b"LMOVE")
                || queued.name.eq_ignore_ascii_case(b"RPOPLPUSH")
            {
                queued.args.get(1).cloned()
            } else {
                // For LPUSH/RPUSH/LPUSHX/RPUSHX, the key is args[0]
                Some(queued.args[0].clone())
            }
        } else {
            None
        };

        // Create a Command from the queued command
        let cmd = Command {
            name: queued.name.clone(),
            args: queued.args.clone(),
        };

        // Store command for replication
        commands_for_replication.push((queued.name, queued.args));

        // Execute the command with full server context
        // Note: We pass None for replication to avoid double-propagation.
        // Commands inside MULTI are propagated as a batch (MULTI + commands + EXEC) below.
        let result = Dispatcher::execute(multi_store, store, server_state, None, None, cmd);

        // Check if list push was successful (returns integer > 0)
        if let Some(key) = push_key {
            let was_success = match &result {
                RespValue::Integer(n) => *n > 0,
                // LMOVE/RPOPLPUSH return bulk string on success
                RespValue::BulkString(_) => true,
                _ => false,
            };
            if was_success && !keys_to_signal.contains(&key) {
                keys_to_signal.push(key);
            }
        }

        results.push(result);
    }

    // Filter keys_to_signal: only signal keys that still exist as non-empty lists
    // This handles cases like MULTI/LPUSH/DEL/SET/EXEC where the key type changed
    keys_to_signal.retain(|key| {
        if let Some(key_type) = store.key_type(key) {
            if key_type == "list" {
                // Check if list is non-empty
                return store.llen(key).unwrap_or(0) > 0;
            }
        }
        false
    });

    // Propagate transaction to replicas (MULTI + commands + EXEC)
    if let Some(repl) = replication {
        // Propagate MULTI
        repl.propagate(&[Bytes::from_static(b"MULTI")]);

        // Propagate each command, converting time-relative commands to absolute time
        for (cmd_name, cmd_args) in commands_for_replication {
            let parts = convert_command_for_replication(&cmd_name, &cmd_args);
            repl.propagate(&parts);
        }

        // Propagate EXEC
        repl.propagate(&[Bytes::from_static(b"EXEC")]);
    }

    // Return keys to signal so main.rs can wake blocked clients
    if keys_to_signal.is_empty() {
        Ok(TransactionResult::Response(RespValue::Array(results)))
    } else {
        Ok(TransactionResult::ResponseWithSignal(
            RespValue::Array(results),
            keys_to_signal,
        ))
    }
}

/// DISCARD - Discard all queued commands
fn cmd_discard(client: &Arc<ClientState>) -> Result<TransactionResult> {
    if !client.in_multi() {
        return Err(Error::Custom("ERR DISCARD without MULTI".to_string()));
    }
    client.discard_multi();
    Ok(TransactionResult::Response(RespValue::ok()))
}

/// WATCH key [key ...] - Watch keys for changes
fn cmd_watch(
    client: &Arc<ClientState>,
    store: &Arc<Store>,
    args: &[Bytes],
) -> Result<TransactionResult> {
    if args.is_empty() {
        return Err(Error::WrongArity("WATCH"));
    }

    if client.in_multi() {
        return Err(Error::Custom(
            "ERR WATCH inside MULTI is not allowed".to_string(),
        ));
    }

    // Record current version for each key
    for key in args {
        let version = store.get_version(key).unwrap_or(0);
        client.watch_key(key.clone(), version);
    }

    Ok(TransactionResult::Response(RespValue::ok()))
}

/// UNWATCH - Forget all watched keys
fn cmd_unwatch(client: &Arc<ClientState>) -> Result<TransactionResult> {
    client.unwatch();
    Ok(TransactionResult::Response(RespValue::ok()))
}

/// Handle command queueing in MULTI mode
/// Returns Some(response) if command was queued or is a transaction command
/// Returns None if not in MULTI mode
pub fn handle_multi_queue(
    client: &Arc<ClientState>,
    cmd_name: &[u8],
    args: &[Bytes],
) -> Option<RespValue> {
    if !client.in_multi() {
        return None;
    }

    // These commands are always executed immediately, even in MULTI
    if cmd_name.eq_ignore_ascii_case(b"EXEC")
        || cmd_name.eq_ignore_ascii_case(b"DISCARD")
        || cmd_name.eq_ignore_ascii_case(b"MULTI")
    {
        return None;
    }

    // Check for forbidden commands
    if is_forbidden_in_multi(cmd_name) {
        // Queue an error
        let mut guard = client.multi_state.lock();
        if let Some(ref mut state) = *guard {
            state.errors.push(format!(
                "ERR {} is not allowed in a transaction",
                String::from_utf8_lossy(cmd_name)
            ));
        }
        return Some(RespValue::error(&format!(
            "ERR {} is not allowed in a transaction",
            String::from_utf8_lossy(cmd_name)
        )));
    }

    // Queue the command
    client.queue_command(Bytes::copy_from_slice(cmd_name), args.to_vec());
    Some(RespValue::SimpleString(Bytes::from_static(b"QUEUED")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn make_test_setup() -> (
        Arc<ClientState>,
        Arc<MultiStore>,
        Arc<Store>,
        Arc<ServerState>,
        Arc<ReplicationManager>,
    ) {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 12345);
        let client = Arc::new(ClientState::new(1, addr, 1, false));
        let multi_store = Arc::new(MultiStore::new());
        let store = multi_store.db(0);
        let server_state = Arc::new(ServerState::new());
        let replication = Arc::new(ReplicationManager::new());
        (client, multi_store, store, server_state, replication)
    }

    #[test]
    fn test_multi_exec() {
        let (client, multi_store, store, server_state, replication) = make_test_setup();

        // Start MULTI
        let result = execute(
            &client,
            &multi_store,
            &store,
            &server_state,
            Some(&replication),
            b"MULTI",
            &[],
        )
        .unwrap();
        assert!(matches!(result, TransactionResult::Response(_)));
        assert!(client.in_multi());

        // Queue some commands
        client.queue_command(
            Bytes::from_static(b"SET"),
            vec![Bytes::from_static(b"key1"), Bytes::from_static(b"value1")],
        );

        // EXEC
        let result = execute(
            &client,
            &multi_store,
            &store,
            &server_state,
            Some(&replication),
            b"EXEC",
            &[],
        )
        .unwrap();
        assert!(matches!(
            result,
            TransactionResult::Response(RespValue::Array(_))
        ));
        assert!(!client.in_multi());
    }

    #[test]
    fn test_discard() {
        let (client, multi_store, store, server_state, replication) = make_test_setup();

        execute(
            &client,
            &multi_store,
            &store,
            &server_state,
            Some(&replication),
            b"MULTI",
            &[],
        )
        .unwrap();
        assert!(client.in_multi());

        execute(
            &client,
            &multi_store,
            &store,
            &server_state,
            Some(&replication),
            b"DISCARD",
            &[],
        )
        .unwrap();
        assert!(!client.in_multi());
    }

    #[test]
    fn test_exec_without_multi() {
        let (client, multi_store, store, server_state, replication) = make_test_setup();
        let result = execute(
            &client,
            &multi_store,
            &store,
            &server_state,
            Some(&replication),
            b"EXEC",
            &[],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_watch_unwatch() {
        let (client, multi_store, store, server_state, replication) = make_test_setup();

        // Set a key
        store.set(Bytes::from_static(b"key1"), Bytes::from_static(b"value1"));

        // Watch it
        execute(
            &client,
            &multi_store,
            &store,
            &server_state,
            Some(&replication),
            b"WATCH",
            &[Bytes::from_static(b"key1")],
        )
        .unwrap();
        assert_eq!(client.get_watched_keys().len(), 1);

        // Unwatch
        execute(
            &client,
            &multi_store,
            &store,
            &server_state,
            Some(&replication),
            b"UNWATCH",
            &[],
        )
        .unwrap();
        assert_eq!(client.get_watched_keys().len(), 0);
    }
}
