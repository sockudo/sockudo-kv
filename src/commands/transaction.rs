//! Transaction command handlers
//!
//! MULTI, EXEC, DISCARD, WATCH, UNWATCH for atomic command execution.

use bytes::Bytes;
use std::sync::Arc;

use crate::client::ClientState;
use crate::commands::Dispatcher;
use crate::error::{Error, Result};
use crate::protocol::{Command, RespValue};
use crate::storage::Store;

/// Result of transaction command execution
pub enum TransactionResult {
    /// Normal response
    Response(RespValue),
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
    store: &Arc<Store>,
    cmd: &[u8],
    args: &[Bytes],
) -> Result<TransactionResult> {
    match cmd.to_ascii_uppercase().as_slice() {
        b"MULTI" => cmd_multi(client),
        b"EXEC" => cmd_exec(client, store),
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

/// EXEC - Execute all queued commands
fn cmd_exec(client: &Arc<ClientState>, store: &Arc<Store>) -> Result<TransactionResult> {
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

    // Execute all queued commands
    let mut results = Vec::with_capacity(multi_state.commands.len());

    for queued in multi_state.commands {
        // Create a Command from the queued command
        let cmd = Command {
            name: queued.name,
            args: queued.args,
        };

        // Execute the command
        let result = Dispatcher::execute_basic(store, cmd);
        results.push(result);
    }

    Ok(TransactionResult::Response(RespValue::Array(results)))
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

    fn make_test_setup() -> (Arc<ClientState>, Arc<Store>) {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 12345);
        let client = Arc::new(ClientState::new(1, addr, 1, false));
        let store = Arc::new(Store::with_capacity(100));
        (client, store)
    }

    #[test]
    fn test_multi_exec() {
        let (client, store) = make_test_setup();

        // Start MULTI
        let result = execute(&client, &store, b"MULTI", &[]).unwrap();
        assert!(matches!(result, TransactionResult::Response(_)));
        assert!(client.in_multi());

        // Queue some commands
        client.queue_command(
            Bytes::from_static(b"SET"),
            vec![Bytes::from_static(b"key1"), Bytes::from_static(b"value1")],
        );

        // EXEC
        let result = execute(&client, &store, b"EXEC", &[]).unwrap();
        assert!(matches!(
            result,
            TransactionResult::Response(RespValue::Array(_))
        ));
        assert!(!client.in_multi());
    }

    #[test]
    fn test_discard() {
        let (client, store) = make_test_setup();

        execute(&client, &store, b"MULTI", &[]).unwrap();
        assert!(client.in_multi());

        execute(&client, &store, b"DISCARD", &[]).unwrap();
        assert!(!client.in_multi());
    }

    #[test]
    fn test_exec_without_multi() {
        let (client, store) = make_test_setup();
        let result = execute(&client, &store, b"EXEC", &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_watch_unwatch() {
        let (client, store) = make_test_setup();

        // Set a key
        store.set(Bytes::from_static(b"key1"), Bytes::from_static(b"value1"));

        // Watch it
        execute(&client, &store, b"WATCH", &[Bytes::from_static(b"key1")]).unwrap();
        assert_eq!(client.get_watched_keys().len(), 1);

        // Unwatch
        execute(&client, &store, b"UNWATCH", &[]).unwrap();
        assert_eq!(client.get_watched_keys().len(), 0);
    }
}
