//! Blocking operations manager for Redis blocking commands (BLPOP, BRPOP, etc.)
//!
//! This module implements Redis-compatible blocking list operations using an
//! event-driven architecture with tokio channels for efficient wakeup.

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::storage::Store;

/// Direction for pop operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PopDirection {
    Left,
    Right,
}

impl PopDirection {
    pub fn as_str(&self) -> &'static str {
        match self {
            PopDirection::Left => "LEFT",
            PopDirection::Right => "RIGHT",
        }
    }
}

/// Type of blocking operation a client is waiting for
#[derive(Debug, Clone)]
pub enum BlockingOperation {
    /// BLPOP/BRPOP: waiting to pop from any of these keys
    Pop {
        keys: Vec<Bytes>,
        direction: PopDirection,
    },
    /// BLMPOP: waiting to pop multiple elements
    MPop {
        keys: Vec<Bytes>,
        direction: PopDirection,
        count: usize,
    },
    /// BLMOVE: waiting to move element between lists
    Move {
        source: Bytes,
        destination: Bytes,
        wherefrom: PopDirection,
        whereto: PopDirection,
    },
    /// BRPOPLPUSH: waiting to pop from source and push to destination
    RPopLPush { source: Bytes, destination: Bytes },
}

impl BlockingOperation {
    /// Get the keys this operation is waiting on
    pub fn waiting_keys(&self) -> Vec<&Bytes> {
        match self {
            BlockingOperation::Pop { keys, .. } => keys.iter().collect(),
            BlockingOperation::MPop { keys, .. } => keys.iter().collect(),
            BlockingOperation::Move { source, .. } => vec![source],
            BlockingOperation::RPopLPush { source, .. } => vec![source],
        }
    }
}

/// A blocked client waiting for data
#[allow(dead_code)]
pub struct BlockedClient {
    /// The client's unique ID
    pub client_id: u64,
    /// Database index the client is operating on
    pub db_index: usize,
    /// The blocking operation
    pub operation: BlockingOperation,
    /// Channel to send result when unblocked
    pub waker: oneshot::Sender<BlockResult>,
}

/// Result sent to a blocked client when unblocked
#[derive(Debug)]
pub enum BlockResult {
    /// Data is available - client should try the operation
    Ready,
    /// Operation timed out
    Timeout,
    /// Client was explicitly unblocked (CLIENT UNBLOCK)
    Unblocked,
}

/// Client registration info - tracks client and their waker
struct ClientRegistration {
    keys: Vec<(usize, Bytes)>,
    waker: Option<oneshot::Sender<BlockResult>>,
}

/// Manager for blocked clients
///
/// This is the central coordinator for all blocking operations. It maintains
/// a mapping from keys to blocked clients and handles signaling when data
/// becomes available.
pub struct BlockingManager {
    /// Map from (db_index, key) -> queue of blocked client IDs
    /// Clients are served in FIFO order (first blocked = first served)
    blocked_clients: DashMap<(usize, Bytes), Mutex<VecDeque<u64>>>,

    /// Map from client_id -> registration info including waker
    /// The waker is stored here and taken when the client is woken
    client_registrations: DashMap<u64, Arc<Mutex<ClientRegistration>>>,
}

impl BlockingManager {
    pub fn new() -> Self {
        Self {
            blocked_clients: DashMap::new(),
            client_registrations: DashMap::new(),
        }
    }

    /// Register a client as blocked on the given keys
    /// Returns a receiver that will be notified when data might be available
    pub fn block_client(
        &self,
        client_id: u64,
        db_index: usize,
        operation: BlockingOperation,
    ) -> oneshot::Receiver<BlockResult> {
        let (tx, rx) = oneshot::channel();

        // Get all keys this operation is waiting on, deduplicated
        // (BLPOP can have the same key multiple times, we only need to register once)
        let keys: Vec<Bytes> = operation.waiting_keys().into_iter().cloned().collect();
        let mut seen = std::collections::HashSet::new();
        let key_pairs: Vec<(usize, Bytes)> = keys
            .into_iter()
            .filter(|k| seen.insert(k.clone()))
            .map(|k| (db_index, k))
            .collect();

        // Store client registration with waker
        self.client_registrations.insert(
            client_id,
            Arc::new(Mutex::new(ClientRegistration {
                keys: key_pairs.clone(),
                waker: Some(tx),
            })),
        );

        // Add client to blocked queue for each key (now deduplicated)
        for (db_idx, key) in key_pairs {
            let db_key = (db_idx, key);

            self.blocked_clients
                .entry(db_key)
                .or_insert_with(|| Mutex::new(VecDeque::new()))
                .lock()
                .push_back(client_id);
        }

        rx
    }

    /// Signal that a key has received data
    /// Wakes up the first blocked client waiting on this key
    /// Returns true if a client was woken up
    pub fn signal_key(&self, db_index: usize, key: &Bytes) -> bool {
        let db_key = (db_index, key.clone());

        // First, find a client to wake and collect info needed for cleanup
        // We need to release the blocked_clients lock before calling cleanup_client
        // to avoid deadlock
        let client_to_cleanup: Option<u64> = {
            if let Some(entry) = self.blocked_clients.get(&db_key) {
                let mut queue = entry.lock();

                // Find first client that hasn't been woken yet
                loop {
                    match queue.pop_front() {
                        Some(client_id) => {
                            // Try to get and wake the client
                            if let Some(reg) = self.client_registrations.get(&client_id) {
                                let mut registration = reg.lock();
                                // Take the waker - this ensures we only wake once
                                if let Some(waker) = registration.waker.take() {
                                    if waker.send(BlockResult::Ready).is_ok() {
                                        // Successfully woke a client
                                        break Some(client_id);
                                    }
                                }
                            }
                            // If send failed or no waker, client already gone - try next
                        }
                        None => break None,
                    }
                }
            } else {
                None
            }
        };

        // Now cleanup outside of the blocked_clients lock
        if let Some(client_id) = client_to_cleanup {
            self.cleanup_client(client_id);
            return true;
        }

        false
    }

    /// Signal multiple keys at once (e.g., after LPUSH with multiple values)
    pub fn signal_keys(&self, db_index: usize, keys: &[Bytes]) {
        for key in keys {
            self.signal_key(db_index, key);
        }
    }

    /// Unblock a specific client (for CLIENT UNBLOCK command)
    pub fn unblock_client(&self, client_id: u64, error: bool) -> bool {
        if let Some(reg) = self.client_registrations.get(&client_id) {
            let mut registration = reg.lock();
            if let Some(waker) = registration.waker.take() {
                let _ = waker.send(if error {
                    BlockResult::Unblocked
                } else {
                    BlockResult::Timeout
                });
            }
            drop(registration);
            drop(reg);
            self.cleanup_client(client_id);
            true
        } else {
            false
        }
    }

    /// Remove a client from all blocked queues (on disconnect or timeout)
    pub fn cleanup_client(&self, client_id: u64) {
        if let Some((_, reg)) = self.client_registrations.remove(&client_id) {
            let registration = reg.lock();
            for (db_index, key) in &registration.keys {
                let db_key = (*db_index, key.clone());
                if let Some(entry) = self.blocked_clients.get(&db_key) {
                    entry.lock().retain(|&id| id != client_id);
                }
            }
        }
    }

    /// Get count of blocked clients (for INFO command)
    pub fn blocked_client_count(&self) -> usize {
        self.client_registrations.len()
    }

    /// Check if a client is blocked
    pub fn is_client_blocked(&self, client_id: u64) -> bool {
        self.client_registrations.contains_key(&client_id)
    }

    /// Get the keys a client is blocked on
    pub fn get_blocked_keys(&self, client_id: u64) -> Option<Vec<(usize, Bytes)>> {
        self.client_registrations
            .get(&client_id)
            .map(|reg| reg.lock().keys.clone())
    }
}

impl Default for BlockingManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Helper functions for parsing blocking command arguments
// ============================================================================

/// Parse timeout argument (supports both integer and float seconds)
pub fn parse_timeout(arg: &[u8]) -> Result<Option<Duration>> {
    let s = std::str::from_utf8(arg).map_err(|_| Error::NotInteger)?;

    // Try parsing as float first (Redis 6.0+ supports fractional timeouts)
    if let Ok(secs) = s.parse::<f64>() {
        if secs < 0.0 {
            return Err(Error::Other("timeout is negative"));
        }
        if secs == 0.0 {
            return Ok(None); // 0 means block indefinitely
        }
        if secs > 9007199254740992.0 {
            return Err(Error::Other("timeout is out of range"));
        }
        return Ok(Some(Duration::from_secs_f64(secs)));
    }

    // Fall back to integer parsing
    let secs: i64 = s.parse().map_err(|_| Error::NotInteger)?;
    if secs < 0 {
        return Err(Error::Other("timeout is negative"));
    }
    if secs == 0 {
        return Ok(None);
    }
    Ok(Some(Duration::from_secs(secs as u64)))
}

/// Parse LEFT/RIGHT direction argument
pub fn parse_direction(arg: &[u8]) -> Result<PopDirection> {
    if arg.eq_ignore_ascii_case(b"LEFT") {
        Ok(PopDirection::Left)
    } else if arg.eq_ignore_ascii_case(b"RIGHT") {
        Ok(PopDirection::Right)
    } else {
        Err(Error::Syntax)
    }
}

/// Check if a command is a blocking list command
pub fn is_blocking_list_command(cmd: &[u8]) -> bool {
    cmd.eq_ignore_ascii_case(b"BLPOP")
        || cmd.eq_ignore_ascii_case(b"BRPOP")
        || cmd.eq_ignore_ascii_case(b"BLMOVE")
        || cmd.eq_ignore_ascii_case(b"BRPOPLPUSH")
        || cmd.eq_ignore_ascii_case(b"BLMPOP")
}

// ============================================================================
// Blocking operation execution
// ============================================================================

/// Try to execute BLPOP/BRPOP immediately, returns Some if data was available
pub fn try_pop(
    store: &Store,
    keys: &[Bytes],
    direction: PopDirection,
) -> Result<Option<(Bytes, Bytes)>> {
    for key in keys {
        // Check type first
        if let Some(key_type) = store.key_type(key) {
            if key_type != "list" {
                return Err(Error::WrongType);
            }
        }

        let result = match direction {
            PopDirection::Left => store.lpop(key, 1),
            PopDirection::Right => store.rpop(key, 1),
        };

        if let Ok(Some(values)) = result {
            if let Some(value) = values.into_iter().next() {
                return Ok(Some((key.clone(), value)));
            }
        }
    }
    Ok(None)
}

/// Try to execute BLMPOP immediately
pub fn try_mpop(
    store: &Store,
    keys: &[Bytes],
    direction: PopDirection,
    count: usize,
) -> Result<Option<(Bytes, Vec<Bytes>)>> {
    for key in keys {
        // Check type first
        if let Some(key_type) = store.key_type(key) {
            if key_type != "list" {
                return Err(Error::WrongType);
            }
        }

        let result = match direction {
            PopDirection::Left => store.lpop(key, count),
            PopDirection::Right => store.rpop(key, count),
        };

        if let Ok(Some(values)) = result {
            if !values.is_empty() {
                return Ok(Some((key.clone(), values)));
            }
        }
    }
    Ok(None)
}

/// Try to execute BLMOVE/BRPOPLPUSH immediately
/// Returns Ok(Some(value)) if data was moved
/// Returns Ok(None) if source is empty (caller should block)
/// Returns Err(WrongType) if source is wrong type OR if source has data but destination is wrong type
pub fn try_move(
    store: &Store,
    source: &Bytes,
    destination: &Bytes,
    wherefrom: PopDirection,
    whereto: PopDirection,
) -> Result<Option<Bytes>> {
    // Check source type first
    if let Some(key_type) = store.key_type(source) {
        if key_type != "list" {
            return Err(Error::WrongType);
        }
    }

    // Check if source has data before checking destination type
    // This allows blocking to happen even if destination is wrong type
    // (the error will be returned when data arrives)
    let source_has_data = store.llen(source).map(|len| len > 0).unwrap_or(false);

    if !source_has_data {
        // Source is empty - return None to indicate blocking needed
        // Don't check destination type yet - we'll check when data arrives
        return Ok(None);
    }

    // Source has data - now check destination type
    // If it's wrong type, we return an error (preventing the pop)
    if let Some(key_type) = store.key_type(destination) {
        if key_type != "list" {
            return Err(Error::WrongType);
        }
    }

    let from_left = wherefrom == PopDirection::Left;
    let to_left = whereto == PopDirection::Left;

    store.lmove(source, destination, from_left, to_left)
}

/// Format BLPOP/BRPOP response
pub fn format_pop_response(key: Bytes, value: Bytes) -> RespValue {
    RespValue::array(vec![RespValue::bulk(key), RespValue::bulk(value)])
}

/// Format BLMPOP response
pub fn format_mpop_response(key: Bytes, values: Vec<Bytes>) -> RespValue {
    RespValue::array(vec![
        RespValue::bulk(key),
        RespValue::array(values.into_iter().map(RespValue::bulk).collect()),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_timeout() {
        assert_eq!(parse_timeout(b"0").unwrap(), None);
        assert_eq!(parse_timeout(b"1").unwrap(), Some(Duration::from_secs(1)));
        assert_eq!(
            parse_timeout(b"0.5").unwrap(),
            Some(Duration::from_millis(500))
        );
        assert!(parse_timeout(b"-1").is_err());
        assert!(parse_timeout(b"invalid").is_err());
    }

    #[test]
    fn test_parse_direction() {
        assert_eq!(parse_direction(b"LEFT").unwrap(), PopDirection::Left);
        assert_eq!(parse_direction(b"left").unwrap(), PopDirection::Left);
        assert_eq!(parse_direction(b"RIGHT").unwrap(), PopDirection::Right);
        assert!(parse_direction(b"MIDDLE").is_err());
    }
}
