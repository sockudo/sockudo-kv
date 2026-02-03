//! Blocking operations manager for Redis blocking commands (BLPOP, BRPOP, etc.)
//!
//! This module implements Redis-compatible blocking list operations using an
//! event-driven architecture with tokio channels for efficient wakeup.
//!
//! FIFO ordering is maintained through a per-key mutex system. When data arrives
//! on a key, clients waiting on that key must acquire the key's mutex before
//! attempting to consume data, ensuring strict first-come-first-served ordering.

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
    /// Data is available on this key - client should try to pop
    Ready(Bytes),
    /// Operation timed out
    Timeout,
    /// Client was explicitly unblocked (CLIENT UNBLOCK)
    Unblocked,
}

/// Client registration info - tracks client and their waker
struct ClientRegistration {
    keys: Vec<(usize, Bytes)>,
    waker: Option<oneshot::Sender<BlockResult>>,
    /// Global sequence number for FIFO ordering across all keys
    sequence: u64,
}

/// FIFO lock for serializing access to a key's data
/// This ensures that when multiple clients are woken for the same key,
/// they attempt to pop in FIFO order
pub struct KeyLock {
    mutex: tokio::sync::Mutex<()>,
}

impl KeyLock {
    fn new() -> Self {
        Self {
            mutex: tokio::sync::Mutex::new(()),
        }
    }

    /// Acquire the lock, returning a guard that releases when dropped
    pub async fn lock(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.mutex.lock().await
    }
}

/// Manager for blocked clients
///
/// This is the central coordinator for all blocking operations. It maintains
/// a mapping from keys to blocked clients and handles signaling when data
/// becomes available.
///
/// FIFO ordering is maintained globally: when data arrives on any key, the
/// globally-first client that is waiting on that key will be woken. This ensures
/// that clients blocked on multiple keys are served in the order they blocked.
pub struct BlockingManager {
    /// Map from (db_index, key) -> queue of blocked client IDs
    /// Clients are served in FIFO order (first blocked = first served)
    blocked_clients: DashMap<(usize, Bytes), Mutex<VecDeque<u64>>>,

    /// Map from client_id -> registration info including waker
    /// The waker is stored here and taken when the client is woken
    client_registrations: DashMap<u64, Arc<Mutex<ClientRegistration>>>,

    /// Per-key locks for FIFO serialization
    /// When a client is woken up, it must acquire this lock before trying to pop
    key_locks: DashMap<(usize, Bytes), Arc<KeyLock>>,

    /// Global sequence counter for FIFO ordering
    next_sequence: std::sync::atomic::AtomicU64,
}

impl BlockingManager {
    pub fn new() -> Self {
        Self {
            blocked_clients: DashMap::new(),
            client_registrations: DashMap::new(),
            key_locks: DashMap::new(),
            next_sequence: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Get or create a lock for a specific key
    /// This lock must be held when attempting to pop data from the key
    pub fn get_key_lock(&self, db_index: usize, key: &Bytes) -> Arc<KeyLock> {
        let db_key = (db_index, key.clone());
        self.key_locks
            .entry(db_key)
            .or_insert_with(|| Arc::new(KeyLock::new()))
            .clone()
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

        // Assign a global sequence number for FIFO ordering
        let sequence = self
            .next_sequence
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Store client registration with waker
        self.client_registrations.insert(
            client_id,
            Arc::new(Mutex::new(ClientRegistration {
                keys: key_pairs.clone(),
                waker: Some(tx),
                sequence,
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
    /// Wakes up the client with the lowest sequence number waiting on this key
    /// Returns true if a client was woken up
    ///
    /// This maintains global FIFO ordering: when multiple keys receive data
    /// (e.g., from a transaction), the globally-first blocked client will be
    /// woken, regardless of which key is signaled first.
    ///
    /// Note: This does NOT cleanup the client - the client is responsible for
    /// cleaning up after themselves when they successfully complete, timeout,
    /// or re-register.
    pub fn signal_key(&self, db_index: usize, key: &Bytes) -> bool {
        let db_key = (db_index, key.clone());

        if let Some(entry) = self.blocked_clients.get(&db_key) {
            let mut queue = entry.lock();

            // Find the client with the lowest sequence number that hasn't been woken yet
            // This ensures global FIFO ordering across all keys
            let mut best_idx: Option<usize> = None;
            let mut best_seq: u64 = u64::MAX;
            let mut indices_to_remove = Vec::new();

            for (idx, &client_id) in queue.iter().enumerate() {
                if let Some(reg) = self.client_registrations.get(&client_id) {
                    let registration = reg.lock();
                    if registration.waker.is_some() {
                        // This client hasn't been woken yet
                        if registration.sequence < best_seq {
                            best_seq = registration.sequence;
                            best_idx = Some(idx);
                        }
                    } else {
                        // Waker already taken - mark for removal
                        indices_to_remove.push(idx);
                    }
                } else {
                    // Registration gone - mark for removal
                    indices_to_remove.push(idx);
                }
            }

            // Remove stale entries (in reverse order to maintain indices)
            for idx in indices_to_remove.into_iter().rev() {
                queue.remove(idx);
            }

            // Wake the best candidate
            if let Some(idx) = best_idx {
                let client_id = queue[idx];
                if let Some(reg) = self.client_registrations.get(&client_id) {
                    let mut registration = reg.lock();
                    if let Some(waker) = registration.waker.take() {
                        if waker.send(BlockResult::Ready(key.clone())).is_ok() {
                            queue.remove(idx);
                            return true;
                        }
                    }
                }
            }
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

    /// Handle SWAPDB - swap database indices for blocked clients and signal them
    ///
    /// When databases are swapped, blocked clients waiting on keys in either database
    /// need to be notified because the data has changed. We:
    /// 1. Swap the db_index in blocked_clients map entries
    /// 2. Update client registrations to reflect new db indices
    /// 3. Signal all affected clients so they re-check for data
    pub fn handle_swapdb(&self, db1: usize, db2: usize) {
        if db1 == db2 {
            return;
        }

        // Collect all keys from both databases that have blocked clients
        let mut db1_keys: Vec<Bytes> = Vec::new();
        let mut db2_keys: Vec<Bytes> = Vec::new();

        // We need to swap entries in blocked_clients map
        // First, collect all entries for both databases
        let mut entries_to_swap: Vec<((usize, Bytes), Mutex<VecDeque<u64>>)> = Vec::new();

        self.blocked_clients.retain(|(db_idx, key), queue| {
            if *db_idx == db1 {
                db1_keys.push(key.clone());
                entries_to_swap.push((
                    (db2, key.clone()),
                    std::mem::replace(queue, Mutex::new(VecDeque::new())),
                ));
                false // Remove this entry
            } else if *db_idx == db2 {
                db2_keys.push(key.clone());
                entries_to_swap.push((
                    (db1, key.clone()),
                    std::mem::replace(queue, Mutex::new(VecDeque::new())),
                ));
                false // Remove this entry
            } else {
                true // Keep entries from other databases
            }
        });

        // Re-insert with swapped db indices
        for (key, queue) in entries_to_swap {
            self.blocked_clients.insert(key, queue);
        }

        // Update client registrations to reflect new db indices
        for reg in self.client_registrations.iter() {
            let mut registration = reg.value().lock();
            for (db_idx, _key) in &mut registration.keys {
                if *db_idx == db1 {
                    *db_idx = db2;
                } else if *db_idx == db2 {
                    *db_idx = db1;
                }
            }
        }

        // Also swap key_locks entries
        let mut lock_entries_to_swap: Vec<((usize, Bytes), Arc<KeyLock>)> = Vec::new();
        self.key_locks.retain(|(db_idx, key), lock| {
            if *db_idx == db1 {
                lock_entries_to_swap.push(((db2, key.clone()), lock.clone()));
                false
            } else if *db_idx == db2 {
                lock_entries_to_swap.push(((db1, key.clone()), lock.clone()));
                false
            } else {
                true
            }
        });
        for (key, lock) in lock_entries_to_swap {
            self.key_locks.insert(key, lock);
        }

        // Signal all affected keys in their NEW database locations
        // db1_keys are now in db2, db2_keys are now in db1
        for key in db1_keys {
            self.signal_key(db2, &key);
        }
        for key in db2_keys {
            self.signal_key(db1, &key);
        }
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
/// If `priority_key` is provided, try that key first before others
pub fn try_pop(
    store: &Store,
    keys: &[Bytes],
    direction: PopDirection,
    priority_key: Option<&Bytes>,
) -> Result<Option<(Bytes, Bytes)>> {
    // If we have a priority key, try it first
    if let Some(pkey) = priority_key {
        if keys.contains(pkey) {
            if let Some(key_type) = store.key_type(pkey) {
                if key_type != "list" {
                    return Err(Error::WrongType);
                }
            }

            let result = match direction {
                PopDirection::Left => store.lpop(pkey, 1),
                PopDirection::Right => store.rpop(pkey, 1),
            };

            if let Ok(Some(values)) = result {
                if let Some(value) = values.into_iter().next() {
                    return Ok(Some((pkey.clone(), value)));
                }
            }
        }
    }

    // Fall back to trying all keys in order
    for key in keys {
        // Skip priority key since we already tried it
        if priority_key.map_or(false, |pk| pk == key) {
            continue;
        }

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
/// If `priority_key` is provided, try that key first before others
pub fn try_mpop(
    store: &Store,
    keys: &[Bytes],
    direction: PopDirection,
    count: usize,
    priority_key: Option<&Bytes>,
) -> Result<Option<(Bytes, Vec<Bytes>)>> {
    // If we have a priority key, try it first
    if let Some(pkey) = priority_key {
        if keys.contains(pkey) {
            if let Some(key_type) = store.key_type(pkey) {
                if key_type != "list" {
                    return Err(Error::WrongType);
                }
            }

            let result = match direction {
                PopDirection::Left => store.lpop(pkey, count),
                PopDirection::Right => store.rpop(pkey, count),
            };

            if let Ok(Some(values)) = result {
                if !values.is_empty() {
                    return Ok(Some((pkey.clone(), values)));
                }
            }
        }
    }

    // Fall back to trying all keys in order
    for key in keys {
        // Skip priority key since we already tried it
        if priority_key.map_or(false, |pk| pk == key) {
            continue;
        }

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
