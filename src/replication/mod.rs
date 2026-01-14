//! Redis Replication System
//!
//! Implements master-replica replication with:
//! - Full sync via RDB transfer
//! - Partial sync via replication backlog
//! - Command propagation to replicas
//! - REPLCONF ACK for offset tracking

pub mod master;
pub mod rdb;
pub mod replica;

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use tokio::sync::broadcast;

/// Replication role
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationRole {
    Master,
    Replica,
}

/// Replication state for a connected replica (on master side)
pub struct ConnectedReplica {
    pub id: u64,
    pub addr: SocketAddr,
    pub offset: AtomicI64,
    pub ack_offset: AtomicI64,
    /// Last time we received an ACK from this replica (unix timestamp)
    pub last_ack_time: AtomicU64,
    pub state: RwLock<ReplicaState>,
    pub capabilities: RwLock<Vec<String>>,
    pub tx: broadcast::Sender<Bytes>,
    /// Announced IP (from REPLCONF ip-address)
    pub announced_ip: RwLock<Option<String>>,
    /// Announced port (from REPLCONF listening-port)
    pub announced_port: RwLock<Option<u16>>,
}

/// Replica connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaState {
    /// Waiting for REPLCONF
    Handshake,
    /// Sending RDB for full sync
    SendingRdb,
    /// Online and receiving commands
    Online,
    /// Disconnected
    Disconnected,
}

/// Replication backlog for partial sync
pub struct ReplicationBacklog {
    /// Circular buffer of commands
    buffer: RwLock<VecDeque<Bytes>>,
    /// Maximum size in bytes (atomic for runtime changes)
    max_size: AtomicU64,
    /// Current size in bytes
    current_size: AtomicU64,
    /// Start offset in backlog
    start_offset: AtomicI64,
}

impl ReplicationBacklog {
    pub fn new(max_size: u64) -> Self {
        Self {
            // Dragonfly-style: minimal initial allocation, grows on demand
            buffer: RwLock::new(VecDeque::with_capacity(64)),
            max_size: AtomicU64::new(max_size),
            current_size: AtomicU64::new(0),
            start_offset: AtomicI64::new(0),
        }
    }

    /// Get current max size
    pub fn max_size(&self) -> u64 {
        self.max_size.load(Ordering::Relaxed)
    }

    /// Resize the backlog (for runtime repl_backlog_size changes)
    pub fn resize(&self, new_size: u64) {
        self.max_size.store(new_size, Ordering::Relaxed);
        // Trim if needed on next push
    }

    /// Add command to backlog
    pub fn push(&self, cmd: Bytes) {
        let cmd_len = cmd.len() as u64;
        let mut buffer = self.buffer.write();
        let max = self.max_size.load(Ordering::Relaxed);

        // Trim old entries if exceeding max size
        while self.current_size.load(Ordering::Relaxed) + cmd_len > max && !buffer.is_empty() {
            if let Some(old) = buffer.pop_front() {
                self.current_size
                    .fetch_sub(old.len() as u64, Ordering::Relaxed);
                self.start_offset
                    .fetch_add(old.len() as i64, Ordering::Relaxed);
            }
        }

        buffer.push_back(cmd);
        self.current_size.fetch_add(cmd_len, Ordering::Relaxed);
    }

    /// Get commands from offset for partial sync
    pub fn get_from_offset(&self, offset: i64) -> Option<Vec<Bytes>> {
        let start = self.start_offset.load(Ordering::Relaxed);
        if offset < start {
            return None; // Offset too old, need full sync
        }

        let buffer = self.buffer.read();
        let mut result = Vec::new();
        let mut current_offset = start;

        for cmd in buffer.iter() {
            if current_offset >= offset {
                result.push(cmd.clone());
            }
            current_offset += cmd.len() as i64;
        }

        Some(result)
    }
}

/// Main replication manager
pub struct ReplicationManager {
    /// Current role
    pub role: RwLock<ReplicationRole>,

    /// Replication ID (40 hex chars)
    pub repl_id: RwLock<String>,
    /// Secondary replication ID (for failover)
    pub repl_id2: RwLock<String>,

    /// Current replication offset
    pub master_repl_offset: AtomicI64,
    /// Secondary offset
    pub second_repl_offset: AtomicI64,

    /// Connected replicas (master side)
    pub replicas: DashMap<u64, Arc<ConnectedReplica>>,
    next_replica_id: AtomicU64,

    /// Replication backlog
    pub backlog: ReplicationBacklog,

    /// Master info (when we are a replica)
    pub master_host: RwLock<Option<String>>,
    pub master_port: RwLock<Option<u16>>,
    pub master_link_status: AtomicBool,

    /// Command broadcast channel
    pub cmd_tx: broadcast::Sender<Bytes>,
}

impl ReplicationManager {
    pub fn new() -> Self {
        Self::with_backlog_size(1024 * 1024) // 1MB default
    }

    /// Create with custom backlog size (for repl_backlog_size config)
    pub fn with_backlog_size(backlog_size: u64) -> Self {
        // Dragonfly-style: smaller buffer, memory efficient
        let (cmd_tx, _) = broadcast::channel(1024);

        Self {
            role: RwLock::new(ReplicationRole::Master),
            repl_id: RwLock::new(generate_repl_id()),
            repl_id2: RwLock::new("0".repeat(40)),
            master_repl_offset: AtomicI64::new(0),
            second_repl_offset: AtomicI64::new(-1),
            replicas: DashMap::new(),
            next_replica_id: AtomicU64::new(1),
            backlog: ReplicationBacklog::new(backlog_size),
            master_host: RwLock::new(None),
            master_port: RwLock::new(None),
            master_link_status: AtomicBool::new(false),
            cmd_tx,
        }
    }

    /// Get current role
    pub fn role(&self) -> ReplicationRole {
        *self.role.read()
    }

    /// Check if we are master
    pub fn is_master(&self) -> bool {
        self.role() == ReplicationRole::Master
    }

    /// Get replication ID
    pub fn repl_id(&self) -> String {
        self.repl_id.read().clone()
    }

    /// Get current offset
    pub fn offset(&self) -> i64 {
        self.master_repl_offset.load(Ordering::Relaxed)
    }

    /// Get connected replica count
    pub fn replica_count(&self) -> usize {
        self.replicas.len()
    }

    /// Register a new replica
    pub fn register_replica(&self, addr: SocketAddr) -> Arc<ConnectedReplica> {
        let id = self.next_replica_id.fetch_add(1, Ordering::Relaxed);
        // Dragonfly-style: smaller buffer for memory efficiency
        let (tx, _) = broadcast::channel(1024);

        let replica = Arc::new(ConnectedReplica {
            id,
            addr,
            offset: AtomicI64::new(0),
            ack_offset: AtomicI64::new(0),
            last_ack_time: AtomicU64::new(0),
            state: RwLock::new(ReplicaState::Handshake),
            capabilities: RwLock::new(Vec::new()),
            tx,
            announced_ip: RwLock::new(None),
            announced_port: RwLock::new(None),
        });

        self.replicas.insert(id, replica.clone());
        replica
    }

    /// Unregister a replica
    pub fn unregister_replica(&self, id: u64) {
        self.replicas.remove(&id);
    }

    /// Count replicas that have ACKed within max_lag_secs
    /// Used for min_replicas_to_write quorum check
    pub fn count_good_replicas(&self, max_lag_secs: u64) -> usize {
        if max_lag_secs == 0 {
            // No lag check, just count online replicas
            return self
                .replicas
                .iter()
                .filter(|r| *r.state.read() == ReplicaState::Online)
                .count();
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        self.replicas
            .iter()
            .filter(|r| {
                // Must be online
                if *r.state.read() != ReplicaState::Online {
                    return false;
                }
                // Check lag time
                let last_ack = r.last_ack_time.load(Ordering::Relaxed);
                if last_ack == 0 {
                    return false; // Never ACKed
                }
                now.saturating_sub(last_ack) <= max_lag_secs
            })
            .count()
    }

    /// Propagate command to all replicas
    pub fn propagate(&self, cmd: &[Bytes]) {
        if !self.is_master() || self.replicas.is_empty() {
            return;
        }

        // Serialize command to RESP
        let serialized = serialize_command(cmd);
        let cmd_len = serialized.len() as i64;

        // Add to backlog
        self.backlog.push(serialized.clone());

        // Update offset
        self.master_repl_offset
            .fetch_add(cmd_len, Ordering::Relaxed);

        // Broadcast to all replicas
        let _ = self.cmd_tx.send(serialized);
    }

    /// Set as replica of master
    pub fn set_replica_of(&self, host: Option<String>, port: Option<u16>) {
        let mut role = self.role.write();

        if host.is_none() {
            // REPLICAOF NO ONE - become master
            *role = ReplicationRole::Master;
            *self.master_host.write() = None;
            *self.master_port.write() = None;
            self.master_link_status.store(false, Ordering::Relaxed);
        } else {
            // Become replica
            *role = ReplicationRole::Replica;
            *self.master_host.write() = host;
            *self.master_port.write() = port;
        }
    }

    /// Get ROLE response
    pub fn role_response(&self) -> Vec<crate::protocol::RespValue> {
        use crate::protocol::RespValue;

        match self.role() {
            ReplicationRole::Master => {
                let mut replicas_info = Vec::new();
                for entry in self.replicas.iter() {
                    let replica = entry.value();
                    replicas_info.push(RespValue::array(vec![
                        RespValue::bulk_string(&replica.addr.ip().to_string()),
                        RespValue::bulk_string(&replica.addr.port().to_string()),
                        RespValue::bulk_string(&replica.offset.load(Ordering::Relaxed).to_string()),
                    ]));
                }
                vec![
                    RespValue::bulk_string("master"),
                    RespValue::integer(self.offset()),
                    RespValue::array(replicas_info),
                ]
            }
            ReplicationRole::Replica => {
                let host = self.master_host.read();
                let port = self.master_port.read();
                let state = if self.master_link_status.load(Ordering::Relaxed) {
                    "connected"
                } else {
                    "connect"
                };
                vec![
                    RespValue::bulk_string("slave"),
                    RespValue::bulk_string(host.as_deref().unwrap_or("")),
                    RespValue::integer(port.unwrap_or(0) as i64),
                    RespValue::bulk_string(state),
                    RespValue::integer(self.offset()),
                ]
            }
        }
    }

    /// Get INFO replication section
    pub fn info_replication(&self) -> String {
        let mut info = String::new();
        info.push_str("# Replication\r\n");

        match self.role() {
            ReplicationRole::Master => {
                info.push_str("role:master\r\n");
                info.push_str(&format!("connected_slaves:{}\r\n", self.replica_count()));

                for (i, entry) in self.replicas.iter().enumerate() {
                    let replica = entry.value();
                    let state = match *replica.state.read() {
                        ReplicaState::Online => "online",
                        ReplicaState::SendingRdb => "send_bulk",
                        _ => "wait_bgsave",
                    };

                    // Use announced IP/port if available, else fall back to socket address
                    let ip = replica
                        .announced_ip
                        .read()
                        .clone()
                        .unwrap_or_else(|| replica.addr.ip().to_string());
                    let port = replica
                        .announced_port
                        .read()
                        .unwrap_or_else(|| replica.addr.port());

                    // Calculate lag in seconds
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    let last_ack = replica.last_ack_time.load(Ordering::Relaxed);
                    let lag = if last_ack > 0 {
                        now.saturating_sub(last_ack)
                    } else {
                        0
                    };

                    info.push_str(&format!(
                        "slave{}:ip={},port={},state={},offset={},lag={}\r\n",
                        i,
                        ip,
                        port,
                        state,
                        replica.ack_offset.load(Ordering::Relaxed),
                        lag
                    ));
                }
            }
            ReplicationRole::Replica => {
                info.push_str("role:slave\r\n");
                let host = self.master_host.read();
                let port = self.master_port.read();
                info.push_str(&format!(
                    "master_host:{}\r\n",
                    host.as_deref().unwrap_or("")
                ));
                info.push_str(&format!("master_port:{}\r\n", port.unwrap_or(0)));
                let status = if self.master_link_status.load(Ordering::Relaxed) {
                    "up"
                } else {
                    "down"
                };
                info.push_str(&format!("master_link_status:{}\r\n", status));
            }
        }

        info.push_str(&format!("master_replid:{}\r\n", self.repl_id()));
        info.push_str(&format!("master_replid2:{}\r\n", self.repl_id2.read()));
        info.push_str(&format!("master_repl_offset:{}\r\n", self.offset()));
        info.push_str(&format!(
            "second_repl_offset:{}\r\n",
            self.second_repl_offset.load(Ordering::Relaxed)
        ));
        info.push_str("repl_backlog_active:1\r\n");
        info.push_str(&format!(
            "repl_backlog_size:{}\r\n",
            self.backlog.max_size()
        ));

        info
    }
}

impl Default for ReplicationManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Generate random 40-char replication ID
fn generate_repl_id() -> String {
    (0..40)
        .map(|_| format!("{:x}", fastrand::u8(..) % 16))
        .collect()
}

/// Serialize command to RESP format
fn serialize_command(parts: &[Bytes]) -> Bytes {
    let mut buf = Vec::new();
    buf.push(b'*');
    buf.extend_from_slice(itoa::Buffer::new().format(parts.len()).as_bytes());
    buf.extend_from_slice(b"\r\n");

    for part in parts {
        buf.push(b'$');
        buf.extend_from_slice(itoa::Buffer::new().format(part.len()).as_bytes());
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(part);
        buf.extend_from_slice(b"\r\n");
    }

    Bytes::from(buf)
}
