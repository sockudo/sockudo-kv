//! Sentinel State Management - Dragonfly-style high performance
//!
//! Lock-free data structures for tracking monitored instances.
//!
//! Key optimizations:
//! 1. Atomic flags using AtomicU32 bitfield
//! 2. Atomic timestamps for all time tracking
//! 3. DashMap for concurrent instance access
//! 4. Zero-copy Bytes for IDs

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use super::config::MasterConfig;
use super::scripts::PendingScript;

// =============================================================================
// Instance Flags (Atomic Bitfield)
// =============================================================================

/// Redis Sentinel instance flags as atomic bitfield
#[derive(Debug)]
pub struct InstanceFlags(AtomicU32);

impl InstanceFlags {
    // Flag bit positions (matching Redis sentinel.h)
    const MASTER: u32 = 1 << 0;
    const SLAVE: u32 = 1 << 1;
    const SENTINEL: u32 = 1 << 2;
    const S_DOWN: u32 = 1 << 3; // Subjectively down
    const O_DOWN: u32 = 1 << 4; // Objectively down
    const MASTER_DOWN: u32 = 1 << 5; // Master reported down by this instance
    const FAILOVER_IN_PROGRESS: u32 = 1 << 6;
    const PROMOTED: u32 = 1 << 7; // Replica promoted to master
    const RECONF_SENT: u32 = 1 << 8; // REPLICAOF sent during failover
    const RECONF_INPROG: u32 = 1 << 9; // Reconfiguration in progress
    const RECONF_DONE: u32 = 1 << 10; // Reconfiguration done
    const DISCONNECTED: u32 = 1 << 11;

    pub fn new() -> Self {
        Self(AtomicU32::new(0))
    }

    pub fn new_master() -> Self {
        Self(AtomicU32::new(Self::MASTER))
    }

    pub fn new_slave() -> Self {
        Self(AtomicU32::new(Self::SLAVE))
    }

    pub fn new_sentinel() -> Self {
        Self(AtomicU32::new(Self::SENTINEL))
    }

    #[inline]
    fn get(&self, flag: u32) -> bool {
        self.0.load(Ordering::Relaxed) & flag != 0
    }

    #[inline]
    fn set(&self, flag: u32, value: bool) {
        if value {
            self.0.fetch_or(flag, Ordering::Relaxed);
        } else {
            self.0.fetch_and(!flag, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn raw(&self) -> u32 {
        self.0.load(Ordering::Relaxed)
    }

    // Master flag
    pub fn is_master(&self) -> bool {
        self.get(Self::MASTER)
    }
    pub fn set_master(&self, v: bool) {
        self.set(Self::MASTER, v);
    }

    // Slave/Replica flag
    pub fn is_slave(&self) -> bool {
        self.get(Self::SLAVE)
    }
    pub fn set_slave(&self, v: bool) {
        self.set(Self::SLAVE, v);
    }

    // Sentinel flag
    pub fn is_sentinel(&self) -> bool {
        self.get(Self::SENTINEL)
    }
    pub fn set_sentinel(&self, v: bool) {
        self.set(Self::SENTINEL, v);
    }

    // SDOWN (Subjectively Down)
    pub fn is_sdown(&self) -> bool {
        self.get(Self::S_DOWN)
    }
    pub fn set_sdown(&self, v: bool) {
        self.set(Self::S_DOWN, v);
    }

    // ODOWN (Objectively Down)
    pub fn is_odown(&self) -> bool {
        self.get(Self::O_DOWN)
    }
    pub fn set_odown(&self, v: bool) {
        self.set(Self::O_DOWN, v);
    }

    // Master Down
    pub fn is_master_down(&self) -> bool {
        self.get(Self::MASTER_DOWN)
    }
    pub fn set_master_down(&self, v: bool) {
        self.set(Self::MASTER_DOWN, v);
    }

    // Failover in progress
    pub fn is_failover_in_progress(&self) -> bool {
        self.get(Self::FAILOVER_IN_PROGRESS)
    }
    pub fn set_failover_in_progress(&self, v: bool) {
        self.set(Self::FAILOVER_IN_PROGRESS, v);
    }

    // Promoted
    pub fn is_promoted(&self) -> bool {
        self.get(Self::PROMOTED)
    }
    pub fn set_promoted(&self, v: bool) {
        self.set(Self::PROMOTED, v);
    }

    // Reconf sent
    pub fn is_reconf_sent(&self) -> bool {
        self.get(Self::RECONF_SENT)
    }
    pub fn set_reconf_sent(&self, v: bool) {
        self.set(Self::RECONF_SENT, v);
    }

    // Reconf in progress
    pub fn is_reconf_inprog(&self) -> bool {
        self.get(Self::RECONF_INPROG)
    }
    pub fn set_reconf_inprog(&self, v: bool) {
        self.set(Self::RECONF_INPROG, v);
    }

    // Reconf done
    pub fn is_reconf_done(&self) -> bool {
        self.get(Self::RECONF_DONE)
    }
    pub fn set_reconf_done(&self, v: bool) {
        self.set(Self::RECONF_DONE, v);
    }

    // Disconnected
    pub fn is_disconnected(&self) -> bool {
        self.get(Self::DISCONNECTED)
    }
    pub fn set_disconnected(&self, v: bool) {
        self.set(Self::DISCONNECTED, v);
    }

    /// Format flags for display (matching Redis format)
    pub fn format(&self) -> String {
        let mut flags = Vec::with_capacity(12);

        if self.is_master() {
            flags.push("master");
        }
        if self.is_slave() {
            flags.push("slave");
        }
        if self.is_sentinel() {
            flags.push("sentinel");
        }
        if self.is_sdown() {
            flags.push("s_down");
        }
        if self.is_odown() {
            flags.push("o_down");
        }
        if self.is_master_down() {
            flags.push("master_down");
        }
        if self.is_failover_in_progress() {
            flags.push("failover_in_progress");
        }
        if self.is_promoted() {
            flags.push("promoted");
        }
        if self.is_reconf_sent() {
            flags.push("reconf_sent");
        }
        if self.is_reconf_inprog() {
            flags.push("reconf_inprog");
        }
        if self.is_reconf_done() {
            flags.push("reconf_done");
        }
        if self.is_disconnected() {
            flags.push("disconnected");
        }

        if flags.is_empty() {
            "none".to_string()
        } else {
            flags.join(",")
        }
    }
}

impl Default for InstanceFlags {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for InstanceFlags {
    fn clone(&self) -> Self {
        Self(AtomicU32::new(self.0.load(Ordering::Relaxed)))
    }
}

// =============================================================================
// Failover State
// =============================================================================

/// Failover state machine states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FailoverState {
    None = 0,
    WaitStart = 1,
    SelectSlave = 2,
    SendSlaveofNoone = 3,
    WaitPromotion = 4,
    ReconfSlaves = 5,
    UpdateConfig = 6,
}

impl FailoverState {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::WaitStart,
            2 => Self::SelectSlave,
            3 => Self::SendSlaveofNoone,
            4 => Self::WaitPromotion,
            5 => Self::ReconfSlaves,
            6 => Self::UpdateConfig,
            _ => Self::None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::WaitStart => "wait-start",
            Self::SelectSlave => "select-slave",
            Self::SendSlaveofNoone => "send-slaveof-noone",
            Self::WaitPromotion => "wait-promotion",
            Self::ReconfSlaves => "reconf-slaves",
            Self::UpdateConfig => "update-config",
        }
    }
}

// =============================================================================
// Replica Instance
// =============================================================================

/// Discovered replica instance with atomic fields
#[derive(Debug)]
pub struct ReplicaInstance {
    /// Replica IP address
    pub ip: String,
    /// Replica port
    pub port: u16,
    /// Redis run ID (40-char hex)
    pub runid: RwLock<String>,
    /// Instance flags (atomic)
    pub flags: InstanceFlags,
    /// Master host this replica is connected to
    pub master_host: RwLock<String>,
    /// Master port this replica is connected to
    pub master_port: AtomicU16,
    /// Master link status (up/down)
    pub master_link_status: AtomicBool,
    /// Time since master link down (milliseconds)
    pub master_link_down_time: AtomicU64,
    /// Replica priority (0 = never promote)
    pub replica_priority: AtomicU32,
    /// Replication offset
    pub repl_offset: AtomicI64,
    /// Last PING sent timestamp
    pub last_ping_time: AtomicU64,
    /// Last successful PONG timestamp
    pub last_pong_time: AtomicU64,
    /// Last INFO refresh timestamp
    pub info_refresh: AtomicU64,
    /// Role reported by INFO
    pub role_reported: RwLock<String>,
    /// Time when role was reported
    pub role_reported_time: AtomicU64,
    /// Pending commands on link
    pub link_pending_commands: AtomicU32,
    /// Link reference count
    pub link_refcount: AtomicU32,
}

impl ReplicaInstance {
    pub fn new(ip: String, port: u16) -> Self {
        let now = current_time_ms();
        Self {
            ip,
            port,
            runid: RwLock::new(String::new()),
            flags: InstanceFlags::new_slave(),
            master_host: RwLock::new(String::new()),
            master_port: AtomicU16::new(0),
            master_link_status: AtomicBool::new(false),
            master_link_down_time: AtomicU64::new(0),
            replica_priority: AtomicU32::new(100), // Default priority
            repl_offset: AtomicI64::new(-1),
            last_ping_time: AtomicU64::new(now),
            last_pong_time: AtomicU64::new(now),
            info_refresh: AtomicU64::new(0),
            role_reported: RwLock::new("slave".to_string()),
            role_reported_time: AtomicU64::new(now),
            link_pending_commands: AtomicU32::new(0),
            link_refcount: AtomicU32::new(1),
        }
    }

    /// Get address as SocketAddr
    pub fn addr(&self) -> Option<SocketAddr> {
        format!("{}:{}", self.ip, self.port).parse().ok()
    }

    /// Get unique key for this replica
    pub fn key(&self) -> String {
        format!("{}:{}", self.ip, self.port)
    }

    /// Check if replica is valid for failover
    pub fn is_valid_for_failover(&self, down_after_ms: u64) -> bool {
        // Not in SDOWN
        if self.flags.is_sdown() {
            return false;
        }

        // Not disconnected too long
        if self.flags.is_disconnected() {
            return false;
        }

        // Priority must be > 0
        if self.replica_priority.load(Ordering::Relaxed) == 0 {
            return false;
        }

        // Check master link down time
        let max_down_time = down_after_ms * 10;
        if self.master_link_down_time.load(Ordering::Relaxed) > max_down_time {
            return false;
        }

        true
    }
}

impl Clone for ReplicaInstance {
    fn clone(&self) -> Self {
        Self {
            ip: self.ip.clone(),
            port: self.port,
            runid: RwLock::new(self.runid.read().clone()),
            flags: self.flags.clone(),
            master_host: RwLock::new(self.master_host.read().clone()),
            master_port: AtomicU16::new(self.master_port.load(Ordering::Relaxed)),
            master_link_status: AtomicBool::new(self.master_link_status.load(Ordering::Relaxed)),
            master_link_down_time: AtomicU64::new(
                self.master_link_down_time.load(Ordering::Relaxed),
            ),
            replica_priority: AtomicU32::new(self.replica_priority.load(Ordering::Relaxed)),
            repl_offset: AtomicI64::new(self.repl_offset.load(Ordering::Relaxed)),
            last_ping_time: AtomicU64::new(self.last_ping_time.load(Ordering::Relaxed)),
            last_pong_time: AtomicU64::new(self.last_pong_time.load(Ordering::Relaxed)),
            info_refresh: AtomicU64::new(self.info_refresh.load(Ordering::Relaxed)),
            role_reported: RwLock::new(self.role_reported.read().clone()),
            role_reported_time: AtomicU64::new(self.role_reported_time.load(Ordering::Relaxed)),
            link_pending_commands: AtomicU32::new(
                self.link_pending_commands.load(Ordering::Relaxed),
            ),
            link_refcount: AtomicU32::new(self.link_refcount.load(Ordering::Relaxed)),
        }
    }
}

// =============================================================================
// Sentinel Instance (Other Sentinels)
// =============================================================================

/// Known Sentinel peer instance
#[derive(Debug)]
pub struct SentinelInstance {
    /// Sentinel ID (40-char hex myid)
    pub id: Bytes,
    /// Sentinel IP address
    pub ip: String,
    /// Sentinel port
    pub port: u16,
    /// Run ID
    pub runid: RwLock<String>,
    /// Instance flags
    pub flags: InstanceFlags,
    /// Last hello message timestamp
    pub last_hello_time: AtomicU64,
    /// Last PING sent timestamp
    pub last_ping_time: AtomicU64,
    /// Last PONG received timestamp
    pub last_pong_time: AtomicU64,
    /// Leader this sentinel voted for
    pub voted_leader: RwLock<Option<String>>,
    /// Epoch when vote was cast
    pub voted_leader_epoch: AtomicU64,
    /// Can failover its master
    pub can_failover: AtomicBool,
}

impl SentinelInstance {
    pub fn new(id: Bytes, ip: String, port: u16) -> Self {
        let now = current_time_ms();
        Self {
            id,
            ip,
            port,
            runid: RwLock::new(String::new()),
            flags: InstanceFlags::new_sentinel(),
            last_hello_time: AtomicU64::new(now),
            last_ping_time: AtomicU64::new(now),
            last_pong_time: AtomicU64::new(now),
            voted_leader: RwLock::new(None),
            voted_leader_epoch: AtomicU64::new(0),
            can_failover: AtomicBool::new(true),
        }
    }

    /// Get address as SocketAddr
    pub fn addr(&self) -> Option<SocketAddr> {
        format!("{}:{}", self.ip, self.port).parse().ok()
    }

    /// Get unique key for this sentinel
    pub fn key(&self) -> String {
        format!("{}:{}", self.ip, self.port)
    }
}

impl Clone for SentinelInstance {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            ip: self.ip.clone(),
            port: self.port,
            runid: RwLock::new(self.runid.read().clone()),
            flags: self.flags.clone(),
            last_hello_time: AtomicU64::new(self.last_hello_time.load(Ordering::Relaxed)),
            last_ping_time: AtomicU64::new(self.last_ping_time.load(Ordering::Relaxed)),
            last_pong_time: AtomicU64::new(self.last_pong_time.load(Ordering::Relaxed)),
            voted_leader: RwLock::new(self.voted_leader.read().clone()),
            voted_leader_epoch: AtomicU64::new(self.voted_leader_epoch.load(Ordering::Relaxed)),
            can_failover: AtomicBool::new(self.can_failover.load(Ordering::Relaxed)),
        }
    }
}

// =============================================================================
// Master Instance
// =============================================================================

/// Monitored master instance with full state
#[derive(Debug)]
pub struct MasterInstance {
    /// Master name (from config)
    pub name: String,
    /// Master IP address
    pub ip: RwLock<String>,
    /// Master port
    pub port: AtomicU16,
    /// Redis run ID (40-char hex)
    pub runid: RwLock<String>,
    /// Instance flags
    pub flags: InstanceFlags,
    /// Configuration for this master
    pub config: RwLock<MasterConfig>,
    /// Configuration epoch
    pub config_epoch: AtomicU64,
    /// Last PING sent timestamp
    pub last_ping_time: AtomicU64,
    /// Last successful PONG timestamp  
    pub last_pong_time: AtomicU64,
    /// Last successful ping reply timestamp
    pub last_ok_ping_reply: AtomicU64,
    /// Last INFO refresh timestamp
    pub info_refresh: AtomicU64,
    /// Role reported by INFO
    pub role_reported: RwLock<String>,
    /// Time when role was reported
    pub role_reported_time: AtomicU64,
    /// Pending commands on link
    pub link_pending_commands: AtomicU32,
    /// Link reference count
    pub link_refcount: AtomicU32,
    /// Connected replicas
    pub replicas: DashMap<String, Arc<ReplicaInstance>>,
    /// Known sentinels monitoring this master
    pub sentinels: DashMap<String, Arc<SentinelInstance>>,
    /// Number of replicas (cached)
    pub num_slaves: AtomicU32,
    /// Number of other sentinels (cached)
    pub num_other_sentinels: AtomicU32,
    /// Failover state
    pub failover_state: AtomicU8,
    /// Failover epoch
    pub failover_epoch: AtomicU64,
    /// Failover start time
    pub failover_start_time: AtomicU64,
    /// Elected leader for failover
    pub failover_leader: RwLock<Option<String>>,
    /// Leader epoch
    pub leader_epoch: AtomicU64,
    /// Promoted replica during failover
    pub promoted_replica: RwLock<Option<String>>,
    /// ODOWN since timestamp
    pub odown_since: AtomicU64,
    /// SDOWN since timestamp
    pub sdown_since: AtomicU64,
    /// Last failover attempt timestamp
    pub failover_state_change_time: AtomicU64,
}

impl MasterInstance {
    pub fn new(name: String, ip: String, port: u16, config: MasterConfig) -> Self {
        let now = current_time_ms();
        Self {
            name,
            ip: RwLock::new(ip),
            port: AtomicU16::new(port),
            runid: RwLock::new(String::new()),
            flags: InstanceFlags::new_master(),
            config: RwLock::new(config),
            config_epoch: AtomicU64::new(0),
            last_ping_time: AtomicU64::new(now),
            last_pong_time: AtomicU64::new(now),
            last_ok_ping_reply: AtomicU64::new(now),
            info_refresh: AtomicU64::new(0),
            role_reported: RwLock::new("master".to_string()),
            role_reported_time: AtomicU64::new(now),
            link_pending_commands: AtomicU32::new(0),
            link_refcount: AtomicU32::new(1),
            replicas: DashMap::new(),
            sentinels: DashMap::new(),
            num_slaves: AtomicU32::new(0),
            num_other_sentinels: AtomicU32::new(0),
            failover_state: AtomicU8::new(FailoverState::None as u8),
            failover_epoch: AtomicU64::new(0),
            failover_start_time: AtomicU64::new(0),
            failover_leader: RwLock::new(None),
            leader_epoch: AtomicU64::new(0),
            promoted_replica: RwLock::new(None),
            odown_since: AtomicU64::new(0),
            sdown_since: AtomicU64::new(0),
            failover_state_change_time: AtomicU64::new(0),
        }
    }

    /// Get address as SocketAddr
    pub fn addr(&self) -> Option<SocketAddr> {
        let ip = self.ip.read();
        let port = self.port.load(Ordering::Relaxed);
        format!("{}:{}", ip, port).parse().ok()
    }

    /// Get current failover state
    pub fn get_failover_state(&self) -> FailoverState {
        FailoverState::from_u8(self.failover_state.load(Ordering::Relaxed))
    }

    /// Set failover state
    pub fn set_failover_state(&self, state: FailoverState) {
        self.failover_state.store(state as u8, Ordering::Relaxed);
        self.failover_state_change_time
            .store(current_time_ms(), Ordering::Relaxed);
    }

    /// Get quorum value
    pub fn quorum(&self) -> u32 {
        self.config.read().quorum
    }

    /// Get down-after-milliseconds value
    pub fn down_after_ms(&self) -> u64 {
        self.config.read().down_after_ms
    }

    /// Get failover timeout
    pub fn failover_timeout(&self) -> u64 {
        self.config.read().failover_timeout
    }

    /// Get parallel syncs
    pub fn parallel_syncs(&self) -> u32 {
        self.config.read().parallel_syncs
    }

    /// Check if quorum is reached for ODOWN
    pub fn is_quorum_reached(&self) -> bool {
        let quorum = self.quorum();
        let mut votes: u32 = 1; // Count ourselves

        for entry in self.sentinels.iter() {
            if entry.value().flags.is_master_down() {
                votes += 1;
            }
        }

        votes >= quorum
    }

    /// Get number of sentinels that can vote (not in SDOWN)
    pub fn reachable_sentinels(&self) -> usize {
        self.sentinels
            .iter()
            .filter(|e| !e.value().flags.is_sdown())
            .count()
    }

    /// Update replica count cache
    pub fn update_counts(&self) {
        self.num_slaves
            .store(self.replicas.len() as u32, Ordering::Relaxed);
        self.num_other_sentinels
            .store(self.sentinels.len() as u32, Ordering::Relaxed);
    }

    /// Get the best replica for failover
    pub fn select_replica_for_failover(&self) -> Option<Arc<ReplicaInstance>> {
        let down_after = self.down_after_ms();

        let mut candidates: Vec<Arc<ReplicaInstance>> = self
            .replicas
            .iter()
            .map(|e| e.value().clone())
            .filter(|r| r.is_valid_for_failover(down_after))
            .collect();

        if candidates.is_empty() {
            return None;
        }

        // Sort by: priority (lower better), repl_offset (higher better), runid (lexicographic)
        candidates.sort_by(|a, b| {
            let prio_a = a.replica_priority.load(Ordering::Relaxed);
            let prio_b = b.replica_priority.load(Ordering::Relaxed);

            if prio_a != prio_b {
                return prio_a.cmp(&prio_b);
            }

            let offset_a = a.repl_offset.load(Ordering::Relaxed);
            let offset_b = b.repl_offset.load(Ordering::Relaxed);

            if offset_a != offset_b {
                return offset_b.cmp(&offset_a); // Higher offset is better
            }

            // Lexicographic by runid
            let runid_a = a.runid.read();
            let runid_b = b.runid.read();
            runid_a.cmp(&runid_b)
        });

        candidates.into_iter().next()
    }
}

// =============================================================================
// Global Sentinel State
// =============================================================================

/// Global Sentinel state container
#[derive(Debug)]
pub struct SentinelState {
    /// This Sentinel's unique ID (40-char hex)
    pub myid: Bytes,
    /// Current epoch (cluster-wide)
    pub current_epoch: AtomicU64,
    /// All monitored masters
    pub masters: DashMap<String, Arc<MasterInstance>>,
    /// TILT mode flag
    pub tilt_mode: AtomicBool,
    /// TILT mode start time
    pub tilt_start_time: AtomicU64,
    /// Number of running scripts
    pub running_scripts: AtomicU32,
    /// Pending scripts queue
    pub scripts_queue: RwLock<VecDeque<PendingScript>>,
    /// Scripts queue length limit
    pub scripts_queue_length: AtomicU32,
    /// Simulated failure flags (for testing)
    pub simulate_failure_flags: AtomicU32,
}

impl SentinelState {
    /// Create new sentinel state with the given ID
    pub fn new(myid: String) -> Self {
        Self {
            myid: Bytes::from(myid),
            current_epoch: AtomicU64::new(0),
            masters: DashMap::new(),
            tilt_mode: AtomicBool::new(false),
            tilt_start_time: AtomicU64::new(0),
            running_scripts: AtomicU32::new(0),
            scripts_queue: RwLock::new(VecDeque::new()),
            scripts_queue_length: AtomicU32::new(0),
            simulate_failure_flags: AtomicU32::new(0),
        }
    }

    /// Add a master to monitor
    pub fn add_master(&self, master: MasterInstance) -> Arc<MasterInstance> {
        let arc = Arc::new(master);
        let name = arc.name.clone();
        self.masters.insert(name, arc.clone());
        arc
    }

    /// Remove a master from monitoring
    pub fn remove_master(&self, name: &str) -> Option<Arc<MasterInstance>> {
        self.masters.remove(name).map(|(_, v)| v)
    }

    /// Get a master by name
    pub fn get_master(&self, name: &str) -> Option<Arc<MasterInstance>> {
        self.masters.get(name).map(|e| e.value().clone())
    }

    /// Get current epoch
    pub fn epoch(&self) -> u64 {
        self.current_epoch.load(Ordering::Relaxed)
    }

    /// Increment and get new epoch
    pub fn increment_epoch(&self) -> u64 {
        self.current_epoch.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Check if in TILT mode
    pub fn is_tilt(&self) -> bool {
        self.tilt_mode.load(Ordering::Relaxed)
    }

    /// Enter TILT mode
    pub fn enter_tilt(&self) {
        self.tilt_mode.store(true, Ordering::Relaxed);
        self.tilt_start_time
            .store(current_time_ms(), Ordering::Relaxed);
    }

    /// Exit TILT mode
    pub fn exit_tilt(&self) {
        self.tilt_mode.store(false, Ordering::Relaxed);
    }

    /// Get TILT duration in seconds (-1 if not in tilt)
    pub fn tilt_since_seconds(&self) -> i64 {
        if !self.is_tilt() {
            return -1;
        }
        let start = self.tilt_start_time.load(Ordering::Relaxed);
        let now = current_time_ms();
        ((now - start) / 1000) as i64
    }

    /// Get number of masters
    pub fn master_count(&self) -> usize {
        self.masters.len()
    }
}

impl Default for SentinelState {
    fn default() -> Self {
        Self::new(generate_sentinel_id())
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Get current time in milliseconds since UNIX epoch
#[inline]
pub fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Generate a random 40-character hex Sentinel ID
pub fn generate_sentinel_id() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    let random_part = fastrand::u64(..) as u128;
    let combined = now ^ random_part;

    format!(
        "{:016x}{:016x}{:08x}",
        combined,
        fastrand::u64(..) as u128 ^ now,
        fastrand::u32(..)
    )
}

/// Atomic u16 (using AtomicU32 internally due to limited platform support)
#[derive(Debug)]
pub struct AtomicU16(AtomicU32);

impl AtomicU16 {
    pub fn new(val: u16) -> Self {
        Self(AtomicU32::new(val as u32))
    }

    pub fn load(&self, order: Ordering) -> u16 {
        self.0.load(order) as u16
    }

    pub fn store(&self, val: u16, order: Ordering) {
        self.0.store(val as u32, order);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_instance_flags() {
        let flags = InstanceFlags::new_master();
        assert!(flags.is_master());
        assert!(!flags.is_slave());
        assert!(!flags.is_sdown());

        flags.set_sdown(true);
        assert!(flags.is_sdown());
        assert!(flags.format().contains("s_down"));

        flags.set_sdown(false);
        assert!(!flags.is_sdown());
    }

    #[test]
    fn test_replica_instance() {
        let replica = ReplicaInstance::new("127.0.0.1".to_string(), 6380);
        assert!(replica.flags.is_slave());
        assert_eq!(replica.key(), "127.0.0.1:6380");
    }

    #[test]
    fn test_master_instance() {
        let config = MasterConfig::new("mymaster".to_string(), "127.0.0.1".to_string(), 6379, 2);
        let master = MasterInstance::new(
            "mymaster".to_string(),
            "127.0.0.1".to_string(),
            6379,
            config,
        );

        assert!(master.flags.is_master());
        assert_eq!(master.quorum(), 2);
        assert_eq!(master.get_failover_state(), FailoverState::None);
    }

    #[test]
    fn test_sentinel_state() {
        let state = SentinelState::default();
        assert_eq!(state.myid.len(), 40);
        assert!(!state.is_tilt());

        state.enter_tilt();
        assert!(state.is_tilt());

        state.exit_tilt();
        assert!(!state.is_tilt());
    }
}
