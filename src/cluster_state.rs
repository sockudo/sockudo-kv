//! Redis Cluster state management - Dragonfly-style high performance
//!
//! Key optimizations:
//! 1. Lock-free slot bitmap using atomics
//! 2. Per-slot atomic ownership (no global RwLock)
//! 3. O(1) IP:Port index for node lookups
//! 4. O(1) Master->Replicas index
//! 5. Atomic node fields (no per-field RwLocks)
//! 6. Pre-allocated buffers to reduce allocation churn

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

/// Total number of hash slots in Redis Cluster
pub const CLUSTER_SLOTS: usize = 16384;

/// Number of AtomicU64 needed for slot bitmap (16384 / 64 = 256)
const SLOT_BITMAP_SIZE: usize = CLUSTER_SLOTS / 64;

/// Node ID length (40 hex characters)
pub const NODE_ID_LEN: usize = 40;

// =============================================================================
// Lock-free Slot Bitmap
// =============================================================================

/// Lock-free slot bitmap using atomic operations
/// Each bit represents ownership of a slot (0-16383)
#[derive(Debug)]
pub struct SlotBitmap {
    bits: [AtomicU64; SLOT_BITMAP_SIZE],
}

impl Default for SlotBitmap {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for SlotBitmap {
    fn clone(&self) -> Self {
        let bits = std::array::from_fn(|i| AtomicU64::new(self.bits[i].load(Ordering::Relaxed)));
        Self { bits }
    }
}

impl SlotBitmap {
    /// Create empty slot bitmap - O(1)
    #[inline]
    pub fn new() -> Self {
        Self {
            bits: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    /// Check if slot is owned - O(1)
    #[inline]
    pub fn has_slot(&self, slot: u16) -> bool {
        if slot >= CLUSTER_SLOTS as u16 {
            return false;
        }
        let idx = slot as usize / 64;
        let bit = slot as usize % 64;
        (self.bits[idx].load(Ordering::Relaxed) >> bit) & 1 == 1
    }

    /// Set slot as owned - O(1) atomic
    #[inline]
    pub fn set_slot(&self, slot: u16) {
        if slot >= CLUSTER_SLOTS as u16 {
            return;
        }
        let idx = slot as usize / 64;
        let bit = slot as usize % 64;
        self.bits[idx].fetch_or(1u64 << bit, Ordering::Relaxed);
    }

    /// Clear slot ownership - O(1) atomic
    #[inline]
    pub fn clear_slot(&self, slot: u16) {
        if slot >= CLUSTER_SLOTS as u16 {
            return;
        }
        let idx = slot as usize / 64;
        let bit = slot as usize % 64;
        self.bits[idx].fetch_and(!(1u64 << bit), Ordering::Relaxed);
    }

    /// Clear all slots - O(256)
    #[inline]
    pub fn clear_all(&self) {
        for bits in &self.bits {
            bits.store(0, Ordering::Relaxed);
        }
    }

    /// Count owned slots - O(256)
    pub fn count(&self) -> usize {
        self.bits
            .iter()
            .map(|b| b.load(Ordering::Relaxed).count_ones() as usize)
            .sum()
    }

    /// Get all owned slots as ranges - O(16384) but optimized
    pub fn get_ranges(&self) -> Vec<(u16, u16)> {
        let mut ranges = Vec::with_capacity(16);
        let mut start: Option<u16> = None;

        for (word_idx, bits) in self.bits.iter().enumerate() {
            let word = bits.load(Ordering::Relaxed);
            if word == 0 {
                // No bits set in this word
                if let Some(s) = start {
                    ranges.push((s, (word_idx * 64 - 1) as u16));
                    start = None;
                }
                continue;
            }
            if word == u64::MAX {
                // All bits set
                if start.is_none() {
                    start = Some((word_idx * 64) as u16);
                }
                continue;
            }
            // Mixed - check each bit
            for bit in 0..64 {
                let slot = (word_idx * 64 + bit) as u16;
                if slot >= CLUSTER_SLOTS as u16 {
                    break;
                }
                if (word >> bit) & 1 == 1 {
                    if start.is_none() {
                        start = Some(slot);
                    }
                } else if let Some(s) = start {
                    ranges.push((s, slot - 1));
                    start = None;
                }
            }
        }

        if let Some(s) = start {
            ranges.push((s, CLUSTER_SLOTS as u16 - 1));
        }

        ranges
    }

    /// Serialize to bytes for gossip - O(256)
    #[inline]
    pub fn to_bytes(&self, out: &mut [u8; 2048]) {
        for (i, bits) in self.bits.iter().enumerate() {
            let word = bits.load(Ordering::Relaxed);
            let bytes = word.to_le_bytes();
            out[i * 8..(i + 1) * 8].copy_from_slice(&bytes);
        }
    }

    /// Load from bytes - O(256)
    #[inline]
    pub fn from_bytes(&self, data: &[u8]) {
        if data.len() < 2048 {
            return;
        }
        for (i, bits) in self.bits.iter().enumerate() {
            let word = u64::from_le_bytes([
                data[i * 8],
                data[i * 8 + 1],
                data[i * 8 + 2],
                data[i * 8 + 3],
                data[i * 8 + 4],
                data[i * 8 + 5],
                data[i * 8 + 6],
                data[i * 8 + 7],
            ]);
            bits.store(word, Ordering::Relaxed);
        }
    }
}

// =============================================================================
// Per-Slot Atomic Ownership Table
// =============================================================================

/// Slot owner index - O(1) lookup with no locking
/// Uses u32 indices into a node table instead of Bytes clones
/// 0 = unassigned, 1 = self, 2+ = other node index
#[derive(Debug)]
pub struct SlotOwnerTable {
    /// Owner index for each slot (0 = none, 1 = self, 2+ = node index)
    owners: [AtomicU32; CLUSTER_SLOTS],
}

impl Default for SlotOwnerTable {
    fn default() -> Self {
        Self::new()
    }
}

impl SlotOwnerTable {
    pub fn new() -> Self {
        Self {
            owners: std::array::from_fn(|_| AtomicU32::new(0)),
        }
    }

    /// Get owner index for slot - O(1)
    #[inline]
    pub fn get(&self, slot: u16) -> u32 {
        if slot >= CLUSTER_SLOTS as u16 {
            return 0;
        }
        self.owners[slot as usize].load(Ordering::Relaxed)
    }

    /// Set owner index for slot - O(1)
    #[inline]
    pub fn set(&self, slot: u16, owner_idx: u32) {
        if slot >= CLUSTER_SLOTS as u16 {
            return;
        }
        self.owners[slot as usize].store(owner_idx, Ordering::Relaxed);
    }

    /// Clear slot ownership - O(1)
    #[inline]
    pub fn clear(&self, slot: u16) {
        self.set(slot, 0);
    }

    /// Clear all slots - O(16384)
    pub fn clear_all(&self) {
        for owner in &self.owners {
            owner.store(0, Ordering::Relaxed);
        }
    }
}

// =============================================================================
// Node Role & Flags (Atomic)
// =============================================================================

/// Node role encoded as atomic u8
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NodeRole {
    Master = 0,
    Replica = 1,
}

impl NodeRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeRole::Master => "master",
            NodeRole::Replica => "slave",
        }
    }

    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => NodeRole::Replica,
            _ => NodeRole::Master,
        }
    }
}

/// Node flags as atomic bitfield
#[derive(Debug)]
pub struct AtomicNodeFlags(AtomicU16);

impl Default for AtomicNodeFlags {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for AtomicNodeFlags {
    fn clone(&self) -> Self {
        Self(AtomicU16::new(self.0.load(Ordering::Relaxed)))
    }
}

impl AtomicNodeFlags {
    const MYSELF: u16 = 1 << 0;
    const MASTER: u16 = 1 << 1;
    const SLAVE: u16 = 1 << 2;
    const PFAIL: u16 = 1 << 3;
    const FAIL: u16 = 1 << 4;
    const HANDSHAKE: u16 = 1 << 5;
    const NOADDR: u16 = 1 << 6;
    const NOFAILOVER: u16 = 1 << 7;

    pub fn new() -> Self {
        Self(AtomicU16::new(0))
    }

    #[inline]
    fn get(&self, flag: u16) -> bool {
        self.0.load(Ordering::Relaxed) & flag != 0
    }

    #[inline]
    fn set(&self, flag: u16, value: bool) {
        if value {
            self.0.fetch_or(flag, Ordering::Relaxed);
        } else {
            self.0.fetch_and(!flag, Ordering::Relaxed);
        }
    }

    pub fn myself(&self) -> bool { self.get(Self::MYSELF) }
    pub fn set_myself(&self, v: bool) { self.set(Self::MYSELF, v); }

    pub fn master(&self) -> bool { self.get(Self::MASTER) }
    pub fn set_master(&self, v: bool) { self.set(Self::MASTER, v); }

    pub fn slave(&self) -> bool { self.get(Self::SLAVE) }
    pub fn set_slave(&self, v: bool) { self.set(Self::SLAVE, v); }

    pub fn pfail(&self) -> bool { self.get(Self::PFAIL) }
    pub fn set_pfail(&self, v: bool) { self.set(Self::PFAIL, v); }

    pub fn fail(&self) -> bool { self.get(Self::FAIL) }
    pub fn set_fail(&self, v: bool) { self.set(Self::FAIL, v); }

    pub fn handshake(&self) -> bool { self.get(Self::HANDSHAKE) }
    pub fn set_handshake(&self, v: bool) { self.set(Self::HANDSHAKE, v); }

    pub fn noaddr(&self) -> bool { self.get(Self::NOADDR) }
    pub fn set_noaddr(&self, v: bool) { self.set(Self::NOADDR, v); }

    pub fn nofailover(&self) -> bool { self.get(Self::NOFAILOVER) }
    pub fn set_nofailover(&self, v: bool) { self.set(Self::NOFAILOVER, v); }

    pub fn format(&self) -> String {
        let mut flags = Vec::with_capacity(8);
        if self.myself() { flags.push("myself"); }
        if self.master() { flags.push("master"); }
        if self.slave() { flags.push("slave"); }
        if self.pfail() { flags.push("fail?"); }
        if self.fail() { flags.push("fail"); }
        if self.handshake() { flags.push("handshake"); }
        if self.noaddr() { flags.push("noaddr"); }
        if self.nofailover() { flags.push("nofailover"); }
        if flags.is_empty() {
            "noflags".to_string()
        } else {
            flags.join(",")
        }
    }
}

/// Legacy flags struct for compatibility
#[derive(Debug, Clone, Copy, Default)]
pub struct NodeFlags {
    pub myself: bool,
    pub master: bool,
    pub slave: bool,
    pub pfail: bool,
    pub fail: bool,
    pub handshake: bool,
    pub noaddr: bool,
    pub nofailover: bool,
}

impl NodeFlags {
    pub fn format(&self) -> String {
        let mut flags = Vec::new();
        if self.myself { flags.push("myself"); }
        if self.master { flags.push("master"); }
        if self.slave { flags.push("slave"); }
        if self.pfail { flags.push("fail?"); }
        if self.fail { flags.push("fail"); }
        if self.handshake { flags.push("handshake"); }
        if self.noaddr { flags.push("noaddr"); }
        if self.nofailover { flags.push("nofailover"); }
        if flags.is_empty() {
            "noflags".to_string()
        } else {
            flags.join(",")
        }
    }
}

// =============================================================================
// Cluster Node (Mostly Lock-Free)
// =============================================================================

/// Cluster node information with atomic fields
#[derive(Debug)]
pub struct ClusterNode {
    /// 40-character hex node ID
    pub id: Bytes,
    /// IP address (immutable after creation)
    pub ip: String,
    /// Client port
    pub port: u16,
    /// Cluster bus port
    pub cport: u16,
    /// TLS port
    pub tls_port: u16,
    /// Node role (atomic)
    role: AtomicU8,
    /// Master node ID (RwLock for rare updates)
    pub master_id: RwLock<Option<Bytes>>,
    /// Ping sent timestamp
    pub ping_sent: AtomicU64,
    /// Pong received timestamp
    pub pong_recv: AtomicU64,
    /// Config epoch
    pub config_epoch: AtomicU64,
    /// Node flags (atomic bitfield)
    pub flags: AtomicNodeFlags,
    /// Slots owned by this node (lock-free bitmap)
    pub slots: SlotBitmap,
    /// Failure reports from other nodes
    pub failure_reports: DashMap<Bytes, u64>,
    /// Link state (0=disconnected, 1=connected)
    pub link_state: AtomicU8,
    /// Node index for slot owner table
    pub node_index: AtomicU32,
}

impl Clone for ClusterNode {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            ip: self.ip.clone(),
            port: self.port,
            cport: self.cport,
            tls_port: self.tls_port,
            role: AtomicU8::new(self.role.load(Ordering::Relaxed)),
            master_id: RwLock::new(self.master_id.read().clone()),
            ping_sent: AtomicU64::new(self.ping_sent.load(Ordering::Relaxed)),
            pong_recv: AtomicU64::new(self.pong_recv.load(Ordering::Relaxed)),
            config_epoch: AtomicU64::new(self.config_epoch.load(Ordering::Relaxed)),
            flags: self.flags.clone(),
            slots: self.slots.clone(),
            failure_reports: self.failure_reports.clone(),
            link_state: AtomicU8::new(self.link_state.load(Ordering::Relaxed)),
            node_index: AtomicU32::new(self.node_index.load(Ordering::Relaxed)),
        }
    }
}

impl ClusterNode {
    /// Create a new cluster node
    pub fn new(id: Bytes, ip: String, port: u16) -> Self {
        Self {
            id,
            ip,
            port,
            cport: port + 10000,
            tls_port: 0,
            role: AtomicU8::new(NodeRole::Master as u8),
            master_id: RwLock::new(None),
            ping_sent: AtomicU64::new(0),
            pong_recv: AtomicU64::new(0),
            config_epoch: AtomicU64::new(0),
            flags: AtomicNodeFlags::new(),
            slots: SlotBitmap::new(),
            failure_reports: DashMap::new(),
            link_state: AtomicU8::new(0),
            node_index: AtomicU32::new(0),
        }
    }

    /// Get node role - O(1)
    #[inline]
    pub fn get_role(&self) -> NodeRole {
        NodeRole::from_u8(self.role.load(Ordering::Relaxed))
    }

    /// Set node role - O(1)
    #[inline]
    pub fn set_role(&self, role: NodeRole) {
        self.role.store(role as u8, Ordering::Relaxed);
    }

    /// Format node info for CLUSTER NODES output
    pub fn format_nodes_line(&self) -> String {
        let master_id = self
            .master_id
            .read()
            .as_ref()
            .map(|s| String::from_utf8_lossy(s).to_string())
            .unwrap_or_else(|| "-".to_string());

        let slots = self.format_slots();

        format!(
            "{} {}:{}@{} {} {} {} {} {} connected{}",
            String::from_utf8_lossy(&self.id),
            self.ip,
            self.port,
            self.cport,
            self.flags.format(),
            master_id,
            self.ping_sent.load(Ordering::Relaxed),
            self.pong_recv.load(Ordering::Relaxed),
            self.config_epoch.load(Ordering::Relaxed),
            if slots.is_empty() {
                String::new()
            } else {
                format!(" {}", slots)
            }
        )
    }

    /// Format slots for CLUSTER NODES output
    fn format_slots(&self) -> String {
        self.slots
            .get_ranges()
            .iter()
            .map(|(start, end)| {
                if start == end {
                    format!("{}", start)
                } else {
                    format!("{}-{}", start, end)
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    }
}

// =============================================================================
// Cluster Link Info
// =============================================================================

/// Link direction for CLUSTER LINKS
#[derive(Debug, Clone, Copy)]
pub enum LinkDirection {
    To,
    From,
}

/// TCP link info for CLUSTER LINKS
#[derive(Debug, Clone)]
pub struct ClusterLink {
    pub direction: LinkDirection,
    pub node_id: Bytes,
    pub create_time: u64,
    pub events: String,
    pub send_buffer_allocated: u64,
    pub send_buffer_used: u64,
}

// =============================================================================
// Slot State
// =============================================================================

/// Slot migration state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    Normal,
    Migrating,
    Importing,
}

// =============================================================================
// IP:Port Index for O(1) Node Lookup
// =============================================================================

/// Composite key for IP:Port lookup
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct IpPortKey {
    pub ip: String,
    pub port: u16,
}

impl IpPortKey {
    pub fn new(ip: impl Into<String>, port: u16) -> Self {
        Self { ip: ip.into(), port }
    }
}

// =============================================================================
// Main Cluster State (Dragonfly-Style)
// =============================================================================

/// High-performance cluster state
pub struct ClusterState {
    /// This node's ID (40 hex chars)
    pub my_id: Bytes,
    /// This node's shard ID (7.2+)
    pub my_shard_id: Bytes,
    /// Current epoch (cluster-wide)
    pub current_epoch: AtomicU64,
    /// Config epoch (this node's config version)
    pub config_epoch: AtomicU64,
    /// Cluster enabled flag
    pub enabled: AtomicBool,
    /// Cluster state OK flag
    pub state_ok: AtomicBool,
    /// Slots owned by this node (lock-free)
    pub slots: SlotBitmap,
    /// All known nodes (node_id -> node)
    pub nodes: DashMap<Bytes, Arc<ClusterNode>>,
    /// Node index counter for slot owner table
    node_index_counter: AtomicU32,
    /// Node index -> Node ID mapping
    node_index_map: DashMap<u32, Bytes>,
    /// IP:Port -> Node ID index for O(1) lookup
    ip_port_index: DashMap<IpPortKey, Bytes>,
    /// Master -> Replicas index
    master_replicas: DashMap<Bytes, Vec<Bytes>>,
    /// Slots being migrated (slot -> target_node_id)
    pub migrating: DashMap<u16, Bytes>,
    /// Slots being imported (slot -> source_node_id)
    pub importing: DashMap<u16, Bytes>,
    /// TCP links to other nodes
    pub links: DashMap<Bytes, ClusterLink>,
    /// This node's IP
    pub my_ip: RwLock<String>,
    /// This node's port
    pub my_port: AtomicU64,
    /// Cluster size (number of master nodes)
    pub size: AtomicU64,
    /// Known slots count
    pub slots_assigned: AtomicU64,
    /// Slots OK count
    pub slots_ok: AtomicU64,
    /// Slots in PFAIL state
    pub slots_pfail: AtomicU64,
    /// Slots in FAIL state
    pub slots_fail: AtomicU64,
    /// Messages received
    pub messages_received: AtomicU64,
    /// Messages sent
    pub messages_sent: AtomicU64,
    /// Per-slot owner table (lock-free u32 indices)
    slot_owners: SlotOwnerTable,
    /// Legacy compatibility: slot -> node_id mapping
    /// Only used for commands that need actual Bytes, protected by RwLock
    pub slots_owner: RwLock<[Option<Bytes>; CLUSTER_SLOTS]>,
}

impl Default for ClusterState {
    fn default() -> Self {
        Self::new()
    }
}

impl ClusterState {
    /// Create new cluster state with random node ID
    pub fn new() -> Self {
        let my_id = generate_node_id();
        let my_shard_id = generate_node_id();

        Self {
            my_id,
            my_shard_id,
            current_epoch: AtomicU64::new(0),
            config_epoch: AtomicU64::new(0),
            enabled: AtomicBool::new(false),
            state_ok: AtomicBool::new(false),
            slots: SlotBitmap::new(),
            nodes: DashMap::new(),
            node_index_counter: AtomicU32::new(2), // 0=none, 1=self, 2+=others
            node_index_map: DashMap::new(),
            ip_port_index: DashMap::new(),
            master_replicas: DashMap::new(),
            migrating: DashMap::new(),
            importing: DashMap::new(),
            links: DashMap::new(),
            my_ip: RwLock::new("127.0.0.1".to_string()),
            my_port: AtomicU64::new(6379),
            size: AtomicU64::new(0),
            slots_assigned: AtomicU64::new(0),
            slots_ok: AtomicU64::new(0),
            slots_pfail: AtomicU64::new(0),
            slots_fail: AtomicU64::new(0),
            messages_received: AtomicU64::new(0),
            messages_sent: AtomicU64::new(0),
            slot_owners: SlotOwnerTable::new(),
            slots_owner: RwLock::new(std::array::from_fn(|_| None)),
        }
    }

    // =========================================================================
    // Cluster Info
    // =========================================================================

    /// Get CLUSTER INFO output - O(1)
    pub fn format_info(&self) -> String {
        let state = if self.state_ok.load(Ordering::Relaxed) { "ok" } else { "fail" };

        format!(
            "cluster_state:{}\r\n\
             cluster_slots_assigned:{}\r\n\
             cluster_slots_ok:{}\r\n\
             cluster_slots_pfail:{}\r\n\
             cluster_slots_fail:{}\r\n\
             cluster_known_nodes:{}\r\n\
             cluster_size:{}\r\n\
             cluster_current_epoch:{}\r\n\
             cluster_my_epoch:{}\r\n\
             cluster_stats_messages_sent:{}\r\n\
             cluster_stats_messages_received:{}\r\n",
            state,
            self.slots_assigned.load(Ordering::Relaxed),
            self.slots_ok.load(Ordering::Relaxed),
            self.slots_pfail.load(Ordering::Relaxed),
            self.slots_fail.load(Ordering::Relaxed),
            self.nodes.len() + 1,
            self.size.load(Ordering::Relaxed),
            self.current_epoch.load(Ordering::Relaxed),
            self.config_epoch.load(Ordering::Relaxed),
            self.messages_sent.load(Ordering::Relaxed),
            self.messages_received.load(Ordering::Relaxed),
        )
    }

    /// Get CLUSTER NODES output - O(N)
    pub fn format_nodes(&self) -> String {
        let mut output = String::with_capacity(1024);

        // Add self first
        let my_ip = self.my_ip.read().clone();
        let my_port = self.my_port.load(Ordering::Relaxed) as u16;
        let slots = self.slots.get_ranges();
        let slots_str = slots
            .iter()
            .map(|(s, e)| {
                if s == e { format!("{}", s) } else { format!("{}-{}", s, e) }
            })
            .collect::<Vec<_>>()
            .join(" ");

        output.push_str(&format!(
            "{} {}:{}@{} myself,master - 0 0 {} connected{}\n",
            String::from_utf8_lossy(&self.my_id),
            my_ip,
            my_port,
            my_port + 10000,
            self.config_epoch.load(Ordering::Relaxed),
            if slots_str.is_empty() { "".to_string() } else { format!(" {}", slots_str) }
        ));

        // Add other nodes
        for node in self.nodes.iter() {
            output.push_str(&node.format_nodes_line());
            output.push('\n');
        }

        output
    }

    // =========================================================================
    // Slot Management (Lock-Free Fast Path)
    // =========================================================================

    /// Add slots to this node - O(N) where N is slot count
    pub fn add_slots(&self, slots: &[u16]) -> usize {
        let mut added = 0;

        // Fast path: update atomic bitmap
        for &slot in slots {
            if slot < CLUSTER_SLOTS as u16 && !self.slots.has_slot(slot) {
                self.slots.set_slot(slot);
                self.slot_owners.set(slot, 1); // 1 = self
                added += 1;
            }
        }

        // Slow path: update legacy map (only if needed for commands)
        if added > 0 {
            let mut owner_map = self.slots_owner.write();
            for &slot in slots {
                if slot < CLUSTER_SLOTS as u16 {
                    owner_map[slot as usize] = Some(self.my_id.clone());
                }
            }
            drop(owner_map);
            self.update_slot_counts();
        }

        added
    }

    /// Add slot range - O(N)
    pub fn add_slot_range(&self, start: u16, end: u16) -> usize {
        if start > end || end >= CLUSTER_SLOTS as u16 {
            return 0;
        }

        let mut added = 0;

        // Fast path: atomic bitmap
        for slot in start..=end {
            if !self.slots.has_slot(slot) {
                self.slots.set_slot(slot);
                self.slot_owners.set(slot, 1);
                added += 1;
            }
        }

        // Slow path: legacy map
        if added > 0 {
            let mut owner_map = self.slots_owner.write();
            for slot in start..=end {
                owner_map[slot as usize] = Some(self.my_id.clone());
            }
            drop(owner_map);
            self.update_slot_counts();
        }

        added
    }

    /// Delete slots - O(N)
    pub fn del_slots(&self, slots: &[u16]) -> usize {
        let mut deleted = 0;

        for &slot in slots {
            if self.slots.has_slot(slot) {
                self.slots.clear_slot(slot);
                self.slot_owners.clear(slot);
                deleted += 1;
            }
        }

        if deleted > 0 {
            let mut owner_map = self.slots_owner.write();
            for &slot in slots {
                owner_map[slot as usize] = None;
            }
            drop(owner_map);
            self.update_slot_counts();
        }

        deleted
    }

    /// Delete slot range - O(N)
    pub fn del_slot_range(&self, start: u16, end: u16) -> usize {
        if start > end || end >= CLUSTER_SLOTS as u16 {
            return 0;
        }

        let mut deleted = 0;

        for slot in start..=end {
            if self.slots.has_slot(slot) {
                self.slots.clear_slot(slot);
                self.slot_owners.clear(slot);
                deleted += 1;
            }
        }

        if deleted > 0 {
            let mut owner_map = self.slots_owner.write();
            for slot in start..=end {
                owner_map[slot as usize] = None;
            }
            drop(owner_map);
            self.update_slot_counts();
        }

        deleted
    }

    /// Flush all slots - O(N)
    pub fn flush_slots(&self) {
        self.slots.clear_all();
        self.slot_owners.clear_all();
        self.migrating.clear();
        self.importing.clear();

        let mut owner_map = self.slots_owner.write();
        for slot in owner_map.iter_mut() {
            *slot = None;
        }
        drop(owner_map);

        self.update_slot_counts();
    }

    /// Update slot counts - O(1)
    fn update_slot_counts(&self) {
        let count = self.slots.count() as u64;
        self.slots_assigned.store(count, Ordering::Relaxed);
        self.slots_ok.store(count, Ordering::Relaxed);
        self.state_ok.store(count > 0, Ordering::Relaxed);
    }

    /// Set slot state - O(1)
    pub fn set_slot(
        &self,
        slot: u16,
        state: SlotState,
        node_id: Option<&Bytes>,
    ) -> Result<(), &'static str> {
        if slot >= CLUSTER_SLOTS as u16 {
            return Err("ERR Invalid slot");
        }

        match state {
            SlotState::Normal => {
                self.migrating.remove(&slot);
                self.importing.remove(&slot);
                if let Some(id) = node_id {
                    let is_self = id == &self.my_id;
                    if is_self {
                        self.slots.set_slot(slot);
                        self.slot_owners.set(slot, 1);
                    } else {
                        self.slots.clear_slot(slot);
                        // Get or create node index
                        let idx = self.get_or_create_node_index(id);
                        self.slot_owners.set(slot, idx);
                    }
                    self.slots_owner.write()[slot as usize] = Some(id.clone());
                }
            }
            SlotState::Migrating => {
                if let Some(id) = node_id {
                    self.migrating.insert(slot, id.clone());
                } else {
                    return Err("ERR Node ID required for MIGRATING");
                }
            }
            SlotState::Importing => {
                if let Some(id) = node_id {
                    self.importing.insert(slot, id.clone());
                } else {
                    return Err("ERR Node ID required for IMPORTING");
                }
            }
        }

        self.update_slot_counts();
        Ok(())
    }

    /// Get or create node index for slot owner table - O(1)
    fn get_or_create_node_index(&self, node_id: &Bytes) -> u32 {
        // Check if node exists and has an index
        if let Some(node) = self.nodes.get(node_id) {
            let idx = node.node_index.load(Ordering::Relaxed);
            if idx != 0 {
                return idx;
            }
        }

        // Allocate new index
        let idx = self.node_index_counter.fetch_add(1, Ordering::Relaxed);
        self.node_index_map.insert(idx, node_id.clone());

        // Update node if exists
        if let Some(node) = self.nodes.get(node_id) {
            node.node_index.store(idx, Ordering::Relaxed);
        }

        idx
    }

    // =========================================================================
    // Epoch Management
    // =========================================================================

    /// Bump epoch - O(1)
    pub fn bump_epoch(&self) -> u64 {
        let new_epoch = self.current_epoch.fetch_add(1, Ordering::SeqCst) + 1;
        self.config_epoch.store(new_epoch, Ordering::SeqCst);
        new_epoch
    }

    /// Set config epoch - O(1)
    pub fn set_config_epoch(&self, epoch: u64) -> bool {
        self.config_epoch
            .compare_exchange(0, epoch, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
    }

    // =========================================================================
    // Node Management
    // =========================================================================

    /// Meet a new node - O(1)
    pub fn meet(&self, ip: String, port: u16, _cport: Option<u16>) -> Bytes {
        let id = generate_node_id();
        let node = ClusterNode::new(id.clone(), ip.clone(), port);

        // Allocate node index
        let idx = self.node_index_counter.fetch_add(1, Ordering::Relaxed);
        node.node_index.store(idx, Ordering::Relaxed);
        self.node_index_map.insert(idx, id.clone());

        // Add to IP:Port index
        self.ip_port_index.insert(IpPortKey::new(&ip, port), id.clone());
        self.ip_port_index.insert(IpPortKey::new(&ip, port + 10000), id.clone());

        self.nodes.insert(id.clone(), Arc::new(node));
        id
    }

    /// Find node by IP:Port - O(1) using index
    pub fn find_node_by_ip_port(&self, ip: &str, port: u16) -> Option<Bytes> {
        // Try exact port first
        if let Some(id) = self.ip_port_index.get(&IpPortKey::new(ip, port)) {
            return Some(id.clone());
        }
        // Try cport
        if let Some(id) = self.ip_port_index.get(&IpPortKey::new(ip, port.saturating_sub(10000))) {
            return Some(id.clone());
        }
        if let Some(id) = self.ip_port_index.get(&IpPortKey::new(ip, port + 10000)) {
            return Some(id.clone());
        }
        None
    }

    /// Rename a node (update ID after handshake) - O(1)
    pub fn rename_node(&self, old_id: &Bytes, new_id: &Bytes) {
        if let Some((_, node)) = self.nodes.remove(old_id) {
            // Create new node with updated ID
            let mut new_node = (*node).clone();
            new_node.id = new_id.clone();

            // Update indexes
            let idx = node.node_index.load(Ordering::Relaxed);
            if idx != 0 {
                self.node_index_map.insert(idx, new_id.clone());
            }

            // Update IP:Port index
            self.ip_port_index.insert(IpPortKey::new(&node.ip, node.port), new_id.clone());
            self.ip_port_index.insert(IpPortKey::new(&node.ip, node.cport), new_id.clone());

            self.nodes.insert(new_id.clone(), Arc::new(new_node));
        }
    }

    /// Add node from gossip - O(1)
    pub fn add_node_from_gossip(&self, id: Bytes, ip: String, port: u16, cport: u16) {
        if self.nodes.contains_key(&id) {
            return;
        }

        let mut node = ClusterNode::new(id.clone(), ip.clone(), port);
        node.cport = cport;

        // Allocate node index
        let idx = self.node_index_counter.fetch_add(1, Ordering::Relaxed);
        node.node_index.store(idx, Ordering::Relaxed);
        self.node_index_map.insert(idx, id.clone());

        // Add to IP:Port index
        self.ip_port_index.insert(IpPortKey::new(&ip, port), id.clone());
        self.ip_port_index.insert(IpPortKey::new(&ip, cport), id.clone());

        self.nodes.insert(id, Arc::new(node));
    }

    /// Forget a node - O(1)
    pub fn forget(&self, node_id: &Bytes) -> bool {
        if node_id == &self.my_id {
            return false;
        }

        if let Some((_, node)) = self.nodes.remove(node_id) {
            // Remove from IP:Port index
            self.ip_port_index.remove(&IpPortKey::new(&node.ip, node.port));
            self.ip_port_index.remove(&IpPortKey::new(&node.ip, node.cport));

            // Remove from node index map
            let idx = node.node_index.load(Ordering::Relaxed);
            if idx != 0 {
                self.node_index_map.remove(&idx);
            }

            // Remove from master replicas if master
            self.master_replicas.remove(node_id);

            // Remove from any master's replica list
            if let Some(master_id) = node.master_id.read().as_ref() {
                if let Some(mut replicas) = self.master_replicas.get_mut(master_id) {
                    replicas.retain(|id| id != node_id);
                }
            }

            return true;
        }

        false
    }

    /// Get node by ID - O(1)
    pub fn get_node(&self, node_id: &Bytes) -> Option<Arc<ClusterNode>> {
        self.nodes.get(node_id).map(|r| r.clone())
    }

    /// Count failure reports for a node - O(1)
    pub fn count_failure_reports(&self, node_id: &Bytes) -> usize {
        if let Some(node) = self.nodes.get(node_id) {
            node.failure_reports.len()
        } else {
            0
        }
    }

    /// Get replicas of a master - O(1) using index
    pub fn get_replicas(&self, master_id: &Bytes) -> Vec<Arc<ClusterNode>> {
        let mut result = Vec::new();
        if let Some(replica_ids) = self.master_replicas.get(master_id) {
            for id in replica_ids.iter() {
                if let Some(node) = self.nodes.get(id) {
                    result.push(node.clone());
                }
            }
        }
        result
    }

    /// Set a node as replica of a master - O(1)
    pub fn set_replica(&self, replica_id: &Bytes, master_id: &Bytes) -> Result<(), &'static str> {
        if replica_id == &self.my_id {
            return Err("ERR Can't replicate myself");
        }
        if !self.nodes.contains_key(master_id) && master_id != &self.my_id {
            return Err("ERR Unknown master node");
        }

        if let Some(node) = self.nodes.get(replica_id) {
            node.set_role(NodeRole::Replica);
            node.flags.set_slave(true);
            node.flags.set_master(false);
            *node.master_id.write() = Some(master_id.clone());

            // Update master->replicas index
            self.master_replicas
                .entry(master_id.clone())
                .or_insert_with(Vec::new)
                .push(replica_id.clone());

            Ok(())
        } else {
            Err("ERR Unknown replica node")
        }
    }

    /// Reset cluster state - O(N)
    pub fn reset(&self, hard: bool) {
        self.flush_slots();

        // Clear indexes first
        self.ip_port_index.clear();
        self.node_index_map.clear();
        self.master_replicas.clear();

        self.nodes.clear();
        self.links.clear();

        if hard {
            self.current_epoch.store(0, Ordering::Relaxed);
            self.config_epoch.store(0, Ordering::Relaxed);
            self.node_index_counter.store(2, Ordering::Relaxed);
        }

        self.state_ok.store(false, Ordering::Relaxed);
        self.size.store(0, Ordering::Relaxed);
    }

    /// Get slot owner ID - O(1)
    pub fn get_slot_owner(&self, slot: u16) -> Option<Bytes> {
        if slot >= CLUSTER_SLOTS as u16 {
            return None;
        }

        let owner_idx = self.slot_owners.get(slot);
        match owner_idx {
            0 => None,
            1 => Some(self.my_id.clone()),
            idx => self.node_index_map.get(&idx).map(|r| r.clone()),
        }
    }

    /// Get slot owner fast (index only) - O(1)
    #[inline]
    pub fn get_slot_owner_index(&self, slot: u16) -> u32 {
        self.slot_owners.get(slot)
    }

    /// Update slot owner from gossip - O(1) per slot
    pub fn update_slot_owner_from_gossip(&self, slot: u16, node_id: &Bytes) {
        if slot >= CLUSTER_SLOTS as u16 {
            return;
        }

        // Only update if we don't own this slot
        if self.slots.has_slot(slot) {
            return;
        }

        let idx = self.get_or_create_node_index(node_id);
        self.slot_owners.set(slot, idx);

        // Update legacy map
        self.slots_owner.write()[slot as usize] = Some(node_id.clone());
    }

    /// Batch update slot owners from gossip bitmap - O(N) but optimized
    pub fn batch_update_slots_from_gossip(&self, sender: &Bytes, slots_bitmap: &[u8]) {
        if slots_bitmap.len() < 2048 {
            return;
        }

        // Get or create sender index once
        let sender_idx = self.get_or_create_node_index(sender);

        // Pre-allocate for batch legacy update
        let mut slots_to_update = Vec::with_capacity(128);

        // Process bitmap efficiently
        for (byte_idx, &byte) in slots_bitmap.iter().enumerate() {
            if byte == 0 {
                continue;
            }
            for bit_idx in 0..8 {
                if (byte >> bit_idx) & 1 == 1 {
                    let slot = (byte_idx * 8 + bit_idx) as u16;
                    if slot < CLUSTER_SLOTS as u16 && !self.slots.has_slot(slot) {
                        self.slot_owners.set(slot, sender_idx);
                        slots_to_update.push(slot);
                    }
                }
            }
        }

        // Batch update legacy map if needed
        if !slots_to_update.is_empty() {
            let mut owner_map = self.slots_owner.write();
            for slot in slots_to_update {
                owner_map[slot as usize] = Some(sender.clone());
            }
        }
    }
}

// =============================================================================
// Key Hash Slot Calculation
// =============================================================================

/// Calculate hash slot for a key using CRC16
/// Supports hash tags: {tag}rest uses only "tag" for hashing
#[inline]
pub fn key_hash_slot(key: &[u8]) -> u16 {
    let hash_key = if let Some(start) = key.iter().position(|&b| b == b'{') {
        if let Some(end) = key[start + 1..].iter().position(|&b| b == b'}') {
            if end > 0 {
                &key[start + 1..start + 1 + end]
            } else {
                key
            }
        } else {
            key
        }
    } else {
        key
    };

    crc16(hash_key) % CLUSTER_SLOTS as u16
}

/// CRC16 implementation (XMODEM) - matches Redis exactly
#[inline]
fn crc16(data: &[u8]) -> u16 {
    static CRC16_TABLE: [u16; 256] = [
        0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
        0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
        0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
        0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
        0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
        0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
        0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
        0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
        0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
        0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
        0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
        0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
        0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
        0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
        0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
        0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
        0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
        0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
        0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
        0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
        0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
        0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
        0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
        0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
        0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
        0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
        0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
        0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
        0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
        0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
        0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
        0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
    ];

    let mut crc: u16 = 0;
    for &byte in data {
        let idx = ((crc >> 8) ^ byte as u16) as usize;
        crc = (crc << 8) ^ CRC16_TABLE[idx];
    }
    crc
}

/// Generate a random 40-character hex node ID
fn generate_node_id() -> Bytes {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let ptr = &now as *const u128 as u64;
    let mut state = now as u64 ^ ptr;

    let mut id = String::with_capacity(NODE_ID_LEN);
    for _ in 0..NODE_ID_LEN {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let nibble = (state >> 60) as u8 & 0xf;
        id.push(if nibble < 10 {
            (b'0' + nibble) as char
        } else {
            (b'a' + nibble - 10) as char
        });
    }

    Bytes::from(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_hash_slot() {
        assert!(key_hash_slot(b"foo") < CLUSTER_SLOTS as u16);
        assert!(key_hash_slot(b"bar") < CLUSTER_SLOTS as u16);
        assert_eq!(key_hash_slot(b"{user}:1"), key_hash_slot(b"{user}:2"));
        assert_eq!(key_hash_slot(b"{user}:profile"), key_hash_slot(b"{user}:settings"));
        assert_eq!(key_hash_slot(b"{}foo"), key_hash_slot(b"{}foo"));
    }

    #[test]
    fn test_slot_bitmap() {
        let bitmap = SlotBitmap::new();

        assert!(!bitmap.has_slot(0));
        bitmap.set_slot(0);
        assert!(bitmap.has_slot(0));

        bitmap.set_slot(100);
        bitmap.set_slot(16383);
        assert_eq!(bitmap.count(), 3);

        bitmap.clear_slot(0);
        assert!(!bitmap.has_slot(0));
        assert_eq!(bitmap.count(), 2);

        bitmap.clear_all();
        assert_eq!(bitmap.count(), 0);
    }

    #[test]
    fn test_slot_ranges() {
        let bitmap = SlotBitmap::new();

        bitmap.set_slot(0);
        bitmap.set_slot(1);
        bitmap.set_slot(2);
        bitmap.set_slot(100);
        bitmap.set_slot(101);

        let ranges = bitmap.get_ranges();
        assert_eq!(ranges, vec![(0, 2), (100, 101)]);
    }

    #[test]
    fn test_slot_owner_table() {
        let table = SlotOwnerTable::new();

        assert_eq!(table.get(0), 0);
        table.set(0, 1);
        assert_eq!(table.get(0), 1);

        table.set(100, 5);
        assert_eq!(table.get(100), 5);

        table.clear(0);
        assert_eq!(table.get(0), 0);
    }

    #[test]
    fn test_atomic_node_flags() {
        let flags = AtomicNodeFlags::new();

        assert!(!flags.myself());
        flags.set_myself(true);
        assert!(flags.myself());

        flags.set_master(true);
        flags.set_slave(false);
        assert!(flags.master());
        assert!(!flags.slave());

        let formatted = flags.format();
        assert!(formatted.contains("myself"));
        assert!(formatted.contains("master"));
    }

    #[test]
    fn test_cluster_state() {
        let state = ClusterState::new();

        assert_eq!(state.my_id.len(), 40);
        assert_eq!(state.slots.count(), 0);

        state.add_slots(&[0, 1, 2, 3]);
        assert_eq!(state.slots.count(), 4);

        state.add_slot_range(100, 199);
        assert_eq!(state.slots.count(), 104);

        state.del_slots(&[0, 1]);
        assert_eq!(state.slots.count(), 102);

        state.flush_slots();
        assert_eq!(state.slots.count(), 0);
    }

    #[test]
    fn test_ip_port_index() {
        let state = ClusterState::new();

        let id = state.meet("192.168.1.1".to_string(), 7000, None);

        // Should find by port
        let found = state.find_node_by_ip_port("192.168.1.1", 7000);
        assert_eq!(found, Some(id.clone()));

        // Should find by cport
        let found = state.find_node_by_ip_port("192.168.1.1", 17000);
        assert_eq!(found, Some(id.clone()));

        // Should not find wrong IP
        let found = state.find_node_by_ip_port("192.168.1.2", 7000);
        assert!(found.is_none());
    }

    #[test]
    fn test_forget_node() {
        let state = ClusterState::new();

        let id = state.meet("192.168.1.1".to_string(), 7000, None);
        assert!(state.nodes.contains_key(&id));
        assert!(state.find_node_by_ip_port("192.168.1.1", 7000).is_some());

        // Forget should succeed
        assert!(state.forget(&id));
        assert!(!state.nodes.contains_key(&id));
        assert!(state.find_node_by_ip_port("192.168.1.1", 7000).is_none());

        // Can't forget self
        assert!(!state.forget(&state.my_id));
    }
}
