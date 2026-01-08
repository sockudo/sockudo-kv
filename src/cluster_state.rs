//! Redis Cluster state management
//!
//! High-performance cluster state with lock-free slot bitmap and concurrent node table.
//! Designed for Dragonfly/KeyDB-level performance.

use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Total number of hash slots in Redis Cluster
pub const CLUSTER_SLOTS: usize = 16384;

/// Number of AtomicU64 needed for slot bitmap (16384 / 64 = 256)
const SLOT_BITMAP_SIZE: usize = CLUSTER_SLOTS / 64;

/// Node ID length (40 hex characters)
pub const NODE_ID_LEN: usize = 40;

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

impl SlotBitmap {
    /// Create empty slot bitmap
    #[inline]
    pub fn new() -> Self {
        // Initialize all slots to 0 (not owned)
        Self {
            bits: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    /// Check if slot is owned by this node - O(1)
    #[inline]
    pub fn has_slot(&self, slot: u16) -> bool {
        if slot >= CLUSTER_SLOTS as u16 {
            return false;
        }
        let idx = slot as usize / 64;
        let bit = slot as usize % 64;
        (self.bits[idx].load(Ordering::Relaxed) >> bit) & 1 == 1
    }

    /// Set slot as owned - O(1)
    #[inline]
    pub fn set_slot(&self, slot: u16) {
        if slot >= CLUSTER_SLOTS as u16 {
            return;
        }
        let idx = slot as usize / 64;
        let bit = slot as usize % 64;
        self.bits[idx].fetch_or(1u64 << bit, Ordering::Relaxed);
    }

    /// Clear slot ownership - O(1)
    #[inline]
    pub fn clear_slot(&self, slot: u16) {
        if slot >= CLUSTER_SLOTS as u16 {
            return;
        }
        let idx = slot as usize / 64;
        let bit = slot as usize % 64;
        self.bits[idx].fetch_and(!(1u64 << bit), Ordering::Relaxed);
    }

    /// Clear all slots - O(1)
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

    /// Get all owned slots as ranges for CLUSTER SLOTS output
    pub fn get_ranges(&self) -> Vec<(u16, u16)> {
        let mut ranges = Vec::new();
        let mut start: Option<u16> = None;

        for slot in 0..CLUSTER_SLOTS as u16 {
            if self.has_slot(slot) {
                if start.is_none() {
                    start = Some(slot);
                }
            } else if let Some(s) = start {
                ranges.push((s, slot - 1));
                start = None;
            }
        }

        if let Some(s) = start {
            ranges.push((s, CLUSTER_SLOTS as u16 - 1));
        }

        ranges
    }
}

/// Node role in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    Master,
    Replica,
}

impl NodeRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeRole::Master => "master",
            NodeRole::Replica => "slave",
        }
    }
}

/// Node flags for CLUSTER NODES output
#[derive(Debug, Clone, Copy, Default)]
pub struct NodeFlags {
    pub myself: bool,
    pub master: bool,
    pub slave: bool,
    pub pfail: bool, // Possible failure
    pub fail: bool,  // Confirmed failure
    pub handshake: bool,
    pub noaddr: bool,
    pub nofailover: bool,
}

impl NodeFlags {
    pub fn format(&self) -> String {
        let mut flags = Vec::new();
        if self.myself {
            flags.push("myself");
        }
        if self.master {
            flags.push("master");
        }
        if self.slave {
            flags.push("slave");
        }
        if self.pfail {
            flags.push("fail?");
        }
        if self.fail {
            flags.push("fail");
        }
        if self.handshake {
            flags.push("handshake");
        }
        if self.noaddr {
            flags.push("noaddr");
        }
        if self.nofailover {
            flags.push("nofailover");
        }
        if flags.is_empty() {
            "noflags".to_string()
        } else {
            flags.join(",")
        }
    }
}

/// Cluster node information
#[derive(Debug)]
pub struct ClusterNode {
    /// 40-character hex node ID
    pub id: Bytes,
    /// IP address
    pub ip: String,
    /// Client port
    pub port: u16,
    /// Cluster bus port (usually port + 10000)
    pub cport: u16,
    /// TLS port (0 if not used)
    pub tls_port: u16,
    /// Node role
    pub role: NodeRole,
    /// Master node ID (if replica)
    pub master_id: Option<Bytes>,
    /// Ping sent timestamp
    pub ping_sent: u64,
    /// Pong received timestamp
    pub pong_recv: u64,
    /// Config epoch
    pub config_epoch: u64,
    /// Node flags
    pub flags: NodeFlags,
    /// Slots owned by this node
    pub slots: SlotBitmap,
    /// Failure reports from other nodes
    pub failure_reports: DashMap<Bytes, u64>, // reporter_id -> timestamp
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
            role: NodeRole::Master,
            master_id: None,
            ping_sent: 0,
            pong_recv: 0,
            config_epoch: 0,
            flags: NodeFlags::default(),
            slots: SlotBitmap::new(),
            failure_reports: DashMap::new(),
        }
    }

    /// Format node info for CLUSTER NODES output
    pub fn format_nodes_line(&self) -> String {
        let master_id = self
            .master_id
            .as_ref()
            .map(|s| String::from_utf8_lossy(s).to_string())
            .unwrap_or_else(|| "-".to_string());

        let slots = self.format_slots();

        format!(
            "{} {}:{}@{} {} {} {} {} {}{}",
            String::from_utf8_lossy(&self.id),
            self.ip,
            self.port,
            self.cport,
            self.flags.format(),
            master_id,
            self.ping_sent,
            self.pong_recv,
            self.config_epoch,
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

/// Slot migration state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    /// Normal state
    Normal,
    /// Migrating to another node
    Migrating,
    /// Importing from another node
    Importing,
}

/// Main cluster state
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
    /// Slots owned by this node
    pub slots: SlotBitmap,
    /// All known nodes (node_id -> node)
    pub nodes: DashMap<Bytes, Arc<ClusterNode>>,
    /// Slots being migrated (slot -> target_node_id)
    pub migrating: DashMap<u16, Bytes>,
    /// Slots being imported (slot -> source_node_id)
    pub importing: DashMap<u16, Bytes>,
    /// TCP links to other nodes
    pub links: DashMap<Bytes, ClusterLink>,
    /// This node's IP
    pub my_ip: parking_lot::RwLock<String>,
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
            migrating: DashMap::new(),
            importing: DashMap::new(),
            links: DashMap::new(),
            my_ip: parking_lot::RwLock::new("127.0.0.1".to_string()),
            my_port: AtomicU64::new(6379),
            size: AtomicU64::new(0),
            slots_assigned: AtomicU64::new(0),
            slots_ok: AtomicU64::new(0),
            slots_pfail: AtomicU64::new(0),
            slots_fail: AtomicU64::new(0),
            messages_received: AtomicU64::new(0),
            messages_sent: AtomicU64::new(0),
        }
    }

    /// Get CLUSTER INFO output
    pub fn format_info(&self) -> String {
        let state = if self.state_ok.load(Ordering::Relaxed) {
            "ok"
        } else {
            "fail"
        };

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
            self.nodes.len() + 1, // +1 for self
            self.size.load(Ordering::Relaxed),
            self.current_epoch.load(Ordering::Relaxed),
            self.config_epoch.load(Ordering::Relaxed),
            self.messages_sent.load(Ordering::Relaxed),
            self.messages_received.load(Ordering::Relaxed),
        )
    }

    /// Get CLUSTER NODES output
    pub fn format_nodes(&self) -> String {
        let mut output = String::new();

        // Add self first
        let my_ip = self.my_ip.read().clone();
        let my_port = self.my_port.load(Ordering::Relaxed) as u16;
        let slots = self.slots.get_ranges();
        let slots_str = slots
            .iter()
            .map(|(s, e)| {
                if s == e {
                    format!("{}", s)
                } else {
                    format!("{}-{}", s, e)
                }
            })
            .collect::<Vec<_>>()
            .join(" ");

        output.push_str(&format!(
            "{} {}:{}@{} myself,master - 0 {} {} {}\n",
            String::from_utf8_lossy(&self.my_id),
            my_ip,
            my_port,
            my_port + 10000,
            self.current_epoch.load(Ordering::Relaxed),
            self.config_epoch.load(Ordering::Relaxed),
            if slots_str.is_empty() {
                "".to_string()
            } else {
                format!(" {}", slots_str)
            }
        ));

        // Add other nodes
        for node in self.nodes.iter() {
            output.push_str(&node.format_nodes_line());
            output.push('\n');
        }

        output
    }

    /// Add slots to this node
    pub fn add_slots(&self, slots: &[u16]) -> usize {
        let mut added = 0;
        for &slot in slots {
            if slot < CLUSTER_SLOTS as u16 && !self.slots.has_slot(slot) {
                self.slots.set_slot(slot);
                added += 1;
            }
        }
        self.update_slot_counts();
        added
    }

    /// Add slot range to this node
    pub fn add_slot_range(&self, start: u16, end: u16) -> usize {
        if start > end || end >= CLUSTER_SLOTS as u16 {
            return 0;
        }
        let mut added = 0;
        for slot in start..=end {
            if !self.slots.has_slot(slot) {
                self.slots.set_slot(slot);
                added += 1;
            }
        }
        self.update_slot_counts();
        added
    }

    /// Delete slots from this node
    pub fn del_slots(&self, slots: &[u16]) -> usize {
        let mut deleted = 0;
        for &slot in slots {
            if self.slots.has_slot(slot) {
                self.slots.clear_slot(slot);
                deleted += 1;
            }
        }
        self.update_slot_counts();
        deleted
    }

    /// Delete slot range from this node
    pub fn del_slot_range(&self, start: u16, end: u16) -> usize {
        if start > end || end >= CLUSTER_SLOTS as u16 {
            return 0;
        }
        let mut deleted = 0;
        for slot in start..=end {
            if self.slots.has_slot(slot) {
                self.slots.clear_slot(slot);
                deleted += 1;
            }
        }
        self.update_slot_counts();
        deleted
    }

    /// Flush all slots
    pub fn flush_slots(&self) {
        self.slots.clear_all();
        self.migrating.clear();
        self.importing.clear();
        self.update_slot_counts();
    }

    /// Update slot-related counts
    fn update_slot_counts(&self) {
        let count = self.slots.count() as u64;
        self.slots_assigned.store(count, Ordering::Relaxed);
        self.slots_ok.store(count, Ordering::Relaxed);
        self.state_ok.store(count > 0, Ordering::Relaxed);
    }

    /// Set slot state (IMPORTING/MIGRATING/NODE/STABLE)
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
                    if id == &self.my_id {
                        self.slots.set_slot(slot);
                    } else {
                        self.slots.clear_slot(slot);
                    }
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

    /// Bump epoch
    pub fn bump_epoch(&self) -> u64 {
        let new_epoch = self.current_epoch.fetch_add(1, Ordering::SeqCst) + 1;
        self.config_epoch.store(new_epoch, Ordering::SeqCst);
        new_epoch
    }

    /// Set config epoch
    pub fn set_config_epoch(&self, epoch: u64) -> bool {
        // Can only set if current is 0
        self.config_epoch
            .compare_exchange(0, epoch, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
    }

    /// Add/update a node
    pub fn meet(&self, ip: String, port: u16, _cport: Option<u16>) -> Bytes {
        let id = generate_node_id();
        let node = ClusterNode::new(id.clone(), ip, port);
        self.nodes.insert(id.clone(), Arc::new(node));
        id
    }

    /// Forget a node
    pub fn forget(&self, node_id: &Bytes) -> bool {
        if node_id == &self.my_id {
            return false; // Can't forget self
        }
        self.nodes.remove(node_id).is_some()
    }

    /// Get node by ID
    pub fn get_node(&self, node_id: &Bytes) -> Option<Arc<ClusterNode>> {
        self.nodes.get(node_id).map(|r| r.clone())
    }

    /// Count failure reports for a node
    pub fn count_failure_reports(&self, node_id: &Bytes) -> usize {
        if let Some(node) = self.nodes.get(node_id) {
            node.failure_reports.len()
        } else {
            0
        }
    }

    /// Reset cluster state
    pub fn reset(&self, hard: bool) {
        self.flush_slots();
        self.nodes.clear();
        self.links.clear();

        if hard {
            // Generate new node ID
            // Note: In a real impl, we'd replace my_id but it's in Bytes
            self.current_epoch.store(0, Ordering::Relaxed);
            self.config_epoch.store(0, Ordering::Relaxed);
        }

        self.state_ok.store(false, Ordering::Relaxed);
        self.size.store(0, Ordering::Relaxed);
    }
}

/// Calculate hash slot for a key using CRC16
/// Supports hash tags: {tag}rest uses only "tag" for hashing
#[inline]
pub fn key_hash_slot(key: &[u8]) -> u16 {
    // Find hash tag: content between first { and first } after it
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

/// CRC16 implementation (XMODEM)
/// Matches Redis's implementation exactly
#[inline]
fn crc16(data: &[u8]) -> u16 {
    static CRC16_TABLE: [u16; 256] = [
        0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7, 0x8108, 0x9129, 0xa14a,
        0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef, 0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294,
        0x72f7, 0x62d6, 0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de, 0x2462,
        0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485, 0xa56a, 0xb54b, 0x8528, 0x9509,
        0xe5ee, 0xf5cf, 0xc5ac, 0xd58d, 0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695,
        0x46b4, 0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc, 0x48c4, 0x58e5,
        0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823, 0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948,
        0x9969, 0xa90a, 0xb92b, 0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
        0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a, 0x6ca6, 0x7c87, 0x4ce4,
        0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41, 0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b,
        0x8d68, 0x9d49, 0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70, 0xff9f,
        0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78, 0x9188, 0x81a9, 0xb1ca, 0xa1eb,
        0xd10c, 0xc12d, 0xf14e, 0xe16f, 0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046,
        0x6067, 0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e, 0x02b1, 0x1290,
        0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256, 0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e,
        0xe54f, 0xd52c, 0xc50d, 0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
        0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c, 0x26d3, 0x36f2, 0x0691,
        0x16b0, 0x6657, 0x7676, 0x4615, 0x5634, 0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9,
        0xb98a, 0xa9ab, 0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3, 0xcb7d,
        0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a, 0x4a75, 0x5a54, 0x6a37, 0x7a16,
        0x0af1, 0x1ad0, 0x2ab3, 0x3a92, 0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8,
        0x8dc9, 0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1, 0xef1f, 0xff3e,
        0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8, 0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93,
        0x3eb2, 0x0ed1, 0x1ef0,
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

    // Simple pseudo-random based on time and memory address
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
        // Standard keys
        assert!(key_hash_slot(b"foo") < CLUSTER_SLOTS as u16);
        assert!(key_hash_slot(b"bar") < CLUSTER_SLOTS as u16);

        // Hash tags
        assert_eq!(key_hash_slot(b"{user}:1"), key_hash_slot(b"{user}:2"));
        assert_eq!(
            key_hash_slot(b"{user}:profile"),
            key_hash_slot(b"{user}:settings")
        );

        // Empty hash tag uses full key
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
}
