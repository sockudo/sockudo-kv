//! Global server state for Redis server commands
//!
//! Tracks slow log, latency samples, ACL users, and server statistics.
//! Designed for lock-free concurrent access with minimal contention.

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::cluster_state::ClusterState;
use crate::config::ServerConfig;
use std::sync::Arc;

/// Maximum slow log entries (default 128)
pub const DEFAULT_SLOWLOG_MAX_LEN: usize = 128;
/// Slow log threshold in microseconds (default 10000 = 10ms)
pub const DEFAULT_SLOWLOG_THRESHOLD_US: u64 = 10000;
/// Maximum ACL log entries (default 128)
pub const DEFAULT_ACL_LOG_MAX_LEN: usize = 128;
/// Latency monitor threshold (default 0 = disabled)
pub const DEFAULT_LATENCY_THRESHOLD_MS: u64 = 0;

/// A slow log entry
#[derive(Debug, Clone)]
pub struct SlowlogEntry {
    pub id: u64,
    pub timestamp: u64,      // Unix timestamp
    pub duration_us: u64,    // Execution time in microseconds
    pub command: Vec<Bytes>, // Command and arguments
    pub client_addr: String,
    pub client_name: String,
}

/// ACL log entry for security events
#[derive(Debug, Clone)]
pub struct AclLogEntry {
    pub count: u64,
    pub reason: AclLogReason,
    pub context: String,
    pub object: String,
    pub username: String,
    pub age_seconds: f64,
    pub client_info: String,
    pub entry_id: u64,
    pub timestamp_created: u64,
    pub timestamp_last_updated: u64,
}

/// Reason for ACL log entry
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclLogReason {
    Auth,    // Failed authentication
    Command, // Command not allowed
    Key,     // Key pattern not allowed
    Channel, // Channel not allowed
}

impl AclLogReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            AclLogReason::Auth => "auth",
            AclLogReason::Command => "command",
            AclLogReason::Key => "key",
            AclLogReason::Channel => "channel",
        }
    }
}

/// Latency sample
#[derive(Debug, Clone, Copy)]
pub struct LatencySample {
    pub timestamp: u64, // Unix timestamp
    pub latency_ms: u64,
}

/// Latency history for an event type
#[derive(Debug, Default)]
pub struct LatencyHistory {
    pub samples: RwLock<VecDeque<LatencySample>>,
    pub max_samples: usize,
}

impl LatencyHistory {
    pub fn new() -> Self {
        Self {
            samples: RwLock::new(VecDeque::with_capacity(160)),
            max_samples: 160,
        }
    }

    pub fn add_sample(&self, timestamp: u64, latency_ms: u64) {
        let mut samples = self.samples.write();
        if samples.len() >= self.max_samples {
            samples.pop_front();
        }
        samples.push_back(LatencySample {
            timestamp,
            latency_ms,
        });
    }

    pub fn get_samples(&self) -> Vec<LatencySample> {
        self.samples.read().iter().copied().collect()
    }

    pub fn latest(&self) -> Option<LatencySample> {
        self.samples.read().back().copied()
    }

    pub fn reset(&self) {
        self.samples.write().clear();
    }
}

/// ACL user definition
#[derive(Debug, Clone)]
pub struct AclUser {
    pub name: Bytes,
    pub enabled: bool,
    pub passwords: Vec<Bytes>,       // Hashed passwords
    pub nopass: bool,                // No password required
    pub commands: AclCommandRules,   // Command permissions
    pub keys: Vec<Bytes>,            // Key patterns (glob)
    pub channels: Vec<Bytes>,        // Channel patterns
    pub selectors: Vec<AclSelector>, // Additional selectors
}

/// Command permission rules
#[derive(Debug, Clone, Default)]
pub struct AclCommandRules {
    pub allow_all: bool,
    pub allowed: Vec<Bytes>,      // +command
    pub denied: Vec<Bytes>,       // -command
    pub allowed_cats: Vec<Bytes>, // +@category
    pub denied_cats: Vec<Bytes>,  // -@category
}

/// Additional ACL selector
#[derive(Debug, Clone)]
pub struct AclSelector {
    pub commands: AclCommandRules,
    pub keys: Vec<Bytes>,
    pub channels: Vec<Bytes>,
}

impl AclUser {
    /// Create default user (all access, no password)
    pub fn default_user() -> Self {
        Self {
            name: Bytes::from_static(b"default"),
            enabled: true,
            passwords: Vec::new(),
            nopass: true,
            commands: AclCommandRules {
                allow_all: true,
                ..Default::default()
            },
            keys: vec![Bytes::from_static(b"*")],
            channels: vec![Bytes::from_static(b"*")],
            selectors: Vec::new(),
        }
    }

    /// Format user rules for ACL LIST
    pub fn format_rules(&self) -> String {
        let mut parts = Vec::new();

        parts.push(format!("user {}", String::from_utf8_lossy(&self.name)));

        if self.enabled {
            parts.push("on".to_string());
        } else {
            parts.push("off".to_string());
        }

        if self.nopass {
            parts.push("nopass".to_string());
        }

        for pwd in &self.passwords {
            parts.push(format!("#{}", hex::encode(pwd)));
        }

        if self.commands.allow_all {
            parts.push("+@all".to_string());
        }

        for cat in &self.commands.allowed_cats {
            parts.push(format!("+@{}", String::from_utf8_lossy(cat)));
        }

        for cat in &self.commands.denied_cats {
            parts.push(format!("-@{}", String::from_utf8_lossy(cat)));
        }

        for cmd in &self.commands.allowed {
            parts.push(format!("+{}", String::from_utf8_lossy(cmd)));
        }

        for cmd in &self.commands.denied {
            parts.push(format!("-{}", String::from_utf8_lossy(cmd)));
        }

        for key in &self.keys {
            parts.push(format!("~{}", String::from_utf8_lossy(key)));
        }

        for chan in &self.channels {
            parts.push(format!("&{}", String::from_utf8_lossy(chan)));
        }

        parts.join(" ")
    }
}

/// Global server state
pub struct ServerState {
    // === Slow Log ===
    pub slowlog: RwLock<VecDeque<SlowlogEntry>>,
    pub slowlog_max_len: AtomicUsize,
    pub slowlog_threshold_us: AtomicU64,
    slowlog_next_id: AtomicU64,

    // === Latency Tracking ===
    pub latency_events: DashMap<Bytes, LatencyHistory>,
    pub latency_threshold_ms: AtomicU64,

    // === ACL ===
    pub acl_users: DashMap<Bytes, AclUser>,
    pub acl_log: RwLock<VecDeque<AclLogEntry>>,
    pub acl_log_max_len: AtomicUsize,
    acl_log_next_id: AtomicU64,

    // === Persistence ===
    pub last_save_time: AtomicU64,
    pub last_bgsave_status: AtomicBool,
    pub aof_enabled: AtomicBool,
    pub rdb_bgsave_in_progress: AtomicBool,
    pub aof_rewrite_in_progress: AtomicBool,

    // === Statistics ===
    pub start_time: Instant,
    pub start_time_unix: u64,
    pub total_commands: AtomicU64,
    pub total_connections: AtomicU64,
    pub rejected_connections: AtomicU64,
    pub expired_keys: AtomicU64,
    pub evicted_keys: AtomicU64,
    pub keyspace_hits: AtomicU64,
    pub keyspace_misses: AtomicU64,

    // === Config ===
    pub databases: AtomicUsize,
    pub maxmemory: AtomicU64,
    pub maxmemory_policy: RwLock<String>,
    pub config: RwLock<ServerConfig>,

    // === Cluster ===
    pub cluster: Arc<ClusterState>,

    // === Client Side Caching ===
    /// Map of key -> list of client IDs tracking it
    pub tracking_table: DashMap<Bytes, Vec<u64>>,
    /// Max keys in tracking table
    pub tracking_table_max_keys: AtomicUsize,
}

impl ServerState {
    pub fn new() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let state = Self {
            slowlog: RwLock::new(VecDeque::with_capacity(DEFAULT_SLOWLOG_MAX_LEN)),
            slowlog_max_len: AtomicUsize::new(DEFAULT_SLOWLOG_MAX_LEN),
            slowlog_threshold_us: AtomicU64::new(DEFAULT_SLOWLOG_THRESHOLD_US),
            slowlog_next_id: AtomicU64::new(0),

            latency_events: DashMap::new(),
            latency_threshold_ms: AtomicU64::new(DEFAULT_LATENCY_THRESHOLD_MS),

            acl_users: DashMap::new(),
            acl_log: RwLock::new(VecDeque::with_capacity(DEFAULT_ACL_LOG_MAX_LEN)),
            acl_log_max_len: AtomicUsize::new(DEFAULT_ACL_LOG_MAX_LEN),
            acl_log_next_id: AtomicU64::new(0),

            last_save_time: AtomicU64::new(now),
            last_bgsave_status: AtomicBool::new(true),
            aof_enabled: AtomicBool::new(false),
            rdb_bgsave_in_progress: AtomicBool::new(false),
            aof_rewrite_in_progress: AtomicBool::new(false),

            start_time: Instant::now(),
            start_time_unix: now,
            total_commands: AtomicU64::new(0),
            total_connections: AtomicU64::new(0),
            rejected_connections: AtomicU64::new(0),
            expired_keys: AtomicU64::new(0),
            evicted_keys: AtomicU64::new(0),
            keyspace_hits: AtomicU64::new(0),
            keyspace_misses: AtomicU64::new(0),

            databases: AtomicUsize::new(16),
            maxmemory: AtomicU64::new(0),
            maxmemory_policy: RwLock::new("noeviction".to_string()),
            config: RwLock::new(ServerConfig::default()),
            cluster: Arc::new(ClusterState::new()),

            tracking_table: DashMap::new(),
            tracking_table_max_keys: AtomicUsize::new(1_000_000), // Default 1M
        };

        // Create default user
        state
            .acl_users
            .insert(Bytes::from_static(b"default"), AclUser::default_user());

        state
    }

    // === Slow Log ===

    /// Add a slow log entry if duration exceeds threshold
    pub fn maybe_add_slowlog(
        &self,
        duration_us: u64,
        command: Vec<Bytes>,
        client_addr: &str,
        client_name: &str,
    ) {
        let threshold = self.slowlog_threshold_us.load(Ordering::Relaxed);
        if threshold == 0 || duration_us < threshold {
            return;
        }

        let id = self.slowlog_next_id.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let entry = SlowlogEntry {
            id,
            timestamp,
            duration_us,
            command,
            client_addr: client_addr.to_string(),
            client_name: client_name.to_string(),
        };

        let mut log = self.slowlog.write();
        let max_len = self.slowlog_max_len.load(Ordering::Relaxed);
        if log.len() >= max_len && max_len > 0 {
            log.pop_back();
        }
        log.push_front(entry);
    }

    /// Get slow log entries
    pub fn get_slowlog(&self, count: Option<usize>) -> Vec<SlowlogEntry> {
        let log = self.slowlog.read();
        match count {
            Some(n) => log.iter().take(n).cloned().collect(),
            None => log.iter().cloned().collect(),
        }
    }

    /// Get slow log length
    pub fn slowlog_len(&self) -> usize {
        self.slowlog.read().len()
    }

    /// Reset slow log
    pub fn slowlog_reset(&self) {
        self.slowlog.write().clear();
    }

    // === Latency Tracking ===

    /// Record latency for an event
    pub fn record_latency(&self, event: &[u8], latency_ms: u64) {
        let threshold = self.latency_threshold_ms.load(Ordering::Relaxed);
        if threshold == 0 || latency_ms < threshold {
            return;
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let event_bytes = Bytes::copy_from_slice(event);
        self.latency_events
            .entry(event_bytes)
            .or_default()
            .add_sample(timestamp, latency_ms);
    }

    /// Get latency history for an event
    pub fn get_latency_history(&self, event: &[u8]) -> Vec<LatencySample> {
        self.latency_events
            .get(event)
            .map(|h| h.get_samples())
            .unwrap_or_default()
    }

    /// Get latest latency samples for all events
    pub fn get_latency_latest(&self) -> Vec<(Bytes, LatencySample)> {
        self.latency_events
            .iter()
            .filter_map(|entry| entry.value().latest().map(|s| (entry.key().clone(), s)))
            .collect()
    }

    /// Reset latency data
    pub fn reset_latency(&self, events: Option<&[Bytes]>) {
        match events {
            Some(evts) => {
                for evt in evts {
                    if let Some(history) = self.latency_events.get(evt) {
                        history.reset();
                    }
                }
            }
            None => {
                for entry in self.latency_events.iter() {
                    entry.value().reset();
                }
            }
        }
    }

    // === ACL ===

    /// Get user by name
    pub fn get_acl_user(&self, name: &[u8]) -> Option<AclUser> {
        self.acl_users.get(name).map(|u| u.clone())
    }

    /// List all user names
    pub fn list_acl_users(&self) -> Vec<Bytes> {
        self.acl_users.iter().map(|e| e.key().clone()).collect()
    }

    /// Add ACL log entry
    pub fn add_acl_log(
        &self,
        reason: AclLogReason,
        context: &str,
        object: &str,
        username: &str,
        client_info: &str,
    ) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let id = self.acl_log_next_id.fetch_add(1, Ordering::Relaxed);

        let entry = AclLogEntry {
            count: 1,
            reason,
            context: context.to_string(),
            object: object.to_string(),
            username: username.to_string(),
            age_seconds: 0.0,
            client_info: client_info.to_string(),
            entry_id: id,
            timestamp_created: now,
            timestamp_last_updated: now,
        };

        let mut log = self.acl_log.write();
        let max_len = self.acl_log_max_len.load(Ordering::Relaxed);
        if log.len() >= max_len && max_len > 0 {
            log.pop_back();
        }
        log.push_front(entry);
    }

    /// Get ACL log entries
    pub fn get_acl_log(&self, count: Option<usize>) -> Vec<AclLogEntry> {
        let log = self.acl_log.read();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let entries: Vec<AclLogEntry> = match count {
            Some(n) => log.iter().take(n).cloned().collect(),
            None => log.iter().cloned().collect(),
        };

        // Update age_seconds
        entries
            .into_iter()
            .map(|mut e| {
                e.age_seconds = (now - e.timestamp_created) as f64;
                e
            })
            .collect()
    }

    /// Reset ACL log
    pub fn reset_acl_log(&self) {
        self.acl_log.write().clear();
    }

    // === Stats ===

    /// Increment command counter
    #[inline]
    pub fn incr_commands(&self) {
        self.total_commands.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment connection counter
    #[inline]
    pub fn incr_connections(&self) {
        self.total_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// Get uptime in seconds
    #[inline]
    pub fn uptime_secs(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    /// Get uptime in days
    #[inline]
    pub fn uptime_days(&self) -> u64 {
        self.uptime_secs() / 86400
    }

    /// Get current unix timestamp
    #[inline]
    pub fn now_unix(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    /// Get current time as (seconds, microseconds)
    #[inline]
    pub fn time(&self) -> (u64, u64) {
        let duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        (duration.as_secs(), duration.subsec_micros() as u64)
    }
    /// Set maxmemory limit
    pub fn set_maxmemory(&self, bytes: u64) {
        self.maxmemory.store(bytes, Ordering::Relaxed);
    }

    /// Set maxmemory policy
    pub fn set_maxmemory_policy(&self, policy: &str) {
        *self.maxmemory_policy.write() = policy.to_string();
    }

    /// Set default user password (requirepass)
    pub fn set_default_user_password(&self, password: &str) {
        if let Some(mut user) = self.acl_users.get_mut(b"default".as_slice()) {
            user.nopass = false;
            // Store simple password hash (in real redis it's sha256)
            // For now we just store the raw password bytes as "hashed" for simplicity
            // or we should use proper hashing.
            // Let's use SHA256 as imported in Cargo.toml
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(password.as_bytes());
            let result = hasher.finalize();
            user.passwords.push(Bytes::copy_from_slice(&result));
        }
    }

    // === Tracking ===

    /// Add a key to client tracking table
    pub fn add_tracking_key(&self, key: Bytes, client_id: u64) {
        // Optimistic check to avoid write lock if already tracking
        if let Some(mut clients) = self.tracking_table.get_mut(&key) {
            if !clients.contains(&client_id) {
                clients.push(client_id);
            }
            return;
        }

        // New key - check limit
        let max_keys = self.tracking_table_max_keys.load(Ordering::Relaxed);
        let current_len = self.tracking_table.len();

        if current_len >= max_keys && max_keys > 0 {
            // Evict a random key (DashMap doesn't support random efficiently, use first from iteration)
            // This is efficient enough for now.
            if let Some(entry) = self.tracking_table.iter().next() {
                // We need to clone the key to remove it after releasing the iterator lock
                // Or just remove current entry? DashMap iterators are tricky with concurrent remove.
                // Safest is to pick key and remove.
                let key_to_evict = entry.key().clone();
                drop(entry);
                self.tracking_table.remove(&key_to_evict);
                // Also notify clients? Redis sends invalidation on eviction.
                // We don't have reference to clients here easily to find the evicted key's trackers.
                // For this task (config implementation), basic eviction suffices.
                // In full implementation, we'd iterate the clients in evicted entry and send invalidation.
            }
        }

        // Insert new key
        self.tracking_table.entry(key).or_default().push(client_id);
    }
}

impl Default for ServerState {
    fn default() -> Self {
        Self::new()
    }
}

/// ACL command categories
pub const ACL_CATEGORIES: &[&str] = &[
    "keyspace",
    "read",
    "write",
    "set",
    "sortedset",
    "list",
    "hash",
    "string",
    "bitmap",
    "hyperloglog",
    "geo",
    "stream",
    "pubsub",
    "admin",
    "fast",
    "slow",
    "blocking",
    "dangerous",
    "connection",
    "transaction",
    "scripting",
];

/// Get commands in a category
pub fn get_commands_in_category(category: &str) -> Vec<&'static str> {
    match category.to_lowercase().as_str() {
        "keyspace" => vec![
            "DEL",
            "DUMP",
            "EXISTS",
            "EXPIRE",
            "EXPIREAT",
            "EXPIRETIME",
            "KEYS",
            "MIGRATE",
            "MOVE",
            "OBJECT",
            "PERSIST",
            "PEXPIRE",
            "PEXPIREAT",
            "PEXPIRETIME",
            "PTTL",
            "RANDOMKEY",
            "RENAME",
            "RENAMENX",
            "RESTORE",
            "SCAN",
            "SORT",
            "SORT_RO",
            "TOUCH",
            "TTL",
            "TYPE",
            "UNLINK",
            "WAIT",
            "WAITAOF",
        ],
        "read" => vec![
            "BITCOUNT",
            "BITFIELD_RO",
            "BITPOS",
            "DBSIZE",
            "DEBUG",
            "DUMP",
            "EXISTS",
            "EXPIRETIME",
            "GEODIST",
            "GEOHASH",
            "GEOPOS",
            "GEORADIUS_RO",
            "GEORADIUSBYMEMBER_RO",
            "GEOSEARCH",
            "GET",
            "GETBIT",
            "GETEX",
            "GETRANGE",
            "HEXISTS",
            "HGET",
            "HGETALL",
            "HINCRBY",
            "HINCRBYFLOAT",
            "HKEYS",
            "HLEN",
            "HMGET",
            "HRANDFIELD",
            "HSCAN",
            "HSTRLEN",
            "HVALS",
            "KEYS",
            "LINDEX",
            "LLEN",
            "LPOS",
            "LRANGE",
            "MGET",
            "OBJECT",
            "PEXPIRETIME",
            "PFCOUNT",
            "PTTL",
            "RANDOMKEY",
            "SCAN",
            "SCARD",
            "SDIFF",
            "SINTER",
            "SINTERCARD",
            "SISMEMBER",
            "SMEMBERS",
            "SMISMEMBER",
            "SORT_RO",
            "SRANDMEMBER",
            "SSCAN",
            "STRLEN",
            "SUNION",
            "TTL",
            "TYPE",
            "XINFO",
            "XLEN",
            "XPENDING",
            "XRANGE",
            "XREAD",
            "XREVRANGE",
            "ZCARD",
            "ZCOUNT",
            "ZDIFF",
            "ZINTER",
            "ZLEXCOUNT",
            "ZMSCORE",
            "ZRANDMEMBER",
            "ZRANGE",
            "ZRANGEBYLEX",
            "ZRANGEBYSCORE",
            "ZRANK",
            "ZREVRANGE",
            "ZREVRANGEBYLEX",
            "ZREVRANGEBYSCORE",
            "ZREVRANK",
            "ZSCAN",
            "ZSCORE",
            "ZUNION",
        ],
        "write" => vec![
            "APPEND",
            "BITFIELD",
            "BITOP",
            "BLMOVE",
            "BLMPOP",
            "BLPOP",
            "BRPOP",
            "BRPOPLPUSH",
            "BZMPOP",
            "BZPOPMAX",
            "BZPOPMIN",
            "COPY",
            "DECR",
            "DECRBY",
            "DEL",
            "EXPIRE",
            "EXPIREAT",
            "FLUSHALL",
            "FLUSHDB",
            "GEOADD",
            "GEORADIUS",
            "GEORADIUSBYMEMBER",
            "GEOSEARCHSTORE",
            "GETDEL",
            "GETSET",
            "HDEL",
            "HINCRBY",
            "HINCRBYFLOAT",
            "HMSET",
            "HSET",
            "HSETNX",
            "INCR",
            "INCRBY",
            "INCRBYFLOAT",
            "LINSERT",
            "LMOVE",
            "LMPOP",
            "LPOP",
            "LPUSH",
            "LPUSHX",
            "LREM",
            "LSET",
            "LTRIM",
            "MIGRATE",
            "MOVE",
            "MSET",
            "MSETNX",
            "PERSIST",
            "PEXPIRE",
            "PEXPIREAT",
            "PFADD",
            "PFMERGE",
            "PSETEX",
            "RENAME",
            "RENAMENX",
            "RESTORE",
            "RPOP",
            "RPOPLPUSH",
            "RPUSH",
            "RPUSHX",
            "SADD",
            "SDIFFSTORE",
            "SET",
            "SETBIT",
            "SETEX",
            "SETNX",
            "SETRANGE",
            "SINTERSTORE",
            "SMOVE",
            "SORT",
            "SPOP",
            "SREM",
            "SUNIONSTORE",
            "UNLINK",
            "XACK",
            "XADD",
            "XAUTOCLAIM",
            "XCLAIM",
            "XDEL",
            "XGROUP",
            "XSETID",
            "XTRIM",
            "ZADD",
            "ZDIFFSTORE",
            "ZINCRBY",
            "ZINTERSTORE",
            "ZMPOP",
            "ZPOPMAX",
            "ZPOPMIN",
            "ZRANGESTORE",
            "ZREM",
            "ZREMRANGEBYLEX",
            "ZREMRANGEBYRANK",
            "ZREMRANGEBYSCORE",
            "ZUNIONSTORE",
        ],
        "set" => vec![
            "SADD",
            "SCARD",
            "SDIFF",
            "SDIFFSTORE",
            "SINTER",
            "SINTERCARD",
            "SINTERSTORE",
            "SISMEMBER",
            "SMEMBERS",
            "SMISMEMBER",
            "SMOVE",
            "SPOP",
            "SRANDMEMBER",
            "SREM",
            "SSCAN",
            "SUNION",
            "SUNIONSTORE",
        ],
        "sortedset" => vec![
            "BZMPOP",
            "BZPOPMAX",
            "BZPOPMIN",
            "ZADD",
            "ZCARD",
            "ZCOUNT",
            "ZDIFF",
            "ZDIFFSTORE",
            "ZINCRBY",
            "ZINTER",
            "ZINTERCARD",
            "ZINTERSTORE",
            "ZLEXCOUNT",
            "ZMPOP",
            "ZMSCORE",
            "ZPOPMAX",
            "ZPOPMIN",
            "ZRANDMEMBER",
            "ZRANGE",
            "ZRANGEBYLEX",
            "ZRANGEBYSCORE",
            "ZRANGESTORE",
            "ZRANK",
            "ZREM",
            "ZREMRANGEBYLEX",
            "ZREMRANGEBYRANK",
            "ZREMRANGEBYSCORE",
            "ZREVRANGE",
            "ZREVRANGEBYLEX",
            "ZREVRANGEBYSCORE",
            "ZREVRANK",
            "ZSCAN",
            "ZSCORE",
            "ZUNION",
            "ZUNIONSTORE",
        ],
        "list" => vec![
            "BLMOVE",
            "BLMPOP",
            "BLPOP",
            "BRPOP",
            "BRPOPLPUSH",
            "LINDEX",
            "LINSERT",
            "LLEN",
            "LMOVE",
            "LMPOP",
            "LPOP",
            "LPOS",
            "LPUSH",
            "LPUSHX",
            "LRANGE",
            "LREM",
            "LSET",
            "LTRIM",
            "RPOP",
            "RPOPLPUSH",
            "RPUSH",
            "RPUSHX",
        ],
        "hash" => vec![
            "HDEL",
            "HEXISTS",
            "HGET",
            "HGETALL",
            "HINCRBY",
            "HINCRBYFLOAT",
            "HKEYS",
            "HLEN",
            "HMGET",
            "HMSET",
            "HRANDFIELD",
            "HSCAN",
            "HSET",
            "HSETNX",
            "HSTRLEN",
            "HVALS",
        ],
        "string" => vec![
            "APPEND",
            "DECR",
            "DECRBY",
            "GET",
            "GETDEL",
            "GETEX",
            "GETRANGE",
            "GETSET",
            "INCR",
            "INCRBY",
            "INCRBYFLOAT",
            "LCS",
            "MGET",
            "MSET",
            "MSETNX",
            "PSETEX",
            "SET",
            "SETEX",
            "SETNX",
            "SETRANGE",
            "STRLEN",
        ],
        "bitmap" => vec![
            "BITCOUNT",
            "BITFIELD",
            "BITFIELD_RO",
            "BITOP",
            "BITPOS",
            "GETBIT",
            "SETBIT",
        ],
        "hyperloglog" => vec!["PFADD", "PFCOUNT", "PFMERGE", "PFDEBUG", "PFSELFTEST"],
        "geo" => vec![
            "GEOADD",
            "GEODIST",
            "GEOHASH",
            "GEOPOS",
            "GEORADIUS",
            "GEORADIUS_RO",
            "GEORADIUSBYMEMBER",
            "GEORADIUSBYMEMBER_RO",
            "GEOSEARCH",
            "GEOSEARCHSTORE",
        ],
        "stream" => vec![
            "XACK",
            "XADD",
            "XAUTOCLAIM",
            "XCLAIM",
            "XDEL",
            "XGROUP",
            "XINFO",
            "XLEN",
            "XPENDING",
            "XRANGE",
            "XREAD",
            "XREADGROUP",
            "XREVRANGE",
            "XSETID",
            "XTRIM",
        ],
        "pubsub" => vec![
            "PSUBSCRIBE",
            "PUBLISH",
            "PUBSUB",
            "PUNSUBSCRIBE",
            "SSUBSCRIBE",
            "SUBSCRIBE",
            "SUNSUBSCRIBE",
            "UNSUBSCRIBE",
        ],
        "admin" => vec![
            "ACL",
            "BGREWRITEAOF",
            "BGSAVE",
            "CLIENT",
            "CLUSTER",
            "CONFIG",
            "DEBUG",
            "FAILOVER",
            "FLUSHALL",
            "FLUSHDB",
            "LASTSAVE",
            "LATENCY",
            "LOLWUT",
            "MEMORY",
            "MODULE",
            "MONITOR",
            "PSYNC",
            "REPLCONF",
            "REPLICAOF",
            "RESET",
            "RESTORE",
            "ROLE",
            "SAVE",
            "SHUTDOWN",
            "SLAVEOF",
            "SLOWLOG",
            "SWAPDB",
            "SYNC",
            "TIME",
        ],
        "fast" => vec![
            "APPEND",
            "ASKING",
            "AUTH",
            "BGSAVE",
            "CLIENT",
            "CLUSTER",
            "COMMAND",
            "CONFIG",
            "DBSIZE",
            "DECR",
            "DECRBY",
            "DEL",
            "DISCARD",
            "ECHO",
            "EXEC",
            "EXISTS",
            "EXPIRE",
            "EXPIREAT",
            "EXPIRETIME",
            "FLUSHALL",
            "FLUSHDB",
            "GET",
            "GETBIT",
            "GETEX",
            "GETSET",
            "HDEL",
            "HEXISTS",
            "HGET",
            "HINCRBY",
            "HINCRBYFLOAT",
            "HLEN",
            "HSETNX",
            "HSET",
            "HSTRLEN",
            "INCR",
            "INCRBY",
            "INCRBYFLOAT",
            "INFO",
            "LASTSAVE",
            "LATENCY",
            "LINDEX",
            "LLEN",
            "LPOP",
            "LPUSH",
            "LPUSHX",
            "LSET",
            "MEMORY",
            "MGET",
            "MSET",
            "MSETNX",
            "MULTI",
            "OBJECT",
            "PERSIST",
            "PEXPIRE",
            "PEXPIREAT",
            "PEXPIRETIME",
            "PFADD",
            "PING",
            "PSETEX",
            "PTTL",
            "PUBLISH",
            "QUIT",
            "RANDOMKEY",
            "READONLY",
            "READWRITE",
            "RENAME",
            "RENAMENX",
            "REPLCONF",
            "RPOP",
            "RPOPLPUSH",
            "RPUSH",
            "RPUSHX",
            "SADD",
            "SCARD",
            "SCRIPT",
            "SELECT",
            "SET",
            "SETBIT",
            "SETEX",
            "SETNX",
            "SETRANGE",
            "SISMEMBER",
            "SLAVEOF",
            "SLOWLOG",
            "SMISMEMBER",
            "SMOVE",
            "SPOP",
            "SREM",
            "STRLEN",
            "TIME",
            "TOUCH",
            "TTL",
            "TYPE",
            "UNLINK",
            "UNWATCH",
            "WATCH",
            "XACK",
            "XLEN",
            "ZADD",
            "ZCARD",
            "ZINCRBY",
            "ZRANK",
            "ZREM",
            "ZREVRANK",
            "ZSCORE",
        ],
        "slow" => vec![
            "ACL",
            "BITCOUNT",
            "BITFIELD",
            "BITFIELD_RO",
            "BITOP",
            "BITPOS",
            "BLMOVE",
            "BLMPOP",
            "BLPOP",
            "BRPOP",
            "BRPOPLPUSH",
            "BZMPOP",
            "BZPOPMAX",
            "BZPOPMIN",
            "COPY",
            "DEBUG",
            "DUMP",
            "EVAL",
            "EVALSHA",
            "FCALL",
            "FCALL_RO",
            "FUNCTION",
            "GEODIST",
            "GEOHASH",
            "GEOPOS",
            "GEORADIUS",
            "GEORADIUSBYMEMBER",
            "GEOSEARCH",
            "GEOSEARCHSTORE",
            "GETDEL",
            "GETRANGE",
            "HGETALL",
            "HKEYS",
            "HMGET",
            "HMSET",
            "HRANDFIELD",
            "HSCAN",
            "HVALS",
            "KEYS",
            "LINSERT",
            "LMOVE",
            "LMPOP",
            "LPOS",
            "LRANGE",
            "LREM",
            "LTRIM",
            "MIGRATE",
            "MOVE",
            "OBJECT",
            "PFCOUNT",
            "PFMERGE",
            "PSUBSCRIBE",
            "PUBSUB",
            "PUNSUBSCRIBE",
            "RESTORE",
            "ROLE",
            "SAVE",
            "SCAN",
            "SCARD",
            "SDIFF",
            "SDIFFSTORE",
            "SHUTDOWN",
            "SINTER",
            "SINTERCARD",
            "SINTERSTORE",
            "SMEMBERS",
            "SORT",
            "SORT_RO",
            "SRANDMEMBER",
            "SSCAN",
            "SSUBSCRIBE",
            "SUBSCRIBE",
            "SUNION",
            "SUNIONSTORE",
            "SUNSUBSCRIBE",
            "SWAPDB",
            "SYNC",
            "UNSUBSCRIBE",
            "WAIT",
            "WAITAOF",
            "XAUTOCLAIM",
            "XCLAIM",
            "XDEL",
            "XGROUP",
            "XINFO",
            "XPENDING",
            "XRANGE",
            "XREAD",
            "XREADGROUP",
            "XREVRANGE",
            "XSETID",
            "XTRIM",
            "ZCOUNT",
            "ZDIFF",
            "ZDIFFSTORE",
            "ZINTER",
            "ZINTERCARD",
            "ZINTERSTORE",
            "ZLEXCOUNT",
            "ZMPOP",
            "ZMSCORE",
            "ZPOPMAX",
            "ZPOPMIN",
            "ZRANDMEMBER",
            "ZRANGE",
            "ZRANGEBYLEX",
            "ZRANGEBYSCORE",
            "ZRANGESTORE",
            "ZREMRANGEBYLEX",
            "ZREMRANGEBYRANK",
            "ZREMRANGEBYSCORE",
            "ZREVRANGE",
            "ZREVRANGEBYLEX",
            "ZREVRANGEBYSCORE",
            "ZSCAN",
            "ZUNION",
            "ZUNIONSTORE",
        ],
        "blocking" => vec![
            "BLMOVE",
            "BLMPOP",
            "BLPOP",
            "BRPOP",
            "BRPOPLPUSH",
            "BZMPOP",
            "BZPOPMAX",
            "BZPOPMIN",
            "WAIT",
            "WAITAOF",
            "XREAD",
            "XREADGROUP",
        ],
        "dangerous" => vec![
            "ACL",
            "BGREWRITEAOF",
            "BGSAVE",
            "CLIENT",
            "CLUSTER",
            "CONFIG",
            "DEBUG",
            "FAILOVER",
            "FLUSHALL",
            "FLUSHDB",
            "KEYS",
            "LASTSAVE",
            "LATENCY",
            "MIGRATE",
            "MODULE",
            "MONITOR",
            "MOVE",
            "OBJECT",
            "PSYNC",
            "REPLCONF",
            "REPLICAOF",
            "RESET",
            "RESTORE",
            "ROLE",
            "SAVE",
            "SHUTDOWN",
            "SLAVEOF",
            "SLOWLOG",
            "SORT",
            "SWAPDB",
            "SYNC",
        ],
        "connection" => vec![
            "AUTH",
            "CLIENT",
            "ECHO",
            "HELLO",
            "PING",
            "QUIT",
            "READONLY",
            "READWRITE",
            "RESET",
            "SELECT",
        ],
        "transaction" => vec!["DISCARD", "EXEC", "MULTI", "UNWATCH", "WATCH"],
        "scripting" => vec![
            "EVAL",
            "EVAL_RO",
            "EVALSHA",
            "EVALSHA_RO",
            "FCALL",
            "FCALL_RO",
            "FUNCTION",
            "SCRIPT",
        ],
        _ => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_state_new() {
        let state = ServerState::new();
        assert_eq!(state.slowlog_len(), 0);
        assert!(state.get_acl_user(b"default").is_some());
    }

    #[test]
    fn test_slowlog() {
        let state = ServerState::new();
        state.slowlog_threshold_us.store(0, Ordering::Relaxed); // Disable threshold

        state.maybe_add_slowlog(
            15000,
            vec![Bytes::from_static(b"SET"), Bytes::from_static(b"key")],
            "127.0.0.1:12345",
            "test",
        );

        // Since threshold is 0, it won't be added
        assert_eq!(state.slowlog_len(), 0);

        // Enable threshold
        state.slowlog_threshold_us.store(10000, Ordering::Relaxed);
        state.maybe_add_slowlog(
            15000,
            vec![Bytes::from_static(b"SET"), Bytes::from_static(b"key")],
            "127.0.0.1:12345",
            "test",
        );
        assert_eq!(state.slowlog_len(), 1);

        state.slowlog_reset();
        assert_eq!(state.slowlog_len(), 0);
    }

    #[test]
    fn test_acl_user_format() {
        let user = AclUser::default_user();
        let rules = user.format_rules();
        assert!(rules.contains("user default"));
        assert!(rules.contains("on"));
        assert!(rules.contains("nopass"));
    }

    #[test]
    fn test_time() {
        let state = ServerState::new();
        let (secs, usecs) = state.time();
        assert!(secs > 0);
        assert!(usecs < 1_000_000);
    }
}
