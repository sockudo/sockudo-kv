//! Client state and connection management
//!
//! Per-connection state for Redis connection commands.
//! Designed for lock-free, zero-copy performance.

use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::Instant;

/// Client flags for various modes
#[derive(Debug, Default)]
pub struct ClientFlags {
    /// CLIENT NO-EVICT ON/OFF
    pub no_evict: AtomicBool,
    /// CLIENT NO-TOUCH ON/OFF
    pub no_touch: AtomicBool,
    /// Skip reply for next command (CLIENT REPLY SKIP)
    pub skip_reply: AtomicBool,
}

impl ClientFlags {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }
}

/// Reply mode for CLIENT REPLY command
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReplyMode {
    #[default]
    On = 0,
    Off = 1,
    Skip = 2,
}

impl From<u8> for ReplyMode {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            1 => ReplyMode::Off,
            2 => ReplyMode::Skip,
            _ => ReplyMode::On,
        }
    }
}

/// Client tracking state for server-assisted client-side caching
#[derive(Debug, Default)]
pub struct TrackingState {
    /// Tracking enabled
    pub enabled: AtomicBool,
    /// Redirect notifications to this client ID
    pub redirect_id: AtomicU64,
    /// BCAST mode - track all keys
    pub bcast: AtomicBool,
    /// OPTIN mode - only track if CLIENT CACHING YES
    pub optin: AtomicBool,
    /// OPTOUT mode - track unless CLIENT CACHING NO
    pub optout: AtomicBool,
    /// NOLOOP - don't send invalidations caused by this client
    pub noloop: AtomicBool,
    /// Track keys in next command (CLIENT CACHING YES/NO effect)
    pub caching_next: AtomicBool,
    /// Prefixes for BCAST mode
    pub prefixes: parking_lot::RwLock<Vec<Bytes>>,
}

impl TrackingState {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn reset(&self) {
        self.enabled.store(false, Ordering::Relaxed);
        self.redirect_id.store(0, Ordering::Relaxed);
        self.bcast.store(false, Ordering::Relaxed);
        self.optin.store(false, Ordering::Relaxed);
        self.optout.store(false, Ordering::Relaxed);
        self.noloop.store(false, Ordering::Relaxed);
        self.caching_next.store(false, Ordering::Relaxed);
        self.prefixes.write().clear();
    }
}

/// Client type for CLIENT LIST
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ClientType {
    #[default]
    Normal,
    Master,
    Replica,
    PubSub,
}

impl ClientType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ClientType::Normal => "normal",
            ClientType::Master => "master",
            ClientType::Replica => "replica",
            ClientType::PubSub => "pubsub",
        }
    }
}

/// A queued command for MULTI/EXEC transactions
#[derive(Debug, Clone)]
pub struct QueuedCommand {
    pub name: Bytes,
    pub args: Vec<Bytes>,
}

/// Transaction state for MULTI/EXEC
#[derive(Debug, Default)]
pub struct MultiState {
    pub commands: Vec<QueuedCommand>,
    pub errors: Vec<String>,
}

impl MultiState {
    pub fn new() -> Self {
        Self {
            commands: Vec::with_capacity(16),
            errors: Vec::new(),
        }
    }

    pub fn queue(&mut self, name: Bytes, args: Vec<Bytes>) {
        self.commands.push(QueuedCommand { name, args });
    }

    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
}

/// Per-connection client state
///
/// This is owned by each connection task, with a reference stored
/// in ClientManager for global operations like CLIENT LIST/KILL.
pub struct ClientState {
    /// Unique client ID (monotonically increasing)
    pub id: u64,
    /// Client socket address
    pub addr: SocketAddr,
    /// Connection name (CLIENT SETNAME)
    pub name: parking_lot::RwLock<Option<Bytes>>,
    /// Selected database index (SELECT command)
    pub db: AtomicU64,
    /// Authentication status
    pub authenticated: AtomicBool,
    /// Authenticated user
    pub user: parking_lot::RwLock<Bytes>,
    /// Library name (CLIENT SETINFO LIB-NAME)
    pub lib_name: parking_lot::RwLock<Option<Bytes>>,
    /// Library version (CLIENT SETINFO LIB-VER)
    pub lib_ver: parking_lot::RwLock<Option<Bytes>>,
    /// Client flags
    pub flags: ClientFlags,
    /// Reply mode
    pub reply_mode: AtomicU8,
    /// Client tracking state
    pub tracking: TrackingState,
    /// Connection creation time
    pub created_at: Instant,
    /// Last command timestamp (epoch ms)
    pub last_cmd_at: AtomicU64,
    /// Client type
    pub client_type: parking_lot::RwLock<ClientType>,
    /// Subscriber ID (for PubSub integration)
    pub sub_id: u64,
    /// Is in pubsub mode
    pub in_pubsub: AtomicBool,
    /// Transaction state (MULTI/EXEC)
    pub multi_state: parking_lot::Mutex<Option<MultiState>>,
    /// Watched keys for WATCH command: (key, version)
    pub watched_keys: parking_lot::RwLock<Vec<(Bytes, u64)>>,
    /// ASK redirect flag (cleared after first command)
    pub asking: AtomicBool,
    /// READONLY mode for replica reads
    pub readonly: AtomicBool,
}

impl ClientState {
    /// Create new client state
    #[inline]
    pub fn new(id: u64, addr: SocketAddr, sub_id: u64, require_auth: bool) -> Self {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Self {
            id,
            addr,
            name: parking_lot::RwLock::new(None),
            db: AtomicU64::new(0),
            authenticated: AtomicBool::new(!require_auth), // If no auth required, authenticated by default
            user: parking_lot::RwLock::new(Bytes::from_static(b"default")),
            lib_name: parking_lot::RwLock::new(None),
            lib_ver: parking_lot::RwLock::new(None),
            flags: ClientFlags::new(),
            reply_mode: AtomicU8::new(ReplyMode::On as u8),
            tracking: TrackingState::new(),
            created_at: Instant::now(),
            last_cmd_at: AtomicU64::new(now_ms),
            client_type: parking_lot::RwLock::new(ClientType::Normal),
            sub_id,
            in_pubsub: AtomicBool::new(false),
            multi_state: parking_lot::Mutex::new(None),
            watched_keys: parking_lot::RwLock::new(Vec::new()),
            asking: AtomicBool::new(false),
            readonly: AtomicBool::new(false),
        }
    }

    /// Check if in MULTI mode
    #[inline]
    pub fn in_multi(&self) -> bool {
        self.multi_state.lock().is_some()
    }

    /// Start a MULTI transaction
    pub fn start_multi(&self) -> bool {
        let mut guard = self.multi_state.lock();
        if guard.is_some() {
            return false; // Already in MULTI
        }
        *guard = Some(MultiState::new());
        true
    }

    /// Queue a command in MULTI mode
    pub fn queue_command(&self, name: Bytes, args: Vec<Bytes>) -> bool {
        let mut guard = self.multi_state.lock();
        if let Some(ref mut state) = *guard {
            state.queue(name, args);
            true
        } else {
            false
        }
    }

    /// Discard transaction
    pub fn discard_multi(&self) {
        *self.multi_state.lock() = None;
        self.watched_keys.write().clear();
    }

    /// Take the MULTI state for execution
    pub fn take_multi(&self) -> Option<MultiState> {
        self.multi_state.lock().take()
    }

    /// Add watched key
    pub fn watch_key(&self, key: Bytes, version: u64) {
        self.watched_keys.write().push((key, version));
    }

    /// Clear watched keys
    pub fn unwatch(&self) {
        self.watched_keys.write().clear();
    }

    /// Get watched keys
    pub fn get_watched_keys(&self) -> Vec<(Bytes, u64)> {
        self.watched_keys.read().clone()
    }

    /// Get current reply mode
    #[inline]
    pub fn reply_mode(&self) -> ReplyMode {
        ReplyMode::from(self.reply_mode.load(Ordering::Relaxed))
    }

    /// Set reply mode
    #[inline]
    pub fn set_reply_mode(&self, mode: ReplyMode) {
        self.reply_mode.store(mode as u8, Ordering::Relaxed);
    }

    /// Check if client should receive reply
    #[inline]
    pub fn should_reply(&self) -> bool {
        let mode = self.reply_mode();
        if mode == ReplyMode::Off {
            return false;
        }
        if mode == ReplyMode::Skip || self.flags.skip_reply.swap(false, Ordering::Relaxed) {
            return false;
        }
        true
    }

    /// Update last command timestamp
    #[inline]
    pub fn touch(&self) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        self.last_cmd_at.store(now_ms, Ordering::Relaxed);
    }

    /// Get connection age in seconds
    #[inline]
    pub fn age_secs(&self) -> u64 {
        self.created_at.elapsed().as_secs()
    }

    /// Get idle time in seconds
    #[inline]
    pub fn idle_secs(&self) -> u64 {
        let last = self.last_cmd_at.load(Ordering::Relaxed);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        (now_ms.saturating_sub(last)) / 1000
    }

    /// Check if authenticated
    #[inline]
    pub fn is_authenticated(&self) -> bool {
        self.authenticated.load(Ordering::Relaxed)
    }

    /// Set authenticated
    #[inline]
    pub fn set_authenticated(&self, auth: bool) {
        self.authenticated.store(auth, Ordering::Relaxed);
    }

    /// Get selected database
    #[inline]
    pub fn db(&self) -> u64 {
        self.db.load(Ordering::Relaxed)
    }

    /// Set selected database
    #[inline]
    pub fn select_db(&self, db: u64) {
        self.db.store(db, Ordering::Relaxed);
    }

    /// Get currently selected database index
    #[inline]
    pub fn current_db(&self) -> usize {
        self.db.load(Ordering::Relaxed) as usize
    }

    /// Reset connection state (RESET command)
    pub fn reset(&self) {
        *self.name.write() = None;
        self.db.store(0, Ordering::Relaxed);
        self.authenticated.store(true, Ordering::Relaxed); // Default to authenticated after reset
        *self.user.write() = Bytes::from_static(b"default");
        *self.lib_name.write() = None;
        *self.lib_ver.write() = None;
        self.flags.no_evict.store(false, Ordering::Relaxed);
        self.flags.no_touch.store(false, Ordering::Relaxed);
        self.flags.skip_reply.store(false, Ordering::Relaxed);
        self.reply_mode
            .store(ReplyMode::On as u8, Ordering::Relaxed);
        self.tracking.reset();
        *self.client_type.write() = ClientType::Normal;
        self.in_pubsub.store(false, Ordering::Relaxed);
        // Clear transaction state
        *self.multi_state.lock() = None;
        self.watched_keys.write().clear();
        // Clear cluster flags
        self.asking.store(false, Ordering::Relaxed);
        self.readonly.store(false, Ordering::Relaxed);
    }

    /// Format client info string (for CLIENT LIST/INFO)
    pub fn format_info(&self) -> String {
        let name = self.name.read();
        let name_str = name
            .as_ref()
            .map(|b| String::from_utf8_lossy(b))
            .unwrap_or_default();
        let lib_name = self.lib_name.read();
        let lib_name_str = lib_name
            .as_ref()
            .map(|b| String::from_utf8_lossy(b))
            .unwrap_or_default();
        let lib_ver = self.lib_ver.read();
        let lib_ver_str = lib_ver
            .as_ref()
            .map(|b| String::from_utf8_lossy(b))
            .unwrap_or_default();
        let _client_type = self.client_type.read();
        let user = self.user.read();
        let user_str = String::from_utf8_lossy(&user);

        format!(
            "id={} addr={} fd=0 name={} age={} idle={} flags=N db={} sub=0 psub=0 ssub=0 multi=-1 qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd=NULL user={} redir={} resp=2 lib-name={} lib-ver={}\n",
            self.id,
            self.addr,
            name_str,
            self.age_secs(),
            self.idle_secs(),
            self.db(),
            user_str,
            self.tracking.redirect_id.load(Ordering::Relaxed),
            lib_name_str,
            lib_ver_str,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_client_state_new() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 12345);
        let client = ClientState::new(1, addr, 100, false);

        assert_eq!(client.id, 1);
        assert_eq!(client.sub_id, 100);
        assert!(client.is_authenticated());
        assert_eq!(client.db(), 0);
    }

    #[test]
    fn test_client_reset() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 12345);
        let client = ClientState::new(1, addr, 100, false);

        client.select_db(5);
        *client.name.write() = Some(Bytes::from_static(b"test"));
        client.set_reply_mode(ReplyMode::Off);

        client.reset();

        assert_eq!(client.db(), 0);
        assert!(client.name.read().is_none());
        assert_eq!(client.reply_mode(), ReplyMode::On);
    }

    #[test]
    fn test_reply_mode() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 12345);
        let client = ClientState::new(1, addr, 100, false);

        assert!(client.should_reply());

        client.set_reply_mode(ReplyMode::Off);
        assert!(!client.should_reply());

        client.set_reply_mode(ReplyMode::On);
        assert!(client.should_reply());
    }
}
