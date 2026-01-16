//! Global client manager for connection tracking
//!
//! Provides O(1) client lookup using DashMap.
//! Used for CLIENT LIST, CLIENT KILL, CLIENT PAUSE, etc.

use crate::client::{ClientState, ClientType};
use bytes::Bytes;
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::Instant;

/// Lightweight client info stored in the manager
/// The full ClientState is owned by the connection task
pub struct ClientInfo {
    pub id: u64,
    pub addr: SocketAddr,
    pub state: Arc<ClientState>,
}

/// Filter criteria for CLIENT KILL
#[derive(Debug, Default)]
pub struct KillFilter {
    pub id: Option<u64>,
    pub client_type: Option<ClientType>,
    pub addr: Option<SocketAddr>,
    pub laddr: Option<SocketAddr>,
    pub user: Option<Bytes>,
    pub skipme: bool,
    pub maxage: Option<u64>,
}

impl KillFilter {
    /// Check if a client matches this filter
    pub fn matches(&self, client: &ClientState, current_id: u64) -> bool {
        // SKIPME - don't kill the client running the command
        if self.skipme && client.id == current_id {
            return false;
        }

        if let Some(id) = self.id
            && client.id != id
        {
            return false;
        }

        if let Some(ref ct) = self.client_type {
            let client_type = client.client_type.read();
            if *client_type != *ct {
                return false;
            }
        }

        if let Some(ref addr) = self.addr
            && client.addr != *addr
        {
            return false;
        }

        if let Some(maxage) = self.maxage
            && client.age_secs() < maxage
        {
            return false;
        }

        // User check would go here if we had ACL

        true
    }
}

/// Global client manager
///
/// Lock-free operations using DashMap for concurrent access.
pub struct ClientManager {
    /// All connected clients: id -> ClientInfo
    clients: DashMap<u64, ClientInfo>,
    /// Next client ID (monotonically increasing)
    next_id: AtomicU64,
    /// Global pause state
    paused: AtomicBool,
    /// Pause timeout (when to auto-resume)
    pause_until: AtomicU64,
    /// Pause mode: true = WRITE only, false = ALL
    pause_write_only: AtomicBool,
    /// Server start time
    start_time: Instant,
    /// Maximum number of clients (0 = unlimited)
    maxclients: AtomicU32,
}

impl ClientManager {
    /// Create new client manager
    pub fn new() -> Self {
        Self {
            clients: DashMap::with_capacity(1024),
            next_id: AtomicU64::new(1),
            paused: AtomicBool::new(false),
            pause_until: AtomicU64::new(0),
            pause_write_only: AtomicBool::new(false),
            start_time: Instant::now(),
            maxclients: AtomicU32::new(0),
        }
    }

    /// Register a new client
    /// Returns the client ID and shared state
    pub fn register(&self, addr: SocketAddr, sub_id: u64) -> Arc<ClientState> {
        self.register_with_auth(addr, sub_id, false)
    }

    /// Register a new client with explicit auth requirement
    pub fn register_with_auth(
        &self,
        addr: SocketAddr,
        sub_id: u64,
        require_auth: bool,
    ) -> Arc<ClientState> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let state = Arc::new(ClientState::new(id, addr, sub_id, require_auth));

        self.clients.insert(
            id,
            ClientInfo {
                id,
                addr,
                state: state.clone(),
            },
        );

        state
    }

    /// Unregister a client
    pub fn unregister(&self, id: u64) {
        self.clients.remove(&id);
    }

    /// Get client count
    #[inline]
    pub fn client_count(&self) -> usize {
        self.clients.len()
    }

    /// Set maximum clients limit
    pub fn set_maxclients(&self, max: u32) {
        self.maxclients.store(max, Ordering::Relaxed);
    }

    /// Check if a new connection can be accepted
    #[inline]
    pub fn can_accept(&self) -> bool {
        let max = self.maxclients.load(Ordering::Relaxed);
        if max == 0 {
            return true; // No limit
        }
        self.clients.len() < max as usize
    }

    /// Get client by ID
    pub fn get_client(&self, id: u64) -> Option<Arc<ClientState>> {
        self.clients.get(&id).map(|c| c.state.clone())
    }

    /// List all clients (for CLIENT LIST)
    pub fn list_clients(
        &self,
        filter_type: Option<ClientType>,
        filter_ids: Option<&[u64]>,
    ) -> Vec<Arc<ClientState>> {
        self.clients
            .iter()
            .filter(|entry| {
                let client = &entry.value().state;

                // Filter by type
                if let Some(ref ct) = filter_type {
                    let client_type = client.client_type.read();
                    if *client_type != *ct {
                        return false;
                    }
                }

                // Filter by IDs
                if let Some(ids) = filter_ids
                    && !ids.contains(&client.id)
                {
                    return false;
                }

                true
            })
            .map(|entry| entry.value().state.clone())
            .collect()
    }

    /// Kill clients matching filter
    /// Returns number of clients killed
    pub fn kill_clients(&self, filter: &KillFilter, current_id: u64) -> usize {
        let mut killed = 0;
        let mut to_remove = Vec::new();

        for entry in self.clients.iter() {
            let client = &entry.value().state;
            if filter.matches(client, current_id) {
                to_remove.push(*entry.key());
                killed += 1;
            }
        }

        for id in to_remove {
            self.clients.remove(&id);
        }

        killed
    }

    /// Pause all clients
    pub fn pause(&self, timeout_ms: u64, write_only: bool) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        self.pause_until
            .store(now_ms + timeout_ms, Ordering::Release);
        self.pause_write_only.store(write_only, Ordering::Release);
        self.paused.store(true, Ordering::Release);
    }

    /// Unpause all clients
    pub fn unpause(&self) {
        self.paused.store(false, Ordering::Release);
        self.pause_until.store(0, Ordering::Relaxed);
    }

    /// Check if clients are paused
    #[inline]
    pub fn is_paused(&self) -> bool {
        if !self.paused.load(Ordering::Acquire) {
            return false;
        }

        // Check if pause has expired
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let until = self.pause_until.load(Ordering::Relaxed);

        if now_ms >= until {
            self.paused.store(false, Ordering::Release);
            return false;
        }

        true
    }

    /// Check if only writes are paused
    #[inline]
    pub fn is_write_paused(&self) -> bool {
        self.is_paused() && self.pause_write_only.load(Ordering::Relaxed)
    }

    /// Get server uptime in seconds
    #[inline]
    pub fn uptime_secs(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }
}

impl Default for ClientManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_client_registration() {
        let manager = ClientManager::new();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 12345);

        let client = manager.register(addr, 1);
        assert_eq!(client.id, 1);
        assert_eq!(manager.client_count(), 1);

        let client2 = manager.register(addr, 2);
        assert_eq!(client2.id, 2);
        assert_eq!(manager.client_count(), 2);

        manager.unregister(1);
        assert_eq!(manager.client_count(), 1);
    }

    #[test]
    fn test_pause_unpause() {
        let manager = ClientManager::new();

        assert!(!manager.is_paused());

        manager.pause(10000, false);
        assert!(manager.is_paused());

        manager.unpause();
        assert!(!manager.is_paused());
    }
}
