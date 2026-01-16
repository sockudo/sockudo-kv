//! Sentinel Configuration - Dragonfly-style
//!
//! Configuration structures for Redis Sentinel monitoring.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::PathBuf;

/// Per-master sentinel monitoring configuration
#[derive(Debug, Clone)]
pub struct MasterConfig {
    /// Master name identifier
    pub name: String,
    /// Master IP address
    pub ip: String,
    /// Master port
    pub port: u16,
    /// Quorum for ODOWN detection
    pub quorum: u32,
    /// Milliseconds before marking as SDOWN (default 30000)
    pub down_after_ms: u64,
    /// Failover timeout in milliseconds (default 180000)
    pub failover_timeout: u64,
    /// Number of replicas to reconfigure in parallel (default 1)
    pub parallel_syncs: u32,
    /// Authentication password for Redis instances
    pub auth_pass: Option<String>,
    /// Authentication username for Redis 6+ ACL
    pub auth_user: Option<String>,
    /// Notification script path
    pub notification_script: Option<PathBuf>,
    /// Client reconfiguration script path
    pub client_reconfig_script: Option<PathBuf>,
    /// Renamed commands mapping (original -> renamed)
    pub rename_commands: HashMap<String, String>,
    /// Master reboot down after period (milliseconds)
    pub master_reboot_down_after_period: u64,
}

impl Default for MasterConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            ip: String::new(),
            port: 6379,
            quorum: 2,
            down_after_ms: 30000,
            failover_timeout: 180000,
            parallel_syncs: 1,
            auth_pass: None,
            auth_user: None,
            notification_script: None,
            client_reconfig_script: None,
            rename_commands: HashMap::new(),
            master_reboot_down_after_period: 0,
        }
    }
}

impl MasterConfig {
    /// Create a new master config with required fields
    pub fn new(name: String, ip: String, port: u16, quorum: u32) -> Self {
        Self {
            name,
            ip,
            port,
            quorum,
            ..Default::default()
        }
    }
}

/// Global sentinel configuration
#[derive(Debug, Clone)]
pub struct SentinelConfig {
    /// Sentinel port (default 26379)
    pub port: u16,
    /// Bind addresses
    pub bind: Vec<String>,
    /// Protected mode (default no for sentinel)
    pub protected_mode: bool,
    /// Announce IP for NAT/Docker environments
    pub announce_ip: Option<String>,
    /// Announce port for NAT/Docker environments
    pub announce_port: Option<u16>,
    /// Unique 40-character hex Sentinel ID
    pub myid: String,
    /// Monitored masters configuration
    pub masters: HashMap<String, MasterConfig>,
    /// Username for authenticating with other Sentinels
    pub sentinel_user: Option<String>,
    /// Password for authenticating with other Sentinels
    pub sentinel_pass: Option<String>,
    /// Enable hostname resolution
    pub resolve_hostnames: bool,
    /// Announce hostnames instead of IPs
    pub announce_hostnames: bool,
    /// Prevent runtime script reconfiguration
    pub deny_scripts_reconfig: bool,
    /// Working directory
    pub dir: String,
    /// Log level
    pub loglevel: String,
    /// Log file path
    pub logfile: String,
    /// Daemonize
    pub daemonize: bool,
    /// PID file path
    pub pidfile: String,
    /// ACL log max length
    pub acllog_max_len: usize,
    /// ACL file path
    pub aclfile: Option<String>,
    /// Require password (compatibility)
    pub requirepass: Option<String>,
}

impl Default for SentinelConfig {
    fn default() -> Self {
        Self {
            port: 26379,
            bind: vec!["0.0.0.0".to_string()],
            protected_mode: false,
            announce_ip: None,
            announce_port: None,
            myid: generate_sentinel_id(),
            masters: HashMap::new(),
            sentinel_user: None,
            sentinel_pass: None,
            resolve_hostnames: false,
            announce_hostnames: false,
            deny_scripts_reconfig: true,
            dir: "/tmp".to_string(),
            loglevel: "notice".to_string(),
            logfile: String::new(),
            daemonize: false,
            pidfile: "/var/run/redis-sentinel.pid".to_string(),
            acllog_max_len: 128,
            aclfile: None,
            requirepass: None,
        }
    }
}

impl SentinelConfig {
    /// Create new sentinel config with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a master to monitor
    pub fn add_master(&mut self, config: MasterConfig) {
        self.masters.insert(config.name.clone(), config);
    }

    /// Remove a master from monitoring
    pub fn remove_master(&mut self, name: &str) -> Option<MasterConfig> {
        self.masters.remove(name)
    }

    /// Get master config by name
    pub fn get_master(&self, name: &str) -> Option<&MasterConfig> {
        self.masters.get(name)
    }

    /// Get mutable master config by name
    pub fn get_master_mut(&mut self, name: &str) -> Option<&mut MasterConfig> {
        self.masters.get_mut(name)
    }

    /// Get the announce IP (or fall back to first bind address)
    pub fn get_announce_ip(&self) -> String {
        self.announce_ip.clone().unwrap_or_else(|| {
            self.bind
                .first()
                .cloned()
                .unwrap_or_else(|| "127.0.0.1".to_string())
        })
    }

    /// Get the announce port (or fall back to configured port)
    pub fn get_announce_port(&self) -> u16 {
        self.announce_port.unwrap_or(self.port)
    }
}

/// Thread-safe wrapper for SentinelConfig
pub struct SharedSentinelConfig(RwLock<SentinelConfig>);

impl SharedSentinelConfig {
    pub fn new(config: SentinelConfig) -> Self {
        Self(RwLock::new(config))
    }

    pub fn read(&self) -> parking_lot::RwLockReadGuard<'_, SentinelConfig> {
        self.0.read()
    }

    pub fn write(&self) -> parking_lot::RwLockWriteGuard<'_, SentinelConfig> {
        self.0.write()
    }
}

impl Default for SharedSentinelConfig {
    fn default() -> Self {
        Self::new(SentinelConfig::default())
    }
}

/// Generate a random 40-character hex Sentinel ID
fn generate_sentinel_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    let random_part = fastrand::u64(..) as u128;
    let combined = now ^ random_part;

    // Generate 40 hex chars (20 bytes worth)
    format!(
        "{:016x}{:016x}{:08x}",
        combined,
        fastrand::u64(..) as u128 ^ now,
        fastrand::u32(..)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sentinel_id_generation() {
        let id1 = generate_sentinel_id();
        let id2 = generate_sentinel_id();

        assert_eq!(id1.len(), 40);
        assert_eq!(id2.len(), 40);
        assert_ne!(id1, id2);

        // Verify all characters are valid hex
        assert!(id1.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_master_config_defaults() {
        let config = MasterConfig::new("mymaster".to_string(), "127.0.0.1".to_string(), 6379, 2);

        assert_eq!(config.down_after_ms, 30000);
        assert_eq!(config.failover_timeout, 180000);
        assert_eq!(config.parallel_syncs, 1);
    }

    #[test]
    fn test_sentinel_config() {
        let mut config = SentinelConfig::new();
        config.add_master(MasterConfig::new(
            "mymaster".to_string(),
            "127.0.0.1".to_string(),
            6379,
            2,
        ));

        assert!(config.get_master("mymaster").is_some());
        assert_eq!(config.port, 26379);
    }
}
