//! Server configuration

/// Redis eviction policies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MaxMemoryPolicy {
    #[default]
    NoEviction,
    AllKeysLru,
    VolatileLru,
    AllKeysLfu,
    VolatileLfu,
    AllKeysRandom,
    VolatileRandom,
    VolatileTtl,
}

impl std::str::FromStr for MaxMemoryPolicy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "noeviction" => Ok(Self::NoEviction),
            "allkeys-lru" => Ok(Self::AllKeysLru),
            "volatile-lru" => Ok(Self::VolatileLru),
            "allkeys-lfu" => Ok(Self::AllKeysLfu),
            "volatile-lfu" => Ok(Self::VolatileLfu),
            "allkeys-random" => Ok(Self::AllKeysRandom),
            "volatile-random" => Ok(Self::VolatileRandom),
            "volatile-ttl" => Ok(Self::VolatileTtl),
            _ => Err(format!("Unknown maxmemory-policy: {}", s)),
        }
    }
}

/// Server configuration
#[derive(Debug, Clone)]
pub struct ServerConfig {
    // --- Network ---
    /// Port to listen on (default: 6379)
    pub port: u16,
    /// Bind address (default: 127.0.0.1)
    pub bind: String,
    /// TCP listen backlog (default: 511)
    pub tcp_backlog: u32,
    /// TCP keepalive in seconds (default: 300)
    pub tcp_keepalive: u32,
    /// Client timeout in seconds (default: 0 - unlimited)
    pub timeout: u64,

    // --- General ---
    /// Number of databases (default: 16)
    pub databases: usize,
    /// Run as daemon (default: no)
    pub daemonize: bool,
    /// Log level (default: "notice")
    pub loglevel: String,
    /// Log file path (default: "" - stdout)
    pub logfile: String,
    /// PID file path
    pub pidfile: String,

    // --- Security ---
    /// Password required for access
    pub requirepass: Option<String>,
    /// Protected mode (default: yes)
    pub protected_mode: bool,

    // --- Memory ---
    /// Max memory limit in bytes (default: 0 - unlimited)
    pub maxmemory: u64,
    /// Eviction policy (default: noeviction)
    pub maxmemory_policy: MaxMemoryPolicy,

    // --- Persistence ---
    /// Append Only File (default: no)
    pub appendonly: bool,
    /// RDB filename (default: dump.rdb)
    pub dbfilename: String,
    /// Working directory (default: ./)
    pub dir: String,
    /// Save points: keys are seconds, values are changes
    pub save_points: Vec<(u64, u64)>,

    // --- Replication ---
    /// Master host:port if replica
    pub replicaof: Option<(String, u16)>,
    /// Replica read-only (default: yes)
    pub replica_read_only: bool,
    /// Replication backlog size in bytes (default: 1MB)
    pub repl_backlog_size: u64,

    // --- Limits & Others ---
    /// Max connected clients (default: 10000)
    pub maxclients: u32,
    /// Slow log execution time threshold in microseconds (default: 10000)
    pub slowlog_log_slower_than: i64,
    /// Slow log max length (default: 128)
    pub slowlog_max_len: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: 6379,
            bind: "127.0.0.1".to_string(),
            tcp_backlog: 511,
            tcp_keepalive: 300,
            timeout: 0,
            databases: 16,
            daemonize: false,
            loglevel: "notice".to_string(),
            logfile: "".to_string(),
            pidfile: "/var/run/redis.pid".to_string(),
            requirepass: None,
            protected_mode: true,
            maxmemory: 0,
            maxmemory_policy: MaxMemoryPolicy::NoEviction,
            appendonly: false,
            dbfilename: "dump.rdb".to_string(),
            dir: "./".to_string(),
            save_points: vec![(3600, 1), (300, 100), (60, 10000)],
            replicaof: None,
            replica_read_only: true,
            repl_backlog_size: 1024 * 1024, // 1MB
            maxclients: 10000,
            slowlog_log_slower_than: 10000,
            slowlog_max_len: 128,
        }
    }
}
