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
    /// Bind addresses (default: ["127.0.0.1", "-::1"])
    pub bind: Vec<String>,
    /// TCP listen backlog (default: 511)
    pub tcp_backlog: u32,
    /// Unix socket path
    pub unixsocket: Option<String>,
    /// Unix socket permissions
    pub unixsocketperm: Option<u32>,
    /// TCP keepalive in seconds (default: 300)
    pub tcp_keepalive: u32,
    /// Client timeout in seconds (default: 0 - unlimited)
    pub timeout: u64,
    /// Socket mark ID (default: 0)
    pub socket_mark_id: u32,
    /// Protected mode (default: yes)
    pub protected_mode: bool,

    // --- TLS/SSL ---
    /// TLS port (default: 0 - disabled)
    pub tls_port: u16,
    pub tls_cert_file: Option<String>,
    pub tls_key_file: Option<String>,
    pub tls_key_file_pass: Option<String>,
    pub tls_client_cert_file: Option<String>,
    pub tls_client_key_file: Option<String>,
    pub tls_client_key_file_pass: Option<String>,
    pub tls_dh_params_file: Option<String>,
    pub tls_ca_cert_file: Option<String>,
    pub tls_ca_cert_dir: Option<String>,
    pub tls_auth_clients: String, // "yes", "no", "optional"
    pub tls_auth_clients_user: Option<String>,
    pub tls_replication: bool,
    pub tls_cluster: bool,
    pub tls_protocols: Option<String>,
    pub tls_ciphers: Option<String>,
    pub tls_ciphersuites: Option<String>,
    pub tls_prefer_server_ciphers: bool,
    pub tls_session_caching: bool,
    pub tls_session_cache_size: usize,
    pub tls_session_cache_timeout: u64,

    // --- General ---
    /// Number of databases (default: 16)
    pub databases: usize,
    /// Run as daemon (default: no)
    pub daemonize: bool,
    pub supervised: String, // "no", "upstart", "systemd", "auto"
    /// Log level (default: "notice")
    pub loglevel: String,
    /// Log file path (default: "" - stdout)
    pub logfile: String,
    /// PID file path
    pub pidfile: String,
    pub crash_log_enabled: bool,
    pub crash_memcheck_enabled: bool,
    pub always_show_logo: bool,
    pub set_proc_title: bool,
    pub proc_title_template: String,

    // --- Security ---
    /// Password required for access (default user)
    pub requirepass: Option<String>,
    pub acl_pubsub_default: String,
    pub acllog_max_len: usize,
    pub aclfile: Option<String>,

    // --- Memory ---
    /// Max memory limit in bytes (default: 0 - unlimited)
    pub maxmemory: u64,
    /// Eviction policy (default: noeviction)
    pub maxmemory_policy: MaxMemoryPolicy,
    pub maxmemory_samples: usize,
    pub maxmemory_eviction_tenacity: u32,
    pub replica_ignore_maxmemory: bool,
    pub active_expire_effort: u32,

    // --- Lazy Freeing ---
    pub lazyfree_lazy_eviction: bool,
    pub lazyfree_lazy_expire: bool,
    pub lazyfree_lazy_server_del: bool,
    pub replica_lazy_flush: bool,
    pub lazyfree_lazy_user_del: bool,
    pub lazyfree_lazy_user_flush: bool,

    // --- Persistence ---
    /// Append Only File (default: no)
    pub appendonly: bool,
    pub appendfilename: String,
    pub appenddirname: String,
    pub appendfsync: String, // "always", "everysec", "no"
    pub no_appendfsync_on_rewrite: bool,
    pub auto_aof_rewrite_percentage: u32,
    pub auto_aof_rewrite_min_size: u64,
    pub aof_load_truncated: bool,
    pub aof_use_rdb_preamble: bool,
    pub aof_timestamp_enabled: bool,

    /// RDB filename (default: dump.rdb)
    pub dbfilename: String,
    /// Working directory (default: ./)
    pub dir: String,
    /// Save points: keys are seconds, values are changes
    pub save_points: Vec<(u64, u64)>,
    pub stop_writes_on_bgsave_error: bool,
    pub rdbcompression: bool,
    pub rdbchecksum: bool,
    pub sanitize_dump_payload: String, // "no", "yes", "clients"
    pub rdb_del_sync_files: bool,

    // --- Replication ---
    /// Master host:port if replica
    pub replicaof: Option<(String, u16)>,
    pub masterauth: Option<String>,
    pub masteruser: Option<String>,
    /// Replica read-only (default: yes)
    pub replica_read_only: bool,
    pub replica_serve_stale_data: bool,
    pub repl_diskless_sync: bool,
    pub repl_diskless_sync_delay: u64,
    pub repl_diskless_sync_max_replicas: u32,
    pub repl_diskless_load: String,
    /// Replication backlog size in bytes (default: 1MB)
    pub repl_backlog_size: u64,
    pub repl_backlog_ttl: u64,
    pub repl_timeout: u64,
    pub repl_disable_tcp_nodelay: bool,
    pub replica_priority: u32,
    pub propagation_error_behavior: String,
    pub replica_ignore_disk_write_errors: String,
    pub replica_announced: bool,
    pub min_replicas_to_write: u32,
    pub min_replicas_max_lag: u64,

    // --- Cluster ---
    pub cluster_enabled: bool,
    pub cluster_config_file: String,
    pub cluster_node_timeout: u64,
    pub cluster_port: u16,
    pub cluster_replica_validity_factor: u32,
    pub cluster_migration_barrier: u32,
    pub cluster_allow_replica_migration: bool,
    pub cluster_require_full_coverage: bool,
    pub cluster_replica_no_failover: bool,
    pub cluster_allow_reads_when_down: bool,
    pub cluster_allow_pubsubshard_when_down: bool,
    pub cluster_link_sendbuf_limit: u64,
    pub cluster_announce_ip: Option<String>,
    pub cluster_announce_port: Option<u16>,
    pub cluster_announce_tls_port: Option<u16>,
    pub cluster_announce_bus_port: Option<u16>,
    pub cluster_announce_hostname: Option<String>,
    pub cluster_announce_human_nodename: Option<String>,
    pub cluster_preferred_endpoint_type: String, // "ip", "hostname", "unknown-endpoint"

    // --- Limits & Others ---
    /// Max connected clients (default: 10000)
    pub maxclients: u32,
    /// Slow log execution time threshold in microseconds (default: 10000)
    pub slowlog_log_slower_than: i64,
    /// Slow log max length (default: 128)
    pub slowlog_max_len: u64,
    pub latency_monitor_threshold: u64,
    pub notify_keyspace_events: String,

    // Hash, Set, ZSet, List limits
    pub hash_max_listpack_entries: usize,
    pub hash_max_listpack_value: usize,
    pub list_max_listpack_size: i32,
    pub list_compress_depth: u32,
    pub set_max_intset_entries: usize,
    pub set_max_listpack_entries: usize,
    pub set_max_listpack_value: usize,
    pub zset_max_listpack_entries: usize,
    pub zset_max_listpack_value: usize,
    pub hll_sparse_max_bytes: usize,
    pub stream_node_max_bytes: usize,
    pub stream_node_max_entries: usize,

    pub activerehashing: bool,
    pub client_output_buffer_limit_normal: String,
    pub client_output_buffer_limit_replica: String,
    pub client_output_buffer_limit_pubsub: String,
    pub client_query_buffer_limit: u64,
    pub proto_max_bulk_len: u64,

    pub hz: u32,
    pub dynamic_hz: bool,
    pub aof_rewrite_incremental_fsync: bool,
    pub rdb_save_incremental_fsync: bool,

    pub lfu_log_factor: u32,
    pub lfu_decay_time: u32,

    pub max_new_connections_per_cycle: u32,
    pub jemalloc_bg_thread: bool,
    pub ignore_warnings: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            // Network
            port: 6379,
            bind: vec!["127.0.0.1".to_string(), "-::1".to_string()],
            tcp_backlog: 511,
            unixsocket: None,
            unixsocketperm: None,
            tcp_keepalive: 300,
            timeout: 0,
            socket_mark_id: 0,
            protected_mode: true,

            // TLS
            tls_port: 0,
            tls_cert_file: None,
            tls_key_file: None,
            tls_key_file_pass: None,
            tls_client_cert_file: None,
            tls_client_key_file: None,
            tls_client_key_file_pass: None,
            tls_dh_params_file: None,
            tls_ca_cert_file: None,
            tls_ca_cert_dir: None,
            tls_auth_clients: "yes".to_string(),
            tls_auth_clients_user: None,
            tls_replication: false,
            tls_cluster: false,
            tls_protocols: None,
            tls_ciphers: None,
            tls_ciphersuites: None,
            tls_prefer_server_ciphers: false,
            tls_session_caching: true,
            tls_session_cache_size: 20480,
            tls_session_cache_timeout: 300,

            // General
            databases: 16,
            daemonize: false,
            supervised: "no".to_string(),
            loglevel: "notice".to_string(),
            logfile: "".to_string(),
            pidfile: "/var/run/redis_6379.pid".to_string(),
            crash_log_enabled: true,
            crash_memcheck_enabled: true,
            always_show_logo: true, // Redis default is actually dependent on TTY but usually yes/no in conf
            set_proc_title: true,
            proc_title_template: "{title} {listen-addr} {server-mode}".to_string(),

            // Security
            requirepass: None,
            acl_pubsub_default: "resetchannels".to_string(),
            acllog_max_len: 128,
            aclfile: None,

            // Memory
            maxmemory: 0,
            maxmemory_policy: MaxMemoryPolicy::NoEviction,
            maxmemory_samples: 5,
            maxmemory_eviction_tenacity: 10,
            replica_ignore_maxmemory: true,
            active_expire_effort: 1,

            // Lazy Freeing
            lazyfree_lazy_eviction: false,
            lazyfree_lazy_expire: false,
            lazyfree_lazy_server_del: false,
            replica_lazy_flush: false,
            lazyfree_lazy_user_del: false,
            lazyfree_lazy_user_flush: false,

            // Persistence
            appendonly: false,
            appendfilename: "appendonly.aof".to_string(),
            appenddirname: "appendonlydir".to_string(),
            appendfsync: "everysec".to_string(),
            no_appendfsync_on_rewrite: false,
            auto_aof_rewrite_percentage: 100,
            auto_aof_rewrite_min_size: 64 * 1024 * 1024,
            aof_load_truncated: true,
            aof_use_rdb_preamble: true,
            aof_timestamp_enabled: false,

            dbfilename: "dump.rdb".to_string(),
            dir: "./".to_string(),
            save_points: vec![(3600, 1), (300, 100), (60, 10000)],
            stop_writes_on_bgsave_error: true,
            rdbcompression: true,
            rdbchecksum: true,
            sanitize_dump_payload: "no".to_string(),
            rdb_del_sync_files: false,

            // Replication
            replicaof: None,
            masterauth: None,
            masteruser: None,
            replica_read_only: true,
            replica_serve_stale_data: true,
            repl_diskless_sync: true, // Redis 7 default yes
            repl_diskless_sync_delay: 5,
            repl_diskless_sync_max_replicas: 0,
            repl_diskless_load: "disabled".to_string(),
            repl_backlog_size: 1024 * 1024,
            repl_backlog_ttl: 3600,
            repl_timeout: 60,
            repl_disable_tcp_nodelay: false,
            replica_priority: 100,
            propagation_error_behavior: "ignore".to_string(),
            replica_ignore_disk_write_errors: "no".to_string(),
            replica_announced: true,
            min_replicas_to_write: 0,
            min_replicas_max_lag: 10,

            // Cluster
            cluster_enabled: false,
            cluster_config_file: "nodes.conf".to_string(),
            cluster_node_timeout: 15000,
            cluster_port: 0,
            cluster_replica_validity_factor: 10,
            cluster_migration_barrier: 1,
            cluster_allow_replica_migration: true,
            cluster_require_full_coverage: true,
            cluster_replica_no_failover: false,
            cluster_allow_reads_when_down: false,
            cluster_allow_pubsubshard_when_down: true,
            cluster_link_sendbuf_limit: 0,
            cluster_announce_ip: None,
            cluster_announce_port: None,
            cluster_announce_tls_port: None,
            cluster_announce_bus_port: None,
            cluster_announce_hostname: None,
            cluster_announce_human_nodename: None,
            cluster_preferred_endpoint_type: "ip".to_string(),

            // Limits & Others
            maxclients: 10000,
            slowlog_log_slower_than: 10000,
            slowlog_max_len: 128,
            latency_monitor_threshold: 0,
            notify_keyspace_events: "".to_string(),

            hash_max_listpack_entries: 512,
            hash_max_listpack_value: 64,
            list_max_listpack_size: -2,
            list_compress_depth: 0,
            set_max_intset_entries: 512,
            set_max_listpack_entries: 128,
            set_max_listpack_value: 64,
            zset_max_listpack_entries: 128,
            zset_max_listpack_value: 64,
            hll_sparse_max_bytes: 3000,
            stream_node_max_bytes: 4096,
            stream_node_max_entries: 100,

            activerehashing: true,
            client_output_buffer_limit_normal: "0 0 0".to_string(),
            client_output_buffer_limit_replica: "256mb 64mb 60".to_string(),
            client_output_buffer_limit_pubsub: "32mb 8mb 60".to_string(),
            client_query_buffer_limit: 1024 * 1024 * 1024, // 1gb
            proto_max_bulk_len: 512 * 1024 * 1024,         // 512mb

            hz: 10,
            dynamic_hz: true,
            aof_rewrite_incremental_fsync: true,
            rdb_save_incremental_fsync: true,

            lfu_log_factor: 10,
            lfu_decay_time: 1,

            max_new_connections_per_cycle: 10,
            jemalloc_bg_thread: true,
            ignore_warnings: None,
        }
    }
}
