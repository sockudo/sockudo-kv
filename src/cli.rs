use crate::config::{MaxMemoryPolicy, ServerConfig};
use clap::Parser;

use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[derive(Parser, Debug)]
#[command(name = "sockudo-kv")]
#[command(author = "Sockudo Team")]
#[command(version = "1.0.0")]
#[command(about = "A high-performance Redis-compatible key-value store", long_about = None)]
pub struct Cli {
    /// Configuration file path
    #[arg(value_name = "CONFIG_FILE")]
    pub config_file: Option<PathBuf>,

    /// Server port (default: 6379)
    #[arg(long)]
    pub port: Option<u16>,

    /// Bind address (default: 127.0.0.1)
    #[arg(long)]
    pub bind: Option<String>,

    /// Number of databases (default: 16)
    #[arg(long)]
    pub databases: Option<usize>,

    /// Run as daemon
    #[arg(long)]
    pub daemonize: Option<bool>,

    /// Log level (debug, verbose, notice, warning)
    #[arg(long)]
    pub loglevel: Option<String>,

    /// Log file path
    #[arg(long)]
    pub logfile: Option<String>,

    /// Password required for access
    #[arg(long)]
    pub requirepass: Option<String>,

    /// Protected mode (yes/no)
    #[arg(long)]
    pub protected_mode: Option<String>,

    /// Max memory limit (e.g., 100mb, 1gb)
    #[arg(long)]
    pub maxmemory: Option<String>,

    /// Max memory policy
    #[arg(long)]
    pub maxmemory_policy: Option<String>,

    /// Append Only File (yes/no)
    #[arg(long)]
    pub appendonly: Option<String>,

    /// RDB filename
    #[arg(long)]
    pub dbfilename: Option<String>,

    /// Working directory
    #[arg(long)]
    pub dir: Option<String>,

    /// Replica of master (host port)
    #[arg(long, num_args = 2, value_names = ["HOST", "PORT"])]
    pub replicaof: Option<Vec<String>>,

    // --- Network ---
    #[arg(long)]
    pub tcp_backlog: Option<u32>,
    #[arg(long)]
    pub unixsocket: Option<String>,
    #[arg(long)]
    pub unixsocketperm: Option<u32>,
    #[arg(long)]
    pub tcp_keepalive: Option<u32>,
    #[arg(long)]
    pub timeout: Option<u64>,
    #[arg(long)]
    pub socket_mark_id: Option<u32>,
    #[arg(long)]
    pub bind_source_addr: Option<String>,

    // --- TLS/SSL ---
    #[arg(long)]
    pub tls_port: Option<u16>,
    #[arg(long)]
    pub tls_cert_file: Option<String>,
    #[arg(long)]
    pub tls_key_file: Option<String>,
    #[arg(long)]
    pub tls_key_file_pass: Option<String>,
    #[arg(long)]
    pub tls_client_cert_file: Option<String>,
    #[arg(long)]
    pub tls_client_key_file: Option<String>,
    #[arg(long)]
    pub tls_client_key_file_pass: Option<String>,
    #[arg(long)]
    pub tls_dh_params_file: Option<String>,
    #[arg(long)]
    pub tls_ca_cert_file: Option<String>,
    #[arg(long)]
    pub tls_ca_cert_dir: Option<String>,
    #[arg(long)]
    pub tls_auth_clients: Option<String>,
    #[arg(long)]
    pub tls_auth_clients_user: Option<String>,
    #[arg(long)]
    pub tls_replication: Option<String>,
    #[arg(long)]
    pub tls_cluster: Option<String>,
    #[arg(long)]
    pub tls_protocols: Option<String>,
    #[arg(long)]
    pub tls_ciphers: Option<String>,
    #[arg(long)]
    pub tls_ciphersuites: Option<String>,
    #[arg(long)]
    pub tls_prefer_server_ciphers: Option<String>,
    #[arg(long)]
    pub tls_session_caching: Option<String>,
    #[arg(long)]
    pub tls_session_cache_size: Option<usize>,
    #[arg(long)]
    pub tls_session_cache_timeout: Option<u64>,

    // --- General ---
    #[arg(long)]
    pub supervised: Option<String>,
    #[arg(long)]
    pub pidfile: Option<String>,
    #[arg(long)]
    pub crash_log_enabled: Option<String>,
    #[arg(long)]
    pub crash_memcheck_enabled: Option<String>,
    #[arg(long)]
    pub always_show_logo: Option<String>,
    #[arg(long)]
    pub set_proc_title: Option<String>,
    #[arg(long)]
    pub proc_title_template: Option<String>,
    #[arg(long)]
    pub enable_protected_configs: Option<String>,
    #[arg(long)]
    pub enable_debug_command: Option<String>,
    #[arg(long)]
    pub enable_module_command: Option<String>,
    #[arg(long)]
    pub syslog_enabled: Option<String>,
    #[arg(long)]
    pub syslog_ident: Option<String>,
    #[arg(long)]
    pub syslog_facility: Option<String>,
    #[arg(long)]
    pub hide_user_data_from_log: Option<String>,
    #[arg(long)]
    pub locale_collate: Option<String>,

    // --- Security ---
    #[arg(long)]
    pub acl_pubsub_default: Option<String>,
    #[arg(long)]
    pub tracking_table_max_keys: Option<u64>,
    #[arg(long)]
    pub acllog_max_len: Option<usize>,
    #[arg(long)]
    pub aclfile: Option<String>,

    // --- Memory ---
    #[arg(long)]
    pub maxmemory_samples: Option<usize>,
    #[arg(long)]
    pub maxmemory_eviction_tenacity: Option<u32>,
    #[arg(long)]
    pub replica_ignore_maxmemory: Option<String>,
    #[arg(long)]
    pub active_expire_effort: Option<u32>,

    // --- Lazy Freeing ---
    #[arg(long)]
    pub lazyfree_lazy_eviction: Option<String>,
    #[arg(long)]
    pub lazyfree_lazy_expire: Option<String>,
    #[arg(long)]
    pub lazyfree_lazy_server_del: Option<String>,
    #[arg(long)]
    pub replica_lazy_flush: Option<String>,
    #[arg(long)]
    pub lazyfree_lazy_user_del: Option<String>,
    #[arg(long)]
    pub lazyfree_lazy_user_flush: Option<String>,

    // --- Persistence ---
    #[arg(long)]
    pub appendfilename: Option<String>,
    #[arg(long)]
    pub appenddirname: Option<String>,
    #[arg(long)]
    pub appendfsync: Option<String>,
    #[arg(long)]
    pub no_appendfsync_on_rewrite: Option<String>,
    #[arg(long)]
    pub auto_aof_rewrite_percentage: Option<u32>,
    #[arg(long)]
    pub auto_aof_rewrite_min_size: Option<String>,
    #[arg(long)]
    pub aof_load_truncated: Option<String>,
    #[arg(long)]
    pub aof_use_rdb_preamble: Option<String>,
    #[arg(long)]
    pub aof_timestamp_enabled: Option<String>,
    #[arg(long)]
    pub aof_load_corrupt_tail_max_size: Option<u64>,
    #[arg(long, action = clap::ArgAction::Append)]
    pub save: Option<Vec<String>>,
    #[arg(long)]
    pub stop_writes_on_bgsave_error: Option<String>,
    #[arg(long)]
    pub rdbcompression: Option<String>,
    #[arg(long)]
    pub rdbchecksum: Option<String>,
    #[arg(long)]
    pub sanitize_dump_payload: Option<String>,
    #[arg(long)]
    pub rdb_del_sync_files: Option<String>,

    // --- Replication ---
    #[arg(long)]
    pub masterauth: Option<String>,
    #[arg(long)]
    pub masteruser: Option<String>,
    #[arg(long)]
    pub replica_read_only: Option<String>,
    #[arg(long)]
    pub replica_serve_stale_data: Option<String>,
    #[arg(long)]
    pub repl_diskless_sync: Option<String>,
    #[arg(long)]
    pub repl_diskless_sync_delay: Option<u64>,
    #[arg(long)]
    pub repl_diskless_sync_max_replicas: Option<u32>,
    #[arg(long)]
    pub repl_diskless_load: Option<String>,
    #[arg(long)]
    pub repl_backlog_size: Option<String>,
    #[arg(long)]
    pub repl_backlog_ttl: Option<u64>,
    #[arg(long)]
    pub repl_timeout: Option<u64>,
    #[arg(long)]
    pub repl_disable_tcp_nodelay: Option<String>,
    #[arg(long)]
    pub replica_priority: Option<u32>,
    #[arg(long)]
    pub propagation_error_behavior: Option<String>,
    #[arg(long)]
    pub replica_ignore_disk_write_errors: Option<String>,
    #[arg(long)]
    pub replica_announced: Option<String>,
    #[arg(long)]
    pub min_replicas_to_write: Option<u32>,
    #[arg(long)]
    pub min_replicas_max_lag: Option<u64>,
    #[arg(long)]
    pub repl_ping_replica_period: Option<u64>,
    #[arg(long)]
    pub replica_full_sync_buffer_limit: Option<String>,
    #[arg(long)]
    pub replica_announce_ip: Option<String>,
    #[arg(long)]
    pub replica_announce_port: Option<u16>,

    // --- Cluster ---
    #[arg(long)]
    pub cluster_enabled: Option<String>,
    #[arg(long)]
    pub cluster_config_file: Option<String>,
    #[arg(long)]
    pub cluster_node_timeout: Option<u64>,
    #[arg(long)]
    pub cluster_port: Option<u16>,
    #[arg(long)]
    pub cluster_replica_validity_factor: Option<u32>,
    #[arg(long)]
    pub cluster_migration_barrier: Option<u32>,
    #[arg(long)]
    pub cluster_allow_replica_migration: Option<String>,
    #[arg(long)]
    pub cluster_require_full_coverage: Option<String>,
    #[arg(long)]
    pub cluster_replica_no_failover: Option<String>,
    #[arg(long)]
    pub cluster_allow_reads_when_down: Option<String>,
    #[arg(long)]
    pub cluster_allow_pubsubshard_when_down: Option<String>,
    #[arg(long)]
    pub cluster_link_sendbuf_limit: Option<String>,
    #[arg(long)]
    pub cluster_announce_ip: Option<String>,
    #[arg(long)]
    pub cluster_announce_port: Option<u16>,
    #[arg(long)]
    pub cluster_announce_tls_port: Option<u16>,
    #[arg(long)]
    pub cluster_announce_bus_port: Option<u16>,
    #[arg(long)]
    pub cluster_announce_hostname: Option<String>,
    #[arg(long)]
    pub cluster_announce_human_nodename: Option<String>,
    #[arg(long)]
    pub cluster_preferred_endpoint_type: Option<String>,
    #[arg(long)]
    pub cluster_compatibility_sample_ratio: Option<u32>,
    #[arg(long)]
    pub cluster_slot_stats_enabled: Option<String>,
    #[arg(long)]
    pub cluster_slot_migration_write_pause_timeout: Option<u64>,
    #[arg(long)]
    pub cluster_slot_migration_handoff_max_lag_bytes: Option<String>,

    // --- Limits & Others ---
    #[arg(long)]
    pub maxclients: Option<u32>,
    #[arg(long)]
    pub slowlog_log_slower_than: Option<i64>,
    #[arg(long)]
    pub slowlog_max_len: Option<u64>,
    #[arg(long)]
    pub latency_monitor_threshold: Option<u64>,
    #[arg(long)]
    pub notify_keyspace_events: Option<String>,
    #[arg(long)]
    pub lookahead: Option<u32>,
    #[arg(long)]
    pub maxmemory_clients: Option<String>,
    #[arg(long)]
    pub max_new_tls_connections_per_cycle: Option<u32>,
    #[arg(long)]
    pub latency_tracking: Option<String>,
    #[arg(long, allow_hyphen_values = true)]
    pub latency_tracking_info_percentiles: Option<String>, // Space separated list
    #[arg(long)]
    pub io_threads: Option<usize>,
    #[arg(long)]
    pub oom_score_adj: Option<String>,
    #[arg(long)]
    pub oom_score_adj_values: Option<String>, // Space separated triple
    #[arg(long)]
    pub disable_thp: Option<String>,
    #[arg(long)]
    pub server_cpulist: Option<String>,
    #[arg(long)]
    pub bio_cpulist: Option<String>,
    #[arg(long)]
    pub aof_rewrite_cpulist: Option<String>,
    #[arg(long)]
    pub bgsave_cpulist: Option<String>,
    #[arg(long)]
    pub shutdown_timeout: Option<u64>,
    #[arg(long)]
    pub shutdown_on_sigint: Option<String>,
    #[arg(long)]
    pub shutdown_on_sigterm: Option<String>,
    #[arg(long)]
    pub lua_time_limit: Option<u64>,

    // --- Hash, Set, ZSet, List, Stream limits ---
    #[arg(long)]
    pub hash_max_listpack_entries: Option<usize>,
    #[arg(long)]
    pub hash_max_listpack_value: Option<usize>,
    #[arg(long)]
    pub list_max_listpack_size: Option<i32>,
    #[arg(long)]
    pub list_compress_depth: Option<u32>,
    #[arg(long)]
    pub set_max_intset_entries: Option<usize>,
    #[arg(long)]
    pub set_max_listpack_entries: Option<usize>,
    #[arg(long)]
    pub set_max_listpack_value: Option<usize>,
    #[arg(long)]
    pub zset_max_listpack_entries: Option<usize>,
    #[arg(long)]
    pub zset_max_listpack_value: Option<usize>,
    #[arg(long)]
    pub hll_sparse_max_bytes: Option<usize>,
    #[arg(long)]
    pub stream_node_max_bytes: Option<String>,
    #[arg(long)]
    pub stream_node_max_entries: Option<usize>,
    #[arg(long)]
    pub activerehashing: Option<String>,
    #[arg(long, action = clap::ArgAction::Append)]
    pub client_output_buffer_limit: Option<Vec<String>>,
    #[arg(long)]
    pub client_query_buffer_limit: Option<String>,
    #[arg(long)]
    pub proto_max_bulk_len: Option<String>,
    #[arg(long)]
    pub hz: Option<u32>,
    #[arg(long)]
    pub dynamic_hz: Option<String>,
    #[arg(long)]
    pub aof_rewrite_incremental_fsync: Option<String>,
    #[arg(long)]
    pub rdb_save_incremental_fsync: Option<String>,
    #[arg(long)]
    pub lfu_log_factor: Option<u32>,
    #[arg(long)]
    pub lfu_decay_time: Option<u32>,
    #[arg(long)]
    pub max_new_connections_per_cycle: Option<u32>,
    #[arg(long)]
    pub jemalloc_bg_thread: Option<String>,
    #[arg(long)]
    pub ignore_warnings: Option<String>,
    #[arg(long)]
    pub activedefrag: Option<String>,
    #[arg(long)]
    pub active_defrag_ignore_bytes: Option<String>,
    #[arg(long)]
    pub active_defrag_threshold_lower: Option<u32>,
    #[arg(long)]
    pub active_defrag_threshold_upper: Option<u32>,
    #[arg(long)]
    pub active_defrag_cycle_min: Option<u32>,
    #[arg(long)]
    pub active_defrag_cycle_max: Option<u32>,
    #[arg(long)]
    pub active_defrag_max_scan_fields: Option<u32>,
}

impl Cli {
    pub fn load_config() -> Result<ServerConfig, String> {
        let cli = Cli::parse();
        let mut config = ServerConfig::default();

        if let Some(path) = &cli.config_file {
            parse_config_file(path, &mut config)?;
        }

        if let Some(port) = cli.port {
            config.port = port;
        }
        if let Some(bind) = cli.bind {
            config.bind = bind.split_whitespace().map(|s| s.to_string()).collect();
        }
        if let Some(dbs) = cli.databases {
            config.databases = dbs;
        }
        if let Some(daemonize) = cli.daemonize {
            config.daemonize = daemonize;
        }
        if let Some(loglevel) = cli.loglevel {
            config.loglevel = loglevel;
        }
        if let Some(logfile) = cli.logfile {
            config.logfile = logfile;
        }
        if let Some(pass) = cli.requirepass {
            config.requirepass = Some(pass);
        }
        if let Some(pm) = cli.protected_mode {
            config.protected_mode = parse_bool(&pm).unwrap_or(true);
        }
        if let Some(mem) = cli.maxmemory {
            config.maxmemory =
                parse_memory(&mem).map_err(|e| format!("Invalid maxmemory: {}", e))?;
        }
        if let Some(pol) = cli.maxmemory_policy {
            config.maxmemory_policy = MaxMemoryPolicy::from_str(&pol)?;
        }
        if let Some(aof) = cli.appendonly {
            config.appendonly = parse_bool(&aof).unwrap_or(false);
        }
        if let Some(dbfile) = cli.dbfilename {
            config.dbfilename = dbfile;
        }
        if let Some(d) = cli.dir {
            config.dir = d;
        }
        if let Some(args) = cli.replicaof
            && args.len() == 2
        {
            let port = args[1]
                .parse::<u16>()
                .map_err(|_| "Invalid replicaof port")?;
            config.replicaof = Some((args[0].clone(), port));
        }

        // --- Network ---
        if let Some(v) = cli.tcp_backlog {
            config.tcp_backlog = v;
        }
        if let Some(v) = cli.unixsocket {
            config.unixsocket = Some(v);
        }
        if let Some(v) = cli.unixsocketperm {
            config.unixsocketperm = Some(v);
        }
        if let Some(v) = cli.tcp_keepalive {
            config.tcp_keepalive = v;
        }
        if let Some(v) = cli.timeout {
            config.timeout = v;
        }
        if let Some(v) = cli.socket_mark_id {
            config.socket_mark_id = v;
        }
        if let Some(v) = cli.bind_source_addr {
            config.bind_source_addr = Some(v);
        }

        // --- TLS/SSL ---
        if let Some(v) = cli.tls_port {
            config.tls_port = v;
        }
        if let Some(v) = cli.tls_cert_file {
            config.tls_cert_file = Some(v);
        }
        if let Some(v) = cli.tls_key_file {
            config.tls_key_file = Some(v);
        }
        if let Some(v) = cli.tls_key_file_pass {
            config.tls_key_file_pass = Some(v);
        }
        if let Some(v) = cli.tls_client_cert_file {
            config.tls_client_cert_file = Some(v);
        }
        if let Some(v) = cli.tls_client_key_file {
            config.tls_client_key_file = Some(v);
        }
        if let Some(v) = cli.tls_client_key_file_pass {
            config.tls_client_key_file_pass = Some(v);
        }
        if let Some(v) = cli.tls_dh_params_file {
            config.tls_dh_params_file = Some(v);
        }
        if let Some(v) = cli.tls_ca_cert_file {
            config.tls_ca_cert_file = Some(v);
        }
        if let Some(v) = cli.tls_ca_cert_dir {
            config.tls_ca_cert_dir = Some(v);
        }
        if let Some(v) = cli.tls_auth_clients {
            config.tls_auth_clients = v;
        }
        if let Some(v) = cli.tls_auth_clients_user {
            config.tls_auth_clients_user = Some(v);
        }
        if let Some(v) = cli.tls_replication {
            config.tls_replication = parse_bool(&v).unwrap_or(false);
        }
        if let Some(v) = cli.tls_cluster {
            config.tls_cluster = parse_bool(&v).unwrap_or(false);
        }
        if let Some(v) = cli.tls_protocols {
            config.tls_protocols = Some(v);
        }
        if let Some(v) = cli.tls_ciphers {
            config.tls_ciphers = Some(v);
        }
        if let Some(v) = cli.tls_ciphersuites {
            config.tls_ciphersuites = Some(v);
        }
        if let Some(v) = cli.tls_prefer_server_ciphers {
            config.tls_prefer_server_ciphers = parse_bool(&v).unwrap_or(false);
        }
        if let Some(v) = cli.tls_session_caching {
            config.tls_session_caching = parse_bool(&v).unwrap_or(true);
        }
        if let Some(v) = cli.tls_session_cache_size {
            config.tls_session_cache_size = v;
        }
        if let Some(v) = cli.tls_session_cache_timeout {
            config.tls_session_cache_timeout = v;
        }

        // --- General ---
        if let Some(v) = cli.supervised {
            config.supervised = v;
        }
        if let Some(v) = cli.pidfile {
            config.pidfile = v;
        }
        if let Some(v) = cli.crash_log_enabled {
            config.crash_log_enabled = parse_bool(&v).unwrap_or(true);
        }
        if let Some(v) = cli.crash_memcheck_enabled {
            config.crash_memcheck_enabled = parse_bool(&v).unwrap_or(true);
        }
        if let Some(v) = cli.always_show_logo {
            config.always_show_logo = parse_bool(&v).unwrap_or(true);
        }
        if let Some(v) = cli.set_proc_title {
            config.set_proc_title = parse_bool(&v).unwrap_or(true);
        }
        if let Some(v) = cli.proc_title_template {
            config.proc_title_template = v;
        }
        if let Some(v) = cli.enable_protected_configs {
            config.enable_protected_configs = parse_bool(&v).unwrap_or(false);
        }
        if let Some(v) = cli.enable_debug_command {
            config.enable_debug_command = parse_bool(&v).unwrap_or(false);
        }
        if let Some(v) = cli.enable_module_command {
            config.enable_module_command = parse_bool(&v).unwrap_or(false);
        }
        if let Some(v) = cli.syslog_enabled {
            config.syslog_enabled = parse_bool(&v).unwrap_or(false);
        }
        if let Some(v) = cli.syslog_ident {
            config.syslog_ident = v;
        }
        if let Some(v) = cli.syslog_facility {
            config.syslog_facility = v;
        }
        if let Some(v) = cli.hide_user_data_from_log {
            config.hide_user_data_from_log = parse_bool(&v).unwrap_or(false);
        }
        if let Some(v) = cli.locale_collate {
            config.locale_collate = v;
        }

        // --- Security ---
        if let Some(v) = cli.acl_pubsub_default {
            config.acl_pubsub_default = v;
        }
        if let Some(v) = cli.tracking_table_max_keys {
            config.tracking_table_max_keys = v;
        }
        if let Some(v) = cli.acllog_max_len {
            config.acllog_max_len = v;
        }
        if let Some(v) = cli.aclfile {
            config.aclfile = Some(v);
        }

        // --- Memory ---
        if let Some(v) = cli.maxmemory_samples {
            config.maxmemory_samples = v;
        }
        if let Some(v) = cli.maxmemory_eviction_tenacity {
            config.maxmemory_eviction_tenacity = v;
        }
        if let Some(v) = cli.replica_ignore_maxmemory {
            config.replica_ignore_maxmemory = parse_bool(&v).unwrap_or(true);
        }
        if let Some(v) = cli.active_expire_effort {
            config.active_expire_effort = v;
        }

        // --- Lazy Freeing ---
        if let Some(v) = cli.lazyfree_lazy_eviction {
            config.lazyfree_lazy_eviction = parse_bool(&v).unwrap_or(false);
        }
        if let Some(v) = cli.lazyfree_lazy_expire {
            config.lazyfree_lazy_expire = parse_bool(&v).unwrap_or(false);
        }
        if let Some(v) = cli.lazyfree_lazy_server_del {
            config.lazyfree_lazy_server_del = parse_bool(&v).unwrap_or(false);
        }
        if let Some(v) = cli.replica_lazy_flush {
            config.replica_lazy_flush = parse_bool(&v).unwrap_or(false);
        }
        if let Some(v) = cli.lazyfree_lazy_user_del {
            config.lazyfree_lazy_user_del = parse_bool(&v).unwrap_or(false);
        }
        if let Some(v) = cli.lazyfree_lazy_user_flush {
            config.lazyfree_lazy_user_flush = parse_bool(&v).unwrap_or(false);
        }

        // --- Persistence ---
        if let Some(v) = cli.appendfilename {
            config.appendfilename = v;
        }
        if let Some(v) = cli.appenddirname {
            config.appenddirname = v;
        }
        if let Some(v) = cli.appendfsync {
            config.appendfsync = v;
        }
        if let Some(v) = cli.no_appendfsync_on_rewrite {
            config.no_appendfsync_on_rewrite = parse_bool(&v).unwrap_or(false);
        }
        if let Some(v) = cli.auto_aof_rewrite_percentage {
            config.auto_aof_rewrite_percentage = v;
        }
        if let Some(v) = cli.auto_aof_rewrite_min_size {
            config.auto_aof_rewrite_min_size = parse_memory(&v)
                .map_err(|e| format!("Invalid auto-aof-rewrite-min-size: {}", e))?;
        }
        if let Some(v) = cli.aof_load_truncated {
            config.aof_load_truncated = parse_bool(&v).unwrap_or(true);
        }
        if let Some(v) = cli.aof_use_rdb_preamble {
            config.aof_use_rdb_preamble = parse_bool(&v).unwrap_or(true);
        }
        if let Some(v) = cli.aof_timestamp_enabled {
            config.aof_timestamp_enabled = parse_bool(&v).unwrap_or(false);
        }
        if let Some(v) = cli.aof_load_corrupt_tail_max_size {
            config.aof_load_corrupt_tail_max_size = v;
        }
        if let Some(args) = cli.save {
            config.save_points.clear();
            for arg in args {
                let parts: Vec<&str> = arg.split_whitespace().collect();
                if parts.len() == 2 {
                    let sec = parts[0]
                        .parse::<u64>()
                        .map_err(|_| "Invalid save seconds")?;
                    let changes = parts[1]
                        .parse::<u64>()
                        .map_err(|_| "Invalid save changes")?;
                    config.save_points.push((sec, changes));
                } else if parts.len() == 1 && parts[0].is_empty() {
                    config.save_points.clear();
                }
            }
        }
        if let Some(v) = cli.stop_writes_on_bgsave_error {
            config.stop_writes_on_bgsave_error = parse_bool(&v).unwrap_or(true);
        }
        if let Some(v) = cli.rdbcompression {
            config.rdbcompression = parse_bool(&v).unwrap_or(true);
        }
        if let Some(v) = cli.rdbchecksum {
            config.rdbchecksum = parse_bool(&v).unwrap_or(true);
        }
        if let Some(v) = cli.sanitize_dump_payload {
            config.sanitize_dump_payload = v;
        }
        if let Some(v) = cli.rdb_del_sync_files {
            config.rdb_del_sync_files = parse_bool(&v).unwrap_or(false);
        }

        // --- Replication ---
        if let Some(v) = cli.masterauth {
            config.masterauth = Some(v);
        }
        if let Some(v) = cli.masteruser {
            config.masteruser = Some(v);
        }
        if let Some(v) = cli.replica_read_only {
            config.replica_read_only = parse_bool(&v).unwrap_or(true);
        }
        if let Some(v) = cli.replica_serve_stale_data {
            config.replica_serve_stale_data = parse_bool(&v).unwrap_or(true);
        }
        if let Some(v) = cli.repl_diskless_sync {
            config.repl_diskless_sync = parse_bool(&v).unwrap_or(true);
        }
        if let Some(v) = cli.repl_diskless_sync_delay {
            config.repl_diskless_sync_delay = v;
        }
        if let Some(v) = cli.repl_diskless_sync_max_replicas {
            config.repl_diskless_sync_max_replicas = v;
        }
        if let Some(v) = cli.repl_diskless_load {
            config.repl_diskless_load = v;
        }
        if let Some(v) = cli.repl_backlog_size {
            config.repl_backlog_size =
                parse_memory(&v).map_err(|e| format!("Invalid repl-backlog-size: {}", e))?;
        }
        if let Some(v) = cli.repl_backlog_ttl {
            config.repl_backlog_ttl = v;
        }
        if let Some(v) = cli.repl_timeout {
            config.repl_timeout = v;
        }
        if let Some(v) = cli.repl_disable_tcp_nodelay {
            config.repl_disable_tcp_nodelay = parse_bool(&v).unwrap_or(false);
        }
        if let Some(v) = cli.replica_priority {
            config.replica_priority = v;
        }
        if let Some(v) = cli.propagation_error_behavior {
            config.propagation_error_behavior = v;
        }
        if let Some(v) = cli.replica_ignore_disk_write_errors {
            config.replica_ignore_disk_write_errors = v;
        }
        if let Some(v) = cli.replica_announced {
            config.replica_announced = parse_bool(&v).unwrap_or(true);
        }
        if let Some(v) = cli.min_replicas_to_write {
            config.min_replicas_to_write = v;
        }
        if let Some(v) = cli.min_replicas_max_lag {
            config.min_replicas_max_lag = v;
        }
        if let Some(v) = cli.repl_ping_replica_period {
            config.repl_ping_replica_period = v;
        }
        if let Some(v) = cli.replica_full_sync_buffer_limit {
            config.replica_full_sync_buffer_limit = parse_memory(&v)
                .map_err(|e| format!("Invalid replica-full-sync-buffer-limit: {}", e))?;
        }
        if let Some(v) = cli.replica_announce_ip {
            config.replica_announce_ip = Some(v);
        }
        if let Some(v) = cli.replica_announce_port {
            config.replica_announce_port = Some(v);
        }

        // --- Cluster ---
        if let Some(v) = cli.cluster_enabled {
            config.cluster_enabled = parse_bool(&v).unwrap_or(false);
        }
        if let Some(v) = cli.cluster_config_file {
            config.cluster_config_file = v;
        }
        if let Some(v) = cli.cluster_node_timeout {
            config.cluster_node_timeout = v;
        }
        if let Some(v) = cli.cluster_port {
            config.cluster_port = v;
        }
        if let Some(v) = cli.cluster_replica_validity_factor {
            config.cluster_replica_validity_factor = v;
        }
        if let Some(v) = cli.cluster_migration_barrier {
            config.cluster_migration_barrier = v;
        }
        if let Some(v) = cli.cluster_allow_replica_migration {
            config.cluster_allow_replica_migration = parse_bool(&v).unwrap_or(true);
        }
        if let Some(v) = cli.cluster_require_full_coverage {
            config.cluster_require_full_coverage = parse_bool(&v).unwrap_or(true);
        }
        if let Some(v) = cli.cluster_replica_no_failover {
            config.cluster_replica_no_failover = parse_bool(&v).unwrap_or(false);
        }
        if let Some(v) = cli.cluster_allow_reads_when_down {
            config.cluster_allow_reads_when_down = parse_bool(&v).unwrap_or(false);
        }
        if let Some(v) = cli.cluster_allow_pubsubshard_when_down {
            config.cluster_allow_pubsubshard_when_down = parse_bool(&v).unwrap_or(true);
        }
        if let Some(v) = cli.cluster_link_sendbuf_limit {
            config.cluster_link_sendbuf_limit = parse_memory(&v)
                .map_err(|e| format!("Invalid cluster-link-sendbuf-limit: {}", e))?;
        }
        if let Some(v) = cli.cluster_announce_ip {
            config.cluster_announce_ip = Some(v);
        }
        if let Some(v) = cli.cluster_announce_port {
            config.cluster_announce_port = Some(v);
        }
        if let Some(v) = cli.cluster_announce_tls_port {
            config.cluster_announce_tls_port = Some(v);
        }
        if let Some(v) = cli.cluster_announce_bus_port {
            config.cluster_announce_bus_port = Some(v);
        }
        if let Some(v) = cli.cluster_announce_hostname {
            config.cluster_announce_hostname = Some(v);
        }
        if let Some(v) = cli.cluster_announce_human_nodename {
            config.cluster_announce_human_nodename = Some(v);
        }
        if let Some(v) = cli.cluster_preferred_endpoint_type {
            config.cluster_preferred_endpoint_type = v;
        }
        if let Some(v) = cli.cluster_compatibility_sample_ratio {
            config.cluster_compatibility_sample_ratio = v;
        }
        if let Some(v) = cli.cluster_slot_stats_enabled {
            config.cluster_slot_stats_enabled = parse_bool(&v).unwrap_or(false);
        }
        if let Some(v) = cli.cluster_slot_migration_write_pause_timeout {
            config.cluster_slot_migration_write_pause_timeout = v;
        }
        if let Some(v) = cli.cluster_slot_migration_handoff_max_lag_bytes {
            config.cluster_slot_migration_handoff_max_lag_bytes =
                parse_memory(&v).map_err(|e| {
                    format!(
                        "Invalid cluster-slot-migration-handoff-max-lag-bytes: {}",
                        e
                    )
                })?;
        }

        // --- Limits & Others ---
        if let Some(v) = cli.maxclients {
            config.maxclients = v;
        }
        if let Some(v) = cli.slowlog_log_slower_than {
            config.slowlog_log_slower_than = v;
        }
        if let Some(v) = cli.slowlog_max_len {
            config.slowlog_max_len = v;
        }
        if let Some(v) = cli.latency_monitor_threshold {
            config.latency_monitor_threshold = v;
        }
        if let Some(v) = cli.notify_keyspace_events {
            config.notify_keyspace_events = v;
        }
        if let Some(v) = cli.lookahead {
            config.lookahead = v;
        }
        if let Some(v) = cli.maxmemory_clients {
            config.maxmemory_clients = v;
        }
        if let Some(v) = cli.max_new_tls_connections_per_cycle {
            config.max_new_tls_connections_per_cycle = v;
        }
        if let Some(v) = cli.latency_tracking {
            config.latency_tracking = parse_bool(&v).unwrap_or(true);
        }
        if let Some(v) = cli.latency_tracking_info_percentiles {
            let mut percentiles = Vec::new();
            for part in v.split_whitespace() {
                let p = part
                    .parse::<f64>()
                    .map_err(|_| "Invalid latency percentile")?;
                percentiles.push(p);
            }
            config.latency_tracking_info_percentiles = percentiles;
        }
        if let Some(v) = cli.io_threads {
            config.io_threads = v;
        }
        if let Some(v) = cli.oom_score_adj {
            config.oom_score_adj = v;
        }
        if let Some(v) = cli.oom_score_adj_values {
            let parts: Vec<&str> = v.split_whitespace().collect();
            if parts.len() == 3 {
                let p1 = parts[0]
                    .parse::<i32>()
                    .map_err(|_| "Invalid oom-score-adj-value")?;
                let p2 = parts[1]
                    .parse::<i32>()
                    .map_err(|_| "Invalid oom-score-adj-value")?;
                let p3 = parts[2]
                    .parse::<i32>()
                    .map_err(|_| "Invalid oom-score-adj-value")?;
                config.oom_score_adj_values = (p1, p2, p3);
            }
        }
        if let Some(v) = cli.disable_thp {
            config.disable_thp = parse_bool(&v).unwrap_or(true);
        }
        if let Some(v) = cli.server_cpulist {
            config.server_cpulist = Some(v);
        }
        if let Some(v) = cli.bio_cpulist {
            config.bio_cpulist = Some(v);
        }
        if let Some(v) = cli.aof_rewrite_cpulist {
            config.aof_rewrite_cpulist = Some(v);
        }
        if let Some(v) = cli.bgsave_cpulist {
            config.bgsave_cpulist = Some(v);
        }
        if let Some(v) = cli.shutdown_timeout {
            config.shutdown_timeout = v;
        }
        if let Some(v) = cli.shutdown_on_sigint {
            config.shutdown_on_sigint = v;
        }
        if let Some(v) = cli.shutdown_on_sigterm {
            config.shutdown_on_sigterm = v;
        }
        if let Some(v) = cli.lua_time_limit {
            config.lua_time_limit = v;
        }
        if let Some(v) = cli.hash_max_listpack_entries {
            config.hash_max_listpack_entries = v;
        }
        if let Some(v) = cli.hash_max_listpack_value {
            config.hash_max_listpack_value = v;
        }
        if let Some(v) = cli.list_max_listpack_size {
            config.list_max_listpack_size = v;
        }
        if let Some(v) = cli.list_compress_depth {
            config.list_compress_depth = v;
        }
        if let Some(v) = cli.set_max_intset_entries {
            config.set_max_intset_entries = v;
        }
        if let Some(v) = cli.set_max_listpack_entries {
            config.set_max_listpack_entries = v;
        }
        if let Some(v) = cli.set_max_listpack_value {
            config.set_max_listpack_value = v;
        }
        if let Some(v) = cli.zset_max_listpack_entries {
            config.zset_max_listpack_entries = v;
        }
        if let Some(v) = cli.zset_max_listpack_value {
            config.zset_max_listpack_value = v;
        }
        if let Some(v) = cli.hll_sparse_max_bytes {
            config.hll_sparse_max_bytes = v;
        }
        if let Some(v) = cli.stream_node_max_bytes {
            config.stream_node_max_bytes = parse_memory(&v)
                .map_err(|e| format!("Invalid stream-node-max-bytes: {}", e))?
                as usize;
        }
        if let Some(v) = cli.stream_node_max_entries {
            config.stream_node_max_entries = v;
        }
        if let Some(v) = cli.activerehashing {
            config.activerehashing = parse_bool(&v).unwrap_or(true);
        }
        if let Some(args) = cli.client_output_buffer_limit {
            for arg in args {
                let parts: Vec<&str> = arg.split_whitespace().collect();
                if parts.len() >= 4 {
                    let class = parts[0];
                    let limit = format!("{} {} {}", parts[1], parts[2], parts[3]);
                    match class.to_lowercase().as_str() {
                        "normal" => config.client_output_buffer_limit_normal = limit,
                        "replica" => config.client_output_buffer_limit_replica = limit,
                        "pubsub" => config.client_output_buffer_limit_pubsub = limit,
                        _ => {}
                    }
                }
            }
        }
        if let Some(v) = cli.client_query_buffer_limit {
            config.client_query_buffer_limit = parse_memory(&v)
                .map_err(|e| format!("Invalid client-query-buffer-limit: {}", e))?;
        }
        if let Some(v) = cli.proto_max_bulk_len {
            config.proto_max_bulk_len =
                parse_memory(&v).map_err(|e| format!("Invalid proto-max-bulk-len: {}", e))?;
        }
        if let Some(v) = cli.hz {
            config.hz = v;
        }
        if let Some(v) = cli.dynamic_hz {
            config.dynamic_hz = parse_bool(&v).unwrap_or(true);
        }
        if let Some(v) = cli.aof_rewrite_incremental_fsync {
            config.aof_rewrite_incremental_fsync = parse_bool(&v).unwrap_or(true);
        }
        if let Some(v) = cli.rdb_save_incremental_fsync {
            config.rdb_save_incremental_fsync = parse_bool(&v).unwrap_or(true);
        }
        if let Some(v) = cli.lfu_log_factor {
            config.lfu_log_factor = v;
        }
        if let Some(v) = cli.lfu_decay_time {
            config.lfu_decay_time = v;
        }
        if let Some(v) = cli.max_new_connections_per_cycle {
            config.max_new_connections_per_cycle = v;
        }
        if let Some(v) = cli.jemalloc_bg_thread {
            config.jemalloc_bg_thread = parse_bool(&v).unwrap_or(true);
        }
        if let Some(v) = cli.ignore_warnings {
            config.ignore_warnings = Some(v);
        }
        if let Some(v) = cli.activedefrag {
            config.activedefrag = parse_bool(&v).unwrap_or(false);
        }
        if let Some(v) = cli.active_defrag_ignore_bytes {
            config.active_defrag_ignore_bytes = parse_memory(&v)
                .map_err(|e| format!("Invalid active-defrag-ignore-bytes: {}", e))?;
        }
        if let Some(v) = cli.active_defrag_threshold_lower {
            config.active_defrag_threshold_lower = v;
        }
        if let Some(v) = cli.active_defrag_threshold_upper {
            config.active_defrag_threshold_upper = v;
        }
        if let Some(v) = cli.active_defrag_cycle_min {
            config.active_defrag_cycle_min = v;
        }
        if let Some(v) = cli.active_defrag_cycle_max {
            config.active_defrag_cycle_max = v;
        }
        if let Some(v) = cli.active_defrag_max_scan_fields {
            config.active_defrag_max_scan_fields = v;
        }

        Ok(config)
    }
}

fn parse_config_file(path: &Path, config: &mut ServerConfig) -> Result<(), String> {
    let content =
        fs::read_to_string(path).map_err(|e| format!("Failed to read config file: {}", e))?;

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let parts = split_args(line)?;
        if parts.is_empty() {
            continue;
        }

        let directive = parts[0].to_lowercase();
        let args = &parts[1..];

        match directive.as_str() {
            "port" if !args.is_empty() => config.port = parse_u16(&args[0], "port")?,
            "bind" if !args.is_empty() => config.bind = args.to_vec(),
            "tcp-backlog" if !args.is_empty() => {
                config.tcp_backlog = parse_u32(&args[0], "tcp-backlog")?
            }
            "unixsocket" if !args.is_empty() => config.unixsocket = Some(args[0].clone()),
            "unixsocketperm" if !args.is_empty() => {
                config.unixsocketperm = Some(parse_octal_or_decimal(&args[0], "unixsocketperm")?)
            }
            "timeout" if !args.is_empty() => config.timeout = parse_u64(&args[0], "timeout")?,
            "tcp-keepalive" if !args.is_empty() => {
                config.tcp_keepalive = parse_u32(&args[0], "tcp-keepalive")?
            }
            "socket-mark-id" if !args.is_empty() => {
                config.socket_mark_id = parse_u32(&args[0], "socket-mark-id")?
            }
            "protected-mode" if !args.is_empty() => config.protected_mode = parse_bool(&args[0])?,
            "bind-source-addr" if !args.is_empty() => {
                config.bind_source_addr = Some(args[0].clone())
            }
            "enable-protected-configs" if !args.is_empty() => {
                config.enable_protected_configs = parse_bool(&args[0])?
            }
            "enable-debug-command" if !args.is_empty() => {
                config.enable_debug_command = parse_bool(&args[0])?
            }
            "enable-module-command" if !args.is_empty() => {
                config.enable_module_command = parse_bool(&args[0])?
            }
            "rename-command" if args.len() >= 2 => {
                config
                    .rename_command
                    .insert(args[0].clone(), args[1].clone());
            }

            // Logging
            "syslog-enabled" if !args.is_empty() => config.syslog_enabled = parse_bool(&args[0])?,
            "syslog-ident" if !args.is_empty() => config.syslog_ident = args[0].clone(),
            "syslog-facility" if !args.is_empty() => config.syslog_facility = args[0].clone(),
            "hide-user-data-from-log" if !args.is_empty() => {
                config.hide_user_data_from_log = parse_bool(&args[0])?
            }
            "locale-collate" if !args.is_empty() => config.locale_collate = args[0].clone(),

            "tls-port" if !args.is_empty() => config.tls_port = parse_u16(&args[0], "tls-port")?,
            "tls-cert-file" if !args.is_empty() => config.tls_cert_file = Some(args[0].clone()),
            "tls-key-file" if !args.is_empty() => config.tls_key_file = Some(args[0].clone()),
            "tls-key-file-pass" if !args.is_empty() => {
                config.tls_key_file_pass = Some(args[0].clone())
            }
            "tls-client-cert-file" if !args.is_empty() => {
                config.tls_client_cert_file = Some(args[0].clone())
            }
            "tls-client-key-file" if !args.is_empty() => {
                config.tls_client_key_file = Some(args[0].clone())
            }
            "tls-client-key-file-pass" if !args.is_empty() => {
                config.tls_client_key_file_pass = Some(args[0].clone())
            }
            "tls-dh-params-file" if !args.is_empty() => {
                config.tls_dh_params_file = Some(args[0].clone())
            }
            "tls-ca-cert-file" if !args.is_empty() => {
                config.tls_ca_cert_file = Some(args[0].clone())
            }
            "tls-ca-cert-dir" if !args.is_empty() => config.tls_ca_cert_dir = Some(args[0].clone()),
            "tls-auth-clients" if !args.is_empty() => config.tls_auth_clients = args[0].clone(),
            "tls-auth-clients-user" if !args.is_empty() => {
                config.tls_auth_clients_user = Some(args[0].clone())
            }
            "tls-replication" if !args.is_empty() => config.tls_replication = parse_bool(&args[0])?,
            "tls-cluster" if !args.is_empty() => config.tls_cluster = parse_bool(&args[0])?,
            "tls-protocols" if !args.is_empty() => config.tls_protocols = Some(args[0].clone()),
            "tls-ciphers" if !args.is_empty() => config.tls_ciphers = Some(args[0].clone()),
            "tls-ciphersuites" if !args.is_empty() => {
                config.tls_ciphersuites = Some(args[0].clone())
            }
            "tls-prefer-server-ciphers" if !args.is_empty() => {
                config.tls_prefer_server_ciphers = parse_bool(&args[0])?
            }
            "tls-session-caching" if !args.is_empty() => {
                config.tls_session_caching = parse_bool(&args[0])?
            }
            "tls-session-cache-size" if !args.is_empty() => {
                config.tls_session_cache_size = parse_usize(&args[0], "tls-session-cache-size")?
            }
            "tls-session-cache-timeout" if !args.is_empty() => {
                config.tls_session_cache_timeout = parse_u64(&args[0], "tls-session-cache-timeout")?
            }

            "daemonize" if !args.is_empty() => config.daemonize = parse_bool(&args[0])?,
            "supervised" if !args.is_empty() => config.supervised = args[0].clone(),
            "loglevel" if !args.is_empty() => config.loglevel = args[0].clone(),
            "logfile" if !args.is_empty() => config.logfile = args[0].clone(),
            "databases" if !args.is_empty() => {
                config.databases = parse_usize(&args[0], "databases")?
            }
            "always-show-logo" if !args.is_empty() => {
                config.always_show_logo = parse_bool(&args[0])?
            }
            "set-proc-title" if !args.is_empty() => config.set_proc_title = parse_bool(&args[0])?,
            "proc-title-template" if !args.is_empty() => {
                config.proc_title_template = args[0].clone()
            }
            "pidfile" if !args.is_empty() => config.pidfile = args[0].clone(),
            "crash-log-enabled" if !args.is_empty() => {
                config.crash_log_enabled = parse_bool(&args[0])?
            }
            "crash-memcheck-enabled" if !args.is_empty() => {
                config.crash_memcheck_enabled = parse_bool(&args[0])?
            }

            "requirepass" if !args.is_empty() => config.requirepass = Some(args[0].clone()),
            "acl-pubsub-default" if !args.is_empty() => config.acl_pubsub_default = args[0].clone(),
            "tracking-table-max-keys" if !args.is_empty() => {
                config.tracking_table_max_keys = parse_u64(&args[0], "tracking-table-max-keys")?
            }

            "acllog-max-len" if !args.is_empty() => {
                config.acllog_max_len = parse_usize(&args[0], "acllog-max-len")?
            }
            "aclfile" if !args.is_empty() => config.aclfile = Some(args[0].clone()),

            "maxmemory" if !args.is_empty() => config.maxmemory = parse_memory(&args[0])?,
            "maxmemory-policy" if !args.is_empty() => {
                config.maxmemory_policy = MaxMemoryPolicy::from_str(&args[0])?
            }
            "maxmemory-samples" if !args.is_empty() => {
                config.maxmemory_samples = parse_usize(&args[0], "maxmemory-samples")?
            }
            "maxmemory-eviction-tenacity" if !args.is_empty() => {
                config.maxmemory_eviction_tenacity =
                    parse_u32(&args[0], "maxmemory-eviction-tenacity")?
            }
            "replica-ignore-maxmemory" if !args.is_empty() => {
                config.replica_ignore_maxmemory = parse_bool(&args[0])?
            }
            "active-expire-effort" if !args.is_empty() => {
                config.active_expire_effort = parse_u32(&args[0], "active-expire-effort")?
            }

            "lazyfree-lazy-eviction" if !args.is_empty() => {
                config.lazyfree_lazy_eviction = parse_bool(&args[0])?
            }
            "lazyfree-lazy-expire" if !args.is_empty() => {
                config.lazyfree_lazy_expire = parse_bool(&args[0])?
            }
            "lazyfree-lazy-server-del" if !args.is_empty() => {
                config.lazyfree_lazy_server_del = parse_bool(&args[0])?
            }
            "replica-lazy-flush" if !args.is_empty() => {
                config.replica_lazy_flush = parse_bool(&args[0])?
            }
            "lazyfree-lazy-user-del" if !args.is_empty() => {
                config.lazyfree_lazy_user_del = parse_bool(&args[0])?
            }
            "lazyfree-lazy-user-flush" if !args.is_empty() => {
                config.lazyfree_lazy_user_flush = parse_bool(&args[0])?
            }

            "appendonly" if !args.is_empty() => config.appendonly = parse_bool(&args[0])?,
            "appendfilename" if !args.is_empty() => config.appendfilename = args[0].clone(),
            "appenddirname" if !args.is_empty() => config.appenddirname = args[0].clone(),
            "appendfsync" if !args.is_empty() => config.appendfsync = args[0].clone(),
            "no-appendfsync-on-rewrite" if !args.is_empty() => {
                config.no_appendfsync_on_rewrite = parse_bool(&args[0])?
            }
            "auto-aof-rewrite-percentage" if !args.is_empty() => {
                config.auto_aof_rewrite_percentage =
                    parse_u32(&args[0], "auto-aof-rewrite-percentage")?
            }
            "auto-aof-rewrite-min_size" if !args.is_empty() => {
                config.auto_aof_rewrite_min_size = parse_memory(&args[0])?
            }
            "aof-load-truncated" if !args.is_empty() => {
                config.aof_load_truncated = parse_bool(&args[0])?
            }
            "aof-use-rdb-preamble" if !args.is_empty() => {
                config.aof_use_rdb_preamble = parse_bool(&args[0])?
            }
            "aof-timestamp-enabled" if !args.is_empty() => {
                config.aof_timestamp_enabled = parse_bool(&args[0])?
            }
            "aof-load-corrupt-tail-max-size" if !args.is_empty() => {
                config.aof_load_corrupt_tail_max_size =
                    parse_u64(&args[0], "aof-load-corrupt-tail-max-size")?
            }

            "dbfilename" if !args.is_empty() => config.dbfilename = args[0].clone(),
            "dir" if !args.is_empty() => config.dir = args[0].clone(),
            "save" => {
                if args.is_empty() || (args.len() == 1 && args[0].is_empty()) {
                    config.save_points.clear();
                } else {
                    config.save_points.clear();
                    let mut i = 0;
                    while i < args.len() {
                        if i + 1 < args.len() {
                            let sec = parse_u64(&args[i], "save seconds")?;
                            let changes = parse_u64(&args[i + 1], "save changes")?;
                            config.save_points.push((sec, changes));
                            i += 2;
                        } else {
                            break;
                        }
                    }
                }
            }
            "stop-writes-on-bgsave-error" if !args.is_empty() => {
                config.stop_writes_on_bgsave_error = parse_bool(&args[0])?
            }
            "rdbcompression" if !args.is_empty() => config.rdbcompression = parse_bool(&args[0])?,
            "rdbchecksum" if !args.is_empty() => config.rdbchecksum = parse_bool(&args[0])?,
            "sanitize-dump-payload" if !args.is_empty() => {
                config.sanitize_dump_payload = args[0].clone()
            }
            "rdb-del-sync-files" if !args.is_empty() => {
                config.rdb_del_sync_files = parse_bool(&args[0])?
            }

            "replicaof" if args.len() >= 2 => {
                let port = parse_u16(&args[1], "replicaof port")?;
                config.replicaof = Some((args[0].clone(), port));
            }
            "masterauth" if !args.is_empty() => config.masterauth = Some(args[0].clone()),
            "masteruser" if !args.is_empty() => config.masteruser = Some(args[0].clone()),
            "replica-read-only" if !args.is_empty() => {
                config.replica_read_only = parse_bool(&args[0])?
            }
            "replica-serve-stale-data" if !args.is_empty() => {
                config.replica_serve_stale_data = parse_bool(&args[0])?
            }
            "repl-diskless-sync" if !args.is_empty() => {
                config.repl_diskless_sync = parse_bool(&args[0])?
            }
            "repl-diskless-sync-delay" if !args.is_empty() => {
                config.repl_diskless_sync_delay = parse_u64(&args[0], "repl-diskless-sync-delay")?
            }
            "repl-diskless-sync-max-replicas" if !args.is_empty() => {
                config.repl_diskless_sync_max_replicas =
                    parse_u32(&args[0], "repl-diskless-sync-max-replicas")?
            }
            "repl-diskless-load" if !args.is_empty() => config.repl_diskless_load = args[0].clone(),
            "repl-backlog-size" if !args.is_empty() => {
                config.repl_backlog_size = parse_memory(&args[0])?
            }
            "repl-backlog-ttl" if !args.is_empty() => {
                config.repl_backlog_ttl = parse_u64(&args[0], "repl-backlog-ttl")?
            }
            "repl-timeout" if !args.is_empty() => {
                config.repl_timeout = parse_u64(&args[0], "repl-timeout")?
            }
            "repl-disable-tcp-nodelay" if !args.is_empty() => {
                config.repl_disable_tcp_nodelay = parse_bool(&args[0])?
            }
            "replica-priority" if !args.is_empty() => {
                config.replica_priority = parse_u32(&args[0], "replica-priority")?
            }
            "propagation-error-behavior" if !args.is_empty() => {
                config.propagation_error_behavior = args[0].clone()
            }
            "replica-ignore-disk-write-errors" if !args.is_empty() => {
                config.replica_ignore_disk_write_errors = args[0].clone()
            }
            "replica-announced" if !args.is_empty() => {
                config.replica_announced = parse_bool(&args[0])?
            }
            "min-replicas-to-write" if !args.is_empty() => {
                config.min_replicas_to_write = parse_u32(&args[0], "min-replicas-to-write")?
            }
            "min-replicas-max-lag" if !args.is_empty() => {
                config.min_replicas_max_lag = parse_u64(&args[0], "min-replicas-max-lag")?
            }
            "repl-ping-replica-period" if !args.is_empty() => {
                config.repl_ping_replica_period = parse_u64(&args[0], "repl-ping-replica-period")?
            }
            "replica-full-sync-buffer-limit" if !args.is_empty() => {
                config.replica_full_sync_buffer_limit = parse_memory(&args[0])?
            }
            "replica-announce-ip" if !args.is_empty() => {
                config.replica_announce_ip = Some(args[0].clone())
            }
            "replica-announce-port" if !args.is_empty() => {
                config.replica_announce_port = Some(parse_u16(&args[0], "replica-announce-port")?)
            }

            "cluster-enabled" if !args.is_empty() => config.cluster_enabled = parse_bool(&args[0])?,
            "cluster-config-file" if !args.is_empty() => {
                config.cluster_config_file = args[0].clone()
            }
            "cluster-node-timeout" if !args.is_empty() => {
                config.cluster_node_timeout = parse_u64(&args[0], "cluster-node-timeout")?
            }
            "cluster-port" if !args.is_empty() => {
                config.cluster_port = parse_u16(&args[0], "cluster-port")?
            }
            "cluster-replica-validity-factor" if !args.is_empty() => {
                config.cluster_replica_validity_factor =
                    parse_u32(&args[0], "cluster-replica-validity-factor")?
            }
            "cluster-migration-barrier" if !args.is_empty() => {
                config.cluster_migration_barrier = parse_u32(&args[0], "cluster-migration-barrier")?
            }
            "cluster-allow-replica-migration" if !args.is_empty() => {
                config.cluster_allow_replica_migration = parse_bool(&args[0])?
            }
            "cluster-require-full-coverage" if !args.is_empty() => {
                config.cluster_require_full_coverage = parse_bool(&args[0])?
            }
            "cluster-replica-no-failover" if !args.is_empty() => {
                config.cluster_replica_no_failover = parse_bool(&args[0])?
            }
            "cluster-allow-reads-when-down" if !args.is_empty() => {
                config.cluster_allow_reads_when_down = parse_bool(&args[0])?
            }
            "cluster-allow-pubsubshard-when-down" if !args.is_empty() => {
                config.cluster_allow_pubsubshard_when_down = parse_bool(&args[0])?
            }
            "cluster-link-sendbuf-limit" if !args.is_empty() => {
                config.cluster_link_sendbuf_limit = parse_memory(&args[0])?
            }
            "cluster-announce-ip" if !args.is_empty() => {
                config.cluster_announce_ip = Some(args[0].clone())
            }
            "cluster-announce-port" if !args.is_empty() => {
                config.cluster_announce_port = Some(parse_u16(&args[0], "cluster-announce-port")?)
            }
            "cluster-announce-tls-port" if !args.is_empty() => {
                config.cluster_announce_tls_port =
                    Some(parse_u16(&args[0], "cluster-announce-tls-port")?)
            }
            "cluster-announce-bus-port" if !args.is_empty() => {
                config.cluster_announce_bus_port =
                    Some(parse_u16(&args[0], "cluster-announce-bus-port")?)
            }
            "cluster-announce-hostname" if !args.is_empty() => {
                config.cluster_announce_hostname = Some(args[0].clone())
            }
            "cluster-announce-human-nodename" if !args.is_empty() => {
                config.cluster_announce_human_nodename = Some(args[0].clone())
            }
            "cluster-preferred-endpoint-type" if !args.is_empty() => {
                config.cluster_preferred_endpoint_type = args[0].clone()
            }
            "cluster-compatibility-sample-ratio" if !args.is_empty() => {
                config.cluster_compatibility_sample_ratio =
                    parse_u32(&args[0], "cluster-compatibility-sample-ratio")?
            }
            "cluster-slot-stats-enabled" if !args.is_empty() => {
                config.cluster_slot_stats_enabled = parse_bool(&args[0])?
            }
            "cluster-slot-migration-write-pause-timeout" if !args.is_empty() => {
                config.cluster_slot_migration_write_pause_timeout =
                    parse_u64(&args[0], "cluster-slot-migration-write-pause-timeout")?
            }
            "cluster-slot-migration-handoff-max-lag-bytes" if !args.is_empty() => {
                config.cluster_slot_migration_handoff_max_lag_bytes = parse_memory(&args[0])?
            }

            "maxclients" if !args.is_empty() => {
                config.maxclients = parse_u32(&args[0], "maxclients")?
            }
            "slowlog-log-slower-than" if !args.is_empty() => {
                config.slowlog_log_slower_than = parse_i64(&args[0], "slowlog-log-slower-than")?
            }
            "slowlog-max-len" if !args.is_empty() => {
                config.slowlog_max_len = parse_u64(&args[0], "slowlog-max-len")?
            }
            "latency-monitor-threshold" if !args.is_empty() => {
                config.latency_monitor_threshold = parse_u64(&args[0], "latency-monitor-threshold")?
            }
            "notify-keyspace-events" if !args.is_empty() => {
                config.notify_keyspace_events = args[0].clone()
            }

            "hash-max-listpack-entries" if !args.is_empty() => {
                config.hash_max_listpack_entries =
                    parse_usize(&args[0], "hash-max-listpack-entries")?
            }
            "hash-max-listpack-value" if !args.is_empty() => {
                config.hash_max_listpack_value = parse_usize(&args[0], "hash-max-listpack-value")?
            }
            "list-max-listpack-size" if !args.is_empty() => {
                config.list_max_listpack_size = parse_i32(&args[0], "list-max-listpack-size")?
            }
            "list-compress-depth" if !args.is_empty() => {
                config.list_compress_depth = parse_u32(&args[0], "list-compress-depth")?
            }
            "set-max-intset-entries" if !args.is_empty() => {
                config.set_max_intset_entries = parse_usize(&args[0], "set-max-intset-entries")?
            }
            "set-max-listpack-entries" if !args.is_empty() => {
                config.set_max_listpack_entries = parse_usize(&args[0], "set-max-listpack-entries")?
            }
            "set-max-listpack-value" if !args.is_empty() => {
                config.set_max_listpack_value = parse_usize(&args[0], "set-max-listpack-value")?
            }
            "zset-max-listpack-entries" if !args.is_empty() => {
                config.zset_max_listpack_entries =
                    parse_usize(&args[0], "zset-max-listpack-entries")?
            }
            "zset-max-listpack-value" if !args.is_empty() => {
                config.zset_max_listpack_value = parse_usize(&args[0], "zset-max-listpack-value")?
            }
            "hll-sparse-max-bytes" if !args.is_empty() => {
                config.hll_sparse_max_bytes = parse_usize(&args[0], "hll-sparse-max-bytes")?
            }
            "stream-node-max-bytes" if !args.is_empty() => {
                config.stream_node_max_bytes = parse_memory(&args[0])? as usize
            }
            "stream-node-max-entries" if !args.is_empty() => {
                config.stream_node_max_entries = parse_usize(&args[0], "stream-node-max-entries")?
            }

            "activerehashing" if !args.is_empty() => config.activerehashing = parse_bool(&args[0])?,

            "client-output-buffer-limit" if args.len() >= 4 => {
                let class = &args[0];
                let limit_str = format!("{} {} {}", args[1], args[2], args[3]);
                match class.to_lowercase().as_str() {
                    "normal" => config.client_output_buffer_limit_normal = limit_str,
                    "replica" => config.client_output_buffer_limit_replica = limit_str,
                    "pubsub" => config.client_output_buffer_limit_pubsub = limit_str,
                    _ => {}
                }
            }
            "client-query-buffer-limit" if !args.is_empty() => {
                config.client_query_buffer_limit = parse_memory(&args[0])?
            }
            "proto-max-bulk-len" if !args.is_empty() => {
                config.proto_max_bulk_len = parse_memory(&args[0])?
            }
            "hz" if !args.is_empty() => config.hz = parse_u32(&args[0], "hz")?,
            "dynamic-hz" if !args.is_empty() => config.dynamic_hz = parse_bool(&args[0])?,
            "aof-rewrite-incremental-fsync" if !args.is_empty() => {
                config.aof_rewrite_incremental_fsync = parse_bool(&args[0])?
            }
            "rdb-save-incremental-fsync" if !args.is_empty() => {
                config.rdb_save_incremental_fsync = parse_bool(&args[0])?
            }
            "lfu-log-factor" if !args.is_empty() => {
                config.lfu_log_factor = parse_u32(&args[0], "lfu-log-factor")?
            }
            "lfu-decay-time" if !args.is_empty() => {
                config.lfu_decay_time = parse_u32(&args[0], "lfu-decay-time")?
            }
            "max-new-connections-per-cycle" if !args.is_empty() => {
                config.max_new_connections_per_cycle =
                    parse_u32(&args[0], "max-new-connections-per-cycle")?
            }
            "jemalloc-bg-thread" if !args.is_empty() => {
                config.jemalloc_bg_thread = parse_bool(&args[0])?
            }
            "ignore-warnings" if !args.is_empty() => config.ignore_warnings = Some(args[0].clone()),

            "lookahead" if !args.is_empty() => config.lookahead = parse_u32(&args[0], "lookahead")?,
            "maxmemory-clients" if !args.is_empty() => config.maxmemory_clients = args[0].clone(),
            "max-new-tls-connections-per-cycle" if !args.is_empty() => {
                config.max_new_tls_connections_per_cycle =
                    parse_u32(&args[0], "max-new-tls-connections-per-cycle")?
            }

            // Latency
            "latency-tracking" if !args.is_empty() => {
                config.latency_tracking = parse_bool(&args[0])?
            }
            "latency-tracking-info-percentiles" if !args.is_empty() => {
                config.latency_tracking_info_percentiles =
                    parse_f64_list(args, "latency-tracking-info-percentiles")?
            }

            // Threading/Kernel
            "io-threads" if !args.is_empty() => {
                config.io_threads = parse_usize(&args[0], "io-threads")?
            }
            "oom-score-adj" if !args.is_empty() => config.oom_score_adj = args[0].clone(),
            "oom-score-adj-values" if args.len() >= 3 => {
                config.oom_score_adj_values = parse_triple_i32(args, "oom-score-adj-values")?
            }
            "disable-thp" if !args.is_empty() => config.disable_thp = parse_bool(&args[0])?,
            "server-cpulist" if !args.is_empty() => config.server_cpulist = Some(args[0].clone()),
            "bio-cpulist" if !args.is_empty() => config.bio_cpulist = Some(args[0].clone()),
            "aof-rewrite-cpulist" if !args.is_empty() => {
                config.aof_rewrite_cpulist = Some(args[0].clone())
            }
            "bgsave-cpulist" if !args.is_empty() => config.bgsave_cpulist = Some(args[0].clone()),

            // Shutdown
            "shutdown-timeout" if !args.is_empty() => {
                config.shutdown_timeout = parse_u64(&args[0], "shutdown-timeout")?
            }
            "shutdown-on-sigint" if !args.is_empty() => config.shutdown_on_sigint = args[0].clone(),
            "shutdown-on-sigterm" if !args.is_empty() => {
                config.shutdown_on_sigterm = args[0].clone()
            }

            // Lua
            "lua-time-limit" | "busy-reply-threshold" if !args.is_empty() => {
                config.lua_time_limit = parse_u64(&args[0], "lua-time-limit")?
            }

            // Active Defrag
            "activedefrag" if !args.is_empty() => config.activedefrag = parse_bool(&args[0])?,
            "active-defrag-ignore-bytes" if !args.is_empty() => {
                config.active_defrag_ignore_bytes = parse_memory(&args[0])?
            }
            "active-defrag-threshold-lower" if !args.is_empty() => {
                config.active_defrag_threshold_lower =
                    parse_u32(&args[0], "active-defrag-threshold-lower")?
            }
            "active-defrag-threshold-upper" if !args.is_empty() => {
                config.active_defrag_threshold_upper =
                    parse_u32(&args[0], "active-defrag-threshold-upper")?
            }
            "active-defrag-cycle-min" if !args.is_empty() => {
                config.active_defrag_cycle_min = parse_u32(&args[0], "active-defrag-cycle-min")?
            }
            "active-defrag-cycle-max" if !args.is_empty() => {
                config.active_defrag_cycle_max = parse_u32(&args[0], "active-defrag-cycle-max")?
            }
            "active-defrag-max-scan-fields" if !args.is_empty() => {
                config.active_defrag_max_scan_fields =
                    parse_u32(&args[0], "active-defrag-max-scan-fields")?
            }

            "include" if !args.is_empty() => {
                let include_path = PathBuf::from(&args[0]);
                if include_path.exists() {
                    parse_config_file(&include_path, config)?;
                }
            }
            _ => {
                // Ignore unknown directives
            }
        }
    }
    Ok(())
}

fn split_args(line: &str) -> Result<Vec<String>, String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let mut quote_char = '\0';
    let mut escape = false;
    let mut token_started = false;

    for c in line.chars() {
        if escape {
            current.push(c);
            escape = false;
            token_started = true;
            continue;
        }

        if c == '\\' {
            escape = true;
            token_started = true;
            continue;
        }

        if in_quote {
            if c == quote_char {
                in_quote = false;
                token_started = true;
            } else {
                current.push(c);
            }
        } else if c == '"' || c == '\'' {
            in_quote = true;
            quote_char = c;
            token_started = true;
        } else if c.is_whitespace() {
            if token_started {
                args.push(current);
                current = String::new();
                token_started = false;
            }
        } else {
            current.push(c);
            token_started = true;
        }
    }

    if in_quote {
        return Err("Unclosed quote".to_string());
    }

    if token_started {
        args.push(current);
    }

    Ok(args)
}

fn parse_bool(s: &str) -> Result<bool, String> {
    match s.to_lowercase().as_str() {
        "yes" | "true" | "1" => Ok(true),
        "no" | "false" | "0" => Ok(false),
        _ => Err(format!("Invalid boolean: {}", s)),
    }
}

fn parse_u16(s: &str, field: &str) -> Result<u16, String> {
    s.parse::<u16>()
        .map_err(|_| format!("Invalid {}: {}", field, s))
}

fn parse_u32(s: &str, field: &str) -> Result<u32, String> {
    s.parse::<u32>()
        .map_err(|_| format!("Invalid {}: {}", field, s))
}

fn parse_u64(s: &str, field: &str) -> Result<u64, String> {
    s.parse::<u64>()
        .map_err(|_| format!("Invalid {}: {}", field, s))
}

fn parse_i64(s: &str, field: &str) -> Result<i64, String> {
    s.parse::<i64>()
        .map_err(|_| format!("Invalid {}: {}", field, s))
}

fn parse_usize(s: &str, field: &str) -> Result<usize, String> {
    s.parse::<usize>()
        .map_err(|_| format!("Invalid {}: {}", field, s))
}

fn parse_i32(s: &str, field: &str) -> Result<i32, String> {
    s.parse::<i32>()
        .map_err(|_| format!("Invalid {}: {}", field, s))
}

fn parse_octal_or_decimal(s: &str, field: &str) -> Result<u32, String> {
    if s.len() > 1 && s.starts_with('0') {
        u32::from_str_radix(s, 8).map_err(|_| format!("Invalid {} (octal): {}", field, s))
    } else {
        s.parse::<u32>()
            .map_err(|_| format!("Invalid {}: {}", field, s))
    }
}

fn parse_memory(s: &str) -> Result<u64, String> {
    let s = s.to_lowercase();
    if s.is_empty() {
        return Ok(0);
    }

    let (num_str, unit) = if s.ends_with("gb") {
        (s.trim_end_matches("gb"), 1024 * 1024 * 1024)
    } else if s.ends_with("mb") {
        (s.trim_end_matches("mb"), 1024 * 1024)
    } else if s.ends_with("kb") {
        (s.trim_end_matches("kb"), 1024)
    } else if s.ends_with('g') {
        (s.trim_end_matches('g'), 1024 * 1024 * 1024)
    } else if s.ends_with('m') {
        (s.trim_end_matches('m'), 1024 * 1024)
    } else if s.ends_with('k') {
        (s.trim_end_matches('k'), 1024)
    } else if s.ends_with('b') {
        (s.trim_end_matches('b'), 1)
    } else {
        (s.as_str(), 1)
    };

    let num = num_str
        .trim()
        .parse::<u64>()
        .map_err(|_| format!("Invalid memory value: {}", s))?;

    Ok(num * unit)
}

fn parse_f64_list(args: &[String], field: &str) -> Result<Vec<f64>, String> {
    let mut res = Vec::with_capacity(args.len());
    for s in args {
        res.push(
            s.parse::<f64>()
                .map_err(|_| format!("Invalid {}: {}", field, s))?,
        );
    }
    Ok(res)
}

fn parse_triple_i32(args: &[String], field: &str) -> Result<(i32, i32, i32), String> {
    if args.len() < 3 {
        return Err(format!("Invalid {}: expected 3 arguments", field));
    }
    let a = parse_i32(&args[0], field)?;
    let b = parse_i32(&args[1], field)?;
    let c = parse_i32(&args[2], field)?;
    Ok((a, b, c))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_args() {
        assert_eq!(split_args("port 6379").unwrap(), vec!["port", "6379"]);
        assert_eq!(split_args("save \"\"").unwrap(), vec!["save", ""]);
        assert_eq!(
            split_args("bind 127.0.0.1 -::1").unwrap(),
            vec!["bind", "127.0.0.1", "-::1"]
        );
        assert_eq!(
            split_args("dir \"/var/redis/\"").unwrap(),
            vec!["dir", "/var/redis/"]
        );
        assert_eq!(
            split_args("client-output-buffer-limit normal 0 0 0").unwrap(),
            vec!["client-output-buffer-limit", "normal", "0", "0", "0"]
        );
    }

    #[test]
    fn test_parse_full_config() {
        use std::io::Write;
        let dir = std::env::temp_dir();
        let config_path = dir.join("redis_test.conf");
        let mut file = std::fs::File::create(&config_path).unwrap();

        write!(
            file,
            "
            port 6380
            bind 127.0.0.1 192.168.1.1
            maxmemory 1gb
            maxmemory-policy allkeys-lru
            save 3600 1 300 100
            dir \"/tmp/redis\"
            requirepass \"secret password\"
            client-output-buffer-limit normal 0 0 0
            client-output-buffer-limit replica 256mb 64mb 60
            replicaof master.host 6379
        "
        )
        .unwrap();

        let mut config = ServerConfig::default();
        parse_config_file(&config_path, &mut config).unwrap();

        assert_eq!(config.port, 6380);
        assert_eq!(config.bind, vec!["127.0.0.1", "192.168.1.1"]);
        assert_eq!(config.maxmemory, 1024 * 1024 * 1024);
        assert_eq!(config.maxmemory_policy, MaxMemoryPolicy::AllKeysLru);
        assert_eq!(config.save_points, vec![(3600, 1), (300, 100)]);
        assert_eq!(config.dir, "/tmp/redis");
        assert_eq!(config.requirepass, Some("secret password".to_string()));
        assert_eq!(config.client_output_buffer_limit_replica, "256mb 64mb 60");
        assert_eq!(config.replicaof, Some(("master.host".to_string(), 6379)));

        let _ = std::fs::remove_file(config_path);
    }

    #[test]
    fn test_parse_new_configs() {
        use std::io::Write;
        let dir = std::env::temp_dir();
        let config_path = dir.join("redis_new_config_test.conf");
        let mut file = std::fs::File::create(&config_path).unwrap();

        write!(
            file,
            "
            bind-source-addr 10.0.0.1
            enable-protected-configs yes
            enable-debug-command no
            enable-module-command yes
            rename-command CONFIG b840fc02d524045429941cc15f59e41cb7be6c52
            syslog-enabled yes
            syslog-ident sockudo
            syslog-facility local1
            hide-user-data-from-log yes
            locale-collate en_US.UTF-8
            tracking-table-max-keys 500
            aof-timestamp-enabled yes
            aof-load-corrupt-tail-max-size 1024
            min-replicas-max-lag 5
            repl-ping-replica-period 15
            replica-full-sync-buffer-limit 32mb
            replica-announce-ip 1.2.3.4
            replica-announce-port 6380
            cluster-preferred-endpoint-type hostname
            cluster-compatibility-sample-ratio 50
            cluster-slot-stats-enabled yes
            cluster-slot-migration-write-pause-timeout 500
            cluster-slot-migration-handoff-max-lag-bytes 2mb
            lookahead 32
            maxmemory-clients 5%
            max-new-tls-connections-per-cycle 5
            latency-tracking yes
            latency-tracking-info-percentiles 50.0 99.0 99.99
            io-threads 4
            oom-score-adj relative
            oom-score-adj-values 100 200 800
            disable-thp yes
            shutdown-timeout 5
            shutdown-on-sigint save
            shutdown-on-sigterm default
            busy-reply-threshold 7000
            activedefrag yes
            active-defrag-ignore-bytes 50mb
            active-defrag-threshold-lower 5
            active-defrag-threshold-upper 90
            active-defrag-cycle-min 2
            active-defrag-cycle-max 30
            active-defrag-max-scan-fields 500
        "
        )
        .unwrap();

        let mut config = ServerConfig::default();
        parse_config_file(&config_path, &mut config).unwrap();

        assert_eq!(config.bind_source_addr, Some("10.0.0.1".to_string()));
        assert!(config.enable_protected_configs);
        assert!(!config.enable_debug_command);
        assert!(config.enable_module_command);
        assert_eq!(
            config.rename_command.get("CONFIG"),
            Some(&"b840fc02d524045429941cc15f59e41cb7be6c52".to_string())
        );
        assert!(config.syslog_enabled);
        assert_eq!(config.syslog_ident, "sockudo");
        assert_eq!(config.syslog_facility, "local1");
        assert!(config.hide_user_data_from_log);
        assert_eq!(config.locale_collate, "en_US.UTF-8");
        assert_eq!(config.tracking_table_max_keys, 500);
        assert!(config.aof_timestamp_enabled);
        assert_eq!(config.aof_load_corrupt_tail_max_size, 1024);
        assert_eq!(config.min_replicas_max_lag, 5);
        assert_eq!(config.repl_ping_replica_period, 15);
        assert_eq!(config.replica_full_sync_buffer_limit, 32 * 1024 * 1024);
        assert_eq!(config.replica_announce_ip, Some("1.2.3.4".to_string()));
        assert_eq!(config.replica_announce_port, Some(6380));
        assert_eq!(config.cluster_preferred_endpoint_type, "hostname");
        assert_eq!(config.cluster_compatibility_sample_ratio, 50);
        assert!(config.cluster_slot_stats_enabled);
        assert_eq!(config.cluster_slot_migration_write_pause_timeout, 500);
        assert_eq!(
            config.cluster_slot_migration_handoff_max_lag_bytes,
            2 * 1024 * 1024
        );
        assert_eq!(config.lookahead, 32);
        assert_eq!(config.maxmemory_clients, "5%");
        assert_eq!(config.max_new_tls_connections_per_cycle, 5);
        assert!(config.latency_tracking);
        assert_eq!(
            config.latency_tracking_info_percentiles,
            vec![50.0, 99.0, 99.99]
        );
        assert_eq!(config.io_threads, 4);
        assert_eq!(config.oom_score_adj, "relative");
        assert_eq!(config.oom_score_adj_values, (100, 200, 800));
        assert!(config.disable_thp);
        assert_eq!(config.shutdown_timeout, 5);
        assert_eq!(config.shutdown_on_sigint, "save");
        assert_eq!(config.shutdown_on_sigterm, "default");
        assert_eq!(config.lua_time_limit, 7000);
        assert!(config.activedefrag);
        assert_eq!(config.active_defrag_ignore_bytes, 50 * 1024 * 1024);
        assert_eq!(config.active_defrag_threshold_lower, 5);
        assert_eq!(config.active_defrag_threshold_upper, 90);
        assert_eq!(config.active_defrag_cycle_min, 2);
        assert_eq!(config.active_defrag_cycle_max, 30);
        assert_eq!(config.active_defrag_max_scan_fields, 500);

        let _ = std::fs::remove_file(config_path);
    }
}
