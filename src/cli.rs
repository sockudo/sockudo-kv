use crate::config::{MaxMemoryPolicy, ServerConfig};
use clap::Parser;

use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[derive(Parser, Debug)]
#[command(name = "sockudo-kv")]
#[command(author = "Sockudo Team")]
#[command(version = "0.2.0")]
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
        } else {
            if c == '"' || c == '\'' {
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
}
