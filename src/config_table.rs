//! Redis-style configuration table system
//!
//! Provides a table-driven approach to configuration management,
//! similar to Redis's standardConfig in config.c

use crate::config::ServerConfig;
use crate::server_state::ServerState;
use std::sync::atomic::Ordering;

/// Config entry flags
#[derive(Clone, Copy, Default)]
pub struct ConfigFlags(u32);

impl ConfigFlags {
    pub const NONE: Self = Self(0);
    pub const IMMUTABLE: Self = Self(1 << 0);
    pub const SENSITIVE: Self = Self(1 << 1);
    pub const MEMORY: Self = Self(1 << 2);
    pub const DEPRECATED: Self = Self(1 << 3);

    #[inline]
    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    #[inline]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

/// Configuration data type
#[derive(Clone, Copy)]
pub enum ConfigType {
    Bool,
    String,
    Integer,
    Memory,
}

/// Getter function type
pub type ConfigGetter = fn(&ServerConfig) -> String;
/// Setter function type  
pub type ConfigSetter = fn(&mut ServerConfig, &str) -> Result<(), String>;
/// Applier function type (applies config to runtime state)
pub type ConfigApplier = fn(&ServerState, &ServerConfig);

/// A configuration entry in the table
pub struct ConfigEntry {
    pub name: &'static str,
    pub alias: Option<&'static str>,
    pub flags: ConfigFlags,
    pub config_type: ConfigType,
    pub default_value: &'static str,
    pub getter: ConfigGetter,
    pub setter: ConfigSetter,
    pub applier: Option<ConfigApplier>,
}

// Helper macros for concise table definition
macro_rules! bool_getter {
    ($field:ident) => {
        |c| if c.$field { "yes".into() } else { "no".into() }
    };
}

macro_rules! bool_setter {
    ($field:ident) => {
        |c, v| {
            c.$field = match v.to_lowercase().as_str() {
                "yes" | "true" | "1" => true,
                "no" | "false" | "0" => false,
                _ => return Err("Invalid boolean".into()),
            };
            Ok(())
        }
    };
}

macro_rules! string_getter {
    ($field:ident) => {
        |c| c.$field.clone()
    };
}

macro_rules! string_setter {
    ($field:ident) => {
        |c, v| {
            c.$field = v.to_string();
            Ok(())
        }
    };
}

macro_rules! int_getter {
    ($field:ident) => {
        |c| c.$field.to_string()
    };
}

macro_rules! int_setter {
    ($field:ident, $ty:ty) => {
        |c, v| {
            c.$field = v.parse::<$ty>().map_err(|_| "Invalid integer")?;
            Ok(())
        }
    };
}

macro_rules! immutable_setter {
    () => {
        |_, _| Err("This config is immutable at runtime".into())
    };
}

/// The static configuration table
pub static CONFIG_TABLE: &[ConfigEntry] = &[
    // === Network ===
    ConfigEntry {
        name: "port",
        alias: None,
        flags: ConfigFlags::IMMUTABLE,
        config_type: ConfigType::Integer,
        default_value: "6379",
        getter: int_getter!(port),
        setter: immutable_setter!(),
        applier: None,
    },
    ConfigEntry {
        name: "bind",
        alias: None,
        flags: ConfigFlags::IMMUTABLE,
        config_type: ConfigType::String,
        default_value: "127.0.0.1",
        getter: |c| c.bind.join(" "),
        setter: immutable_setter!(),
        applier: None,
    },
    ConfigEntry {
        name: "tcp-backlog",
        alias: None,
        flags: ConfigFlags::IMMUTABLE,
        config_type: ConfigType::Integer,
        default_value: "511",
        getter: int_getter!(tcp_backlog),
        setter: immutable_setter!(),
        applier: None,
    },
    ConfigEntry {
        name: "tcp-keepalive",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "300",
        getter: int_getter!(tcp_keepalive),
        setter: int_setter!(tcp_keepalive, u32),
        applier: None,
    },
    ConfigEntry {
        name: "timeout",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "0",
        getter: int_getter!(timeout),
        setter: int_setter!(timeout, u64),
        applier: None,
    },
    ConfigEntry {
        name: "protected-mode",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "yes",
        getter: bool_getter!(protected_mode),
        setter: bool_setter!(protected_mode),
        applier: None,
    },
    // === General ===
    ConfigEntry {
        name: "databases",
        alias: None,
        flags: ConfigFlags::IMMUTABLE,
        config_type: ConfigType::Integer,
        default_value: "16",
        getter: int_getter!(databases),
        setter: immutable_setter!(),
        applier: None,
    },
    ConfigEntry {
        name: "daemonize",
        alias: None,
        flags: ConfigFlags::IMMUTABLE,
        config_type: ConfigType::Bool,
        default_value: "no",
        getter: bool_getter!(daemonize),
        setter: immutable_setter!(),
        applier: None,
    },
    ConfigEntry {
        name: "loglevel",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::String,
        default_value: "notice",
        getter: string_getter!(loglevel),
        setter: string_setter!(loglevel),
        applier: None,
    },
    ConfigEntry {
        name: "logfile",
        alias: None,
        flags: ConfigFlags::IMMUTABLE,
        config_type: ConfigType::String,
        default_value: "",
        getter: string_getter!(logfile),
        setter: immutable_setter!(),
        applier: None,
    },
    // === Slow Log ===
    ConfigEntry {
        name: "slowlog-log-slower-than",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "10000",
        getter: |c| c.slowlog_log_slower_than.to_string(),
        setter: |c, v| {
            c.slowlog_log_slower_than = v.parse().map_err(|_| "Invalid integer")?;
            Ok(())
        },
        applier: Some(|s, c| {
            s.slowlog_threshold_us
                .store(c.slowlog_log_slower_than as u64, Ordering::Relaxed);
        }),
    },
    ConfigEntry {
        name: "slowlog-max-len",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "128",
        getter: int_getter!(slowlog_max_len),
        setter: int_setter!(slowlog_max_len, u64),
        applier: Some(|s, c| {
            s.slowlog_max_len
                .store(c.slowlog_max_len as usize, Ordering::Relaxed);
        }),
    },
    // === Latency ===
    ConfigEntry {
        name: "latency-monitor-threshold",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "0",
        getter: int_getter!(latency_monitor_threshold),
        setter: int_setter!(latency_monitor_threshold, u64),
        applier: Some(|s, c| {
            s.latency_threshold_ms
                .store(c.latency_monitor_threshold, Ordering::Relaxed);
        }),
    },
    ConfigEntry {
        name: "latency-tracking-info-percentiles",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::String, // It's a list but we treat input as string
        default_value: "50 99 99.9",
        getter: |c| {
            c.latency_tracking_info_percentiles
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<_>>()
                .join(" ")
        },
        setter: |c, v| {
            let percentiles: Result<Vec<f64>, _> =
                v.split_whitespace().map(|s| s.parse::<f64>()).collect();
            match percentiles {
                Ok(p) => {
                    c.latency_tracking_info_percentiles = p;
                    Ok(())
                }
                Err(_) => Err("Invalid float in percentiles".into()),
            }
        },
        applier: None,
    },
    // === ACL ===
    ConfigEntry {
        name: "acllog-max-len",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "128",
        getter: int_getter!(acllog_max_len),
        setter: int_setter!(acllog_max_len, usize),
        applier: Some(|s, c| {
            s.acl_log_max_len.store(c.acllog_max_len, Ordering::Relaxed);
        }),
    },
    // === Persistence ===
    ConfigEntry {
        name: "appendonly",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "no",
        getter: bool_getter!(appendonly),
        setter: bool_setter!(appendonly),
        applier: Some(|s, c| {
            s.aof_enabled.store(c.appendonly, Ordering::Relaxed);
        }),
    },
    ConfigEntry {
        name: "appendfilename",
        alias: None,
        flags: ConfigFlags::IMMUTABLE,
        config_type: ConfigType::String,
        default_value: "appendonly.aof",
        getter: string_getter!(appendfilename),
        setter: immutable_setter!(),
        applier: None,
    },
    ConfigEntry {
        name: "appendfsync",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::String,
        default_value: "everysec",
        getter: string_getter!(appendfsync),
        setter: string_setter!(appendfsync),
        applier: None,
    },
    ConfigEntry {
        name: "dir",
        alias: None,
        flags: ConfigFlags::IMMUTABLE,
        config_type: ConfigType::String,
        default_value: "./",
        getter: string_getter!(dir),
        setter: immutable_setter!(),
        applier: None,
    },
    ConfigEntry {
        name: "dbfilename",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::String,
        default_value: "dump.rdb",
        getter: string_getter!(dbfilename),
        setter: string_setter!(dbfilename),
        applier: None,
    },
    // === Memory ===
    ConfigEntry {
        name: "maxmemory",
        alias: None,
        flags: ConfigFlags::MEMORY,
        config_type: ConfigType::Memory,
        default_value: "0",
        getter: int_getter!(maxmemory),
        setter: int_setter!(maxmemory, u64),
        applier: Some(|s, c| {
            s.maxmemory.store(c.maxmemory, Ordering::Relaxed);
        }),
    },
    ConfigEntry {
        name: "maxmemory-policy",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::String,
        default_value: "noeviction",
        getter: |c| format!("{:?}", c.maxmemory_policy).to_lowercase(),
        setter: |c, v| {
            c.maxmemory_policy = v.parse().map_err(|e: String| e)?;
            Ok(())
        },
        applier: Some(|s, c| {
            *s.maxmemory_policy.write() = format!("{:?}", c.maxmemory_policy).to_lowercase();
        }),
    },
    ConfigEntry {
        name: "maxmemory-samples",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "5",
        getter: int_getter!(maxmemory_samples),
        setter: int_setter!(maxmemory_samples, usize),
        applier: None,
    },
    // === Limits ===
    ConfigEntry {
        name: "maxclients",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "10000",
        getter: int_getter!(maxclients),
        setter: int_setter!(maxclients, u32),
        applier: None, // Would need ClientManager reference
    },
    // === Lua ===
    ConfigEntry {
        name: "lua-time-limit",
        alias: Some("busy-reply-threshold"),
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "5000",
        getter: int_getter!(lua_time_limit),
        setter: int_setter!(lua_time_limit, u64),
        applier: None,
    },
    // === Data Structure Limits ===
    ConfigEntry {
        name: "hash-max-listpack-entries",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "512",
        getter: int_getter!(hash_max_listpack_entries),
        setter: int_setter!(hash_max_listpack_entries, usize),
        applier: None,
    },
    ConfigEntry {
        name: "hash-max-listpack-value",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "64",
        getter: int_getter!(hash_max_listpack_value),
        setter: int_setter!(hash_max_listpack_value, usize),
        applier: None,
    },
    ConfigEntry {
        name: "list-max-listpack-size",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "-2",
        getter: |c| c.list_max_listpack_size.to_string(),
        setter: |c, v| {
            c.list_max_listpack_size = v.parse().map_err(|_| "Invalid integer")?;
            Ok(())
        },
        applier: None,
    },
    ConfigEntry {
        name: "set-max-intset-entries",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "512",
        getter: int_getter!(set_max_intset_entries),
        setter: int_setter!(set_max_intset_entries, usize),
        applier: None,
    },
    ConfigEntry {
        name: "zset-max-listpack-entries",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "128",
        getter: int_getter!(zset_max_listpack_entries),
        setter: int_setter!(zset_max_listpack_entries, usize),
        applier: None,
    },
    ConfigEntry {
        name: "zset-max-listpack-value",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "64",
        getter: int_getter!(zset_max_listpack_value),
        setter: int_setter!(zset_max_listpack_value, usize),
        applier: None,
    },
    // === Threading ===
    ConfigEntry {
        name: "io-threads",
        alias: None,
        flags: ConfigFlags::IMMUTABLE,
        config_type: ConfigType::Integer,
        default_value: "1",
        getter: int_getter!(io_threads),
        setter: immutable_setter!(),
        applier: None,
    },
    ConfigEntry {
        name: "hz",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "10",
        getter: int_getter!(hz),
        setter: int_setter!(hz, u32),
        applier: None,
    },
    // === Lazy Free ===
    ConfigEntry {
        name: "lazyfree-lazy-eviction",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "no",
        getter: bool_getter!(lazyfree_lazy_eviction),
        setter: bool_setter!(lazyfree_lazy_eviction),
        applier: None,
    },
    ConfigEntry {
        name: "lazyfree-lazy-expire",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "no",
        getter: bool_getter!(lazyfree_lazy_expire),
        setter: bool_setter!(lazyfree_lazy_expire),
        applier: None,
    },
    ConfigEntry {
        name: "lazyfree-lazy-server-del",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "no",
        getter: bool_getter!(lazyfree_lazy_server_del),
        setter: bool_setter!(lazyfree_lazy_server_del),
        applier: None,
    },
    ConfigEntry {
        name: "lazyfree-lazy-user-del",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "no",
        getter: bool_getter!(lazyfree_lazy_user_del),
        setter: bool_setter!(lazyfree_lazy_user_del),
        applier: None,
    },
    // === Active Defrag ===
    ConfigEntry {
        name: "activedefrag",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "no",
        getter: bool_getter!(activedefrag),
        setter: bool_setter!(activedefrag),
        applier: None,
    },
    // === LFU ===
    ConfigEntry {
        name: "lfu-log-factor",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "10",
        getter: int_getter!(lfu_log_factor),
        setter: int_setter!(lfu_log_factor, u32),
        applier: None,
    },
    ConfigEntry {
        name: "lfu-decay-time",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "1",
        getter: int_getter!(lfu_decay_time),
        setter: int_setter!(lfu_decay_time, u32),
        applier: None,
    },
    // === Replication ===
    ConfigEntry {
        name: "replica-read-only",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "yes",
        getter: bool_getter!(replica_read_only),
        setter: bool_setter!(replica_read_only),
        applier: None,
    },
    ConfigEntry {
        name: "repl-backlog-size",
        alias: None,
        flags: ConfigFlags::MEMORY,
        config_type: ConfigType::Memory,
        default_value: "1048576",
        getter: int_getter!(repl_backlog_size),
        setter: int_setter!(repl_backlog_size, u64),
        applier: None,
    },
    ConfigEntry {
        name: "repl-timeout",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "60",
        getter: int_getter!(repl_timeout),
        setter: int_setter!(repl_timeout, u64),
        applier: None,
    },
    // === Cluster ===
    ConfigEntry {
        name: "cluster-enabled",
        alias: None,
        flags: ConfigFlags::IMMUTABLE,
        config_type: ConfigType::Bool,
        default_value: "no",
        getter: bool_getter!(cluster_enabled),
        setter: immutable_setter!(),
        applier: None,
    },
    ConfigEntry {
        name: "cluster-node-timeout",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "15000",
        getter: int_getter!(cluster_node_timeout),
        setter: int_setter!(cluster_node_timeout, u64),
        applier: None,
    },
    // === TLS ===
    ConfigEntry {
        name: "tls-port",
        alias: None,
        flags: ConfigFlags::IMMUTABLE,
        config_type: ConfigType::Integer,
        default_value: "0",
        getter: int_getter!(tls_port),
        setter: immutable_setter!(),
        applier: None,
    },
    ConfigEntry {
        name: "tls-replication",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "no",
        getter: bool_getter!(tls_replication),
        setter: bool_setter!(tls_replication),
        applier: None,
    },
    ConfigEntry {
        name: "tls-cluster",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "no",
        getter: bool_getter!(tls_cluster),
        setter: bool_setter!(tls_cluster),
        applier: None,
    },
    ConfigEntry {
        name: "tls-auth-clients",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::String,
        default_value: "yes",
        getter: string_getter!(tls_auth_clients),
        setter: string_setter!(tls_auth_clients),
        applier: None,
    },
    ConfigEntry {
        name: "tls-prefer-server-ciphers",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "no",
        getter: bool_getter!(tls_prefer_server_ciphers),
        setter: bool_setter!(tls_prefer_server_ciphers),
        applier: None,
    },
    ConfigEntry {
        name: "tls-session-caching",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "yes",
        getter: bool_getter!(tls_session_caching),
        setter: bool_setter!(tls_session_caching),
        applier: None,
    },
    ConfigEntry {
        name: "tls-session-cache-size",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "20480",
        getter: int_getter!(tls_session_cache_size),
        setter: int_setter!(tls_session_cache_size, usize),
        applier: None,
    },
    ConfigEntry {
        name: "tls-session-cache-timeout",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "300",
        getter: int_getter!(tls_session_cache_timeout),
        setter: int_setter!(tls_session_cache_timeout, u64),
        applier: None,
    },
    // === General ===
    ConfigEntry {
        name: "pidfile",
        alias: None,
        flags: ConfigFlags::IMMUTABLE,
        config_type: ConfigType::String,
        default_value: "/var/run/redis_6379.pid",
        getter: string_getter!(pidfile),
        setter: immutable_setter!(),
        applier: None,
    },
    ConfigEntry {
        name: "supervised",
        alias: None,
        flags: ConfigFlags::IMMUTABLE,
        config_type: ConfigType::String,
        default_value: "no",
        getter: string_getter!(supervised),
        setter: immutable_setter!(),
        applier: None,
    },
    ConfigEntry {
        name: "always-show-logo",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "no",
        getter: bool_getter!(always_show_logo),
        setter: bool_setter!(always_show_logo),
        applier: None,
    },
    ConfigEntry {
        name: "set-proc-title",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "yes",
        getter: bool_getter!(set_proc_title),
        setter: bool_setter!(set_proc_title),
        applier: None,
    },
    ConfigEntry {
        name: "proc-title-template",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::String,
        default_value: "{title} {listen-addr} {server-mode}",
        getter: string_getter!(proc_title_template),
        setter: string_setter!(proc_title_template),
        applier: None,
    },
    ConfigEntry {
        name: "crash-log-enabled",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "yes",
        getter: bool_getter!(crash_log_enabled),
        setter: bool_setter!(crash_log_enabled),
        applier: None,
    },
    ConfigEntry {
        name: "crash-memcheck-enabled",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "yes",
        getter: bool_getter!(crash_memcheck_enabled),
        setter: bool_setter!(crash_memcheck_enabled),
        applier: None,
    },
    ConfigEntry {
        name: "enable-protected-configs",
        alias: None,
        flags: ConfigFlags::IMMUTABLE,
        config_type: ConfigType::Bool,
        default_value: "no",
        getter: bool_getter!(enable_protected_configs),
        setter: immutable_setter!(),
        applier: None,
    },
    ConfigEntry {
        name: "enable-debug-command",
        alias: None,
        flags: ConfigFlags::IMMUTABLE,
        config_type: ConfigType::Bool,
        default_value: "no",
        getter: bool_getter!(enable_debug_command),
        setter: immutable_setter!(),
        applier: None,
    },
    ConfigEntry {
        name: "enable-module-command",
        alias: None,
        flags: ConfigFlags::IMMUTABLE,
        config_type: ConfigType::Bool,
        default_value: "no",
        getter: bool_getter!(enable_module_command),
        setter: immutable_setter!(),
        applier: None,
    },
    ConfigEntry {
        name: "syslog-enabled",
        alias: None,
        flags: ConfigFlags::IMMUTABLE,
        config_type: ConfigType::Bool,
        default_value: "no",
        getter: bool_getter!(syslog_enabled),
        setter: immutable_setter!(),
        applier: None,
    },
    ConfigEntry {
        name: "syslog-ident",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::String,
        default_value: "redis",
        getter: string_getter!(syslog_ident),
        setter: string_setter!(syslog_ident),
        applier: None,
    },
    ConfigEntry {
        name: "syslog-facility",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::String,
        default_value: "local0",
        getter: string_getter!(syslog_facility),
        setter: string_setter!(syslog_facility),
        applier: None,
    },
    ConfigEntry {
        name: "hide-user-data-from-log",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "no",
        getter: bool_getter!(hide_user_data_from_log),
        setter: bool_setter!(hide_user_data_from_log),
        applier: None,
    },
    ConfigEntry {
        name: "locale-collate",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::String,
        default_value: "",
        getter: string_getter!(locale_collate),
        setter: string_setter!(locale_collate),
        applier: None,
    },
    ConfigEntry {
        name: "notify-keyspace-events",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::String,
        default_value: "",
        getter: string_getter!(notify_keyspace_events),
        setter: string_setter!(notify_keyspace_events),
        applier: None,
    },
    ConfigEntry {
        name: "dynamic-hz",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "yes",
        getter: bool_getter!(dynamic_hz),
        setter: bool_setter!(dynamic_hz),
        applier: None,
    },
    ConfigEntry {
        name: "activerehashing",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "yes",
        getter: bool_getter!(activerehashing),
        setter: bool_setter!(activerehashing),
        applier: None,
    },
    // === Security ===
    ConfigEntry {
        name: "acl-pubsub-default",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::String,
        default_value: "resetchannels",
        getter: string_getter!(acl_pubsub_default),
        setter: string_setter!(acl_pubsub_default),
        applier: None,
    },
    ConfigEntry {
        name: "tracking-table-max-keys",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "1000000",
        getter: int_getter!(tracking_table_max_keys),
        setter: int_setter!(tracking_table_max_keys, u64),
        applier: Some(|s, c| {
            s.tracking_table_max_keys
                .store(c.tracking_table_max_keys as usize, Ordering::Relaxed);
        }),
    },
    // === Memory ===
    ConfigEntry {
        name: "maxmemory-eviction-tenacity",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "10",
        getter: int_getter!(maxmemory_eviction_tenacity),
        setter: int_setter!(maxmemory_eviction_tenacity, u32),
        applier: None,
    },
    ConfigEntry {
        name: "replica-ignore-maxmemory",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "yes",
        getter: bool_getter!(replica_ignore_maxmemory),
        setter: bool_setter!(replica_ignore_maxmemory),
        applier: None,
    },
    ConfigEntry {
        name: "active-expire-effort",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "1",
        getter: int_getter!(active_expire_effort),
        setter: int_setter!(active_expire_effort, u32),
        applier: None,
    },
    ConfigEntry {
        name: "maxmemory-clients",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::String,
        default_value: "0",
        getter: string_getter!(maxmemory_clients),
        setter: string_setter!(maxmemory_clients),
        applier: None,
    },
    // === Lazy Free (additional) ===
    ConfigEntry {
        name: "replica-lazy-flush",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "no",
        getter: bool_getter!(replica_lazy_flush),
        setter: bool_setter!(replica_lazy_flush),
        applier: None,
    },
    ConfigEntry {
        name: "lazyfree-lazy-user-flush",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "no",
        getter: bool_getter!(lazyfree_lazy_user_flush),
        setter: bool_setter!(lazyfree_lazy_user_flush),
        applier: None,
    },
    // === Persistence (additional) ===
    ConfigEntry {
        name: "appenddirname",
        alias: None,
        flags: ConfigFlags::IMMUTABLE,
        config_type: ConfigType::String,
        default_value: "appendonlydir",
        getter: string_getter!(appenddirname),
        setter: immutable_setter!(),
        applier: None,
    },
    ConfigEntry {
        name: "no-appendfsync-on-rewrite",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "no",
        getter: bool_getter!(no_appendfsync_on_rewrite),
        setter: bool_setter!(no_appendfsync_on_rewrite),
        applier: None,
    },
    ConfigEntry {
        name: "auto-aof-rewrite-percentage",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "100",
        getter: int_getter!(auto_aof_rewrite_percentage),
        setter: int_setter!(auto_aof_rewrite_percentage, u32),
        applier: None,
    },
    ConfigEntry {
        name: "auto-aof-rewrite-min-size",
        alias: None,
        flags: ConfigFlags::MEMORY,
        config_type: ConfigType::Memory,
        default_value: "67108864",
        getter: int_getter!(auto_aof_rewrite_min_size),
        setter: int_setter!(auto_aof_rewrite_min_size, u64),
        applier: None,
    },
    ConfigEntry {
        name: "aof-load-truncated",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "yes",
        getter: bool_getter!(aof_load_truncated),
        setter: bool_setter!(aof_load_truncated),
        applier: None,
    },
    ConfigEntry {
        name: "aof-use-rdb-preamble",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "yes",
        getter: bool_getter!(aof_use_rdb_preamble),
        setter: bool_setter!(aof_use_rdb_preamble),
        applier: None,
    },
    ConfigEntry {
        name: "aof-timestamp-enabled",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "no",
        getter: bool_getter!(aof_timestamp_enabled),
        setter: bool_setter!(aof_timestamp_enabled),
        applier: None,
    },
    ConfigEntry {
        name: "stop-writes-on-bgsave-error",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "yes",
        getter: bool_getter!(stop_writes_on_bgsave_error),
        setter: bool_setter!(stop_writes_on_bgsave_error),
        applier: None,
    },
    ConfigEntry {
        name: "rdbcompression",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "yes",
        getter: bool_getter!(rdbcompression),
        setter: bool_setter!(rdbcompression),
        applier: None,
    },
    ConfigEntry {
        name: "rdbchecksum",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "yes",
        getter: bool_getter!(rdbchecksum),
        setter: bool_setter!(rdbchecksum),
        applier: None,
    },
    ConfigEntry {
        name: "sanitize-dump-payload",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::String,
        default_value: "no",
        getter: string_getter!(sanitize_dump_payload),
        setter: string_setter!(sanitize_dump_payload),
        applier: None,
    },
    ConfigEntry {
        name: "rdb-del-sync-files",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "no",
        getter: bool_getter!(rdb_del_sync_files),
        setter: bool_setter!(rdb_del_sync_files),
        applier: None,
    },
    ConfigEntry {
        name: "aof-rewrite-incremental-fsync",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "yes",
        getter: bool_getter!(aof_rewrite_incremental_fsync),
        setter: bool_setter!(aof_rewrite_incremental_fsync),
        applier: None,
    },
    ConfigEntry {
        name: "rdb-save-incremental-fsync",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "yes",
        getter: bool_getter!(rdb_save_incremental_fsync),
        setter: bool_setter!(rdb_save_incremental_fsync),
        applier: None,
    },
    // === Replication ===
    ConfigEntry {
        name: "replica-serve-stale-data",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "yes",
        getter: bool_getter!(replica_serve_stale_data),
        setter: bool_setter!(replica_serve_stale_data),
        applier: None,
    },
    ConfigEntry {
        name: "repl-diskless-sync",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "no",
        getter: bool_getter!(repl_diskless_sync),
        setter: bool_setter!(repl_diskless_sync),
        applier: None,
    },
    ConfigEntry {
        name: "repl-diskless-sync-delay",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "5",
        getter: int_getter!(repl_diskless_sync_delay),
        setter: int_setter!(repl_diskless_sync_delay, u64),
        applier: None,
    },
    ConfigEntry {
        name: "repl-diskless-sync-max-replicas",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "0",
        getter: int_getter!(repl_diskless_sync_max_replicas),
        setter: int_setter!(repl_diskless_sync_max_replicas, u32),
        applier: None,
    },
    ConfigEntry {
        name: "repl-diskless-load",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::String,
        default_value: "disabled",
        getter: string_getter!(repl_diskless_load),
        setter: string_setter!(repl_diskless_load),
        applier: None,
    },
    ConfigEntry {
        name: "repl-backlog-ttl",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "3600",
        getter: int_getter!(repl_backlog_ttl),
        setter: int_setter!(repl_backlog_ttl, u64),
        applier: None,
    },
    ConfigEntry {
        name: "repl-disable-tcp-nodelay",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "no",
        getter: bool_getter!(repl_disable_tcp_nodelay),
        setter: bool_setter!(repl_disable_tcp_nodelay),
        applier: None,
    },
    ConfigEntry {
        name: "replica-priority",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "100",
        getter: int_getter!(replica_priority),
        setter: int_setter!(replica_priority, u32),
        applier: None,
    },
    ConfigEntry {
        name: "replica-announced",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "yes",
        getter: bool_getter!(replica_announced),
        setter: bool_setter!(replica_announced),
        applier: None,
    },
    ConfigEntry {
        name: "min-replicas-to-write",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "0",
        getter: int_getter!(min_replicas_to_write),
        setter: int_setter!(min_replicas_to_write, u32),
        applier: None,
    },
    ConfigEntry {
        name: "min-replicas-max-lag",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "10",
        getter: int_getter!(min_replicas_max_lag),
        setter: int_setter!(min_replicas_max_lag, u64),
        applier: None,
    },
    ConfigEntry {
        name: "repl-ping-replica-period",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "10",
        getter: int_getter!(repl_ping_replica_period),
        setter: int_setter!(repl_ping_replica_period, u64),
        applier: None,
    },
    ConfigEntry {
        name: "propagation-error-behavior",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::String,
        default_value: "ignore",
        getter: string_getter!(propagation_error_behavior),
        setter: string_setter!(propagation_error_behavior),
        applier: None,
    },
    // === Cluster (additional) ===
    ConfigEntry {
        name: "cluster-config-file",
        alias: None,
        flags: ConfigFlags::IMMUTABLE,
        config_type: ConfigType::String,
        default_value: "nodes.conf",
        getter: string_getter!(cluster_config_file),
        setter: immutable_setter!(),
        applier: None,
    },
    ConfigEntry {
        name: "cluster-port",
        alias: None,
        flags: ConfigFlags::IMMUTABLE,
        config_type: ConfigType::Integer,
        default_value: "0",
        getter: int_getter!(cluster_port),
        setter: immutable_setter!(),
        applier: None,
    },
    ConfigEntry {
        name: "cluster-replica-validity-factor",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "10",
        getter: int_getter!(cluster_replica_validity_factor),
        setter: int_setter!(cluster_replica_validity_factor, u32),
        applier: None,
    },
    ConfigEntry {
        name: "cluster-migration-barrier",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "1",
        getter: int_getter!(cluster_migration_barrier),
        setter: int_setter!(cluster_migration_barrier, u32),
        applier: None,
    },
    ConfigEntry {
        name: "cluster-allow-replica-migration",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "yes",
        getter: bool_getter!(cluster_allow_replica_migration),
        setter: bool_setter!(cluster_allow_replica_migration),
        applier: None,
    },
    ConfigEntry {
        name: "cluster-require-full-coverage",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "yes",
        getter: bool_getter!(cluster_require_full_coverage),
        setter: bool_setter!(cluster_require_full_coverage),
        applier: None,
    },
    ConfigEntry {
        name: "cluster-replica-no-failover",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "no",
        getter: bool_getter!(cluster_replica_no_failover),
        setter: bool_setter!(cluster_replica_no_failover),
        applier: None,
    },
    ConfigEntry {
        name: "cluster-allow-reads-when-down",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "no",
        getter: bool_getter!(cluster_allow_reads_when_down),
        setter: bool_setter!(cluster_allow_reads_when_down),
        applier: None,
    },
    ConfigEntry {
        name: "cluster-allow-pubsubshard-when-down",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "yes",
        getter: bool_getter!(cluster_allow_pubsubshard_when_down),
        setter: bool_setter!(cluster_allow_pubsubshard_when_down),
        applier: None,
    },
    ConfigEntry {
        name: "cluster-link-sendbuf-limit",
        alias: None,
        flags: ConfigFlags::MEMORY,
        config_type: ConfigType::Memory,
        default_value: "0",
        getter: int_getter!(cluster_link_sendbuf_limit),
        setter: int_setter!(cluster_link_sendbuf_limit, u64),
        applier: None,
    },
    ConfigEntry {
        name: "cluster-preferred-endpoint-type",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::String,
        default_value: "ip",
        getter: string_getter!(cluster_preferred_endpoint_type),
        setter: string_setter!(cluster_preferred_endpoint_type),
        applier: None,
    },
    // === Data Structures (additional) ===
    ConfigEntry {
        name: "list-compress-depth",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "0",
        getter: int_getter!(list_compress_depth),
        setter: int_setter!(list_compress_depth, u32),
        applier: None,
    },
    ConfigEntry {
        name: "set-max-listpack-entries",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "128",
        getter: int_getter!(set_max_listpack_entries),
        setter: int_setter!(set_max_listpack_entries, usize),
        applier: None,
    },
    ConfigEntry {
        name: "set-max-listpack-value",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "64",
        getter: int_getter!(set_max_listpack_value),
        setter: int_setter!(set_max_listpack_value, usize),
        applier: None,
    },
    ConfigEntry {
        name: "hll-sparse-max-bytes",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "3000",
        getter: int_getter!(hll_sparse_max_bytes),
        setter: int_setter!(hll_sparse_max_bytes, usize),
        applier: None,
    },
    ConfigEntry {
        name: "stream-node-max-bytes",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "4096",
        getter: int_getter!(stream_node_max_bytes),
        setter: int_setter!(stream_node_max_bytes, usize),
        applier: None,
    },
    ConfigEntry {
        name: "stream-node-max-entries",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "100",
        getter: int_getter!(stream_node_max_entries),
        setter: int_setter!(stream_node_max_entries, usize),
        applier: None,
    },
    // === Client Limits ===
    ConfigEntry {
        name: "client-output-buffer-limit-normal",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::String,
        default_value: "0 0 0",
        getter: string_getter!(client_output_buffer_limit_normal),
        setter: string_setter!(client_output_buffer_limit_normal),
        applier: None,
    },
    ConfigEntry {
        name: "client-output-buffer-limit-replica",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::String,
        default_value: "256mb 64mb 60",
        getter: string_getter!(client_output_buffer_limit_replica),
        setter: string_setter!(client_output_buffer_limit_replica),
        applier: None,
    },
    ConfigEntry {
        name: "client-output-buffer-limit-pubsub",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::String,
        default_value: "32mb 8mb 60",
        getter: string_getter!(client_output_buffer_limit_pubsub),
        setter: string_setter!(client_output_buffer_limit_pubsub),
        applier: None,
    },
    ConfigEntry {
        name: "client-query-buffer-limit",
        alias: None,
        flags: ConfigFlags::MEMORY,
        config_type: ConfigType::Memory,
        default_value: "1073741824",
        getter: int_getter!(client_query_buffer_limit),
        setter: int_setter!(client_query_buffer_limit, u64),
        applier: None,
    },
    ConfigEntry {
        name: "proto-max-bulk-len",
        alias: None,
        flags: ConfigFlags::MEMORY,
        config_type: ConfigType::Memory,
        default_value: "536870912",
        getter: int_getter!(proto_max_bulk_len),
        setter: int_setter!(proto_max_bulk_len, u64),
        applier: None,
    },
    // === Kernel/Threading ===
    ConfigEntry {
        name: "oom-score-adj",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::String,
        default_value: "no",
        getter: string_getter!(oom_score_adj),
        setter: string_setter!(oom_score_adj),
        applier: None,
    },
    ConfigEntry {
        name: "disable-thp",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "yes",
        getter: bool_getter!(disable_thp),
        setter: bool_setter!(disable_thp),
        applier: None,
    },
    ConfigEntry {
        name: "jemalloc-bg-thread",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "yes",
        getter: bool_getter!(jemalloc_bg_thread),
        setter: bool_setter!(jemalloc_bg_thread),
        applier: None,
    },
    // === Shutdown ===
    ConfigEntry {
        name: "shutdown-timeout",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "25",
        getter: int_getter!(shutdown_timeout),
        setter: int_setter!(shutdown_timeout, u64),
        applier: None,
    },
    ConfigEntry {
        name: "shutdown-on-sigint",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::String,
        default_value: "default",
        getter: string_getter!(shutdown_on_sigint),
        setter: string_setter!(shutdown_on_sigint),
        applier: None,
    },
    ConfigEntry {
        name: "shutdown-on-sigterm",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::String,
        default_value: "default",
        getter: string_getter!(shutdown_on_sigterm),
        setter: string_setter!(shutdown_on_sigterm),
        applier: None,
    },
    // === Latency Tracking ===
    ConfigEntry {
        name: "latency-tracking",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Bool,
        default_value: "yes",
        getter: bool_getter!(latency_tracking),
        setter: bool_setter!(latency_tracking),
        applier: None,
    },
    // === Active Defrag (additional) ===
    ConfigEntry {
        name: "active-defrag-ignore-bytes",
        alias: None,
        flags: ConfigFlags::MEMORY,
        config_type: ConfigType::Memory,
        default_value: "104857600",
        getter: int_getter!(active_defrag_ignore_bytes),
        setter: int_setter!(active_defrag_ignore_bytes, u64),
        applier: None,
    },
    ConfigEntry {
        name: "active-defrag-threshold-lower",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "10",
        getter: int_getter!(active_defrag_threshold_lower),
        setter: int_setter!(active_defrag_threshold_lower, u32),
        applier: None,
    },
    ConfigEntry {
        name: "active-defrag-threshold-upper",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "100",
        getter: int_getter!(active_defrag_threshold_upper),
        setter: int_setter!(active_defrag_threshold_upper, u32),
        applier: None,
    },
    ConfigEntry {
        name: "active-defrag-cycle-min",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "1",
        getter: int_getter!(active_defrag_cycle_min),
        setter: int_setter!(active_defrag_cycle_min, u32),
        applier: None,
    },
    ConfigEntry {
        name: "active-defrag-cycle-max",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "25",
        getter: int_getter!(active_defrag_cycle_max),
        setter: int_setter!(active_defrag_cycle_max, u32),
        applier: None,
    },
    ConfigEntry {
        name: "active-defrag-max-scan-fields",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "1000",
        getter: int_getter!(active_defrag_max_scan_fields),
        setter: int_setter!(active_defrag_max_scan_fields, u32),
        applier: None,
    },
    // === Misc ===
    ConfigEntry {
        name: "lookahead",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "0",
        getter: int_getter!(lookahead),
        setter: int_setter!(lookahead, u32),
        applier: None,
    },
    ConfigEntry {
        name: "max-new-connections-per-cycle",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "10",
        getter: int_getter!(max_new_connections_per_cycle),
        setter: int_setter!(max_new_connections_per_cycle, u32),
        applier: None,
    },
    ConfigEntry {
        name: "max-new-tls-connections-per-cycle",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "1",
        getter: int_getter!(max_new_tls_connections_per_cycle),
        setter: int_setter!(max_new_tls_connections_per_cycle, u32),
        applier: None,
    },
    // === Socket ===
    ConfigEntry {
        name: "socket-mark-id",
        alias: None,
        flags: ConfigFlags::NONE,
        config_type: ConfigType::Integer,
        default_value: "0",
        getter: int_getter!(socket_mark_id),
        setter: int_setter!(socket_mark_id, u32),
        applier: None,
    },
];

/// Find a config entry by name or alias
pub fn find_config(name: &str) -> Option<&'static ConfigEntry> {
    let name_lower = name.to_lowercase();
    CONFIG_TABLE
        .iter()
        .find(|e| e.name == name_lower || e.alias.map(|a| a == name_lower).unwrap_or(false))
}

/// Check if a pattern matches a config name (supports * glob)
pub fn matches_pattern(pattern: &str, name: &str) -> bool {
    crate::pattern::matches_pattern(pattern, name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_config() {
        assert!(find_config("port").is_some());
        assert!(find_config("PORT").is_some());
        assert!(find_config("nonexistent").is_none());
    }

    #[test]
    fn test_matches_pattern() {
        assert!(matches_pattern("*", "anything"));
        assert!(matches_pattern("slow*", "slowlog-max-len"));
        assert!(matches_pattern("*timeout", "repl-timeout"));
        assert!(matches_pattern("port", "port"));
        assert!(!matches_pattern("port", "report"));
    }

    #[test]
    fn test_config_flags() {
        assert!(ConfigFlags::IMMUTABLE.contains(ConfigFlags::IMMUTABLE));
        assert!(!ConfigFlags::IMMUTABLE.contains(ConfigFlags::SENSITIVE));
        let combined = ConfigFlags::IMMUTABLE.union(ConfigFlags::SENSITIVE);
        assert!(combined.contains(ConfigFlags::IMMUTABLE));
        assert!(combined.contains(ConfigFlags::SENSITIVE));
    }
}
