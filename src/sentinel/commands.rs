//! Sentinel Commands
//!
//! Implementation of all SENTINEL subcommands.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::config::{MasterConfig, SharedSentinelConfig};
use super::election::LeaderElection;
use super::failover::SentinelFailover;
use super::state::{MasterInstance, SentinelState};
use crate::resp::RespValue;
use bytes::Bytes;

/// Execute a SENTINEL subcommand
pub fn execute(
    args: &[Bytes],
    state: &Arc<SentinelState>,
    config: &Arc<SharedSentinelConfig>,
    failover: Option<&Arc<SentinelFailover>>,
    election: Option<&Arc<LeaderElection>>,
) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR wrong number of arguments for 'sentinel' command".into());
    }

    let subcommand = String::from_utf8_lossy(&args[0]).to_uppercase();
    let sub_args = &args[1..];

    match subcommand.as_str() {
        "MYID" => sentinel_myid(state),
        "MASTERS" => sentinel_masters(state),
        "MASTER" => sentinel_master(sub_args, state),
        "REPLICAS" | "SLAVES" => sentinel_replicas(sub_args, state),
        "SENTINELS" => sentinel_sentinels(sub_args, state),
        "GET-MASTER-ADDR-BY-NAME" => sentinel_get_master_addr(sub_args, state),
        "MONITOR" => sentinel_monitor(sub_args, state, config),
        "REMOVE" => sentinel_remove(sub_args, state, config),
        "SET" => sentinel_set(sub_args, state),
        "RESET" => sentinel_reset(sub_args, state),
        "CKQUORUM" => sentinel_ckquorum(sub_args, state),
        "FLUSHCONFIG" => sentinel_flushconfig(config),
        "FAILOVER" => sentinel_failover_cmd(sub_args, state, failover),
        "CONFIG" => sentinel_config(sub_args, config),
        "IS-MASTER-DOWN-BY-ADDR" => sentinel_is_master_down(sub_args, state, election),
        "INFO-CACHE" => sentinel_info_cache(sub_args, state),
        "PENDING-SCRIPTS" => sentinel_pending_scripts(state),
        "SIMULATE-FAILURE" => sentinel_simulate_failure(sub_args, state),
        "DEBUG" => sentinel_debug(sub_args),
        "HELP" => sentinel_help(),
        _ => RespValue::Error(format!("ERR Unknown SENTINEL subcommand '{}'", subcommand).into()),
    }
}

/// SENTINEL MYID - return this Sentinel's unique ID
fn sentinel_myid(state: &Arc<SentinelState>) -> RespValue {
    RespValue::BulkString(state.myid.clone())
}

/// SENTINEL MASTERS - list all monitored masters
fn sentinel_masters(state: &Arc<SentinelState>) -> RespValue {
    let mut masters = Vec::new();

    for entry in state.masters.iter() {
        let master = entry.value();
        masters.push(format_master_info(master));
    }

    RespValue::Array(masters)
}

/// SENTINEL MASTER <name> - show info about a specific master
fn sentinel_master(args: &[Bytes], state: &Arc<SentinelState>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error(
            "ERR wrong number of arguments for 'sentinel master' command".into(),
        );
    }

    let name = String::from_utf8_lossy(&args[0]);

    match state.get_master(&name) {
        Some(master) => format_master_info(&master),
        None => RespValue::Error(format!("ERR No such master with that name: {}", name).into()),
    }
}

/// SENTINEL REPLICAS <name> - list replicas for a master
fn sentinel_replicas(args: &[Bytes], state: &Arc<SentinelState>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error(
            "ERR wrong number of arguments for 'sentinel replicas' command".into(),
        );
    }

    let name = String::from_utf8_lossy(&args[0]);

    let master = match state.get_master(&name) {
        Some(m) => m,
        None => {
            return RespValue::Error(format!("ERR No such master with that name: {}", name).into());
        }
    };

    let mut replicas = Vec::new();
    for entry in master.replicas.iter() {
        let replica = entry.value();
        replicas.push(format_replica_info(replica, &master.name));
    }

    RespValue::Array(replicas)
}

/// SENTINEL SENTINELS <name> - list sentinels for a master
fn sentinel_sentinels(args: &[Bytes], state: &Arc<SentinelState>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error(
            "ERR wrong number of arguments for 'sentinel sentinels' command".into(),
        );
    }

    let name = String::from_utf8_lossy(&args[0]);

    let master = match state.get_master(&name) {
        Some(m) => m,
        None => {
            return RespValue::Error(format!("ERR No such master with that name: {}", name).into());
        }
    };

    let mut sentinels = Vec::new();
    for entry in master.sentinels.iter() {
        let sentinel = entry.value();
        sentinels.push(format_sentinel_info(sentinel, &master.name));
    }

    RespValue::Array(sentinels)
}

/// SENTINEL GET-MASTER-ADDR-BY-NAME <name> - get master IP:port
fn sentinel_get_master_addr(args: &[Bytes], state: &Arc<SentinelState>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error(
            "ERR wrong number of arguments for 'sentinel get-master-addr-by-name' command".into(),
        );
    }

    let name = String::from_utf8_lossy(&args[0]);

    match state.get_master(&name) {
        Some(master) => {
            let ip = master.ip.read().clone();
            let port = master.port.load(Ordering::Relaxed);
            RespValue::Array(vec![
                RespValue::BulkString(Bytes::from(ip)),
                RespValue::BulkString(Bytes::from(port.to_string())),
            ])
        }
        None => RespValue::Null,
    }
}

/// SENTINEL MONITOR <name> <ip> <port> <quorum>
fn sentinel_monitor(
    args: &[Bytes],
    state: &Arc<SentinelState>,
    config: &Arc<SharedSentinelConfig>,
) -> RespValue {
    if args.len() < 4 {
        return RespValue::Error(
            "ERR wrong number of arguments for 'sentinel monitor' command".into(),
        );
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let ip = String::from_utf8_lossy(&args[1]).to_string();
    let port: u16 = match String::from_utf8_lossy(&args[2]).parse() {
        Ok(p) => p,
        Err(_) => return RespValue::Error("ERR Invalid port".into()),
    };
    let quorum: u32 = match String::from_utf8_lossy(&args[3]).parse() {
        Ok(q) => q,
        Err(_) => return RespValue::Error("ERR Invalid quorum".into()),
    };

    // Check if already monitoring
    if state.masters.contains_key(&name) {
        return RespValue::Error("ERR Duplicated master name".into());
    }

    // Create master config
    let master_config = MasterConfig::new(name.clone(), ip.clone(), port, quorum);

    // Add to config
    config.write().add_master(master_config.clone());

    // Add to state
    let master = MasterInstance::new(name.clone(), ip, port, master_config);
    state.add_master(master);

    log::info!("Started monitoring master {} at quorum {}", name, quorum);

    RespValue::ok()
}

/// SENTINEL REMOVE <name>
fn sentinel_remove(
    args: &[Bytes],
    state: &Arc<SentinelState>,
    config: &Arc<SharedSentinelConfig>,
) -> RespValue {
    if args.is_empty() {
        return RespValue::Error(
            "ERR wrong number of arguments for 'sentinel remove' command".into(),
        );
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();

    // Remove from state
    if state.remove_master(&name).is_none() {
        return RespValue::Error(format!("ERR No such master with that name: {}", name).into());
    }

    // Remove from config
    config.write().remove_master(&name);

    log::info!("Stopped monitoring master {}", name);

    RespValue::ok()
}

/// SENTINEL SET <name> <option> <value> [<option> <value> ...]
fn sentinel_set(args: &[Bytes], state: &Arc<SentinelState>) -> RespValue {
    if args.len() < 3 || args.len() % 2 == 0 {
        return RespValue::Error("ERR wrong number of arguments for 'sentinel set' command".into());
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();

    let master = match state.get_master(&name) {
        Some(m) => m,
        None => {
            return RespValue::Error(format!("ERR No such master with that name: {}", name).into());
        }
    };

    let mut config = master.config.write();

    for pair in args[1..].chunks(2) {
        let option = String::from_utf8_lossy(&pair[0]).to_lowercase();
        let value = String::from_utf8_lossy(&pair[1]).to_string();

        match option.as_str() {
            "down-after-milliseconds" => match value.parse::<u64>() {
                Ok(v) => config.down_after_ms = v,
                Err(_) => {
                    return RespValue::Error("ERR Invalid down-after-milliseconds value".into());
                }
            },
            "failover-timeout" => match value.parse::<u64>() {
                Ok(v) => config.failover_timeout = v,
                Err(_) => return RespValue::Error("ERR Invalid failover-timeout value".into()),
            },
            "parallel-syncs" => match value.parse::<u32>() {
                Ok(v) => config.parallel_syncs = v,
                Err(_) => return RespValue::Error("ERR Invalid parallel-syncs value".into()),
            },
            "quorum" => match value.parse::<u32>() {
                Ok(v) => config.quorum = v,
                Err(_) => return RespValue::Error("ERR Invalid quorum value".into()),
            },
            "auth-pass" => {
                config.auth_pass = if value.is_empty() { None } else { Some(value) };
            }
            "auth-user" => {
                config.auth_user = if value.is_empty() { None } else { Some(value) };
            }
            _ => {
                return RespValue::Error(
                    format!("ERR Unknown option or value for 'sentinel set'").into(),
                );
            }
        }
    }

    RespValue::ok()
}

/// SENTINEL RESET <pattern>
fn sentinel_reset(args: &[Bytes], state: &Arc<SentinelState>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error(
            "ERR wrong number of arguments for 'sentinel reset' command".into(),
        );
    }

    let pattern = String::from_utf8_lossy(&args[0]);
    let mut count = 0;

    // Collect masters to reset
    let masters: Vec<String> = state
        .masters
        .iter()
        .filter(|e| glob_match(&pattern, e.key()))
        .map(|e| e.key().clone())
        .collect();

    for name in masters {
        if let Some(master) = state.get_master(&name) {
            // Clear replicas and sentinels
            master.replicas.clear();
            master.sentinels.clear();
            master.update_counts();

            // Reset flags
            master.flags.set_sdown(false);
            master.flags.set_odown(false);

            count += 1;
        }
    }

    RespValue::Integer(count)
}

/// SENTINEL CKQUORUM <name>
fn sentinel_ckquorum(args: &[Bytes], state: &Arc<SentinelState>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error(
            "ERR wrong number of arguments for 'sentinel ckquorum' command".into(),
        );
    }

    let name = String::from_utf8_lossy(&args[0]);

    let master = match state.get_master(&name) {
        Some(m) => m,
        None => {
            return RespValue::Error(format!("ERR No such master with that name: {}", name).into());
        }
    };

    let quorum = master.quorum();
    let reachable = master.reachable_sentinels() + 1; // +1 for ourselves
    let total = master.sentinels.len() + 1;

    if reachable as u32 >= quorum {
        RespValue::SimpleString(
            format!(
                "OK {} usable Sentinels. Quorum and failover authorization is possible",
                reachable
            )
            .into(),
        )
    } else {
        RespValue::Error(
            format!(
                "NOQUORUM {} usable Sentinels out of {}. Need {} for quorum",
                reachable, total, quorum
            )
            .into(),
        )
    }
}

/// SENTINEL FLUSHCONFIG
fn sentinel_flushconfig(_config: &Arc<SharedSentinelConfig>) -> RespValue {
    // This would trigger a config save
    // For now, just return OK
    RespValue::ok()
}

/// SENTINEL FAILOVER <name>
fn sentinel_failover_cmd(
    args: &[Bytes],
    state: &Arc<SentinelState>,
    failover: Option<&Arc<SentinelFailover>>,
) -> RespValue {
    if args.is_empty() {
        return RespValue::Error(
            "ERR wrong number of arguments for 'sentinel failover' command".into(),
        );
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();

    let master = match state.get_master(&name) {
        Some(m) => m,
        None => {
            return RespValue::Error(format!("ERR No such master with that name: {}", name).into());
        }
    };

    let failover = match failover {
        Some(f) => f.clone(),
        None => return RespValue::Error("ERR Failover not available".into()),
    };

    // Spawn failover task
    tokio::spawn(async move {
        if let Err(e) = failover.force_failover(&master).await {
            log::error!("Forced failover failed: {}", e);
        }
    });

    RespValue::ok()
}

/// SENTINEL CONFIG GET/SET
fn sentinel_config(args: &[Bytes], config: &Arc<SharedSentinelConfig>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error(
            "ERR wrong number of arguments for 'sentinel config' command".into(),
        );
    }

    let subcmd = String::from_utf8_lossy(&args[0]).to_uppercase();

    match subcmd.as_str() {
        "GET" => {
            if args.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments".into());
            }
            let param = String::from_utf8_lossy(&args[1]).to_lowercase();
            let cfg = config.read();

            match param.as_str() {
                "resolve-hostnames" => RespValue::Array(vec![
                    RespValue::BulkString(Bytes::from("resolve-hostnames")),
                    RespValue::BulkString(Bytes::from(if cfg.resolve_hostnames {
                        "yes"
                    } else {
                        "no"
                    })),
                ]),
                "announce-hostnames" => RespValue::Array(vec![
                    RespValue::BulkString(Bytes::from("announce-hostnames")),
                    RespValue::BulkString(Bytes::from(if cfg.announce_hostnames {
                        "yes"
                    } else {
                        "no"
                    })),
                ]),
                "announce-ip" => RespValue::Array(vec![
                    RespValue::BulkString(Bytes::from("announce-ip")),
                    RespValue::BulkString(Bytes::from(cfg.announce_ip.clone().unwrap_or_default())),
                ]),
                "announce-port" => RespValue::Array(vec![
                    RespValue::BulkString(Bytes::from("announce-port")),
                    RespValue::BulkString(Bytes::from(
                        cfg.announce_port.map(|p| p.to_string()).unwrap_or_default(),
                    )),
                ]),
                _ => RespValue::Error("ERR Unknown config parameter".into()),
            }
        }
        "SET" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments".into());
            }
            let param = String::from_utf8_lossy(&args[1]).to_lowercase();
            let value = String::from_utf8_lossy(&args[2]).to_string();
            let mut cfg = config.write();

            match param.as_str() {
                "resolve-hostnames" => {
                    cfg.resolve_hostnames = value == "yes" || value == "1" || value == "true";
                    RespValue::ok()
                }
                "announce-hostnames" => {
                    cfg.announce_hostnames = value == "yes" || value == "1" || value == "true";
                    RespValue::ok()
                }
                "announce-ip" => {
                    cfg.announce_ip = if value.is_empty() { None } else { Some(value) };
                    RespValue::ok()
                }
                "announce-port" => match value.parse::<u16>() {
                    Ok(p) => {
                        cfg.announce_port = if p == 0 { None } else { Some(p) };
                        RespValue::ok()
                    }
                    Err(_) => RespValue::Error("ERR Invalid port".into()),
                },
                _ => RespValue::Error("ERR Unknown config parameter".into()),
            }
        }
        _ => RespValue::Error("ERR SENTINEL CONFIG subcommand must be GET or SET".into()),
    }
}

/// SENTINEL IS-MASTER-DOWN-BY-ADDR <ip> <port> <current-epoch> <runid>
fn sentinel_is_master_down(
    args: &[Bytes],
    state: &Arc<SentinelState>,
    election: Option<&Arc<LeaderElection>>,
) -> RespValue {
    if args.len() < 4 {
        return RespValue::Error(
            "ERR wrong number of arguments for 'sentinel is-master-down-by-addr' command".into(),
        );
    }

    let ip = String::from_utf8_lossy(&args[0]).to_string();
    let port: u16 = match String::from_utf8_lossy(&args[1]).parse() {
        Ok(p) => p,
        Err(_) => return RespValue::Error("ERR Invalid port".into()),
    };
    let epoch: u64 = match String::from_utf8_lossy(&args[2]).parse() {
        Ok(e) => e,
        Err(_) => return RespValue::Error("ERR Invalid epoch".into()),
    };
    let runid = String::from_utf8_lossy(&args[3]).to_string();

    // Find master by IP:port
    let mut is_down = 0i64;
    let mut leader_runid = "*".to_string();
    let mut leader_epoch = 0u64;

    for entry in state.masters.iter() {
        let master = entry.value();
        let master_ip = master.ip.read().clone();
        let master_port = master.port.load(Ordering::Relaxed);

        if master_ip == ip && master_port == port {
            is_down = if master.flags.is_sdown() { 1 } else { 0 };

            // If runid is not "*", this is a vote request
            if runid != "*" {
                if let Some(election) = election {
                    let response =
                        election.handle_vote_request(&master.name, &ip, port, epoch, &runid);

                    if response.vote_granted {
                        leader_runid = response.leader_runid;
                        leader_epoch = response.leader_epoch;
                    } else if !response.leader_runid.is_empty() {
                        leader_runid = response.leader_runid;
                        leader_epoch = response.leader_epoch;
                    }
                }
            }
            break;
        }
    }

    RespValue::Array(vec![
        RespValue::Integer(is_down),
        RespValue::BulkString(Bytes::from(leader_runid)),
        RespValue::Integer(leader_epoch as i64),
    ])
}

/// SENTINEL INFO-CACHE <name>
fn sentinel_info_cache(args: &[Bytes], state: &Arc<SentinelState>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error(
            "ERR wrong number of arguments for 'sentinel info-cache' command".into(),
        );
    }

    let name = String::from_utf8_lossy(&args[0]);

    let master = match state.get_master(&name) {
        Some(m) => m,
        None => {
            return RespValue::Error(format!("ERR No such master with that name: {}", name).into());
        }
    };

    // Return cached INFO for master and replicas
    let mut result = Vec::new();

    // Master info
    let master_ip = master.ip.read().clone();
    let master_port = master.port.load(Ordering::Relaxed);
    result.push(RespValue::Array(vec![
        RespValue::BulkString(Bytes::from(format!("{}:{}", master_ip, master_port))),
        RespValue::Integer(master.info_refresh.load(Ordering::Relaxed) as i64),
        RespValue::BulkString(Bytes::from("# Cached INFO (not available)")),
    ]));

    // Replica info
    for entry in master.replicas.iter() {
        let replica = entry.value();
        result.push(RespValue::Array(vec![
            RespValue::BulkString(Bytes::from(format!("{}:{}", replica.ip, replica.port))),
            RespValue::Integer(replica.info_refresh.load(Ordering::Relaxed) as i64),
            RespValue::BulkString(Bytes::from("# Cached INFO (not available)")),
        ]));
    }

    RespValue::Array(result)
}

/// SENTINEL PENDING-SCRIPTS
fn sentinel_pending_scripts(state: &Arc<SentinelState>) -> RespValue {
    let count = state.scripts_queue_length.load(Ordering::Relaxed);
    let running = state.running_scripts.load(Ordering::Relaxed);

    RespValue::Array(vec![
        RespValue::BulkString(Bytes::from("pending_scripts")),
        RespValue::Integer(count as i64),
        RespValue::BulkString(Bytes::from("running_scripts")),
        RespValue::Integer(running as i64),
    ])
}

/// SENTINEL SIMULATE-FAILURE [crash-after-election] [crash-after-promotion] [help]
fn sentinel_simulate_failure(args: &[Bytes], state: &Arc<SentinelState>) -> RespValue {
    if args.is_empty() {
        return RespValue::Error(
            "ERR wrong number of arguments for 'sentinel simulate-failure' command".into(),
        );
    }

    let flag = String::from_utf8_lossy(&args[0]).to_lowercase();

    match flag.as_str() {
        "crash-after-election" => {
            state.simulate_failure_flags.fetch_or(1, Ordering::Relaxed);
            RespValue::ok()
        }
        "crash-after-promotion" => {
            state.simulate_failure_flags.fetch_or(2, Ordering::Relaxed);
            RespValue::ok()
        }
        "help" => RespValue::Array(vec![
            RespValue::BulkString(Bytes::from("SENTINEL SIMULATE-FAILURE <flag>")),
            RespValue::BulkString(Bytes::from(
                "flags: crash-after-election, crash-after-promotion",
            )),
        ]),
        _ => RespValue::Error("ERR Unknown failure simulation flag".into()),
    }
}

/// Handle DEBUG command
fn sentinel_debug(_args: &[Bytes]) -> RespValue {
    // Debug commands for testing sentinel internals
    // Reserved for future implementation
    RespValue::SimpleString(Bytes::from("OK"))
}

/// SENTINEL HELP
fn sentinel_help() -> RespValue {
    let commands = vec![
        "SENTINEL MYID",
        "SENTINEL MASTERS",
        "SENTINEL MASTER <name>",
        "SENTINEL REPLICAS <name>",
        "SENTINEL SENTINELS <name>",
        "SENTINEL GET-MASTER-ADDR-BY-NAME <name>",
        "SENTINEL CKQUORUM <name>",
        "SENTINEL FAILOVER <name>",
        "SENTINEL FLUSHCONFIG",
        "SENTINEL MONITOR <name> <ip> <port> <quorum>",
        "SENTINEL REMOVE <name>",
        "SENTINEL SET <name> <option> <value>",
        "SENTINEL RESET <pattern>",
        "SENTINEL CONFIG GET <param>",
        "SENTINEL CONFIG SET <param> <value>",
        "SENTINEL IS-MASTER-DOWN-BY-ADDR <ip> <port> <epoch> <runid>",
        "SENTINEL INFO-CACHE <name>",
        "SENTINEL PENDING-SCRIPTS",
        "SENTINEL SIMULATE-FAILURE <flag>",
    ];

    RespValue::Array(
        commands
            .into_iter()
            .map(|s| RespValue::BulkString(Bytes::from(s)))
            .collect(),
    )
}

// =============================================================================
// Formatting helpers
// =============================================================================

fn format_master_info(master: &Arc<MasterInstance>) -> RespValue {
    let ip = master.ip.read().clone();
    let port = master.port.load(Ordering::Relaxed);
    let runid = master.runid.read().clone();
    let flags = master.flags.format();
    let num_slaves = master.num_slaves.load(Ordering::Relaxed);
    let num_sentinels = master.num_other_sentinels.load(Ordering::Relaxed);
    let quorum = master.quorum();
    let failover_timeout = master.failover_timeout();
    let parallel_syncs = master.parallel_syncs();
    let config_epoch = master.config_epoch.load(Ordering::Relaxed);
    let down_after = master.down_after_ms();
    let last_ping = master.last_ping_time.load(Ordering::Relaxed);
    let last_pong = master.last_pong_time.load(Ordering::Relaxed);

    RespValue::Array(vec![
        RespValue::BulkString(Bytes::from("name")),
        RespValue::BulkString(Bytes::from(master.name.clone())),
        RespValue::BulkString(Bytes::from("ip")),
        RespValue::BulkString(Bytes::from(ip)),
        RespValue::BulkString(Bytes::from("port")),
        RespValue::BulkString(Bytes::from(port.to_string())),
        RespValue::BulkString(Bytes::from("runid")),
        RespValue::BulkString(Bytes::from(runid)),
        RespValue::BulkString(Bytes::from("flags")),
        RespValue::BulkString(Bytes::from(flags)),
        RespValue::BulkString(Bytes::from("link-pending-commands")),
        RespValue::BulkString(Bytes::from(
            master
                .link_pending_commands
                .load(Ordering::Relaxed)
                .to_string(),
        )),
        RespValue::BulkString(Bytes::from("link-refcount")),
        RespValue::BulkString(Bytes::from(
            master.link_refcount.load(Ordering::Relaxed).to_string(),
        )),
        RespValue::BulkString(Bytes::from("last-ping-sent")),
        RespValue::BulkString(Bytes::from(last_ping.to_string())),
        RespValue::BulkString(Bytes::from("last-ok-ping-reply")),
        RespValue::BulkString(Bytes::from(
            master
                .last_ok_ping_reply
                .load(Ordering::Relaxed)
                .to_string(),
        )),
        RespValue::BulkString(Bytes::from("last-ping-reply")),
        RespValue::BulkString(Bytes::from(last_pong.to_string())),
        RespValue::BulkString(Bytes::from("down-after-milliseconds")),
        RespValue::BulkString(Bytes::from(down_after.to_string())),
        RespValue::BulkString(Bytes::from("info-refresh")),
        RespValue::BulkString(Bytes::from(
            master.info_refresh.load(Ordering::Relaxed).to_string(),
        )),
        RespValue::BulkString(Bytes::from("role-reported")),
        RespValue::BulkString(Bytes::from(master.role_reported.read().clone())),
        RespValue::BulkString(Bytes::from("role-reported-time")),
        RespValue::BulkString(Bytes::from(
            master
                .role_reported_time
                .load(Ordering::Relaxed)
                .to_string(),
        )),
        RespValue::BulkString(Bytes::from("config-epoch")),
        RespValue::BulkString(Bytes::from(config_epoch.to_string())),
        RespValue::BulkString(Bytes::from("num-slaves")),
        RespValue::BulkString(Bytes::from(num_slaves.to_string())),
        RespValue::BulkString(Bytes::from("num-other-sentinels")),
        RespValue::BulkString(Bytes::from(num_sentinels.to_string())),
        RespValue::BulkString(Bytes::from("quorum")),
        RespValue::BulkString(Bytes::from(quorum.to_string())),
        RespValue::BulkString(Bytes::from("failover-timeout")),
        RespValue::BulkString(Bytes::from(failover_timeout.to_string())),
        RespValue::BulkString(Bytes::from("parallel-syncs")),
        RespValue::BulkString(Bytes::from(parallel_syncs.to_string())),
    ])
}

fn format_replica_info(
    replica: &Arc<super::state::ReplicaInstance>,
    _master_name: &str,
) -> RespValue {
    let runid = replica.runid.read().clone();
    let flags = replica.flags.format();
    let master_link_status = if replica.master_link_status.load(Ordering::Relaxed) {
        "ok"
    } else {
        "err"
    };

    RespValue::Array(vec![
        RespValue::BulkString(Bytes::from("name")),
        RespValue::BulkString(Bytes::from(format!("{}:{}", replica.ip, replica.port))),
        RespValue::BulkString(Bytes::from("ip")),
        RespValue::BulkString(Bytes::from(replica.ip.clone())),
        RespValue::BulkString(Bytes::from("port")),
        RespValue::BulkString(Bytes::from(replica.port.to_string())),
        RespValue::BulkString(Bytes::from("runid")),
        RespValue::BulkString(Bytes::from(runid)),
        RespValue::BulkString(Bytes::from("flags")),
        RespValue::BulkString(Bytes::from(flags)),
        RespValue::BulkString(Bytes::from("link-pending-commands")),
        RespValue::BulkString(Bytes::from(
            replica
                .link_pending_commands
                .load(Ordering::Relaxed)
                .to_string(),
        )),
        RespValue::BulkString(Bytes::from("link-refcount")),
        RespValue::BulkString(Bytes::from(
            replica.link_refcount.load(Ordering::Relaxed).to_string(),
        )),
        RespValue::BulkString(Bytes::from("last-ping-sent")),
        RespValue::BulkString(Bytes::from(
            replica.last_ping_time.load(Ordering::Relaxed).to_string(),
        )),
        RespValue::BulkString(Bytes::from("last-ok-ping-reply")),
        RespValue::BulkString(Bytes::from(
            replica.last_pong_time.load(Ordering::Relaxed).to_string(),
        )),
        RespValue::BulkString(Bytes::from("last-ping-reply")),
        RespValue::BulkString(Bytes::from(
            replica.last_pong_time.load(Ordering::Relaxed).to_string(),
        )),
        RespValue::BulkString(Bytes::from("master-link-down-time")),
        RespValue::BulkString(Bytes::from(
            replica
                .master_link_down_time
                .load(Ordering::Relaxed)
                .to_string(),
        )),
        RespValue::BulkString(Bytes::from("master-link-status")),
        RespValue::BulkString(Bytes::from(master_link_status)),
        RespValue::BulkString(Bytes::from("master-host")),
        RespValue::BulkString(Bytes::from(replica.master_host.read().clone())),
        RespValue::BulkString(Bytes::from("master-port")),
        RespValue::BulkString(Bytes::from(
            replica.master_port.load(Ordering::Relaxed).to_string(),
        )),
        RespValue::BulkString(Bytes::from("slave-priority")),
        RespValue::BulkString(Bytes::from(
            replica.replica_priority.load(Ordering::Relaxed).to_string(),
        )),
        RespValue::BulkString(Bytes::from("slave-repl-offset")),
        RespValue::BulkString(Bytes::from(
            replica.repl_offset.load(Ordering::Relaxed).to_string(),
        )),
    ])
}

fn format_sentinel_info(
    sentinel: &Arc<super::state::SentinelInstance>,
    _master_name: &str,
) -> RespValue {
    let runid = sentinel.runid.read().clone();
    let flags = sentinel.flags.format();

    RespValue::Array(vec![
        RespValue::BulkString(Bytes::from("name")),
        RespValue::BulkString(Bytes::from(format!("{}:{}", sentinel.ip, sentinel.port))),
        RespValue::BulkString(Bytes::from("ip")),
        RespValue::BulkString(Bytes::from(sentinel.ip.clone())),
        RespValue::BulkString(Bytes::from("port")),
        RespValue::BulkString(Bytes::from(sentinel.port.to_string())),
        RespValue::BulkString(Bytes::from("runid")),
        RespValue::BulkString(Bytes::from(runid)),
        RespValue::BulkString(Bytes::from("flags")),
        RespValue::BulkString(Bytes::from(flags)),
        RespValue::BulkString(Bytes::from("link-pending-commands")),
        RespValue::BulkString(Bytes::from("0")),
        RespValue::BulkString(Bytes::from("link-refcount")),
        RespValue::BulkString(Bytes::from("1")),
        RespValue::BulkString(Bytes::from("last-hello-message")),
        RespValue::BulkString(Bytes::from(
            sentinel.last_hello_time.load(Ordering::Relaxed).to_string(),
        )),
        RespValue::BulkString(Bytes::from("voted-leader")),
        RespValue::BulkString(Bytes::from(
            sentinel
                .voted_leader
                .read()
                .clone()
                .unwrap_or_else(|| "?".to_string()),
        )),
        RespValue::BulkString(Bytes::from("voted-leader-epoch")),
        RespValue::BulkString(Bytes::from(
            sentinel
                .voted_leader_epoch
                .load(Ordering::Relaxed)
                .to_string(),
        )),
    ])
}

/// Simple glob pattern matching
fn glob_match(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    let pattern_chars: Vec<char> = pattern.chars().collect();
    let text_chars: Vec<char> = text.chars().collect();

    glob_match_impl(&pattern_chars, &text_chars)
}

fn glob_match_impl(pattern: &[char], text: &[char]) -> bool {
    let mut pi = 0;
    let mut ti = 0;
    let mut star_pi = usize::MAX;
    let mut star_ti = usize::MAX;

    while ti < text.len() {
        if pi < pattern.len() && (pattern[pi] == '?' || pattern[pi] == text[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pattern.len() && pattern[pi] == '*' {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    while pi < pattern.len() && pattern[pi] == '*' {
        pi += 1;
    }

    pi == pattern.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("my*", "mymaster"));
        assert!(glob_match("*master", "mymaster"));
        assert!(glob_match("my?aster", "mymaster"));
        assert!(!glob_match("other*", "mymaster"));
    }
}
