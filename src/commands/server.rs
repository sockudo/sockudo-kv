//! Server command handlers
//!
//! Implements ACL, COMMAND, CONFIG, MEMORY, LATENCY, SLOWLOG, and server management commands.

use bytes::Bytes;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::server_state::{
    ACL_CATEGORIES, AclCommandRules, AclUser, ServerState, get_commands_in_category,
};
use crate::storage::Store;

/// Execute a server command
pub fn execute(
    store: &Store,
    server: &Arc<ServerState>,
    cmd: &[u8],
    args: &[Bytes],
) -> Result<RespValue> {
    match cmd.to_ascii_uppercase().as_slice() {
        // ACL commands
        b"ACL" => cmd_acl(server, args),

        // COMMAND introspection
        b"COMMAND" => cmd_command(args),

        // Memory commands
        b"MEMORY" => cmd_memory(store, args),

        // Latency commands
        b"LATENCY" => cmd_latency(server, args),

        // Slowlog commands
        b"SLOWLOG" => cmd_slowlog(server, args),

        // Server management
        b"BGREWRITEAOF" => Ok(RespValue::bulk_string(
            "Background append only file rewriting started",
        )),

        b"LASTSAVE" => Ok(RespValue::integer(
            server
                .last_save_time
                .load(std::sync::atomic::Ordering::Relaxed) as i64,
        )),
        b"LOLWUT" => cmd_lolwut(args),
        b"SHUTDOWN" => Err(Error::Custom("ERR Server is shutting down".to_string())),
        b"TIME" => {
            let (secs, usecs) = server.time();
            Ok(RespValue::array(vec![
                RespValue::bulk_string(&secs.to_string()),
                RespValue::bulk_string(&usecs.to_string()),
            ]))
        }

        // Replication stubs
        b"FAILOVER" => Ok(RespValue::ok()),
        b"PSYNC" => Ok(RespValue::bulk_string(
            "+FULLRESYNC 0000000000000000000000000000000000000000 0",
        )),
        b"REPLCONF" => Ok(RespValue::ok()),
        b"REPLICAOF" | b"SLAVEOF" => Ok(RespValue::ok()),
        b"ROLE" => Ok(RespValue::array(vec![
            RespValue::bulk_string("master"),
            RespValue::integer(0),
            RespValue::array(vec![]),
        ])),
        b"SYNC" => Ok(RespValue::bulk_string("")),

        // Module stubs
        b"MODULE" => cmd_module(args),

        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd).into_owned(),
        )),
    }
}

// === ACL Commands ===

fn cmd_acl(server: &Arc<ServerState>, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("ACL"));
    }

    match args[0].to_ascii_uppercase().as_slice() {
        b"CAT" => {
            if args.len() == 1 {
                let cats: Vec<RespValue> = ACL_CATEGORIES
                    .iter()
                    .map(|c| RespValue::bulk_string(c))
                    .collect();
                Ok(RespValue::array(cats))
            } else {
                let cat = std::str::from_utf8(&args[1]).map_err(|_| Error::Syntax)?;
                let cmds = get_commands_in_category(cat);
                if cmds.is_empty() {
                    Err(Error::Custom(format!("ERR Unknown ACL category '{}'", cat)))
                } else {
                    let resp: Vec<RespValue> = cmds
                        .iter()
                        .map(|c| RespValue::bulk_string(&c.to_lowercase()))
                        .collect();
                    Ok(RespValue::array(resp))
                }
            }
        }
        b"DELUSER" => {
            if args.len() < 2 {
                return Err(Error::WrongArity("ACL DELUSER"));
            }
            let mut deleted = 0i64;
            for name in &args[1..] {
                if name.as_ref() == b"default" {
                    continue; // Can't delete default user
                }
                if server.acl_users.remove(name).is_some() {
                    deleted += 1;
                }
            }
            Ok(RespValue::integer(deleted))
        }
        b"DRYRUN" => {
            // ACL DRYRUN username command [arg ...]
            if args.len() < 3 {
                return Err(Error::WrongArity("ACL DRYRUN"));
            }
            let username = &args[1];
            let command = &args[2];

            // Check if user exists
            if !server.acl_users.contains_key(username.as_ref()) {
                return Ok(RespValue::error(&format!(
                    "ERR User '{}' not found",
                    String::from_utf8_lossy(username)
                )));
            }

            // In our simplified ACL, users can run all commands
            // Return OK indicating the command would be allowed
            // TODO: Add actual command permission checking when full ACL is implemented
            let _cmd_upper = command.to_ascii_uppercase();
            Ok(RespValue::ok())
        }
        b"GENPASS" => {
            let bits = if args.len() > 1 {
                std::str::from_utf8(&args[1])
                    .map_err(|_| Error::NotInteger)?
                    .parse::<usize>()
                    .map_err(|_| Error::NotInteger)?
            } else {
                256
            };
            let bytes = bits.div_ceil(8);
            let password: String = (0..bytes)
                .map(|_| format!("{:02x}", fastrand::u8(..)))
                .collect();
            Ok(RespValue::bulk_string(
                &password[..std::cmp::min(password.len(), bits / 4)],
            ))
        }
        b"GETUSER" => {
            if args.len() != 2 {
                return Err(Error::WrongArity("ACL GETUSER"));
            }
            match server.get_acl_user(&args[1]) {
                Some(user) => Ok(format_acl_user(&user)),
                None => Ok(RespValue::null()),
            }
        }
        b"LIST" => {
            let users: Vec<RespValue> = server
                .acl_users
                .iter()
                .map(|e| RespValue::bulk_string(&e.value().format_rules()))
                .collect();
            Ok(RespValue::array(users))
        }
        b"LOAD" => Ok(RespValue::ok()),
        b"LOG" => {
            if args.len() > 1 && args[1].eq_ignore_ascii_case(b"RESET") {
                server.reset_acl_log();
                return Ok(RespValue::ok());
            }
            let count = if args.len() > 1 {
                Some(
                    std::str::from_utf8(&args[1])
                        .map_err(|_| Error::NotInteger)?
                        .parse()
                        .map_err(|_| Error::NotInteger)?,
                )
            } else {
                None
            };
            let entries = server.get_acl_log(count);
            let resp: Vec<RespValue> = entries.iter().map(format_acl_log_entry).collect();
            Ok(RespValue::array(resp))
        }
        b"SAVE" => Ok(RespValue::ok()),
        b"SETUSER" => {
            if args.len() < 2 {
                return Err(Error::WrongArity("ACL SETUSER"));
            }
            let name = args[1].clone();

            // Insert default if not exists
            server
                .acl_users
                .entry(name.clone())
                .or_insert_with(|| AclUser {
                    name: name.clone(),
                    enabled: false,
                    passwords: Vec::new(),
                    nopass: false,
                    commands: AclCommandRules::default(),
                    keys: Vec::new(),
                    channels: Vec::new(),
                    selectors: Vec::new(),
                });

            // Now get mutable reference and apply rules
            if let Some(mut user) = server.acl_users.get_mut(&name) {
                for rule in &args[2..] {
                    let rule_str = std::str::from_utf8(rule).map_err(|_| Error::Syntax)?;
                    apply_acl_rule(&mut user, rule_str)?;
                }
            }
            Ok(RespValue::ok())
        }
        b"USERS" => {
            let users: Vec<RespValue> = server
                .list_acl_users()
                .iter()
                .map(|n| RespValue::bulk(n.clone()))
                .collect();
            Ok(RespValue::array(users))
        }
        b"WHOAMI" => Ok(RespValue::bulk_string("default")),
        _ => Err(Error::Custom(format!(
            "ERR Unknown subcommand or wrong number of arguments for '{}'",
            String::from_utf8_lossy(&args[0])
        ))),
    }
}

fn format_acl_user(user: &AclUser) -> RespValue {
    let mut result = Vec::new();

    result.push(RespValue::bulk_string("flags"));
    let mut flags = Vec::new();
    if user.enabled {
        flags.push(RespValue::bulk_string("on"));
    } else {
        flags.push(RespValue::bulk_string("off"));
    }
    if user.nopass {
        flags.push(RespValue::bulk_string("nopass"));
    }
    if user.commands.allow_all {
        flags.push(RespValue::bulk_string("allcommands"));
    }
    result.push(RespValue::array(flags));

    result.push(RespValue::bulk_string("passwords"));
    let pwds: Vec<RespValue> = user
        .passwords
        .iter()
        .map(|p| RespValue::bulk_string(&hex::encode(p)))
        .collect();
    result.push(RespValue::array(pwds));

    result.push(RespValue::bulk_string("commands"));
    result.push(RespValue::bulk_string(if user.commands.allow_all {
        "+@all"
    } else {
        ""
    }));

    result.push(RespValue::bulk_string("keys"));
    let keys: Vec<RespValue> = user
        .keys
        .iter()
        .map(|k| RespValue::bulk(k.clone()))
        .collect();
    result.push(RespValue::array(keys));

    result.push(RespValue::bulk_string("channels"));
    let chans: Vec<RespValue> = user
        .channels
        .iter()
        .map(|c| RespValue::bulk(c.clone()))
        .collect();
    result.push(RespValue::array(chans));

    result.push(RespValue::bulk_string("selectors"));
    result.push(RespValue::array(vec![]));

    RespValue::array(result)
}

fn format_acl_log_entry(entry: &crate::server_state::AclLogEntry) -> RespValue {
    RespValue::array(vec![
        RespValue::bulk_string("count"),
        RespValue::integer(entry.count as i64),
        RespValue::bulk_string("reason"),
        RespValue::bulk_string(entry.reason.as_str()),
        RespValue::bulk_string("context"),
        RespValue::bulk_string(&entry.context),
        RespValue::bulk_string("object"),
        RespValue::bulk_string(&entry.object),
        RespValue::bulk_string("username"),
        RespValue::bulk_string(&entry.username),
        RespValue::bulk_string("age-seconds"),
        RespValue::bulk_string(&format!("{:.3}", entry.age_seconds)),
        RespValue::bulk_string("client-info"),
        RespValue::bulk_string(&entry.client_info),
        RespValue::bulk_string("entry-id"),
        RespValue::integer(entry.entry_id as i64),
        RespValue::bulk_string("timestamp-created"),
        RespValue::integer(entry.timestamp_created as i64),
        RespValue::bulk_string("timestamp-last-updated"),
        RespValue::integer(entry.timestamp_last_updated as i64),
    ])
}

fn apply_acl_rule(user: &mut AclUser, rule: &str) -> Result<()> {
    match rule.to_lowercase().as_str() {
        "on" => user.enabled = true,
        "off" => user.enabled = false,
        "nopass" => user.nopass = true,
        "resetpass" => {
            user.passwords.clear();
            user.nopass = false;
        }
        "allkeys" | "~*" => user.keys = vec![Bytes::from_static(b"*")],
        "resetkeys" => user.keys.clear(),
        "allchannels" | "&*" => user.channels = vec![Bytes::from_static(b"*")],
        "resetchannels" => user.channels.clear(),
        "allcommands" | "+@all" => user.commands.allow_all = true,
        "nocommands" | "-@all" => user.commands.allow_all = false,
        r if r.starts_with('>') => user
            .passwords
            .push(Bytes::copy_from_slice(r[1..].as_bytes())),
        r if r.starts_with('<') => user.passwords.retain(|p| p.as_ref() != r[1..].as_bytes()),
        r if r.starts_with("~") => user.keys.push(Bytes::copy_from_slice(r[1..].as_bytes())),
        r if r.starts_with("&") => user
            .channels
            .push(Bytes::copy_from_slice(r[1..].as_bytes())),
        r if r.starts_with("+@") => user
            .commands
            .allowed_cats
            .push(Bytes::copy_from_slice(r[2..].as_bytes())),
        r if r.starts_with("-@") => user
            .commands
            .denied_cats
            .push(Bytes::copy_from_slice(r[2..].as_bytes())),
        r if r.starts_with('+') => user
            .commands
            .allowed
            .push(Bytes::copy_from_slice(r[1..].as_bytes())),
        r if r.starts_with('-') => user
            .commands
            .denied
            .push(Bytes::copy_from_slice(r[1..].as_bytes())),
        _ => {}
    }
    Ok(())
}

// === COMMAND Introspection ===

fn cmd_command(args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Ok(get_all_commands_info());
    }

    match args[0].to_ascii_uppercase().as_slice() {
        b"COUNT" => Ok(RespValue::integer(get_command_count())),
        b"DOCS" => cmd_command_docs(&args[1..]),
        b"GETKEYS" => cmd_command_getkeys(&args[1..]),
        b"GETKEYSANDFLAGS" => cmd_command_getkeys(&args[1..]),
        b"INFO" => cmd_command_info(&args[1..]),
        b"LIST" => cmd_command_list(&args[1..]),
        _ => Err(Error::Custom(format!(
            "ERR Unknown subcommand '{}'",
            String::from_utf8_lossy(&args[0])
        ))),
    }
}

fn get_command_count() -> i64 {
    200
} // Approximate

fn get_all_commands_info() -> RespValue {
    // Return basic info for common commands
    let commands = ["GET", "SET", "DEL", "PING", "INFO", "KEYS", "SCAN"];
    let info: Vec<RespValue> = commands
        .iter()
        .map(|c| get_single_command_info(c))
        .collect();
    RespValue::array(info)
}

fn get_single_command_info(name: &str) -> RespValue {
    let (arity, first_key, last_key, step) = match name.to_uppercase().as_str() {
        "GET" => (2, 1, 1, 1),
        "SET" => (-3, 1, 1, 1),
        "DEL" => (-2, 1, -1, 1),
        "MGET" => (-2, 1, -1, 1),
        "MSET" => (-3, 1, -1, 2),
        "PING" => (-1, 0, 0, 0),
        "INFO" => (-1, 0, 0, 0),
        _ => (-1, 0, 0, 0),
    };

    RespValue::array(vec![
        RespValue::bulk_string(&name.to_lowercase()),
        RespValue::integer(arity),
        RespValue::array(vec![RespValue::bulk_string("fast")]),
        RespValue::integer(first_key),
        RespValue::integer(last_key),
        RespValue::integer(step),
        RespValue::array(vec![]),
        RespValue::array(vec![]),
        RespValue::array(vec![]),
        RespValue::array(vec![]),
    ])
}

fn cmd_command_docs(args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Ok(RespValue::array(vec![]));
    }
    let docs: Vec<RespValue> = args
        .iter()
        .map(|cmd| {
            let name = String::from_utf8_lossy(cmd).to_lowercase();
            RespValue::array(vec![
                RespValue::bulk_string(&name),
                RespValue::array(vec![
                    RespValue::bulk_string("summary"),
                    RespValue::bulk_string(&format!("{} command", name)),
                    RespValue::bulk_string("since"),
                    RespValue::bulk_string("1.0.0"),
                    RespValue::bulk_string("group"),
                    RespValue::bulk_string("generic"),
                ]),
            ])
        })
        .collect();
    Ok(RespValue::array(docs))
}

fn cmd_command_getkeys(args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("COMMAND GETKEYS"));
    }
    // Simplified: return first argument as key for most commands
    if args.len() > 1 {
        Ok(RespValue::array(vec![RespValue::bulk(args[1].clone())]))
    } else {
        Ok(RespValue::array(vec![]))
    }
}

fn cmd_command_info(args: &[Bytes]) -> Result<RespValue> {
    let info: Vec<RespValue> = args
        .iter()
        .map(|c| get_single_command_info(&String::from_utf8_lossy(c)))
        .collect();
    Ok(RespValue::array(info))
}

fn cmd_command_list(args: &[Bytes]) -> Result<RespValue> {
    let all_cmds = vec![
        "GET",
        "SET",
        "DEL",
        "MGET",
        "MSET",
        "INCR",
        "DECR",
        "APPEND",
        "LPUSH",
        "RPUSH",
        "LPOP",
        "RPOP",
        "LRANGE",
        "LLEN",
        "SADD",
        "SREM",
        "SMEMBERS",
        "SISMEMBER",
        "SCARD",
        "HSET",
        "HGET",
        "HDEL",
        "HGETALL",
        "HKEYS",
        "HVALS",
        "ZADD",
        "ZREM",
        "ZRANGE",
        "ZSCORE",
        "ZCARD",
        "PING",
        "ECHO",
        "INFO",
        "DBSIZE",
        "KEYS",
        "SCAN",
        "TYPE",
        "TTL",
        "EXPIRE",
        "PUBLISH",
        "SUBSCRIBE",
        "PSUBSCRIBE",
        "MULTI",
        "EXEC",
        "DISCARD",
        "WATCH",
        "ACL",
        "CONFIG",
        "COMMAND",
        "CLIENT",
        "MEMORY",
        "SLOWLOG",
        "LATENCY",
    ];

    let mut result: Vec<&str> = all_cmds.clone();

    // Handle FILTERBY
    if args.len() >= 2
        && args[0].eq_ignore_ascii_case(b"FILTERBY")
        && args.len() >= 3
        && args[1].eq_ignore_ascii_case(b"ACLCAT")
    {
        let cat = std::str::from_utf8(&args[2]).unwrap_or("");
        result = get_commands_in_category(cat);
    }

    let resp: Vec<RespValue> = result
        .iter()
        .map(|c| RespValue::bulk_string(&c.to_lowercase()))
        .collect();
    Ok(RespValue::array(resp))
}

// === MEMORY Commands ===

fn cmd_memory(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("MEMORY"));
    }

    match args[0].to_ascii_uppercase().as_slice() {
        b"DOCTOR" => Ok(RespValue::bulk_string("Sam, I have no memory problems")),
        b"MALLOC-SIZE" | b"MALLOC-STATS" => Ok(RespValue::bulk_string(
            "Memory allocator stats not available",
        )),
        b"PURGE" => Ok(RespValue::ok()),
        b"STATS" => Ok(memory_stats(store)),
        b"USAGE" => {
            if args.len() < 2 {
                return Err(Error::WrongArity("MEMORY USAGE"));
            }
            // Estimate: base overhead + rough size
            let size = if store.exists(&args[1]) { 64 } else { 0 };
            if size > 0 {
                Ok(RespValue::integer(size))
            } else {
                Ok(RespValue::null())
            }
        }
        _ => Err(Error::Custom(format!(
            "ERR Unknown subcommand '{}'",
            String::from_utf8_lossy(&args[0])
        ))),
    }
}

fn memory_stats(store: &Store) -> RespValue {
    let key_count = store.len() as i64;
    RespValue::array(vec![
        RespValue::bulk_string("peak.allocated"),
        RespValue::integer(0),
        RespValue::bulk_string("total.allocated"),
        RespValue::integer(0),
        RespValue::bulk_string("startup.allocated"),
        RespValue::integer(0),
        RespValue::bulk_string("replication.backlog"),
        RespValue::integer(0),
        RespValue::bulk_string("clients.slaves"),
        RespValue::integer(0),
        RespValue::bulk_string("clients.normal"),
        RespValue::integer(0),
        RespValue::bulk_string("cluster.links"),
        RespValue::integer(0),
        RespValue::bulk_string("aof.buffer"),
        RespValue::integer(0),
        RespValue::bulk_string("lua.caches"),
        RespValue::integer(0),
        RespValue::bulk_string("functions.caches"),
        RespValue::integer(0),
        RespValue::bulk_string("db.0"),
        RespValue::array(vec![
            RespValue::bulk_string("overhead.hashtable.main"),
            RespValue::integer(key_count * 64),
            RespValue::bulk_string("overhead.hashtable.expires"),
            RespValue::integer(0),
            RespValue::bulk_string("overhead.hashtable.slot-to-keys"),
            RespValue::integer(0),
        ]),
        RespValue::bulk_string("overhead.total"),
        RespValue::integer(0),
        RespValue::bulk_string("keys.count"),
        RespValue::integer(key_count),
        RespValue::bulk_string("keys.bytes-per-key"),
        RespValue::integer(64),
        RespValue::bulk_string("dataset.bytes"),
        RespValue::integer(key_count * 64),
        RespValue::bulk_string("dataset.percentage"),
        RespValue::bulk_string("0"),
        RespValue::bulk_string("peak.percentage"),
        RespValue::bulk_string("0"),
        RespValue::bulk_string("allocator.allocated"),
        RespValue::integer(0),
        RespValue::bulk_string("allocator.active"),
        RespValue::integer(0),
        RespValue::bulk_string("allocator.resident"),
        RespValue::integer(0),
        RespValue::bulk_string("allocator-fragmentation.ratio"),
        RespValue::bulk_string("1.0"),
        RespValue::bulk_string("allocator-fragmentation.bytes"),
        RespValue::integer(0),
        RespValue::bulk_string("allocator-rss.ratio"),
        RespValue::bulk_string("1.0"),
        RespValue::bulk_string("allocator-rss.bytes"),
        RespValue::integer(0),
        RespValue::bulk_string("rss-overhead.ratio"),
        RespValue::bulk_string("1.0"),
        RespValue::bulk_string("rss-overhead.bytes"),
        RespValue::integer(0),
        RespValue::bulk_string("fragmentation"),
        RespValue::bulk_string("1.0"),
        RespValue::bulk_string("fragmentation.bytes"),
        RespValue::integer(0),
    ])
}

// === LATENCY Commands ===

fn cmd_latency(server: &Arc<ServerState>, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("LATENCY"));
    }

    match args[0].to_ascii_uppercase().as_slice() {
        b"DOCTOR" => Ok(RespValue::bulk_string(
            "I have no latency reports to show you.",
        )),
        b"GRAPH" => {
            if args.len() < 2 {
                return Err(Error::WrongArity("LATENCY GRAPH"));
            }
            Ok(RespValue::bulk_string(""))
        }
        b"HISTOGRAM" => Ok(RespValue::array(vec![])),
        b"HISTORY" => {
            if args.len() < 2 {
                return Err(Error::WrongArity("LATENCY HISTORY"));
            }
            let samples = server.get_latency_history(&args[1]);
            let resp: Vec<RespValue> = samples
                .iter()
                .map(|s| {
                    RespValue::array(vec![
                        RespValue::integer(s.timestamp as i64),
                        RespValue::integer(s.latency_ms as i64),
                    ])
                })
                .collect();
            Ok(RespValue::array(resp))
        }
        b"LATEST" => {
            let latest = server.get_latency_latest();
            let resp: Vec<RespValue> = latest
                .iter()
                .map(|(name, sample)| {
                    RespValue::array(vec![
                        RespValue::bulk(name.clone()),
                        RespValue::integer(sample.timestamp as i64),
                        RespValue::integer(sample.latency_ms as i64),
                        RespValue::integer(sample.latency_ms as i64),
                    ])
                })
                .collect();
            Ok(RespValue::array(resp))
        }
        b"RESET" => {
            if args.len() > 1 {
                let events: Vec<Bytes> = args[1..].to_vec();
                server.reset_latency(Some(&events));
            } else {
                server.reset_latency(None);
            }
            Ok(RespValue::ok())
        }
        _ => Err(Error::Custom(format!(
            "ERR Unknown subcommand '{}'",
            String::from_utf8_lossy(&args[0])
        ))),
    }
}

// === SLOWLOG Commands ===

fn cmd_slowlog(server: &Arc<ServerState>, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("SLOWLOG"));
    }

    match args[0].to_ascii_uppercase().as_slice() {
        b"GET" => {
            let count = if args.len() > 1 {
                Some(
                    std::str::from_utf8(&args[1])
                        .map_err(|_| Error::NotInteger)?
                        .parse()
                        .map_err(|_| Error::NotInteger)?,
                )
            } else {
                None
            };
            let entries = server.get_slowlog(count);
            let resp: Vec<RespValue> = entries
                .iter()
                .map(|e| {
                    let cmd_parts: Vec<RespValue> = e
                        .command
                        .iter()
                        .map(|c| RespValue::bulk(c.clone()))
                        .collect();
                    RespValue::array(vec![
                        RespValue::integer(e.id as i64),
                        RespValue::integer(e.timestamp as i64),
                        RespValue::integer(e.duration_us as i64),
                        RespValue::array(cmd_parts),
                        RespValue::bulk_string(&e.client_addr),
                        RespValue::bulk_string(&e.client_name),
                    ])
                })
                .collect();
            Ok(RespValue::array(resp))
        }
        b"LEN" => Ok(RespValue::integer(server.slowlog_len() as i64)),
        b"RESET" => {
            server.slowlog_reset();
            Ok(RespValue::ok())
        }
        _ => Err(Error::Custom(format!(
            "ERR Unknown subcommand '{}'",
            String::from_utf8_lossy(&args[0])
        ))),
    }
}

// === Other Commands ===

// SAVE and BGSAVE are handled in main.rs because they need access to MultiStore

fn cmd_lolwut(_args: &[Bytes]) -> Result<RespValue> {
    let art = r#"
   _____            _              _       
  / ____|          | |            | |      
 | (___   ___   ___| | ___   _  __| | ___  
  \___ \ / _ \ / __| |/ / | | |/ _` |/ _ \ 
  ____) | (_) | (__|   <| |_| | (_| | (_) |
 |_____/ \___/ \___|_|\_\\__,_|\__,_|\___/ 
                                           
sockudo-kv ver. 7.0.0
"#;
    Ok(RespValue::bulk_string(art))
}

fn cmd_module(args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("MODULE"));
    }

    match args[0].to_ascii_uppercase().as_slice() {
        b"LIST" => Ok(RespValue::array(vec![])),
        b"LOAD" | b"LOADEX" => Err(Error::Custom(
            "ERR Module loading not supported".to_string(),
        )),
        b"UNLOAD" => Err(Error::Custom("ERR No such module".to_string())),
        _ => Err(Error::Custom(format!(
            "ERR Unknown subcommand '{}'",
            String::from_utf8_lossy(&args[0])
        ))),
    }
}
