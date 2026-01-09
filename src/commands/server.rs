//! Server command handlers
//!
//! Implements ACL, COMMAND, CONFIG, MEMORY, LATENCY, SLOWLOG, and server management commands.

use bytes::Bytes;
use std::sync::Arc;

use crate::config_table::{CONFIG_TABLE, ConfigFlags, find_config, matches_pattern};
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

        // CONFIG command
        b"CONFIG" => cmd_config(server, args),

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
            .push(Bytes::copy_from_slice(&r.as_bytes()[1..])),
        r if r.starts_with('<') => user.passwords.retain(|p| p.as_ref() != &r.as_bytes()[1..]),
        r if r.starts_with("~") => user.keys.push(Bytes::copy_from_slice(&r.as_bytes()[1..])),
        r if r.starts_with("&") => user
            .channels
            .push(Bytes::copy_from_slice(&r.as_bytes()[1..])),
        r if r.starts_with("+@") => user
            .commands
            .allowed_cats
            .push(Bytes::copy_from_slice(&r.as_bytes()[2..])),
        r if r.starts_with("-@") => user
            .commands
            .denied_cats
            .push(Bytes::copy_from_slice(&r.as_bytes()[2..])),
        r if r.starts_with('+') => user
            .commands
            .allowed
            .push(Bytes::copy_from_slice(&r.as_bytes()[1..])),
        r if r.starts_with('-') => user
            .commands
            .denied
            .push(Bytes::copy_from_slice(&r.as_bytes()[1..])),
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
        // String commands
        "GET",
        "SET",
        "SETNX",
        "SETEX",
        "PSETEX",
        "MGET",
        "MSET",
        "MSETNX",
        "INCR",
        "DECR",
        "INCRBY",
        "DECRBY",
        "INCRBYFLOAT",
        "APPEND",
        "STRLEN",
        "GETRANGE",
        "SETRANGE",
        "GETSET",
        "GETEX",
        "GETDEL",
        // List commands
        "LPUSH",
        "RPUSH",
        "LPUSHX",
        "RPUSHX",
        "LPOP",
        "RPOP",
        "LRANGE",
        "LLEN",
        "LINDEX",
        "LSET",
        "LINSERT",
        "LREM",
        "LTRIM",
        "LPOS",
        "LMOVE",
        "LMPOP",
        "BLPOP",
        "BRPOP",
        "BLMOVE",
        "BLMPOP",
        "RPOPLPUSH",
        "BRPOPLPUSH",
        // Set commands
        "SADD",
        "SREM",
        "SMEMBERS",
        "SISMEMBER",
        "SMISMEMBER",
        "SCARD",
        "SPOP",
        "SRANDMEMBER",
        "SDIFF",
        "SDIFFSTORE",
        "SINTER",
        "SINTERSTORE",
        "SINTERCARD",
        "SUNION",
        "SUNIONSTORE",
        "SMOVE",
        "SSCAN",
        // Hash commands
        "HSET",
        "HGET",
        "HDEL",
        "HEXISTS",
        "HLEN",
        "HKEYS",
        "HVALS",
        "HGETALL",
        "HINCRBY",
        "HINCRBYFLOAT",
        "HMSET",
        "HMGET",
        "HSETNX",
        "HSTRLEN",
        "HSCAN",
        "HRANDFIELD",
        // Sorted Set commands
        "ZADD",
        "ZREM",
        "ZSCORE",
        "ZRANK",
        "ZREVRANK",
        "ZCARD",
        "ZCOUNT",
        "ZLEXCOUNT",
        "ZRANGE",
        "ZRANGESTORE",
        "ZRANGEBYLEX",
        "ZRANGEBYSCORE",
        "ZREVRANGE",
        "ZREVRANGEBYLEX",
        "ZREVRANGEBYSCORE",
        "ZINCRBY",
        "ZPOPMIN",
        "ZPOPMAX",
        "BZPOPMIN",
        "BZPOPMAX",
        "ZMPOP",
        "BZMPOP",
        "ZINTER",
        "ZINTERSTORE",
        "ZINTERCARD",
        "ZUNION",
        "ZUNIONSTORE",
        "ZDIFF",
        "ZDIFFSTORE",
        "ZRANDMEMBER",
        "ZMSCORE",
        "ZSCAN",
        // Generic commands
        "DEL",
        "EXISTS",
        "EXPIRE",
        "EXPIREAT",
        "PEXPIRE",
        "PEXPIREAT",
        "EXPIRETIME",
        "PEXPIRETIME",
        "TTL",
        "PTTL",
        "PERSIST",
        "TYPE",
        "KEYS",
        "SCAN",
        "RENAME",
        "RENAMENX",
        "COPY",
        "DUMP",
        "RESTORE",
        "SORT",
        "SORT_RO",
        "OBJECT",
        "TOUCH",
        "UNLINK",
        "WAIT",
        "WAITAOF",
        "RANDOMKEY",
        "DBSIZE",
        "FLUSHDB",
        "FLUSHALL",
        "MOVE",
        // Connection commands
        "PING",
        "ECHO",
        "SELECT",
        "QUIT",
        "AUTH",
        "CLIENT",
        "HELLO",
        "RESET",
        // Server commands
        "INFO",
        "CONFIG",
        "COMMAND",
        "ACL",
        "MEMORY",
        "SLOWLOG",
        "LATENCY",
        "TIME",
        "DBSIZE",
        "BGSAVE",
        "BGREWRITEAOF",
        "LASTSAVE",
        "SAVE",
        "SHUTDOWN",
        "DEBUG",
        "LOLWUT",
        "SWAPDB",
        // Pub/Sub commands
        "PUBLISH",
        "SUBSCRIBE",
        "PSUBSCRIBE",
        "UNSUBSCRIBE",
        "PUNSUBSCRIBE",
        "PUBSUB",
        "SSUBSCRIBE",
        "SUNSUBSCRIBE",
        "SPUBLISH",
        // Transaction commands
        "MULTI",
        "EXEC",
        "DISCARD",
        "WATCH",
        "UNWATCH",
        // Scripting commands
        "EVAL",
        "EVALSHA",
        "EVAL_RO",
        "EVALSHA_RO",
        "SCRIPT",
        "FUNCTION",
        "FCALL",
        "FCALL_RO",
        // Cluster commands
        "CLUSTER",
        "READONLY",
        "READWRITE",
        "ASKING",
        // Replication commands
        "REPLICAOF",
        "SLAVEOF",
        "ROLE",
        "PSYNC",
        "REPLCONF",
        "SYNC",
        "FAILOVER",
        // HyperLogLog commands
        "PFADD",
        "PFCOUNT",
        "PFMERGE",
        "PFDEBUG",
        "PFSELFTEST",
        // Bitmap commands
        "SETBIT",
        "GETBIT",
        "BITCOUNT",
        "BITPOS",
        "BITOP",
        "BITFIELD",
        "BITFIELD_RO",
        // Stream commands
        "XADD",
        "XREAD",
        "XREADGROUP",
        "XRANGE",
        "XREVRANGE",
        "XLEN",
        "XINFO",
        "XDEL",
        "XTRIM",
        "XGROUP",
        "XACK",
        "XCLAIM",
        "XAUTOCLAIM",
        "XPENDING",
        "XSETID",
        // Geo commands
        "GEOADD",
        "GEODIST",
        "GEOHASH",
        "GEOPOS",
        "GEORADIUS",
        "GEORADIUSBYMEMBER",
        "GEOSEARCH",
        "GEOSEARCHSTORE",
        // JSON commands
        "JSON.GET",
        "JSON.SET",
        "JSON.DEL",
        "JSON.MGET",
        "JSON.TYPE",
        "JSON.NUMINCRBY",
        "JSON.ARRAPPEND",
        "JSON.ARRINDEX",
        "JSON.ARRINSERT",
        "JSON.ARRLEN",
        "JSON.ARRPOP",
        "JSON.ARRTRIM",
        "JSON.OBJKEYS",
        "JSON.OBJLEN",
        "JSON.STRLEN",
        "JSON.STRAPPEND",
        "JSON.CLEAR",
        "JSON.TOGGLE",
        // TimeSeries commands
        "TS.CREATE",
        "TS.ADD",
        "TS.MADD",
        "TS.INCRBY",
        "TS.DECRBY",
        "TS.CREATERULE",
        "TS.DELETERULE",
        "TS.RANGE",
        "TS.REVRANGE",
        "TS.MRANGE",
        "TS.MREVRANGE",
        "TS.GET",
        "TS.MGET",
        "TS.INFO",
        "TS.QUERYINDEX",
        "TS.ALTER",
        "TS.DEL",
        // Search commands
        "FT.CREATE",
        "FT.SEARCH",
        "FT.AGGREGATE",
        "FT.INFO",
        "FT.DROPINDEX",
        "FT.ALIASADD",
        "FT.ALIASUPDATE",
        "FT.ALIASDEL",
        "FT._LIST",
        "FT.EXPLAIN",
        "FT.EXPLAINCLI",
        "FT.PROFILE",
        "FT.TAGVALS",
        "FT.SUGADD",
        "FT.SUGGET",
        "FT.SUGDEL",
        "FT.SUGLEN",
        "FT.SYNDUMP",
        "FT.SYNUPDATE",
        "FT.SPELLCHECK",
        "FT.DICTADD",
        "FT.DICTDEL",
        "FT.DICTDUMP",
        "FT.CONFIG",
        // Vector commands
        "VADD",
        "VCARD",
        "VDIM",
        "VEMB",
        "VGETATTR",
        "VINFO",
        "VISMEMBER",
        "VLINKS",
        "VRANDMEMBER",
        "VRANGE",
        "VREM",
        "VSETATTR",
        "VSIM",
        // Module command
        "MODULE",
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

// FFI for mimalloc stats
use std::ffi::{CStr, c_void};
use std::os::raw::c_char;

unsafe extern "C" {
    fn mi_stats_print_out(out: extern "C" fn(*const c_char, *mut c_void), arg: *mut c_void);
}

extern "C" fn mi_stats_callback(msg: *const c_char, arg: *mut c_void) {
    unsafe {
        if !msg.is_null() {
            let c_str = CStr::from_ptr(msg);
            if let Ok(str_slice) = c_str.to_str() {
                let buffer = &mut *(arg as *mut String);
                buffer.push_str(str_slice);
            }
        }
    }
}

fn cmd_memory(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("MEMORY"));
    }

    match args[0].to_ascii_uppercase().as_slice() {
        b"DOCTOR" => Ok(RespValue::bulk_string("Sam, I have no memory problems")),
        b"MALLOC-SIZE" => Ok(RespValue::bulk_string(
            "Memory allocator stats not available",
        )),
        b"MALLOC-STATS" => {
            let mut buffer = String::new();
            unsafe {
                mi_stats_print_out(mi_stats_callback, &mut buffer as *mut _ as *mut c_void);
            }
            Ok(RespValue::bulk_string(&buffer))
        }
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
        RespValue::bulk_string("0.0"),
        RespValue::bulk_string("allocator-fragmentation.bytes"),
        RespValue::integer(0),
        RespValue::bulk_string("allocator-rss.ratio"),
        RespValue::bulk_string("0.0"),
        RespValue::bulk_string("allocator-rss.bytes"),
        RespValue::integer(0),
        RespValue::bulk_string("rss-overhead.ratio"),
        RespValue::bulk_string("0.0"),
        RespValue::bulk_string("rss-overhead.bytes"),
        RespValue::integer(0),
        RespValue::bulk_string("fragmentation"),
        RespValue::bulk_string("0.0"),
        RespValue::bulk_string("fragmentation.bytes"),
        RespValue::integer(0),
        RespValue::bulk_string("allocator.name"),
        RespValue::bulk_string("mimalloc"),
        RespValue::bulk_string("allocator.note"),
        RespValue::bulk_string("Use MEMORY MALLOC-STATS for detailed mimalloc stats"),
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

// === CONFIG Command ===

fn cmd_config(server: &Arc<ServerState>, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::Syntax);
    }

    match args[0].to_ascii_uppercase().as_slice() {
        b"GET" => {
            if args.len() < 2 {
                return Err(Error::WrongArity("CONFIG GET"));
            }
            cmd_config_get(server, &args[1])
        }
        b"SET" => {
            if args.len() < 3 {
                return Err(Error::WrongArity("CONFIG SET"));
            }
            cmd_config_set(server, &args[1], &args[2])
        }
        b"RESETSTAT" => Ok(RespValue::ok()),
        // ... regex continue from existing code
        b"REWRITE" => Ok(RespValue::ok()),
        _ => Err(Error::Custom(format!(
            "ERR Unknown subcommand '{}'",
            String::from_utf8_lossy(&args[0])
        ))),
    }
}

fn cmd_config_set(server: &Arc<ServerState>, parameter: &[u8], value: &[u8]) -> Result<RespValue> {
    let param_str = std::str::from_utf8(parameter)
        .map_err(|_| Error::Custom("Invalid parameter name".into()))?;
    let value_str =
        std::str::from_utf8(value).map_err(|_| Error::Custom("Invalid value".into()))?;

    // Find config entry in table
    let entry = find_config(param_str)
        .ok_or_else(|| Error::Custom(format!("ERR Unknown CONFIG parameter: {}", param_str)))?;

    // Check if config is immutable
    if entry.flags.contains(ConfigFlags::IMMUTABLE) {
        return Err(Error::Custom(format!(
            "ERR CONFIG parameter '{}' is immutable",
            param_str
        )));
    }

    // Acquire lock and apply setter
    let mut config = server.config.write();
    (entry.setter)(&mut config, value_str).map_err(|e| Error::Custom(format!("ERR {}", e)))?;

    // Apply runtime changes if applier exists
    if let Some(applier) = entry.applier {
        applier(server, &config);
    }

    Ok(RespValue::ok())
}

fn cmd_config_get(server: &Arc<ServerState>, pattern: &[u8]) -> Result<RespValue> {
    let pattern_str = std::str::from_utf8(pattern).unwrap_or("*");
    let config = server.config.read();
    let mut result = Vec::new();

    // Use CONFIG_TABLE for configs in the table
    for entry in CONFIG_TABLE.iter() {
        if matches_pattern(pattern_str, entry.name) {
            result.push(RespValue::bulk_string(entry.name));
            result.push(RespValue::bulk_string(&(entry.getter)(&config)));
        }
        // Also check alias
        if let Some(alias) = entry.alias
            && matches_pattern(pattern_str, alias)
        {
            result.push(RespValue::bulk_string(alias));
            result.push(RespValue::bulk_string(&(entry.getter)(&config)));
        }
    }

    // Add configs with optional values or special formatting (not in table)
    let mut add_legacy = |name: &str, value: String| {
        if find_config(name).is_none() && matches_pattern(pattern_str, name) {
            result.push(RespValue::bulk_string(name));
            result.push(RespValue::bulk_string(&value));
        }
    };

    // Optional string configs (can't use macros for Option<String>)
    if let Some(v) = &config.unixsocket {
        add_legacy("unixsocket", v.clone());
    }
    if let Some(v) = config.unixsocketperm {
        add_legacy("unixsocketperm", v.to_string());
    }
    if let Some(v) = &config.bind_source_addr {
        add_legacy("bind-source-addr", v.clone());
    }
    if let Some(v) = &config.tls_cert_file {
        add_legacy("tls-cert-file", v.clone());
    }
    if let Some(v) = &config.tls_key_file {
        add_legacy("tls-key-file", v.clone());
    }
    if let Some(v) = &config.tls_key_file_pass {
        add_legacy("tls-key-file-pass", v.clone());
    }
    if let Some(v) = &config.tls_client_cert_file {
        add_legacy("tls-client-cert-file", v.clone());
    }
    if let Some(v) = &config.tls_client_key_file {
        add_legacy("tls-client-key-file", v.clone());
    }
    if let Some(v) = &config.tls_dh_params_file {
        add_legacy("tls-dh-params-file", v.clone());
    }
    if let Some(v) = &config.tls_ca_cert_file {
        add_legacy("tls-ca-cert-file", v.clone());
    }
    if let Some(v) = &config.tls_ca_cert_dir {
        add_legacy("tls-ca-cert-dir", v.clone());
    }
    if let Some(v) = &config.tls_protocols {
        add_legacy("tls-protocols", v.clone());
    }
    if let Some(v) = &config.tls_ciphers {
        add_legacy("tls-ciphers", v.clone());
    }
    if let Some(v) = &config.tls_ciphersuites {
        add_legacy("tls-ciphersuites", v.clone());
    }
    if let Some(v) = &config.requirepass {
        add_legacy("requirepass", v.clone());
    }
    if let Some(v) = &config.masterauth {
        add_legacy("masterauth", v.clone());
    }
    if let Some(v) = &config.masteruser {
        add_legacy("masteruser", v.clone());
    }
    if let Some(v) = &config.aclfile {
        add_legacy("aclfile", v.clone());
    }
    if let Some(v) = &config.server_cpulist {
        add_legacy("server-cpulist", v.clone());
    }
    if let Some(v) = &config.bio_cpulist {
        add_legacy("bio-cpulist", v.clone());
    }
    if let Some(v) = &config.aof_rewrite_cpulist {
        add_legacy("aof-rewrite-cpulist", v.clone());
    }
    if let Some(v) = &config.bgsave_cpulist {
        add_legacy("bgsave-cpulist", v.clone());
    }
    if let Some(v) = &config.ignore_warnings {
        add_legacy("ignore-warnings", v.clone());
    }
    if let Some((host, port)) = &config.replicaof {
        add_legacy("replicaof", format!("{} {}", host, port));
    }
    if let Some(v) = &config.replica_announce_ip {
        add_legacy("replica-announce-ip", v.clone());
    }
    if let Some(v) = config.replica_announce_port {
        add_legacy("replica-announce-port", v.to_string());
    }
    if let Some(v) = &config.cluster_announce_ip {
        add_legacy("cluster-announce-ip", v.clone());
    }
    if let Some(v) = config.cluster_announce_port {
        add_legacy("cluster-announce-port", v.to_string());
    }
    if let Some(v) = config.cluster_announce_bus_port {
        add_legacy("cluster-announce-bus-port", v.to_string());
    }
    if let Some(v) = &config.cluster_announce_hostname {
        add_legacy("cluster-announce-hostname", v.clone());
    }
    // Special formatting for save points
    if matches_pattern(pattern_str, "save") {
        let save_str = config
            .save_points
            .iter()
            .map(|(s, c)| format!("{} {}", s, c))
            .collect::<Vec<_>>()
            .join(" ");
        add_legacy("save", save_str);
    }

    Ok(RespValue::array(result))
}
