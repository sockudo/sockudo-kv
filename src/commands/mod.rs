pub mod bitmap;
pub mod cluster;
pub mod connection;
pub mod generic;
pub mod geo;
pub mod hash;
pub mod hyperloglog;
pub mod json;
pub mod list;
pub mod pubsub;
pub mod scripting;
pub mod search;
pub mod server;
pub mod set;
pub mod sorted_set;
pub mod stream;
pub mod string;
pub mod timeseries;
pub mod transaction;
pub mod vector;

use bytes::Bytes;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::protocol::{Command, RespValue};
use crate::server_state::ServerState;
use crate::storage::Store;

/// Command dispatcher - routes commands to appropriate handlers
pub struct Dispatcher;

impl Dispatcher {
    /// Execute a command with full server state (for external commands)
    #[inline]
    pub fn execute(store: &Store, server: &Arc<ServerState>, cmd: Command) -> RespValue {
        match Self::execute_inner(store, Some(server), &cmd) {
            Ok(resp) => resp,
            Err(e) => RespValue::error(&e.to_string()),
        }
    }

    /// Execute a command without server state (for Lua scripts and transactions)
    /// Server-specific commands will return an error in this mode
    #[inline]
    pub fn execute_basic(store: &Store, cmd: Command) -> RespValue {
        match Self::execute_inner(store, None, &cmd) {
            Ok(resp) => resp,
            Err(e) => RespValue::error(&e.to_string()),
        }
    }

    fn execute_inner(
        store: &Store,
        server: Option<&Arc<ServerState>>,
        cmd: &Command,
    ) -> Result<RespValue> {
        let args = &cmd.args;
        let cmd_name = cmd.name();

        // Server commands - need ServerState
        if is_server_command(cmd_name) {
            return match server {
                Some(srv) => server::execute(store, srv, cmd_name, args),
                None => Err(Error::Custom(
                    "ERR server commands not available in this context".to_string(),
                )),
            };
        }

        // Config commands
        if cmd.is_command(b"CONFIG") {
            return cmd_config(args);
        }
        if cmd.is_command(b"INFO") {
            return Ok(cmd_info(args));
        }

        // Generic commands
        if cmd.is_command(b"DEL") {
            return cmd_del(store, args);
        }
        if cmd.is_command(b"EXISTS") {
            return cmd_exists(store, args);
        }
        if cmd.is_command(b"EXPIRE") {
            return cmd_expire(store, args);
        }
        if cmd.is_command(b"PEXPIRE") {
            return cmd_pexpire(store, args);
        }
        if cmd.is_command(b"TTL") {
            return cmd_ttl(store, args);
        }
        if cmd.is_command(b"PTTL") {
            return cmd_pttl(store, args);
        }
        if cmd.is_command(b"PERSIST") {
            return cmd_persist(store, args);
        }
        if cmd.is_command(b"TYPE") {
            return cmd_type(store, args);
        }
        if cmd.is_command(b"DBSIZE") {
            return Ok(RespValue::integer(store.len() as i64));
        }
        if cmd.is_command(b"FLUSHDB") || cmd.is_command(b"FLUSHALL") {
            store.flush();
            return Ok(RespValue::ok());
        }

        // New generic commands (COPY, DUMP, RESTORE, KEYS, SCAN, RENAME, etc.)
        match generic::execute(store, cmd_name, args) {
            Ok(resp) => return Ok(resp),
            Err(Error::UnknownCommand(_)) => {}
            Err(e) => return Err(e),
        }

        // Try each command module
        // Only continue to next module if it returns UnknownCommand
        // Other errors (WrongArity, Syntax, etc.) should be returned immediately
        let cmd_name = cmd.name();

        match string::execute(store, cmd_name, args) {
            Ok(resp) => return Ok(resp),
            Err(Error::UnknownCommand(_)) => {}
            Err(e) => return Err(e),
        }
        match list::execute(store, cmd_name, args) {
            Ok(resp) => return Ok(resp),
            Err(Error::UnknownCommand(_)) => {}
            Err(e) => return Err(e),
        }
        match hash::execute(store, cmd_name, args) {
            Ok(resp) => return Ok(resp),
            Err(Error::UnknownCommand(_)) => {}
            Err(e) => return Err(e),
        }
        match set::execute(store, cmd_name, args) {
            Ok(resp) => return Ok(resp),
            Err(Error::UnknownCommand(_)) => {}
            Err(e) => return Err(e),
        }
        match sorted_set::execute(store, cmd_name, args) {
            Ok(resp) => return Ok(resp),
            Err(Error::UnknownCommand(_)) => {}
            Err(e) => return Err(e),
        }
        match hyperloglog::execute(store, cmd_name, args) {
            Ok(resp) => return Ok(resp),
            Err(Error::UnknownCommand(_)) => {}
            Err(e) => return Err(e),
        }

        // Bitmap commands (SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP, BITFIELD, BITFIELD_RO)
        if cmd_name.len() >= 3 && cmd_name[..3].eq_ignore_ascii_case(b"BIT")
            || cmd_name.eq_ignore_ascii_case(b"SETBIT")
            || cmd_name.eq_ignore_ascii_case(b"GETBIT")
        {
            return bitmap::execute(store, cmd_name, args);
        }

        // JSON commands (check if starts with JSON.)
        if cmd_name.len() > 5 && cmd_name[..5].eq_ignore_ascii_case(b"JSON.") {
            return json::execute(store, cmd_name, args);
        }

        // TimeSeries commands (check if starts with TS.)
        if cmd_name.len() > 3 && cmd_name[..3].eq_ignore_ascii_case(b"TS.") {
            return timeseries::execute(store, cmd_name, args);
        }

        // Search commands (check if starts with FT.)
        if cmd_name.len() > 3 && cmd_name[..3].eq_ignore_ascii_case(b"FT.") {
            return search::execute(store, cmd_name, args);
        }

        // Geo commands (check if starts with GEO)
        if cmd_name.len() >= 3 && cmd_name[..3].eq_ignore_ascii_case(b"GEO") {
            return geo::execute(store, cmd_name, args);
        }

        // Stream commands (check if starts with X)
        if !cmd_name.is_empty() && (cmd_name[0] == b'X' || cmd_name[0] == b'x') {
            return stream::execute(store, cmd_name, args);
        }

        // Scripting commands (EVAL, EVALSHA, SCRIPT, FUNCTION, FCALL)
        if cmd_name.eq_ignore_ascii_case(b"EVAL")
            || cmd_name.eq_ignore_ascii_case(b"EVAL_RO")
            || cmd_name.eq_ignore_ascii_case(b"EVALSHA")
            || cmd_name.eq_ignore_ascii_case(b"EVALSHA_RO")
            || cmd_name.eq_ignore_ascii_case(b"SCRIPT")
            || cmd_name.eq_ignore_ascii_case(b"FUNCTION")
            || cmd_name.eq_ignore_ascii_case(b"FCALL")
            || cmd_name.eq_ignore_ascii_case(b"FCALL_RO")
        {
            return scripting::execute(store, cmd_name, args);
        }

        // Vector commands (VADD, VCARD, VDIM, VEMB, VGETATTR, VINFO, VISMEMBER, VLINKS, VRANDMEMBER, VRANGE, VREM, VSETATTR, VSIM)
        if !cmd_name.is_empty() && (cmd_name[0] == b'V' || cmd_name[0] == b'v') {
            match vector::execute(store, cmd_name, args) {
                Ok(resp) => return Ok(resp),
                Err(Error::UnknownCommand(_)) => {}
                Err(e) => return Err(e),
            }
        }

        Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd_name).into_owned(),
        ))
    }
}

fn cmd_del(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("DEL"));
    }
    let count: i64 = args.iter().map(|k| if store.del(k) { 1 } else { 0 }).sum();
    Ok(RespValue::integer(count))
}

fn cmd_exists(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("EXISTS"));
    }
    let count: i64 = args
        .iter()
        .map(|k| if store.exists(k) { 1 } else { 0 })
        .sum();
    Ok(RespValue::integer(count))
}

fn cmd_expire(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("EXPIRE"));
    }
    let seconds: i64 = std::str::from_utf8(&args[1])
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)?;
    let result = store.expire(&args[0], seconds * 1000);
    Ok(RespValue::integer(if result { 1 } else { 0 }))
}

fn cmd_pexpire(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("PEXPIRE"));
    }
    let ms: i64 = std::str::from_utf8(&args[1])
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)?;
    let result = store.expire(&args[0], ms);
    Ok(RespValue::integer(if result { 1 } else { 0 }))
}

fn cmd_ttl(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("TTL"));
    }
    let pttl = store.pttl(&args[0]);
    let ttl = if pttl >= 0 { pttl / 1000 } else { pttl };
    Ok(RespValue::integer(ttl))
}

fn cmd_pttl(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("PTTL"));
    }
    Ok(RespValue::integer(store.pttl(&args[0])))
}

fn cmd_persist(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("PERSIST"));
    }
    let result = store.persist(&args[0]);
    Ok(RespValue::integer(if result { 1 } else { 0 }))
}

fn cmd_type(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("TYPE"));
    }
    let type_str = store.key_type(&args[0]).unwrap_or("none");
    Ok(RespValue::SimpleString(Bytes::copy_from_slice(
        type_str.as_bytes(),
    )))
}

fn cmd_config(args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::Syntax);
    }

    let subcommand = &args[0];

    // Case-insensitive comparison for subcommand
    if subcommand.len() == 3 && subcommand.eq_ignore_ascii_case(b"GET") {
        // CONFIG GET - return empty array or default values
        if args.len() < 2 {
            return Err(Error::WrongArity("CONFIG GET"));
        }
        // For redis-benchmark compatibility, return some common configs
        let param = &args[1];
        if param.as_ref() == b"save" || param.as_ref() == b"*save*" {
            return Ok(RespValue::array(vec![
                RespValue::bulk(Bytes::from_static(b"save")),
                RespValue::bulk(Bytes::from_static(b"")),
            ]));
        }
        if param.as_ref() == b"appendonly" || param.as_ref() == b"*append*" {
            return Ok(RespValue::array(vec![
                RespValue::bulk(Bytes::from_static(b"appendonly")),
                RespValue::bulk(Bytes::from_static(b"no")),
            ]));
        }
        // For any other parameter, return empty array
        return Ok(RespValue::array(vec![]));
    } else if subcommand.len() == 3 && subcommand.eq_ignore_ascii_case(b"SET") {
        // CONFIG SET - accept but ignore
        if args.len() < 3 {
            return Err(Error::WrongArity("CONFIG SET"));
        }
        return Ok(RespValue::ok());
    } else if subcommand.eq_ignore_ascii_case(b"RESETSTAT") {
        return Ok(RespValue::ok());
    } else if subcommand.eq_ignore_ascii_case(b"REWRITE") {
        return Ok(RespValue::ok());
    }

    Err(Error::Syntax)
}

fn cmd_info(args: &[Bytes]) -> RespValue {
    // Return minimal INFO response for redis-benchmark compatibility
    let section = if args.is_empty() {
        b"all"
    } else {
        args[0].as_ref()
    };

    // Return basic info string
    let info = if section.eq_ignore_ascii_case(b"server") || section.eq_ignore_ascii_case(b"all") {
        "# Server\r\nredis_version:7.0.0\r\nredis_mode:standalone\r\nos:Linux\r\narch_bits:64\r\n"
    } else if section.eq_ignore_ascii_case(b"replication") {
        "# Replication\r\nrole:master\r\nconnected_slaves:0\r\n"
    } else if section.eq_ignore_ascii_case(b"stats") {
        "# Stats\r\ntotal_connections_received:0\r\ntotal_commands_processed:0\r\n"
    } else {
        ""
    };

    RespValue::bulk(Bytes::copy_from_slice(info.as_bytes()))
}

/// Check if command is a server management command
#[inline]
fn is_server_command(cmd: &[u8]) -> bool {
    cmd.eq_ignore_ascii_case(b"ACL")
        || cmd.eq_ignore_ascii_case(b"COMMAND")
        || cmd.eq_ignore_ascii_case(b"MEMORY")
        || cmd.eq_ignore_ascii_case(b"LATENCY")
        || cmd.eq_ignore_ascii_case(b"SLOWLOG")
        || cmd.eq_ignore_ascii_case(b"BGREWRITEAOF")
        || cmd.eq_ignore_ascii_case(b"BGSAVE")
        || cmd.eq_ignore_ascii_case(b"LASTSAVE")
        || cmd.eq_ignore_ascii_case(b"LOLWUT")
        || cmd.eq_ignore_ascii_case(b"SAVE")
        || cmd.eq_ignore_ascii_case(b"SHUTDOWN")
        || cmd.eq_ignore_ascii_case(b"TIME")
        || cmd.eq_ignore_ascii_case(b"FAILOVER")
        || cmd.eq_ignore_ascii_case(b"PSYNC")
        || cmd.eq_ignore_ascii_case(b"REPLCONF")
        || cmd.eq_ignore_ascii_case(b"REPLICAOF")
        || cmd.eq_ignore_ascii_case(b"ROLE")
        || cmd.eq_ignore_ascii_case(b"SLAVEOF")
        || cmd.eq_ignore_ascii_case(b"SYNC")
        || cmd.eq_ignore_ascii_case(b"MODULE")
        || cmd.eq_ignore_ascii_case(b"SWAPDB")
}
