//! Connection command handlers
//!
//! All Redis connection commands: AUTH, CLIENT, ECHO, HELLO, PING, QUIT, RESET, SELECT
//!
//! Performance: Zero-copy where possible, lock-free atomics for state management.

use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::client::{ClientState, ClientType, ReplyMode, TrackingState};
use crate::client_manager::{ClientManager, KillFilter};
use crate::error::{Error, Result};
use crate::protocol::RespValue;

/// Number of databases (Redis default)
pub const NUM_DATABASES: u16 = 16;

/// Result of connection command execution
/// Some commands (QUIT) need to signal connection close
pub enum ConnectionResult {
    /// Normal response
    Response(RespValue),
    /// Close connection after sending response
    Quit(RespValue),
    /// Skip response (CLIENT REPLY OFF/SKIP)
    NoReply,
}

/// Execute a connection command
pub fn execute(
    client: &Arc<ClientState>,
    manager: &Arc<ClientManager>,
    cmd: &[u8],
    args: &[Bytes],
) -> Result<ConnectionResult> {
    match cmd.to_ascii_uppercase().as_slice() {
        b"PING" => Ok(ConnectionResult::Response(cmd_ping(args))),
        b"ECHO" => cmd_echo(args).map(ConnectionResult::Response),
        b"QUIT" => Ok(ConnectionResult::Quit(RespValue::ok())),
        b"RESET" => Ok(ConnectionResult::Response(cmd_reset(client))),
        b"SELECT" => cmd_select(client, args).map(ConnectionResult::Response),
        b"AUTH" => cmd_auth(client, manager, args).map(ConnectionResult::Response),
        b"CLIENT" => cmd_client(client, manager, args),
        b"HELLO" => cmd_hello(client, manager, args).map(ConnectionResult::Response),
        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd).into_owned(),
        )),
    }
}

// ==================== Basic Commands ====================

/// PING [message]
#[inline]
fn cmd_ping(args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        RespValue::SimpleString(Bytes::from_static(b"PONG"))
    } else {
        RespValue::bulk(args[0].clone())
    }
}

/// ECHO message
#[inline]
fn cmd_echo(args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("ECHO"));
    }
    Ok(RespValue::bulk(args[0].clone()))
}

/// RESET - Reset connection state
fn cmd_reset(client: &Arc<ClientState>) -> RespValue {
    client.reset();
    RespValue::SimpleString(Bytes::from_static(b"RESET"))
}

/// SELECT index - Select database (0-15)
fn cmd_select(client: &Arc<ClientState>, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("SELECT"));
    }

    let index: u64 = std::str::from_utf8(&args[0])
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)?;

    if index >= NUM_DATABASES as u64 {
        return Err(Error::Custom(format!(
            "ERR DB index is out of range (0-{})",
            NUM_DATABASES - 1
        )));
    }

    client.select_db(index);
    Ok(RespValue::ok())
}

// ==================== Authentication ====================

/// AUTH [username] password
fn cmd_auth(
    client: &Arc<ClientState>,
    manager: &Arc<ClientManager>,
    args: &[Bytes],
) -> Result<RespValue> {
    if args.is_empty() || args.len() > 2 {
        return Err(Error::WrongArity("AUTH"));
    }

    let (username, password) = if args.len() == 1 {
        (None, &args[0])
    } else {
        (Some(&args[0]), &args[1])
    };

    let valid = match username {
        Some(user) => manager.validate_credentials(user, password),
        None => manager.validate_password(password),
    };

    if valid {
        client.set_authenticated(true);
        Ok(RespValue::ok())
    } else {
        client.set_authenticated(false);
        Err(Error::Custom(
            "WRONGPASS invalid username-password pair or user is disabled".to_string(),
        ))
    }
}

// ==================== CLIENT Subcommands ====================

/// CLIENT <subcommand> [args...]
fn cmd_client(
    client: &Arc<ClientState>,
    manager: &Arc<ClientManager>,
    args: &[Bytes],
) -> Result<ConnectionResult> {
    if args.is_empty() {
        return Err(Error::Syntax);
    }

    let subcmd = &args[0];
    let subargs = &args[1..];

    match subcmd.to_ascii_uppercase().as_slice() {
        b"ID" => Ok(ConnectionResult::Response(RespValue::integer(
            client.id as i64,
        ))),

        b"GETNAME" => {
            let name = client.name.read();
            Ok(ConnectionResult::Response(match name.as_ref() {
                Some(n) => RespValue::bulk(n.clone()),
                None => RespValue::null(),
            }))
        }

        b"SETNAME" => {
            if subargs.len() != 1 {
                return Err(Error::WrongArity("CLIENT SETNAME"));
            }
            // Validate name (no spaces)
            if subargs[0].contains(&b' ') {
                return Err(Error::Custom(
                    "ERR Client names cannot contain spaces, newlines or special characters"
                        .to_string(),
                ));
            }
            *client.name.write() = Some(subargs[0].clone());
            Ok(ConnectionResult::Response(RespValue::ok()))
        }

        b"LIST" => {
            let (filter_type, filter_ids) = parse_list_filters(subargs)?;
            let ids_vec: Vec<u64>;
            let filter_ids_ref = match filter_ids {
                Some(ids) => {
                    ids_vec = ids;
                    Some(ids_vec.as_slice())
                }
                None => None,
            };

            let clients = manager.list_clients(filter_type, filter_ids_ref);
            let mut output = String::new();
            for c in clients {
                output.push_str(&c.format_info());
            }
            Ok(ConnectionResult::Response(RespValue::bulk(Bytes::from(
                output,
            ))))
        }

        b"INFO" => Ok(ConnectionResult::Response(RespValue::bulk(Bytes::from(
            client.format_info(),
        )))),

        b"KILL" => cmd_client_kill(client, manager, subargs),

        b"PAUSE" => cmd_client_pause(manager, subargs),

        b"UNPAUSE" => {
            manager.unpause();
            Ok(ConnectionResult::Response(RespValue::ok()))
        }

        b"REPLY" => cmd_client_reply(client, subargs),

        b"SETINFO" => cmd_client_setinfo(client, subargs),

        b"CACHING" => cmd_client_caching(client, subargs),

        b"GETREDIR" => {
            let redir = client
                .tracking
                .redirect_id
                .load(std::sync::atomic::Ordering::Relaxed);
            Ok(ConnectionResult::Response(RespValue::integer(redir as i64)))
        }

        b"TRACKING" => cmd_client_tracking(client, manager, subargs),

        b"TRACKINGINFO" => Ok(ConnectionResult::Response(format_tracking_info(
            &client.tracking,
        ))),

        b"NO-EVICT" => {
            if subargs.len() != 1 {
                return Err(Error::WrongArity("CLIENT NO-EVICT"));
            }
            let on = subargs[0].eq_ignore_ascii_case(b"ON");
            client.flags.no_evict.store(on, Ordering::Relaxed);
            Ok(ConnectionResult::Response(RespValue::ok()))
        }

        b"NO-TOUCH" => {
            if subargs.len() != 1 {
                return Err(Error::WrongArity("CLIENT NO-TOUCH"));
            }
            let on = subargs[0].eq_ignore_ascii_case(b"ON");
            client.flags.no_touch.store(on, Ordering::Relaxed);
            Ok(ConnectionResult::Response(RespValue::ok()))
        }

        b"UNBLOCK" => cmd_client_unblock(subargs),

        _ => Err(Error::Custom(format!(
            "ERR unknown subcommand '{}'. Try CLIENT HELP.",
            String::from_utf8_lossy(subcmd)
        ))),
    }
}

/// Parse CLIENT LIST filters
fn parse_list_filters(args: &[Bytes]) -> Result<(Option<ClientType>, Option<Vec<u64>>)> {
    let mut filter_type = None;
    let mut filter_ids = None;
    let mut i = 0;

    while i < args.len() {
        if args[i].eq_ignore_ascii_case(b"TYPE") {
            if i + 1 >= args.len() {
                return Err(Error::Syntax);
            }
            filter_type = Some(parse_client_type(&args[i + 1])?);
            i += 2;
        } else if args[i].eq_ignore_ascii_case(b"ID") {
            i += 1;
            let mut ids = Vec::new();
            while i < args.len() && !args[i].eq_ignore_ascii_case(b"TYPE") {
                let id: u64 = std::str::from_utf8(&args[i])
                    .map_err(|_| Error::NotInteger)?
                    .parse()
                    .map_err(|_| Error::NotInteger)?;
                ids.push(id);
                i += 1;
            }
            filter_ids = Some(ids);
        } else {
            return Err(Error::Syntax);
        }
    }

    Ok((filter_type, filter_ids))
}

/// Parse client type string
fn parse_client_type(s: &[u8]) -> Result<ClientType> {
    match s.to_ascii_uppercase().as_slice() {
        b"NORMAL" => Ok(ClientType::Normal),
        b"MASTER" => Ok(ClientType::Master),
        b"SLAVE" | b"REPLICA" => Ok(ClientType::Replica),
        b"PUBSUB" => Ok(ClientType::PubSub),
        _ => Err(Error::Custom(format!(
            "ERR Unknown client type '{}'",
            String::from_utf8_lossy(s)
        ))),
    }
}

/// CLIENT KILL [filters...]
fn cmd_client_kill(
    client: &Arc<ClientState>,
    manager: &Arc<ClientManager>,
    args: &[Bytes],
) -> Result<ConnectionResult> {
    if args.is_empty() {
        return Err(Error::WrongArity("CLIENT KILL"));
    }

    // Old syntax: CLIENT KILL ip:port
    if args.len() == 1 && !args[0].iter().any(|&b| b.is_ascii_alphabetic()) {
        // Check if it looks like an address
        let addr_str = std::str::from_utf8(&args[0]).map_err(|_| Error::Syntax)?;
        if let Ok(addr) = addr_str.parse() {
            let filter = KillFilter {
                addr: Some(addr),
                skipme: true,
                ..Default::default()
            };
            let killed = manager.kill_clients(&filter, client.id);
            return Ok(ConnectionResult::Response(RespValue::integer(
                killed as i64,
            )));
        }
    }

    // New syntax: CLIENT KILL [filter options...]
    let filter = parse_kill_filter(args)?;
    let killed = manager.kill_clients(&filter, client.id);
    Ok(ConnectionResult::Response(RespValue::integer(
        killed as i64,
    )))
}

/// Parse CLIENT KILL filter options
fn parse_kill_filter(args: &[Bytes]) -> Result<KillFilter> {
    let mut filter = KillFilter {
        skipme: true, // Default to SKIPME YES
        ..Default::default()
    };
    let mut i = 0;

    while i < args.len() {
        let opt = args[i].to_ascii_uppercase();
        match opt.as_slice() {
            b"ID" => {
                if i + 1 >= args.len() {
                    return Err(Error::Syntax);
                }
                filter.id = Some(
                    std::str::from_utf8(&args[i + 1])
                        .map_err(|_| Error::NotInteger)?
                        .parse()
                        .map_err(|_| Error::NotInteger)?,
                );
                i += 2;
            }
            b"TYPE" => {
                if i + 1 >= args.len() {
                    return Err(Error::Syntax);
                }
                filter.client_type = Some(parse_client_type(&args[i + 1])?);
                i += 2;
            }
            b"ADDR" => {
                if i + 1 >= args.len() {
                    return Err(Error::Syntax);
                }
                let addr_str = std::str::from_utf8(&args[i + 1]).map_err(|_| Error::Syntax)?;
                filter.addr = Some(addr_str.parse().map_err(|_| Error::Syntax)?);
                i += 2;
            }
            b"LADDR" => {
                if i + 1 >= args.len() {
                    return Err(Error::Syntax);
                }
                let addr_str = std::str::from_utf8(&args[i + 1]).map_err(|_| Error::Syntax)?;
                filter.laddr = Some(addr_str.parse().map_err(|_| Error::Syntax)?);
                i += 2;
            }
            b"USER" => {
                if i + 1 >= args.len() {
                    return Err(Error::Syntax);
                }
                filter.user = Some(args[i + 1].clone());
                i += 2;
            }
            b"SKIPME" => {
                if i + 1 >= args.len() {
                    return Err(Error::Syntax);
                }
                filter.skipme = args[i + 1].eq_ignore_ascii_case(b"YES");
                i += 2;
            }
            b"MAXAGE" => {
                if i + 1 >= args.len() {
                    return Err(Error::Syntax);
                }
                filter.maxage = Some(
                    std::str::from_utf8(&args[i + 1])
                        .map_err(|_| Error::NotInteger)?
                        .parse()
                        .map_err(|_| Error::NotInteger)?,
                );
                i += 2;
            }
            _ => {
                // Old syntax fallback - might be ip:port
                let addr_str = std::str::from_utf8(&args[i]).map_err(|_| Error::Syntax)?;
                if let Ok(addr) = addr_str.parse() {
                    filter.addr = Some(addr);
                    i += 1;
                } else {
                    return Err(Error::Syntax);
                }
            }
        }
    }

    Ok(filter)
}

/// CLIENT PAUSE timeout [WRITE | ALL]
fn cmd_client_pause(manager: &Arc<ClientManager>, args: &[Bytes]) -> Result<ConnectionResult> {
    if args.is_empty() {
        return Err(Error::WrongArity("CLIENT PAUSE"));
    }

    let timeout: u64 = std::str::from_utf8(&args[0])
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)?;

    let write_only = if args.len() > 1 {
        args[1].eq_ignore_ascii_case(b"WRITE")
    } else {
        false
    };

    manager.pause(timeout, write_only);
    Ok(ConnectionResult::Response(RespValue::ok()))
}

/// CLIENT REPLY ON|OFF|SKIP
fn cmd_client_reply(client: &Arc<ClientState>, args: &[Bytes]) -> Result<ConnectionResult> {
    if args.len() != 1 {
        return Err(Error::WrongArity("CLIENT REPLY"));
    }

    match args[0].to_ascii_uppercase().as_slice() {
        b"ON" => {
            client.set_reply_mode(ReplyMode::On);
            Ok(ConnectionResult::Response(RespValue::ok()))
        }
        b"OFF" => {
            client.set_reply_mode(ReplyMode::Off);
            Ok(ConnectionResult::NoReply)
        }
        b"SKIP" => {
            client.flags.skip_reply.store(true, Ordering::Relaxed);
            Ok(ConnectionResult::NoReply)
        }
        _ => Err(Error::Syntax),
    }
}

/// CLIENT SETINFO <LIB-NAME libname | LIB-VER libver>
fn cmd_client_setinfo(client: &Arc<ClientState>, args: &[Bytes]) -> Result<ConnectionResult> {
    if args.len() != 2 {
        return Err(Error::WrongArity("CLIENT SETINFO"));
    }

    match args[0].to_ascii_uppercase().as_slice() {
        b"LIB-NAME" => {
            *client.lib_name.write() = Some(args[1].clone());
            Ok(ConnectionResult::Response(RespValue::ok()))
        }
        b"LIB-VER" => {
            *client.lib_ver.write() = Some(args[1].clone());
            Ok(ConnectionResult::Response(RespValue::ok()))
        }
        _ => Err(Error::Custom(format!(
            "ERR Unknown argument '{}' for CLIENT SETINFO",
            String::from_utf8_lossy(&args[0])
        ))),
    }
}

/// CLIENT CACHING YES|NO
fn cmd_client_caching(client: &Arc<ClientState>, args: &[Bytes]) -> Result<ConnectionResult> {
    if args.len() != 1 {
        return Err(Error::WrongArity("CLIENT CACHING"));
    }

    if !client.tracking.enabled.load(Ordering::Relaxed) {
        return Err(Error::Custom(
            "ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled".to_string(),
        ));
    }

    let yes = args[0].eq_ignore_ascii_case(b"YES");
    client.tracking.caching_next.store(yes, Ordering::Relaxed);
    Ok(ConnectionResult::Response(RespValue::ok()))
}

/// CLIENT TRACKING ON|OFF [options...]
fn cmd_client_tracking(
    client: &Arc<ClientState>,
    manager: &Arc<ClientManager>,
    args: &[Bytes],
) -> Result<ConnectionResult> {
    if args.is_empty() {
        return Err(Error::WrongArity("CLIENT TRACKING"));
    }

    let on = args[0].eq_ignore_ascii_case(b"ON");

    if !on {
        // Turn off tracking
        client.tracking.reset();
        return Ok(ConnectionResult::Response(RespValue::ok()));
    }

    // Parse tracking options
    let mut redirect_id: Option<u64> = None;
    let mut bcast = false;
    let mut optin = false;
    let mut optout = false;
    let mut noloop = false;
    let mut prefixes: Vec<Bytes> = Vec::new();

    let mut i = 1;
    while i < args.len() {
        match args[i].to_ascii_uppercase().as_slice() {
            b"REDIRECT" => {
                if i + 1 >= args.len() {
                    return Err(Error::Syntax);
                }
                redirect_id = Some(
                    std::str::from_utf8(&args[i + 1])
                        .map_err(|_| Error::NotInteger)?
                        .parse()
                        .map_err(|_| Error::NotInteger)?,
                );
                i += 2;
            }
            b"PREFIX" => {
                if i + 1 >= args.len() {
                    return Err(Error::Syntax);
                }
                prefixes.push(args[i + 1].clone());
                i += 2;
            }
            b"BCAST" => {
                bcast = true;
                i += 1;
            }
            b"OPTIN" => {
                optin = true;
                i += 1;
            }
            b"OPTOUT" => {
                optout = true;
                i += 1;
            }
            b"NOLOOP" => {
                noloop = true;
                i += 1;
            }
            _ => return Err(Error::Syntax),
        }
    }

    // Validate options
    if optin && optout {
        return Err(Error::Custom(
            "ERR You can't use both OPTIN and OPTOUT options".to_string(),
        ));
    }

    // Validate redirect client exists
    if let Some(redir_id) = redirect_id
        && manager.get_client(redir_id).is_none()
    {
        return Err(Error::Custom(format!(
            "ERR The client ID {} does not exist",
            redir_id
        )));
    }

    // Apply settings
    client.tracking.enabled.store(true, Ordering::Relaxed);
    client
        .tracking
        .redirect_id
        .store(redirect_id.unwrap_or(0), Ordering::Relaxed);
    client.tracking.bcast.store(bcast, Ordering::Relaxed);
    client.tracking.optin.store(optin, Ordering::Relaxed);
    client.tracking.optout.store(optout, Ordering::Relaxed);
    client.tracking.noloop.store(noloop, Ordering::Relaxed);
    *client.tracking.prefixes.write() = prefixes;

    Ok(ConnectionResult::Response(RespValue::ok()))
}

/// Format CLIENT TRACKINGINFO response
fn format_tracking_info(tracking: &TrackingState) -> RespValue {
    let mut result = Vec::new();

    result.push(RespValue::bulk_string("flags"));
    let mut flags = Vec::new();
    if tracking.enabled.load(Ordering::Relaxed) {
        flags.push(RespValue::bulk_string("on"));
    } else {
        flags.push(RespValue::bulk_string("off"));
    }
    if tracking.bcast.load(Ordering::Relaxed) {
        flags.push(RespValue::bulk_string("bcast"));
    }
    if tracking.optin.load(Ordering::Relaxed) {
        flags.push(RespValue::bulk_string("optin"));
    }
    if tracking.optout.load(Ordering::Relaxed) {
        flags.push(RespValue::bulk_string("optout"));
    }
    if tracking.noloop.load(Ordering::Relaxed) {
        flags.push(RespValue::bulk_string("noloop"));
    }
    result.push(RespValue::array(flags));

    result.push(RespValue::bulk_string("redirect"));
    result.push(RespValue::integer(
        tracking.redirect_id.load(Ordering::Relaxed) as i64,
    ));

    result.push(RespValue::bulk_string("prefixes"));
    let prefixes = tracking.prefixes.read();
    let prefix_arr: Vec<RespValue> = prefixes
        .iter()
        .map(|p| RespValue::bulk(p.clone()))
        .collect();
    result.push(RespValue::array(prefix_arr));

    RespValue::array(result)
}

/// CLIENT UNBLOCK client-id [TIMEOUT|ERROR]
fn cmd_client_unblock(args: &[Bytes]) -> Result<ConnectionResult> {
    if args.is_empty() {
        return Err(Error::WrongArity("CLIENT UNBLOCK"));
    }

    let _client_id: u64 = std::str::from_utf8(&args[0])
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)?;

    // TODO: Implement actual unblocking when we have blocking commands
    // For now, return 0 (no client was unblocked)
    Ok(ConnectionResult::Response(RespValue::integer(0)))
}

// ==================== HELLO Command ====================

/// HELLO [protover [AUTH username password] [SETNAME clientname]]
fn cmd_hello(
    client: &Arc<ClientState>,
    manager: &Arc<ClientManager>,
    args: &[Bytes],
) -> Result<RespValue> {
    let mut protover: i64 = 2; // Default to RESP2

    let mut i = 0;

    // Parse protocol version
    if !args.is_empty() {
        protover = std::str::from_utf8(&args[0])
            .map_err(|_| Error::NotInteger)?
            .parse()
            .map_err(|_| Error::NotInteger)?;

        if !(2..=3).contains(&protover) {
            return Err(Error::Custom(
                "NOPROTO sorry this protocol version is not supported".to_string(),
            ));
        }
        i = 1;
    }

    // Parse optional arguments
    while i < args.len() {
        match args[i].to_ascii_uppercase().as_slice() {
            b"AUTH" => {
                if i + 2 >= args.len() {
                    return Err(Error::Syntax);
                }
                let username = &args[i + 1];
                let password = &args[i + 2];

                if !manager.validate_credentials(username, password) {
                    return Err(Error::Custom(
                        "WRONGPASS invalid username-password pair".to_string(),
                    ));
                }
                client.set_authenticated(true);
                i += 3;
            }
            b"SETNAME" => {
                if i + 1 >= args.len() {
                    return Err(Error::Syntax);
                }
                *client.name.write() = Some(args[i + 1].clone());
                i += 2;
            }
            _ => return Err(Error::Syntax),
        }
    }

    // Build HELLO response
    let mut response = Vec::new();

    response.push(RespValue::bulk_string("server"));
    response.push(RespValue::bulk_string("sockudo-kv"));

    response.push(RespValue::bulk_string("version"));
    response.push(RespValue::bulk_string("7.0.0"));

    response.push(RespValue::bulk_string("proto"));
    response.push(RespValue::integer(protover));

    response.push(RespValue::bulk_string("id"));
    response.push(RespValue::integer(client.id as i64));

    response.push(RespValue::bulk_string("mode"));
    response.push(RespValue::bulk_string("standalone"));

    response.push(RespValue::bulk_string("role"));
    response.push(RespValue::bulk_string("master"));

    response.push(RespValue::bulk_string("modules"));
    response.push(RespValue::array(vec![]));

    Ok(RespValue::array(response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn make_test_client() -> (Arc<ClientState>, Arc<ClientManager>) {
        let manager = Arc::new(ClientManager::new());
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 12345);
        let client = manager.register(addr, 1);
        (client, manager)
    }

    #[test]
    fn test_ping() {
        let (client, manager) = make_test_client();
        let result = execute(&client, &manager, b"PING", &[]).unwrap();
        match result {
            ConnectionResult::Response(r) => {
                assert!(matches!(r, RespValue::SimpleString(_)));
            }
            _ => panic!("Expected response"),
        }
    }

    #[test]
    fn test_echo() {
        let (client, manager) = make_test_client();
        let result = execute(&client, &manager, b"ECHO", &[Bytes::from_static(b"hello")]).unwrap();
        match result {
            ConnectionResult::Response(r) => {
                assert!(matches!(r, RespValue::BulkString(_)));
            }
            _ => panic!("Expected response"),
        }
    }

    #[test]
    fn test_select() {
        let (client, manager) = make_test_client();
        execute(&client, &manager, b"SELECT", &[Bytes::from_static(b"5")]).unwrap();
        assert_eq!(client.db(), 5);

        // Out of range
        let err = execute(&client, &manager, b"SELECT", &[Bytes::from_static(b"20")]);
        assert!(err.is_err());
    }

    #[test]
    fn test_client_id() {
        let (client, manager) = make_test_client();
        let result = execute(&client, &manager, b"CLIENT", &[Bytes::from_static(b"ID")]).unwrap();
        match result {
            ConnectionResult::Response(RespValue::Integer(id)) => {
                assert_eq!(id, client.id as i64);
            }
            _ => panic!("Expected integer"),
        }
    }

    #[test]
    fn test_client_setname_getname() {
        let (client, manager) = make_test_client();

        execute(
            &client,
            &manager,
            b"CLIENT",
            &[Bytes::from_static(b"SETNAME"), Bytes::from_static(b"test")],
        )
        .unwrap();

        let result = execute(
            &client,
            &manager,
            b"CLIENT",
            &[Bytes::from_static(b"GETNAME")],
        )
        .unwrap();
        match result {
            ConnectionResult::Response(RespValue::BulkString(name)) => {
                assert_eq!(name.as_ref(), b"test");
            }
            _ => panic!("Expected bulk string"),
        }
    }
}
