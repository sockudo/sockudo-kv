use bytes::Bytes;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::replication::ReplicationManager;
use crate::server_state::ServerState;
use crate::storage::Store;

pub fn execute(
    store: &Store,
    cmd: &[u8],
    args: &[Bytes],
    server: Option<&Arc<ServerState>>,
    replication: Option<&Arc<ReplicationManager>>,
) -> Result<RespValue> {
    match cmd {
        b"SADD" => cmd_sadd(store, args),
        b"SREM" => cmd_srem(store, args),
        b"SISMEMBER" => cmd_sismember(store, args),
        b"SMISMEMBER" => cmd_smismember(store, args),
        b"SMEMBERS" => cmd_smembers(store, args),
        b"SCARD" => cmd_scard(store, args),
        b"SPOP" => cmd_spop(store, args, server, replication),
        b"SRANDMEMBER" => cmd_srandmember(store, args),
        b"SDIFF" => cmd_sdiff(store, args),
        b"SINTER" => cmd_sinter(store, args),
        b"SINTERCARD" => cmd_sintercard(store, args),
        b"SUNION" => cmd_sunion(store, args),
        b"SDIFFSTORE" => cmd_sdiffstore(store, args),
        b"SINTERSTORE" => cmd_sinterstore(store, args),
        b"SUNIONSTORE" => cmd_sunionstore(store, args),
        b"SMOVE" => cmd_smove(store, args),
        b"SSCAN" => cmd_sscan(store, args),
        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd).into_owned(),
        )),
    }
}

#[inline]
fn resp_array<I>(iter: I) -> RespValue
where
    I: IntoIterator<Item = Bytes>,
{
    RespValue::array(iter.into_iter().map(RespValue::bulk).collect())
}

/// SADD key member [member ...]
fn cmd_sadd(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("SADD"));
    }
    let key = args[0].clone();
    let members: Vec<Bytes> = args[1..].to_vec();
    let added = store.sadd(key, members)?;
    Ok(RespValue::integer(added as i64))
}

/// SREM key member [member ...]
fn cmd_srem(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("SREM"));
    }
    let removed = store.srem(&args[0], &args[1..]);
    Ok(RespValue::integer(removed as i64))
}

/// SISMEMBER key member
fn cmd_sismember(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 2 {
        return Err(Error::WrongArity("SISMEMBER"));
    }
    let is_member = store.sismember(&args[0], &args[1])?;
    Ok(RespValue::integer(if is_member { 1 } else { 0 }))
}

/// SMISMEMBER key member [member ...]
fn cmd_smismember(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("SMISMEMBER"));
    }
    // Check if key exists and is not a set type - return WRONGTYPE
    store.sismember(&args[0], &args[1])?;

    let mut results = Vec::with_capacity(args.len() - 1);
    for m in &args[1..] {
        let is_member = store.sismember(&args[0], m)?;
        results.push(RespValue::integer(if is_member { 1 } else { 0 }));
    }
    Ok(RespValue::array(results))
}

/// SMEMBERS key
fn cmd_smembers(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("SMEMBERS"));
    }
    Ok(resp_array(store.smembers(&args[0])?))
}

/// SCARD key
fn cmd_scard(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("SCARD"));
    }
    Ok(RespValue::integer(store.scard(&args[0])? as i64))
}

/// SPOP key [count]
fn cmd_spop(
    store: &Store,
    args: &[Bytes],
    server: Option<&Arc<ServerState>>,
    replication: Option<&Arc<ReplicationManager>>,
) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("SPOP"));
    }

    let count = if args.len() > 1 {
        let c = parse_int(&args[1])?;
        if c < 0 {
            return Err(Error::Other("count must be positive"));
        }
        c as usize
    } else {
        1
    };

    let (result, key_deleted) = store.spop(&args[0], count);

    // Handle replication: if key was deleted, propagate as DEL or UNLINK
    if let Some(repl) = replication {
        if key_deleted {
            // Check lazyfree-lazy-server-del config
            let use_unlink = server
                .map(|s| {
                    s.lazyfree_lazy_server_del
                        .load(std::sync::atomic::Ordering::Relaxed)
                })
                .unwrap_or(false);

            if use_unlink {
                let parts = vec![Bytes::from_static(b"UNLINK"), args[0].clone()];
                repl.propagate(&parts);
            } else {
                let parts = vec![Bytes::from_static(b"DEL"), args[0].clone()];
                repl.propagate(&parts);
            }
        } else if !result.is_empty() {
            // Normal propagation - propagate actual SPOP command
            let mut parts = Vec::with_capacity(3);
            parts.push(Bytes::from_static(b"SPOP"));
            parts.push(args[0].clone());
            parts.push(Bytes::from(count.to_string()));
            repl.propagate(&parts);
        }
    }

    if args.len() == 1 {
        if let Some(member) = result.into_iter().next() {
            Ok(RespValue::bulk(member))
        } else {
            Ok(RespValue::null())
        }
    } else {
        Ok(resp_array(result))
    }
}

/// SRANDMEMBER key [count]
fn cmd_srandmember(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("SRANDMEMBER"));
    }

    if args.len() == 1 {
        let result = store.srandmember(&args[0], 1);
        if let Some(member) = result.into_iter().next() {
            Ok(RespValue::bulk(member))
        } else {
            Ok(RespValue::null())
        }
    } else {
        let count = parse_int(&args[1])?;
        // i64::MIN cannot be safely negated (unsigned_abs overflow)
        if count == i64::MIN {
            return Err(Error::Other("value is out of range"));
        }
        Ok(resp_array(store.srandmember(&args[0], count)))
    }
}

#[inline]
fn parse_int(b: &[u8]) -> Result<i64> {
    std::str::from_utf8(b)
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)
}

/// SDIFF key [key ...]
fn cmd_sdiff(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("SDIFF"));
    }
    Ok(resp_array(store.sdiff(args)?))
}

/// SINTER key [key ...]
fn cmd_sinter(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("SINTER"));
    }
    Ok(resp_array(store.sinter(args)?))
}

/// SINTERCARD numkeys key [key ...] [LIMIT limit]
fn cmd_sintercard(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("SINTERCARD"));
    }

    let numkeys: usize = std::str::from_utf8(&args[0])
        .map_err(|_| Error::Other("numkeys should be greater than 0"))?
        .parse()
        .map_err(|_| Error::Other("numkeys should be greater than 0"))?;

    if numkeys == 0 {
        return Err(Error::Other("numkeys should be greater than 0"));
    }

    if args.len() == 1 {
        return Err(Error::WrongArity("SINTERCARD"));
    }

    if args.len() < 1 + numkeys {
        return Err(Error::Other(
            "Number of keys can't be greater than number of args",
        ));
    }

    let keys = &args[1..1 + numkeys];
    let mut limit: usize = 0;

    let mut i = 1 + numkeys;
    while i < args.len() {
        match args[i].to_ascii_uppercase().as_slice() {
            b"LIMIT" => {
                if i + 1 >= args.len() {
                    return Err(Error::Syntax);
                }
                let limit_str = std::str::from_utf8(&args[i + 1])
                    .map_err(|_| Error::Other("LIMIT can't be negative"))?;
                // First try to parse as i64 to detect negative numbers
                let limit_i64: i64 = limit_str
                    .parse()
                    .map_err(|_| Error::Other("LIMIT can't be negative"))?;
                if limit_i64 < 0 {
                    return Err(Error::Other("LIMIT can't be negative"));
                }
                limit = limit_i64 as usize;
                i += 2;
            }
            _ => return Err(Error::Syntax),
        }
    }

    let count = store.sintercard(keys, limit)?;
    Ok(RespValue::integer(count as i64))
}

/// SUNION key [key ...]
fn cmd_sunion(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("SUNION"));
    }
    Ok(resp_array(store.sunion(args)?))
}

/// SDIFFSTORE destination key [key ...]
fn cmd_sdiffstore(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("SDIFFSTORE"));
    }
    let result = store.sdiff(&args[1..])?;
    let count = store.set_store(args[0].clone(), result);
    Ok(RespValue::integer(count as i64))
}

/// SINTERSTORE destination key [key ...]
fn cmd_sinterstore(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("SINTERSTORE"));
    }
    let result = store.sinter(&args[1..])?;
    let count = store.set_store(args[0].clone(), result);
    Ok(RespValue::integer(count as i64))
}

/// SUNIONSTORE destination key [key ...]
fn cmd_sunionstore(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("SUNIONSTORE"));
    }
    let result = store.sunion(&args[1..])?;
    let count = store.set_store(args[0].clone(), result);
    Ok(RespValue::integer(count as i64))
}

/// SMOVE source destination member
fn cmd_smove(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 3 {
        return Err(Error::WrongArity("SMOVE"));
    }
    let moved = store.smove(&args[0], args[1].clone(), args[2].clone())?;
    Ok(RespValue::integer(if moved { 1 } else { 0 }))
}

/// SSCAN key cursor [MATCH pattern] [COUNT count]
fn cmd_sscan(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("SSCAN"));
    }

    let cursor: u64 = std::str::from_utf8(&args[1])
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)?;

    let mut pattern: Option<&[u8]> = None;
    let mut count: usize = 10;

    let mut i = 2;
    while i < args.len() {
        match args[i].to_ascii_uppercase().as_slice() {
            b"MATCH" => {
                if i + 1 >= args.len() {
                    return Err(Error::Syntax);
                }
                pattern = Some(&args[i + 1]);
                i += 2;
            }
            b"COUNT" => {
                if i + 1 >= args.len() {
                    return Err(Error::Syntax);
                }
                count = std::str::from_utf8(&args[i + 1])
                    .map_err(|_| Error::NotInteger)?
                    .parse()
                    .map_err(|_| Error::NotInteger)?;
                i += 2;
            }
            _ => return Err(Error::Syntax),
        }
    }

    let (next_cursor, members) = store.sscan(&args[0], cursor, pattern, count)?;

    Ok(RespValue::array(vec![
        RespValue::bulk(Bytes::from(next_cursor.to_string())),
        resp_array(members),
    ]))
}
