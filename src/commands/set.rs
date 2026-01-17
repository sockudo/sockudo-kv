use bytes::Bytes;

use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::storage::Store;

pub fn execute(store: &Store, cmd: &[u8], args: &[Bytes]) -> Result<RespValue> {
    match cmd {
        b"SADD" => cmd_sadd(store, args),
        b"SREM" => cmd_srem(store, args),
        b"SISMEMBER" => cmd_sismember(store, args),
        b"SMISMEMBER" => cmd_smismember(store, args),
        b"SMEMBERS" => cmd_smembers(store, args),
        b"SCARD" => cmd_scard(store, args),
        b"SPOP" => cmd_spop(store, args),
        b"SRANDMEMBER" => cmd_srandmember(store, args),
        b"SDIFF" => cmd_sdiff(store, args),
        b"SINTER" => cmd_sinter(store, args),
        b"SUNION" => cmd_sunion(store, args),
        b"SDIFFSTORE" => cmd_sdiffstore(store, args),
        b"SINTERSTORE" => cmd_sinterstore(store, args),
        b"SUNIONSTORE" => cmd_sunionstore(store, args),
        b"SMOVE" => cmd_smove(store, args),
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
    let is_member = store.sismember(&args[0], &args[1]);
    Ok(RespValue::integer(if is_member { 1 } else { 0 }))
}

/// SMISMEMBER key member [member ...]
fn cmd_smismember(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("SMISMEMBER"));
    }
    let results: Vec<RespValue> = args[1..]
        .iter()
        .map(|m| RespValue::integer(if store.sismember(&args[0], m) { 1 } else { 0 }))
        .collect();
    Ok(RespValue::array(results))
}

/// SMEMBERS key
fn cmd_smembers(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("SMEMBERS"));
    }
    Ok(resp_array(store.smembers(&args[0])))
}

/// SCARD key
fn cmd_scard(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("SCARD"));
    }
    Ok(RespValue::integer(store.scard(&args[0]) as i64))
}

/// SPOP key [count]
fn cmd_spop(store: &Store, args: &[Bytes]) -> Result<RespValue> {
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

    let result = store.spop(&args[0], count);

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
    Ok(resp_array(store.sdiff(args)))
}

/// SINTER key [key ...]
fn cmd_sinter(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("SINTER"));
    }
    Ok(resp_array(store.sinter(args)))
}

/// SUNION key [key ...]
fn cmd_sunion(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("SUNION"));
    }
    Ok(resp_array(store.sunion(args)))
}

/// SDIFFSTORE destination key [key ...]
fn cmd_sdiffstore(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("SDIFFSTORE"));
    }
    let result = store.sdiff(&args[1..]);
    let count = store.set_store(args[0].clone(), result);
    Ok(RespValue::integer(count as i64))
}

/// SINTERSTORE destination key [key ...]
fn cmd_sinterstore(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("SINTERSTORE"));
    }
    let result = store.sinter(&args[1..]);
    let count = store.set_store(args[0].clone(), result);
    Ok(RespValue::integer(count as i64))
}

/// SUNIONSTORE destination key [key ...]
fn cmd_sunionstore(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("SUNIONSTORE"));
    }
    let result = store.sunion(&args[1..]);
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
