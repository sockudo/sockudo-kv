use bytes::Bytes;

use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::storage::Store;

#[inline]
fn parse_int(b: &[u8]) -> Result<i64> {
    std::str::from_utf8(b)
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)
}

#[inline]
fn parse_float(b: &[u8]) -> Result<f64> {
    std::str::from_utf8(b)
        .map_err(|_| Error::NotFloat)?
        .parse()
        .map_err(|_| Error::NotFloat)
}

pub fn execute(store: &Store, cmd: &[u8], args: &[Bytes]) -> Result<RespValue> {
    match cmd {
        b"HSET" => cmd_hset(store, args),
        b"HGET" => cmd_hget(store, args),
        b"HDEL" => cmd_hdel(store, args),
        b"HEXISTS" => cmd_hexists(store, args),
        b"HGETALL" => cmd_hgetall(store, args),
        b"HKEYS" => cmd_hkeys(store, args),
        b"HVALS" => cmd_hvals(store, args),
        b"HLEN" => cmd_hlen(store, args),
        b"HMSET" => cmd_hmset(store, args),
        b"HMGET" => cmd_hmget(store, args),
        b"HSETNX" => cmd_hsetnx(store, args),
        b"HINCRBY" => cmd_hincrby(store, args),
        b"HINCRBYFLOAT" => cmd_hincrbyfloat(store, args),
        b"HSTRLEN" => cmd_hstrlen(store, args),
        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd).into_owned(),
        )),
    }
}

/// HSET key field value [field value ...]
fn cmd_hset(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
        return Err(Error::WrongArity("HSET"));
    }
    let key = args[0].clone();
    let fields: Vec<(Bytes, Bytes)> = args[1..]
        .chunks_exact(2)
        .map(|c| (c[0].clone(), c[1].clone()))
        .collect();
    let added = store.hset(key, fields)?;
    Ok(RespValue::integer(added as i64))
}

/// HGET key field
fn cmd_hget(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 2 {
        return Err(Error::WrongArity("HGET"));
    }
    match store.hget(&args[0], &args[1]) {
        Some(v) => Ok(RespValue::bulk(v)),
        None => Ok(RespValue::null()),
    }
}

/// HDEL key field [field ...]
fn cmd_hdel(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("HDEL"));
    }
    let removed = store.hdel(&args[0], &args[1..]);
    Ok(RespValue::integer(removed as i64))
}

/// HEXISTS key field
fn cmd_hexists(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 2 {
        return Err(Error::WrongArity("HEXISTS"));
    }
    let exists = store.hexists(&args[0], &args[1]);
    Ok(RespValue::integer(if exists { 1 } else { 0 }))
}

/// HGETALL key
fn cmd_hgetall(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("HGETALL"));
    }
    let pairs = store.hgetall(&args[0]);
    let mut result = Vec::with_capacity(pairs.len() * 2);
    for (k, v) in pairs {
        result.push(RespValue::bulk(k));
        result.push(RespValue::bulk(v));
    }
    Ok(RespValue::array(result))
}

/// HKEYS key
fn cmd_hkeys(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("HKEYS"));
    }
    let pairs = store.hgetall(&args[0]);
    Ok(RespValue::array(
        pairs.into_iter().map(|(k, _)| RespValue::bulk(k)).collect(),
    ))
}

/// HVALS key
fn cmd_hvals(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("HVALS"));
    }
    let pairs = store.hgetall(&args[0]);
    Ok(RespValue::array(
        pairs.into_iter().map(|(_, v)| RespValue::bulk(v)).collect(),
    ))
}

/// HLEN key
fn cmd_hlen(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("HLEN"));
    }
    Ok(RespValue::integer(store.hlen(&args[0]) as i64))
}

/// HMSET key field value [field value ...] (deprecated, use HSET)
fn cmd_hmset(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
        return Err(Error::WrongArity("HMSET"));
    }
    let key = args[0].clone();
    let fields: Vec<(Bytes, Bytes)> = args[1..]
        .chunks_exact(2)
        .map(|c| (c[0].clone(), c[1].clone()))
        .collect();
    store.hset(key, fields)?;
    Ok(RespValue::ok())
}

/// HMGET key field [field ...]
fn cmd_hmget(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("HMGET"));
    }
    let results: Vec<RespValue> = args[1..]
        .iter()
        .map(|f| {
            store
                .hget(&args[0], f)
                .map(RespValue::bulk)
                .unwrap_or(RespValue::null())
        })
        .collect();
    Ok(RespValue::array(results))
}

/// HSETNX key field value
fn cmd_hsetnx(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 3 {
        return Err(Error::WrongArity("HSETNX"));
    }
    if store.hexists(&args[0], &args[1]) {
        return Ok(RespValue::integer(0));
    }
    store.hset(args[0].clone(), vec![(args[1].clone(), args[2].clone())])?;
    Ok(RespValue::integer(1))
}

/// HINCRBY key field increment
fn cmd_hincrby(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 3 {
        return Err(Error::WrongArity("HINCRBY"));
    }
    let delta = parse_int(&args[2])?;
    let new_val = store.hincrby(args[0].clone(), args[1].clone(), delta)?;
    Ok(RespValue::integer(new_val))
}

/// HINCRBYFLOAT key field increment
fn cmd_hincrbyfloat(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 3 {
        return Err(Error::WrongArity("HINCRBYFLOAT"));
    }
    let delta = parse_float(&args[2])?;
    let new_val = store.hincrbyfloat(args[0].clone(), args[1].clone(), delta)?;
    Ok(RespValue::bulk_string(&format!("{}", new_val)))
}

/// HSTRLEN key field
fn cmd_hstrlen(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 2 {
        return Err(Error::WrongArity("HSTRLEN"));
    }
    let len = store.hget(&args[0], &args[1]).map(|v| v.len()).unwrap_or(0);
    Ok(RespValue::integer(len as i64))
}
