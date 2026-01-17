use bytes::Bytes;

use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::storage::Store;
use std::sync::Arc;

#[inline]
fn parse_int(b: &[u8]) -> Result<i64> {
    std::str::from_utf8(b)
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)
}

#[inline]
fn parse_float(b: &[u8]) -> Result<f64> {
    let val = std::str::from_utf8(b)
        .map_err(|_| Error::NotFloat)?
        .parse()
        .map_err(|_| Error::NotFloat)?;
    // Reject NaN and Infinity as input values
    if f64::is_infinite(val) || f64::is_nan(val) {
        return Err(Error::Custom("ERR value is NaN or Infinity".to_string()));
    }
    Ok(val)
}

use std::sync::atomic::Ordering;

pub fn execute(
    store: &Store,
    cmd: &[u8],
    args: &[Bytes],
    client: Option<&crate::client::ClientState>,
    replication: Option<&Arc<crate::replication::ReplicationManager>>,
) -> Result<RespValue> {
    match cmd {
        b"HSET" => cmd_hset(store, args, replication),
        b"HGET" => cmd_hget(store, args),
        b"HDEL" => cmd_hdel(store, args, replication),
        b"HEXISTS" => cmd_hexists(store, args),
        b"HGETALL" => cmd_hgetall(store, args),
        b"HKEYS" => cmd_hkeys(store, args),
        b"HVALS" => cmd_hvals(store, args),
        b"HLEN" => cmd_hlen(store, args),
        b"HMSET" => cmd_hmset(store, args, replication),
        b"HMGET" => cmd_hmget(store, args),
        b"HSETNX" => cmd_hsetnx(store, args, replication),
        b"HINCRBY" => cmd_hincrby(store, args, replication),
        b"HINCRBYFLOAT" => cmd_hincrbyfloat(store, args, replication),
        b"HSTRLEN" => cmd_hstrlen(store, args),
        b"HRANDFIELD" => cmd_hrandfield(store, args, client),
        b"HGETDEL" => cmd_hgetdel(store, args, replication),
        b"HSCAN" => cmd_hscan(store, args),
        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd).into_owned(),
        )),
    }
}

/// HSET key field value [field value ...]
fn cmd_hset(
    store: &Store,
    args: &[Bytes],
    replication: Option<&Arc<crate::replication::ReplicationManager>>,
) -> Result<RespValue> {
    if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
        return Err(Error::WrongArity("HSET"));
    }
    let key = args[0].clone();
    let fields: Vec<(Bytes, Bytes)> = args[1..]
        .chunks_exact(2)
        .map(|c| (c[0].clone(), c[1].clone()))
        .collect();
    let added = store.hset(key, fields)?;

    // Propagate HSET
    if let Some(repl) = replication {
        let mut parts = Vec::with_capacity(1 + args.len());
        parts.push(Bytes::from_static(b"HSET"));
        parts.extend_from_slice(args);
        repl.propagate(&parts);
    }
    Ok(RespValue::integer(added as i64))
}

/// HGET key field
fn cmd_hget(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 2 {
        return Err(Error::WrongArity("HGET"));
    }
    match store.hget(&args[0], &args[1])? {
        Some(v) => Ok(RespValue::bulk(v)),
        None => Ok(RespValue::null()),
    }
}

/// HDEL key field [field ...]
/// HDEL key field [field ...]
fn cmd_hdel(
    store: &Store,
    args: &[Bytes],
    replication: Option<&Arc<crate::replication::ReplicationManager>>,
) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("HDEL"));
    }
    let count = store.hdel(&args[0], &args[1..])?;

    if count > 0 {
        if let Some(repl) = replication {
            let mut parts = Vec::with_capacity(1 + args.len());
            parts.push(Bytes::from_static(b"HDEL"));
            parts.extend_from_slice(args);
            repl.propagate(&parts);
        }
    }
    Ok(RespValue::integer(count as i64))
}

/// HEXISTS key field
fn cmd_hexists(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 2 {
        return Err(Error::WrongArity("HEXISTS"));
    }
    let exists = store.hexists(&args[0], &args[1])?;
    Ok(RespValue::integer(if exists { 1 } else { 0 }))
}

/// HGETALL key
fn cmd_hgetall(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("HGETALL"));
    }
    let pairs = store.hgetall(&args[0])?;
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
    let pairs = store.hgetall(&args[0])?;
    Ok(RespValue::array(
        pairs.into_iter().map(|(k, _)| RespValue::bulk(k)).collect(),
    ))
}

/// HVALS key
fn cmd_hvals(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("HVALS"));
    }
    let pairs = store.hgetall(&args[0])?;
    Ok(RespValue::array(
        pairs.into_iter().map(|(_, v)| RespValue::bulk(v)).collect(),
    ))
}

/// HLEN key
fn cmd_hlen(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("HLEN"));
    }
    Ok(RespValue::integer(store.hlen(&args[0])? as i64))
}

/// HMSET key field value [field value ...] (deprecated, use HSET)
/// HMSET key field value [field value ...] (deprecated, use HSET)
fn cmd_hmset(
    store: &Store,
    args: &[Bytes],
    replication: Option<&Arc<crate::replication::ReplicationManager>>,
) -> Result<RespValue> {
    if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
        return Err(Error::WrongArity("HMSET"));
    }
    let key = args[0].clone();
    let fields: Vec<(Bytes, Bytes)> = args[1..]
        .chunks_exact(2)
        .map(|c| (c[0].clone(), c[1].clone()))
        .collect();
    store.hset(key, fields)?;

    // Propagate HMSET (as HSET for compatibility/simplicity)
    if let Some(repl) = replication {
        let mut parts = Vec::with_capacity(1 + args.len());
        parts.push(Bytes::from_static(b"HSET"));
        parts.extend_from_slice(args);
        repl.propagate(&parts);
    }
    Ok(RespValue::ok())
}

/// HMGET key field [field ...]
fn cmd_hmget(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("HMGET"));
    }
    let mut results = Vec::with_capacity(args.len() - 1);
    for f in &args[1..] {
        match store.hget(&args[0], f)? {
            Some(v) => results.push(RespValue::bulk(v)),
            None => results.push(RespValue::null()),
        }
    }
    Ok(RespValue::array(results))
}

/// HSETNX key field value
fn cmd_hsetnx(
    store: &Store,
    args: &[Bytes],
    replication: Option<&Arc<crate::replication::ReplicationManager>>,
) -> Result<RespValue> {
    if args.len() != 3 {
        return Err(Error::WrongArity("HSETNX"));
    }
    if store.hexists(&args[0], &args[1])? {
        return Ok(RespValue::integer(0));
    }
    store.hset(args[0].clone(), vec![(args[1].clone(), args[2].clone())])?;

    // Propagate HSETNX as HSET (since it succeeded)
    if let Some(repl) = replication {
        let parts = vec![
            Bytes::from_static(b"HSET"),
            args[0].clone(),
            args[1].clone(),
            args[2].clone(),
        ];
        repl.propagate(&parts);
    }

    Ok(RespValue::integer(1))
}

/// HINCRBY key field increment
fn cmd_hincrby(
    store: &Store,
    args: &[Bytes],
    replication: Option<&Arc<crate::replication::ReplicationManager>>,
) -> Result<RespValue> {
    if args.len() != 3 {
        return Err(Error::WrongArity("HINCRBY"));
    }
    let delta = parse_int(&args[2])?;
    let new_val = store.hincrby(args[0].clone(), args[1].clone(), delta)?;

    // Propagate HINCRBY
    if let Some(repl) = replication {
        let mut parts = Vec::with_capacity(1 + args.len());
        parts.push(Bytes::from_static(b"HINCRBY"));
        parts.extend_from_slice(args);
        repl.propagate(&parts);
    }

    Ok(RespValue::integer(new_val))
}

/// HINCRBYFLOAT key field increment
fn cmd_hincrbyfloat(
    store: &Store,
    args: &[Bytes],
    replication: Option<&Arc<crate::replication::ReplicationManager>>,
) -> Result<RespValue> {
    if args.len() != 3 {
        return Err(Error::WrongArity("HINCRBYFLOAT"));
    }
    let delta = parse_float(&args[2])?;
    let new_val = store.hincrbyfloat(args[0].clone(), args[1].clone(), delta)?;

    // Propagate HINCRBYFLOAT
    if let Some(repl) = replication {
        let mut parts = Vec::with_capacity(1 + args.len());
        parts.push(Bytes::from_static(b"HINCRBYFLOAT"));
        parts.extend_from_slice(args);
        repl.propagate(&parts);
    }
    Ok(RespValue::bulk_string(&format!("{}", new_val)))
}

/// HSTRLEN key field
fn cmd_hstrlen(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 2 {
        return Err(Error::WrongArity("HSTRLEN"));
    }
    let len = store
        .hget(&args[0], &args[1])?
        .map(|v| v.len())
        .unwrap_or(0);
    Ok(RespValue::integer(len as i64))
}

/// HRANDFIELD key [count [WITHVALUES]]
/// HRANDFIELD key [count [WITHVALUES]]
fn cmd_hrandfield(
    store: &Store,
    args: &[Bytes],
    client: Option<&crate::client::ClientState>,
) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("HRANDFIELD"));
    }

    // Parse count and WITHVALUES flag
    let count = if args.len() >= 2 {
        let c = parse_int(&args[1])?;

        // Check for range errors to prevent allocation panic or overflow
        if c.checked_abs().map(|a| a > i64::MAX / 2).unwrap_or(true) {
            // Error if count is negative (duplicates allowed -> huge allocation)
            // OR if WITHVALUES is used (potential overflow logic from Redis)
            if c < 0 || (args.len() >= 3 && args[2].eq_ignore_ascii_case(b"WITHVALUES")) {
                return Err(Error::Custom(
                    "ERR value is out of range, must be positive".to_string(),
                ));
            }
        }
        c
    } else {
        1
    };

    let with_values = if args.len() >= 3 {
        if args[2].eq_ignore_ascii_case(b"WITHVALUES") {
            true
        } else {
            return Err(Error::Syntax);
        }
    } else {
        false
    };

    // Get random fields from the hash
    let result = store.hrandfield(&args[0], count, with_values)?;

    // Single field without count parameter
    if args.len() == 1 {
        if let Some(field) = result.first() {
            return Ok(RespValue::bulk(field.clone()));
        } else {
            return Ok(RespValue::null());
        }
    }

    // Check for RESP3
    let is_resp3 = client
        .map(|c| c.protocol_version.load(Ordering::Relaxed) == 3)
        .unwrap_or(false);

    // Multiple fields (with count parameter)
    if is_resp3 && with_values {
        // Return array of arrays (pairs)
        let pairs: Vec<RespValue> = result
            .chunks_exact(2)
            .map(|chunk| {
                RespValue::array(vec![
                    RespValue::bulk(chunk[0].clone()),
                    RespValue::bulk(chunk[1].clone()),
                ])
            })
            .collect();
        Ok(RespValue::array(pairs))
    } else {
        // Flat array
        Ok(RespValue::array(
            result.into_iter().map(RespValue::bulk).collect(),
        ))
    }
}

/// HGETDEL key FIELDS numfields field [field ...]
/// HGETDEL key FIELDS numfields field [field ...]
fn cmd_hgetdel(
    store: &Store,
    args: &[Bytes],
    replication: Option<&Arc<crate::replication::ReplicationManager>>,
) -> Result<RespValue> {
    // HGETDEL requires: key FIELDS numfields field [field ...]
    // Validate minimum arguments: need at least key FIELDS numfields field
    if args.len() < 4 {
        // Check for specific error cases
        if args.len() >= 2 && !args[1].eq_ignore_ascii_case(b"FIELDS") && args.len() >= 4 {
            return Err(Error::Custom(
                "ERR mandatory argument FIELDS is missing, or not at the right position"
                    .to_string(),
            ));
        }
        return Err(Error::WrongArity("HGETDEL"));
    }

    // Check for FIELDS keyword (required)
    if !args[1].eq_ignore_ascii_case(b"FIELDS") {
        return Err(Error::Custom(
            "ERR mandatory argument FIELDS is missing, or not at the right position".to_string(),
        ));
    }

    // Parse numfields - must be positive integer
    let num_fields: i64 = parse_int(&args[2]).map_err(|_| {
        Error::Custom("ERR Number of fields must be a positive integer".to_string())
    })?;

    if num_fields <= 0 {
        return Err(Error::Custom(
            "ERR Number of fields must be a positive integer".to_string(),
        ));
    }

    let num_fields = num_fields as usize;

    // Check numfields matches actual number of fields provided
    let actual_fields = args.len() - 3;
    if actual_fields != num_fields {
        return Err(Error::Custom(
            "ERR The numfields parameter must match the number of arguments".to_string(),
        ));
    }

    let fields = args[3..3 + num_fields].to_vec();

    let values = store.hgetdel(&args[0], &fields)?;

    // Propagate as HDEL with all requested fields (Redis behavior)
    // Only propagate if at least one field was actually deleted
    if let Some(repl) = replication {
        let any_deleted = values.iter().any(|v| v.is_some());
        if any_deleted {
            let mut parts = Vec::with_capacity(2 + fields.len());
            parts.push(Bytes::from_static(b"HDEL"));
            parts.push(args[0].clone()); // Key
            parts.extend(fields.clone());
            repl.propagate(&parts);
        }
    }

    // Return Array of values (or nulls for non-existent fields)
    let resp: Vec<RespValue> = values
        .into_iter()
        .map(|v| match v {
            Some(val) => RespValue::bulk(val),
            None => RespValue::null(),
        })
        .collect();
    Ok(RespValue::array(resp))
}

/// HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]
fn cmd_hscan(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("HSCAN"));
    }

    let cursor: u64 = parse_int(&args[1])
        .map(|c| c as u64)
        .map_err(|_| Error::NotInteger)?;

    let mut pattern: Option<&[u8]> = None;
    let mut count: usize = 10;
    let mut no_values = false;

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
                count = parse_int(&args[i + 1])? as usize;
                i += 2;
            }
            b"NOVALUES" => {
                no_values = true;
                i += 1;
            }
            _ => return Err(Error::Syntax),
        }
    }

    let (next_cursor, items) = store.hscan(&args[0], cursor, pattern, count, no_values)?;

    // Scan returns flat list of key value (or just keys if NOVALUES)
    Ok(RespValue::array(vec![
        RespValue::bulk(Bytes::from(next_cursor.to_string())),
        RespValue::array(items.into_iter().map(RespValue::bulk).collect()),
    ]))
}
