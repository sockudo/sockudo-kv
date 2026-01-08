//! Generic command handlers
//!
//! Implements COPY, DUMP, RESTORE, KEYS, SCAN, RENAME, TOUCH, UNLINK,
//! OBJECT, SORT, EXPIREAT, EXPIRETIME, RANDOMKEY, WAIT, WAITAOF and related commands.

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::storage::{DataType, Entry, Store};

/// Execute generic commands
pub fn execute(store: &Store, cmd: &[u8], args: &[Bytes]) -> Result<RespValue> {
    match cmd.to_ascii_uppercase().as_slice() {
        b"COPY" => cmd_copy(store, args),
        b"DUMP" => cmd_dump(store, args),
        b"RESTORE" => cmd_restore(store, args),
        b"EXPIREAT" => cmd_expireat(store, args),
        b"PEXPIREAT" => cmd_pexpireat(store, args),
        b"EXPIRETIME" => cmd_expiretime(store, args),
        b"PEXPIRETIME" => cmd_pexpiretime(store, args),
        b"KEYS" => cmd_keys(store, args),
        b"SCAN" => cmd_scan(store, args),
        b"RANDOMKEY" => cmd_randomkey(store),
        b"RENAME" => cmd_rename(store, args),
        b"RENAMENX" => cmd_renamenx(store, args),
        b"TOUCH" => cmd_touch(store, args),
        b"UNLINK" => cmd_unlink(store, args),
        b"OBJECT" => cmd_object(store, args),
        b"SORT" => cmd_sort(store, args),
        b"SORT_RO" => cmd_sort_ro(store, args),
        b"WAIT" => cmd_wait(args),
        b"WAITAOF" => cmd_waitaof(args),
        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd).into_owned(),
        )),
    }
}

// ==================== COPY ====================

/// COPY source destination [DB destination-db] [REPLACE]
fn cmd_copy(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("COPY"));
    }

    let source = &args[0];
    let dest = &args[1];
    let mut replace = false;

    // Parse options
    let mut i = 2;
    while i < args.len() {
        let arg = args[i].to_ascii_uppercase();
        match arg.as_slice() {
            b"REPLACE" => replace = true,
            b"DB" => {
                // DB option is for cross-database copy, not supported in single-store mode
                i += 1; // Skip the db number
            }
            _ => return Err(Error::Syntax),
        }
        i += 1;
    }

    let result = store.copy_key(source, dest, replace);
    Ok(RespValue::integer(if result { 1 } else { 0 }))
}

// ==================== DUMP ====================

/// DUMP key
fn cmd_dump(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("DUMP"));
    }

    match store.dump_key(&args[0]) {
        Some(data) => Ok(RespValue::bulk(Bytes::from(data))),
        None => Ok(RespValue::Null),
    }
}

// ==================== RESTORE ====================

/// RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]
fn cmd_restore(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("RESTORE"));
    }

    let key = &args[0];
    let ttl: i64 = std::str::from_utf8(&args[1])
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)?;
    let data = &args[2];

    let mut replace = false;
    let mut absttl = false;

    // Parse options
    let mut i = 3;
    while i < args.len() {
        let arg = args[i].to_ascii_uppercase();
        match arg.as_slice() {
            b"REPLACE" => replace = true,
            b"ABSTTL" => absttl = true,
            b"IDLETIME" | b"FREQ" => {
                i += 1; // Skip the value (we don't support these)
            }
            _ => return Err(Error::Syntax),
        }
        i += 1;
    }

    match store.restore_key(key, ttl, data, replace, absttl) {
        Ok(()) => Ok(RespValue::ok()),
        Err(msg) => Ok(RespValue::error(msg)),
    }
}

// ==================== EXPIREAT ====================

/// EXPIREAT key unix-time-seconds [NX | XX | GT | LT]
fn cmd_expireat(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("EXPIREAT"));
    }

    let key = &args[0];
    let timestamp: i64 = std::str::from_utf8(&args[1])
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)?;

    let timestamp_ms = timestamp * 1000;

    let (mut nx, mut xx, mut gt, mut lt) = (false, false, false, false);
    for arg in args.iter().skip(2) {
        match arg.to_ascii_uppercase().as_slice() {
            b"NX" => nx = true,
            b"XX" => xx = true,
            b"GT" => gt = true,
            b"LT" => lt = true,
            _ => return Err(Error::Syntax),
        }
    }

    let result = store.expire_at(key, timestamp_ms, nx, xx, gt, lt);
    Ok(RespValue::integer(if result { 1 } else { 0 }))
}

/// PEXPIREAT key unix-time-milliseconds [NX | XX | GT | LT]
fn cmd_pexpireat(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("PEXPIREAT"));
    }

    let key = &args[0];
    let timestamp_ms: i64 = std::str::from_utf8(&args[1])
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)?;

    let (mut nx, mut xx, mut gt, mut lt) = (false, false, false, false);
    for arg in args.iter().skip(2) {
        match arg.to_ascii_uppercase().as_slice() {
            b"NX" => nx = true,
            b"XX" => xx = true,
            b"GT" => gt = true,
            b"LT" => lt = true,
            _ => return Err(Error::Syntax),
        }
    }

    let result = store.expire_at(key, timestamp_ms, nx, xx, gt, lt);
    Ok(RespValue::integer(if result { 1 } else { 0 }))
}

// ==================== EXPIRETIME ====================

/// EXPIRETIME key
fn cmd_expiretime(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("EXPIRETIME"));
    }

    let ms = store.expire_time_ms(&args[0]);
    let seconds = if ms >= 0 { ms / 1000 } else { ms };
    Ok(RespValue::integer(seconds))
}

/// PEXPIRETIME key
fn cmd_pexpiretime(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("PEXPIRETIME"));
    }

    Ok(RespValue::integer(store.expire_time_ms(&args[0])))
}

// ==================== KEYS ====================

/// KEYS pattern
fn cmd_keys(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("KEYS"));
    }

    let keys = store.keys_pattern(&args[0]);
    Ok(RespValue::array(
        keys.into_iter().map(RespValue::bulk).collect(),
    ))
}

// ==================== SCAN ====================

/// SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
fn cmd_scan(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("SCAN"));
    }

    let cursor: u64 = std::str::from_utf8(&args[0])
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)?;

    let mut pattern: Option<&[u8]> = None;
    let mut count: usize = 10;
    let mut type_filter: Option<&[u8]> = None;

    let mut i = 1;
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
            b"TYPE" => {
                if i + 1 >= args.len() {
                    return Err(Error::Syntax);
                }
                type_filter = Some(&args[i + 1]);
                i += 2;
            }
            _ => return Err(Error::Syntax),
        }
    }

    let (next_cursor, keys) = store.scan(cursor, pattern, count, type_filter);

    Ok(RespValue::array(vec![
        RespValue::bulk(Bytes::from(next_cursor.to_string())),
        RespValue::array(keys.into_iter().map(RespValue::bulk).collect()),
    ]))
}

// ==================== RANDOMKEY ====================

/// RANDOMKEY
fn cmd_randomkey(store: &Store) -> Result<RespValue> {
    match store.random_key() {
        Some(key) => Ok(RespValue::bulk(key)),
        None => Ok(RespValue::Null),
    }
}

// ==================== RENAME ====================

/// RENAME key newkey
fn cmd_rename(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 2 {
        return Err(Error::WrongArity("RENAME"));
    }

    if store.rename(&args[0], &args[1]) {
        Ok(RespValue::ok())
    } else {
        Ok(RespValue::error("ERR no such key"))
    }
}

/// RENAMENX key newkey
fn cmd_renamenx(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 2 {
        return Err(Error::WrongArity("RENAMENX"));
    }

    let result = store.rename_nx(&args[0], &args[1]);
    if result == -1 {
        Ok(RespValue::error("ERR no such key"))
    } else {
        Ok(RespValue::integer(result))
    }
}

// ==================== TOUCH ====================

/// TOUCH key [key ...]
fn cmd_touch(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("TOUCH"));
    }

    let count = store.touch(args);
    Ok(RespValue::integer(count))
}

// ==================== UNLINK ====================

/// UNLINK key [key ...] - async delete (same as DEL for now)
fn cmd_unlink(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("UNLINK"));
    }

    let count: i64 = args.iter().map(|k| if store.del(k) { 1 } else { 0 }).sum();
    Ok(RespValue::integer(count))
}

// ==================== OBJECT ====================

/// OBJECT ENCODING key | OBJECT FREQ key | OBJECT IDLETIME key | OBJECT REFCOUNT key
fn cmd_object(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("OBJECT"));
    }

    let subcommand = args[0].to_ascii_uppercase();
    match subcommand.as_slice() {
        b"ENCODING" => {
            if args.len() != 2 {
                return Err(Error::WrongArity("OBJECT ENCODING"));
            }
            match store.object_encoding(&args[1]) {
                Some(enc) => Ok(RespValue::bulk(Bytes::from(enc))),
                None => Ok(RespValue::Null),
            }
        }
        b"FREQ" => {
            if args.len() != 2 {
                return Err(Error::WrongArity("OBJECT FREQ"));
            }
            if store.exists(&args[1]) {
                Ok(RespValue::integer(0)) // We don't track frequency
            } else {
                Ok(RespValue::Null)
            }
        }
        b"IDLETIME" => {
            if args.len() != 2 {
                return Err(Error::WrongArity("OBJECT IDLETIME"));
            }
            if store.exists(&args[1]) {
                Ok(RespValue::integer(0)) // We don't track idle time
            } else {
                Ok(RespValue::Null)
            }
        }
        b"REFCOUNT" => {
            if args.len() != 2 {
                return Err(Error::WrongArity("OBJECT REFCOUNT"));
            }
            if store.exists(&args[1]) {
                Ok(RespValue::integer(1)) // Always 1 in our implementation
            } else {
                Ok(RespValue::Null)
            }
        }
        b"HELP" => Ok(RespValue::array(vec![
            RespValue::bulk(Bytes::from_static(b"OBJECT ENCODING <key>")),
            RespValue::bulk(Bytes::from_static(b"OBJECT FREQ <key>")),
            RespValue::bulk(Bytes::from_static(b"OBJECT IDLETIME <key>")),
            RespValue::bulk(Bytes::from_static(b"OBJECT REFCOUNT <key>")),
        ])),
        _ => Err(Error::Custom(format!(
            "ERR Unknown subcommand or wrong number of arguments for '{}'",
            String::from_utf8_lossy(&subcommand)
        ))),
    }
}

// ==================== SORT ====================

/// SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC | DESC] [ALPHA] [STORE destination]
fn cmd_sort(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    sort_impl(store, args, false)
}

/// SORT_RO key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC | DESC] [ALPHA]
fn cmd_sort_ro(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    sort_impl(store, args, true)
}

fn sort_impl(store: &Store, args: &[Bytes], read_only: bool) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("SORT"));
    }

    let key = &args[0];
    let mut offset: usize = 0;
    let mut count: Option<usize> = None;
    let mut desc = false;
    let mut alpha = false;
    let mut store_dest: Option<&Bytes> = None;
    let mut get_patterns: Vec<&Bytes> = Vec::new();
    let mut by_pattern: Option<&Bytes> = None;

    // Parse options
    let mut i = 1;
    while i < args.len() {
        match args[i].to_ascii_uppercase().as_slice() {
            b"BY" => {
                if i + 1 >= args.len() {
                    return Err(Error::Syntax);
                }
                by_pattern = Some(&args[i + 1]);
                i += 2;
            }
            b"LIMIT" => {
                if i + 2 >= args.len() {
                    return Err(Error::Syntax);
                }
                offset = std::str::from_utf8(&args[i + 1])
                    .map_err(|_| Error::NotInteger)?
                    .parse()
                    .map_err(|_| Error::NotInteger)?;
                count = Some(
                    std::str::from_utf8(&args[i + 2])
                        .map_err(|_| Error::NotInteger)?
                        .parse()
                        .map_err(|_| Error::NotInteger)?,
                );
                i += 3;
            }
            b"GET" => {
                if i + 1 >= args.len() {
                    return Err(Error::Syntax);
                }
                get_patterns.push(&args[i + 1]);
                i += 2;
            }
            b"ASC" => {
                desc = false;
                i += 1;
            }
            b"DESC" => {
                desc = true;
                i += 1;
            }
            b"ALPHA" => {
                alpha = true;
                i += 1;
            }
            b"STORE" => {
                if read_only {
                    return Err(Error::Custom(
                        "ERR SORT_RO does not support STORE option".into(),
                    ));
                }
                if i + 1 >= args.len() {
                    return Err(Error::Syntax);
                }
                store_dest = Some(&args[i + 1]);
                i += 2;
            }
            _ => return Err(Error::Syntax),
        }
    }

    // Get elements to sort
    let elements: Vec<Bytes> = match store.data.get(key.as_ref()) {
        Some(entry) if !entry.is_expired() => match &entry.data {
            DataType::List(l) => l.iter().cloned().collect(),
            DataType::Set(s) => s.iter().map(|x| x.clone()).collect(),
            DataType::SortedSet(zs) => {
                // Use scores HashMap for iteration
                zs.scores.keys().cloned().collect()
            }
            _ => return Err(Error::WrongType),
        },
        _ => Vec::new(),
    };

    // Sort elements
    let mut sorted: Vec<(Bytes, Option<f64>, Option<Bytes>)> = elements
        .into_iter()
        .map(|e| {
            if let Some(pattern) = by_pattern {
                // BY pattern - look up external key
                let lookup_key = substitute_pattern(pattern, &e);
                if let Some(entry) = store.data.get(lookup_key.as_ref())
                    && !entry.is_expired()
                    && let Some(s) = entry.data.as_string()
                {
                    return (e, parse_sort_key(s, alpha), Some(s.clone()));
                }
                (e.clone(), None, None)
            } else {
                (e.clone(), parse_sort_key(&e, alpha), None)
            }
        })
        .collect();

    // Sort
    sorted.sort_by(|a, b| {
        let cmp = match (&a.1, &b.1) {
            (Some(x), Some(y)) => x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => a.0.cmp(&b.0),
        };
        if desc { cmp.reverse() } else { cmp }
    });

    // Apply LIMIT
    let total = sorted.len();
    let start = offset.min(total);
    let end = count.map(|c| (start + c).min(total)).unwrap_or(total);
    let sorted = &sorted[start..end];

    // Build result
    let result: Vec<Bytes> = if get_patterns.is_empty() {
        sorted.iter().map(|(e, _, _)| e.clone()).collect()
    } else {
        let mut res = Vec::new();
        for (e, _, _) in sorted {
            for pattern in &get_patterns {
                if pattern.as_ref() == b"#" {
                    res.push(e.clone());
                } else {
                    let lookup_key = substitute_pattern(pattern, e);
                    if let Some(entry) = store.data.get(lookup_key.as_ref())
                        && !entry.is_expired()
                        && let Some(s) = entry.data.as_string()
                    {
                        res.push(s.clone());
                        continue;
                    }
                    res.push(Bytes::new()); // nil equivalent
                }
            }
        }
        res
    };

    // STORE if requested
    if let Some(dest) = store_dest {
        use std::collections::VecDeque;
        let list: VecDeque<Bytes> = result.iter().cloned().collect();
        store.del(dest);
        store
            .data
            .insert(dest.clone(), Entry::new(DataType::List(list)));
        store
            .key_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return Ok(RespValue::integer(result.len() as i64));
    }

    Ok(RespValue::array(
        result.into_iter().map(RespValue::bulk).collect(),
    ))
}

fn substitute_pattern(pattern: &Bytes, value: &Bytes) -> Bytes {
    // Replace * with value
    if let Some(pos) = pattern.iter().position(|&b| b == b'*') {
        let mut result = Vec::with_capacity(pattern.len() + value.len());
        result.extend_from_slice(&pattern[..pos]);
        result.extend_from_slice(value);
        result.extend_from_slice(&pattern[pos + 1..]);
        Bytes::from(result)
    } else {
        pattern.clone()
    }
}

fn parse_sort_key(data: &[u8], alpha: bool) -> Option<f64> {
    if alpha {
        // For alpha sort, use first few bytes as float for ordering
        let mut val = 0u64;
        for (i, &b) in data.iter().take(8).enumerate() {
            val |= (b as u64) << (56 - i * 8);
        }
        Some(f64::from_bits(val))
    } else {
        std::str::from_utf8(data)
            .ok()
            .and_then(|s| s.trim().parse().ok())
    }
}

// ==================== WAIT ====================

/// WAIT numreplicas timeout - stub implementation
fn cmd_wait(args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 2 {
        return Err(Error::WrongArity("WAIT"));
    }
    // In standalone mode, we have no replicas, so return 0 immediately
    Ok(RespValue::integer(0))
}

/// WAITAOF numlocal numreplicas timeout - stub implementation
fn cmd_waitaof(args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 3 {
        return Err(Error::WrongArity("WAITAOF"));
    }
    // Return [0, 0] - no local AOF, no replicas
    Ok(RespValue::array(vec![
        RespValue::integer(0),
        RespValue::integer(0),
    ]))
}
