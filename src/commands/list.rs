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

pub fn execute(store: &Store, cmd: &[u8], args: &[Bytes]) -> Result<RespValue> {
    match cmd {
        b"LPUSH" => cmd_lpush(store, args),
        b"RPUSH" => cmd_rpush(store, args),
        b"LPUSHX" => cmd_lpushx(store, args),
        b"RPUSHX" => cmd_rpushx(store, args),
        b"LPOP" => cmd_lpop(store, args),
        b"RPOP" => cmd_rpop(store, args),
        b"LLEN" => cmd_llen(store, args),
        b"LRANGE" => cmd_lrange(store, args),
        b"LINDEX" => cmd_lindex(store, args),
        b"LSET" => cmd_lset(store, args),
        b"LREM" => cmd_lrem(store, args),
        b"LTRIM" => cmd_ltrim(store, args),
        b"LINSERT" => cmd_linsert(store, args),
        b"LPOS" => cmd_lpos(store, args),
        b"LMOVE" => cmd_lmove(store, args),
        b"RPOPLPUSH" => cmd_rpoplpush(store, args),
        b"LMPOP" => cmd_lmpop(store, args),
        // Blocking commands - inside MULTI they run with timeout=0 (non-blocking)
        b"BLPOP" => cmd_blpop_nonblocking(store, args),
        b"BRPOP" => cmd_brpop_nonblocking(store, args),
        b"BLMOVE" => cmd_blmove_nonblocking(store, args),
        b"BRPOPLPUSH" => cmd_brpoplpush_nonblocking(store, args),
        b"BLMPOP" => cmd_blmpop_nonblocking(store, args),
        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd).into_owned(),
        )),
    }
}

/// LPUSH key element [element ...]
fn cmd_lpush(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("LPUSH"));
    }
    let key = args[0].clone();
    let values: Vec<Bytes> = args[1..].to_vec();
    let len = store.lpush(key, values)?;
    Ok(RespValue::integer(len as i64))
}

/// RPUSH key element [element ...]
fn cmd_rpush(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("RPUSH"));
    }
    let key = args[0].clone();
    let values: Vec<Bytes> = args[1..].to_vec();
    let len = store.rpush(key, values)?;
    Ok(RespValue::integer(len as i64))
}

/// LPUSHX key element [element ...]
fn cmd_lpushx(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("LPUSHX"));
    }
    if !store.exists(&args[0]) {
        return Ok(RespValue::integer(0));
    }
    let key = args[0].clone();
    let values: Vec<Bytes> = args[1..].to_vec();
    let len = store.lpush(key, values)?;
    Ok(RespValue::integer(len as i64))
}

/// RPUSHX key element [element ...]
fn cmd_rpushx(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("RPUSHX"));
    }
    if !store.exists(&args[0]) {
        return Ok(RespValue::integer(0));
    }
    let key = args[0].clone();
    let values: Vec<Bytes> = args[1..].to_vec();
    let len = store.rpush(key, values)?;
    Ok(RespValue::integer(len as i64))
}

/// LPOP key [count]
fn cmd_lpop(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() || args.len() > 2 {
        return Err(Error::WrongArity("LPOP"));
    }
    let has_count = args.len() > 1;
    let count = if has_count {
        let c = parse_int(&args[1])?;
        if c < 0 {
            return Err(Error::Other("value is out of range, must be positive"));
        }
        c as usize
    } else {
        1
    };

    // For count=0 on existing key, return empty array
    // For non-existing key, return null array (since count was specified)
    if count == 0 && has_count {
        // Check if key exists first
        if store.exists(&args[0]) {
            return Ok(RespValue::array(vec![]));
        } else {
            return Ok(RespValue::null_array());
        }
    }

    match store.lpop(&args[0], count)? {
        Some(values) if !has_count => {
            // Single pop (no count arg) returns bulk string
            Ok(RespValue::bulk(values.into_iter().next().unwrap()))
        }
        Some(values) => {
            // With count arg, returns array
            Ok(RespValue::array(
                values.into_iter().map(RespValue::bulk).collect(),
            ))
        }
        // Key doesn't exist or is empty
        None if has_count => Ok(RespValue::null_array()),
        None => Ok(RespValue::null()),
    }
}

/// LMOVE source destination LEFT|RIGHT LEFT|RIGHT
/// Atomically returns and removes the first/last element of the source list,
/// and pushes the element at the first/last element of the destination list.
fn cmd_lmove(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 4 {
        return Err(Error::WrongArity("LMOVE"));
    }

    let source = &args[0];
    let destination = &args[1];
    let wherefrom = parse_direction(&args[2])?;
    let whereto = parse_direction(&args[3])?;

    match store.lmove(source, destination, wherefrom, whereto)? {
        Some(value) => Ok(RespValue::bulk(value)),
        None => Ok(RespValue::null()),
    }
}

/// RPOPLPUSH source destination (deprecated, use LMOVE)
/// Atomically returns and removes the last element of the source list,
/// and pushes the element at the first element of the destination list.
fn cmd_rpoplpush(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 2 {
        return Err(Error::WrongArity("RPOPLPUSH"));
    }

    let source = &args[0];
    let destination = &args[1];

    // RPOPLPUSH is equivalent to LMOVE source destination RIGHT LEFT
    match store.lmove(source, destination, false, true)? {
        Some(value) => Ok(RespValue::bulk(value)),
        None => Ok(RespValue::null()),
    }
}

/// Parse LEFT/RIGHT direction argument
fn parse_direction(arg: &[u8]) -> Result<bool> {
    if arg.eq_ignore_ascii_case(b"LEFT") {
        Ok(true)
    } else if arg.eq_ignore_ascii_case(b"RIGHT") {
        Ok(false)
    } else {
        Err(Error::Syntax)
    }
}

/// RPOP key [count]
fn cmd_rpop(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() || args.len() > 2 {
        return Err(Error::WrongArity("RPOP"));
    }
    let has_count = args.len() > 1;
    let count = if has_count {
        let c = parse_int(&args[1])?;
        if c < 0 {
            return Err(Error::Other("value is out of range, must be positive"));
        }
        c as usize
    } else {
        1
    };

    // For count=0 on existing key, return empty array
    // For non-existing key, return null array (since count was specified)
    if count == 0 && has_count {
        if store.exists(&args[0]) {
            return Ok(RespValue::array(vec![]));
        } else {
            return Ok(RespValue::null_array());
        }
    }

    match store.rpop(&args[0], count)? {
        Some(values) if !has_count => {
            // Single pop (no count arg) returns bulk string
            Ok(RespValue::bulk(values.into_iter().next().unwrap()))
        }
        Some(values) => {
            // With count arg, returns array
            Ok(RespValue::array(
                values.into_iter().map(RespValue::bulk).collect(),
            ))
        }
        // Key doesn't exist or is empty
        None if has_count => Ok(RespValue::null_array()),
        None => Ok(RespValue::null()),
    }
}

/// LLEN key
fn cmd_llen(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("LLEN"));
    }
    Ok(RespValue::integer(store.llen(&args[0])? as i64))
}

/// LRANGE key start stop
fn cmd_lrange(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 3 {
        return Err(Error::WrongArity("LRANGE"));
    }
    let start = parse_int(&args[1])?;
    let stop = parse_int(&args[2])?;
    let values = store.lrange(&args[0], start, stop)?;
    Ok(RespValue::array(
        values.into_iter().map(RespValue::bulk).collect(),
    ))
}

/// LINDEX key index
fn cmd_lindex(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 2 {
        return Err(Error::WrongArity("LINDEX"));
    }
    let index = parse_int(&args[1])?;
    match store.lindex(&args[0], index)? {
        Some(v) => Ok(RespValue::bulk(v)),
        None => Ok(RespValue::null()),
    }
}

/// LSET key index element
fn cmd_lset(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 3 {
        return Err(Error::WrongArity("LSET"));
    }
    let index = parse_int(&args[1])?;
    store.lset(&args[0], index, args[2].clone())?;
    Ok(RespValue::ok())
}

/// LREM key count element
fn cmd_lrem(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 3 {
        return Err(Error::WrongArity("LREM"));
    }
    let count = parse_int(&args[1])?;
    let removed = store.lrem(&args[0], count, &args[2]);
    Ok(RespValue::integer(removed as i64))
}

/// LTRIM key start stop
fn cmd_ltrim(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 3 {
        return Err(Error::WrongArity("LTRIM"));
    }
    let start = parse_int(&args[1])?;
    let stop = parse_int(&args[2])?;
    store.ltrim(&args[0], start, stop)?;
    Ok(RespValue::ok())
}

/// LINSERT key BEFORE|AFTER pivot element
fn cmd_linsert(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 4 {
        return Err(Error::WrongArity("LINSERT"));
    }

    let before = if args[1].eq_ignore_ascii_case(b"BEFORE") {
        true
    } else if args[1].eq_ignore_ascii_case(b"AFTER") {
        false
    } else {
        return Err(Error::Syntax);
    };

    let result = store.linsert(&args[0], before, &args[2], args[3].clone())?;
    Ok(RespValue::integer(result))
}

/// LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]
fn cmd_lpos(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("LPOS"));
    }

    let key = &args[0];
    let element = &args[1];
    let mut rank: i64 = 1;
    let mut count: usize = 0; // 0 means return single value, not array
    let mut maxlen: usize = 0;
    let mut count_specified = false;

    let mut i = 2;
    while i < args.len() {
        let opt = &args[i];
        if opt.eq_ignore_ascii_case(b"RANK") {
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            rank = parse_int(&args[i])?;
            if rank == 0 {
                return Err(Error::Other(
                    "RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list",
                ));
            }
            // i64::MIN cannot be negated safely, treat as out of range
            if rank == i64::MIN {
                return Err(Error::Other("value is out of range"));
            }
        } else if opt.eq_ignore_ascii_case(b"COUNT") {
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            let c = parse_int(&args[i])?;
            if c < 0 {
                return Err(Error::Other("COUNT can't be negative"));
            }
            count = c as usize;
            count_specified = true;
        } else if opt.eq_ignore_ascii_case(b"MAXLEN") {
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            let m = parse_int(&args[i])?;
            if m < 0 {
                return Err(Error::Other("MAXLEN can't be negative"));
            }
            maxlen = m as usize;
        } else {
            return Err(Error::Syntax);
        }
        i += 1;
    }

    // When COUNT is not specified, we want exactly 1 result
    let search_count = if count_specified {
        if count == 0 { usize::MAX } else { count }
    } else {
        1
    };

    match store.lpos(key, element, rank, search_count, maxlen)? {
        Some(positions) => {
            if count_specified {
                // Return array
                Ok(RespValue::array(
                    positions.into_iter().map(RespValue::integer).collect(),
                ))
            } else {
                // Return single value or null
                if let Some(&pos) = positions.first() {
                    Ok(RespValue::integer(pos))
                } else {
                    Ok(RespValue::null())
                }
            }
        }
        None => Ok(RespValue::null()),
    }
}

/// LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
/// Pops one or more elements from the first non-empty list key from the list of provided key names.
fn cmd_lmpop(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    // LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
    if args.len() < 3 {
        return Err(Error::WrongArity("LMPOP"));
    }

    // Parse numkeys
    let numkeys: usize = std::str::from_utf8(&args[0])
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)?;

    if numkeys == 0 {
        return Err(Error::Other("numkeys should be greater than 0"));
    }

    // Validate we have enough arguments: numkeys + keys + direction
    if args.len() < 1 + numkeys + 1 {
        return Err(Error::Syntax);
    }

    // Parse keys
    let keys: Vec<&Bytes> = args[1..1 + numkeys].iter().collect();

    // Parse direction (LEFT|RIGHT)
    let direction_idx = 1 + numkeys;
    let direction = &args[direction_idx];
    let pop_left = if direction.eq_ignore_ascii_case(b"LEFT") {
        true
    } else if direction.eq_ignore_ascii_case(b"RIGHT") {
        false
    } else {
        return Err(Error::Syntax);
    };

    // Parse optional COUNT
    let mut count: usize = 1;
    let mut i = direction_idx + 1;
    while i < args.len() {
        if args[i].eq_ignore_ascii_case(b"COUNT") {
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            let c = parse_int(&args[i])?;
            if c <= 0 {
                return Err(Error::Other("count should be greater than 0"));
            }
            count = c as usize;
        } else {
            return Err(Error::Syntax);
        }
        i += 1;
    }

    // Try each key in order, return from the first non-empty one
    for key in keys {
        // Check type first
        if let Some(key_type) = store.key_type(key) {
            if key_type != "list" {
                return Err(Error::WrongType);
            }
        }

        let result = if pop_left {
            store.lpop(key, count)?
        } else {
            store.rpop(key, count)?
        };

        if let Some(values) = result {
            if !values.is_empty() {
                // Return [key, [elements...]]
                return Ok(RespValue::array(vec![
                    RespValue::bulk(key.clone()),
                    RespValue::array(values.into_iter().map(RespValue::bulk).collect()),
                ]));
            }
        }
    }

    // No non-empty list found
    Ok(RespValue::null_array())
}

// ============================================================================
// Non-blocking versions of blocking commands (used inside MULTI/EXEC)
// ============================================================================

/// BLPOP key [key ...] timeout - non-blocking version for MULTI
/// Returns null_array if no data, otherwise [key, value]
fn cmd_blpop_nonblocking(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("BLPOP"));
    }
    // Last arg is timeout, but we ignore it in non-blocking mode
    let keys = &args[..args.len() - 1];

    for key in keys {
        if let Some(key_type) = store.key_type(key) {
            if key_type != "list" {
                return Err(Error::WrongType);
            }
        }
        if let Some(values) = store.lpop(key, 1)? {
            if let Some(value) = values.into_iter().next() {
                return Ok(RespValue::array(vec![
                    RespValue::bulk(key.clone()),
                    RespValue::bulk(value),
                ]));
            }
        }
    }
    Ok(RespValue::null_array())
}

/// BRPOP key [key ...] timeout - non-blocking version for MULTI
fn cmd_brpop_nonblocking(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("BRPOP"));
    }
    let keys = &args[..args.len() - 1];

    for key in keys {
        if let Some(key_type) = store.key_type(key) {
            if key_type != "list" {
                return Err(Error::WrongType);
            }
        }
        if let Some(values) = store.rpop(key, 1)? {
            if let Some(value) = values.into_iter().next() {
                return Ok(RespValue::array(vec![
                    RespValue::bulk(key.clone()),
                    RespValue::bulk(value),
                ]));
            }
        }
    }
    Ok(RespValue::null_array())
}

/// BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout - non-blocking version
fn cmd_blmove_nonblocking(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 5 {
        return Err(Error::WrongArity("BLMOVE"));
    }
    let source = &args[0];
    let destination = &args[1];
    let wherefrom = &args[2];
    let whereto = &args[3];
    // args[4] is timeout, ignored in non-blocking mode

    let from_left = wherefrom.eq_ignore_ascii_case(b"LEFT");
    let to_left = whereto.eq_ignore_ascii_case(b"LEFT");

    if !from_left && !wherefrom.eq_ignore_ascii_case(b"RIGHT") {
        return Err(Error::Syntax);
    }
    if !to_left && !whereto.eq_ignore_ascii_case(b"RIGHT") {
        return Err(Error::Syntax);
    }

    match store.lmove(source, destination, from_left, to_left)? {
        Some(value) => Ok(RespValue::bulk(value)),
        None => Ok(RespValue::Null),
    }
}

/// BRPOPLPUSH source destination timeout - non-blocking version
fn cmd_brpoplpush_nonblocking(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 3 {
        return Err(Error::WrongArity("BRPOPLPUSH"));
    }
    let source = &args[0];
    let destination = &args[1];
    // args[2] is timeout, ignored

    // BRPOPLPUSH is equivalent to LMOVE source destination RIGHT LEFT
    match store.lmove(source, destination, false, true)? {
        Some(value) => Ok(RespValue::bulk(value)),
        None => Ok(RespValue::Null),
    }
}

/// BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count] - non-blocking version
fn cmd_blmpop_nonblocking(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    // BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
    if args.len() < 4 {
        return Err(Error::WrongArity("BLMPOP"));
    }

    // args[0] is timeout, ignored in non-blocking mode
    let numkeys: usize = std::str::from_utf8(&args[1])
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)?;

    if numkeys == 0 {
        return Err(Error::Other("numkeys must be positive"));
    }

    if args.len() < 2 + numkeys + 1 {
        return Err(Error::Syntax);
    }

    let keys = &args[2..2 + numkeys];
    let direction_idx = 2 + numkeys;
    let pop_left = args[direction_idx].eq_ignore_ascii_case(b"LEFT");
    if !pop_left && !args[direction_idx].eq_ignore_ascii_case(b"RIGHT") {
        return Err(Error::Syntax);
    }

    // Parse optional COUNT
    let mut count: usize = 1;
    let mut i = direction_idx + 1;
    while i < args.len() {
        if args[i].eq_ignore_ascii_case(b"COUNT") {
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            count = std::str::from_utf8(&args[i])
                .map_err(|_| Error::NotInteger)?
                .parse()
                .map_err(|_| Error::NotInteger)?;
            if count == 0 {
                return Err(Error::Other("count must be positive"));
            }
        } else {
            return Err(Error::Syntax);
        }
        i += 1;
    }

    // Try each key
    for key in keys {
        if let Some(key_type) = store.key_type(key) {
            if key_type != "list" {
                return Err(Error::WrongType);
            }
        }
        let result = if pop_left {
            store.lpop(key, count)?
        } else {
            store.rpop(key, count)?
        };
        if let Some(values) = result {
            if !values.is_empty() {
                return Ok(RespValue::array(vec![
                    RespValue::bulk(key.clone()),
                    RespValue::array(values.into_iter().map(RespValue::bulk).collect()),
                ]));
            }
        }
    }

    Ok(RespValue::null_array())
}
