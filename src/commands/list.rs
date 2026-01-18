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
    let count = if args.len() > 1 {
        let c = parse_int(&args[1])?;
        if c < 0 {
            return Err(Error::Other("value is out of range, must be positive"));
        }
        c as usize
    } else {
        1
    };

    // count=0 returns empty array (not null)
    if count == 0 && args.len() > 1 {
        return Ok(RespValue::array(vec![]));
    }

    match store.lpop(&args[0], count) {
        Some(values) if args.len() == 1 => {
            // Single pop returns bulk string
            Ok(RespValue::bulk(values.into_iter().next().unwrap()))
        }
        Some(values) => {
            // Multiple pop returns array
            Ok(RespValue::array(
                values.into_iter().map(RespValue::bulk).collect(),
            ))
        }
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
    let count = if args.len() > 1 {
        let c = parse_int(&args[1])?;
        if c < 0 {
            return Err(Error::Other("value is out of range, must be positive"));
        }
        c as usize
    } else {
        1
    };

    // count=0 returns empty array (not null)
    if count == 0 && args.len() > 1 {
        return Ok(RespValue::array(vec![]));
    }

    match store.rpop(&args[0], count) {
        Some(values) if args.len() == 1 => Ok(RespValue::bulk(values.into_iter().next().unwrap())),
        Some(values) => Ok(RespValue::array(
            values.into_iter().map(RespValue::bulk).collect(),
        )),
        None => Ok(RespValue::null()),
    }
}

/// LLEN key
fn cmd_llen(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("LLEN"));
    }
    Ok(RespValue::integer(store.llen(&args[0]) as i64))
}

/// LRANGE key start stop
fn cmd_lrange(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 3 {
        return Err(Error::WrongArity("LRANGE"));
    }
    let start = parse_int(&args[1])?;
    let stop = parse_int(&args[2])?;
    let values = store.lrange(&args[0], start, stop);
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
    match store.lindex(&args[0], index) {
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

    match store.lpos(key, element, rank, search_count, maxlen) {
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
