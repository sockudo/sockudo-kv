use bytes::Bytes;

use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::storage::Store;

#[inline]
fn parse_float(b: &[u8]) -> Result<f64> {
    std::str::from_utf8(b)
        .map_err(|_| Error::NotFloat)?
        .parse()
        .map_err(|_| Error::NotFloat)
}

#[inline]
fn parse_int(b: &[u8]) -> Result<i64> {
    std::str::from_utf8(b)
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)
}

/// Parse score bound (may be -inf, +inf, or (exclusive prefix)
fn parse_score_bound(b: &[u8], is_min: bool) -> Result<(f64, bool)> {
    let s = std::str::from_utf8(b).map_err(|_| Error::NotFloat)?;
    if s.eq_ignore_ascii_case("-inf") {
        return Ok((f64::NEG_INFINITY, false));
    }
    if s.eq_ignore_ascii_case("+inf") || s.eq_ignore_ascii_case("inf") {
        return Ok((f64::INFINITY, false));
    }
    if let Some(stripped) = s.strip_prefix('(') {
        let val: f64 = stripped.parse().map_err(|_| Error::NotFloat)?;
        // Exclusive: for min, add tiny epsilon; for max, subtract
        let adjusted = if is_min {
            val + f64::EPSILON
        } else {
            val - f64::EPSILON
        };
        return Ok((adjusted, true));
    }
    let val: f64 = s.parse().map_err(|_| Error::NotFloat)?;
    Ok((val, false))
}

pub fn execute(store: &Store, cmd: &[u8], args: &[Bytes]) -> Result<RespValue> {
    match cmd {
        b"ZADD" => cmd_zadd(store, args),
        b"ZSCORE" => cmd_zscore(store, args),
        b"ZCARD" => cmd_zcard(store, args),
        b"ZRANK" => cmd_zrank(store, args),
        b"ZRANGE" => cmd_zrange(store, args),
        b"ZREM" => cmd_zrem(store, args),
        b"ZINCRBY" => cmd_zincrby(store, args),
        b"ZCOUNT" => cmd_zcount(store, args),
        b"ZREVRANK" => cmd_zrevrank(store, args),
        b"ZREVRANGE" => cmd_zrevrange(store, args),
        b"ZRANGEBYSCORE" => cmd_zrangebyscore(store, args),
        b"ZREVRANGEBYSCORE" => cmd_zrevrangebyscore(store, args),
        b"ZPOPMIN" => cmd_zpopmin(store, args),
        b"ZPOPMAX" => cmd_zpopmax(store, args),
        b"ZMSCORE" => cmd_zmscore(store, args),
        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd).into_owned(),
        )),
    }
}

/// ZADD key score member [score member ...]
fn cmd_zadd(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
        return Err(Error::WrongArity("ZADD"));
    }
    let key = args[0].clone();
    let members: Vec<(f64, Bytes)> = args[1..]
        .chunks_exact(2)
        .map(|c| Ok((parse_float(&c[0])?, c[1].clone())))
        .collect::<Result<Vec<_>>>()?;
    let added = store.zadd(key, members)?;
    Ok(RespValue::integer(added as i64))
}

/// ZSCORE key member
fn cmd_zscore(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 2 {
        return Err(Error::WrongArity("ZSCORE"));
    }
    match store.zscore(&args[0], &args[1]) {
        Some(score) => Ok(RespValue::bulk_string(&score.to_string())),
        None => Ok(RespValue::null()),
    }
}

/// ZCARD key
fn cmd_zcard(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("ZCARD"));
    }
    Ok(RespValue::integer(store.zcard(&args[0]) as i64))
}

/// ZRANK key member
fn cmd_zrank(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 2 {
        return Err(Error::WrongArity("ZRANK"));
    }
    match store.zrank(&args[0], &args[1]) {
        Some(rank) => Ok(RespValue::integer(rank as i64)),
        None => Ok(RespValue::null()),
    }
}

/// ZRANGE key start stop [WITHSCORES]
fn cmd_zrange(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("ZRANGE"));
    }
    let start = parse_int(&args[1])?;
    let stop = parse_int(&args[2])?;
    let with_scores = args
        .get(3)
        .map(|a| a.eq_ignore_ascii_case(b"WITHSCORES"))
        .unwrap_or(false);

    let results = store.zrange(&args[0], start, stop, with_scores);
    build_zrange_response(results, with_scores)
}

/// ZREM key member [member ...]
fn cmd_zrem(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("ZREM"));
    }
    let removed = store.zrem(&args[0], &args[1..]);
    Ok(RespValue::integer(removed as i64))
}

/// ZINCRBY key increment member
fn cmd_zincrby(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 3 {
        return Err(Error::WrongArity("ZINCRBY"));
    }
    let delta = parse_float(&args[1])?;
    let new_score = store.zincrby(args[0].clone(), args[2].clone(), delta)?;
    Ok(RespValue::bulk_string(&new_score.to_string()))
}

/// ZCOUNT key min max
fn cmd_zcount(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 3 {
        return Err(Error::WrongArity("ZCOUNT"));
    }
    let (min, _) = parse_score_bound(&args[1], true)?;
    let (max, _) = parse_score_bound(&args[2], false)?;
    Ok(RespValue::integer(store.zcount(&args[0], min, max) as i64))
}

/// ZREVRANK key member
fn cmd_zrevrank(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 2 {
        return Err(Error::WrongArity("ZREVRANK"));
    }
    match store.zrevrank(&args[0], &args[1]) {
        Some(rank) => Ok(RespValue::integer(rank as i64)),
        None => Ok(RespValue::null()),
    }
}

/// ZREVRANGE key start stop [WITHSCORES]
fn cmd_zrevrange(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("ZREVRANGE"));
    }
    let start = parse_int(&args[1])?;
    let stop = parse_int(&args[2])?;
    let with_scores = args
        .get(3)
        .map(|a| a.eq_ignore_ascii_case(b"WITHSCORES"))
        .unwrap_or(false);

    let results = store.zrevrange(&args[0], start, stop, with_scores);
    build_zrange_response(results, with_scores)
}

/// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
fn cmd_zrangebyscore(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("ZRANGEBYSCORE"));
    }
    let (min, _) = parse_score_bound(&args[1], true)?;
    let (max, _) = parse_score_bound(&args[2], false)?;

    let mut with_scores = false;
    let mut offset = 0usize;
    let mut count = 0usize;

    let mut i = 3;
    while i < args.len() {
        if args[i].eq_ignore_ascii_case(b"WITHSCORES") {
            with_scores = true;
        } else if args[i].eq_ignore_ascii_case(b"LIMIT") {
            if i + 2 >= args.len() {
                return Err(Error::Syntax);
            }
            offset = parse_int(&args[i + 1])?.max(0) as usize;
            count = parse_int(&args[i + 2])?.max(0) as usize;
            i += 2;
        } else {
            return Err(Error::Syntax);
        }
        i += 1;
    }

    let results = store.zrangebyscore(&args[0], min, max, with_scores, offset, count);
    build_zrange_response(results, with_scores)
}

/// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
fn cmd_zrevrangebyscore(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("ZREVRANGEBYSCORE"));
    }
    let (max, _) = parse_score_bound(&args[1], false)?;
    let (min, _) = parse_score_bound(&args[2], true)?;

    let mut with_scores = false;
    let mut offset = 0usize;
    let mut count = 0usize;

    let mut i = 3;
    while i < args.len() {
        if args[i].eq_ignore_ascii_case(b"WITHSCORES") {
            with_scores = true;
        } else if args[i].eq_ignore_ascii_case(b"LIMIT") {
            if i + 2 >= args.len() {
                return Err(Error::Syntax);
            }
            offset = parse_int(&args[i + 1])?.max(0) as usize;
            count = parse_int(&args[i + 2])?.max(0) as usize;
            i += 2;
        } else {
            return Err(Error::Syntax);
        }
        i += 1;
    }

    let results = store.zrevrangebyscore(&args[0], max, min, with_scores, offset, count);
    build_zrange_response(results, with_scores)
}

/// ZPOPMIN key [count]
fn cmd_zpopmin(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("ZPOPMIN"));
    }
    let count = if args.len() > 1 {
        parse_int(&args[1])?.max(0) as usize
    } else {
        1
    };

    let results = store.zpopmin(&args[0], count);
    build_pop_response(results)
}

/// ZPOPMAX key [count]
fn cmd_zpopmax(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("ZPOPMAX"));
    }
    let count = if args.len() > 1 {
        parse_int(&args[1])?.max(0) as usize
    } else {
        1
    };

    let results = store.zpopmax(&args[0], count);
    build_pop_response(results)
}

/// ZMSCORE key member [member ...]
fn cmd_zmscore(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("ZMSCORE"));
    }
    let scores = store.zmscore(&args[0], &args[1..]);
    Ok(RespValue::array(
        scores
            .into_iter()
            .map(|s| match s {
                Some(score) => RespValue::bulk_string(&score.to_string()),
                None => RespValue::null(),
            })
            .collect(),
    ))
}

/// Helper to build response for ZRANGE-style commands
fn build_zrange_response(
    results: Vec<(Bytes, Option<f64>)>,
    with_scores: bool,
) -> Result<RespValue> {
    let mut resp = Vec::with_capacity(results.len() * if with_scores { 2 } else { 1 });
    for (member, score) in results {
        resp.push(RespValue::bulk(member));
        if let Some(s) = score {
            resp.push(RespValue::bulk_string(&s.to_string()));
        }
    }
    Ok(RespValue::array(resp))
}

/// Helper to build response for ZPOP commands
fn build_pop_response(results: Vec<(Bytes, f64)>) -> Result<RespValue> {
    if results.is_empty() {
        return Ok(RespValue::array(vec![]));
    }
    let mut resp = Vec::with_capacity(results.len() * 2);
    for (member, score) in results {
        resp.push(RespValue::bulk(member));
        resp.push(RespValue::bulk_string(&score.to_string()));
    }
    Ok(RespValue::array(resp))
}
