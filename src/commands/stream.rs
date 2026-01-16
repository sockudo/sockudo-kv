//! Stream command handlers
//!
//! Implements all Redis stream commands (XADD, XREAD, XRANGE, etc.)

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::storage::Store;
use crate::storage::StreamId;
use crate::storage::ops::stream_ops::TrimStrategy;

/// Execute a stream command
pub fn execute(store: &Store, cmd: &[u8], args: &[Bytes]) -> Result<RespValue> {
    match cmd.to_ascii_uppercase().as_slice() {
        b"XADD" => cmd_xadd(store, args),
        b"XLEN" => cmd_xlen(store, args),
        b"XDEL" => cmd_xdel(store, args),
        b"XRANGE" => cmd_xrange(store, args),
        b"XREVRANGE" => cmd_xrevrange(store, args),
        b"XTRIM" => cmd_xtrim(store, args),
        b"XREAD" => cmd_xread(store, args),
        b"XREADGROUP" => cmd_xreadgroup(store, args),
        b"XGROUP" => cmd_xgroup(store, args),
        b"XACK" => cmd_xack(store, args),
        b"XCLAIM" => cmd_xclaim(store, args),
        b"XAUTOCLAIM" => cmd_xautoclaim(store, args),
        b"XPENDING" => cmd_xpending(store, args),
        b"XINFO" => cmd_xinfo(store, args),
        b"XSETID" => cmd_xsetid(store, args),
        // Redis 8.2 commands - basic compatibility
        b"XACKDEL" => cmd_xackdel(store, args),
        b"XDELEX" => cmd_xdelex(store, args),
        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd).into_owned(),
        )),
    }
}

/// Parse stream ID from bytes, returning error if invalid
#[inline]
fn parse_id(s: &[u8]) -> Result<StreamId> {
    StreamId::parse(s).ok_or(Error::Syntax)
}

/// Parse integer from bytes
#[inline]
fn parse_int(s: &[u8]) -> Result<i64> {
    std::str::from_utf8(s)
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)
}

/// Parse usize from bytes
#[inline]
fn parse_usize(s: &[u8]) -> Result<usize> {
    std::str::from_utf8(s)
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)
}

/// Build entry response array
#[inline]
fn entry_to_resp(id: StreamId, fields: Vec<(Bytes, Bytes)>) -> RespValue {
    let fields_arr: Vec<RespValue> = fields
        .into_iter()
        .flat_map(|(k, v)| vec![RespValue::bulk(k), RespValue::bulk(v)])
        .collect();
    RespValue::array(vec![
        RespValue::bulk(Bytes::from(id.to_string())),
        RespValue::array(fields_arr),
    ])
}

// ==================== Basic Stream Commands ====================

/// XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold [LIMIT count]] <*|id> field value [field value ...]
fn cmd_xadd(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 4 {
        return Err(Error::WrongArity("XADD"));
    }

    let key = args[0].clone();
    let mut idx = 1;
    let mut nomkstream = false;
    let mut trim: Option<TrimStrategy> = None;

    // Parse options
    while idx < args.len() {
        let arg = &args[idx];
        if arg.eq_ignore_ascii_case(b"NOMKSTREAM") {
            nomkstream = true;
            idx += 1;
        } else if arg.eq_ignore_ascii_case(b"MAXLEN") {
            idx += 1;
            if idx >= args.len() {
                return Err(Error::Syntax);
            }
            let approx = if args[idx].as_ref() == b"~" {
                idx += 1;
                true
            } else if args[idx].as_ref() == b"=" {
                idx += 1;
                false
            } else {
                false
            };
            if idx >= args.len() {
                return Err(Error::Syntax);
            }
            let threshold = parse_usize(&args[idx])?;
            idx += 1;
            // Skip LIMIT for now
            if idx < args.len() && args[idx].eq_ignore_ascii_case(b"LIMIT") {
                idx += 2; // Skip LIMIT count
            }
            trim = Some(TrimStrategy::MaxLen { threshold, approx });
        } else if arg.eq_ignore_ascii_case(b"MINID") {
            idx += 1;
            if idx >= args.len() {
                return Err(Error::Syntax);
            }
            let approx = if args[idx].as_ref() == b"~" {
                idx += 1;
                true
            } else if args[idx].as_ref() == b"=" {
                idx += 1;
                false
            } else {
                false
            };
            if idx >= args.len() {
                return Err(Error::Syntax);
            }
            let threshold = parse_id(&args[idx])?;
            idx += 1;
            // Skip LIMIT for now
            if idx < args.len() && args[idx].eq_ignore_ascii_case(b"LIMIT") {
                idx += 2;
            }
            trim = Some(TrimStrategy::MinId { threshold, approx });
        } else if arg.eq_ignore_ascii_case(b"KEEPREF")
            || arg.eq_ignore_ascii_case(b"DELREF")
            || arg.eq_ignore_ascii_case(b"ACKED")
        {
            // Redis 8.2 options - skip for compatibility
            idx += 1;
        } else {
            break;
        }
    }

    // Parse ID
    if idx >= args.len() {
        return Err(Error::Syntax);
    }
    let id_bytes = &args[idx];
    idx += 1;

    // Parse fields
    if !(args.len() - idx).is_multiple_of(2) || args.len() == idx {
        return Err(Error::WrongArity("XADD"));
    }
    let mut fields = Vec::with_capacity((args.len() - idx) / 2);
    while idx + 1 < args.len() {
        fields.push((args[idx].clone(), args[idx + 1].clone()));
        idx += 2;
    }

    // Determine ID
    let id = if id_bytes.as_ref() == b"*" {
        None
    } else {
        // Try to parse explicit ID or auto-seq
        if let Some(explicit_id) = StreamId::parse(id_bytes) {
            Some(explicit_id)
        } else {
            return Err(Error::Stream(
                "ERR Invalid stream ID specified as stream command argument".into(),
            ));
        }
    };

    match store.xadd(key, id, fields, nomkstream, trim)? {
        Some(new_id) => Ok(RespValue::bulk(Bytes::from(new_id.to_string()))),
        None => Ok(RespValue::null()),
    }
}

/// XLEN key
fn cmd_xlen(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("XLEN"));
    }
    Ok(RespValue::integer(store.xlen(&args[0]) as i64))
}

/// XDEL key id [id ...]
fn cmd_xdel(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("XDEL"));
    }
    let key = &args[0];
    let mut ids = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        ids.push(parse_id(arg)?);
    }
    let deleted = store.xdel(key, &ids)?;
    Ok(RespValue::integer(deleted as i64))
}

/// XRANGE key start end [COUNT count]
fn cmd_xrange(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("XRANGE"));
    }
    let key = &args[0];
    let start = parse_id(&args[1])?;
    let end = parse_id(&args[2])?;

    let mut count = None;
    if args.len() >= 5 && args[3].eq_ignore_ascii_case(b"COUNT") {
        count = Some(parse_usize(&args[4])?);
    }

    let entries = store.xrange(key, start, end, count)?;
    let result: Vec<RespValue> = entries
        .into_iter()
        .map(|(id, fields)| entry_to_resp(id, fields))
        .collect();
    Ok(RespValue::array(result))
}

/// XREVRANGE key end start [COUNT count]
fn cmd_xrevrange(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("XREVRANGE"));
    }
    let key = &args[0];
    let end = parse_id(&args[1])?;
    let start = parse_id(&args[2])?;

    let mut count = None;
    if args.len() >= 5 && args[3].eq_ignore_ascii_case(b"COUNT") {
        count = Some(parse_usize(&args[4])?);
    }

    let entries = store.xrevrange(key, end, start, count)?;
    let result: Vec<RespValue> = entries
        .into_iter()
        .map(|(id, fields)| entry_to_resp(id, fields))
        .collect();
    Ok(RespValue::array(result))
}

/// XTRIM key <MAXLEN|MINID> [=|~] threshold [LIMIT count]
fn cmd_xtrim(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("XTRIM"));
    }
    let key = &args[0];
    let mut idx = 1;

    // Skip KEEPREF/DELREF/ACKED options at the end (Redis 8.2)
    let strategy = if args[idx].eq_ignore_ascii_case(b"MAXLEN") {
        idx += 1;
        let approx = if idx < args.len() && args[idx].as_ref() == b"~" {
            idx += 1;
            true
        } else if idx < args.len() && args[idx].as_ref() == b"=" {
            idx += 1;
            false
        } else {
            false
        };
        if idx >= args.len() {
            return Err(Error::Syntax);
        }
        TrimStrategy::MaxLen {
            threshold: parse_usize(&args[idx])?,
            approx,
        }
    } else if args[idx].eq_ignore_ascii_case(b"MINID") {
        idx += 1;
        let approx = if idx < args.len() && args[idx].as_ref() == b"~" {
            idx += 1;
            true
        } else if idx < args.len() && args[idx].as_ref() == b"=" {
            idx += 1;
            false
        } else {
            false
        };
        if idx >= args.len() {
            return Err(Error::Syntax);
        }
        TrimStrategy::MinId {
            threshold: parse_id(&args[idx])?,
            approx,
        }
    } else {
        return Err(Error::Syntax);
    };

    let deleted = store.xtrim(key, strategy)?;
    Ok(RespValue::integer(deleted as i64))
}

/// XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
fn cmd_xread(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("XREAD"));
    }

    let mut idx = 0;
    let mut count = None;
    let mut _block = None; // Not implemented yet

    // Parse options
    while idx < args.len() {
        if args[idx].eq_ignore_ascii_case(b"COUNT") {
            idx += 1;
            if idx >= args.len() {
                return Err(Error::Syntax);
            }
            count = Some(parse_usize(&args[idx])?);
            idx += 1;
        } else if args[idx].eq_ignore_ascii_case(b"BLOCK") {
            idx += 1;
            if idx >= args.len() {
                return Err(Error::Syntax);
            }
            _block = Some(parse_int(&args[idx])?);
            idx += 1;
        } else if args[idx].eq_ignore_ascii_case(b"STREAMS") {
            idx += 1;
            break;
        } else {
            return Err(Error::Syntax);
        }
    }

    // Parse keys and ids
    let remaining = args.len() - idx;
    if remaining == 0 || !remaining.is_multiple_of(2) {
        return Err(Error::Syntax);
    }
    let num_streams = remaining / 2;
    let keys: Vec<Bytes> = args[idx..idx + num_streams].to_vec();
    let id_bytes = &args[idx + num_streams..];

    // Parse IDs - $ means last ID
    let mut ids = Vec::with_capacity(num_streams);
    for (i, id_arg) in id_bytes.iter().enumerate() {
        if id_arg.as_ref() == b"$" {
            // Get current last ID
            let last_id = store.xlen(&keys[i]);
            if last_id == 0 {
                ids.push(StreamId::ZERO);
            } else {
                // This is a simplification - ideally we'd get the actual last ID
                ids.push(StreamId::MAX);
            }
        } else {
            ids.push(parse_id(id_arg)?);
        }
    }

    let results = store.xread(&keys, &ids, count)?;

    if results.is_empty() {
        return Ok(RespValue::null());
    }

    let resp: Vec<RespValue> = results
        .into_iter()
        .map(|(key, entries)| {
            let entries_arr: Vec<RespValue> = entries
                .into_iter()
                .map(|(id, fields)| entry_to_resp(id, fields))
                .collect();
            RespValue::array(vec![RespValue::bulk(key), RespValue::array(entries_arr)])
        })
        .collect();
    Ok(RespValue::array(resp))
}

// ==================== Consumer Group Commands ====================

/// XGROUP CREATE|CREATECONSUMER|DELCONSUMER|DESTROY|SETID ...
fn cmd_xgroup(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("XGROUP"));
    }

    let subcmd = &args[0];

    if subcmd.eq_ignore_ascii_case(b"CREATE") {
        // XGROUP CREATE key group <id|$> [MKSTREAM] [ENTRIESREAD entries-read]
        if args.len() < 4 {
            return Err(Error::WrongArity("XGROUP CREATE"));
        }
        let key = args[1].clone();
        let group = args[2].clone();
        let id = if args[3].as_ref() == b"$" {
            StreamId::MAX // Will be resolved to last ID
        } else {
            parse_id(&args[3])?
        };

        let mut mkstream = false;
        let mut entries_read = None;
        let mut idx = 4;
        while idx < args.len() {
            if args[idx].eq_ignore_ascii_case(b"MKSTREAM") {
                mkstream = true;
                idx += 1;
            } else if args[idx].eq_ignore_ascii_case(b"ENTRIESREAD") {
                idx += 1;
                if idx >= args.len() {
                    return Err(Error::Syntax);
                }
                entries_read = Some(parse_usize(&args[idx])? as u64);
                idx += 1;
            } else {
                idx += 1; // Skip unknown options
            }
        }

        store.xgroup_create(key, group, id, mkstream, entries_read)?;
        Ok(RespValue::ok())
    } else if subcmd.eq_ignore_ascii_case(b"CREATECONSUMER") {
        // XGROUP CREATECONSUMER key group consumer
        if args.len() < 4 {
            return Err(Error::WrongArity("XGROUP CREATECONSUMER"));
        }
        let created = store.xgroup_createconsumer(&args[1], &args[2], args[3].clone())?;
        Ok(RespValue::integer(if created { 1 } else { 0 }))
    } else if subcmd.eq_ignore_ascii_case(b"DELCONSUMER") {
        // XGROUP DELCONSUMER key group consumer
        if args.len() < 4 {
            return Err(Error::WrongArity("XGROUP DELCONSUMER"));
        }
        let pending = store.xgroup_delconsumer(&args[1], &args[2], &args[3])?;
        Ok(RespValue::integer(pending as i64))
    } else if subcmd.eq_ignore_ascii_case(b"DESTROY") {
        // XGROUP DESTROY key group
        if args.len() < 3 {
            return Err(Error::WrongArity("XGROUP DESTROY"));
        }
        let destroyed = store.xgroup_destroy(&args[1], &args[2])?;
        Ok(RespValue::integer(if destroyed { 1 } else { 0 }))
    } else if subcmd.eq_ignore_ascii_case(b"SETID") {
        // XGROUP SETID key group <id|$> [ENTRIESREAD entries-read]
        if args.len() < 4 {
            return Err(Error::WrongArity("XGROUP SETID"));
        }
        let id = if args[3].as_ref() == b"$" {
            StreamId::MAX
        } else {
            parse_id(&args[3])?
        };

        let mut entries_read = None;
        if args.len() >= 6 && args[4].eq_ignore_ascii_case(b"ENTRIESREAD") {
            entries_read = Some(parse_usize(&args[5])? as u64);
        }

        store.xgroup_setid(&args[1], &args[2], id, entries_read)?;
        Ok(RespValue::ok())
    } else {
        Err(Error::Syntax)
    }
}

/// XREADGROUP GROUP group consumer [COUNT count] [BLOCK ms] [NOACK] STREAMS key [key ...] id [id ...]
fn cmd_xreadgroup(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 7 {
        return Err(Error::WrongArity("XREADGROUP"));
    }

    // Parse GROUP group consumer
    if !args[0].eq_ignore_ascii_case(b"GROUP") {
        return Err(Error::Syntax);
    }
    let group = args[1].clone();
    let consumer = args[2].clone();

    let mut idx = 3;
    let mut count = None;
    let mut _block = None;
    let mut noack = false;

    // Parse options
    while idx < args.len() {
        if args[idx].eq_ignore_ascii_case(b"COUNT") {
            idx += 1;
            if idx >= args.len() {
                return Err(Error::Syntax);
            }
            count = Some(parse_usize(&args[idx])?);
            idx += 1;
        } else if args[idx].eq_ignore_ascii_case(b"BLOCK") {
            idx += 1;
            if idx >= args.len() {
                return Err(Error::Syntax);
            }
            _block = Some(parse_int(&args[idx])?);
            idx += 1;
        } else if args[idx].eq_ignore_ascii_case(b"NOACK") {
            noack = true;
            idx += 1;
        } else if args[idx].eq_ignore_ascii_case(b"CLAIM") {
            // Skip CLAIM min-idle-time for now
            idx += 2;
        } else if args[idx].eq_ignore_ascii_case(b"STREAMS") {
            idx += 1;
            break;
        } else {
            return Err(Error::Syntax);
        }
    }

    // Parse keys and ids
    let remaining = args.len() - idx;
    if remaining == 0 || !remaining.is_multiple_of(2) {
        return Err(Error::Syntax);
    }
    let num_streams = remaining / 2;
    let keys: Vec<Bytes> = args[idx..idx + num_streams].to_vec();
    let ids: Vec<Bytes> = args[idx + num_streams..].to_vec();

    let results = store.xreadgroup(&group, consumer, &keys, &ids, count, noack)?;

    if results.is_empty() {
        return Ok(RespValue::null());
    }

    let resp: Vec<RespValue> = results
        .into_iter()
        .map(|(key, entries)| {
            let entries_arr: Vec<RespValue> = entries
                .into_iter()
                .map(|(id, fields)| entry_to_resp(id, fields))
                .collect();
            RespValue::array(vec![RespValue::bulk(key), RespValue::array(entries_arr)])
        })
        .collect();
    Ok(RespValue::array(resp))
}

/// XACK key group id [id ...]
fn cmd_xack(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("XACK"));
    }
    let key = &args[0];
    let group = &args[1];
    let mut ids = Vec::with_capacity(args.len() - 2);
    for arg in &args[2..] {
        ids.push(parse_id(arg)?);
    }
    let acked = store.xack(key, group, &ids)?;
    Ok(RespValue::integer(acked as i64))
}

/// XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME ms] [RETRYCOUNT count] [FORCE] [JUSTID]
fn cmd_xclaim(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 5 {
        return Err(Error::WrongArity("XCLAIM"));
    }
    let key = &args[0];
    let group = &args[1];
    let consumer = args[2].clone();
    let min_idle_time = parse_int(&args[3])?;

    let mut idx = 4;
    let mut ids = Vec::new();
    let mut idle = None;
    let mut time = None;
    let mut retry_count = None;
    let mut force = false;
    let mut justid = false;

    // Parse IDs and options
    while idx < args.len() {
        if args[idx].eq_ignore_ascii_case(b"IDLE") {
            idx += 1;
            if idx >= args.len() {
                return Err(Error::Syntax);
            }
            idle = Some(parse_int(&args[idx])?);
            idx += 1;
        } else if args[idx].eq_ignore_ascii_case(b"TIME") {
            idx += 1;
            if idx >= args.len() {
                return Err(Error::Syntax);
            }
            time = Some(parse_int(&args[idx])?);
            idx += 1;
        } else if args[idx].eq_ignore_ascii_case(b"RETRYCOUNT") {
            idx += 1;
            if idx >= args.len() {
                return Err(Error::Syntax);
            }
            retry_count = Some(parse_usize(&args[idx])? as u32);
            idx += 1;
        } else if args[idx].eq_ignore_ascii_case(b"FORCE") {
            force = true;
            idx += 1;
        } else if args[idx].eq_ignore_ascii_case(b"JUSTID") {
            justid = true;
            idx += 1;
        } else if args[idx].eq_ignore_ascii_case(b"LASTID") {
            // Skip LASTID option
            idx += 2;
        } else {
            // Assume it's an ID
            ids.push(parse_id(&args[idx])?);
            idx += 1;
        }
    }

    if ids.is_empty() {
        return Err(Error::Syntax);
    }

    let claimed = store.xclaim(
        key,
        group,
        consumer,
        min_idle_time,
        &ids,
        idle,
        time,
        retry_count,
        force,
        justid,
    )?;

    if justid {
        let result: Vec<RespValue> = claimed
            .into_iter()
            .map(|(id, _)| RespValue::bulk(Bytes::from(id.to_string())))
            .collect();
        Ok(RespValue::array(result))
    } else {
        let result: Vec<RespValue> = claimed
            .into_iter()
            .filter_map(|(id, fields)| fields.map(|f| entry_to_resp(id, f)))
            .collect();
        Ok(RespValue::array(result))
    }
}

/// XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]
fn cmd_xautoclaim(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 5 {
        return Err(Error::WrongArity("XAUTOCLAIM"));
    }
    let key = &args[0];
    let group = &args[1];
    let consumer = args[2].clone();
    let min_idle_time = parse_int(&args[3])?;
    let start = parse_id(&args[4])?;

    let mut count = 100;
    let mut justid = false;
    let mut idx = 5;

    while idx < args.len() {
        if args[idx].eq_ignore_ascii_case(b"COUNT") {
            idx += 1;
            if idx >= args.len() {
                return Err(Error::Syntax);
            }
            count = parse_usize(&args[idx])?;
            idx += 1;
        } else if args[idx].eq_ignore_ascii_case(b"JUSTID") {
            justid = true;
            idx += 1;
        } else {
            idx += 1;
        }
    }

    let result = store.xautoclaim(key, group, consumer, min_idle_time, start, count, justid)?;

    // Response: [next_start_id, [[id, [field, value, ...]], ...], [deleted_ids...]]
    let claimed_resp: Vec<RespValue> = if justid {
        result
            .claimed
            .into_iter()
            .map(|(id, _)| RespValue::bulk(Bytes::from(id.to_string())))
            .collect()
    } else {
        result
            .claimed
            .into_iter()
            .map(|(id, fields)| entry_to_resp(id, fields))
            .collect()
    };

    let deleted_resp: Vec<RespValue> = result
        .deleted_ids
        .into_iter()
        .map(|id| RespValue::bulk(Bytes::from(id.to_string())))
        .collect();

    Ok(RespValue::array(vec![
        RespValue::bulk(Bytes::from(result.next_start_id.to_string())),
        RespValue::array(claimed_resp),
        RespValue::array(deleted_resp),
    ]))
}

/// XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
fn cmd_xpending(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("XPENDING"));
    }
    let key = &args[0];
    let group = &args[1];

    if args.len() == 2 {
        // Summary mode
        let summary = store.xpending_summary(key, group)?;
        let consumers_arr: Vec<RespValue> = summary
            .consumers
            .into_iter()
            .map(|(name, count)| {
                RespValue::array(vec![
                    RespValue::bulk(name),
                    RespValue::bulk(Bytes::from(count.to_string())),
                ])
            })
            .collect();

        Ok(RespValue::array(vec![
            RespValue::integer(summary.count as i64),
            summary.min_id.map_or(RespValue::null(), |id| {
                RespValue::bulk(Bytes::from(id.to_string()))
            }),
            summary.max_id.map_or(RespValue::null(), |id| {
                RespValue::bulk(Bytes::from(id.to_string()))
            }),
            RespValue::array(consumers_arr),
        ]))
    } else {
        // Range mode
        let mut idx = 2;
        let mut idle = None;

        if args[idx].eq_ignore_ascii_case(b"IDLE") {
            idx += 1;
            if idx >= args.len() {
                return Err(Error::Syntax);
            }
            idle = Some(parse_int(&args[idx])?);
            idx += 1;
        }

        if args.len() < idx + 3 {
            return Err(Error::WrongArity("XPENDING"));
        }

        let start = parse_id(&args[idx])?;
        let end = parse_id(&args[idx + 1])?;
        let count = parse_usize(&args[idx + 2])?;
        let consumer = if args.len() > idx + 3 {
            Some(args[idx + 3].as_ref())
        } else {
            None
        };

        let pending = store.xpending_range(key, group, start, end, count, consumer, idle)?;

        let result: Vec<RespValue> = pending
            .into_iter()
            .map(|(id, consumer, idle_time, delivery_count)| {
                RespValue::array(vec![
                    RespValue::bulk(Bytes::from(id.to_string())),
                    RespValue::bulk(consumer),
                    RespValue::integer(idle_time),
                    RespValue::integer(delivery_count as i64),
                ])
            })
            .collect();

        Ok(RespValue::array(result))
    }
}

// ==================== Info Commands ====================

/// XINFO STREAM|GROUPS|CONSUMERS key [group] [FULL [COUNT count]]
fn cmd_xinfo(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("XINFO"));
    }

    let subcmd = &args[0];

    if subcmd.eq_ignore_ascii_case(b"STREAM") {
        let key = &args[1];
        let mut full = false;
        let mut count = None;

        for i in 2..args.len() {
            if args[i].eq_ignore_ascii_case(b"FULL") {
                full = true;
            } else if args[i].eq_ignore_ascii_case(b"COUNT") && i + 1 < args.len() {
                count = Some(parse_usize(&args[i + 1])?);
            }
        }

        let info = store.xinfo_stream(key, full, count)?;

        let mut result = vec![
            RespValue::bulk(Bytes::from_static(b"length")),
            RespValue::integer(info.length as i64),
            RespValue::bulk(Bytes::from_static(b"radix-tree-keys")),
            RespValue::integer(info.radix_tree_keys as i64),
            RespValue::bulk(Bytes::from_static(b"radix-tree-nodes")),
            RespValue::integer(info.radix_tree_nodes as i64),
            RespValue::bulk(Bytes::from_static(b"last-generated-id")),
            RespValue::bulk(Bytes::from(info.last_generated_id.to_string())),
            RespValue::bulk(Bytes::from_static(b"max-deleted-entry-id")),
            RespValue::bulk(Bytes::from(info.max_deleted_entry_id.to_string())),
            RespValue::bulk(Bytes::from_static(b"entries-added")),
            RespValue::integer(info.entries_added as i64),
            RespValue::bulk(Bytes::from_static(b"groups")),
            RespValue::integer(info.groups as i64),
        ];

        if let Some((id, fields)) = info.first_entry {
            result.push(RespValue::bulk(Bytes::from_static(b"first-entry")));
            result.push(entry_to_resp(id, fields));
        } else {
            result.push(RespValue::bulk(Bytes::from_static(b"first-entry")));
            result.push(RespValue::null());
        }

        if let Some((id, fields)) = info.last_entry {
            result.push(RespValue::bulk(Bytes::from_static(b"last-entry")));
            result.push(entry_to_resp(id, fields));
        } else {
            result.push(RespValue::bulk(Bytes::from_static(b"last-entry")));
            result.push(RespValue::null());
        }

        Ok(RespValue::array(result))
    } else if subcmd.eq_ignore_ascii_case(b"GROUPS") {
        let key = &args[1];
        let groups = store.xinfo_groups(key)?;

        let result: Vec<RespValue> = groups
            .into_iter()
            .map(|g| {
                RespValue::array(vec![
                    RespValue::bulk(Bytes::from_static(b"name")),
                    RespValue::bulk(g.name),
                    RespValue::bulk(Bytes::from_static(b"consumers")),
                    RespValue::integer(g.consumers as i64),
                    RespValue::bulk(Bytes::from_static(b"pending")),
                    RespValue::integer(g.pending as i64),
                    RespValue::bulk(Bytes::from_static(b"last-delivered-id")),
                    RespValue::bulk(Bytes::from(g.last_delivered_id.to_string())),
                    RespValue::bulk(Bytes::from_static(b"entries-read")),
                    g.entries_read
                        .map_or(RespValue::null(), |v| RespValue::integer(v as i64)),
                    RespValue::bulk(Bytes::from_static(b"lag")),
                    g.lag
                        .map_or(RespValue::null(), |v| RespValue::integer(v as i64)),
                ])
            })
            .collect();

        Ok(RespValue::array(result))
    } else if subcmd.eq_ignore_ascii_case(b"CONSUMERS") {
        if args.len() < 3 {
            return Err(Error::WrongArity("XINFO CONSUMERS"));
        }
        let key = &args[1];
        let group = &args[2];

        let consumers = store.xinfo_consumers(key, group)?;

        let result: Vec<RespValue> = consumers
            .into_iter()
            .map(|c| {
                RespValue::array(vec![
                    RespValue::bulk(Bytes::from_static(b"name")),
                    RespValue::bulk(c.name),
                    RespValue::bulk(Bytes::from_static(b"pending")),
                    RespValue::integer(c.pending as i64),
                    RespValue::bulk(Bytes::from_static(b"idle")),
                    RespValue::integer(c.idle),
                    RespValue::bulk(Bytes::from_static(b"inactive")),
                    RespValue::integer(c.inactive),
                ])
            })
            .collect();

        Ok(RespValue::array(result))
    } else {
        Err(Error::Syntax)
    }
}

/// XSETID key last-id [ENTRIESADDED entries-added] [MAXDELETEDID max-deleted-id]
fn cmd_xsetid(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("XSETID"));
    }
    let key = &args[0];
    let last_id = parse_id(&args[1])?;

    let mut entries_added = None;
    let mut max_deleted_id = None;
    let mut idx = 2;

    while idx < args.len() {
        if args[idx].eq_ignore_ascii_case(b"ENTRIESADDED") {
            idx += 1;
            if idx >= args.len() {
                return Err(Error::Syntax);
            }
            entries_added = Some(parse_usize(&args[idx])? as u64);
            idx += 1;
        } else if args[idx].eq_ignore_ascii_case(b"MAXDELETEDID") {
            idx += 1;
            if idx >= args.len() {
                return Err(Error::Syntax);
            }
            max_deleted_id = Some(parse_id(&args[idx])?);
            idx += 1;
        } else {
            idx += 1;
        }
    }

    store.xsetid(key, last_id, entries_added, max_deleted_id)?;
    Ok(RespValue::ok())
}

// ==================== Redis 8.2 Commands (Compatibility) ====================

/// XACKDEL key group [KEEPREF|DELREF|ACKED] IDS numids id [id ...]
fn cmd_xackdel(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    // Implementation: ACK the messages (DELREF behavior is not implemented)
    if args.len() < 4 {
        return Err(Error::WrongArity("XACKDEL"));
    }
    let key = &args[0];
    let group = &args[1];

    // Find IDS position
    let mut idx = 2;
    while idx < args.len() {
        if args[idx].eq_ignore_ascii_case(b"IDS") {
            break;
        }
        idx += 1;
    }

    if idx >= args.len() || !args[idx].eq_ignore_ascii_case(b"IDS") {
        return Err(Error::Syntax);
    }
    idx += 1;

    if idx >= args.len() {
        return Err(Error::Syntax);
    }
    let num_ids = parse_usize(&args[idx])?;
    idx += 1;

    let mut ids = Vec::with_capacity(num_ids);
    for i in 0..num_ids {
        if idx + i >= args.len() {
            return Err(Error::Syntax);
        }
        ids.push(parse_id(&args[idx + i])?);
    }

    let acked = store.xack(key, group, &ids)?;
    Ok(RespValue::integer(acked as i64))
}

/// XDELEX key [KEEPREF|DELREF|ACKED] IDS numids id [id ...]
fn cmd_xdelex(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    // Implementation: Delete the entries (reference options not implemented)
    if args.len() < 3 {
        return Err(Error::WrongArity("XDELEX"));
    }
    let key = &args[0];

    // Find IDS position
    let mut idx = 1;
    while idx < args.len() {
        if args[idx].eq_ignore_ascii_case(b"IDS") {
            break;
        }
        idx += 1;
    }

    if idx >= args.len() || !args[idx].eq_ignore_ascii_case(b"IDS") {
        return Err(Error::Syntax);
    }
    idx += 1;

    if idx >= args.len() {
        return Err(Error::Syntax);
    }
    let num_ids = parse_usize(&args[idx])?;
    idx += 1;

    let mut ids = Vec::with_capacity(num_ids);
    for i in 0..num_ids {
        if idx + i >= args.len() {
            return Err(Error::Syntax);
        }
        ids.push(parse_id(&args[idx + i])?);
    }

    let deleted = store.xdel(key, &ids)?;
    Ok(RespValue::integer(deleted as i64))
}
