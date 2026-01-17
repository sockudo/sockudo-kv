use bytes::Bytes;

use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::storage::{Store, now_ms};

/// Case-insensitive comparison without allocation
#[inline]
fn eq_ignore_ascii_case(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.eq_ignore_ascii_case(y))
}

/// Parse integer from bytes
#[inline]
fn parse_int(b: &[u8]) -> Result<i64> {
    std::str::from_utf8(b)
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)
}

/// Parse float from bytes
#[inline]
fn parse_float(b: &[u8]) -> Result<f64> {
    std::str::from_utf8(b)
        .map_err(|_| Error::NotFloat)?
        .parse()
        .map_err(|_| Error::NotFloat)
}

/// SET command conditional options (Redis 8.4+)
#[derive(Default, Clone)]
enum SetCondition {
    #[default]
    None,
    Nx,            // Only set if not exists
    Xx,            // Only set if exists
    Ifeq(Bytes),   // Only set if value equals
    Ifne(Bytes),   // Only set if value not equals
    Ifdeq(String), // Only set if digest equals
    Ifdne(String), // Only set if digest not equals
}

/// SET command options
#[derive(Default)]
struct SetOptions {
    condition: SetCondition, // NX, XX, IFEQ, IFNE, IFDEQ, IFDNE
    get: bool,               // Return old value
    ex: Option<i64>,         // Expire in seconds
    px: Option<i64>,         // Expire in milliseconds
    exat: Option<i64>,       // Expire at unix time (seconds)
    pxat: Option<i64>,       // Expire at unix time (ms)
    keepttl: bool,           // Keep existing TTL
}

/// Execute string commands
pub fn execute(store: &Store, cmd: &[u8], args: &[Bytes]) -> Result<RespValue> {
    match cmd {
        b"GET" => cmd_get(store, args),
        b"SET" => cmd_set(store, args),
        b"SETNX" => cmd_setnx(store, args),
        b"SETEX" => cmd_setex(store, args),
        b"PSETEX" => cmd_psetex(store, args),
        b"MGET" => cmd_mget(store, args),
        b"MSET" => cmd_mset(store, args),
        b"MSETEX" => cmd_msetex(store, args),
        b"MSETNX" => cmd_msetnx(store, args),
        b"APPEND" => cmd_append(store, args),
        b"STRLEN" => cmd_strlen(store, args),
        b"INCR" => cmd_incr(store, args),
        b"INCRBY" => cmd_incrby(store, args),
        b"INCRBYFLOAT" => cmd_incrbyfloat(store, args),
        b"DECR" => cmd_decr(store, args),
        b"DECRBY" => cmd_decrby(store, args),
        b"GETRANGE" => cmd_getrange(store, args),
        b"SUBSTR" => cmd_getrange(store, args), // SUBSTR is alias for GETRANGE
        b"SETRANGE" => cmd_setrange(store, args),
        b"GETSET" => cmd_getset(store, args),
        b"GETEX" => cmd_getex(store, args),
        b"GETDEL" => cmd_getdel(store, args),
        b"LCS" => cmd_lcs(store, args),
        b"DIGEST" => cmd_digest(store, args),
        b"DELEX" => cmd_delex(store, args),
        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd).into_owned(),
        )),
    }
}

/// GET key
fn cmd_get(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("GET"));
    }
    Ok(match store.get(&args[0]) {
        Some(v) => RespValue::bulk(v),
        None => RespValue::null(),
    })
}

/// SET key value [NX|XX|IFEQ value|IFNE value|IFDEQ digest|IFDNE digest] [GET] [EX seconds|PX milliseconds|EXAT unix-time|PXAT unix-time-ms] [KEEPTTL]
fn cmd_set(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("SET"));
    }

    // Fast path: simple SET key value (most common case)
    if args.len() == 2 {
        store.set(args[0].clone(), args[1].clone());
        return Ok(RespValue::ok());
    }

    let key = &args[0];
    let value = &args[1];
    let mut opts = SetOptions::default();
    let mut has_condition = false;

    let mut i = 2;
    while i < args.len() {
        let arg = &args[i];

        // Use case-insensitive comparison without allocation
        if eq_ignore_ascii_case(arg, b"NX") {
            if has_condition {
                return Err(Error::Syntax); // Multiple conditions
            }
            opts.condition = SetCondition::Nx;
            has_condition = true;
        } else if eq_ignore_ascii_case(arg, b"XX") {
            if has_condition {
                return Err(Error::Syntax); // Multiple conditions
            }
            opts.condition = SetCondition::Xx;
            has_condition = true;
        } else if eq_ignore_ascii_case(arg, b"IFEQ") {
            if has_condition {
                return Err(Error::Syntax);
            }
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            opts.condition = SetCondition::Ifeq(args[i].clone());
            has_condition = true;
        } else if eq_ignore_ascii_case(arg, b"IFNE") {
            if has_condition {
                return Err(Error::Syntax);
            }
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            opts.condition = SetCondition::Ifne(args[i].clone());
            has_condition = true;
        } else if eq_ignore_ascii_case(arg, b"IFDEQ") {
            if has_condition {
                return Err(Error::Syntax);
            }
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            let digest = std::str::from_utf8(&args[i])
                .map_err(|_| Error::Syntax)?
                .to_string();
            opts.condition = SetCondition::Ifdeq(digest);
            has_condition = true;
        } else if eq_ignore_ascii_case(arg, b"IFDNE") {
            if has_condition {
                return Err(Error::Syntax);
            }
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            let digest = std::str::from_utf8(&args[i])
                .map_err(|_| Error::Syntax)?
                .to_string();
            opts.condition = SetCondition::Ifdne(digest);
            has_condition = true;
        } else if eq_ignore_ascii_case(arg, b"GET") {
            opts.get = true;
        } else if eq_ignore_ascii_case(arg, b"KEEPTTL") {
            opts.keepttl = true;
        } else if eq_ignore_ascii_case(arg, b"EX") {
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            opts.ex = Some(parse_int(&args[i])?);
        } else if eq_ignore_ascii_case(arg, b"PX") {
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            opts.px = Some(parse_int(&args[i])?);
        } else if eq_ignore_ascii_case(arg, b"EXAT") {
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            opts.exat = Some(parse_int(&args[i])?);
        } else if eq_ignore_ascii_case(arg, b"PXAT") {
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            opts.pxat = Some(parse_int(&args[i])?);
        } else {
            return Err(Error::Syntax);
        }
        i += 1;
    }

    // Get old value if GET option - must return WRONGTYPE if key exists but is not a string
    let old_value = if opts.get {
        store.get_or_wrongtype(key)?
    } else {
        None
    };

    // Check conditions
    let exists = store.exists(key);
    match &opts.condition {
        SetCondition::None => {}
        SetCondition::Nx => {
            if exists {
                return Ok(if opts.get {
                    old_value.map(RespValue::bulk).unwrap_or(RespValue::null())
                } else {
                    RespValue::null()
                });
            }
        }
        SetCondition::Xx => {
            if !exists {
                return Ok(RespValue::null());
            }
        }
        SetCondition::Ifeq(expected) => {
            let current = store.get(key);
            if current.as_ref().map(|v| v.as_ref()) != Some(expected.as_ref()) {
                return Ok(if opts.get {
                    old_value.map(RespValue::bulk).unwrap_or(RespValue::null())
                } else {
                    RespValue::null()
                });
            }
        }
        SetCondition::Ifne(expected) => {
            let current = store.get(key);
            if current.as_ref().map(|v| v.as_ref()) == Some(expected.as_ref()) {
                return Ok(if opts.get {
                    old_value.map(RespValue::bulk).unwrap_or(RespValue::null())
                } else {
                    RespValue::null()
                });
            }
        }
        SetCondition::Ifdeq(expected_digest) => {
            if let Some(current) = store.get(key) {
                let current_hash = xxhash_rust::xxh3::xxh3_64(&current);
                let current_digest = format!("{:016x}", current_hash);
                if current_digest != *expected_digest {
                    return Ok(if opts.get {
                        old_value.map(RespValue::bulk).unwrap_or(RespValue::null())
                    } else {
                        RespValue::null()
                    });
                }
            } else {
                // Key doesn't exist, digest can't match
                return Ok(RespValue::null());
            }
        }
        SetCondition::Ifdne(expected_digest) => {
            if let Some(current) = store.get(key) {
                let current_hash = xxhash_rust::xxh3::xxh3_64(&current);
                let current_digest = format!("{:016x}", current_hash);
                if current_digest == *expected_digest {
                    return Ok(if opts.get {
                        old_value.map(RespValue::bulk).unwrap_or(RespValue::null())
                    } else {
                        RespValue::null()
                    });
                }
            }
            // Key doesn't exist or digest doesn't match - proceed with set
        }
    }

    // Calculate expiration
    let expire_ms = if let Some(ex) = opts.ex {
        Some(ex * 1000)
    } else if let Some(px) = opts.px {
        Some(px)
    } else if let Some(exat) = opts.exat {
        Some(exat * 1000 - now_ms())
    } else if let Some(pxat) = opts.pxat {
        Some(pxat - now_ms())
    } else if opts.keepttl {
        // Get existing TTL
        store.get_with_ttl(key).and_then(|(_, ttl)| ttl)
    } else {
        None
    };

    // Set the value
    match expire_ms {
        Some(ms) if ms > 0 => store.set_ex(key.clone(), value.clone(), ms),
        _ => store.set(key.clone(), value.clone()),
    }

    Ok(if opts.get {
        old_value.map(RespValue::bulk).unwrap_or(RespValue::null())
    } else {
        RespValue::ok()
    })
}

/// SETNX key value
fn cmd_setnx(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 2 {
        return Err(Error::WrongArity("SETNX"));
    }
    let result = store.setnx(args[0].clone(), args[1].clone());
    Ok(RespValue::integer(if result { 1 } else { 0 }))
}

/// SETEX key seconds value
fn cmd_setex(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 3 {
        return Err(Error::WrongArity("SETEX"));
    }
    let seconds = parse_int(&args[1])?;
    if seconds <= 0 {
        return Err(Error::InvalidExpireTime);
    }
    store.set_ex(args[0].clone(), args[2].clone(), seconds * 1000);
    Ok(RespValue::ok())
}

/// PSETEX key milliseconds value
fn cmd_psetex(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 3 {
        return Err(Error::WrongArity("PSETEX"));
    }
    let ms = parse_int(&args[1])?;
    if ms <= 0 {
        return Err(Error::InvalidExpireTime);
    }
    store.set_ex(args[0].clone(), args[2].clone(), ms);
    Ok(RespValue::ok())
}

/// MGET key [key ...]
fn cmd_mget(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("MGET"));
    }
    let results = store.mget(args);
    Ok(RespValue::array(
        results
            .into_iter()
            .map(|v| v.map(RespValue::bulk).unwrap_or(RespValue::null()))
            .collect(),
    ))
}

/// MSET key value [key value ...]
fn cmd_mset(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() || !args.len().is_multiple_of(2) {
        return Err(Error::WrongArity("MSET"));
    }
    let pairs: Vec<(Bytes, Bytes)> = args
        .chunks_exact(2)
        .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
        .collect();
    store.mset(pairs);
    Ok(RespValue::ok())
}

/// MSETNX key value [key value ...]
fn cmd_msetnx(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() || !args.len().is_multiple_of(2) {
        return Err(Error::WrongArity("MSETNX"));
    }

    // Check if any key exists
    for chunk in args.chunks_exact(2) {
        if store.exists(&chunk[0]) {
            return Ok(RespValue::integer(0));
        }
    }

    // Set all keys
    let pairs: Vec<(Bytes, Bytes)> = args
        .chunks_exact(2)
        .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
        .collect();
    store.mset(pairs);
    Ok(RespValue::integer(1))
}

/// MSETEX numkeys key value [key value ...] [NX | XX] [EX seconds | PX ms | EXAT | PXAT | KEEPTTL]
fn cmd_msetex(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("MSETEX"));
    }

    // Parse numkeys - must return specific error message for invalid values
    let numkeys_i64 = match parse_int(&args[0]) {
        Ok(n) => n,
        Err(_) => return Err(Error::Custom("invalid numkeys value".into())),
    };
    if numkeys_i64 <= 0 {
        return Err(Error::Custom("invalid numkeys value".into()));
    }

    // Check for overflow when converting to usize (numkeys * 2 must not overflow)
    let numkeys = numkeys_i64 as usize;
    let kv_count = match numkeys.checked_mul(2) {
        Some(c) => c,
        None => return Err(Error::Custom("invalid numkeys value".into())),
    };

    // Check if we have enough arguments for key-value pairs
    // args[0] is numkeys, then we need numkeys*2 args for keys and values
    if args.len() < 1 + kv_count {
        return Err(Error::Custom("wrong number of key-value pairs".into()));
    }

    // Parse key-value pairs
    let pairs: Vec<(Bytes, Bytes)> = args[1..1 + kv_count]
        .chunks_exact(2)
        .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
        .collect();

    // Parse options - track which expiration options are set
    let mut nx = false;
    let mut xx = false;
    let mut has_ex = false;
    let mut has_px = false;
    let mut has_exat = false;
    let mut has_pxat = false;
    let mut expire_ms: Option<i64> = None;
    let mut expire_at_ms: Option<i64> = None;
    let mut keepttl = false;

    let mut i = 1 + kv_count;
    while i < args.len() {
        let arg = &args[i];
        if eq_ignore_ascii_case(arg, b"NX") {
            nx = true;
        } else if eq_ignore_ascii_case(arg, b"XX") {
            xx = true;
        } else if eq_ignore_ascii_case(arg, b"KEEPTTL") {
            keepttl = true;
        } else if eq_ignore_ascii_case(arg, b"EX") {
            has_ex = true;
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            expire_ms = Some(parse_int(&args[i])? * 1000);
        } else if eq_ignore_ascii_case(arg, b"PX") {
            has_px = true;
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            expire_ms = Some(parse_int(&args[i])?);
        } else if eq_ignore_ascii_case(arg, b"EXAT") {
            has_exat = true;
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            expire_at_ms = Some(parse_int(&args[i])? * 1000);
        } else if eq_ignore_ascii_case(arg, b"PXAT") {
            has_pxat = true;
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            expire_at_ms = Some(parse_int(&args[i])?);
        } else {
            return Err(Error::Syntax);
        }
        i += 1;
    }

    // NX and XX are mutually exclusive
    if nx && xx {
        return Err(Error::Syntax);
    }

    // Expiration options are mutually exclusive (EX, PX, EXAT, PXAT)
    let has_expiration = has_ex || has_px || has_exat || has_pxat;
    let expire_count = [has_ex, has_px, has_exat, has_pxat]
        .iter()
        .filter(|&&x| x)
        .count();
    if expire_count > 1 {
        return Err(Error::Syntax);
    }

    // KEEPTTL conflicts with expiration flags
    if keepttl && has_expiration {
        return Err(Error::Syntax);
    }

    // Execute based on expiration type
    let success = if let Some(expire_at) = expire_at_ms {
        store.mset_exat(pairs, expire_at, nx, xx)
    } else if let Some(expire) = expire_ms {
        store.mset_ex(pairs, expire, nx, xx)
    } else if keepttl {
        store.mset_keepttl(pairs, nx, xx)
    } else {
        // No expiration specified - use default MSET behavior
        if nx || xx {
            // Check conditions for NX/XX
            for (key, _) in &pairs {
                let exists = store.exists(key);
                if nx && exists {
                    return Ok(RespValue::integer(0));
                }
                if xx && !exists {
                    return Ok(RespValue::integer(0));
                }
            }
        }
        store.mset(pairs);
        true
    };

    // MSETEX always returns integer in Redis 8.4: 1 for success, 0 for NX/XX failure
    Ok(RespValue::integer(if success { 1 } else { 0 }))
}

/// APPEND key value
fn cmd_append(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 2 {
        return Err(Error::WrongArity("APPEND"));
    }
    let len = store.append(args[0].clone(), &args[1])?;
    Ok(RespValue::integer(len as i64))
}

/// STRLEN key
fn cmd_strlen(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("STRLEN"));
    }
    Ok(RespValue::integer(store.strlen(&args[0]) as i64))
}

/// INCR key
fn cmd_incr(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("INCR"));
    }
    let val = store.incr(args[0].clone(), 1)?;
    Ok(RespValue::integer(val))
}

/// INCRBY key increment
fn cmd_incrby(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 2 {
        return Err(Error::WrongArity("INCRBY"));
    }
    let delta = parse_int(&args[1])?;
    let val = store.incr(args[0].clone(), delta)?;
    Ok(RespValue::integer(val))
}

/// INCRBYFLOAT key increment
fn cmd_incrbyfloat(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 2 {
        return Err(Error::WrongArity("INCRBYFLOAT"));
    }
    let delta = parse_float(&args[1])?;
    let val = store.incr_float(args[0].clone(), delta)?;
    Ok(RespValue::bulk_string(&format_float(val)))
}

/// DECR key
fn cmd_decr(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("DECR"));
    }
    let val = store.incr(args[0].clone(), -1)?;
    Ok(RespValue::integer(val))
}

/// DECRBY key decrement
fn cmd_decrby(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 2 {
        return Err(Error::WrongArity("DECRBY"));
    }
    let delta = parse_int(&args[1])?;
    let val = store.incr(args[0].clone(), -delta)?;
    Ok(RespValue::integer(val))
}

/// GETRANGE key start end
fn cmd_getrange(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 3 {
        return Err(Error::WrongArity("GETRANGE"));
    }
    let start = parse_int(&args[1])?;
    let end = parse_int(&args[2])?;
    let result = store.getrange(&args[0], start, end)?;
    Ok(RespValue::bulk(result))
}

/// SETRANGE key offset value
fn cmd_setrange(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 3 {
        return Err(Error::WrongArity("SETRANGE"));
    }
    let offset = parse_int(&args[1])?;
    if offset < 0 {
        return Err(Error::Other("offset is out of range"));
    }
    // Max string size check (512MB)
    if offset as usize + args[2].len() > 512 * 1024 * 1024 {
        return Err(Error::Custom(
            "string exceeds maximum allowed size (512MB)".into(),
        ));
    }
    let len = store.setrange(args[0].clone(), offset as usize, &args[2])?;
    Ok(RespValue::integer(len as i64))
}

/// GETSET key value (deprecated but still supported)
fn cmd_getset(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 2 {
        return Err(Error::WrongArity("GETSET"));
    }
    let old = store.get(&args[0]);
    store.set(args[0].clone(), args[1].clone());
    Ok(old.map(RespValue::bulk).unwrap_or(RespValue::null()))
}

/// GETEX key [EXAT unix-time-seconds] [PXAT unix-time-ms] [EX seconds] [PX ms] [PERSIST]
fn cmd_getex(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("GETEX"));
    }

    let key = &args[0];
    let value = match store.get(key) {
        Some(v) => v,
        None => return Ok(RespValue::null()),
    };

    if args.len() > 1 {
        let opt = &args[1];
        let opt_upper: Vec<u8> = opt.iter().map(|b| b.to_ascii_uppercase()).collect();

        match opt_upper.as_slice() {
            b"PERSIST" => {
                store.persist(key);
            }
            b"EX" | b"PX" | b"EXAT" | b"PXAT" => {
                if args.len() < 3 {
                    return Err(Error::Syntax);
                }
                let time_val = parse_int(&args[2])?;
                match opt_upper.as_slice() {
                    b"EX" => store.expire(key, time_val * 1000),
                    b"PX" => store.expire(key, time_val),
                    b"EXAT" => {
                        let ttl = time_val * 1000 - now_ms();
                        store.expire(key, ttl)
                    }
                    b"PXAT" => {
                        let ttl = time_val - now_ms();
                        store.expire(key, ttl)
                    }
                    _ => unreachable!(),
                };
            }
            _ => return Err(Error::Syntax),
        }
    }

    Ok(RespValue::bulk(value))
}

/// GETDEL key
fn cmd_getdel(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("GETDEL"));
    }
    Ok(match store.getdel(&args[0]) {
        Some(v) => RespValue::bulk(v),
        None => RespValue::null(),
    })
}

/// LCS key1 key2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN]
fn cmd_lcs(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("LCS"));
    }

    let s1 = store.get(&args[0]).unwrap_or_default();
    let s2 = store.get(&args[1]).unwrap_or_default();

    let mut len_only = false;
    let mut idx = false;
    let mut min_match_len = 0usize;
    let mut with_match_len = false;

    let mut i = 2;
    while i < args.len() {
        let opt: Vec<u8> = args[i].iter().map(|b| b.to_ascii_uppercase()).collect();
        match opt.as_slice() {
            b"LEN" => len_only = true,
            b"IDX" => idx = true,
            b"WITHMATCHLEN" => with_match_len = true,
            b"MINMATCHLEN" => {
                i += 1;
                if i >= args.len() {
                    return Err(Error::Syntax);
                }
                min_match_len = parse_int(&args[i])?.max(0) as usize;
            }
            _ => return Err(Error::Syntax),
        }
        i += 1;
    }

    // Compute LCS using dynamic programming
    let (lcs_str, matches) = compute_lcs(&s1, &s2, idx, min_match_len);

    if len_only {
        return Ok(RespValue::integer(lcs_str.len() as i64));
    }

    if idx {
        let result = vec![
            RespValue::bulk_string("matches"),
            RespValue::array(
                matches
                    .into_iter()
                    .map(|(a_range, b_range, len)| {
                        let mut match_arr = vec![
                            RespValue::array(vec![
                                RespValue::integer(a_range.0 as i64),
                                RespValue::integer(a_range.1 as i64),
                            ]),
                            RespValue::array(vec![
                                RespValue::integer(b_range.0 as i64),
                                RespValue::integer(b_range.1 as i64),
                            ]),
                        ];
                        if with_match_len {
                            match_arr.push(RespValue::integer(len as i64));
                        }
                        RespValue::array(match_arr)
                    })
                    .collect(),
            ),
            RespValue::bulk_string("len"),
            RespValue::integer(lcs_str.len() as i64),
        ];
        return Ok(RespValue::array(result));
    }

    Ok(RespValue::bulk(lcs_str))
}

/// Compute LCS with optional index tracking
/// Ported exactly from Redis t_string.c lcsCommand()
fn compute_lcs(
    a: &[u8],
    b: &[u8],
    track_idx: bool,
    min_match_len: usize,
) -> (Bytes, Vec<((usize, usize), (usize, usize), usize)>) {
    let alen = a.len();
    let blen = b.len();

    if alen == 0 || blen == 0 {
        return (Bytes::new(), vec![]);
    }

    // DP table using flat array like Redis: LCS[i,j] = lcs[j + i*(blen+1)]
    let mut lcs_table = vec![0u32; (alen + 1) * (blen + 1)];
    let lcs = |i: usize, j: usize| -> usize { j + i * (blen + 1) };

    // Build the LCS table
    for i in 1..=alen {
        for j in 1..=blen {
            if a[i - 1] == b[j - 1] {
                lcs_table[lcs(i, j)] = lcs_table[lcs(i - 1, j - 1)] + 1;
            } else {
                let lcs1 = lcs_table[lcs(i - 1, j)];
                let lcs2 = lcs_table[lcs(i, j - 1)];
                lcs_table[lcs(i, j)] = lcs1.max(lcs2);
            }
        }
    }

    let lcs_len = lcs_table[lcs(alen, blen)] as usize;
    let mut result = vec![0u8; lcs_len];
    let mut matches = Vec::new();

    // Track current contiguous match range
    // alen signals "no range in progress" (sentinel value like Redis)
    let mut arange_start: usize = alen;
    let mut arange_end: usize = 0;
    let mut brange_start: usize = 0;
    let mut brange_end: usize = 0;

    let mut i = alen;
    let mut j = blen;
    let mut idx = lcs_len;

    // Backtrack to find LCS and matching ranges (exactly like Redis)
    while idx > 0 && i > 0 && j > 0 {
        let mut emit_range = false;

        if a[i - 1] == b[j - 1] {
            // Match found - store the character
            result[idx - 1] = a[i - 1];

            if track_idx {
                if arange_start == alen {
                    // Start a new range
                    arange_start = i - 1;
                    arange_end = i - 1;
                    brange_start = j - 1;
                    brange_end = j - 1;
                } else {
                    // Try to extend the range backward (contiguous match)
                    if arange_start == i && brange_start == j {
                        arange_start -= 1;
                        brange_start -= 1;
                    } else {
                        // Not contiguous, emit current range
                        emit_range = true;
                    }
                }

                // Emit if we reached the start of either string
                if arange_start == 0 || brange_start == 0 {
                    emit_range = true;
                }
            }

            idx -= 1;
            i -= 1;
            j -= 1;
        } else {
            // No match - follow the larger LCS path
            let lcs1 = lcs_table[lcs(i - 1, j)];
            let lcs2 = lcs_table[lcs(i, j - 1)];
            if lcs1 > lcs2 {
                i -= 1;
            } else {
                j -= 1;
            }
            // If we had a range in progress, emit it
            if track_idx && arange_start != alen {
                emit_range = true;
            }
        }

        // Emit the current range if needed (exactly like Redis)
        if emit_range {
            let match_len = arange_end - arange_start + 1;
            if min_match_len == 0 || match_len >= min_match_len {
                matches.push((
                    (arange_start, arange_end),
                    (brange_start, brange_end),
                    match_len,
                ));
            }
            arange_start = alen; // Reset sentinel - restart at next match
        }
    }

    (Bytes::from(result), matches)
}

/// Format float like Redis
fn format_float(f: f64) -> String {
    if f.fract() == 0.0 && f.abs() < 1e15 {
        format!("{:.0}", f)
    } else {
        let s = format!("{:.17}", f);
        s.trim_end_matches('0').trim_end_matches('.').to_string()
    }
}

/// DIGEST key
/// Get the XXH3 hash digest for the value stored in the specified key as a hexadecimal string.
/// Keys must be of type string.
/// Available since Redis 8.4.0
fn cmd_digest(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("DIGEST"));
    }

    // Get string value, returning WRONGTYPE if key exists but is not a string
    match store.get_or_wrongtype(&args[0])? {
        Some(value) => {
            // Compute XXH3 64-bit hash
            let hash = xxhash_rust::xxh3::xxh3_64(&value);
            // Format as lowercase hexadecimal string (16 chars for 64-bit)
            Ok(RespValue::bulk_string(&format!("{:016x}", hash)))
        }
        None => Ok(RespValue::null()),
    }
}

/// DELEX key [IFEQ value | IFNE value | IFDEQ digest | IFDNE digest]
/// Conditionally removes the specified key based on value or hash digest comparison.
/// Available since Redis 8.4.0
fn cmd_delex(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() || args.len() > 3 {
        return Err(Error::WrongArity("DELEX"));
    }

    let key = &args[0];

    // If no condition, just delete (like DEL)
    if args.len() == 1 {
        let deleted = store.del(key);
        return Ok(RespValue::integer(if deleted { 1 } else { 0 }));
    }

    // Parse condition
    if args.len() != 3 {
        return Err(Error::Syntax);
    }

    let condition = &args[1];
    let cond_value = &args[2];

    // Get current value
    let current = match store.get_or_wrongtype(key)? {
        Some(v) => v,
        None => return Ok(RespValue::integer(0)), // Key doesn't exist
    };

    let should_delete = if eq_ignore_ascii_case(condition, b"IFEQ") {
        // Delete if value equals
        current.as_ref() == cond_value.as_ref()
    } else if eq_ignore_ascii_case(condition, b"IFNE") {
        // Delete if value not equals
        current.as_ref() != cond_value.as_ref()
    } else if eq_ignore_ascii_case(condition, b"IFDEQ") {
        // Delete if digest equals
        let current_hash = xxhash_rust::xxh3::xxh3_64(&current);
        let current_digest = format!("{:016x}", current_hash);
        let cond_digest = std::str::from_utf8(cond_value).unwrap_or("");
        current_digest == cond_digest
    } else if eq_ignore_ascii_case(condition, b"IFDNE") {
        // Delete if digest not equals
        let current_hash = xxhash_rust::xxh3::xxh3_64(&current);
        let current_digest = format!("{:016x}", current_hash);
        let cond_digest = std::str::from_utf8(cond_value).unwrap_or("");
        current_digest != cond_digest
    } else {
        return Err(Error::Syntax);
    };

    if should_delete {
        store.del(key);
        Ok(RespValue::integer(1))
    } else {
        Ok(RespValue::integer(0))
    }
}
