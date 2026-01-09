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

/// SET command options
#[derive(Default)]
struct SetOptions {
    nx: bool,          // Only set if not exists
    xx: bool,          // Only set if exists
    get: bool,         // Return old value
    ex: Option<i64>,   // Expire in seconds
    px: Option<i64>,   // Expire in milliseconds
    exat: Option<i64>, // Expire at unix time (seconds)
    pxat: Option<i64>, // Expire at unix time (ms)
    keepttl: bool,     // Keep existing TTL
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

/// SET key value [NX|XX] [GET] [EX seconds|PX milliseconds|EXAT unix-time|PXAT unix-time-ms] [KEEPTTL]
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

    let mut i = 2;
    while i < args.len() {
        let arg = &args[i];

        // Use case-insensitive comparison without allocation
        if eq_ignore_ascii_case(arg, b"NX") {
            opts.nx = true;
        } else if eq_ignore_ascii_case(arg, b"XX") {
            opts.xx = true;
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

    // NX and XX are mutually exclusive
    if opts.nx && opts.xx {
        return Err(Error::Syntax);
    }

    // Get old value if GET option
    let old_value = if opts.get { store.get(key) } else { None };

    // Check NX/XX conditions
    let exists = store.exists(key);
    if opts.nx && exists {
        return Ok(if opts.get {
            old_value.map(RespValue::bulk).unwrap_or(RespValue::null())
        } else {
            RespValue::null()
        });
    }
    if opts.xx && !exists {
        return Ok(RespValue::null());
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

    // Parse numkeys
    let numkeys = parse_int(&args[0])? as usize;
    if numkeys == 0 {
        return Err(Error::Other("at least one key required"));
    }

    // Need numkeys * 2 arguments for keys and values, plus at least expiration option
    let min_args = 1 + numkeys * 2;
    if args.len() < min_args {
        return Err(Error::WrongArity("MSETEX"));
    }

    // Parse key-value pairs
    let pairs: Vec<(Bytes, Bytes)> = args[1..1 + numkeys * 2]
        .chunks_exact(2)
        .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
        .collect();

    // Parse options
    let mut nx = false;
    let mut xx = false;
    let mut expire_ms: Option<i64> = None;
    let mut expire_at_ms: Option<i64> = None;
    let _keepttl = false;

    let mut i = 1 + numkeys * 2;
    while i < args.len() {
        let arg = &args[i];
        if eq_ignore_ascii_case(arg, b"NX") {
            nx = true;
        } else if eq_ignore_ascii_case(arg, b"XX") {
            xx = true;
        } else if eq_ignore_ascii_case(arg, b"KEEPTTL") {
            // KEEPTTL not really applicable for MSETEX but we accept it
        } else if eq_ignore_ascii_case(arg, b"EX") {
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            expire_ms = Some(parse_int(&args[i])? * 1000);
        } else if eq_ignore_ascii_case(arg, b"PX") {
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            expire_ms = Some(parse_int(&args[i])?);
        } else if eq_ignore_ascii_case(arg, b"EXAT") {
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            expire_at_ms = Some(parse_int(&args[i])? * 1000);
        } else if eq_ignore_ascii_case(arg, b"PXAT") {
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

    // Execute based on expiration type
    let success = if let Some(expire_at) = expire_at_ms {
        store.mset_exat(pairs, expire_at, nx, xx)
    } else if let Some(expire) = expire_ms {
        store.mset_ex(pairs, expire, nx, xx)
    } else {
        // No expiration specified - use default MSET behavior
        if nx || xx {
            // Check conditions for NX/XX
            for (key, _) in &pairs {
                let exists = store.exists(key);
                if nx && exists {
                    return Ok(RespValue::null());
                }
                if xx && !exists {
                    return Ok(RespValue::null());
                }
            }
        }
        store.mset(pairs);
        true
    };

    if success {
        Ok(RespValue::ok())
    } else {
        Ok(RespValue::null())
    }
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
    let result = store.getrange(&args[0], start, end);
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
fn compute_lcs(
    a: &[u8],
    b: &[u8],
    track_idx: bool,
    min_match_len: usize,
) -> (Bytes, Vec<((usize, usize), (usize, usize), usize)>) {
    let m = a.len();
    let n = b.len();

    if m == 0 || n == 0 {
        return (Bytes::new(), vec![]);
    }

    // DP table
    let mut dp = vec![vec![0u16; n + 1]; m + 1];

    for i in 1..=m {
        for j in 1..=n {
            if a[i - 1] == b[j - 1] {
                dp[i][j] = dp[i - 1][j - 1] + 1;
            } else {
                dp[i][j] = dp[i - 1][j].max(dp[i][j - 1]);
            }
        }
    }

    // Backtrack to find LCS
    let mut lcs = Vec::with_capacity(dp[m][n] as usize);
    let mut i = m;
    let mut j = n;

    while i > 0 && j > 0 {
        if a[i - 1] == b[j - 1] {
            lcs.push(a[i - 1]);
            i -= 1;
            j -= 1;
        } else if dp[i - 1][j] > dp[i][j - 1] {
            i -= 1;
        } else {
            j -= 1;
        }
    }

    lcs.reverse();

    // Track matches if needed
    let matches = if track_idx {
        find_lcs_matches(a, b, &lcs, min_match_len)
    } else {
        vec![]
    };

    (Bytes::from(lcs), matches)
}

/// Find matching ranges for LCS
fn find_lcs_matches(
    a: &[u8],
    b: &[u8],
    _lcs: &[u8],
    min_len: usize,
) -> Vec<((usize, usize), (usize, usize), usize)> {
    let mut matches = vec![];
    let m = a.len();
    let n = b.len();

    // Find common substrings
    let mut i = 0;
    while i < m {
        let mut j = 0;
        while j < n {
            if a[i] == b[j] {
                let start_i = i;
                let start_j = j;
                while i < m && j < n && a[i] == b[j] {
                    i += 1;
                    j += 1;
                }
                let len = i - start_i;
                if len >= min_len {
                    matches.push(((start_i, i - 1), (start_j, j - 1), len));
                }
            } else {
                j += 1;
            }
        }
        i += 1;
    }

    matches
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
