//! JSON command handlers using sonic-rs for hyper-performance
//!
//! Implements all 25 RedisJSON commands with focus on performance.

use bytes::Bytes;
use sonic_rs::{JsonContainerTrait, JsonValueTrait, Value, from_slice, to_vec};

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

#[inline]
fn bytes_to_str(b: &[u8]) -> Result<&str> {
    std::str::from_utf8(b).map_err(|_| Error::Syntax)
}

pub fn execute(store: &Store, cmd: &[u8], args: &[Bytes]) -> Result<RespValue> {
    // All JSON commands start with "JSON."
    match cmd {
        b"JSON.SET" => cmd_json_set(store, args),
        b"JSON.GET" => cmd_json_get(store, args),
        b"JSON.DEL" => cmd_json_del(store, args),
        b"JSON.FORGET" => cmd_json_del(store, args), // Alias for DEL
        b"JSON.TYPE" => cmd_json_type(store, args),
        b"JSON.ARRAPPEND" => cmd_json_arrappend(store, args),
        b"JSON.ARRINDEX" => cmd_json_arrindex(store, args),
        b"JSON.ARRINSERT" => cmd_json_arrinsert(store, args),
        b"JSON.ARRLEN" => cmd_json_arrlen(store, args),
        b"JSON.ARRPOP" => cmd_json_arrpop(store, args),
        b"JSON.ARRTRIM" => cmd_json_arrtrim(store, args),
        b"JSON.CLEAR" => cmd_json_clear(store, args),
        b"JSON.DEBUG" => cmd_json_debug(store, args),
        b"JSON.MERGE" => cmd_json_merge(store, args),
        b"JSON.MGET" => cmd_json_mget(store, args),
        b"JSON.MSET" => cmd_json_mset(store, args),
        b"JSON.NUMINCRBY" => cmd_json_numincrby(store, args),
        b"JSON.NUMMULTBY" => cmd_json_nummultby(store, args),
        b"JSON.OBJKEYS" => cmd_json_objkeys(store, args),
        b"JSON.OBJLEN" => cmd_json_objlen(store, args),
        b"JSON.RESP" => cmd_json_resp(store, args),
        b"JSON.STRAPPEND" => cmd_json_strappend(store, args),
        b"JSON.STRLEN" => cmd_json_strlen(store, args),
        b"JSON.TOGGLE" => cmd_json_toggle(store, args),
        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd).into_owned(),
        )),
    }
}

/// JSON.SET key path value [NX|XX]
fn cmd_json_set(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("JSON.SET"));
    }

    let key = args[0].clone();
    let path = bytes_to_str(&args[1])?;
    let value = &args[2];

    let mut nx = false;
    let mut xx = false;

    for arg in args.iter().skip(3) {
        if arg.eq_ignore_ascii_case(b"NX") {
            nx = true;
        } else if arg.eq_ignore_ascii_case(b"XX") {
            xx = true;
        }
    }

    if store.json_set(key, path, value, nx, xx)? {
        Ok(RespValue::ok())
    } else {
        Ok(RespValue::null())
    }
}

/// JSON.GET key [INDENT indent] [NEWLINE newline] [SPACE space] [path ...]
fn cmd_json_get(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("JSON.GET"));
    }

    let key = &args[0];
    let mut paths = Vec::new();
    let mut i = 1;

    // Skip formatting options (INDENT, NEWLINE, SPACE)
    while i < args.len() {
        let arg = &args[i];
        if arg.eq_ignore_ascii_case(b"INDENT")
            || arg.eq_ignore_ascii_case(b"NEWLINE")
            || arg.eq_ignore_ascii_case(b"SPACE")
        {
            i += 2; // Skip option and value
        } else {
            paths.push(bytes_to_str(arg)?);
            i += 1;
        }
    }

    match store.json_get(key, &paths) {
        Some(json) => Ok(RespValue::bulk(json)),
        None => Ok(RespValue::null()),
    }
}

/// JSON.DEL key [path]
fn cmd_json_del(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("JSON.DEL"));
    }

    let key = &args[0];
    let path = if args.len() > 1 {
        Some(bytes_to_str(&args[1])?)
    } else {
        None
    };

    let deleted = store.json_del(key, path)?;
    Ok(RespValue::integer(deleted as i64))
}

/// JSON.TYPE key [path]
fn cmd_json_type(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("JSON.TYPE"));
    }

    let key = &args[0];
    let path = if args.len() > 1 {
        Some(bytes_to_str(&args[1])?)
    } else {
        None
    };

    match store.json_type(key, path) {
        Some(types) => {
            if types.len() == 1 {
                Ok(RespValue::bulk_string(types[0]))
            } else {
                Ok(RespValue::array(
                    types.into_iter().map(RespValue::bulk_string).collect(),
                ))
            }
        }
        None => Ok(RespValue::null()),
    }
}

/// JSON.ARRAPPEND key path value [value ...]
fn cmd_json_arrappend(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("JSON.ARRAPPEND"));
    }

    let key = &args[0];
    let path = bytes_to_str(&args[1])?;

    let values: Result<Vec<Value>> = args[2..]
        .iter()
        .map(|b| from_slice(b).map_err(|_| Error::JsonParse))
        .collect();

    let results = store.json_arrappend(key, path, values?)?;
    build_int_array_response(results)
}

/// JSON.ARRINDEX key path value [start [stop]]
fn cmd_json_arrindex(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("JSON.ARRINDEX"));
    }

    let key = &args[0];
    let path = bytes_to_str(&args[1])?;
    let value: Value = from_slice(&args[2]).map_err(|_| Error::JsonParse)?;
    let start = if args.len() > 3 {
        parse_int(&args[3])?
    } else {
        0
    };
    let stop = if args.len() > 4 {
        parse_int(&args[4])?
    } else {
        0
    };

    let results = store.json_arrindex(key, path, &value, start, stop)?;
    build_int_array_response(results)
}

/// JSON.ARRINSERT key path index value [value ...]
fn cmd_json_arrinsert(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 4 {
        return Err(Error::WrongArity("JSON.ARRINSERT"));
    }

    let key = &args[0];
    let path = bytes_to_str(&args[1])?;
    let index = parse_int(&args[2])?;

    let values: Result<Vec<Value>> = args[3..]
        .iter()
        .map(|b| from_slice(b).map_err(|_| Error::JsonParse))
        .collect();

    let results = store.json_arrinsert(key, path, index, values?)?;
    build_int_array_response(results)
}

/// JSON.ARRLEN key [path]
fn cmd_json_arrlen(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("JSON.ARRLEN"));
    }

    let key = &args[0];
    let path = if args.len() > 1 {
        Some(bytes_to_str(&args[1])?)
    } else {
        None
    };

    let results = store.json_arrlen(key, path)?;
    build_int_array_response(results)
}

/// JSON.ARRPOP key [path [index]]
fn cmd_json_arrpop(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("JSON.ARRPOP"));
    }

    let key = &args[0];
    let path = if args.len() > 1 {
        Some(bytes_to_str(&args[1])?)
    } else {
        None
    };
    let index = if args.len() > 2 {
        parse_int(&args[2])?
    } else {
        -1
    };

    let results = store.json_arrpop(key, path, index)?;

    if results.len() == 1 {
        match &results[0] {
            Some(v) => {
                let bytes = to_vec(v).map_err(|_| Error::JsonParse)?;
                Ok(RespValue::bulk(Bytes::from(bytes)))
            }
            None => Ok(RespValue::null()),
        }
    } else {
        Ok(RespValue::array(
            results
                .into_iter()
                .map(|r| match r {
                    Some(v) => to_vec(&v)
                        .ok()
                        .map(|b| RespValue::bulk(Bytes::from(b)))
                        .unwrap_or(RespValue::null()),
                    None => RespValue::null(),
                })
                .collect(),
        ))
    }
}

/// JSON.ARRTRIM key path start stop
fn cmd_json_arrtrim(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 4 {
        return Err(Error::WrongArity("JSON.ARRTRIM"));
    }

    let key = &args[0];
    let path = bytes_to_str(&args[1])?;
    let start = parse_int(&args[2])?;
    let stop = parse_int(&args[3])?;

    let results = store.json_arrtrim(key, path, start, stop)?;
    build_int_array_response(results)
}

/// JSON.CLEAR key [path]
fn cmd_json_clear(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("JSON.CLEAR"));
    }

    let key = &args[0];
    let path = if args.len() > 1 {
        Some(bytes_to_str(&args[1])?)
    } else {
        None
    };

    let cleared = store.json_clear(key, path)?;
    Ok(RespValue::integer(cleared as i64))
}

/// JSON.DEBUG subcommand
fn cmd_json_debug(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("JSON.DEBUG"));
    }

    let subcmd = &args[0];
    if subcmd.eq_ignore_ascii_case(b"MEMORY") {
        if args.len() < 2 {
            return Err(Error::WrongArity("JSON.DEBUG MEMORY"));
        }
        let key = &args[1];
        let path = if args.len() > 2 {
            Some(bytes_to_str(&args[2])?)
        } else {
            None
        };

        let results = store.json_debug_memory(key, path)?;
        if results.len() == 1 {
            match results[0] {
                Some(size) => Ok(RespValue::integer(size as i64)),
                None => Ok(RespValue::null()),
            }
        } else {
            Ok(RespValue::array(
                results
                    .into_iter()
                    .map(|r| match r {
                        Some(size) => RespValue::integer(size as i64),
                        None => RespValue::null(),
                    })
                    .collect(),
            ))
        }
    } else {
        Err(Error::Syntax)
    }
}

/// JSON.MERGE key path value
fn cmd_json_merge(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("JSON.MERGE"));
    }

    let key = args[0].clone();
    let path = bytes_to_str(&args[1])?;
    let value = &args[2];

    store.json_merge(key, path, value)?;
    Ok(RespValue::ok())
}

/// JSON.MGET key [key ...] path
fn cmd_json_mget(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("JSON.MGET"));
    }

    // Last argument is the path
    let path = bytes_to_str(args.last().unwrap())?;
    let keys = &args[..args.len() - 1];

    let results = store.json_mget(keys, path);
    Ok(RespValue::array(
        results
            .into_iter()
            .map(|r| match r {
                Some(b) => RespValue::bulk(b),
                None => RespValue::null(),
            })
            .collect(),
    ))
}

/// JSON.MSET key path value [key path value ...]
fn cmd_json_mset(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 || !args.len().is_multiple_of(3) {
        return Err(Error::WrongArity("JSON.MSET"));
    }

    let items: Result<Vec<_>> = args
        .chunks_exact(3)
        .map(|chunk| {
            let key = chunk[0].clone();
            let path = bytes_to_str(&chunk[1])?;
            let value: &[u8] = &chunk[2];
            Ok((key, path, value))
        })
        .collect();

    store.json_mset(items?)?;
    Ok(RespValue::ok())
}

/// JSON.NUMINCRBY key path value
fn cmd_json_numincrby(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 3 {
        return Err(Error::WrongArity("JSON.NUMINCRBY"));
    }

    let key = &args[0];
    let path = bytes_to_str(&args[1])?;
    let value = parse_float(&args[2])?;

    let results = store.json_numincrby(key, path, value)?;
    build_float_array_response(results)
}

/// JSON.NUMMULTBY key path value
fn cmd_json_nummultby(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 3 {
        return Err(Error::WrongArity("JSON.NUMMULTBY"));
    }

    let key = &args[0];
    let path = bytes_to_str(&args[1])?;
    let value = parse_float(&args[2])?;

    let results = store.json_nummultby(key, path, value)?;
    build_float_array_response(results)
}

/// JSON.OBJKEYS key [path]
fn cmd_json_objkeys(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("JSON.OBJKEYS"));
    }

    let key = &args[0];
    let path = if args.len() > 1 {
        Some(bytes_to_str(&args[1])?)
    } else {
        None
    };

    let results = store.json_objkeys(key, path)?;
    if results.len() == 1 {
        match &results[0] {
            Some(keys) => Ok(RespValue::array(
                keys.iter().map(|k| RespValue::bulk_string(k)).collect(),
            )),
            None => Ok(RespValue::null()),
        }
    } else {
        Ok(RespValue::array(
            results
                .into_iter()
                .map(|r| match r {
                    Some(keys) => RespValue::array(
                        keys.into_iter()
                            .map(|k| RespValue::bulk_string(&k))
                            .collect(),
                    ),
                    None => RespValue::null(),
                })
                .collect(),
        ))
    }
}

/// JSON.OBJLEN key [path]
fn cmd_json_objlen(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("JSON.OBJLEN"));
    }

    let key = &args[0];
    let path = if args.len() > 1 {
        Some(bytes_to_str(&args[1])?)
    } else {
        None
    };

    let results = store.json_objlen(key, path)?;
    build_int_array_response(results)
}

/// JSON.RESP key [path]
fn cmd_json_resp(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("JSON.RESP"));
    }

    let key = &args[0];
    let path = if args.len() > 1 {
        Some(bytes_to_str(&args[1])?)
    } else {
        None
    };

    let results = store.json_resp(key, path)?;
    build_json_resp_response(results)
}

/// JSON.STRAPPEND key [path] value
fn cmd_json_strappend(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("JSON.STRAPPEND"));
    }

    let key = &args[0];
    let (path, value_idx) = if args.len() == 2 {
        ("$", 1)
    } else {
        (bytes_to_str(&args[1])?, 2)
    };

    // Value should be a JSON string
    let value: Value = from_slice(&args[value_idx]).map_err(|_| Error::JsonParse)?;
    let str_value = value
        .as_str()
        .ok_or(Error::JsonTypeMismatch("string", "other"))?;

    let results = store.json_strappend(key, path, str_value)?;
    build_int_array_response(results)
}

/// JSON.STRLEN key [path]
fn cmd_json_strlen(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("JSON.STRLEN"));
    }

    let key = &args[0];
    let path = if args.len() > 1 {
        Some(bytes_to_str(&args[1])?)
    } else {
        None
    };

    let results = store.json_strlen(key, path)?;
    build_int_array_response(results)
}

/// JSON.TOGGLE key path
fn cmd_json_toggle(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("JSON.TOGGLE"));
    }

    let key = &args[0];
    let path = bytes_to_str(&args[1])?;

    let results = store.json_toggle(key, path)?;
    if results.len() == 1 {
        match results[0] {
            Some(b) => Ok(RespValue::integer(if b { 1 } else { 0 })),
            None => Ok(RespValue::null()),
        }
    } else {
        Ok(RespValue::array(
            results
                .into_iter()
                .map(|r| match r {
                    Some(b) => RespValue::integer(if b { 1 } else { 0 }),
                    None => RespValue::null(),
                })
                .collect(),
        ))
    }
}

// ==================== Helper functions ====================

fn build_int_array_response(results: Vec<Option<i64>>) -> Result<RespValue> {
    if results.len() == 1 {
        match results[0] {
            Some(n) => Ok(RespValue::integer(n)),
            None => Ok(RespValue::null()),
        }
    } else {
        Ok(RespValue::array(
            results
                .into_iter()
                .map(|r| match r {
                    Some(n) => RespValue::integer(n),
                    None => RespValue::null(),
                })
                .collect(),
        ))
    }
}

fn build_float_array_response(results: Vec<Option<f64>>) -> Result<RespValue> {
    if results.len() == 1 {
        match results[0] {
            Some(n) => Ok(RespValue::bulk_string(&n.to_string())),
            None => Ok(RespValue::null()),
        }
    } else {
        Ok(RespValue::array(
            results
                .into_iter()
                .map(|r| match r {
                    Some(n) => RespValue::bulk_string(&n.to_string()),
                    None => RespValue::null(),
                })
                .collect(),
        ))
    }
}

fn build_json_resp_response(results: Vec<Option<Value>>) -> Result<RespValue> {
    fn value_to_resp(v: &Value) -> RespValue {
        if v.is_null() {
            RespValue::null()
        } else if let Some(b) = v.as_bool() {
            RespValue::bulk_string(if b { "true" } else { "false" })
        } else if let Some(n) = v.as_i64() {
            RespValue::integer(n)
        } else if let Some(n) = v.as_f64() {
            RespValue::bulk_string(&n.to_string())
        } else if let Some(s) = v.as_str() {
            RespValue::bulk_string(s)
        } else if let Some(arr) = v.as_array() {
            let mut resp = vec![RespValue::bulk_string("[")];
            for item in arr {
                resp.push(value_to_resp(item));
            }
            RespValue::array(resp)
        } else if let Some(obj) = v.as_object() {
            let mut resp = vec![RespValue::bulk_string("{")];
            for (k, val) in obj.iter() {
                resp.push(RespValue::bulk_string(k));
                resp.push(value_to_resp(val));
            }
            RespValue::array(resp)
        } else {
            RespValue::null()
        }
    }

    if results.len() == 1 {
        match &results[0] {
            Some(v) => Ok(value_to_resp(v)),
            None => Ok(RespValue::null()),
        }
    } else {
        Ok(RespValue::array(
            results
                .into_iter()
                .map(|r| match r {
                    Some(v) => value_to_resp(&v),
                    None => RespValue::null(),
                })
                .collect(),
        ))
    }
}
