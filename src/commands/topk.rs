//! Top-K command handlers
//!
//! Implements all TOPK.* Redis commands.

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::storage::Store;
use crate::storage::ops::topk_ops::TopKListResult;

/// Execute Top-K commands
pub fn execute(store: &Store, cmd: &[u8], args: &[Bytes]) -> Result<RespValue> {
    match cmd.to_ascii_uppercase().as_slice() {
        b"TOPK.RESERVE" => cmd_topk_reserve(store, args),
        b"TOPK.ADD" => cmd_topk_add(store, args),
        b"TOPK.INCRBY" => cmd_topk_incrby(store, args),
        b"TOPK.QUERY" => cmd_topk_query(store, args),
        b"TOPK.COUNT" => cmd_topk_count(store, args),
        b"TOPK.LIST" => cmd_topk_list(store, args),
        b"TOPK.INFO" => cmd_topk_info(store, args),
        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd).into_owned(),
        )),
    }
}

/// TOPK.RESERVE {key} {topk} [width] [depth] [decay]
fn cmd_topk_reserve(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("TOPK.RESERVE"));
    }

    let key = args[0].clone();
    let topk: usize = String::from_utf8_lossy(&args[1])
        .parse()
        .map_err(|_| Error::Other("ERR bad topk"))?;

    // Parse optional positional arguments: width, depth, decay
    let width = if args.len() > 2 {
        Some(
            String::from_utf8_lossy(&args[2])
                .parse::<usize>()
                .map_err(|_| Error::Other("ERR bad width"))?,
        )
    } else {
        None
    };

    let depth = if args.len() > 3 {
        Some(
            String::from_utf8_lossy(&args[3])
                .parse::<usize>()
                .map_err(|_| Error::Other("ERR bad depth"))?,
        )
    } else {
        None
    };

    let decay = if args.len() > 4 {
        Some(
            String::from_utf8_lossy(&args[4])
                .parse::<f64>()
                .map_err(|_| Error::Other("ERR bad decay"))?,
        )
    } else {
        None
    };

    if topk == 0 {
        return Err(Error::Other("ERR k should be larger than 0"));
    }

    match store.topk_reserve(key, topk, width, depth, decay) {
        Ok(_) => Ok(RespValue::ok()),
        Err(e) => Err(e),
    }
}

/// TOPK.ADD {key} {item} [item ...]
fn cmd_topk_add(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("TOPK.ADD"));
    }

    let key = &args[0];
    let items: Vec<&[u8]> = args[1..].iter().map(|b| b.as_ref()).collect();

    match store.topk_add(key, &items) {
        Ok(results) => {
            let resp_results: Vec<RespValue> = results
                .into_iter()
                .map(|opt| match opt {
                    Some(evicted) => RespValue::bulk(evicted),
                    None => RespValue::Null,
                })
                .collect();
            Ok(RespValue::Array(resp_results))
        }
        Err(e) => Err(e),
    }
}

/// TOPK.INCRBY {key} {item} {increment} [{item} {increment} ...]
fn cmd_topk_incrby(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return Err(Error::WrongArity("TOPK.INCRBY"));
    }

    let key = &args[0];
    let mut items: Vec<(&[u8], u64)> = Vec::with_capacity((args.len() - 1) / 2);

    let mut i = 1;
    while i + 1 < args.len() {
        let item = args[i].as_ref();
        let increment: u64 = String::from_utf8_lossy(&args[i + 1])
            .parse()
            .map_err(|_| Error::Other("ERR bad increment"))?;
        items.push((item, increment));
        i += 2;
    }

    match store.topk_incrby(key, &items) {
        Ok(results) => {
            let resp_results: Vec<RespValue> = results
                .into_iter()
                .map(|opt| match opt {
                    Some(evicted) => RespValue::bulk(evicted),
                    None => RespValue::Null,
                })
                .collect();
            Ok(RespValue::Array(resp_results))
        }
        Err(e) => Err(e),
    }
}

/// TOPK.QUERY {key} {item} [item ...]
fn cmd_topk_query(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("TOPK.QUERY"));
    }

    let key = &args[0];
    let items: Vec<&[u8]> = args[1..].iter().map(|b| b.as_ref()).collect();

    match store.topk_query(key, &items) {
        Ok(results) => {
            let resp_results: Vec<RespValue> = results
                .into_iter()
                .map(|b| RespValue::integer(if b { 1 } else { 0 }))
                .collect();
            Ok(RespValue::Array(resp_results))
        }
        Err(e) => Err(e),
    }
}

/// TOPK.COUNT {key} {item} [item ...]
fn cmd_topk_count(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("TOPK.COUNT"));
    }

    let key = &args[0];
    let items: Vec<&[u8]> = args[1..].iter().map(|b| b.as_ref()).collect();

    match store.topk_count(key, &items) {
        Ok(results) => {
            let resp_results: Vec<RespValue> = results
                .into_iter()
                .map(|c| RespValue::integer(c as i64))
                .collect();
            Ok(RespValue::Array(resp_results))
        }
        Err(e) => Err(e),
    }
}

/// TOPK.LIST {key} [WITHCOUNT]
fn cmd_topk_list(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("TOPK.LIST"));
    }

    let key = &args[0];
    let with_count = args.len() > 1 && args[1].to_ascii_uppercase() == b"WITHCOUNT".as_slice();

    match store.topk_list(key, with_count) {
        Ok(result) => match result {
            TopKListResult::Items(items) => {
                let resp_items: Vec<RespValue> = items.into_iter().map(RespValue::bulk).collect();
                Ok(RespValue::Array(resp_items))
            }
            TopKListResult::WithCount(items) => {
                let mut resp_items: Vec<RespValue> = Vec::with_capacity(items.len() * 2);
                for (item, count) in items {
                    resp_items.push(RespValue::bulk(item));
                    resp_items.push(RespValue::integer(count as i64));
                }
                Ok(RespValue::Array(resp_items))
            }
        },
        Err(e) => Err(e),
    }
}

/// TOPK.INFO {key}
fn cmd_topk_info(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("TOPK.INFO"));
    }

    let key = &args[0];

    match store.topk_info(key)? {
        Some((k, width, depth, decay)) => Ok(RespValue::Array(vec![
            RespValue::bulk_string("k"),
            RespValue::integer(k as i64),
            RespValue::bulk_string("width"),
            RespValue::integer(width as i64),
            RespValue::bulk_string("depth"),
            RespValue::integer(depth as i64),
            RespValue::bulk_string("decay"),
            RespValue::bulk_string(&format!("{:.3}", decay)),
        ])),
        None => Err(Error::Other("ERR not found")),
    }
}
