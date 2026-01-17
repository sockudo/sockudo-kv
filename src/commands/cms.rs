//! Count-Min Sketch command handlers
//!
//! Implements all CMS.* Redis commands.

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::storage::Store;

/// Execute Count-Min Sketch commands
pub fn execute(store: &Store, cmd: &[u8], args: &[Bytes]) -> Result<RespValue> {
    match cmd.to_ascii_uppercase().as_slice() {
        b"CMS.INITBYDIM" => cmd_cms_initbydim(store, args),
        b"CMS.INITBYPROB" => cmd_cms_initbyprob(store, args),
        b"CMS.INCRBY" => cmd_cms_incrby(store, args),
        b"CMS.QUERY" => cmd_cms_query(store, args),
        b"CMS.MERGE" => cmd_cms_merge(store, args),
        b"CMS.INFO" => cmd_cms_info(store, args),
        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd).into_owned(),
        )),
    }
}

/// CMS.INITBYDIM {key} {width} {depth}
fn cmd_cms_initbydim(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("CMS.INITBYDIM"));
    }

    let key = args[0].clone();
    let width: usize = String::from_utf8_lossy(&args[1])
        .parse()
        .map_err(|_| Error::Other("ERR bad width"))?;
    let depth: usize = String::from_utf8_lossy(&args[2])
        .parse()
        .map_err(|_| Error::Other("ERR bad depth"))?;

    if width == 0 {
        return Err(Error::Other("ERR width should be larger than 0"));
    }
    if depth == 0 {
        return Err(Error::Other("ERR depth should be larger than 0"));
    }

    match store.cms_initbydim(key, width, depth) {
        Ok(_) => Ok(RespValue::ok()),
        Err(e) => Err(e),
    }
}

/// CMS.INITBYPROB {key} {error} {probability}
fn cmd_cms_initbyprob(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("CMS.INITBYPROB"));
    }

    let key = args[0].clone();
    let error: f64 = String::from_utf8_lossy(&args[1])
        .parse()
        .map_err(|_| Error::Other("ERR bad error"))?;
    let probability: f64 = String::from_utf8_lossy(&args[2])
        .parse()
        .map_err(|_| Error::Other("ERR bad probability"))?;

    match store.cms_initbyprob(key, error, probability) {
        Ok(_) => Ok(RespValue::ok()),
        Err(e) => Err(e),
    }
}

/// CMS.INCRBY {key} {item} {increment} [{item} {increment} ...]
fn cmd_cms_incrby(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return Err(Error::WrongArity("CMS.INCRBY"));
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

    match store.cms_incrby(key, &items) {
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

/// CMS.QUERY {key} {item} [item ...]
fn cmd_cms_query(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("CMS.QUERY"));
    }

    let key = &args[0];
    let items: Vec<&[u8]> = args[1..].iter().map(|b| b.as_ref()).collect();

    match store.cms_query(key, &items) {
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

/// CMS.MERGE {dest} {numKeys} {src} [src ...] [WEIGHTS {weight} [weight ...]]
fn cmd_cms_merge(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("CMS.MERGE"));
    }

    let dest_key = args[0].clone();
    let num_keys: usize = String::from_utf8_lossy(&args[1])
        .parse()
        .map_err(|_| Error::Other("ERR bad numKeys"))?;

    if args.len() < 2 + num_keys {
        return Err(Error::WrongArity("CMS.MERGE"));
    }

    let source_keys: Vec<Bytes> = args[2..2 + num_keys].to_vec();

    // Check for WEIGHTS option
    let mut weights: Option<Vec<u64>> = None;
    let mut i = 2 + num_keys;
    while i < args.len() {
        if args[i].to_ascii_uppercase() == b"WEIGHTS".as_slice() {
            i += 1;
            if args.len() < i + num_keys {
                return Err(Error::WrongArity("CMS.MERGE"));
            }
            let mut w = Vec::with_capacity(num_keys);
            for j in 0..num_keys {
                let weight: u64 = String::from_utf8_lossy(&args[i + j])
                    .parse()
                    .map_err(|_| Error::Other("ERR bad weight"))?;
                w.push(weight);
            }
            weights = Some(w);
            break;
        }
        i += 1;
    }

    match store.cms_merge(dest_key, &source_keys, weights.as_deref()) {
        Ok(()) => Ok(RespValue::ok()),
        Err(e) => Err(e),
    }
}

/// CMS.INFO {key}
fn cmd_cms_info(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("CMS.INFO"));
    }

    let key = &args[0];

    match store.cms_info(key)? {
        Some((width, depth, count)) => Ok(RespValue::Array(vec![
            RespValue::bulk_string("width"),
            RespValue::integer(width as i64),
            RespValue::bulk_string("depth"),
            RespValue::integer(depth as i64),
            RespValue::bulk_string("count"),
            RespValue::integer(count as i64),
        ])),
        None => Err(Error::Other("ERR not found")),
    }
}
