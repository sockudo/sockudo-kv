use bytes::Bytes;

use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::storage::Store;

/// Execute Bloom Filter commands
pub fn execute(store: &Store, cmd: &[u8], args: &[Bytes]) -> Result<RespValue> {
    match cmd.to_ascii_uppercase().as_slice() {
        b"BF.RESERVE" => cmd_bf_reserve(store, args),
        b"BF.ADD" => cmd_bf_add(store, args),
        b"BF.MADD" => cmd_bf_madd(store, args),
        b"BF.EXISTS" => cmd_bf_exists(store, args),
        b"BF.MEXISTS" => cmd_bf_mexists(store, args),
        b"BF.INFO" => cmd_bf_info(store, args),
        b"BF.CARD" => cmd_bf_card(store, args),
        b"BF.INSERT" => cmd_bf_insert(store, args),
        b"BF.SCANDUMP" => cmd_bf_scandump(store, args),
        b"BF.LOADCHUNK" => cmd_bf_loadchunk(store, args),
        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd).into_owned(),
        )),
    }
}

/// BF.RESERVE {key} {error_rate} {capacity} [EXPANSION expansion] [NONSCALING]
fn cmd_bf_reserve(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("BF.RESERVE"));
    }

    let key = args[0].clone();
    let error_rate: f64 = String::from_utf8_lossy(&args[1])
        .parse()
        .map_err(|_| Error::Other("ERR bad error rate"))?;
    let capacity: usize = String::from_utf8_lossy(&args[2])
        .parse()
        .map_err(|_| Error::Other("ERR bad capacity"))?;

    let mut expansion = 2;
    let mut nonscaling = false;

    let mut i = 3;
    while i < args.len() {
        let arg = args[i].to_ascii_uppercase();
        match arg.as_slice() {
            b"EXPANSION" => {
                if i + 1 >= args.len() {
                    return Err(Error::WrongArity("BF.RESERVE"));
                }
                expansion = String::from_utf8_lossy(&args[i + 1])
                    .parse()
                    .map_err(|_| Error::Other("ERR bad expansion"))?;
                i += 2;
            }
            b"NONSCALING" => {
                nonscaling = true;
                i += 1;
            }
            _ => return Err(Error::Syntax),
        }
    }

    if error_rate <= 0.0 || error_rate >= 1.0 {
        return Err(Error::Other("ERR error rate should be between 0 and 1"));
    }
    if capacity == 0 {
        return Err(Error::Other("ERR capacity should be larger than 0"));
    }
    if expansion < 1 {
        return Err(Error::Other(
            "ERR expansion should be greater or equal to 1",
        ));
    }

    match store.bf_reserve(key, error_rate, capacity, expansion, nonscaling) {
        Ok(_) => Ok(RespValue::ok()),
        Err(e) => Err(e),
    }
}

/// BF.ADD {key} {item}
fn cmd_bf_add(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("BF.ADD"));
    }

    let key = &args[0];
    let item = &args[1];

    match store.bf_add(key, item) {
        Ok(added) => Ok(RespValue::integer(if added { 1 } else { 0 })),
        Err(e) => Err(e),
    }
}

/// BF.MADD {key} {item} [item ...]
fn cmd_bf_madd(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("BF.MADD"));
    }

    let key = &args[0];
    let items: Vec<&[u8]> = args[1..].iter().map(|b| b.as_ref()).collect();

    match store.bf_madd(key, &items) {
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

/// BF.EXISTS {key} {item}
fn cmd_bf_exists(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("BF.EXISTS"));
    }

    let key = &args[0];
    let item = &args[1];

    match store.bf_exists(key, item) {
        Ok(exists) => Ok(RespValue::integer(if exists { 1 } else { 0 })),
        Err(e) => Err(e),
    }
}

/// BF.MEXISTS {key} {item} [item ...]
fn cmd_bf_mexists(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("BF.MEXISTS"));
    }

    let key = &args[0];
    let items: Vec<&[u8]> = args[1..].iter().map(|b| b.as_ref()).collect();

    match store.bf_mexists(key, &items) {
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

/// BF.INFO {key} [CAPACITY | SIZE | FILTERS | ITEMS | EXPANSION]
fn cmd_bf_info(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("BF.INFO"));
    }

    let key = &args[0];
    let info = match store.bf_info(key)? {
        Some(i) => i,
        None => return Err(Error::Other("ERR item not found")),
    };

    if args.len() > 1 {
        let field = args[1].to_ascii_uppercase();
        match field.as_slice() {
            b"CAPACITY" => Ok(RespValue::integer(info.0 as i64)),
            b"SIZE" => Ok(RespValue::integer(info.1 as i64)),
            b"FILTERS" => Ok(RespValue::integer(info.2 as i64)),
            b"ITEMS" => Ok(RespValue::integer(info.3 as i64)),
            b"EXPANSION" => Ok(RespValue::integer(info.4 as i64)),
            _ => Err(Error::Other("ERR invalid info argument")),
        }
    } else {
        Ok(RespValue::Array(vec![
            RespValue::bulk_string("Capacity"),
            RespValue::integer(info.0 as i64),
            RespValue::bulk_string("Size"),
            RespValue::integer(info.1 as i64),
            RespValue::bulk_string("Number of filters"),
            RespValue::integer(info.2 as i64),
            RespValue::bulk_string("Number of items inserted"),
            RespValue::integer(info.3 as i64),
            RespValue::bulk_string("Expansion rate"),
            RespValue::integer(info.4 as i64),
        ]))
    }
}

/// BF.CARD {key}
fn cmd_bf_card(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("BF.CARD"));
    }

    let key = &args[0];
    let card = store.bf_card(key)?;
    Ok(RespValue::integer(card as i64))
}

/// BF.INSERT {key} [CAPACITY cap] [ERROR err] [EXPANSION exp] [NOCREATE] [NONSCALING] ITEMS {item} [item ...]
fn cmd_bf_insert(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("BF.INSERT"));
    }

    let key = args[0].clone();
    let mut capacity = None;
    let mut error_rate = None;
    let mut expansion = None;
    let mut nonscaling = None;
    let mut nocreate = false;
    let mut items_start_idx = 0;

    let mut i = 1;
    while i < args.len() {
        let arg = args[i].to_ascii_uppercase();
        match arg.as_slice() {
            b"CAPACITY" => {
                if i + 1 >= args.len() {
                    return Err(Error::Syntax);
                }
                capacity = Some(
                    String::from_utf8_lossy(&args[i + 1])
                        .parse()
                        .map_err(|_| Error::Other("ERR bad capacity"))?,
                );
                i += 2;
            }
            b"ERROR" => {
                if i + 1 >= args.len() {
                    return Err(Error::Syntax);
                }
                error_rate = Some(
                    String::from_utf8_lossy(&args[i + 1])
                        .parse()
                        .map_err(|_| Error::Other("ERR bad error rate"))?,
                );
                i += 2;
            }
            b"EXPANSION" => {
                if i + 1 >= args.len() {
                    return Err(Error::Syntax);
                }
                expansion = Some(
                    String::from_utf8_lossy(&args[i + 1])
                        .parse()
                        .map_err(|_| Error::Other("ERR bad expansion"))?,
                );
                i += 2;
            }
            b"NOCREATE" => {
                nocreate = true;
                i += 1;
            }
            b"NONSCALING" => {
                nonscaling = Some(true);
                i += 1;
            }
            b"ITEMS" => {
                items_start_idx = i + 1;
                break;
            }
            _ => return Err(Error::UnknownCommand("BF.INSERT arg".into())),
        }
    }

    if items_start_idx == 0 || items_start_idx >= args.len() {
        return Err(Error::Syntax);
    }

    let items: Vec<&[u8]> = args[items_start_idx..].iter().map(|b| b.as_ref()).collect();

    match store.bf_insert(
        key, &items, capacity, error_rate, expansion, nonscaling, nocreate,
    )? {
        Some(results) => {
            let resp_results: Vec<RespValue> = results
                .into_iter()
                .map(|b| RespValue::integer(if b { 1 } else { 0 }))
                .collect();
            Ok(RespValue::Array(resp_results))
        }
        None => Err(Error::Other("ERR not found")),
    }
}

/// BF.SCANDUMP {key} {iterator}
fn cmd_bf_scandump(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("BF.SCANDUMP"));
    }

    let key = &args[0];
    let iterator: usize = String::from_utf8_lossy(args[1].as_ref())
        .parse()
        .map_err(|_| Error::Other("ERR bad iterator"))?;

    match store.bf_scandump(key, iterator)? {
        Some((next_iter, data)) => Ok(RespValue::Array(vec![
            RespValue::integer(next_iter as i64),
            RespValue::bulk(Bytes::copy_from_slice(&data)),
        ])),
        None => Ok(RespValue::Array(vec![
            RespValue::integer(0),
            RespValue::SimpleString(Bytes::from_static(b"")),
        ])),
    }
}

/// BF.LOADCHUNK {key} {iterator} {data}
fn cmd_bf_loadchunk(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("BF.LOADCHUNK"));
    }

    let key = args[0].clone();
    let iterator: usize = String::from_utf8_lossy(args[1].as_ref())
        .parse()
        .map_err(|_| Error::Other("ERR bad iterator"))?;
    let data = &args[2];

    store.bf_loadchunk(key, iterator, data)?;
    Ok(RespValue::ok())
}
