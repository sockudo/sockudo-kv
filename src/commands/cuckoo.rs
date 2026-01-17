//! Cuckoo Filter command handlers
//!
//! Implements CF.* commands for probabilistic set membership with deletion support.

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::storage::Store;

/// Execute Cuckoo Filter commands
pub fn execute(store: &Store, cmd: &[u8], args: &[Bytes]) -> Result<RespValue> {
    match cmd.to_ascii_uppercase().as_slice() {
        b"CF.RESERVE" => cmd_cf_reserve(store, args),
        b"CF.ADD" => cmd_cf_add(store, args),
        b"CF.ADDNX" => cmd_cf_addnx(store, args),
        b"CF.INSERT" => cmd_cf_insert(store, args),
        b"CF.INSERTNX" => cmd_cf_insertnx(store, args),
        b"CF.EXISTS" => cmd_cf_exists(store, args),
        b"CF.MEXISTS" => cmd_cf_mexists(store, args),
        b"CF.COUNT" => cmd_cf_count(store, args),
        b"CF.DEL" => cmd_cf_del(store, args),
        b"CF.INFO" => cmd_cf_info(store, args),
        b"CF.SCANDUMP" => cmd_cf_scandump(store, args),
        b"CF.LOADCHUNK" => cmd_cf_loadchunk(store, args),
        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd).into_owned(),
        )),
    }
}

/// CF.RESERVE key capacity [BUCKETSIZE size] [MAXITERATIONS num] [EXPANSION rate]
fn cmd_cf_reserve(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("CF.RESERVE"));
    }

    let key = args[0].clone();
    let capacity: usize = String::from_utf8_lossy(&args[1])
        .parse()
        .map_err(|_| Error::Other("ERR bad capacity"))?;

    if capacity == 0 {
        return Err(Error::Other("ERR capacity should be larger than 0"));
    }

    let mut bucket_size = None;
    let mut max_iterations = None;
    let mut expansion = None;

    let mut i = 2;
    while i < args.len() {
        let arg = args[i].to_ascii_uppercase();
        match arg.as_slice() {
            b"BUCKETSIZE" => {
                if i + 1 >= args.len() {
                    return Err(Error::WrongArity("CF.RESERVE"));
                }
                bucket_size = Some(
                    String::from_utf8_lossy(&args[i + 1])
                        .parse()
                        .map_err(|_| Error::Other("ERR bad bucket size"))?,
                );
                i += 2;
            }
            b"MAXITERATIONS" => {
                if i + 1 >= args.len() {
                    return Err(Error::WrongArity("CF.RESERVE"));
                }
                max_iterations = Some(
                    String::from_utf8_lossy(&args[i + 1])
                        .parse()
                        .map_err(|_| Error::Other("ERR bad max iterations"))?,
                );
                i += 2;
            }
            b"EXPANSION" => {
                if i + 1 >= args.len() {
                    return Err(Error::WrongArity("CF.RESERVE"));
                }
                expansion = Some(
                    String::from_utf8_lossy(&args[i + 1])
                        .parse()
                        .map_err(|_| Error::Other("ERR bad expansion"))?,
                );
                i += 2;
            }
            _ => return Err(Error::Syntax),
        }
    }

    // Validate bucket_size if provided
    if let Some(bs) = bucket_size {
        if bs == 0 || bs > 255 {
            return Err(Error::Other("ERR bucket size should be between 1 and 255"));
        }
    }

    // Validate max_iterations if provided
    if let Some(mi) = max_iterations {
        if mi == 0 {
            return Err(Error::Other("ERR max iterations should be larger than 0"));
        }
    }

    match store.cf_reserve(key, capacity, bucket_size, max_iterations, expansion) {
        Ok(_) => Ok(RespValue::ok()),
        Err(e) => Err(e),
    }
}

/// CF.ADD key item
fn cmd_cf_add(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("CF.ADD"));
    }

    let key = &args[0];
    let item = &args[1];

    match store.cf_add(key, item) {
        Ok(added) => Ok(RespValue::integer(if added { 1 } else { 0 })),
        Err(e) => Err(e),
    }
}

/// CF.ADDNX key item
fn cmd_cf_addnx(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("CF.ADDNX"));
    }

    let key = &args[0];
    let item = &args[1];

    match store.cf_addnx(key, item) {
        Ok(added) => Ok(RespValue::integer(if added { 1 } else { 0 })),
        Err(e) => Err(e),
    }
}

/// CF.INSERT key [CAPACITY cap] [NOCREATE] ITEMS item [item ...]
fn cmd_cf_insert(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("CF.INSERT"));
    }

    let key = args[0].clone();
    let mut capacity = None;
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
            b"NOCREATE" => {
                nocreate = true;
                i += 1;
            }
            b"ITEMS" => {
                items_start_idx = i + 1;
                break;
            }
            _ => return Err(Error::Syntax),
        }
    }

    if items_start_idx == 0 || items_start_idx >= args.len() {
        return Err(Error::Syntax);
    }

    let items: Vec<&[u8]> = args[items_start_idx..].iter().map(|b| b.as_ref()).collect();

    match store.cf_insert(key, &items, capacity, nocreate)? {
        Some(results) => {
            let resp_results: Vec<RespValue> =
                results.into_iter().map(RespValue::integer).collect();
            Ok(RespValue::Array(resp_results))
        }
        None => Err(Error::Other("ERR not found")),
    }
}

/// CF.INSERTNX key [CAPACITY cap] [NOCREATE] ITEMS item [item ...]
fn cmd_cf_insertnx(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("CF.INSERTNX"));
    }

    let key = args[0].clone();
    let mut capacity = None;
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
            b"NOCREATE" => {
                nocreate = true;
                i += 1;
            }
            b"ITEMS" => {
                items_start_idx = i + 1;
                break;
            }
            _ => return Err(Error::Syntax),
        }
    }

    if items_start_idx == 0 || items_start_idx >= args.len() {
        return Err(Error::Syntax);
    }

    let items: Vec<&[u8]> = args[items_start_idx..].iter().map(|b| b.as_ref()).collect();

    match store.cf_insertnx(key, &items, capacity, nocreate)? {
        Some(results) => {
            let resp_results: Vec<RespValue> =
                results.into_iter().map(RespValue::integer).collect();
            Ok(RespValue::Array(resp_results))
        }
        None => Err(Error::Other("ERR not found")),
    }
}

/// CF.EXISTS key item
fn cmd_cf_exists(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("CF.EXISTS"));
    }

    let key = &args[0];
    let item = &args[1];

    match store.cf_exists(key, item) {
        Ok(exists) => Ok(RespValue::integer(if exists { 1 } else { 0 })),
        Err(e) => Err(e),
    }
}

/// CF.MEXISTS key item [item ...]
fn cmd_cf_mexists(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("CF.MEXISTS"));
    }

    let key = &args[0];
    let items: Vec<&[u8]> = args[1..].iter().map(|b| b.as_ref()).collect();

    match store.cf_mexists(key, &items) {
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

/// CF.COUNT key item
fn cmd_cf_count(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("CF.COUNT"));
    }

    let key = &args[0];
    let item = &args[1];

    match store.cf_count(key, item) {
        Ok(count) => Ok(RespValue::integer(count as i64)),
        Err(e) => Err(e),
    }
}

/// CF.DEL key item
fn cmd_cf_del(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("CF.DEL"));
    }

    let key = &args[0];
    let item = &args[1];

    match store.cf_del(key, item) {
        Ok(deleted) => Ok(RespValue::integer(if deleted { 1 } else { 0 })),
        Err(e) => Err(e),
    }
}

/// CF.INFO key
fn cmd_cf_info(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("CF.INFO"));
    }

    let key = &args[0];
    let info = match store.cf_info(key)? {
        Some(i) => i,
        None => return Err(Error::Other("ERR not found")),
    };

    // info is: (capacity, size_bytes, num_buckets, num_filters, num_items, bucket_size, expansion, max_iterations)
    Ok(RespValue::Array(vec![
        RespValue::bulk_string("Size"),
        RespValue::integer(info.1 as i64),
        RespValue::bulk_string("Number of buckets"),
        RespValue::integer(info.2 as i64),
        RespValue::bulk_string("Number of filters"),
        RespValue::integer(info.3 as i64),
        RespValue::bulk_string("Number of items inserted"),
        RespValue::integer(info.4 as i64),
        RespValue::bulk_string("Number of items deleted"),
        RespValue::integer(0), // We don't track deleted count separately
        RespValue::bulk_string("Bucket size"),
        RespValue::integer(info.5 as i64),
        RespValue::bulk_string("Expansion rate"),
        RespValue::integer(info.6 as i64),
        RespValue::bulk_string("Max iterations"),
        RespValue::integer(info.7 as i64),
    ]))
}

/// CF.SCANDUMP key iterator
fn cmd_cf_scandump(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("CF.SCANDUMP"));
    }

    let key = &args[0];
    let iterator: usize = String::from_utf8_lossy(args[1].as_ref())
        .parse()
        .map_err(|_| Error::Other("ERR bad iterator"))?;

    match store.cf_scandump(key, iterator)? {
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

/// CF.LOADCHUNK key iterator data
fn cmd_cf_loadchunk(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("CF.LOADCHUNK"));
    }

    let key = args[0].clone();
    let iterator: usize = String::from_utf8_lossy(args[1].as_ref())
        .parse()
        .map_err(|_| Error::Other("ERR bad iterator"))?;
    let data = &args[2];

    store.cf_loadchunk(key, iterator, data)?;
    Ok(RespValue::ok())
}
