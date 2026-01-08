use bytes::Bytes;

use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::storage::Store;

pub fn execute(store: &Store, cmd: &[u8], args: &[Bytes]) -> Result<RespValue> {
    match cmd {
        b"PFADD" => cmd_pfadd(store, args),
        b"PFCOUNT" => cmd_pfcount(store, args),
        b"PFMERGE" => cmd_pfmerge(store, args),
        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd).into_owned(),
        )),
    }
}

/// PFADD key element [element ...]
fn cmd_pfadd(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("PFADD"));
    }
    let changed = store.pfadd(args[0].clone(), &args[1..])?;
    Ok(RespValue::integer(if changed { 1 } else { 0 }))
}

/// PFCOUNT key [key ...]
fn cmd_pfcount(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("PFCOUNT"));
    }
    Ok(RespValue::integer(store.pfcount(args) as i64))
}

/// PFMERGE destkey sourcekey [sourcekey ...]
fn cmd_pfmerge(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("PFMERGE"));
    }
    store.pfmerge(args[0].clone(), &args[1..])?;
    Ok(RespValue::ok())
}
