use bytes::Bytes;

use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::storage::Store;

pub fn execute(store: &Store, cmd: &[u8], args: &[Bytes]) -> Result<RespValue> {
    match cmd.to_ascii_uppercase().as_slice() {
        b"PFADD" => cmd_pfadd(store, args),
        b"PFCOUNT" => cmd_pfcount(store, args),
        b"PFMERGE" => cmd_pfmerge(store, args),
        b"PFDEBUG" => cmd_pfdebug(store, args),
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

/// PFDEBUG subcommand key
/// Internal debugging command for HyperLogLog
fn cmd_pfdebug(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("PFDEBUG"));
    }

    let subcommand = args[0].to_ascii_uppercase();
    let key = &args[1];

    match subcommand.as_slice() {
        b"GETREG" => {
            // Return all 16384 register values as an array
            match store.pf_get_registers(key)? {
                Some(registers) => {
                    let values: Vec<RespValue> = registers
                        .into_iter()
                        .map(|r| RespValue::integer(r as i64))
                        .collect();
                    Ok(RespValue::Array(values))
                }
                None => Ok(RespValue::Array(vec![])),
            }
        }
        b"ENCODING" => match store.pf_encoding(key)? {
            Some(enc) => Ok(RespValue::bulk_string(enc)),
            None => Err(Error::Other("The specified key does not exist")),
        },
        b"DECODE" => match store.pf_decode_sparse(key)? {
            Some(Some(decoded)) => Ok(RespValue::bulk_string(&decoded)),
            Some(None) => Err(Error::Other("HLL encoding is not sparse")),
            None => Err(Error::Other("The specified key does not exist")),
        },
        b"TODENSE" => match store.pf_to_dense(key)? {
            Some(was_sparse) => Ok(RespValue::integer(if was_sparse { 1 } else { 0 })),
            None => Err(Error::Other("The specified key does not exist")),
        },
        _ => Err(Error::Custom(format!(
            "Unknown PFDEBUG subcommand '{}'",
            String::from_utf8_lossy(&subcommand)
        ))),
    }
}
