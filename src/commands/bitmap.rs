use bytes::Bytes;

use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::storage::Store;
use crate::storage::ops::bitmap_ops::{BitfieldEncoding, BitfieldOp, OverflowMode};

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

/// Parse offset - handles both literal offset and #-prefixed (multiplied by encoding bits)
#[inline]
fn parse_offset(b: &[u8], encoding: BitfieldEncoding) -> Result<usize> {
    if b.starts_with(b"#") {
        // Multiplied offset
        let multiplier: usize = std::str::from_utf8(&b[1..])
            .map_err(|_| Error::NotInteger)?
            .parse()
            .map_err(|_| Error::NotInteger)?;
        Ok(multiplier * encoding.bits as usize)
    } else {
        let offset: usize = std::str::from_utf8(b)
            .map_err(|_| Error::NotInteger)?
            .parse()
            .map_err(|_| Error::NotInteger)?;
        Ok(offset)
    }
}

/// Execute bitmap commands
pub fn execute(store: &Store, cmd: &[u8], args: &[Bytes]) -> Result<RespValue> {
    match cmd.to_ascii_uppercase().as_slice() {
        b"SETBIT" => cmd_setbit(store, args),
        b"GETBIT" => cmd_getbit(store, args),
        b"BITCOUNT" => cmd_bitcount(store, args),
        b"BITPOS" => cmd_bitpos(store, args),
        b"BITOP" => cmd_bitop(store, args),
        b"BITFIELD" => cmd_bitfield(store, args),
        b"BITFIELD_RO" => cmd_bitfield_ro(store, args),
        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd).into_owned(),
        )),
    }
}

/// SETBIT key offset value
/// Sets or clears the bit at offset in the string value stored at key.
fn cmd_setbit(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 3 {
        return Err(Error::WrongArity("SETBIT"));
    }

    let key = args[0].clone();
    let offset: usize = std::str::from_utf8(&args[1])
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)?;

    let value: i64 = parse_int(&args[2])?;
    if value != 0 && value != 1 {
        return Err(Error::Custom(
            "bit is not an integer or out of range".into(),
        ));
    }

    // Redis limits offset to 2^32-1
    if offset > 4_294_967_295 {
        return Err(Error::Custom(
            "bit offset is not an integer or out of range".into(),
        ));
    }

    let old_bit = store.setbit(key, offset, value == 1)?;
    Ok(RespValue::integer(old_bit as i64))
}

/// GETBIT key offset
/// Returns the bit value at offset in the string value stored at key.
fn cmd_getbit(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 2 {
        return Err(Error::WrongArity("GETBIT"));
    }

    let offset: usize = std::str::from_utf8(&args[1])
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)?;

    let bit = store.getbit(&args[0], offset)?;
    Ok(RespValue::integer(bit as i64))
}

/// BITCOUNT key [start end [BYTE | BIT]]
/// Count the number of set bits in a string.
fn cmd_bitcount(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("BITCOUNT"));
    }

    let key = &args[0];
    let (start, end, use_bit_index) = match args.len() {
        1 => (None, None, false),
        3 => {
            let start = parse_int(&args[1])?;
            let end = parse_int(&args[2])?;
            (Some(start), Some(end), false)
        }
        4 => {
            let start = parse_int(&args[1])?;
            let end = parse_int(&args[2])?;
            let mode = &args[3];
            let use_bit = if eq_ignore_ascii_case(mode, b"BIT") {
                true
            } else if eq_ignore_ascii_case(mode, b"BYTE") {
                false
            } else {
                return Err(Error::Syntax);
            };
            (Some(start), Some(end), use_bit)
        }
        _ => return Err(Error::Syntax),
    };

    let count = store.bitcount(key, start, end, use_bit_index)?;
    Ok(RespValue::integer(count as i64))
}

/// BITPOS key bit [start [end [BYTE | BIT]]]
/// Find first bit set or clear in a string.
fn cmd_bitpos(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("BITPOS"));
    }

    let key = &args[0];
    let bit: i64 = parse_int(&args[1])?;
    if bit != 0 && bit != 1 {
        return Err(Error::Custom(
            "bit is not an integer or out of range".into(),
        ));
    }

    let (start, end, use_bit_index) = match args.len() {
        2 => (None, None, false),
        3 => {
            let start = parse_int(&args[2])?;
            (Some(start), None, false)
        }
        4 => {
            let start = parse_int(&args[2])?;
            let end = parse_int(&args[3])?;
            (Some(start), Some(end), false)
        }
        5 => {
            let start = parse_int(&args[2])?;
            let end = parse_int(&args[3])?;
            let mode = &args[4];
            let use_bit = if eq_ignore_ascii_case(mode, b"BIT") {
                true
            } else if eq_ignore_ascii_case(mode, b"BYTE") {
                false
            } else {
                return Err(Error::Syntax);
            };
            (Some(start), Some(end), use_bit)
        }
        _ => return Err(Error::Syntax),
    };

    let pos = store.bitpos(key, bit as u8, start, end, use_bit_index)?;
    Ok(RespValue::integer(pos))
}

/// BITOP operation destkey key [key ...]
/// Perform bitwise operations between strings.
fn cmd_bitop(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("BITOP"));
    }

    let operation = &args[0];
    let destkey = args[1].clone();
    let keys: Vec<Bytes> = args[2..].to_vec();

    // Validate operation
    let op_upper = operation.to_ascii_uppercase();
    match op_upper.as_slice() {
        b"AND" | b"OR" | b"XOR" | b"NOT" | b"DIFF" | b"DIFF1" | b"ANDOR" | b"ONE" => {}
        _ => return Err(Error::Syntax),
    }

    // NOT requires exactly one source key
    if op_upper.as_slice() == b"NOT" && keys.len() != 1 {
        return Err(Error::Custom("BITOP NOT requires one key".into()));
    }

    let result_len = store.bitop(operation, destkey, &keys)?;
    Ok(RespValue::integer(result_len as i64))
}

/// BITFIELD key [GET encoding offset | [OVERFLOW WRAP|SAT|FAIL] SET encoding offset value | INCRBY encoding offset increment ...]
/// Perform arbitrary bitfield integer operations.
fn cmd_bitfield(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("BITFIELD"));
    }

    let key = args[0].clone();
    let mut ops = Vec::new();
    let mut i = 1;

    while i < args.len() {
        let subcommand = args[i].to_ascii_uppercase();

        match subcommand.as_slice() {
            b"GET" => {
                if i + 2 >= args.len() {
                    return Err(Error::Syntax);
                }
                let encoding = BitfieldEncoding::parse(&args[i + 1])
                    .ok_or(Error::Custom("Invalid encoding".into()))?;
                let offset = parse_offset(&args[i + 2], encoding)?;
                ops.push(BitfieldOp::Get { encoding, offset });
                i += 3;
            }
            b"SET" => {
                if i + 3 >= args.len() {
                    return Err(Error::Syntax);
                }
                let encoding = BitfieldEncoding::parse(&args[i + 1])
                    .ok_or(Error::Custom("Invalid encoding".into()))?;
                let offset = parse_offset(&args[i + 2], encoding)?;
                let value = parse_int(&args[i + 3])?;
                ops.push(BitfieldOp::Set {
                    encoding,
                    offset,
                    value,
                });
                i += 4;
            }
            b"INCRBY" => {
                if i + 3 >= args.len() {
                    return Err(Error::Syntax);
                }
                let encoding = BitfieldEncoding::parse(&args[i + 1])
                    .ok_or(Error::Custom("Invalid encoding".into()))?;
                let offset = parse_offset(&args[i + 2], encoding)?;
                let increment = parse_int(&args[i + 3])?;
                ops.push(BitfieldOp::IncrBy {
                    encoding,
                    offset,
                    increment,
                });
                i += 4;
            }
            b"OVERFLOW" => {
                if i + 1 >= args.len() {
                    return Err(Error::Syntax);
                }
                let mode = OverflowMode::from_bytes(&args[i + 1]).ok_or(Error::Syntax)?;
                ops.push(BitfieldOp::Overflow(mode));
                i += 2;
            }
            _ => return Err(Error::Syntax),
        }
    }

    let results = store.bitfield(key, ops)?;

    let resp: Vec<RespValue> = results
        .into_iter()
        .map(|r| match r {
            Some(v) => RespValue::integer(v),
            None => RespValue::Null,
        })
        .collect();

    Ok(RespValue::array(resp))
}

/// BITFIELD_RO key [GET encoding offset ...]
/// Read-only version of BITFIELD (only GET operations allowed).
fn cmd_bitfield_ro(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("BITFIELD_RO"));
    }

    let key = &args[0];
    let mut ops = Vec::new();
    let mut i = 1;

    while i < args.len() {
        let subcommand = args[i].to_ascii_uppercase();

        match subcommand.as_slice() {
            b"GET" => {
                if i + 2 >= args.len() {
                    return Err(Error::Syntax);
                }
                let encoding = BitfieldEncoding::parse(&args[i + 1])
                    .ok_or(Error::Custom("Invalid encoding".into()))?;
                let offset = parse_offset(&args[i + 2], encoding)?;
                ops.push(BitfieldOp::Get { encoding, offset });
                i += 3;
            }
            _ => return Err(Error::Syntax), // Only GET allowed
        }
    }

    let results = store.bitfield_ro(key, ops)?;

    let resp: Vec<RespValue> = results
        .into_iter()
        .map(|r| match r {
            Some(v) => RespValue::integer(v),
            None => RespValue::Null,
        })
        .collect();

    Ok(RespValue::array(resp))
}
