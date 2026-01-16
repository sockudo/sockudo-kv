//! Vector set command handlers
//!
//! Commands: VADD, VCARD, VDIM, VEMB, VGETATTR, VINFO, VISMEMBER,
//! VLINKS, VRANDMEMBER, VRANGE, VREM, VSETATTR, VSIM

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::storage::Store;
use crate::storage::VectorQuantization;

/// Case-insensitive comparison
#[inline]
fn eq_ignore_case(a: &[u8], b: &[u8]) -> bool {
    a.eq_ignore_ascii_case(b)
}

/// Parse float from bytes
#[inline]
fn parse_float(b: &[u8]) -> Result<f32> {
    std::str::from_utf8(b)
        .map_err(|_| Error::NotFloat)?
        .parse()
        .map_err(|_| Error::NotFloat)
}

/// Parse integer from bytes
#[inline]
fn parse_int(b: &[u8]) -> Result<i64> {
    std::str::from_utf8(b)
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)
}

/// Execute Vector commands
pub fn execute(store: &Store, cmd: &[u8], args: &[Bytes]) -> Result<RespValue> {
    match cmd.to_ascii_uppercase().as_slice() {
        b"VADD" => cmd_vadd(store, args),
        b"VCARD" => cmd_vcard(store, args),
        b"VDIM" => cmd_vdim(store, args),
        b"VEMB" => cmd_vemb(store, args),
        b"VGETATTR" => cmd_vgetattr(store, args),
        b"VINFO" => cmd_vinfo(store, args),
        b"VISMEMBER" => cmd_vismember(store, args),
        b"VLINKS" => cmd_vlinks(store, args),
        b"VRANDMEMBER" => cmd_vrandmember(store, args),
        b"VRANGE" => cmd_vrange(store, args),
        b"VREM" => cmd_vrem(store, args),
        b"VSETATTR" => cmd_vsetattr(store, args),
        b"VSIM" => cmd_vsim(store, args),
        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd).into_owned(),
        )),
    }
}

/// VADD key [REDUCE dim] (FP32 | VALUES num) vector element [CAS] [NOQUANT|Q8|BIN] [EF ef] [SETATTR attrs] [M links]
fn cmd_vadd(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 4 {
        return Err(Error::WrongArity("VADD"));
    }

    let key = args[0].clone();
    let mut idx = 1;
    let mut reduce_dim: Option<usize> = None;
    let mut quant = VectorQuantization::NoQuant;
    let mut ef_construction: Option<usize> = None;
    let mut m: Option<usize> = None;
    let mut attrs: Option<Box<sonic_rs::Value>> = None;

    // Parse optional REDUCE
    if idx < args.len() && eq_ignore_case(&args[idx], b"REDUCE") {
        idx += 1;
        if idx >= args.len() {
            return Err(Error::Syntax);
        }
        reduce_dim = Some(parse_int(&args[idx])? as usize);
        idx += 1;
    }

    // Parse FP32 or VALUES num
    if idx >= args.len() {
        return Err(Error::Syntax);
    }

    let vector_count: usize;
    if eq_ignore_case(&args[idx], b"FP32") {
        idx += 1;
        if idx >= args.len() {
            return Err(Error::Syntax);
        }
        // FP32 blob - 4 bytes per float, little-endian
        let blob = &args[idx];
        if !blob.len().is_multiple_of(4) {
            return Err(Error::Custom(
                "ERR FP32 blob must be multiple of 4 bytes".into(),
            ));
        }
        vector_count = blob.len() / 4;
        let mut vector = Vec::with_capacity(vector_count);
        for chunk in blob.chunks_exact(4) {
            let bytes: [u8; 4] = chunk.try_into().unwrap();
            vector.push(f32::from_le_bytes(bytes));
        }
        idx += 1;

        // Parse element name
        if idx >= args.len() {
            return Err(Error::Syntax);
        }
        let element = args[idx].clone();
        idx += 1;

        // Parse remaining options inline
        let mut quant = VectorQuantization::NoQuant;
        let mut ef_construction: Option<usize> = None;
        let mut m: Option<usize> = None;
        let mut attrs: Option<Box<sonic_rs::Value>> = None;

        while idx < args.len() {
            if eq_ignore_case(&args[idx], b"NOQUANT") {
                quant = VectorQuantization::NoQuant;
                idx += 1;
            } else if eq_ignore_case(&args[idx], b"Q8") {
                quant = VectorQuantization::Q8;
                idx += 1;
            } else if eq_ignore_case(&args[idx], b"BIN") {
                quant = VectorQuantization::Binary;
                idx += 1;
            } else if eq_ignore_case(&args[idx], b"EF") {
                idx += 1;
                if idx >= args.len() {
                    return Err(Error::Syntax);
                }
                ef_construction = Some(parse_int(&args[idx])? as usize);
                idx += 1;
            } else if eq_ignore_case(&args[idx], b"M") {
                idx += 1;
                if idx >= args.len() {
                    return Err(Error::Syntax);
                }
                m = Some(parse_int(&args[idx])? as usize);
                idx += 1;
            } else if eq_ignore_case(&args[idx], b"SETATTR") {
                idx += 1;
                if idx >= args.len() {
                    return Err(Error::Syntax);
                }
                let json_str = std::str::from_utf8(&args[idx]).map_err(|_| Error::Syntax)?;
                let value: sonic_rs::Value =
                    sonic_rs::from_str(json_str).map_err(|_| Error::Syntax)?;
                attrs = Some(Box::new(value));
                idx += 1;
            } else {
                idx += 1;
            }
        }

        return match store.vadd(
            key,
            reduce_dim,
            vector,
            element,
            quant,
            ef_construction,
            m,
            attrs,
        ) {
            Ok(added) => Ok(RespValue::integer(if added { 1 } else { 0 })),
            Err(_) => Ok(RespValue::integer(0)),
        };
    } else if eq_ignore_case(&args[idx], b"VALUES") {
        idx += 1;
        if idx >= args.len() {
            return Err(Error::Syntax);
        }
        vector_count = parse_int(&args[idx])? as usize;
        idx += 1;
    } else {
        return Err(Error::Syntax);
    }

    // Parse vector values
    if idx + vector_count > args.len() {
        return Err(Error::Syntax);
    }

    let mut vector = Vec::with_capacity(vector_count);
    for i in 0..vector_count {
        vector.push(parse_float(&args[idx + i])?);
    }
    idx += vector_count;

    // Parse element name
    if idx >= args.len() {
        return Err(Error::Syntax);
    }
    let element = args[idx].clone();
    idx += 1;

    // Parse optional arguments
    while idx < args.len() {
        if eq_ignore_case(&args[idx], b"CAS") {
            // Compare-and-swap - just acknowledge for now
            idx += 1;
        } else if eq_ignore_case(&args[idx], b"NOQUANT") {
            quant = VectorQuantization::NoQuant;
            idx += 1;
        } else if eq_ignore_case(&args[idx], b"Q8") {
            quant = VectorQuantization::Q8;
            idx += 1;
        } else if eq_ignore_case(&args[idx], b"BIN") {
            quant = VectorQuantization::Binary;
            idx += 1;
        } else if eq_ignore_case(&args[idx], b"EF") {
            idx += 1;
            if idx >= args.len() {
                return Err(Error::Syntax);
            }
            ef_construction = Some(parse_int(&args[idx])? as usize);
            idx += 1;
        } else if eq_ignore_case(&args[idx], b"SETATTR") {
            idx += 1;
            if idx >= args.len() {
                return Err(Error::Syntax);
            }
            let json_str = std::str::from_utf8(&args[idx]).map_err(|_| Error::Syntax)?;
            let value: sonic_rs::Value = sonic_rs::from_str(json_str).map_err(|_| Error::Syntax)?;
            attrs = Some(Box::new(value));
            idx += 1;
        } else if eq_ignore_case(&args[idx], b"M") {
            idx += 1;
            if idx >= args.len() {
                return Err(Error::Syntax);
            }
            m = Some(parse_int(&args[idx])? as usize);
            idx += 1;
        } else {
            idx += 1; // Skip unknown options
        }
    }

    match store.vadd(
        key,
        reduce_dim,
        vector,
        element,
        quant,
        ef_construction,
        m,
        attrs,
    ) {
        Ok(added) => Ok(RespValue::integer(if added { 1 } else { 0 })),
        Err(_) => Ok(RespValue::integer(0)),
    }
}

/// VCARD key
fn cmd_vcard(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("VCARD"));
    }
    Ok(RespValue::integer(store.vcard(&args[0])))
}

/// VDIM key
fn cmd_vdim(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("VDIM"));
    }
    match store.vdim(&args[0]) {
        Some(dim) => Ok(RespValue::integer(dim as i64)),
        None => Ok(RespValue::null()),
    }
}

/// VEMB key element [RAW]
fn cmd_vemb(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("VEMB"));
    }

    let raw = args.len() > 2 && eq_ignore_case(&args[2], b"RAW");

    match store.vemb(&args[0], &args[1], raw) {
        Some(vector) => {
            let values: Vec<RespValue> = vector
                .iter()
                .map(|&v| RespValue::bulk(Bytes::from(v.to_string())))
                .collect();
            Ok(RespValue::array(values))
        }
        None => Ok(RespValue::null()),
    }
}

/// VGETATTR key element
fn cmd_vgetattr(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("VGETATTR"));
    }

    match store.vgetattr(&args[0], &args[1]) {
        Some(attrs) => Ok(RespValue::bulk(Bytes::from(attrs))),
        None => Ok(RespValue::null()),
    }
}

/// VINFO key
fn cmd_vinfo(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("VINFO"));
    }

    match store.vinfo(&args[0]) {
        Some(info) => {
            let values: Vec<RespValue> = info
                .into_iter()
                .flat_map(|(k, v)| {
                    vec![
                        RespValue::bulk(Bytes::from(k)),
                        RespValue::bulk(Bytes::from(v)),
                    ]
                })
                .collect();
            Ok(RespValue::array(values))
        }
        None => Ok(RespValue::null()),
    }
}

/// VISMEMBER key element
fn cmd_vismember(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("VISMEMBER"));
    }

    let is_member = store.vismember(&args[0], &args[1]);
    Ok(RespValue::integer(if is_member { 1 } else { 0 }))
}

/// VLINKS key element [WITHSCORES]
fn cmd_vlinks(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("VLINKS"));
    }

    let with_scores = args.len() > 2 && eq_ignore_case(&args[2], b"WITHSCORES");

    match store.vlinks(&args[0], &args[1], with_scores) {
        Some(layers) => {
            let layer_values: Vec<RespValue> = layers
                .into_iter()
                .map(|layer| {
                    let neighbors: Vec<RespValue> = layer
                        .into_iter()
                        .map(|(elem, score)| {
                            if let Some(s) = score {
                                RespValue::array(vec![
                                    RespValue::bulk(elem),
                                    RespValue::bulk(Bytes::from(s.to_string())),
                                ])
                            } else {
                                RespValue::bulk(elem)
                            }
                        })
                        .collect();
                    RespValue::array(neighbors)
                })
                .collect();
            Ok(RespValue::array(layer_values))
        }
        None => Ok(RespValue::null()),
    }
}

/// VRANDMEMBER key [count]
fn cmd_vrandmember(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("VRANDMEMBER"));
    }

    let count = if args.len() > 1 {
        Some(parse_int(&args[1])?)
    } else {
        None
    };

    let members = store.vrandmember(&args[0], count);

    if count.is_none() && members.len() == 1 {
        // Single member request
        Ok(RespValue::bulk(members.into_iter().next().unwrap()))
    } else if members.is_empty() && count.is_none() {
        Ok(RespValue::null())
    } else {
        Ok(RespValue::array(
            members.into_iter().map(RespValue::bulk).collect(),
        ))
    }
}

/// VRANGE key start end [count]
fn cmd_vrange(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("VRANGE"));
    }

    let count = if args.len() > 3 {
        Some(parse_int(&args[3])? as usize)
    } else {
        None
    };

    let elements = store.vrange(&args[0], &args[1], &args[2], count);
    Ok(RespValue::array(
        elements.into_iter().map(RespValue::bulk).collect(),
    ))
}

/// VREM key element
fn cmd_vrem(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("VREM"));
    }

    let removed = store.vrem(&args[0], &args[1]);
    Ok(RespValue::integer(if removed { 1 } else { 0 }))
}

/// VSETATTR key element "{ JSON obj }"
fn cmd_vsetattr(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("VSETATTR"));
    }

    let attrs = if args[2].is_empty() || args[2].as_ref() == b"null" {
        None
    } else {
        let json_str = std::str::from_utf8(&args[2]).map_err(|_| Error::Syntax)?;
        let value: sonic_rs::Value = sonic_rs::from_str(json_str).map_err(|_| Error::Syntax)?;
        Some(Box::new(value))
    };

    let updated = store.vsetattr(&args[0], &args[1], attrs);
    Ok(RespValue::integer(if updated { 1 } else { 0 }))
}

/// VSIM key (ELE | FP32 | VALUES num) (vector | element) [WITHSCORES] [WITHATTRIBS] [COUNT num] [EPSILON delta] [EF ef] [FILTER expr] [FILTER-EF ef] [TRUTH] [NOTHREAD]
fn cmd_vsim(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("VSIM"));
    }

    let key = &args[0];
    let mut idx = 1;

    // Parse query type
    let query_vector: Vec<f32>;

    if eq_ignore_case(&args[idx], b"ELE") {
        idx += 1;
        if idx >= args.len() {
            return Err(Error::Syntax);
        }
        // Query by element - get its vector
        match store.vemb(key, &args[idx], false) {
            Some(v) => query_vector = v,
            None => return Ok(RespValue::array(vec![])),
        }
        idx += 1;
    } else if eq_ignore_case(&args[idx], b"FP32") {
        idx += 1;
        if idx >= args.len() {
            return Err(Error::Syntax);
        }
        // FP32 blob - 4 bytes per float, little-endian
        let blob = &args[idx];
        if !blob.len().is_multiple_of(4) {
            return Err(Error::Custom(
                "ERR FP32 blob must be multiple of 4 bytes".into(),
            ));
        }
        let mut vec = Vec::with_capacity(blob.len() / 4);
        for chunk in blob.chunks_exact(4) {
            let bytes: [u8; 4] = chunk.try_into().unwrap();
            vec.push(f32::from_le_bytes(bytes));
        }
        query_vector = vec;
        idx += 1;
    } else if eq_ignore_case(&args[idx], b"VALUES") {
        idx += 1;
        if idx >= args.len() {
            return Err(Error::Syntax);
        }
        let vector_count = parse_int(&args[idx])? as usize;
        idx += 1;

        if idx + vector_count > args.len() {
            return Err(Error::Syntax);
        }

        query_vector = (0..vector_count)
            .map(|i| parse_float(&args[idx + i]))
            .collect::<Result<Vec<_>>>()?;
        idx += vector_count;
    } else {
        return Err(Error::Syntax);
    }

    // Parse optional arguments
    let mut with_scores = false;
    let mut with_attribs = false;
    let mut count: usize = 10;
    let mut ef: Option<usize> = None;
    let mut filter: Option<String> = None;

    while idx < args.len() {
        if eq_ignore_case(&args[idx], b"WITHSCORES") {
            with_scores = true;
            idx += 1;
        } else if eq_ignore_case(&args[idx], b"WITHATTRIBS") {
            with_attribs = true;
            idx += 1;
        } else if eq_ignore_case(&args[idx], b"COUNT") {
            idx += 1;
            if idx >= args.len() {
                return Err(Error::Syntax);
            }
            count = parse_int(&args[idx])? as usize;
            idx += 1;
        } else if eq_ignore_case(&args[idx], b"EF") {
            idx += 1;
            if idx >= args.len() {
                return Err(Error::Syntax);
            }
            ef = Some(parse_int(&args[idx])? as usize);
            idx += 1;
        } else if eq_ignore_case(&args[idx], b"EPSILON") {
            idx += 2; // Skip EPSILON and value
        } else if eq_ignore_case(&args[idx], b"FILTER") {
            idx += 1;
            if idx >= args.len() {
                return Err(Error::Syntax);
            }
            filter = Some(String::from_utf8_lossy(&args[idx]).into_owned());
            idx += 1;
        } else if eq_ignore_case(&args[idx], b"FILTER-EF") {
            idx += 2; // Skip FILTER-EF and value
        } else {
            // Skip unknown options like TRUTH, NOTHREAD, etc.
            idx += 1;
        }
    }

    let results = store.vsim(
        key,
        &query_vector,
        count,
        ef,
        with_scores,
        with_attribs,
        filter.as_deref(),
    );

    let values: Vec<RespValue> = results
        .into_iter()
        .map(|(elem, score, attrs)| {
            let mut result = vec![RespValue::bulk(elem)];
            if let Some(s) = score {
                result.push(RespValue::bulk(Bytes::from(s.to_string())));
            }
            if let Some(a) = attrs {
                result.push(RespValue::bulk(Bytes::from(a)));
            }
            if result.len() == 1 {
                result.pop().unwrap()
            } else {
                RespValue::array(result)
            }
        })
        .collect();

    Ok(RespValue::array(values))
}
