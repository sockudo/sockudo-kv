//! Vector set command handlers
//!
//! Commands: VADD, VCARD, VDIM, VEMB, VGETATTR, VINFO, VISMEMBER,
//! VLINKS, VRANDMEMBER, VRANGE, VREM, VSETATTR, VSIM

use bytes::Bytes;
use std::sync::atomic::Ordering;

use crate::client::ClientState;
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
pub fn execute(
    store: &Store,
    cmd: &[u8],
    args: &[Bytes],
    client: Option<&ClientState>,
) -> Result<RespValue> {
    match cmd.to_ascii_uppercase().as_slice() {
        b"VADD" => cmd_vadd(store, args, client),
        b"VCARD" => cmd_vcard(store, args),
        b"VDIM" => cmd_vdim(store, args),
        b"VEMB" => cmd_vemb(store, args, client),
        b"VGETATTR" => cmd_vgetattr(store, args),
        b"VINFO" => cmd_vinfo(store, args),
        b"VISMEMBER" => cmd_vismember(store, args, client),
        b"VLINKS" => cmd_vlinks(store, args, client),
        b"VRANDMEMBER" => cmd_vrandmember(store, args),
        b"VRANGE" => cmd_vrange(store, args),
        b"VREM" => cmd_vrem(store, args, client),
        b"VSETATTR" => cmd_vsetattr(store, args, client),
        b"VSIM" => cmd_vsim(store, args, client),
        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd).into_owned(),
        )),
    }
}

/// Helper to check if RESP3 protocol is in use
#[inline]
fn is_resp3(client: Option<&ClientState>) -> bool {
    client
        .map(|c| c.protocol_version.load(Ordering::Relaxed) == 3)
        .unwrap_or(false)
}

/// VADD key [REDUCE dim] (FP32 | VALUES num) vector element [CAS] [NOQUANT|Q8|BIN] [EF ef] [SETATTR attrs] [M links]
/// RESP2: Integer reply (1 if added, 0 if not added)
/// RESP3: Boolean reply (true if added, false if not added)
fn cmd_vadd(store: &Store, args: &[Bytes], client: Option<&ClientState>) -> Result<RespValue> {
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
        let mut fixed_level: Option<u8> = None;

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
            } else if eq_ignore_case(&args[idx], b"_LEVEL") {
                // Internal option for replication determinism - not part of public API
                idx += 1;
                if idx >= args.len() {
                    return Err(Error::Syntax);
                }
                fixed_level = Some(parse_int(&args[idx])? as u8);
                idx += 1;
            } else {
                idx += 1;
            }
        }

        let (added, _level) = store.vadd(
            key,
            reduce_dim,
            vector,
            element,
            quant,
            ef_construction,
            m,
            attrs,
            fixed_level,
        )?;

        return if is_resp3(client) {
            Ok(RespValue::boolean(added))
        } else {
            Ok(RespValue::integer(if added { 1 } else { 0 }))
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
    let mut fixed_level: Option<u8> = None;
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
        } else if eq_ignore_case(&args[idx], b"_LEVEL") {
            // Internal option for replication determinism - not part of public API
            idx += 1;
            if idx >= args.len() {
                return Err(Error::Syntax);
            }
            fixed_level = Some(parse_int(&args[idx])? as u8);
            idx += 1;
        } else {
            idx += 1; // Skip unknown options
        }
    }

    let (added, _level) = store.vadd(
        key,
        reduce_dim,
        vector,
        element,
        quant,
        ef_construction,
        m,
        attrs,
        fixed_level,
    )?;

    if is_resp3(client) {
        Ok(RespValue::boolean(added))
    } else {
        Ok(RespValue::integer(if added { 1 } else { 0 }))
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
/// Returns the number of dimensions of the vectors in the specified vector set.
/// RESP2/RESP3: Integer reply on success, Simple error reply if key doesn't exist
fn cmd_vdim(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("VDIM"));
    }
    match store.vdim(&args[0]) {
        Some(dim) => Ok(RespValue::integer(dim as i64)),
        None => Err(Error::Custom("ERR no such key".into())),
    }
}

/// VEMB key element [RAW]
/// Returns the approximate vector associated with a given element.
/// RESP2: Array of bulk strings (float values as strings)
/// RESP3: Array of doubles
/// With RAW: [quant_type, raw_blob, norm, (range for q8)]
fn cmd_vemb(store: &Store, args: &[Bytes], client: Option<&ClientState>) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("VEMB"));
    }

    let raw = args.len() > 2 && eq_ignore_case(&args[2], b"RAW");
    let resp3 = is_resp3(client);

    match store.vemb_full(&args[0], &args[1], raw) {
        Some(result) => {
            if raw {
                // RAW format: [quant_type, raw_blob, norm, (range for q8)]
                let mut values: Vec<RespValue> = vec![
                    RespValue::SimpleString(Bytes::from(result.quant_type)),
                    RespValue::bulk(result.raw_data),
                ];

                if resp3 {
                    values.push(RespValue::double(result.norm as f64));
                    if let Some(range) = result.quant_range {
                        values.push(RespValue::double(range as f64));
                    }
                } else {
                    values.push(RespValue::bulk(Bytes::from(result.norm.to_string())));
                    if let Some(range) = result.quant_range {
                        values.push(RespValue::bulk(Bytes::from(range.to_string())));
                    }
                }

                Ok(RespValue::array(values))
            } else {
                // Normal format: array of floats
                let values: Vec<RespValue> = if resp3 {
                    result
                        .vector
                        .iter()
                        .map(|&v| RespValue::double(v as f64))
                        .collect()
                } else {
                    result
                        .vector
                        .iter()
                        .map(|&v| RespValue::bulk(Bytes::from(v.to_string())))
                        .collect()
                };
                Ok(RespValue::array(values))
            }
        }
        None => Ok(RespValue::null()),
    }
}

/// VGETATTR key element
/// Returns the JSON attributes associated with an element.
/// RESP2/RESP3: Simple string reply with JSON, or null if not found
fn cmd_vgetattr(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("VGETATTR"));
    }

    match store.vgetattr(&args[0], &args[1]) {
        Some(attrs) => Ok(RespValue::SimpleString(Bytes::from(attrs))),
        None => Ok(RespValue::null()),
    }
}

/// VINFO key
fn cmd_vinfo(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    use crate::storage::ops::vector_ops::VInfoValue;

    if args.is_empty() {
        return Err(Error::WrongArity("VINFO"));
    }

    match store.vinfo(&args[0]) {
        Some(info) => {
            let values: Vec<RespValue> = info
                .into_iter()
                .flat_map(|(k, v)| {
                    let val = match v {
                        VInfoValue::Int(i) => RespValue::integer(i),
                        VInfoValue::Str(s) => RespValue::bulk(Bytes::from(s)),
                    };
                    vec![RespValue::bulk(Bytes::from(k)), val]
                })
                .collect();
            Ok(RespValue::array(values))
        }
        None => Ok(RespValue::null()),
    }
}

/// VISMEMBER key element
/// Check if an element exists in a vector set.
/// RESP2: Integer reply (1 if exists, 0 if not)
/// RESP3: Boolean reply (true if exists, false if not)
fn cmd_vismember(store: &Store, args: &[Bytes], client: Option<&ClientState>) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("VISMEMBER"));
    }

    let is_member = store.vismember(&args[0], &args[1]);

    if is_resp3(client) {
        Ok(RespValue::boolean(is_member))
    } else {
        Ok(RespValue::integer(if is_member { 1 } else { 0 }))
    }
}

/// VLINKS key element [WITHSCORES]
/// Return the neighbors of a specified element in a vector set.
/// RESP2 without WITHSCORES: Array of arrays of bulk strings
/// RESP2 with WITHSCORES: Array of arrays with interleaved [element, score, ...]
/// RESP3 without WITHSCORES: Array of arrays of bulk strings
/// RESP3 with WITHSCORES: Array of maps {element: score, ...}
fn cmd_vlinks(store: &Store, args: &[Bytes], client: Option<&ClientState>) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("VLINKS"));
    }

    let with_scores = args.len() > 2 && eq_ignore_case(&args[2], b"WITHSCORES");
    let resp3 = is_resp3(client);

    match store.vlinks(&args[0], &args[1], with_scores) {
        Some(layers) => {
            let layer_values: Vec<RespValue> = layers
                .into_iter()
                .map(|layer| {
                    if with_scores && resp3 {
                        // RESP3 with WITHSCORES: map of element -> score
                        let pairs: Vec<(RespValue, RespValue)> = layer
                            .into_iter()
                            .map(|(elem, score)| {
                                let score_val = score
                                    .map(|s| RespValue::double(s as f64))
                                    .unwrap_or(RespValue::null());
                                (RespValue::bulk(elem), score_val)
                            })
                            .collect();
                        RespValue::map(pairs)
                    } else if with_scores {
                        // RESP2 with WITHSCORES: interleaved array [elem, score, elem, score, ...]
                        let mut items: Vec<RespValue> = Vec::new();
                        for (elem, score) in layer {
                            items.push(RespValue::bulk(elem));
                            if let Some(s) = score {
                                items.push(RespValue::bulk(Bytes::from(s.to_string())));
                            }
                        }
                        RespValue::array(items)
                    } else {
                        // No scores: array of elements
                        let neighbors: Vec<RespValue> = layer
                            .into_iter()
                            .map(|(elem, _)| RespValue::bulk(elem))
                            .collect();
                        RespValue::array(neighbors)
                    }
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
/// Remove an element from a vector set.
/// RESP2: Integer reply (1 if removed, 0 if not)
/// RESP3: Boolean reply (true if removed, false if not)
fn cmd_vrem(store: &Store, args: &[Bytes], client: Option<&ClientState>) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("VREM"));
    }

    let removed = store.vrem(&args[0], &args[1]);

    if is_resp3(client) {
        Ok(RespValue::boolean(removed))
    } else {
        Ok(RespValue::integer(if removed { 1 } else { 0 }))
    }
}

/// VSETATTR key element "{ JSON obj }"
/// Associate a JSON object with an element in a vector set.
/// RESP2: Integer reply (1 if set, 0 if key/element not found)
/// RESP3: Boolean reply (true if set, false if key/element not found)
/// Note: Invalid JSON is stored as-is and filtered out during VSIM FILTER
fn cmd_vsetattr(store: &Store, args: &[Bytes], client: Option<&ClientState>) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("VSETATTR"));
    }

    let attrs = if args[2].is_empty() {
        // Empty string means remove attributes
        None
    } else {
        let json_str = std::str::from_utf8(&args[2]).map_err(|_| Error::Syntax)?;
        // Try to parse as JSON, but store as string value if parsing fails
        // This allows storing arbitrary strings; filters will handle them appropriately
        match sonic_rs::from_str::<sonic_rs::Value>(json_str) {
            Ok(value) => Some(Box::new(value)),
            Err(_) => {
                // Store as a raw string value - will be filtered out by FILTER expressions
                Some(Box::new(sonic_rs::Value::from(json_str)))
            }
        }
    };

    let updated = store.vsetattr(&args[0], &args[1], attrs);

    if is_resp3(client) {
        Ok(RespValue::boolean(updated))
    } else {
        Ok(RespValue::integer(if updated { 1 } else { 0 }))
    }
}

/// Maximum allowed EF value
const MAX_EF_VALUE: usize = 1_000_000;

/// VSIM key (ELE | FP32 | VALUES num) (vector | element) [WITHSCORES] [WITHATTRIBS] [COUNT num] [EPSILON delta] [EF ef] [FILTER expr] [FILTER-EF ef] [TRUTH] [NOTHREAD]
fn cmd_vsim(store: &Store, args: &[Bytes], client: Option<&ClientState>) -> Result<RespValue> {
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
    let mut epsilon: Option<f32> = None;
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
            let ef_val = parse_int(&args[idx])? as usize;
            if ef_val > MAX_EF_VALUE {
                return Err(Error::Custom(format!(
                    "ERR invalid EF value: {} exceeds maximum allowed value {}",
                    ef_val, MAX_EF_VALUE
                )));
            }
            ef = Some(ef_val);
            idx += 1;
        } else if eq_ignore_case(&args[idx], b"EPSILON") {
            idx += 1;
            if idx >= args.len() {
                return Err(Error::Syntax);
            }
            epsilon = Some(parse_float(&args[idx])?);
            idx += 1;
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
        epsilon,
    )?;

    // Check if RESP3 protocol
    let resp3 = is_resp3(client);

    // RESP3: Return map when WITHSCORES or WITHATTRIBS is used
    // Map has element as key, and value is:
    // - Just WITHSCORES: double (the score)
    // - Just WITHATTRIBS: bulk string (the JSON attributes) or null
    // - Both: array [double score, bulk string attrs]
    if resp3 && (with_scores || with_attribs) {
        let pairs: Vec<(RespValue, RespValue)> = results
            .into_iter()
            .map(|(elem, score, attrs)| {
                let key = RespValue::bulk(elem);
                let value = if with_scores && with_attribs {
                    // Both: value is array [score, attrs]
                    let score_val = score
                        .map(|s| RespValue::double(s as f64))
                        .unwrap_or(RespValue::null());
                    let attrs_val = attrs
                        .map(|a| RespValue::bulk(Bytes::from(a)))
                        .unwrap_or(RespValue::null());
                    RespValue::array(vec![score_val, attrs_val])
                } else if with_scores {
                    // Just scores: value is the score as double
                    score
                        .map(|s| RespValue::double(s as f64))
                        .unwrap_or(RespValue::null())
                } else {
                    // Just attribs: value is the attrs directly
                    attrs
                        .map(|a| RespValue::bulk(Bytes::from(a)))
                        .unwrap_or(RespValue::null())
                };
                (key, value)
            })
            .collect();
        return Ok(RespValue::map(pairs));
    }

    // RESP2 format: flat array with alternating values
    // - No options: [elem1, elem2, ...]
    // - WITHSCORES: [elem1, score1, elem2, score2, ...]
    // - WITHATTRIBS: [elem1, attr1, elem2, attr2, ...]
    // - WITHSCORES + WITHATTRIBS: [elem1, score1, attr1, elem2, score2, attr2, ...]
    let mut values: Vec<RespValue> = Vec::new();
    for (elem, score, attrs) in results {
        values.push(RespValue::bulk(elem));
        if let Some(s) = score {
            values.push(RespValue::bulk(Bytes::from(s.to_string())));
        }
        if let Some(a) = attrs {
            values.push(RespValue::bulk(Bytes::from(a)));
        } else if with_attribs {
            // Return null for missing attributes when WITHATTRIBS is requested
            values.push(RespValue::null());
        }
    }

    Ok(RespValue::array(values))
}
