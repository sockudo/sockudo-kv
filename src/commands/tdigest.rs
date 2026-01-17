//! T-Digest command handlers
//!
//! Implements all TDIGEST.* commands for quantile estimation.

use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::storage::Store;
use bytes::Bytes;

/// Execute a T-Digest command
pub fn execute(store: &Store, cmd: &[u8], args: &[Bytes]) -> Result<RespValue> {
    match cmd.to_ascii_uppercase().as_slice() {
        b"TDIGEST.CREATE" => cmd_create(store, args),
        b"TDIGEST.ADD" => cmd_add(store, args),
        b"TDIGEST.MERGE" => cmd_merge(store, args),
        b"TDIGEST.RESET" => cmd_reset(store, args),
        b"TDIGEST.QUANTILE" => cmd_quantile(store, args),
        b"TDIGEST.CDF" => cmd_cdf(store, args),
        b"TDIGEST.RANK" => cmd_rank(store, args),
        b"TDIGEST.REVRANK" => cmd_revrank(store, args),
        b"TDIGEST.BYRANK" => cmd_byrank(store, args),
        b"TDIGEST.BYREVRANK" => cmd_byrevrank(store, args),
        b"TDIGEST.MIN" => cmd_min(store, args),
        b"TDIGEST.MAX" => cmd_max(store, args),
        b"TDIGEST.TRIMMED_MEAN" => cmd_trimmed_mean(store, args),
        b"TDIGEST.INFO" => cmd_info(store, args),
        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd).into_owned(),
        )),
    }
}

/// Parse a float argument
fn parse_float(arg: &Bytes) -> Result<f64> {
    std::str::from_utf8(arg)
        .map_err(|_| Error::NotFloat)?
        .parse()
        .map_err(|_| Error::NotFloat)
}

/// Parse an integer argument
fn parse_int(arg: &Bytes) -> Result<i64> {
    std::str::from_utf8(arg)
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)
}

/// TDIGEST.CREATE key [COMPRESSION compression]
fn cmd_create(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("TDIGEST.CREATE"));
    }

    let key = &args[0];
    let mut compression = 100.0;

    // Parse optional COMPRESSION
    let mut i = 1;
    while i < args.len() {
        if args[i].eq_ignore_ascii_case(b"COMPRESSION") {
            if i + 1 >= args.len() {
                return Err(Error::Syntax);
            }
            compression = parse_float(&args[i + 1])?;
            if compression < 10.0 {
                return Err(Error::Custom(
                    "ERR T-Digest: compression must be at least 10".to_string(),
                ));
            }
            i += 2;
        } else {
            return Err(Error::Syntax);
        }
    }

    match store.tdigest_create(key, compression) {
        Ok(_) => Ok(RespValue::ok()),
        Err(e) => Err(Error::Custom(e.to_string())),
    }
}

/// TDIGEST.ADD key value [value ...]
fn cmd_add(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("TDIGEST.ADD"));
    }

    let key = &args[0];

    // Parse values
    let mut values = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        values.push(parse_float(arg)?);
    }

    match store.tdigest_add(key, &values) {
        Ok(()) => Ok(RespValue::ok()),
        Err(e) => Err(Error::Custom(e.to_string())),
    }
}

/// TDIGEST.MERGE destkey numkeys sourcekey [sourcekey ...] [COMPRESSION compression] [OVERRIDE]
fn cmd_merge(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("TDIGEST.MERGE"));
    }

    let dest_key = &args[0];
    let num_keys: usize = std::str::from_utf8(&args[1])
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)?;

    if args.len() < 2 + num_keys {
        return Err(Error::WrongArity("TDIGEST.MERGE"));
    }

    let source_keys: Vec<Bytes> = args[2..2 + num_keys].to_vec();

    let mut compression = None;
    let mut override_dest = false;

    // Parse optional arguments
    let mut i = 2 + num_keys;
    while i < args.len() {
        if args[i].eq_ignore_ascii_case(b"COMPRESSION") {
            if i + 1 >= args.len() {
                return Err(Error::Syntax);
            }
            compression = Some(parse_float(&args[i + 1])?);
            i += 2;
        } else if args[i].eq_ignore_ascii_case(b"OVERRIDE") {
            override_dest = true;
            i += 1;
        } else {
            return Err(Error::Syntax);
        }
    }

    match store.tdigest_merge(dest_key, &source_keys, compression, override_dest) {
        Ok(()) => Ok(RespValue::ok()),
        Err(e) => Err(Error::Custom(e.to_string())),
    }
}

/// TDIGEST.RESET key
fn cmd_reset(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("TDIGEST.RESET"));
    }

    match store.tdigest_reset(&args[0]) {
        Ok(()) => Ok(RespValue::ok()),
        Err(e) => Err(Error::Custom(e.to_string())),
    }
}

/// TDIGEST.QUANTILE key quantile [quantile ...]
fn cmd_quantile(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("TDIGEST.QUANTILE"));
    }

    let key = &args[0];

    // Parse quantiles
    let mut quantiles = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        let q = parse_float(arg)?;
        if !(0.0..=1.0).contains(&q) {
            return Err(Error::Custom(
                "ERR T-Digest: quantile must be between 0 and 1".to_string(),
            ));
        }
        quantiles.push(q);
    }

    match store.tdigest_quantile(key, &quantiles) {
        Ok(values) => {
            let resp: Vec<RespValue> = values
                .into_iter()
                .map(|v| {
                    if v.is_nan() {
                        RespValue::Null
                    } else {
                        RespValue::bulk(Bytes::from(v.to_string()))
                    }
                })
                .collect();
            Ok(RespValue::Array(resp))
        }
        Err(e) => Err(Error::Custom(e.to_string())),
    }
}

/// TDIGEST.CDF key value [value ...]
fn cmd_cdf(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("TDIGEST.CDF"));
    }

    let key = &args[0];

    // Parse values
    let mut values = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        values.push(parse_float(arg)?);
    }

    match store.tdigest_cdf(key, &values) {
        Ok(cdfs) => {
            let resp: Vec<RespValue> = cdfs
                .into_iter()
                .map(|v| {
                    if v.is_nan() {
                        RespValue::Null
                    } else {
                        RespValue::bulk(Bytes::from(v.to_string()))
                    }
                })
                .collect();
            Ok(RespValue::Array(resp))
        }
        Err(e) => Err(Error::Custom(e.to_string())),
    }
}

/// TDIGEST.RANK key value [value ...]
fn cmd_rank(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("TDIGEST.RANK"));
    }

    let key = &args[0];

    // Parse values
    let mut values = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        values.push(parse_float(arg)?);
    }

    match store.tdigest_rank(key, &values) {
        Ok(ranks) => {
            let resp: Vec<RespValue> = ranks.into_iter().map(RespValue::integer).collect();
            Ok(RespValue::Array(resp))
        }
        Err(e) => Err(Error::Custom(e.to_string())),
    }
}

/// TDIGEST.REVRANK key value [value ...]
fn cmd_revrank(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("TDIGEST.REVRANK"));
    }

    let key = &args[0];

    // Parse values
    let mut values = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        values.push(parse_float(arg)?);
    }

    match store.tdigest_revrank(key, &values) {
        Ok(ranks) => {
            let resp: Vec<RespValue> = ranks.into_iter().map(RespValue::integer).collect();
            Ok(RespValue::Array(resp))
        }
        Err(e) => Err(Error::Custom(e.to_string())),
    }
}

/// TDIGEST.BYRANK key rank [rank ...]
fn cmd_byrank(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("TDIGEST.BYRANK"));
    }

    let key = &args[0];

    // Parse ranks
    let mut ranks = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        ranks.push(parse_int(arg)?);
    }

    match store.tdigest_byrank(key, &ranks) {
        Ok(values) => {
            let resp: Vec<RespValue> = values
                .into_iter()
                .map(|v| {
                    if v.is_nan() {
                        RespValue::Null
                    } else if v == f64::INFINITY {
                        RespValue::bulk(Bytes::from_static(b"inf"))
                    } else if v == f64::NEG_INFINITY {
                        RespValue::bulk(Bytes::from_static(b"-inf"))
                    } else {
                        RespValue::bulk(Bytes::from(v.to_string()))
                    }
                })
                .collect();
            Ok(RespValue::Array(resp))
        }
        Err(e) => Err(Error::Custom(e.to_string())),
    }
}

/// TDIGEST.BYREVRANK key rank [rank ...]
fn cmd_byrevrank(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("TDIGEST.BYREVRANK"));
    }

    let key = &args[0];

    // Parse ranks
    let mut ranks = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        ranks.push(parse_int(arg)?);
    }

    match store.tdigest_byrevrank(key, &ranks) {
        Ok(values) => {
            let resp: Vec<RespValue> = values
                .into_iter()
                .map(|v| {
                    if v.is_nan() {
                        RespValue::Null
                    } else if v == f64::INFINITY {
                        RespValue::bulk(Bytes::from_static(b"inf"))
                    } else if v == f64::NEG_INFINITY {
                        RespValue::bulk(Bytes::from_static(b"-inf"))
                    } else {
                        RespValue::bulk(Bytes::from(v.to_string()))
                    }
                })
                .collect();
            Ok(RespValue::Array(resp))
        }
        Err(e) => Err(Error::Custom(e.to_string())),
    }
}

/// TDIGEST.MIN key
fn cmd_min(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("TDIGEST.MIN"));
    }

    match store.tdigest_min(&args[0]) {
        Ok(v) => {
            if v.is_nan() {
                Ok(RespValue::Null)
            } else {
                Ok(RespValue::bulk(Bytes::from(v.to_string())))
            }
        }
        Err(e) => Err(Error::Custom(e.to_string())),
    }
}

/// TDIGEST.MAX key
fn cmd_max(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("TDIGEST.MAX"));
    }

    match store.tdigest_max(&args[0]) {
        Ok(v) => {
            if v.is_nan() {
                Ok(RespValue::Null)
            } else {
                Ok(RespValue::bulk(Bytes::from(v.to_string())))
            }
        }
        Err(e) => Err(Error::Custom(e.to_string())),
    }
}

/// TDIGEST.TRIMMED_MEAN key low_cut_quantile high_cut_quantile
fn cmd_trimmed_mean(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 3 {
        return Err(Error::WrongArity("TDIGEST.TRIMMED_MEAN"));
    }

    let key = &args[0];
    let low_quantile = parse_float(&args[1])?;
    let high_quantile = parse_float(&args[2])?;

    if low_quantile < 0.0 || low_quantile > 1.0 || high_quantile < 0.0 || high_quantile > 1.0 {
        return Err(Error::Custom(
            "ERR T-Digest: quantiles must be between 0 and 1".to_string(),
        ));
    }

    if low_quantile >= high_quantile {
        return Err(Error::Custom(
            "ERR T-Digest: low quantile must be less than high quantile".to_string(),
        ));
    }

    match store.tdigest_trimmed_mean(key, low_quantile, high_quantile) {
        Ok(v) => {
            if v.is_nan() {
                Ok(RespValue::Null)
            } else {
                Ok(RespValue::bulk(Bytes::from(v.to_string())))
            }
        }
        Err(e) => Err(Error::Custom(e.to_string())),
    }
}

/// TDIGEST.INFO key
fn cmd_info(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() != 1 {
        return Err(Error::WrongArity("TDIGEST.INFO"));
    }

    match store.tdigest_info(&args[0]) {
        Ok(info) => {
            let resp = vec![
                RespValue::bulk(Bytes::from_static(b"Compression")),
                RespValue::integer(info.compression as i64),
                RespValue::bulk(Bytes::from_static(b"Capacity")),
                RespValue::integer(info.capacity as i64),
                RespValue::bulk(Bytes::from_static(b"Merged nodes")),
                RespValue::integer(info.merged_nodes as i64),
                RespValue::bulk(Bytes::from_static(b"Unmerged nodes")),
                RespValue::integer(info.unmerged_nodes as i64),
                RespValue::bulk(Bytes::from_static(b"Merged weight")),
                RespValue::bulk(Bytes::from(info.merged_weight.to_string())),
                RespValue::bulk(Bytes::from_static(b"Unmerged weight")),
                RespValue::bulk(Bytes::from(info.unmerged_weight.to_string())),
                RespValue::bulk(Bytes::from_static(b"Observations")),
                RespValue::integer((info.merged_weight + info.unmerged_weight) as i64),
                RespValue::bulk(Bytes::from_static(b"Total compressions")),
                RespValue::integer(info.total_compressions as i64),
                RespValue::bulk(Bytes::from_static(b"Memory usage")),
                RespValue::integer(info.memory_usage as i64),
            ];
            Ok(RespValue::Array(resp))
        }
        Err(e) => Err(Error::Custom(e.to_string())),
    }
}
