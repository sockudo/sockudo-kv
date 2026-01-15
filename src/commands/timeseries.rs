//! TimeSeries command handlers
//!
//! Commands: TS.CREATE, TS.ADD, TS.MADD, TS.GET, TS.DEL, TS.INCRBY, TS.DECRBY,
//! TS.ALTER, TS.INFO, TS.RANGE, TS.REVRANGE, TS.MRANGE, TS.MREVRANGE,
//! TS.MGET, TS.QUERYINDEX, TS.CREATERULE, TS.DELETERULE

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::storage::{Aggregation, DuplicatePolicy, Store, now_ms};

/// Parse integer from bytes
#[inline]
fn parse_int(b: &[u8]) -> Result<i64> {
    std::str::from_utf8(b)
        .map_err(|_| Error::NotInteger)?
        .parse()
        .map_err(|_| Error::NotInteger)
}

/// Parse float from bytes
#[inline]
fn parse_float(b: &[u8]) -> Result<f64> {
    std::str::from_utf8(b)
        .map_err(|_| Error::NotFloat)?
        .parse()
        .map_err(|_| Error::NotFloat)
}

/// Case-insensitive comparison
#[inline]
fn eq_ignore_ascii_case(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.eq_ignore_ascii_case(y))
}

/// Execute TimeSeries commands
pub fn execute(store: &Store, cmd: &[u8], args: &[Bytes]) -> Result<RespValue> {
    match cmd {
        b"TS.CREATE" => cmd_ts_create(store, args),
        b"TS.ADD" => cmd_ts_add(store, args),
        b"TS.MADD" => cmd_ts_madd(store, args),
        b"TS.GET" => cmd_ts_get(store, args),
        b"TS.DEL" => cmd_ts_del(store, args),
        b"TS.INCRBY" => cmd_ts_incrby(store, args, 1.0),
        b"TS.DECRBY" => cmd_ts_incrby(store, args, -1.0),
        b"TS.ALTER" => cmd_ts_alter(store, args),
        b"TS.INFO" => cmd_ts_info(store, args),
        b"TS.RANGE" => cmd_ts_range(store, args, false),
        b"TS.REVRANGE" => cmd_ts_range(store, args, true),
        b"TS.MRANGE" => cmd_ts_mrange(store, args, false),
        b"TS.MREVRANGE" => cmd_ts_mrange(store, args, true),
        b"TS.MGET" => cmd_ts_mget(store, args),
        b"TS.QUERYINDEX" => cmd_ts_queryindex(store, args),
        b"TS.CREATERULE" => cmd_ts_createrule(store, args),
        b"TS.DELETERULE" => cmd_ts_deleterule(store, args),
        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd).into_owned(),
        )),
    }
}

/// TS.CREATE key [RETENTION retentionPeriod] [ENCODING ...] [CHUNK SIZE size]
/// [DUPLICATE_POLICY policy] [LABELS label value ...]
fn cmd_ts_create(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("TS.CREATE"));
    }

    let key = args[0].clone();
    let mut retention = 0i64;
    let mut _chunk_size = 4096usize;
    let mut duplicate_policy = DuplicatePolicy::Block;
    let mut _ignore_max_time_diff = 0i64;
    let mut _ignore_max_val_diff = 0.0f64;
    let mut labels = Vec::new();

    let mut i = 1;
    while i < args.len() {
        let opt = &args[i];
        if eq_ignore_ascii_case(opt, b"RETENTION") {
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            retention = parse_int(&args[i])?;
        } else if eq_ignore_ascii_case(opt, b"ENCODING") {
            i += 1; // Skip encoding value (we don't use it)
        } else if eq_ignore_ascii_case(opt, b"CHUNK_SIZE") || eq_ignore_ascii_case(opt, b"CHUNK") {
            i += 1;
            if eq_ignore_ascii_case(&args[i], b"SIZE") {
                i += 1;
            }
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            _chunk_size = parse_int(&args[i])? as usize;
        } else if eq_ignore_ascii_case(opt, b"DUPLICATE_POLICY") {
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            duplicate_policy = DuplicatePolicy::from_bytes(&args[i]).ok_or(Error::Syntax)?;
        } else if eq_ignore_ascii_case(opt, b"IGNORE") {
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            _ignore_max_time_diff = parse_int(&args[i])?;
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            _ignore_max_val_diff = parse_float(&args[i])?;
        } else if eq_ignore_ascii_case(opt, b"LABELS") {
            i += 1;
            while i + 1 < args.len() {
                let label = String::from_utf8_lossy(&args[i]).into_owned();
                let value = String::from_utf8_lossy(&args[i + 1]).into_owned();
                labels.push((label, value));
                i += 2;
            }
            break;
        } else {
            return Err(Error::Syntax);
        }
        i += 1;
    }

    store.ts_create(key, retention, duplicate_policy, labels)?;
    Ok(RespValue::ok())
}

/// TS.ADD key timestamp value [RETENTION retentionPeriod] [ENCODING [COMPRESSED|UNCOMPRESSED]]
/// [CHUNK_SIZE size] [ON_DUPLICATE policy] [LABELS label value ...]
fn cmd_ts_add(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("TS.ADD"));
    }

    let key = args[0].clone();
    let timestamp = if args[1].as_ref() == b"*" {
        now_ms()
    } else {
        parse_int(&args[1])?
    };
    let value = parse_float(&args[2])?;

    // Parse additional options
    let mut retention: Option<i64> = None;
    let mut on_duplicate: Option<DuplicatePolicy> = None;
    let mut labels: Option<Vec<(String, String)>> = None;

    let mut i = 3;
    while i < args.len() {
        let opt = &args[i];
        if eq_ignore_ascii_case(opt, b"RETENTION") {
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            retention = Some(parse_int(&args[i])?);
        } else if eq_ignore_ascii_case(opt, b"ENCODING") {
            i += 1; // Skip encoding value (we don't use it, always compressed internally)
        } else if eq_ignore_ascii_case(opt, b"CHUNK_SIZE") {
            i += 1; // Skip chunk size value (we use fixed internal chunking)
        } else if eq_ignore_ascii_case(opt, b"ON_DUPLICATE") {
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            on_duplicate = Some(DuplicatePolicy::from_bytes(&args[i]).ok_or(Error::Syntax)?);
        } else if eq_ignore_ascii_case(opt, b"LABELS") {
            i += 1;
            let mut new_labels = Vec::new();
            while i + 1 < args.len() {
                // Check if next token is another option keyword
                if eq_ignore_ascii_case(&args[i], b"RETENTION")
                    || eq_ignore_ascii_case(&args[i], b"ENCODING")
                    || eq_ignore_ascii_case(&args[i], b"CHUNK_SIZE")
                    || eq_ignore_ascii_case(&args[i], b"ON_DUPLICATE")
                {
                    break;
                }
                let label = String::from_utf8_lossy(&args[i]).into_owned();
                let value = String::from_utf8_lossy(&args[i + 1]).into_owned();
                new_labels.push((label, value));
                i += 2;
            }
            labels = Some(new_labels);
            continue; // Don't increment i again
        } else {
            return Err(Error::Syntax);
        }
        i += 1;
    }

    // Use ts_add_with_options if any options are provided, else use simple ts_add
    let result =
        store.ts_add_with_options(key, timestamp, value, retention, on_duplicate, labels)?;
    Ok(RespValue::integer(result))
}

/// TS.MADD key timestamp value [key timestamp value ...]
fn cmd_ts_madd(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 || !args.len().is_multiple_of(3) {
        return Err(Error::WrongArity("TS.MADD"));
    }

    let mut samples = Vec::new();
    for chunk in args.chunks_exact(3) {
        let key = chunk[0].clone();
        let timestamp = if chunk[1].as_ref() == b"*" {
            now_ms()
        } else {
            parse_int(&chunk[1])?
        };
        let value = parse_float(&chunk[2])?;
        samples.push((key, timestamp, value));
    }

    let results = store.ts_madd(samples);
    Ok(RespValue::array(
        results
            .into_iter()
            .map(|r| match r {
                Ok(ts) => RespValue::integer(ts),
                Err(e) => RespValue::error(&e.to_string()),
            })
            .collect(),
    ))
}

/// TS.GET key [LATEST]
fn cmd_ts_get(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("TS.GET"));
    }

    match store.ts_get(&args[0]) {
        Some((ts, val)) => Ok(RespValue::array(vec![
            RespValue::integer(ts),
            RespValue::bulk_string(&val.to_string()),
        ])),
        None => Ok(RespValue::array(vec![])),
    }
}

/// TS.DEL key fromTimestamp toTimestamp
fn cmd_ts_del(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("TS.DEL"));
    }

    let from = parse_int(&args[1])?;
    let to = parse_int(&args[2])?;
    let count = store.ts_del(&args[0], from, to)?;
    Ok(RespValue::integer(count as i64))
}

/// TS.INCRBY / TS.DECRBY key value [TIMESTAMP timestamp] [options...]
fn cmd_ts_incrby(store: &Store, args: &[Bytes], sign: f64) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("TS.INCRBY"));
    }

    let key = args[0].clone();
    let value = parse_float(&args[1])? * sign;
    let mut timestamp = None;

    let mut i = 2;
    while i < args.len() {
        if eq_ignore_ascii_case(&args[i], b"TIMESTAMP") {
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            timestamp = Some(if args[i].as_ref() == b"*" {
                now_ms()
            } else {
                parse_int(&args[i])?
            });
        }
        i += 1;
    }

    let result = store.ts_incrby(key, value, timestamp)?;
    Ok(RespValue::integer(result))
}

/// TS.ALTER key [options...]
fn cmd_ts_alter(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("TS.ALTER"));
    }

    let key = &args[0];
    let mut retention = None;
    let mut _chunk_size = None;
    let mut duplicate_policy = None;
    let mut labels = None;

    let mut i = 1;
    while i < args.len() {
        let opt = &args[i];
        if eq_ignore_ascii_case(opt, b"RETENTION") {
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            retention = Some(parse_int(&args[i])?);
        } else if eq_ignore_ascii_case(opt, b"CHUNK_SIZE") {
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            _chunk_size = Some(parse_int(&args[i])? as usize);
        } else if eq_ignore_ascii_case(opt, b"DUPLICATE_POLICY") {
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            duplicate_policy = Some(DuplicatePolicy::from_bytes(&args[i]).ok_or(Error::Syntax)?);
        } else if eq_ignore_ascii_case(opt, b"LABELS") {
            i += 1;
            let mut new_labels = Vec::new();
            while i + 1 < args.len() {
                let label = String::from_utf8_lossy(&args[i]).into_owned();
                let value = String::from_utf8_lossy(&args[i + 1]).into_owned();
                new_labels.push((label, value));
                i += 2;
            }
            labels = Some(new_labels);
            break;
        }
        i += 1;
    }

    store.ts_alter(key, retention, duplicate_policy, labels)?;
    Ok(RespValue::ok())
}

/// TS.INFO key [DEBUG]
fn cmd_ts_info(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("TS.INFO"));
    }

    match store.ts_info(&args[0]) {
        Some(info) => {
            let mut result = vec![
                RespValue::bulk_string("totalSamples"),
                RespValue::integer(info.total_samples as i64),
                RespValue::bulk_string("memoryUsage"),
                RespValue::integer(info.memory_usage as i64),
                RespValue::bulk_string("firstTimestamp"),
                RespValue::integer(info.first_timestamp),
                RespValue::bulk_string("lastTimestamp"),
                RespValue::integer(info.last_timestamp),
                RespValue::bulk_string("retentionTime"),
                RespValue::integer(info.retention_ms),
                RespValue::bulk_string("chunkCount"),
                RespValue::integer(info.chunk_count as i64),
                RespValue::bulk_string("chunkSize"),
                RespValue::integer(info.chunk_size as i64),
                RespValue::bulk_string("duplicatePolicy"),
                RespValue::bulk_string(info.duplicate_policy.as_str()),
            ];

            // Add labels
            result.push(RespValue::bulk_string("labels"));
            let labels: Vec<RespValue> = info
                .labels
                .iter()
                .flat_map(|(k, v)| vec![RespValue::bulk_string(k), RespValue::bulk_string(v)])
                .collect();
            result.push(RespValue::array(labels));

            // Add rules
            result.push(RespValue::bulk_string("rules"));
            let rules: Vec<RespValue> = info
                .rules
                .iter()
                .map(|r| {
                    RespValue::array(vec![
                        RespValue::bulk(r.dest_key.clone()),
                        RespValue::integer(r.bucket_duration),
                    ])
                })
                .collect();
            result.push(RespValue::array(rules));

            Ok(RespValue::array(result))
        }
        None => Err(Error::Other("TSDB: the key does not exist")),
    }
}

/// TS.RANGE / TS.REVRANGE key fromTimestamp toTimestamp [options...]
fn cmd_ts_range(store: &Store, args: &[Bytes], reverse: bool) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity(if reverse {
            "TS.REVRANGE"
        } else {
            "TS.RANGE"
        }));
    }

    let key = &args[0];
    let from = if args[1].as_ref() == b"-" {
        i64::MIN
    } else {
        parse_int(&args[1])?
    };
    let to = if args[2].as_ref() == b"+" {
        i64::MAX
    } else {
        parse_int(&args[2])?
    };

    let mut count = None;
    let mut aggregation = None;

    let mut i = 3;
    while i < args.len() {
        let opt = &args[i];
        if eq_ignore_ascii_case(opt, b"COUNT") {
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            count = Some(parse_int(&args[i])? as usize);
        } else if eq_ignore_ascii_case(opt, b"AGGREGATION") {
            i += 1;
            if i + 1 >= args.len() {
                return Err(Error::Syntax);
            }
            let agg = Aggregation::from_bytes(&args[i]).ok_or(Error::Syntax)?;
            i += 1;
            let bucket = parse_int(&args[i])?;
            aggregation = Some((agg, bucket));
        }
        i += 1;
    }

    let samples = if reverse {
        store.ts_revrange(key, from, to, count, aggregation)
    } else {
        store.ts_range(key, from, to, count, aggregation)
    };

    match samples {
        Some(data) => Ok(RespValue::array(
            data.into_iter()
                .map(|(ts, val)| {
                    RespValue::array(vec![
                        RespValue::integer(ts),
                        RespValue::bulk_string(&val.to_string()),
                    ])
                })
                .collect(),
        )),
        None => Ok(RespValue::array(vec![])),
    }
}

/// TS.MRANGE / TS.MREVRANGE
fn cmd_ts_mrange(store: &Store, args: &[Bytes], reverse: bool) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity(if reverse {
            "TS.MREVRANGE"
        } else {
            "TS.MRANGE"
        }));
    }

    let from = if args[0].as_ref() == b"-" {
        i64::MIN
    } else {
        parse_int(&args[0])?
    };
    let to = if args[1].as_ref() == b"+" {
        i64::MAX
    } else {
        parse_int(&args[1])?
    };

    // Parse filters
    let mut filters = Vec::new();
    let mut count = None;
    let mut aggregation = None;

    let mut i = 2;
    while i < args.len() {
        if eq_ignore_ascii_case(&args[i], b"FILTER") {
            i += 1;
            while i < args.len()
                && !eq_ignore_ascii_case(&args[i], b"COUNT")
                && !eq_ignore_ascii_case(&args[i], b"AGGREGATION")
            {
                // Parse filter like "label=value"
                let filter_str = String::from_utf8_lossy(&args[i]);
                if let Some(eq_pos) = filter_str.find('=') {
                    let label = filter_str[..eq_pos].to_string();
                    let value = filter_str[eq_pos + 1..].to_string();
                    filters.push((label, value));
                }
                i += 1;
            }
        } else if eq_ignore_ascii_case(&args[i], b"COUNT") {
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            count = Some(parse_int(&args[i])? as usize);
            i += 1;
        } else if eq_ignore_ascii_case(&args[i], b"AGGREGATION") {
            i += 1;
            if i + 1 >= args.len() {
                return Err(Error::Syntax);
            }
            let agg = Aggregation::from_bytes(&args[i]).ok_or(Error::Syntax)?;
            i += 1;
            let bucket = parse_int(&args[i])?;
            aggregation = Some((agg, bucket));
            i += 1;
        } else {
            i += 1;
        }
    }

    let results = store.ts_mrange(from, to, &filters, count, aggregation);

    Ok(RespValue::array(
        results
            .into_iter()
            .map(|(key, samples)| {
                // Fetch labels from ts_info
                let labels = store
                    .ts_info(&key)
                    .map(|info| {
                        info.labels
                            .iter()
                            .flat_map(|(k, v)| {
                                vec![RespValue::bulk_string(k), RespValue::bulk_string(v)]
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();

                RespValue::array(vec![
                    RespValue::bulk(key),
                    RespValue::array(labels),
                    RespValue::array(
                        samples
                            .into_iter()
                            .map(|(ts, val)| {
                                RespValue::array(vec![
                                    RespValue::integer(ts),
                                    RespValue::bulk_string(&val.to_string()),
                                ])
                            })
                            .collect(),
                    ),
                ])
            })
            .collect(),
    ))
}

/// TS.MGET FILTER filter...
fn cmd_ts_mget(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("TS.MGET"));
    }

    // Parse filters
    let mut filters = Vec::new();
    let mut i = 0;

    // Skip optional LATEST
    if i < args.len() && eq_ignore_ascii_case(&args[i], b"LATEST") {
        i += 1;
    }

    // Skip optional WITHLABELS
    if i < args.len() && eq_ignore_ascii_case(&args[i], b"WITHLABELS") {
        i += 1;
    }

    // Expect FILTER
    if i < args.len() && eq_ignore_ascii_case(&args[i], b"FILTER") {
        i += 1;
    }

    while i < args.len() {
        let filter_str = String::from_utf8_lossy(&args[i]);
        if let Some(eq_pos) = filter_str.find('=') {
            let label = filter_str[..eq_pos].to_string();
            let value = filter_str[eq_pos + 1..].to_string();
            filters.push((label, value));
        }
        i += 1;
    }

    let results = store.ts_mget(&filters);

    Ok(RespValue::array(
        results
            .into_iter()
            .map(|(key, sample)| {
                // Fetch labels from ts_info
                let labels = store
                    .ts_info(&key)
                    .map(|info| {
                        info.labels
                            .iter()
                            .flat_map(|(k, v)| {
                                vec![RespValue::bulk_string(k), RespValue::bulk_string(v)]
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();

                let sample_arr = match sample {
                    Some((ts, val)) => vec![
                        RespValue::integer(ts),
                        RespValue::bulk_string(&val.to_string()),
                    ],
                    None => vec![],
                };
                RespValue::array(vec![
                    RespValue::bulk(key),
                    RespValue::array(labels),
                    RespValue::array(sample_arr),
                ])
            })
            .collect(),
    ))
}

/// TS.QUERYINDEX filter...
fn cmd_ts_queryindex(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("TS.QUERYINDEX"));
    }

    let mut filters = Vec::new();
    for arg in args {
        let filter_str = String::from_utf8_lossy(arg);
        if let Some(eq_pos) = filter_str.find('=') {
            let label = filter_str[..eq_pos].to_string();
            let value = filter_str[eq_pos + 1..].to_string();
            filters.push((label, value));
        }
    }

    let keys = store.ts_query_index(&filters);
    Ok(RespValue::array(
        keys.into_iter().map(RespValue::bulk).collect(),
    ))
}

/// TS.CREATERULE sourceKey destKey AGGREGATION aggregationType bucketDuration
fn cmd_ts_createrule(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 4 {
        return Err(Error::WrongArity("TS.CREATERULE"));
    }

    let source_key = &args[0];
    let dest_key = args[1].clone();

    // Find AGGREGATION
    let mut i = 2;
    if !eq_ignore_ascii_case(&args[i], b"AGGREGATION") {
        return Err(Error::Syntax);
    }
    i += 1;

    if i + 1 >= args.len() {
        return Err(Error::Syntax);
    }

    let aggregation = Aggregation::from_bytes(&args[i]).ok_or(Error::Syntax)?;
    i += 1;
    let bucket_duration = parse_int(&args[i])?;

    let align_timestamp = if i + 1 < args.len() {
        parse_int(&args[i + 1]).unwrap_or(0)
    } else {
        0
    };

    store.ts_createrule(
        source_key,
        dest_key,
        aggregation,
        bucket_duration,
        align_timestamp,
    )?;
    Ok(RespValue::ok())
}

/// TS.DELETERULE sourceKey destKey
fn cmd_ts_deleterule(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("TS.DELETERULE"));
    }

    store.ts_deleterule(&args[0], &args[1])?;
    Ok(RespValue::ok())
}
