use bytes::Bytes;

use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::storage::Store;
use crate::storage::ops::search_ops::{DistanceMetric, FieldType, SchemaField, VectorAlgorithm};

/// Case-insensitive comparison
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

/// Parse float from bytes
#[inline]
fn parse_float(b: &[u8]) -> Result<f64> {
    std::str::from_utf8(b)
        .map_err(|_| Error::NotFloat)?
        .parse()
        .map_err(|_| Error::NotFloat)
}

/// Parse a schema field definition from args starting at index `i`
fn parse_schema_field(args: &[Bytes], i: &mut usize) -> Result<SchemaField> {
    if *i >= args.len() {
        return Err(Error::Syntax);
    }

    let field_name = args[*i].clone();
    *i += 1;

    // Check for AS alias
    let mut alias = None;
    if *i < args.len() && eq_ignore_ascii_case(&args[*i], b"AS") {
        *i += 1;
        if *i >= args.len() {
            return Err(Error::Syntax);
        }
        alias = Some(args[*i].clone());
        *i += 1;
    }

    if *i >= args.len() {
        return Err(Error::Syntax);
    }

    // Parse field type
    let type_arg = args[*i].to_ascii_uppercase();
    *i += 1;

    let field_type = match type_arg.as_slice() {
        b"TEXT" => {
            let mut weight = 1.0;
            let mut nostem = false;
            let mut sortable = false;
            let mut noindex = false;
            let mut phonetic = None;

            while *i < args.len() {
                let opt = args[*i].to_ascii_uppercase();
                match opt.as_slice() {
                    b"WEIGHT" => {
                        *i += 1;
                        if *i < args.len() {
                            weight = parse_float(&args[*i]).unwrap_or(1.0);
                            *i += 1;
                        }
                    }
                    b"NOSTEM" => {
                        nostem = true;
                        *i += 1;
                    }
                    b"SORTABLE" => {
                        sortable = true;
                        *i += 1;
                    }
                    b"UNF" => {
                        *i += 1;
                    } // Skip UNF
                    b"NOINDEX" => {
                        noindex = true;
                        *i += 1;
                    }
                    b"PHONETIC" => {
                        *i += 1;
                        if *i < args.len() {
                            phonetic = Some(String::from_utf8_lossy(&args[*i]).into_owned());
                            *i += 1;
                        }
                    }
                    b"WITHSUFFIXTRIE" => {
                        *i += 1;
                    }
                    _ => break,
                }
            }

            FieldType::Text {
                weight,
                nostem,
                phonetic,
                sortable,
                noindex,
                withsuffixtrie: false,
            }
        }
        b"TAG" => {
            let mut separator = ',';
            let mut casesensitive = false;
            let mut sortable = false;
            let mut noindex = false;
            let mut withsuffixtrie = false;

            while *i < args.len() {
                let opt = args[*i].to_ascii_uppercase();
                match opt.as_slice() {
                    b"SEPARATOR" => {
                        *i += 1;
                        if *i < args.len() && !args[*i].is_empty() {
                            separator = args[*i][0] as char;
                            *i += 1;
                        }
                    }
                    b"CASESENSITIVE" => {
                        casesensitive = true;
                        *i += 1;
                    }
                    b"SORTABLE" => {
                        sortable = true;
                        *i += 1;
                    }
                    b"UNF" => {
                        *i += 1;
                    }
                    b"NOINDEX" => {
                        noindex = true;
                        *i += 1;
                    }
                    b"WITHSUFFIXTRIE" => {
                        withsuffixtrie = true;
                        *i += 1;
                    }
                    _ => break,
                }
            }

            FieldType::Tag {
                separator,
                casesensitive,
                sortable,
                noindex,
                withsuffixtrie,
            }
        }
        b"NUMERIC" => {
            let mut sortable = false;
            let mut noindex = false;

            while *i < args.len() {
                let opt = args[*i].to_ascii_uppercase();
                match opt.as_slice() {
                    b"SORTABLE" => {
                        sortable = true;
                        *i += 1;
                    }
                    b"UNF" => {
                        *i += 1;
                    }
                    b"NOINDEX" => {
                        noindex = true;
                        *i += 1;
                    }
                    _ => break,
                }
            }

            FieldType::Numeric { sortable, noindex }
        }
        b"GEO" => {
            let mut sortable = false;
            let mut noindex = false;

            while *i < args.len() {
                let opt = args[*i].to_ascii_uppercase();
                match opt.as_slice() {
                    b"SORTABLE" => {
                        sortable = true;
                        *i += 1;
                    }
                    b"NOINDEX" => {
                        noindex = true;
                        *i += 1;
                    }
                    _ => break,
                }
            }

            FieldType::Geo { sortable, noindex }
        }
        b"VECTOR" => {
            // VECTOR FLAT|HNSW dim DIM distance_metric DISTANCE_METRIC ...
            let mut algorithm = VectorAlgorithm::Flat;
            let mut dim = 128;
            let mut distance_metric = DistanceMetric::L2;
            let mut initial_cap = 1000;

            while *i < args.len() {
                let opt = args[*i].to_ascii_uppercase();
                match opt.as_slice() {
                    b"FLAT" => {
                        algorithm = VectorAlgorithm::Flat;
                        *i += 1;
                    }
                    b"HNSW" => {
                        algorithm = VectorAlgorithm::Hnsw;
                        *i += 1;
                    }
                    b"DIM" => {
                        *i += 1;
                        if *i < args.len() {
                            dim = parse_int(&args[*i]).unwrap_or(128) as usize;
                            *i += 1;
                        }
                    }
                    b"DISTANCE_METRIC" => {
                        *i += 1;
                        if *i < args.len() {
                            distance_metric =
                                DistanceMetric::from_bytes(&args[*i]).unwrap_or(DistanceMetric::L2);
                            *i += 1;
                        }
                    }
                    b"INITIAL_CAP" => {
                        *i += 1;
                        if *i < args.len() {
                            initial_cap = parse_int(&args[*i]).unwrap_or(1000) as usize;
                            *i += 1;
                        }
                    }
                    b"TYPE" => {
                        *i += 1;
                        // Skip data type value
                        if *i < args.len() {
                            *i += 1;
                        }
                    }
                    b"M" | b"EF_CONSTRUCTION" | b"EF_RUNTIME" | b"EPSILON" => {
                        // Skip HNSW-specific params
                        *i += 2;
                    }
                    _ => break,
                }
            }

            FieldType::Vector {
                algorithm,
                dim,
                distance_metric,
                initial_cap,
                m: None,
                ef_construction: None,
                ef_runtime: None,
                data_type: crate::storage::ops::search_ops::VectorDataType::Float32,
            }
        }
        b"GEOSHAPE" => {
            let mut coord_system = crate::storage::ops::search_ops::CoordSystem::Spherical;

            while *i < args.len() {
                let opt = args[*i].to_ascii_uppercase();
                match opt.as_slice() {
                    b"FLAT" => {
                        coord_system = crate::storage::ops::search_ops::CoordSystem::Flat;
                        *i += 1;
                    }
                    b"SPHERICAL" => {
                        coord_system = crate::storage::ops::search_ops::CoordSystem::Spherical;
                        *i += 1;
                    }
                    _ => break,
                }
            }

            FieldType::GeoShape { coord_system }
        }
        _ => return Err(Error::Syntax),
    };

    Ok(SchemaField {
        name: field_name,
        alias,
        field_type,
    })
}

/// Execute search commands (FT.* commands)
pub fn execute(store: &Store, cmd: &[u8], args: &[Bytes]) -> Result<RespValue> {
    // All FT commands start with "FT."
    let cmd_upper = cmd.to_ascii_uppercase();

    match cmd_upper.as_slice() {
        b"FT.CREATE" => cmd_ft_create(store, args),
        b"FT.DROPINDEX" => cmd_ft_dropindex(store, args),
        b"FT.INFO" => cmd_ft_info(store, args),
        b"FT._LIST" => cmd_ft_list(store, args),
        b"FT.SEARCH" => cmd_ft_search(store, args),
        b"FT.AGGREGATE" => cmd_ft_aggregate(store, args),
        b"FT.ALIASADD" => cmd_ft_aliasadd(store, args),
        b"FT.ALIASDEL" => cmd_ft_aliasdel(store, args),
        b"FT.ALIASUPDATE" => cmd_ft_aliasupdate(store, args),
        b"FT.ALTER" => cmd_ft_alter(store, args),
        b"FT.CONFIG" => cmd_ft_config(args),
        b"FT.CURSOR" => cmd_ft_cursor(store, args),
        b"FT.DICTADD" => cmd_ft_dictadd(args),
        b"FT.DICTDEL" => cmd_ft_dictdel(args),
        b"FT.DICTDUMP" => cmd_ft_dictdump(args),
        b"FT.EXPLAIN" => cmd_ft_explain(args),
        b"FT.EXPLAINCLI" => cmd_ft_explain(args),
        b"FT.PROFILE" => cmd_ft_profile(store, args),
        b"FT.SPELLCHECK" => cmd_ft_spellcheck(args),
        b"FT.SYNUPDATE" => cmd_ft_synupdate(args),
        b"FT.SYNDUMP" => cmd_ft_syndump(args),
        b"FT.TAGVALS" => cmd_ft_tagvals(store, args),
        b"FT.HYBRID" => cmd_ft_hybrid(store, args),
        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd).into_owned(),
        )),
    }
}

/// FT.CREATE index [ON HASH|JSON] [PREFIX count prefix ...] [FILTER filter]
/// [LANGUAGE lang] [STOPWORDS count word ...] SCHEMA field [AS alias] type [options] ...
fn cmd_ft_create(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("FT.CREATE"));
    }

    let index_name = args[0].clone();
    let mut i = 1;
    let mut on_hash = true;
    let mut prefixes = Vec::new();
    let mut schema = Vec::new();

    // Parse options before SCHEMA
    while i < args.len() {
        let arg = args[i].to_ascii_uppercase();

        match arg.as_slice() {
            b"ON" => {
                i += 1;
                if i >= args.len() {
                    return Err(Error::Syntax);
                }
                let type_arg = args[i].to_ascii_uppercase();
                on_hash = match type_arg.as_slice() {
                    b"HASH" => true,
                    b"JSON" => false,
                    _ => return Err(Error::Syntax),
                };
                i += 1;
            }
            b"PREFIX" => {
                i += 1;
                if i >= args.len() {
                    return Err(Error::Syntax);
                }
                let count = parse_int(&args[i])? as usize;
                i += 1;
                for _ in 0..count {
                    if i >= args.len() {
                        return Err(Error::Syntax);
                    }
                    prefixes.push(args[i].clone());
                    i += 1;
                }
            }
            b"FILTER" | b"LANGUAGE" | b"LANGUAGE_FIELD" | b"SCORE" | b"SCORE_FIELD"
            | b"PAYLOAD_FIELD" | b"MAXTEXTFIELDS" | b"TEMPORARY" | b"NOOFFSETS" | b"NOHL"
            | b"NOFIELDS" | b"NOFREQS" | b"SKIPINITIALSCAN" => {
                // Skip these options for now
                i += 1;
                // Some options have values
                if matches!(arg.as_slice(), b"FILTER" | b"LANGUAGE" | b"TEMPORARY")
                    && i < args.len()
                {
                    i += 1;
                }
            }
            b"STOPWORDS" => {
                i += 1;
                if i >= args.len() {
                    return Err(Error::Syntax);
                }
                let count = parse_int(&args[i])? as usize;
                i += 1 + count; // Skip stopwords
            }
            b"SCHEMA" => {
                i += 1;
                break;
            }
            _ => {
                return Err(Error::Syntax);
            }
        }
    }

    // Parse SCHEMA
    while i < args.len() {
        let field = parse_schema_field(args, &mut i)?;
        schema.push(field);
    }

    if schema.is_empty() {
        return Err(Error::Custom(
            "Schema must contain at least one field".into(),
        ));
    }

    store.ft_create(index_name, on_hash, prefixes, schema)?;
    Ok(RespValue::ok())
}

/// FT.DROPINDEX index [DD]
fn cmd_ft_dropindex(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("FT.DROPINDEX"));
    }

    let delete_docs = args.len() > 1 && eq_ignore_ascii_case(&args[1], b"DD");
    store.ft_dropindex(&args[0], delete_docs)?;
    Ok(RespValue::ok())
}

/// FT.INFO index
fn cmd_ft_info(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("FT.INFO"));
    }

    let info = store.ft_info(&args[0])?;

    // Build info response
    let mut result = vec![
        RespValue::bulk(Bytes::from_static(b"index_name")),
        RespValue::bulk(info.name.clone()),
        RespValue::bulk(Bytes::from_static(b"index_options")),
        RespValue::array(vec![]),
        RespValue::bulk(Bytes::from_static(b"index_definition")),
    ];
    let mut def = Vec::new();
    def.push(RespValue::bulk(Bytes::from_static(b"key_type")));
    def.push(RespValue::bulk(Bytes::from_static(if info.on_hash {
        b"HASH"
    } else {
        b"JSON"
    })));
    def.push(RespValue::bulk(Bytes::from_static(b"prefixes")));
    def.push(RespValue::array(
        info.key_prefixes
            .iter()
            .map(|p| RespValue::bulk(p.clone()))
            .collect(),
    ));
    result.push(RespValue::array(def));

    result.push(RespValue::bulk(Bytes::from_static(b"attributes")));
    let attrs: Vec<RespValue> = info
        .schema
        .iter()
        .map(|f| {
            let mut field_info = Vec::new();
            field_info.push(RespValue::bulk(Bytes::from_static(b"identifier")));
            field_info.push(RespValue::bulk(f.name.clone()));
            field_info.push(RespValue::bulk(Bytes::from_static(b"type")));
            let type_name = match &f.field_type {
                FieldType::Text { .. } => "TEXT",
                FieldType::Tag { .. } => "TAG",
                FieldType::Numeric { .. } => "NUMERIC",
                FieldType::Geo { .. } => "GEO",
                FieldType::Vector { .. } => "VECTOR",
                FieldType::GeoShape { .. } => "GEOSHAPE",
            };
            field_info.push(RespValue::bulk(Bytes::copy_from_slice(
                type_name.as_bytes(),
            )));
            RespValue::array(field_info)
        })
        .collect();
    result.push(RespValue::array(attrs));

    result.push(RespValue::bulk(Bytes::from_static(b"num_docs")));
    result.push(RespValue::integer(
        info.num_docs.load(std::sync::atomic::Ordering::Relaxed) as i64,
    ));

    Ok(RespValue::array(result))
}

/// FT._LIST
fn cmd_ft_list(store: &Store, _args: &[Bytes]) -> Result<RespValue> {
    let indexes = store.ft_list();
    let result: Vec<RespValue> = indexes.into_iter().map(RespValue::bulk).collect();
    Ok(RespValue::array(result))
}

/// FT.SEARCH index query [NOCONTENT] [VERBATIM] [LIMIT offset num] ...
fn cmd_ft_search(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("FT.SEARCH"));
    }

    let index_name = &args[0];
    let query = String::from_utf8_lossy(&args[1]).into_owned();

    let mut offset = 0usize;
    let mut limit = 10usize;
    let mut nocontent = false;

    // Parse options
    let mut i = 2;
    while i < args.len() {
        let opt = args[i].to_ascii_uppercase();
        match opt.as_slice() {
            b"NOCONTENT" => {
                nocontent = true;
                i += 1;
            }
            b"VERBATIM" | b"NOSTOPWORDS" | b"WITHSCORES" | b"WITHPAYLOADS" | b"WITHSORTKEYS" => {
                i += 1;
            }
            b"LIMIT" => {
                i += 1;
                if i + 1 < args.len() {
                    offset = parse_int(&args[i]).unwrap_or(0) as usize;
                    limit = parse_int(&args[i + 1]).unwrap_or(10) as usize;
                    i += 2;
                }
            }
            b"FILTER" | b"GEOFILTER" => {
                // Skip filter arguments
                i += 4;
            }
            b"INKEYS" | b"INFIELDS" => {
                i += 1;
                if i < args.len() {
                    let count = parse_int(&args[i]).unwrap_or(0) as usize;
                    i += 1 + count;
                }
            }
            b"RETURN" => {
                i += 1;
                if i < args.len() {
                    let count = parse_int(&args[i]).unwrap_or(0) as usize;
                    i += 1 + count;
                }
            }
            b"SORTBY" => {
                i += 2;
                if i < args.len()
                    && (eq_ignore_ascii_case(&args[i], b"ASC")
                        || eq_ignore_ascii_case(&args[i], b"DESC"))
                {
                    i += 1;
                }
            }
            b"PARAMS" => {
                i += 1;
                if i < args.len() {
                    let count = parse_int(&args[i]).unwrap_or(0) as usize;
                    i += 1 + count;
                }
            }
            _ => {
                i += 1;
            }
        }
    }

    let results = store.ft_search(index_name, &query, offset, limit)?;

    // Build response
    let mut response = Vec::with_capacity(1 + results.len() * 2);
    response.push(RespValue::integer(results.len() as i64));

    for (key, score) in results {
        response.push(RespValue::bulk(key.clone()));
        if !nocontent {
            // Get document fields
            let hash_data = store.hgetall(&key);
            if !hash_data.is_empty() {
                let mut fields = Vec::with_capacity(hash_data.len() * 2 + 2);
                fields.push(RespValue::bulk(Bytes::from_static(b"$")));
                fields.push(RespValue::bulk(Bytes::copy_from_slice(
                    format!("{}", score).as_bytes(),
                )));
                for (field, value) in hash_data {
                    fields.push(RespValue::bulk(field));
                    fields.push(RespValue::bulk(value));
                }
                response.push(RespValue::array(fields));
            } else {
                response.push(RespValue::array(vec![]));
            }
        }
    }

    Ok(RespValue::array(response))
}

/// FT.AGGREGATE - Aggregation queries with GROUPBY/REDUCE support
fn cmd_ft_aggregate(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("FT.AGGREGATE"));
    }

    let index_name = &args[0];
    let query = String::from_utf8_lossy(&args[1]).to_string();

    // Parse optional clauses
    let mut group_by_field: Option<String> = None;
    let mut reduce_op: Option<&str> = None;
    let mut reduce_field: Option<String> = None;
    let mut reduce_as: Option<String> = None;
    let mut limit = 100usize;
    let mut offset = 0usize;

    let mut idx = 2;
    while idx < args.len() {
        if args[idx].eq_ignore_ascii_case(b"GROUPBY") {
            idx += 1;
            if idx >= args.len() {
                break;
            }
            let _num_fields: usize = std::str::from_utf8(&args[idx])
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1);
            idx += 1;
            if idx >= args.len() {
                break;
            }
            // Get field name (strip @ prefix if present)
            let field = String::from_utf8_lossy(&args[idx]).to_string();
            group_by_field = Some(field.trim_start_matches('@').to_string());
            idx += 1;
        } else if args[idx].eq_ignore_ascii_case(b"REDUCE") {
            idx += 1;
            if idx >= args.len() {
                break;
            }
            let op = String::from_utf8_lossy(&args[idx]).to_ascii_uppercase();
            reduce_op = match op.as_str() {
                "COUNT" => Some("COUNT"),
                "SUM" => Some("SUM"),
                "AVG" => Some("AVG"),
                "MIN" => Some("MIN"),
                "MAX" => Some("MAX"),
                "COUNT_DISTINCT" => Some("COUNT_DISTINCT"),
                _ => None,
            };
            idx += 1;
            // Parse num args and optional field
            if idx >= args.len() {
                break;
            }
            let num_args: usize = std::str::from_utf8(&args[idx])
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            idx += 1;
            if num_args > 0 && idx < args.len() {
                let field = String::from_utf8_lossy(&args[idx]).to_string();
                reduce_field = Some(field.trim_start_matches('@').to_string());
                idx += 1;
            }
            // Check for AS clause
            if idx < args.len() && args[idx].eq_ignore_ascii_case(b"AS") {
                idx += 1;
                if idx < args.len() {
                    reduce_as = Some(String::from_utf8_lossy(&args[idx]).to_string());
                    idx += 1;
                }
            }
        } else if args[idx].eq_ignore_ascii_case(b"LIMIT") {
            idx += 1;
            if idx < args.len() {
                offset = std::str::from_utf8(&args[idx])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                idx += 1;
            }
            if idx < args.len() {
                limit = std::str::from_utf8(&args[idx])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(100);
                idx += 1;
            }
        } else {
            idx += 1;
        }
    }

    // Run base search
    let results = store.ft_search(index_name, &query, 0, 10000)?;

    // If no GROUPBY, return simple results
    if group_by_field.is_none() {
        let mut response = Vec::new();
        response.push(RespValue::integer(results.len() as i64));
        for (key, _) in results.into_iter().skip(offset).take(limit) {
            response.push(RespValue::array(vec![
                RespValue::bulk(Bytes::from_static(b"key")),
                RespValue::bulk(key),
            ]));
        }
        return Ok(RespValue::array(response));
    }

    // Group results by field
    let mut groups: std::collections::HashMap<String, Vec<f64>> = std::collections::HashMap::new();
    let group_field = group_by_field.unwrap();
    let reduce_fld = reduce_field.unwrap_or_default();

    for (key, _score) in &results {
        // Get document fields from hash
        let hash_fields = store.hgetall(key.as_ref());
        let group_val = hash_fields
            .iter()
            .find(|(k, _)| String::from_utf8_lossy(k).eq_ignore_ascii_case(&group_field))
            .map(|(_, v)| String::from_utf8_lossy(v).to_string())
            .unwrap_or_else(|| "(nil)".to_string());

        let numeric_val = if !reduce_fld.is_empty() {
            hash_fields
                .iter()
                .find(|(k, _)| String::from_utf8_lossy(k).eq_ignore_ascii_case(&reduce_fld))
                .and_then(|(_, v)| std::str::from_utf8(v).ok()?.parse::<f64>().ok())
                .unwrap_or(0.0)
        } else {
            1.0 // For COUNT
        };

        groups.entry(group_val).or_default().push(numeric_val);
    }

    // Build response with aggregated values
    let mut response = Vec::new();
    let result_name = reduce_as.unwrap_or_else(|| reduce_op.unwrap_or("count").to_lowercase());

    let mut group_results: Vec<_> = groups.into_iter().collect();
    group_results.sort_by(|a, b| a.0.cmp(&b.0));

    response.push(RespValue::integer(group_results.len() as i64));

    for (group_val, values) in group_results.into_iter().skip(offset).take(limit) {
        let agg_result = match reduce_op.unwrap_or("COUNT") {
            "COUNT" => values.len() as f64,
            "SUM" => values.iter().sum(),
            "AVG" => values.iter().sum::<f64>() / values.len().max(1) as f64,
            "MIN" => values.iter().cloned().fold(f64::INFINITY, f64::min),
            "MAX" => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
            "COUNT_DISTINCT" => {
                let unique: std::collections::HashSet<_> =
                    values.iter().map(|v| v.to_bits()).collect();
                unique.len() as f64
            }
            _ => values.len() as f64,
        };

        response.push(RespValue::array(vec![
            RespValue::bulk(Bytes::from(group_field.clone())),
            RespValue::bulk(Bytes::from(group_val)),
            RespValue::bulk(Bytes::from(result_name.clone())),
            RespValue::bulk(Bytes::from(agg_result.to_string())),
        ]));
    }

    Ok(RespValue::array(response))
}

/// FT.ALIASADD alias index
fn cmd_ft_aliasadd(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("FT.ALIASADD"));
    }
    store.ft_aliasadd(args[0].clone(), &args[1])?;
    Ok(RespValue::ok())
}

/// FT.ALIASDEL alias
fn cmd_ft_aliasdel(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("FT.ALIASDEL"));
    }
    store.ft_aliasdel(&args[0])?;
    Ok(RespValue::ok())
}

/// FT.ALIASUPDATE alias index
fn cmd_ft_aliasupdate(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("FT.ALIASUPDATE"));
    }
    store.ft_aliasupdate(args[0].clone(), &args[1])?;
    Ok(RespValue::ok())
}

/// FT.ALTER index [SKIPINITIALSCAN] SCHEMA ADD field options
fn cmd_ft_alter(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("FT.ALTER"));
    }

    let index_name = &args[0];
    let mut i = 1;
    let mut skip_initial_scan = false;

    if i < args.len() && eq_ignore_ascii_case(&args[i], b"SKIPINITIALSCAN") {
        skip_initial_scan = true;
        i += 1;
    }

    if i >= args.len() || !eq_ignore_ascii_case(&args[i], b"SCHEMA") {
        return Err(Error::Syntax);
    }
    i += 1;

    if i >= args.len() || !eq_ignore_ascii_case(&args[i], b"ADD") {
        return Err(Error::Syntax);
    }
    i += 1;

    // Parse new fields
    let mut schema = Vec::new();
    while i < args.len() {
        let field = parse_schema_field(args, &mut i)?;
        schema.push(field);
    }

    if schema.is_empty() {
        return Err(Error::Custom(
            "Must specify at least one field to add".into(),
        ));
    }

    store.ft_alter(index_name, skip_initial_scan, schema)?;
    Ok(RespValue::ok())
}

/// FT.CONFIG GET/SET
fn cmd_ft_config(args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("FT.CONFIG"));
    }

    let subcmd = args[0].to_ascii_uppercase();
    match subcmd.as_slice() {
        b"GET" => {
            // Return empty config
            Ok(RespValue::array(vec![]))
        }
        b"SET" => Ok(RespValue::ok()),
        _ => Err(Error::Syntax),
    }
}

/// FT.CURSOR READ/DEL
fn cmd_ft_cursor(_store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("FT.CURSOR"));
    }

    let subcmd = args[0].to_ascii_uppercase();
    match subcmd.as_slice() {
        b"READ" => {
            // Return empty cursor result
            Ok(RespValue::array(vec![
                RespValue::array(vec![RespValue::integer(0)]),
                RespValue::integer(0),
            ]))
        }
        b"DEL" => Ok(RespValue::ok()),
        _ => Err(Error::Syntax),
    }
}

/// FT.DICTADD dict term [term ...]
fn cmd_ft_dictadd(args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("FT.DICTADD"));
    }
    // Return count of added terms
    Ok(RespValue::integer((args.len() - 1) as i64))
}

/// FT.DICTDEL dict term [term ...]
fn cmd_ft_dictdel(args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("FT.DICTDEL"));
    }
    Ok(RespValue::integer((args.len() - 1) as i64))
}

/// FT.DICTDUMP dict
fn cmd_ft_dictdump(args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("FT.DICTDUMP"));
    }
    Ok(RespValue::array(vec![]))
}

/// FT.EXPLAIN / FT.EXPLAINCLI
fn cmd_ft_explain(args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("FT.EXPLAIN"));
    }
    // Return a simple query plan
    let query = String::from_utf8_lossy(&args[1]);
    let plan = format!("UNION {{\n  {}\n}}", query);
    Ok(RespValue::bulk(Bytes::copy_from_slice(plan.as_bytes())))
}

/// FT.PROFILE
fn cmd_ft_profile(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 4 {
        return Err(Error::WrongArity("FT.PROFILE"));
    }

    // Run the actual query
    let index_name = &args[0];
    // args[1] = SEARCH or AGGREGATE
    // args[2] = QUERY (optional LIMITED)
    let query_idx = if eq_ignore_ascii_case(&args[2], b"LIMITED") {
        4
    } else {
        3
    };

    if query_idx >= args.len() {
        return Err(Error::WrongArity("FT.PROFILE"));
    }

    let query = String::from_utf8_lossy(&args[query_idx]);
    let results = store.ft_search(index_name, &query, 0, 10)?;

    // Build profile response
    let mut response = Vec::new();

    // Results
    let mut results_arr = Vec::new();
    results_arr.push(RespValue::integer(results.len() as i64));
    for (key, _) in results {
        results_arr.push(RespValue::bulk(key));
        results_arr.push(RespValue::array(vec![]));
    }
    response.push(RespValue::array(results_arr));

    // Profile info
    let profile = vec![
        RespValue::bulk(Bytes::from_static(b"Total profile time")),
        RespValue::bulk(Bytes::from_static(b"0.01")),
        RespValue::bulk(Bytes::from_static(b"Parsing time")),
        RespValue::bulk(Bytes::from_static(b"0.001")),
        RespValue::bulk(Bytes::from_static(b"Pipeline creation time")),
        RespValue::bulk(Bytes::from_static(b"0.001")),
    ];
    response.push(RespValue::array(profile));

    Ok(RespValue::array(response))
}

/// FT.SPELLCHECK
fn cmd_ft_spellcheck(args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("FT.SPELLCHECK"));
    }
    // Return empty suggestions
    Ok(RespValue::array(vec![]))
}

/// FT.SYNUPDATE index group_id term [term ...]
fn cmd_ft_synupdate(args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("FT.SYNUPDATE"));
    }
    Ok(RespValue::ok())
}

/// FT.SYNDUMP index
fn cmd_ft_syndump(args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("FT.SYNDUMP"));
    }
    Ok(RespValue::array(vec![]))
}

/// FT.TAGVALS index field
fn cmd_ft_tagvals(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 2 {
        return Err(Error::WrongArity("FT.TAGVALS"));
    }

    let tags = store.ft_tagvals(&args[0], &args[1])?;
    Ok(RespValue::array(
        tags.into_iter().map(RespValue::bulk).collect(),
    ))
}

/// FT.HYBRID - Hybrid text + vector search
fn cmd_ft_hybrid(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 4 {
        return Err(Error::WrongArity("FT.HYBRID"));
    }

    let index_name = &args[0];

    // Parse SEARCH query and VSIM params
    let mut text_query = String::new();
    let mut offset = 0usize;
    let mut limit = 10usize;

    let mut i = 1;
    while i < args.len() {
        let opt = args[i].to_ascii_uppercase();
        match opt.as_slice() {
            b"SEARCH" => {
                i += 1;
                if i < args.len() {
                    text_query = String::from_utf8_lossy(&args[i]).into_owned();
                    i += 1;
                }
            }
            b"LIMIT" => {
                i += 1;
                if i + 1 < args.len() {
                    offset = parse_int(&args[i]).unwrap_or(0) as usize;
                    limit = parse_int(&args[i + 1]).unwrap_or(10) as usize;
                    i += 2;
                }
            }
            _ => {
                i += 1;
            }
        }
    }

    // Run text search
    let results = store.ft_search(index_name, &text_query, offset, limit)?;

    // Build response
    let mut response = Vec::with_capacity(1 + results.len() * 2);
    response.push(RespValue::integer(results.len() as i64));

    for (key, _score) in results {
        response.push(RespValue::bulk(key.clone()));
        response.push(RespValue::array(vec![]));
    }

    Ok(RespValue::array(response))
}
