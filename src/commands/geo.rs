//! Geospatial command handlers
//!
//! Commands: GEOADD, GEODIST, GEOHASH, GEOPOS, GEORADIUS, GEORADIUSBYMEMBER,
//! GEORADIUS_RO, GEORADIUSBYMEMBER_RO, GEOSEARCH, GEOSEARCHSTORE

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::storage::Store;
use crate::storage::ops::geo_ops::{GeoResult, geohash_encode_52bit};

/// Case-insensitive comparison
#[inline]
fn eq_ignore_case(a: &[u8], b: &[u8]) -> bool {
    a.eq_ignore_ascii_case(b)
}

/// Parse float from bytes
#[inline]
fn parse_float(b: &[u8]) -> Result<f64> {
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

/// Execute GEO commands
pub fn execute(store: &Store, cmd: &[u8], args: &[Bytes]) -> Result<RespValue> {
    match cmd.to_ascii_uppercase().as_slice() {
        b"GEOADD" => cmd_geoadd(store, args),
        b"GEODIST" => cmd_geodist(store, args),
        b"GEOHASH" => cmd_geohash(store, args),
        b"GEOPOS" => cmd_geopos(store, args),
        b"GEORADIUS" => cmd_georadius(store, args, false),
        b"GEORADIUS_RO" => cmd_georadius(store, args, true),
        b"GEORADIUSBYMEMBER" => cmd_georadiusbymember(store, args, false),
        b"GEORADIUSBYMEMBER_RO" => cmd_georadiusbymember(store, args, true),
        b"GEOSEARCH" => cmd_geosearch(store, args),
        b"GEOSEARCHSTORE" => cmd_geosearchstore(store, args),
        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(cmd).into_owned(),
        )),
    }
}

/// GEOADD key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]
fn cmd_geoadd(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 4 {
        return Err(Error::WrongArity("GEOADD"));
    }

    let key = args[0].clone();
    let mut nx = false;
    let mut xx = false;
    let mut ch = false;
    let mut i = 1;

    // Parse options - they must all come before coordinates
    while i < args.len() {
        let arg_upper = args[i].to_ascii_uppercase();
        if arg_upper == b"NX" {
            nx = true;
            i += 1;
        } else if arg_upper == b"XX" {
            xx = true;
            i += 1;
        } else if arg_upper == b"CH" {
            ch = true;
            i += 1;
        } else {
            // Not an option - must be a coordinate
            // Try to parse as float to verify it's a coordinate
            if parse_float(&args[i]).is_err() {
                // Not a valid float - it's an invalid option
                return Err(Error::Syntax);
            }
            break;
        }
    }

    // Check NX+XX conflict - Redis returns syntax error
    if nx && xx {
        return Err(Error::Syntax);
    }

    // Check we have enough args for lon/lat/member triplets
    let remaining = args.len() - i;
    if remaining < 3 || remaining % 3 != 0 {
        return Err(Error::WrongArity("GEOADD"));
    }

    // Parse members
    let mut members = Vec::new();
    while i + 2 < args.len() {
        let lon = parse_float(&args[i])?;
        let lat = parse_float(&args[i + 1])?;
        let member = args[i + 2].clone();

        // Validate coordinates
        if !(-180.0..=180.0).contains(&lon) {
            return Err(Error::Custom(format!(
                "ERR invalid longitude,latitude pair {:.6},{:.6}",
                lon, lat
            )));
        }
        if !(-85.05112878..=85.05112878).contains(&lat) {
            return Err(Error::Custom(format!(
                "ERR invalid longitude,latitude pair {:.6},{:.6}",
                lon, lat
            )));
        }

        members.push((lon, lat, member));
        i += 3;
    }

    let count = store.geo_add(key, members, nx, xx, ch)?;
    Ok(RespValue::integer(count as i64))
}

/// GEODIST key member1 member2 [M|KM|FT|MI]
fn cmd_geodist(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 3 {
        return Err(Error::WrongArity("GEODIST"));
    }

    let unit = if args.len() > 3 {
        args[3].as_ref()
    } else {
        b"m"
    };

    match store.geo_dist(&args[0], &args[1], &args[2], unit)? {
        Some(dist) => Ok(RespValue::bulk_string(&format!("{:.4}", dist))),
        None => Ok(RespValue::Null),
    }
}

/// GEOHASH key member [member ...]
fn cmd_geohash(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("GEOHASH"));
    }

    if args.len() == 1 {
        // Just key, no members
        return Ok(RespValue::array(vec![]));
    }

    let members: Vec<Bytes> = args[1..].to_vec();
    let hashes = store.geo_hash(&args[0], &members)?;

    Ok(RespValue::array(
        hashes
            .into_iter()
            .map(|h| match h {
                Some(s) => RespValue::bulk_string(&s),
                None => RespValue::Null,
            })
            .collect(),
    ))
}

/// GEOPOS key member [member ...]
fn cmd_geopos(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("GEOPOS"));
    }

    if args.len() == 1 {
        // Just key, no members
        return Ok(RespValue::array(vec![]));
    }

    let members: Vec<Bytes> = args[1..].to_vec();
    let positions = store.geo_pos(&args[0], &members)?;

    Ok(RespValue::array(
        positions
            .into_iter()
            .map(|pos| match pos {
                Some((lon, lat)) => RespValue::array(vec![
                    RespValue::bulk_string(&format!("{:.17}", lon)),
                    RespValue::bulk_string(&format!("{:.17}", lat)),
                ]),
                None => RespValue::Null,
            })
            .collect(),
    ))
}

/// Parsed GEORADIUS options
#[derive(Default)]
struct RadiusOptions {
    with_coord: bool,
    with_dist: bool,
    with_hash: bool,
    count: Option<usize>,
    ascending: Option<bool>,
    any: bool,
    store_key: Option<Bytes>,
    store_dist: bool,
}

/// Helper to parse GEORADIUS options
fn parse_radius_options(args: &[Bytes], start: usize, allow_store: bool) -> Result<RadiusOptions> {
    let mut opts = RadiusOptions::default();
    let mut i = start;

    while i < args.len() {
        let arg = args[i].to_ascii_uppercase();
        match arg.as_slice() {
            b"WITHCOORD" => {
                opts.with_coord = true;
                i += 1;
            }
            b"WITHDIST" => {
                opts.with_dist = true;
                i += 1;
            }
            b"WITHHASH" => {
                opts.with_hash = true;
                i += 1;
            }
            b"COUNT" => {
                if i + 1 >= args.len() {
                    return Err(Error::Syntax);
                }
                i += 1;
                let c = parse_int(&args[i])? as usize;
                opts.count = Some(c);
                i += 1;
            }
            b"ASC" => {
                opts.ascending = Some(true);
                i += 1;
            }
            b"DESC" => {
                opts.ascending = Some(false);
                i += 1;
            }
            b"ANY" => {
                opts.any = true;
                i += 1;
            }
            b"STORE" => {
                if !allow_store {
                    return Err(Error::Syntax);
                }
                if i + 1 >= args.len() {
                    return Err(Error::Syntax);
                }
                i += 1;
                opts.store_key = Some(args[i].clone());
                i += 1;
            }
            b"STOREDIST" => {
                if !allow_store {
                    return Err(Error::Syntax);
                }
                if i + 1 >= args.len() {
                    return Err(Error::Syntax);
                }
                i += 1;
                opts.store_key = Some(args[i].clone());
                opts.store_dist = true;
                i += 1;
            }
            _ => {
                return Err(Error::Syntax);
            }
        }
    }

    // ANY requires COUNT
    if opts.any && opts.count.is_none() {
        return Err(Error::Custom(
            "ERR the ANY option requires the COUNT option".to_string(),
        ));
    }

    // STORE is incompatible with WITH* options
    if opts.store_key.is_some() && (opts.with_coord || opts.with_dist || opts.with_hash) {
        return Err(Error::Custom(
            "ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options".to_string()
        ));
    }

    Ok(opts)
}

/// Format GeoResult to RespValue
fn format_geo_result(
    result: GeoResult,
    with_coord: bool,
    with_dist: bool,
    with_hash: bool,
) -> RespValue {
    if !with_coord && !with_dist && !with_hash {
        return RespValue::bulk(result.member);
    }

    let mut arr = vec![RespValue::bulk(result.member)];

    if with_dist {
        if let Some(d) = result.distance {
            arr.push(RespValue::bulk_string(&format!("{:.4}", d)));
        }
    }

    if with_hash {
        if let Some(h) = result.hash {
            arr.push(RespValue::integer(h as i64));
        }
    }

    if with_coord {
        if let (Some(lon), Some(lat)) = (result.longitude, result.latitude) {
            arr.push(RespValue::array(vec![
                RespValue::bulk_string(&format!("{:.17}", lon)),
                RespValue::bulk_string(&format!("{:.17}", lat)),
            ]));
        }
    }

    RespValue::array(arr)
}

/// GEORADIUS key longitude latitude radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC] [STORE key] [STOREDIST key]
fn cmd_georadius(store: &Store, args: &[Bytes], read_only: bool) -> Result<RespValue> {
    if args.len() < 5 {
        return Err(Error::WrongArity("GEORADIUS"));
    }

    let lon = parse_float(&args[1])?;
    let lat = parse_float(&args[2])?;
    let radius = parse_float(&args[3])?;
    let unit = &args[4];

    let opts = parse_radius_options(args, 5, !read_only)?;

    // Redis rule: if COUNT is specified without ANY, and no sort order given, default to ASC
    let ascending = if opts.count.is_some() && !opts.any && opts.ascending.is_none() {
        Some(true) // Implicit ASC sort
    } else {
        opts.ascending
    };

    // For STORE, we need hash and coord; for STOREDIST we need dist
    let need_hash = opts.store_key.is_some() && !opts.store_dist;
    let need_coord = opts.store_key.is_some() && !opts.store_dist;

    // Execute search
    let results = store.geo_radius(
        &args[0],
        lon,
        lat,
        radius,
        unit,
        opts.with_coord || need_coord,
        opts.with_dist || ascending.is_some() || opts.store_dist,
        opts.with_hash || need_hash,
        opts.count,
        ascending,
        opts.any,
    )?;

    // Handle STORE/STOREDIST
    if let Some(dest) = opts.store_key {
        let count = results.len();
        let entries: Vec<(f64, Bytes)> = results
            .into_iter()
            .map(|r| {
                let score = if opts.store_dist {
                    r.distance.unwrap_or(0.0)
                } else {
                    r.hash.unwrap_or_else(|| {
                        // Re-encode if hash not available
                        if let (Some(lon), Some(lat)) = (r.longitude, r.latitude) {
                            geohash_encode_52bit(lon, lat)
                        } else {
                            0.0
                        }
                    })
                };
                (score, r.member)
            })
            .collect();

        if !entries.is_empty() {
            store.del(&dest);
            let _ = store.zadd(dest, entries);
        }
        return Ok(RespValue::integer(count as i64));
    }

    Ok(RespValue::array(
        results
            .into_iter()
            .map(|r| format_geo_result(r, opts.with_coord, opts.with_dist, opts.with_hash))
            .collect(),
    ))
}

/// GEORADIUSBYMEMBER key member radius M|KM|FT|MI [options...]
fn cmd_georadiusbymember(store: &Store, args: &[Bytes], read_only: bool) -> Result<RespValue> {
    if args.len() < 4 {
        return Err(Error::WrongArity("GEORADIUSBYMEMBER"));
    }

    let radius = parse_float(&args[2])?;
    let unit = &args[3];

    let opts = parse_radius_options(args, 4, !read_only)?;

    // Redis rule: if COUNT is specified without ANY, and no sort order given, default to ASC
    let ascending = if opts.count.is_some() && !opts.any && opts.ascending.is_none() {
        Some(true) // Implicit ASC sort
    } else {
        opts.ascending
    };

    // For STORE, we need hash and coord; for STOREDIST we need dist
    let need_hash = opts.store_key.is_some() && !opts.store_dist;
    let need_coord = opts.store_key.is_some() && !opts.store_dist;

    let results = store.geo_radius_by_member(
        &args[0],
        &args[1],
        radius,
        unit,
        opts.with_coord || need_coord,
        opts.with_dist || ascending.is_some() || opts.store_dist,
        opts.with_hash || need_hash,
        opts.count,
        ascending,
        opts.any,
    )?;

    // Handle STORE/STOREDIST
    if let Some(dest) = opts.store_key {
        let count = results.len();
        let entries: Vec<(f64, Bytes)> = results
            .into_iter()
            .map(|r| {
                let score = if opts.store_dist {
                    r.distance.unwrap_or(0.0)
                } else {
                    r.hash.unwrap_or_else(|| {
                        geohash_encode_52bit(r.longitude.unwrap_or(0.0), r.latitude.unwrap_or(0.0))
                    })
                };
                (score, r.member)
            })
            .collect();

        if !entries.is_empty() {
            store.del(&dest);
            let _ = store.zadd(dest, entries);
        }
        return Ok(RespValue::integer(count as i64));
    }

    Ok(RespValue::array(
        results
            .into_iter()
            .map(|r| format_geo_result(r, opts.with_coord, opts.with_dist, opts.with_hash))
            .collect(),
    ))
}

/// GEOSEARCH key <FROMMEMBER member | FROMLONLAT lon lat> <BYRADIUS radius unit | BYBOX w h unit> [ASC|DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]
fn cmd_geosearch(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 4 {
        return Err(Error::WrongArity("GEOSEARCH"));
    }

    let key = &args[0];
    let mut i = 1;

    // Parse FROM clause
    let mut from_member: Option<Bytes> = None;
    let mut from_lonlat: Option<(f64, f64)> = None;

    while i < args.len() {
        if eq_ignore_case(&args[i], b"FROMMEMBER") {
            if from_lonlat.is_some() {
                return Err(Error::Syntax);
            }
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            from_member = Some(args[i].clone());
            i += 1;
        } else if eq_ignore_case(&args[i], b"FROMLONLAT") {
            if from_member.is_some() {
                return Err(Error::Syntax);
            }
            i += 1;
            if i + 1 >= args.len() {
                return Err(Error::Syntax);
            }
            let lo = parse_float(&args[i])?;
            let la = parse_float(&args[i + 1])?;
            from_lonlat = Some((lo, la));
            i += 2;
        } else {
            break;
        }
    }

    // Exactly one FROM clause required
    if from_member.is_none() && from_lonlat.is_none() {
        return Err(Error::Custom(
            "ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH"
                .to_string(),
        ));
    }

    // Get center coordinates
    let (lon, lat) = if let Some(member) = &from_member {
        // Check if key exists first (zcard returns 0 for non-existing key)
        // geo_pos returns all None if key doesn't exist or has wrong type
        let positions = store.geo_pos(key, &[member.clone()])?;
        match positions.into_iter().next() {
            Some(Some((lo, la))) => (lo, la),
            Some(None) => {
                // Member not found - check if key exists at all
                if store.zcard(key) == 0 {
                    // Key doesn't exist - return empty result
                    return Ok(RespValue::array(vec![]));
                }
                // Key exists but member not found
                return Err(Error::Custom(
                    "ERR could not decode requested zset member".to_string(),
                ));
            }
            None => {
                // Empty response from geo_pos - shouldn't happen
                return Ok(RespValue::array(vec![]));
            }
        }
    } else if let Some((lo, la)) = from_lonlat {
        (lo, la)
    } else {
        unreachable!()
    };

    // Parse BY clause
    let mut by_radius: Option<(f64, Bytes)> = None;
    let mut by_box: Option<(f64, f64, Bytes)> = None;

    while i < args.len() {
        if eq_ignore_case(&args[i], b"BYRADIUS") {
            if by_box.is_some() {
                return Err(Error::Syntax);
            }
            i += 1;
            if i + 1 >= args.len() {
                return Err(Error::Syntax);
            }
            let radius = parse_float(&args[i])?;
            let unit = args[i + 1].clone();
            by_radius = Some((radius, unit));
            i += 2;
        } else if eq_ignore_case(&args[i], b"BYBOX") {
            if by_radius.is_some() {
                return Err(Error::Syntax);
            }
            i += 1;
            if i + 2 >= args.len() {
                return Err(Error::Syntax);
            }
            let width = parse_float(&args[i])?;
            let height = parse_float(&args[i + 1])?;
            let unit = args[i + 2].clone();
            by_box = Some((width, height, unit));
            i += 3;
        } else {
            break;
        }
    }

    // Exactly one BY clause required
    if by_radius.is_none() && by_box.is_none() {
        return Err(Error::Custom(
            "ERR exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCH".to_string(),
        ));
    }

    // Parse remaining options
    let mut with_coord = false;
    let mut with_dist = false;
    let mut with_hash = false;
    let mut count: Option<usize> = None;
    let mut ascending: Option<bool> = None;
    let mut any = false;

    while i < args.len() {
        let arg = args[i].to_ascii_uppercase();
        match arg.as_slice() {
            b"WITHCOORD" => {
                with_coord = true;
                i += 1;
            }
            b"WITHDIST" => {
                with_dist = true;
                i += 1;
            }
            b"WITHHASH" => {
                with_hash = true;
                i += 1;
            }
            b"COUNT" => {
                if i + 1 >= args.len() {
                    return Err(Error::Syntax);
                }
                i += 1;
                count = Some(parse_int(&args[i])? as usize);
                i += 1;
            }
            b"ASC" => {
                ascending = Some(true);
                i += 1;
            }
            b"DESC" => {
                ascending = Some(false);
                i += 1;
            }
            b"ANY" => {
                any = true;
                i += 1;
            }
            b"STOREDIST" => {
                // STOREDIST is not valid in GEOSEARCH, only in GEOSEARCHSTORE
                return Err(Error::Syntax);
            }
            _ => {
                return Err(Error::Syntax);
            }
        }
    }

    // ANY requires COUNT
    if any && count.is_none() {
        return Err(Error::Custom(
            "ERR the ANY option requires the COUNT option".to_string(),
        ));
    }

    // Redis rule: if COUNT is specified without ANY, and no sort order given, default to ASC
    let ascending = if count.is_some() && !any && ascending.is_none() {
        Some(true) // Implicit ASC sort
    } else {
        ascending
    };

    // Execute search
    let results = if let Some((radius, unit)) = by_radius {
        store.geo_radius(
            key,
            lon,
            lat,
            radius,
            &unit,
            with_coord,
            with_dist || ascending.is_some(),
            with_hash,
            count,
            ascending,
            any,
        )?
    } else if let Some((width, height, unit)) = by_box {
        store.geo_search_box(
            key,
            lon,
            lat,
            width,
            height,
            &unit,
            with_coord,
            with_dist || ascending.is_some(),
            with_hash,
            count,
            ascending,
            any,
        )?
    } else {
        unreachable!()
    };

    Ok(RespValue::array(
        results
            .into_iter()
            .map(|r| format_geo_result(r, with_coord, with_dist, with_hash))
            .collect(),
    ))
}

/// GEOSEARCHSTORE destination source <FROMMEMBER member | FROMLONLAT lon lat> <BYRADIUS radius unit | BYBOX w h unit> [ASC|DESC] [COUNT count [ANY]] [STOREDIST]
fn cmd_geosearchstore(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 5 {
        return Err(Error::WrongArity("GEOSEARCHSTORE"));
    }

    let dest = args[0].clone();
    let source = &args[1];
    let mut i = 2;
    let mut store_dist = false;

    // Parse FROM clause
    let mut from_member: Option<Bytes> = None;
    let mut from_lonlat: Option<(f64, f64)> = None;

    while i < args.len() {
        if eq_ignore_case(&args[i], b"FROMMEMBER") {
            if from_lonlat.is_some() {
                return Err(Error::Syntax);
            }
            i += 1;
            if i >= args.len() {
                return Err(Error::Syntax);
            }
            from_member = Some(args[i].clone());
            i += 1;
        } else if eq_ignore_case(&args[i], b"FROMLONLAT") {
            if from_member.is_some() {
                return Err(Error::Syntax);
            }
            i += 1;
            if i + 1 >= args.len() {
                return Err(Error::Syntax);
            }
            let lo = parse_float(&args[i])?;
            let la = parse_float(&args[i + 1])?;
            from_lonlat = Some((lo, la));
            i += 2;
        } else {
            break;
        }
    }

    // Exactly one FROM clause required
    if from_member.is_none() && from_lonlat.is_none() {
        return Err(Error::Custom(
            "ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCHSTORE"
                .to_string(),
        ));
    }

    // Get center coordinates
    let (lon, lat) = if let Some(member) = &from_member {
        // geo_pos returns all None if key doesn't exist or has wrong type
        let positions = store.geo_pos(source, &[member.clone()])?;
        match positions.into_iter().next() {
            Some(Some((lo, la))) => (lo, la),
            Some(None) => {
                // Member not found - check if key exists at all
                if store.zcard(source) == 0 {
                    // Source key doesn't exist - return 0
                    return Ok(RespValue::integer(0));
                }
                // Key exists but member not found
                return Err(Error::Custom(
                    "ERR could not decode requested zset member".to_string(),
                ));
            }
            None => {
                // Empty response - shouldn't happen
                return Ok(RespValue::integer(0));
            }
        }
    } else if let Some((lo, la)) = from_lonlat {
        (lo, la)
    } else {
        unreachable!()
    };

    // Parse BY clause
    let mut by_radius: Option<(f64, Bytes)> = None;
    let mut by_box: Option<(f64, f64, Bytes)> = None;

    while i < args.len() {
        if eq_ignore_case(&args[i], b"BYRADIUS") {
            if by_box.is_some() {
                return Err(Error::Syntax);
            }
            i += 1;
            if i + 1 >= args.len() {
                return Err(Error::Syntax);
            }
            let radius = parse_float(&args[i])?;
            let unit = args[i + 1].clone();
            by_radius = Some((radius, unit));
            i += 2;
        } else if eq_ignore_case(&args[i], b"BYBOX") {
            if by_radius.is_some() {
                return Err(Error::Syntax);
            }
            i += 1;
            if i + 2 >= args.len() {
                return Err(Error::Syntax);
            }
            let width = parse_float(&args[i])?;
            let height = parse_float(&args[i + 1])?;
            let unit = args[i + 2].clone();
            by_box = Some((width, height, unit));
            i += 3;
        } else {
            break;
        }
    }

    // Exactly one BY clause required
    if by_radius.is_none() && by_box.is_none() {
        return Err(Error::Custom(
            "ERR exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCHSTORE".to_string(),
        ));
    }

    // Parse remaining options
    let mut count: Option<usize> = None;
    let mut ascending: Option<bool> = None;
    let mut any = false;

    while i < args.len() {
        let arg = args[i].to_ascii_uppercase();
        match arg.as_slice() {
            b"COUNT" => {
                if i + 1 >= args.len() {
                    return Err(Error::Syntax);
                }
                i += 1;
                count = Some(parse_int(&args[i])? as usize);
                i += 1;
            }
            b"ASC" => {
                ascending = Some(true);
                i += 1;
            }
            b"DESC" => {
                ascending = Some(false);
                i += 1;
            }
            b"ANY" => {
                any = true;
                i += 1;
            }
            b"STOREDIST" => {
                store_dist = true;
                i += 1;
            }
            b"STORE" => {
                // STORE is not valid in GEOSEARCHSTORE (destination already specified)
                return Err(Error::Syntax);
            }
            b"WITHCOORD" | b"WITHDIST" | b"WITHHASH" => {
                // WITH* options not allowed in GEOSEARCHSTORE
                return Err(Error::Syntax);
            }
            _ => {
                return Err(Error::Syntax);
            }
        }
    }

    // ANY requires COUNT
    if any && count.is_none() {
        return Err(Error::Custom(
            "ERR the ANY option requires the COUNT option".to_string(),
        ));
    }

    // Execute search (always need dist for STOREDIST, always need hash for STORE)
    let results = if let Some((radius, unit)) = by_radius {
        store.geo_radius(
            source, lon, lat, radius, &unit, true, // with_coord for hash recalculation
            true, // with_dist
            true, // with_hash
            count, ascending, any,
        )?
    } else if let Some((width, height, unit)) = by_box {
        store.geo_search_box(
            source, lon, lat, width, height, &unit, true, true, true, count, ascending, any,
        )?
    } else {
        unreachable!()
    };

    // Store results in destination sorted set
    let result_count = results.len();

    if result_count == 0 {
        // Delete destination if it exists and result is empty
        store.del(&dest);
        return Ok(RespValue::integer(0));
    }

    let entries: Vec<(f64, Bytes)> = results
        .into_iter()
        .map(|r| {
            let score = if store_dist {
                r.distance.unwrap_or(0.0)
            } else {
                r.hash.unwrap_or_else(|| {
                    if let (Some(lo), Some(la)) = (r.longitude, r.latitude) {
                        geohash_encode_52bit(lo, la)
                    } else {
                        0.0
                    }
                })
            };
            (score, r.member)
        })
        .collect();

    // Delete destination first, then add new entries
    store.del(&dest);
    let _ = store.zadd(dest, entries);

    Ok(RespValue::integer(result_count as i64))
}
