//! Geospatial command handlers
//!
//! Commands: GEOADD, GEODIST, GEOHASH, GEOPOS, GEORADIUS, GEORADIUSBYMEMBER,
//! GEORADIUS_RO, GEORADIUSBYMEMBER_RO, GEOSEARCH, GEOSEARCHSTORE

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::storage::Store;
use crate::storage::ops::geo_ops::GeoResult;

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

    // Parse options
    while i < args.len() {
        if eq_ignore_case(&args[i], b"NX") {
            nx = true;
            i += 1;
        } else if eq_ignore_case(&args[i], b"XX") {
            xx = true;
            i += 1;
        } else if eq_ignore_case(&args[i], b"CH") {
            ch = true;
            i += 1;
        } else {
            break;
        }
    }

    // Check we have enough args for lon/lat/member triplets
    if !(args.len() - i).is_multiple_of(3) || args.len() - i < 3 {
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
            return Err(Error::Other("invalid longitude"));
        }
        if !(-85.05112878..=85.05112878).contains(&lat) {
            return Err(Error::Other("invalid latitude"));
        }

        members.push((lon, lat, member));
        i += 3;
    }

    eprintln!(
        "[DEBUG] GEOADD: key={:?}, members.len()={}, nx={}, xx={}, ch={}",
        String::from_utf8_lossy(&key),
        members.len(),
        nx,
        xx,
        ch
    );
    let count = store.geo_add(key, members, nx, xx, ch)?;
    eprintln!("[DEBUG] GEOADD: returned count={}", count);
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

    match store.geo_dist(&args[0], &args[1], &args[2], unit) {
        Some(dist) => Ok(RespValue::bulk_string(&format!("{:.4}", dist))),
        None => Ok(RespValue::Null),
    }
}

/// GEOHASH key member [member ...]
fn cmd_geohash(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.is_empty() {
        return Err(Error::WrongArity("GEOHASH"));
    }

    let members: Vec<Bytes> = args[1..].to_vec();
    let hashes = store.geo_hash(&args[0], &members);

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

    let members: Vec<Bytes> = args[1..].to_vec();
    let positions = store.geo_pos(&args[0], &members);

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

/// Helper to parse GEORADIUS options
fn parse_radius_options(
    args: &[Bytes],
    start: usize,
) -> (bool, bool, bool, Option<usize>, Option<bool>) {
    let mut with_coord = false;
    let mut with_dist = false;
    let mut with_hash = false;
    let mut count = None;
    let mut ascending = None;

    let mut i = start;
    while i < args.len() {
        if eq_ignore_case(&args[i], b"WITHCOORD") {
            with_coord = true;
        } else if eq_ignore_case(&args[i], b"WITHDIST") {
            with_dist = true;
        } else if eq_ignore_case(&args[i], b"WITHHASH") {
            with_hash = true;
        } else if eq_ignore_case(&args[i], b"COUNT") && i + 1 < args.len() {
            i += 1;
            if let Ok(c) = parse_int(&args[i]) {
                count = Some(c as usize);
            }
        } else if eq_ignore_case(&args[i], b"ASC") {
            ascending = Some(true);
        } else if eq_ignore_case(&args[i], b"DESC") {
            ascending = Some(false);
        }
        i += 1;
    }

    (with_coord, with_dist, with_hash, count, ascending)
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

    if with_dist
        && let Some(d) = result.distance {
            arr.push(RespValue::bulk_string(&format!("{:.4}", d)));
        }

    if with_hash
        && let Some(h) = result.hash {
            arr.push(RespValue::integer(h as i64));
        }

    if with_coord
        && let (Some(lon), Some(lat)) = (result.longitude, result.latitude) {
            arr.push(RespValue::array(vec![
                RespValue::bulk_string(&format!("{:.17}", lon)),
                RespValue::bulk_string(&format!("{:.17}", lat)),
            ]));
        }

    RespValue::array(arr)
}

/// GEORADIUS key longitude latitude radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC]
fn cmd_georadius(store: &Store, args: &[Bytes], _read_only: bool) -> Result<RespValue> {
    if args.len() < 5 {
        return Err(Error::WrongArity("GEORADIUS"));
    }

    let lon = parse_float(&args[1])?;
    let lat = parse_float(&args[2])?;
    let radius = parse_float(&args[3])?;
    let unit = &args[4];

    let (with_coord, with_dist, with_hash, count, ascending) = parse_radius_options(args, 5);

    let results = store.geo_radius(
        &args[0],
        lon,
        lat,
        radius,
        unit,
        with_coord,
        with_dist || ascending.is_some(), // Need dist for sorting
        with_hash,
        count,
        ascending,
    );

    Ok(RespValue::array(
        results
            .into_iter()
            .map(|r| format_geo_result(r, with_coord, with_dist, with_hash))
            .collect(),
    ))
}

/// GEORADIUSBYMEMBER key member radius M|KM|FT|MI [options...]
fn cmd_georadiusbymember(store: &Store, args: &[Bytes], _read_only: bool) -> Result<RespValue> {
    if args.len() < 4 {
        return Err(Error::WrongArity("GEORADIUSBYMEMBER"));
    }

    let radius = parse_float(&args[2])?;
    let unit = &args[3];

    let (with_coord, with_dist, with_hash, count, ascending) = parse_radius_options(args, 4);

    match store.geo_radius_by_member(
        &args[0],
        &args[1],
        radius,
        unit,
        with_coord,
        with_dist || ascending.is_some(),
        with_hash,
        count,
        ascending,
    ) {
        Some(results) => Ok(RespValue::array(
            results
                .into_iter()
                .map(|r| format_geo_result(r, with_coord, with_dist, with_hash))
                .collect(),
        )),
        None => Ok(RespValue::Null),
    }
}

/// GEOSEARCH key <FROMMEMBER member | FROMLONLAT lon lat> <BYRADIUS radius unit | BYBOX w h unit> [options...]
fn cmd_geosearch(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 4 {
        return Err(Error::WrongArity("GEOSEARCH"));
    }

    let key = &args[0];
    let mut i = 1;

    // Parse FROM
    let (lon, lat) = if eq_ignore_case(&args[i], b"FROMMEMBER") {
        i += 1;
        if i >= args.len() {
            return Err(Error::Syntax);
        }
        // Get member position
        match store.geo_pos(key, &[args[i].clone()]).into_iter().next() {
            Some(Some((lo, la))) => {
                i += 1;
                (lo, la)
            }
            _ => return Err(Error::Other("member not found")),
        }
    } else if eq_ignore_case(&args[i], b"FROMLONLAT") {
        i += 1;
        if i + 1 >= args.len() {
            return Err(Error::Syntax);
        }
        let lo = parse_float(&args[i])?;
        let la = parse_float(&args[i + 1])?;
        i += 2;
        (lo, la)
    } else {
        return Err(Error::Syntax);
    };

    // Parse BY
    if i >= args.len() {
        return Err(Error::Syntax);
    }

    let (with_coord, with_dist, with_hash, count, ascending) = parse_radius_options(args, i);

    let results = if eq_ignore_case(&args[i], b"BYRADIUS") {
        i += 1;
        if i + 1 >= args.len() {
            return Err(Error::Syntax);
        }
        let radius = parse_float(&args[i])?;
        let unit = &args[i + 1];
        store.geo_radius(
            key,
            lon,
            lat,
            radius,
            unit,
            with_coord,
            with_dist || ascending.is_some(),
            with_hash,
            count,
            ascending,
        )
    } else if eq_ignore_case(&args[i], b"BYBOX") {
        i += 1;
        if i + 2 >= args.len() {
            return Err(Error::Syntax);
        }
        let width = parse_float(&args[i])?;
        let height = parse_float(&args[i + 1])?;
        let unit = &args[i + 2];
        store.geo_search_box(
            key,
            lon,
            lat,
            width,
            height,
            unit,
            with_coord,
            with_dist || ascending.is_some(),
            with_hash,
            count,
            ascending,
        )
    } else {
        return Err(Error::Syntax);
    };

    Ok(RespValue::array(
        results
            .into_iter()
            .map(|r| format_geo_result(r, with_coord, with_dist, with_hash))
            .collect(),
    ))
}

/// GEOSEARCHSTORE destination source <options...>
fn cmd_geosearchstore(store: &Store, args: &[Bytes]) -> Result<RespValue> {
    if args.len() < 5 {
        return Err(Error::WrongArity("GEOSEARCHSTORE"));
    }

    let dest = args[0].clone();
    let source = &args[1];
    let mut i = 2;
    let mut store_dist = false;

    // Parse FROM
    let (lon, lat) = if eq_ignore_case(&args[i], b"FROMMEMBER") {
        i += 1;
        if i >= args.len() {
            return Err(Error::Syntax);
        }
        match store.geo_pos(source, &[args[i].clone()]).into_iter().next() {
            Some(Some((lo, la))) => {
                i += 1;
                (lo, la)
            }
            _ => return Err(Error::Other("member not found")),
        }
    } else if eq_ignore_case(&args[i], b"FROMLONLAT") {
        i += 1;
        if i + 1 >= args.len() {
            return Err(Error::Syntax);
        }
        let lo = parse_float(&args[i])?;
        let la = parse_float(&args[i + 1])?;
        i += 2;
        (lo, la)
    } else {
        return Err(Error::Syntax);
    };

    // Check for STOREDIST
    for arg in &args[i..] {
        if eq_ignore_case(arg, b"STOREDIST") {
            store_dist = true;
        }
    }

    let (_, _, _, count, ascending) = parse_radius_options(args, i);

    // Parse BY and execute search
    if i >= args.len() {
        return Err(Error::Syntax);
    }

    let results = if eq_ignore_case(&args[i], b"BYRADIUS") {
        i += 1;
        if i + 1 >= args.len() {
            return Err(Error::Syntax);
        }
        let radius = parse_float(&args[i])?;
        let unit = &args[i + 1];
        store.geo_radius(
            source, lon, lat, radius, unit, false, true, false, count, ascending,
        )
    } else if eq_ignore_case(&args[i], b"BYBOX") {
        i += 1;
        if i + 2 >= args.len() {
            return Err(Error::Syntax);
        }
        let width = parse_float(&args[i])?;
        let height = parse_float(&args[i + 1])?;
        let unit = &args[i + 2];
        store.geo_search_box(
            source, lon, lat, width, height, unit, false, true, false, count, ascending,
        )
    } else {
        return Err(Error::Syntax);
    };

    // Store results in destination sorted set
    let count = results.len();
    let entries: Vec<(f64, Bytes)> = results
        .into_iter()
        .map(|r| {
            let score = if store_dist {
                r.distance.unwrap_or(0.0)
            } else {
                r.hash.unwrap_or(0.0)
            };
            (score, r.member)
        })
        .collect();

    // Use ZADD to store
    let _ = store.zadd(dest, entries);

    Ok(RespValue::integer(count as i64))
}
