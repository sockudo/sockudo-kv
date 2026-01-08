//! Geospatial operations for the Store
//!
//! Performance optimizations:
//! - Uses SortedSet internally (geohash as score)
//! - Fast 52-bit geohash encoding
//! - Haversine distance calculation
//! - Bounding box pre-filtering for radius queries

use bytes::Bytes;

use crate::error::Result;
use crate::storage::Store;

// ==================== Constants ====================

/// Earth's radius in meters
const EARTH_RADIUS_M: f64 = 6372797.560856;
/// Mercator projection max latitude  
const MERCATOR_MAX: f64 = 85.05112878;
/// Conversion factors
const M_PER_KM: f64 = 1000.0;
const M_PER_FT: f64 = 0.3048;
const M_PER_MI: f64 = 1609.344;

/// Geohash precision (Redis uses 26 steps = 52 bits)
const GEO_STEP: u8 = 26;

// ==================== Redis-Compatible Geohash Encoding ====================
// This matches exactly what Redis, KeyDB, and Dragonfly use

/// Encode latitude and longitude to a 52-bit geohash (Redis-compatible)
/// Uses interleaved bits: longitude in even positions, latitude in odd
#[inline]
pub fn geohash_encode(longitude: f64, latitude: f64) -> f64 {
    // Clamp to valid ranges
    let lat = latitude.clamp(-MERCATOR_MAX, MERCATOR_MAX);
    let lon = longitude.clamp(-180.0, 180.0);

    // Normalize to 0-1 range (matching Redis geohash.c)
    let lat_offset = (lat - (-90.0)) / 180.0;
    let lon_offset = (lon - (-180.0)) / 360.0;

    // Quantize to 26-bit integers
    let lat_bits = (lat_offset * ((1u64 << GEO_STEP) as f64)) as u64;
    let lon_bits = (lon_offset * ((1u64 << GEO_STEP) as f64)) as u64;

    // Interleave bits (Redis algorithm)
    interleave64(lat_bits, lon_bits) as f64
}

/// Decode a 52-bit geohash back to (longitude, latitude) - Redis-compatible
#[inline]
pub fn geohash_decode(hash: f64) -> (f64, f64) {
    let hash = hash as u64;

    // Deinterleave bits
    let (lat_bits, lon_bits) = deinterleave64(hash);

    // Convert back to coordinates
    let lat_offset = lat_bits as f64 / ((1u64 << GEO_STEP) as f64);
    let lon_offset = lon_bits as f64 / ((1u64 << GEO_STEP) as f64);

    let latitude = lat_offset * 180.0 - 90.0;
    let longitude = lon_offset * 360.0 - 180.0;

    (longitude, latitude)
}

/// Interleave two 26-bit integers into a 52-bit integer
/// lon bits go to even positions (0,2,4...), lat bits to odd (1,3,5...)
#[inline]
fn interleave64(lat_bits: u64, lon_bits: u64) -> u64 {
    let mut result: u64 = 0;
    for i in 0..GEO_STEP as usize {
        result |= ((lon_bits >> i) & 1) << (i * 2);
        result |= ((lat_bits >> i) & 1) << (i * 2 + 1);
    }
    result
}

/// Deinterleave a 52-bit integer back to two 26-bit integers
#[inline]
fn deinterleave64(interleaved: u64) -> (u64, u64) {
    let mut lat_bits: u64 = 0;
    let mut lon_bits: u64 = 0;
    for i in 0..GEO_STEP as usize {
        lon_bits |= ((interleaved >> (i * 2)) & 1) << i;
        lat_bits |= ((interleaved >> (i * 2 + 1)) & 1) << i;
    }
    (lat_bits, lon_bits)
}

/// Convert geohash to base32 string (11 characters) - Redis-compatible alphabet
#[inline]
pub fn geohash_to_string(hash: f64) -> String {
    // Redis geohash alphabet (not standard base32)
    const ALPHABET: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";
    let hash = hash as u64;
    let mut result = String::with_capacity(11);

    // Process 5 bits at a time (52 bits = 10.4 chars, we use 11)
    for i in 0..11 {
        let shift = 52_i32 - ((i + 1) * 5);
        let idx = if shift >= 0 {
            ((hash >> shift) & 0x1F) as usize
        } else {
            ((hash << (-shift)) & 0x1F) as usize
        };
        result.push(ALPHABET[idx] as char);
    }

    result
}

// ==================== Distance Calculation ====================

/// Calculate distance between two points using Haversine formula
#[inline]
pub fn haversine_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let lat1_rad = lat1.to_radians();
    let lat2_rad = lat2.to_radians();
    let delta_lat = (lat2 - lat1).to_radians();
    let delta_lon = (lon2 - lon1).to_radians();

    let a = (delta_lat / 2.0).sin().powi(2)
        + lat1_rad.cos() * lat2_rad.cos() * (delta_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();

    EARTH_RADIUS_M * c
}

/// Convert distance to meters based on unit
#[inline]
pub fn to_meters(distance: f64, unit: &[u8]) -> f64 {
    match unit.to_ascii_uppercase().as_slice() {
        b"KM" => distance * M_PER_KM,
        b"FT" => distance * M_PER_FT,
        b"MI" => distance * M_PER_MI,
        _ => distance, // Default: meters
    }
}

/// Convert meters to specified unit
#[inline]
pub fn from_meters(meters: f64, unit: &[u8]) -> f64 {
    match unit.to_ascii_uppercase().as_slice() {
        b"KM" => meters / M_PER_KM,
        b"FT" => meters / M_PER_FT,
        b"MI" => meters / M_PER_MI,
        _ => meters, // Default: meters
    }
}

// ==================== Bounding Box ====================

/// Calculate bounding box for a radius search
#[inline]
fn bounding_box(lon: f64, lat: f64, radius_m: f64) -> (f64, f64, f64, f64) {
    let lat_delta = (radius_m / EARTH_RADIUS_M).to_degrees();
    let lon_delta = (radius_m / (EARTH_RADIUS_M * lat.to_radians().cos())).to_degrees();

    let min_lat = (lat - lat_delta).max(-MERCATOR_MAX);
    let max_lat = (lat + lat_delta).min(MERCATOR_MAX);
    let min_lon = lon - lon_delta;
    let max_lon = lon + lon_delta;

    (min_lon, min_lat, max_lon, max_lat)
}

// ==================== Store Operations ====================

/// Geo search result
#[derive(Debug, Clone)]
pub struct GeoResult {
    pub member: Bytes,
    pub distance: Option<f64>,
    pub hash: Option<f64>,
    pub longitude: Option<f64>,
    pub latitude: Option<f64>,
}

impl Store {
    /// GEOADD key [NX|XX] [CH] longitude latitude member [...]
    pub fn geo_add(
        &self,
        key: Bytes,
        members: Vec<(f64, f64, Bytes)>,
        _nx: bool,
        _xx: bool,
        _ch: bool,
    ) -> Result<usize> {
        // Convert to sorted set entries with geohash as score
        let entries: Vec<(f64, Bytes)> = members
            .into_iter()
            .map(|(lon, lat, member)| (geohash_encode(lon, lat), member))
            .collect();

        // Use basic zadd (NX/XX/CH not implemented in base zadd)
        self.zadd(key, entries)
    }

    /// GEODIST key member1 member2 [unit]
    pub fn geo_dist(&self, key: &[u8], member1: &[u8], member2: &[u8], unit: &[u8]) -> Option<f64> {
        let score1 = self.zscore(key, member1)?;
        let score2 = self.zscore(key, member2)?;

        let (lon1, lat1) = geohash_decode(score1);
        let (lon2, lat2) = geohash_decode(score2);

        let dist_m = haversine_distance(lon1, lat1, lon2, lat2);
        Some(from_meters(dist_m, unit))
    }

    /// GEOHASH key member [member ...]
    pub fn geo_hash(&self, key: &[u8], members: &[Bytes]) -> Vec<Option<String>> {
        members
            .iter()
            .map(|member| {
                self.zscore(key, member)
                    .map(geohash_to_string)
            })
            .collect()
    }

    /// GEOPOS key member [member ...]
    pub fn geo_pos(&self, key: &[u8], members: &[Bytes]) -> Vec<Option<(f64, f64)>> {
        members
            .iter()
            .map(|member| self.zscore(key, member).map(geohash_decode))
            .collect()
    }

    /// GEORADIUS / GEOSEARCH - search within radius from point
    pub fn geo_radius(
        &self,
        key: &[u8],
        lon: f64,
        lat: f64,
        radius: f64,
        unit: &[u8],
        with_coord: bool,
        with_dist: bool,
        with_hash: bool,
        count: Option<usize>,
        ascending: Option<bool>,
    ) -> Vec<GeoResult> {
        let radius_m = to_meters(radius, unit);
        let (min_lon, min_lat, max_lon, max_lat) = bounding_box(lon, lat, radius_m);

        // Get all members in bounding box range
        let min_hash = geohash_encode(min_lon, min_lat);
        let max_hash = geohash_encode(max_lon, max_lat);

        // Get candidates from sorted set
        // Redis/Valkey use reverse order (high to low geohash) for unsorted results
        let raw = self.zrevrangebyscore(key, max_hash, min_hash, true, 0, usize::MAX);
        let candidates: Vec<(f64, Bytes)> = raw
            .into_iter()
            .filter_map(|(member, score)| score.map(|s| (s, member)))
            .collect();

        // Filter by actual distance
        let mut results: Vec<GeoResult> = candidates
            .into_iter()
            .filter_map(|(hash, member)| {
                let (member_lon, member_lat) = geohash_decode(hash);
                let dist_m = haversine_distance(lon, lat, member_lon, member_lat);
                let dist_unit = from_meters(dist_m, unit);

                eprintln!(
                    "[DEBUG] Candidate: {:?}, hash={:.0}, dist={:.4} {:?}",
                    String::from_utf8_lossy(&member),
                    hash,
                    dist_unit,
                    String::from_utf8_lossy(unit)
                );

                if dist_m <= radius_m {
                    Some(GeoResult {
                        member,
                        distance: if with_dist { Some(dist_unit) } else { None },
                        hash: if with_hash { Some(hash) } else { None },
                        longitude: if with_coord { Some(member_lon) } else { None },
                        latitude: if with_coord { Some(member_lat) } else { None },
                    })
                } else {
                    None
                }
            })
            .collect();

        // Sort by distance
        eprintln!(
            "[DEBUG] geo_radius: ascending={:?}, results.len()={}",
            ascending,
            results.len()
        );
        if let Some(asc) = ascending {
            eprintln!("[DEBUG] Sorting by distance, asc={}", asc);
            results.sort_by(|a, b| {
                let da = a.distance.unwrap_or(0.0);
                let db = b.distance.unwrap_or(0.0);
                if asc {
                    da.partial_cmp(&db).unwrap()
                } else {
                    db.partial_cmp(&da).unwrap()
                }
            });
        } else {
            eprintln!("[DEBUG] NOT sorting - returning in geohash order");
        }

        // Apply count limit
        if let Some(c) = count {
            results.truncate(c);
        }

        results
    }

    /// GEORADIUSBYMEMBER - search from member position
    pub fn geo_radius_by_member(
        &self,
        key: &[u8],
        member: &[u8],
        radius: f64,
        unit: &[u8],
        with_coord: bool,
        with_dist: bool,
        with_hash: bool,
        count: Option<usize>,
        ascending: Option<bool>,
    ) -> Option<Vec<GeoResult>> {
        let score = self.zscore(key, member)?;
        let (lon, lat) = geohash_decode(score);
        Some(self.geo_radius(
            key, lon, lat, radius, unit, with_coord, with_dist, with_hash, count, ascending,
        ))
    }

    /// GEOSEARCH with box shape
    pub fn geo_search_box(
        &self,
        key: &[u8],
        lon: f64,
        lat: f64,
        width: f64,
        height: f64,
        unit: &[u8],
        with_coord: bool,
        with_dist: bool,
        with_hash: bool,
        count: Option<usize>,
        ascending: Option<bool>,
    ) -> Vec<GeoResult> {
        let width_m = to_meters(width, unit) / 2.0;
        let height_m = to_meters(height, unit) / 2.0;

        // Calculate box corners
        let lat_delta = (height_m / EARTH_RADIUS_M).to_degrees();
        let lon_delta = (width_m / (EARTH_RADIUS_M * lat.to_radians().cos())).to_degrees();

        let min_lat = lat - lat_delta;
        let max_lat = lat + lat_delta;
        let min_lon = lon - lon_delta;
        let max_lon = lon + lon_delta;

        let min_hash = geohash_encode(min_lon, min_lat);
        let max_hash = geohash_encode(max_lon, max_lat);

        // Use reverse order to match Redis/Valkey behavior
        let raw = self.zrevrangebyscore(key, max_hash, min_hash, true, 0, usize::MAX);
        let candidates: Vec<(f64, Bytes)> = raw
            .into_iter()
            .filter_map(|(member, score)| score.map(|s| (s, member)))
            .collect();

        let mut results: Vec<GeoResult> = candidates
            .into_iter()
            .filter_map(|(hash, member)| {
                let (member_lon, member_lat) = geohash_decode(hash);

                // Check if within box
                if member_lon >= min_lon
                    && member_lon <= max_lon
                    && member_lat >= min_lat
                    && member_lat <= max_lat
                {
                    let dist_m = haversine_distance(lon, lat, member_lon, member_lat);
                    Some(GeoResult {
                        member,
                        distance: if with_dist {
                            Some(from_meters(dist_m, unit))
                        } else {
                            None
                        },
                        hash: if with_hash { Some(hash) } else { None },
                        longitude: if with_coord { Some(member_lon) } else { None },
                        latitude: if with_coord { Some(member_lat) } else { None },
                    })
                } else {
                    None
                }
            })
            .collect();

        // Sort and limit
        if let Some(asc) = ascending {
            results.sort_by(|a, b| {
                let da = a.distance.unwrap_or(0.0);
                let db = b.distance.unwrap_or(0.0);
                if asc {
                    da.partial_cmp(&db).unwrap()
                } else {
                    db.partial_cmp(&da).unwrap()
                }
            });
        }

        if let Some(c) = count {
            results.truncate(c);
        }

        results
    }
}
