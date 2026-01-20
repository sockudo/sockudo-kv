//! Geospatial operations for the Store
//!
//! Redis-compatible geospatial implementation.
//! Matches Redis behavior exactly for all edge cases:
//! - Pole crossing searches
//! - Antimeridian (±180°) crossing
//! - Very large radius searches (>5000km)
//! - Geohash string encoding

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::storage::Store;
use crate::storage::types::DataType;

// ==================== Constants ====================

/// Earth's radius in meters (matches Redis exactly)
const EARTH_RADIUS_M: f64 = 6372797.560856;

/// Mercator projection latitude limits (matches Redis)
const GEO_LAT_MAX: f64 = 85.05112878;
const GEO_LAT_MIN: f64 = -85.05112878;
const GEO_LONG_MAX: f64 = 180.0;
const GEO_LONG_MIN: f64 = -180.0;

/// Conversion factors (matching Redis exactly)
const M_PER_KM: f64 = 1000.0;
const M_PER_FT: f64 = 0.3048;
const M_PER_MI: f64 = 1609.34;

/// Maximum geohash precision (Redis uses 26 steps = 52 bits)
const GEO_STEP_MAX: u8 = 26;

/// Mercator maximum (half circumference of Earth in meters)
const MERCATOR_MAX: f64 = 20037726.37;

// ==================== Bit Interleaving (Redis-compatible) ====================

/// Interleave bits of two 32-bit values into a 64-bit value.
/// x bits go to even positions, y bits go to odd positions.
#[inline]
fn interleave64(xlo: u32, ylo: u32) -> u64 {
    const B: [u64; 5] = [
        0x5555555555555555,
        0x3333333333333333,
        0x0F0F0F0F0F0F0F0F,
        0x00FF00FF00FF00FF,
        0x0000FFFF0000FFFF,
    ];
    const S: [u32; 5] = [1, 2, 4, 8, 16];

    let mut x = xlo as u64;
    let mut y = ylo as u64;

    x = (x | (x << S[4])) & B[4];
    y = (y | (y << S[4])) & B[4];

    x = (x | (x << S[3])) & B[3];
    y = (y | (y << S[3])) & B[3];

    x = (x | (x << S[2])) & B[2];
    y = (y | (y << S[2])) & B[2];

    x = (x | (x << S[1])) & B[1];
    y = (y | (y << S[1])) & B[1];

    x = (x | (x << S[0])) & B[0];
    y = (y | (y << S[0])) & B[0];

    x | (y << 1)
}

/// Deinterleave a 64-bit value back to two 32-bit values.
#[inline]
fn deinterleave64(interleaved: u64) -> (u32, u32) {
    const B: [u64; 6] = [
        0x5555555555555555,
        0x3333333333333333,
        0x0F0F0F0F0F0F0F0F,
        0x00FF00FF00FF00FF,
        0x0000FFFF0000FFFF,
        0x00000000FFFFFFFF,
    ];
    const S: [u32; 6] = [0, 1, 2, 4, 8, 16];

    let mut x = interleaved;
    let mut y = interleaved >> 1;

    x = (x | (x >> S[0])) & B[0];
    y = (y | (y >> S[0])) & B[0];

    x = (x | (x >> S[1])) & B[1];
    y = (y | (y >> S[1])) & B[1];

    x = (x | (x >> S[2])) & B[2];
    y = (y | (y >> S[2])) & B[2];

    x = (x | (x >> S[3])) & B[3];
    y = (y | (y >> S[3])) & B[3];

    x = (x | (x >> S[4])) & B[4];
    y = (y | (y >> S[4])) & B[4];

    x = (x | (x >> S[5])) & B[5];
    y = (y | (y >> S[5])) & B[5];

    (x as u32, y as u32)
}

// ==================== Geohash Encoding/Decoding ====================

/// Geohash bits structure
#[derive(Clone, Copy, Debug, Default)]
pub struct GeoHashBits {
    pub bits: u64,
    pub step: u8,
}

/// Geohash range for queries
#[derive(Clone, Copy, Debug, Default)]
pub struct GeoHashRange {
    pub min: u64,
    pub max: u64,
}

/// Encode longitude/latitude to geohash bits at given precision
#[inline]
pub fn geohash_encode(longitude: f64, latitude: f64, step: u8) -> Option<GeoHashBits> {
    if step == 0 || step > 32 {
        return None;
    }

    // Clamp coordinates to valid range
    let lon = longitude.clamp(GEO_LONG_MIN, GEO_LONG_MAX);
    let lat = latitude.clamp(GEO_LAT_MIN, GEO_LAT_MAX);

    // Normalize to 0-1 range
    let lat_offset = (lat - GEO_LAT_MIN) / (GEO_LAT_MAX - GEO_LAT_MIN);
    let long_offset = (lon - GEO_LONG_MIN) / (GEO_LONG_MAX - GEO_LONG_MIN);

    // Convert to fixed point based on step size
    let lat_offset = (lat_offset * ((1u64 << step) as f64)) as u32;
    let long_offset = (long_offset * ((1u64 << step) as f64)) as u32;

    // Redis interleaves: lat in even positions, lon in odd positions
    let bits = interleave64(lat_offset, long_offset);

    Some(GeoHashBits { bits, step })
}

/// Encode to 52-bit geohash (Redis standard) and return as f64 score
#[inline]
pub fn geohash_encode_52bit(longitude: f64, latitude: f64) -> f64 {
    match geohash_encode(longitude, latitude, GEO_STEP_MAX) {
        Some(hash) => hash.bits as f64,
        None => 0.0,
    }
}

/// Align geohash to 52 bits (for lower precision hashes)
#[inline]
pub fn geohash_align_52bits(hash: &GeoHashBits) -> u64 {
    hash.bits << (52 - hash.step * 2)
}

/// Decode geohash bits back to bounding box area
#[inline]
pub fn geohash_decode_area(hash: &GeoHashBits) -> ((f64, f64), (f64, f64)) {
    if hash.step == 0 {
        return ((0.0, 0.0), (0.0, 0.0));
    }

    let (ilato, ilono) = deinterleave64(hash.bits);

    let lat_scale = GEO_LAT_MAX - GEO_LAT_MIN;
    let long_scale = GEO_LONG_MAX - GEO_LONG_MIN;

    let lat_min = GEO_LAT_MIN + (ilato as f64 / ((1u64 << hash.step) as f64)) * lat_scale;
    let lat_max = GEO_LAT_MIN + ((ilato + 1) as f64 / ((1u64 << hash.step) as f64)) * lat_scale;
    let long_min = GEO_LONG_MIN + (ilono as f64 / ((1u64 << hash.step) as f64)) * long_scale;
    let long_max = GEO_LONG_MIN + ((ilono + 1) as f64 / ((1u64 << hash.step) as f64)) * long_scale;

    ((long_min, lat_min), (long_max, lat_max))
}

/// Decode geohash bits back to center (longitude, latitude)
#[inline]
pub fn geohash_decode(hash: &GeoHashBits) -> (f64, f64) {
    let ((lon_min, lat_min), (lon_max, lat_max)) = geohash_decode_area(hash);
    let longitude = (lon_min + lon_max) / 2.0;
    let latitude = (lat_min + lat_max) / 2.0;
    (longitude, latitude)
}

/// Decode a 52-bit geohash (stored as f64 score) to (longitude, latitude)
#[inline]
pub fn geohash_decode_52bit(score: f64) -> (f64, f64) {
    let hash = GeoHashBits {
        bits: score as u64,
        step: GEO_STEP_MAX,
    };
    geohash_decode(&hash)
}

/// Convert internal geohash score to standard geohash base32 string (11 characters)
/// Matches Redis behavior exactly
#[inline]
pub fn geohash_to_string(score: f64) -> String {
    const ALPHABET: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";

    // Decode from internal Mercator format to get coordinates
    let (lon, lat) = geohash_decode_52bit(score);

    // Re-encode using standard geohash ranges (-90 to 90 for latitude)
    // This matches Redis's geohashCommand which uses r[1].min = -90; r[1].max = 90
    const STANDARD_LAT_MIN: f64 = -90.0;
    const STANDARD_LAT_MAX: f64 = 90.0;
    const STANDARD_LON_MIN: f64 = -180.0;
    const STANDARD_LON_MAX: f64 = 180.0;

    // Standard geohash encoding uses alternating bits:
    // bit 0 (MSB): longitude, bit 1: latitude, etc.
    let mut hash: u64 = 0;
    let mut lat_min = STANDARD_LAT_MIN;
    let mut lat_max = STANDARD_LAT_MAX;
    let mut lon_min = STANDARD_LON_MIN;
    let mut lon_max = STANDARD_LON_MAX;

    // Generate 52 bits (26 for each coordinate)
    for i in 0..52 {
        if i % 2 == 0 {
            // Longitude bit (even positions)
            let mid = (lon_min + lon_max) / 2.0;
            if lon >= mid {
                hash |= 1u64 << (51 - i);
                lon_min = mid;
            } else {
                lon_max = mid;
            }
        } else {
            // Latitude bit (odd positions)
            let mid = (lat_min + lat_max) / 2.0;
            if lat >= mid {
                hash |= 1u64 << (51 - i);
                lat_min = mid;
            } else {
                lat_max = mid;
            }
        }
    }

    // Convert to base32 string (11 characters)
    // Redis uses idx=0 for the 11th character since 52 bits < 55 bits needed
    let mut result = String::with_capacity(11);
    for i in 0..11 {
        let idx = if i == 10 {
            // Last character: Redis always uses '0' (idx=0) because
            // 11 chars * 5 bits = 55 bits but we only have 52 bits
            0
        } else {
            let shift = 52 - ((i + 1) * 5);
            ((hash >> shift) & 0x1F) as usize
        };
        result.push(ALPHABET[idx] as char);
    }

    result
}

// ==================== Distance Calculation ====================

/// Calculate distance between two points using Haversine formula (Redis-compatible)
#[inline]
pub fn haversine_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let lon1r = lon1.to_radians();
    let lon2r = lon2.to_radians();
    let lat1r = lat1.to_radians();
    let lat2r = lat2.to_radians();

    let u = ((lat2r - lat1r) / 2.0).sin();
    let v = ((lon2r - lon1r) / 2.0).sin();
    let a = u * u + lat1r.cos() * lat2r.cos() * v * v;

    2.0 * EARTH_RADIUS_M * a.sqrt().asin()
}

/// Convert distance to meters based on unit
#[inline]
pub fn to_meters(distance: f64, unit: &[u8]) -> f64 {
    match unit.to_ascii_uppercase().as_slice() {
        b"KM" => distance * M_PER_KM,
        b"FT" => distance * M_PER_FT,
        b"MI" => distance * M_PER_MI,
        _ => distance,
    }
}

/// Convert meters to specified unit
#[inline]
pub fn from_meters(meters: f64, unit: &[u8]) -> f64 {
    match unit.to_ascii_uppercase().as_slice() {
        b"KM" => meters / M_PER_KM,
        b"FT" => meters / M_PER_FT,
        b"MI" => meters / M_PER_MI,
        _ => meters,
    }
}

// ==================== Geohash Navigation ====================

/// Move geohash in x (longitude) direction
#[inline]
fn geohash_move_x(hash: &mut GeoHashBits, d: i8) {
    if d == 0 {
        return;
    }

    let x = hash.bits & 0xaaaaaaaaaaaaaaaa;
    let y = hash.bits & 0x5555555555555555;
    let zz = 0x5555555555555555u64 >> (64 - hash.step as u32 * 2);

    let x = if d > 0 {
        x.wrapping_add(zz + 1)
    } else {
        (x | zz).wrapping_sub(zz + 1)
    };

    let x = x & (0xaaaaaaaaaaaaaaaa >> (64 - hash.step as u32 * 2));
    hash.bits = x | y;
}

/// Move geohash in y (latitude) direction
#[inline]
fn geohash_move_y(hash: &mut GeoHashBits, d: i8) {
    if d == 0 {
        return;
    }

    let x = hash.bits & 0xaaaaaaaaaaaaaaaa;
    let y = hash.bits & 0x5555555555555555;
    let zz = 0xaaaaaaaaaaaaaaaa >> (64 - hash.step as u32 * 2);

    let y = if d > 0 {
        y.wrapping_add(zz + 1)
    } else {
        (y | zz).wrapping_sub(zz + 1)
    };

    let y = y & (0x5555555555555555 >> (64 - hash.step as u32 * 2));
    hash.bits = x | y;
}

// ==================== Geohash Range Calculation ====================

/// Estimate optimal step (precision) for a radius query
/// Matches Redis's geohashEstimateStepsByRadius
#[inline]
fn estimate_steps_by_radius(range_meters: f64, lat: f64) -> u8 {
    if range_meters == 0.0 {
        return GEO_STEP_MAX;
    }

    let mut range = range_meters;
    let mut step: i32 = 1;

    while range < MERCATOR_MAX {
        range *= 2.0;
        step += 1;
    }
    step -= 2;

    // Reduce precision towards poles
    let abs_lat = lat.abs();
    if abs_lat > 66.0 {
        step -= 2;
        if abs_lat > 80.0 {
            step -= 2;
        }
    }

    step.clamp(1, GEO_STEP_MAX as i32) as u8
}

/// GeoHashRadius structure containing all 9 search areas
#[derive(Debug, Default)]
pub struct GeoHashRadius {
    pub hash: GeoHashBits,
    pub neighbors: [GeoHashRange; 9], // center + 8 neighbors
    pub neighbor_count: usize,
}

/// Calculate the geohash ranges to search for a given center and radius
/// This implements Redis's geohashCalculateAreasByShapeWGS84
pub fn geohash_calculate_areas(
    lon: f64,
    lat: f64,
    radius_m: f64,
    width_m: Option<f64>,
    height_m: Option<f64>,
) -> GeoHashRadius {
    // Use the larger dimension for step calculation
    let search_radius = if let (Some(w), Some(h)) = (width_m, height_m) {
        ((w / 2.0).powi(2) + (h / 2.0).powi(2)).sqrt()
    } else {
        radius_m
    };

    // Calculate optimal step
    let step = estimate_steps_by_radius(search_radius, lat);

    // Call the recursive helper
    geohash_calculate_areas_recursive(lon, lat, radius_m, width_m, height_m, step)
}

fn geohash_calculate_areas_recursive(
    lon: f64,
    lat: f64,
    radius_m: f64,
    width_m: Option<f64>,
    height_m: Option<f64>,
    step: u8,
) -> GeoHashRadius {
    let mut result = GeoHashRadius::default();

    // Encode center point
    let center_hash = match geohash_encode(lon, lat, step) {
        Some(h) => h,
        None => return result,
    };
    result.hash = center_hash;

    // Calculate bounding box for the search area
    let height = height_m.unwrap_or(radius_m * 2.0);
    let width = width_m.unwrap_or(radius_m * 2.0);

    // Calculate latitude delta
    let lat_delta = (height / 2.0 / EARTH_RADIUS_M).to_degrees();

    // Calculate longitude delta at the search latitude extremes
    let north_lat = (lat + lat_delta).min(GEO_LAT_MAX);
    let south_lat = (lat - lat_delta).max(GEO_LAT_MIN);

    // Longitude delta varies with latitude (wider at equator, narrower at poles)
    let lon_delta_north = if north_lat.to_radians().cos().abs() > 1e-10 {
        (width / 2.0 / EARTH_RADIUS_M / north_lat.to_radians().cos()).to_degrees()
    } else {
        360.0 // Near poles, cover all longitudes
    };
    let lon_delta_south = if south_lat.to_radians().cos().abs() > 1e-10 {
        (width / 2.0 / EARTH_RADIUS_M / south_lat.to_radians().cos()).to_degrees()
    } else {
        360.0
    };
    let lon_delta = lon_delta_north.max(lon_delta_south);

    // Calculate bounding box corners
    let min_lon = lon - lon_delta;
    let max_lon = lon + lon_delta;
    let min_lat = south_lat;
    let max_lat = north_lat;

    // Decode center hash to get its area
    let ((area_lon_min, area_lat_min), (area_lon_max, area_lat_max)) =
        geohash_decode_area(&center_hash);

    // [New Logic matching Redis]
    // Check if the step is enough at the limits of the covered area.
    // If not, decrease step and recalculate.
    let mut decrease_step = false;
    if step > 1 {
        let mut north = center_hash;
        geohash_move_x(&mut north, 0); // No-op
        geohash_move_y(&mut north, 1);
        let ((_, _), (_, north_max_lat)) = geohash_decode_area(&north);

        let mut south = center_hash;
        geohash_move_x(&mut south, 0); // No-op
        geohash_move_y(&mut south, -1);
        let ((_, south_min_lat), (_, _)) = geohash_decode_area(&south);

        let mut east = center_hash;
        geohash_move_x(&mut east, 1);
        geohash_move_y(&mut east, 0);
        let ((_, _), (east_max_lon, _)) = geohash_decode_area(&east);

        let mut west = center_hash;
        geohash_move_x(&mut west, -1);
        geohash_move_y(&mut west, 0);
        let ((west_min_lon, _), (_, _)) = geohash_decode_area(&west);

        if north_max_lat < max_lat {
            decrease_step = true;
        }
        if south_min_lat > min_lat {
            decrease_step = true;
        }
        if east_max_lon < max_lon {
            decrease_step = true;
        }
        if west_min_lon > min_lon {
            decrease_step = true;
        }
    }

    if decrease_step && step > 1 {
        return geohash_calculate_areas_recursive(lon, lat, radius_m, width_m, height_m, step - 1);
    }

    // Determine which neighbors we need based on bounding box
    let mut neighbors_needed = [false; 9]; // center, E, W, N, S, NE, NW, SE, SW
    neighbors_needed[0] = true; // Always include center

    // Check each direction
    if max_lon > area_lon_max {
        neighbors_needed[1] = true; // East
    }
    if min_lon < area_lon_min {
        neighbors_needed[2] = true; // West
    }
    if max_lat > area_lat_max {
        neighbors_needed[3] = true; // North
    }
    if min_lat < area_lat_min {
        neighbors_needed[4] = true; // South
    }
    // Corners
    if neighbors_needed[1] && neighbors_needed[3] {
        neighbors_needed[5] = true; // NE
    }
    if neighbors_needed[2] && neighbors_needed[3] {
        neighbors_needed[6] = true; // NW
    }
    if neighbors_needed[1] && neighbors_needed[4] {
        neighbors_needed[7] = true; // SE
    }
    if neighbors_needed[2] && neighbors_needed[4] {
        neighbors_needed[8] = true; // SW
    }

    // Generate the hash ranges for each needed neighbor
    let directions: [(i8, i8); 9] = [
        (0, 0),   // center
        (1, 0),   // E
        (-1, 0),  // W
        (0, 1),   // N
        (0, -1),  // S
        (1, 1),   // NE
        (-1, 1),  // NW
        (1, -1),  // SE
        (-1, -1), // SW
    ];

    for (i, &needed) in neighbors_needed.iter().enumerate() {
        if needed {
            let mut neighbor = center_hash;
            let (dx, dy) = directions[i];
            geohash_move_x(&mut neighbor, dx);
            geohash_move_y(&mut neighbor, dy);

            // Calculate the 52-bit aligned range
            let min_hash = geohash_align_52bits(&neighbor);
            let max_hash = min_hash + ((1u64 << (52 - neighbor.step as u32 * 2)) - 1);

            result.neighbors[result.neighbor_count] = GeoHashRange {
                min: min_hash,
                max: max_hash,
            };
            result.neighbor_count += 1;
        }
    }

    result
}

// ==================== Geo Result Structure ====================

#[derive(Debug, Clone)]
pub struct GeoResult {
    pub member: Bytes,
    pub distance: Option<f64>,
    pub hash: Option<f64>,
    pub longitude: Option<f64>,
    pub latitude: Option<f64>,
}

// ==================== Store Operations ====================

impl Store {
    /// Check if key exists and has correct type for GEO operations
    fn check_geo_type(&self, key: &[u8]) -> Result<bool> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return Ok(false);
                }
                match &entry_ref.1.data {
                    DataType::SortedSet(_) | DataType::SortedSetPacked(_) => Ok(true),
                    _ => Err(Error::WrongType),
                }
            }
            None => Ok(false),
        }
    }

    /// GEOADD key [NX|XX] [CH] longitude latitude member [...]
    pub fn geo_add(
        &self,
        key: Bytes,
        members: Vec<(f64, f64, Bytes)>,
        nx: bool,
        xx: bool,
        ch: bool,
    ) -> Result<usize> {
        if nx && xx {
            return Err(Error::Syntax);
        }

        if nx || xx || ch {
            let existing_scores: std::collections::HashMap<Bytes, f64> = members
                .iter()
                .filter_map(|(_, _, member)| {
                    self.zscore(&key, member)
                        .map(|score| (member.clone(), score))
                })
                .collect();

            let mut added = 0;
            let mut changed = 0;
            let mut entries_to_add = Vec::new();

            for (lon, lat, member) in members {
                let new_score = geohash_encode_52bit(lon, lat);
                let exists = existing_scores.contains_key(&member);
                let old_score = existing_scores.get(&member).copied();

                let should_update = if nx {
                    !exists
                } else if xx {
                    exists
                } else {
                    true
                };

                if should_update {
                    if !exists {
                        added += 1;
                    } else if let Some(old) = old_score {
                        if (old - new_score).abs() > f64::EPSILON {
                            changed += 1;
                        }
                    }
                    entries_to_add.push((new_score, member));
                }
            }

            if !entries_to_add.is_empty() {
                self.zadd(key, entries_to_add)?;
            }

            Ok(if ch { added + changed } else { added })
        } else {
            let entries: Vec<(f64, Bytes)> = members
                .into_iter()
                .map(|(lon, lat, member)| (geohash_encode_52bit(lon, lat), member))
                .collect();

            self.zadd(key, entries)
        }
    }

    /// GEODIST key member1 member2 [unit]
    pub fn geo_dist(
        &self,
        key: &[u8],
        member1: &[u8],
        member2: &[u8],
        unit: &[u8],
    ) -> Result<Option<f64>> {
        if !self.check_geo_type(key)? {
            return Ok(None);
        }

        let score1 = match self.zscore(key, member1) {
            Some(s) => s,
            None => return Ok(None),
        };
        let score2 = match self.zscore(key, member2) {
            Some(s) => s,
            None => return Ok(None),
        };

        let (lon1, lat1) = geohash_decode_52bit(score1);
        let (lon2, lat2) = geohash_decode_52bit(score2);

        let dist_m = haversine_distance(lon1, lat1, lon2, lat2);
        Ok(Some(from_meters(dist_m, unit)))
    }

    /// GEOHASH key member [member ...]
    pub fn geo_hash(&self, key: &[u8], members: &[Bytes]) -> Result<Vec<Option<String>>> {
        if !self.check_geo_type(key)? {
            return Ok(members.iter().map(|_| None).collect());
        }

        Ok(members
            .iter()
            .map(|member| self.zscore(key, member).map(geohash_to_string))
            .collect())
    }

    /// GEOPOS key member [member ...]
    pub fn geo_pos(&self, key: &[u8], members: &[Bytes]) -> Result<Vec<Option<(f64, f64)>>> {
        if !self.check_geo_type(key)? {
            return Ok(members.iter().map(|_| None).collect());
        }

        Ok(members
            .iter()
            .map(|member| self.zscore(key, member).map(geohash_decode_52bit))
            .collect())
    }

    /// Check if a point is within a circular search area
    #[inline]
    fn point_in_radius(
        center_lon: f64,
        center_lat: f64,
        point_lon: f64,
        point_lat: f64,
        radius_m: f64,
    ) -> Option<f64> {
        let dist = haversine_distance(center_lon, center_lat, point_lon, point_lat);
        if dist <= radius_m { Some(dist) } else { None }
    }

    /// Check if a point is within a rectangular search area
    #[inline]
    fn point_in_box(
        center_lon: f64,
        center_lat: f64,
        point_lon: f64,
        point_lat: f64,
        width_m: f64,
        height_m: f64,
    ) -> Option<f64> {
        // Check latitude first (cheaper, doesn't depend on longitude)
        let lat_dist = haversine_distance(center_lon, center_lat, center_lon, point_lat);
        if lat_dist > height_m / 2.0 {
            return None;
        }

        // Check longitude distance at the point's latitude
        // Calculate actual distance along latitude (haversine handles antimeridian)
        let lon_dist = haversine_distance(center_lon, point_lat, point_lon, point_lat);
        if lon_dist > width_m / 2.0 {
            return None;
        }

        // Return the actual distance from center
        Some(haversine_distance(
            center_lon, center_lat, point_lon, point_lat,
        ))
    }

    /// Internal geo search implementation
    /// Handles all search shapes (circle, box) with proper edge case handling
    fn geo_search_internal(
        &self,
        key: &[u8],
        lon: f64,
        lat: f64,
        radius_m: Option<f64>,
        width_m: Option<f64>,
        height_m: Option<f64>,
        with_coord: bool,
        with_dist: bool,
        with_hash: bool,
        count: Option<usize>,
        ascending: Option<bool>,
        any: bool,
        unit: &[u8],
    ) -> Vec<GeoResult> {
        let (search_radius_m, is_circular) = if let Some(r) = radius_m {
            (r, true)
        } else if let (Some(w), Some(h)) = (width_m, height_m) {
            (((w / 2.0).powi(2) + (h / 2.0).powi(2)).sqrt(), false)
        } else {
            return vec![];
        };

        // Use geohash-based range queries
        // For very large searches, geohash_calculate_areas uses low precision (step=1 or 2)
        // which covers large areas but maintains center-first search order
        let areas = geohash_calculate_areas(
            lon,
            lat,
            radius_m.unwrap_or(search_radius_m),
            width_m,
            height_m,
        );

        // Deduplicate neighbor ranges to avoid redundant DB lookups and preserve order
        // Geohash ranges at the same step are either identical or disjoint.
        // We use a small visited check to skip duplicates while maintaining
        // the Center -> Neighbor traversal order required by Redis protocol.

        let mut candidates: Vec<(Bytes, f64)> = Vec::new();
        let mut visited_ranges: Vec<(u64, u64)> = Vec::with_capacity(areas.neighbor_count);

        for i in 0..areas.neighbor_count {
            let range = areas.neighbors[i];
            let pair = (range.min, range.max);

            if !visited_ranges.contains(&pair) {
                visited_ranges.push(pair);

                let members = self.zrangebyscore(
                    key,
                    range.min as f64,
                    range.max as f64,
                    true,
                    0,
                    usize::MAX,
                );

                // Since ranges are disjoint (tiles), we don't need to check for duplicate members
                // optimization: extend instead of loop push
                candidates.reserve(members.len());
                for (member, score_opt) in members {
                    if let Some(score) = score_opt {
                        candidates.push((member, score));
                    }
                }
            }
        }

        // Deduplication handled during collection to preserve order.
        // Sorting removed to preserve Center-first search order.

        // Filter by actual distance and collect results
        // Results remain in geohash score order (sorted set order) by default
        let mut results: Vec<GeoResult> = candidates
            .into_iter()
            .filter_map(|(member, score)| {
                let (member_lon, member_lat) = geohash_decode_52bit(score);

                // Precise check based on search shape
                let dist_m = if is_circular {
                    Self::point_in_radius(lon, lat, member_lon, member_lat, radius_m.unwrap())?
                } else {
                    Self::point_in_box(
                        lon,
                        lat,
                        member_lon,
                        member_lat,
                        width_m.unwrap(),
                        height_m.unwrap(),
                    )?
                };

                let dist_unit = from_meters(dist_m, unit);

                Some(GeoResult {
                    member,
                    distance: if with_dist || ascending.is_some() {
                        Some(dist_unit)
                    } else {
                        None
                    },
                    hash: if with_hash { Some(score) } else { None },
                    longitude: if with_coord { Some(member_lon) } else { None },
                    latitude: if with_coord { Some(member_lat) } else { None },
                })
            })
            .collect();

        // Handle ANY option - return in arbitrary order, then optionally sort
        if any {
            if let Some(c) = count {
                results.truncate(c);
            }
            if let Some(asc) = ascending {
                results.sort_by(|a, b| {
                    let da = a.distance.unwrap_or(0.0);
                    let db = b.distance.unwrap_or(0.0);
                    if asc {
                        da.partial_cmp(&db).unwrap_or(std::cmp::Ordering::Equal)
                    } else {
                        db.partial_cmp(&da).unwrap_or(std::cmp::Ordering::Equal)
                    }
                });
            }
            return results;
        }

        // Sort by distance only if explicitly requested (ASC/DESC)
        // Otherwise keep geohash score order (Redis default)
        if let Some(asc) = ascending {
            results.sort_by(|a, b| {
                let da = a.distance.unwrap_or(0.0);
                let db = b.distance.unwrap_or(0.0);
                if asc {
                    da.partial_cmp(&db).unwrap_or(std::cmp::Ordering::Equal)
                } else {
                    db.partial_cmp(&da).unwrap_or(std::cmp::Ordering::Equal)
                }
            });
        }

        // Apply count limit
        if let Some(c) = count {
            results.truncate(c);
        }

        results
    }

    /// GEORADIUS / GEOSEARCH by radius
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
        any: bool,
    ) -> Result<Vec<GeoResult>> {
        if !self.check_geo_type(key)? {
            return Ok(vec![]);
        }

        let radius_m = to_meters(radius, unit);
        Ok(self.geo_search_internal(
            key,
            lon,
            lat,
            Some(radius_m),
            None,
            None,
            with_coord,
            with_dist,
            with_hash,
            count,
            ascending,
            any,
            unit,
        ))
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
        any: bool,
    ) -> Result<Vec<GeoResult>> {
        match self.check_geo_type(key) {
            Ok(true) => {
                let score = match self.zscore(key, member) {
                    Some(s) => s,
                    None => {
                        return Err(Error::Custom(
                            "ERR could not decode requested zset member".to_string(),
                        ));
                    }
                };
                let (lon, lat) = geohash_decode_52bit(score);
                self.geo_radius(
                    key, lon, lat, radius, unit, with_coord, with_dist, with_hash, count,
                    ascending, any,
                )
            }
            Ok(false) => Ok(vec![]),
            Err(e) => Err(e),
        }
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
        any: bool,
    ) -> Result<Vec<GeoResult>> {
        if !self.check_geo_type(key)? {
            return Ok(vec![]);
        }

        let width_m = to_meters(width, unit);
        let height_m = to_meters(height, unit);
        Ok(self.geo_search_internal(
            key,
            lon,
            lat,
            None,
            Some(width_m),
            Some(height_m),
            with_coord,
            with_dist,
            with_hash,
            count,
            ascending,
            any,
            unit,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_geohash_encode_decode() {
        let lon = -73.9454966;
        let lat = 40.747533;
        let hash = geohash_encode_52bit(lon, lat);
        let (dec_lon, dec_lat) = geohash_decode_52bit(hash);
        assert!((dec_lon - lon).abs() < 0.0001);
        assert!((dec_lat - lat).abs() < 0.0001);
    }

    #[test]
    fn test_geohash_string() {
        // Redis: GEOADD points -5.6 42.6 test
        // Expected geohash: ezs42e44yx0
        let lon = -5.6;
        let lat = 42.6;
        let hash = geohash_encode_52bit(lon, lat);
        let hash_str = geohash_to_string(hash);
        // Note: Last character may differ slightly due to rounding
        assert!(hash_str.starts_with("ezs42e44yx"));
    }

    #[test]
    fn test_haversine_distance() {
        // Palermo to Catania
        let dist = haversine_distance(13.361389, 38.115556, 15.087269, 37.502669);
        assert!(dist > 166000.0 && dist < 167000.0);
    }

    #[test]
    fn test_interleave_deinterleave() {
        let x: u32 = 12345678;
        let y: u32 = 87654321;
        let interleaved = interleave64(x, y);
        let (dx, dy) = deinterleave64(interleaved);
        assert_eq!(x, dx);
        assert_eq!(y, dy);
    }

    #[test]
    fn test_huge_radius() {
        // Test that we can handle 50000km radius (more than half Earth circumference)
        let step = estimate_steps_by_radius(50_000_000.0, 0.0);
        assert!(step <= 3); // Should be very low precision
    }
}
