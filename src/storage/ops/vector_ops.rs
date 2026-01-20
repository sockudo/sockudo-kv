//! Vector set operations for HNSW-based similarity search
//!
//! This module implements Redis-compatible vector similarity search using HNSW
//! (Hierarchical Navigable Small World) graphs with SIMD-optimized distance computations.

use bytes::Bytes;
use sonic_rs::JsonValueTrait;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};

use crate::error::Result;
use crate::storage::Store;
use crate::storage::types::{DataType, Entry, VectorNode, VectorQuantization, VectorSetData};

// Import SIMD-optimized distance functions
use super::simd_distance::{
    cosine_distance_auto, dot_product_auto, dot_product_q8_auto, hamming_distance_auto,
};

/// Value type for VINFO response - can be integer or string
#[derive(Debug, Clone)]
pub enum VInfoValue {
    Int(i64),
    Str(String),
}

/// Result type for VEMB command with full information
#[derive(Debug, Clone)]
pub struct VembResult {
    /// The de-quantized/de-normalized vector
    pub vector: Vec<f32>,
    /// Quantization type: "fp32", "q8", or "bin"
    pub quant_type: String,
    /// Raw binary data (for RAW mode)
    pub raw_data: Bytes,
    /// L2 norm before normalization
    pub norm: f32,
    /// Quantization range (only for q8)
    pub quant_range: Option<f32>,
}

// ==================== SIMD-Optimized Distance Functions ====================
// These functions use the SIMD-optimized implementations from simd_distance.rs
// with support for AVX-512, AVX2, NEON, and scalar fallback.

/// Compute cosine similarity between two vectors (uses SIMD)
/// Returns value in range [-1, 1] for normalized vectors
#[inline]
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> Option<f32> {
    if a.len() != b.len() {
        return None;
    }
    let distance = cosine_distance_auto(a, b);
    // similarity = 1 - distance
    Some(1.0 - distance)
}

/// Compute cosine distance using SIMD-optimized functions
/// Range: [0, 2] where 0 = identical, 1 = orthogonal, 2 = opposite
#[inline]
pub fn cosine_distance(a: &[f32], b: &[f32]) -> Option<f32> {
    if a.len() != b.len() {
        return None;
    }
    Some(cosine_distance_auto(a, b))
}

/// Fast cosine distance using precomputed norms from VectorNode
/// This is ~2-3x faster than cosine_distance for repeated queries
#[inline]
pub fn cosine_distance_fast(
    query: &[f32],
    query_norm: f32,
    query_inv_norm: f32,
    node: &VectorNode,
) -> f32 {
    if query_norm < 1e-10 || node.is_zero_vector() {
        return 1.0; // Treat zero vectors as orthogonal
    }

    let dot = dot_product_auto(query, &node.vector);
    let similarity = dot * query_inv_norm * node.inv_norm;
    (1.0 - similarity).clamp(0.0, 2.0)
}

/// Compute distance for quantized vectors (Q8)
#[inline]
#[allow(dead_code)]
pub fn cosine_distance_q8(a: &[i8], b: &[i8], scale_a: f32, scale_b: f32) -> f32 {
    let dot = dot_product_q8_auto(a, b) as f32;
    let scaled_dot = dot * scale_a * scale_b;
    // Approximate cosine distance from quantized dot product
    (1.0 - scaled_dot).clamp(0.0, 2.0)
}

/// Compute Hamming distance for binary quantized vectors (uses SIMD POPCNT)
#[inline]
#[allow(dead_code)]
pub fn hamming_distance_bin(a: &[u64], b: &[u64]) -> u32 {
    hamming_distance_auto(a, b)
}

/// Legacy magnitude squared function (still used in some places)
#[inline]
fn magnitude_sq(v: &[f32]) -> f32 {
    dot_product_auto(v, v)
}

#[inline]
#[allow(dead_code)]
pub fn euclidean_distance_sq(a: &[f32], b: &[f32]) -> Option<f32> {
    if a.len() != b.len() {
        return None;
    }
    Some(super::simd_distance::l2_distance_sq_auto(a, b))
}

// ==================== HNSW Helper Types ====================

#[derive(Clone, Copy)]
struct Candidate {
    distance: f32,
    idx: u32,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .distance
            .partial_cmp(&self.distance)
            .unwrap_or(Ordering::Equal)
    }
}

#[derive(Clone, Copy)]
struct MaxCandidate {
    distance: f32,
    idx: u32,
}

impl PartialEq for MaxCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}

impl Eq for MaxCandidate {}

impl PartialOrd for MaxCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MaxCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .partial_cmp(&other.distance)
            .unwrap_or(Ordering::Equal)
    }
}

// ==================== HNSW Helper Functions ====================

/// Greedy search on a single layer using precomputed query norms
fn search_layer_single_impl(
    vs: &VectorSetData,
    query: &[f32],
    query_norm: f32,
    query_inv_norm: f32,
    ep: u32,
    layer: usize,
) -> u32 {
    let mut curr = ep;
    let mut curr_dist = match vs.nodes.get(curr as usize) {
        Some(n) => Some(cosine_distance_fast(query, query_norm, query_inv_norm, n)),
        None => return ep,
    };

    loop {
        let mut changed = false;
        if let Some(node) = vs.nodes.get(curr as usize)
            && layer < node.connections.len()
        {
            for &neighbor in &node.connections[layer] {
                if let Some(n) = vs.nodes.get(neighbor as usize) {
                    let dist = cosine_distance_fast(query, query_norm, query_inv_norm, n);
                    if Some(dist) < curr_dist {
                        curr = neighbor;
                        curr_dist = Some(dist);
                        changed = true;
                    }
                }
            }
        }
        if !changed {
            break;
        }
    }
    curr
}

/// Search layer with ef parameter using precomputed query norms
fn search_layer_impl(
    vs: &VectorSetData,
    query: &[f32],
    query_norm: f32,
    query_inv_norm: f32,
    ep: u32,
    ef: usize,
    layer: usize,
) -> Vec<Candidate> {
    let mut visited = HashSet::new();
    let mut candidates = BinaryHeap::new();
    let mut results = BinaryHeap::new();

    let ep_dist = match vs.nodes.get(ep as usize) {
        Some(n) => cosine_distance_fast(query, query_norm, query_inv_norm, n),
        None => return vec![],
    };

    visited.insert(ep);
    candidates.push(Candidate {
        distance: ep_dist,
        idx: ep,
    });
    results.push(MaxCandidate {
        distance: ep_dist,
        idx: ep,
    });

    while let Some(curr) = candidates.pop() {
        let furthest = results.peek().map(|c| c.distance).unwrap_or(f32::MAX);
        if curr.distance > furthest {
            break;
        }

        if let Some(node) = vs.nodes.get(curr.idx as usize)
            && layer < node.connections.len()
        {
            for &neighbor in &node.connections[layer] {
                if visited.insert(neighbor)
                    && let Some(n) = vs.nodes.get(neighbor as usize)
                {
                    let dist = cosine_distance_fast(query, query_norm, query_inv_norm, n);
                    let furthest = results.peek().map(|c| c.distance).unwrap_or(f32::MAX);

                    if dist < furthest || results.len() < ef {
                        candidates.push(Candidate {
                            distance: dist,
                            idx: neighbor,
                        });
                        results.push(MaxCandidate {
                            distance: dist,
                            idx: neighbor,
                        });

                        if results.len() > ef {
                            results.pop();
                        }
                    }
                }
            }
        }
    }

    let mut result: Vec<_> = results
        .into_iter()
        .map(|c| Candidate {
            distance: c.distance,
            idx: c.idx,
        })
        .collect();
    result.sort_by(|a, b| {
        a.distance
            .partial_cmp(&b.distance)
            .unwrap_or(Ordering::Equal)
    });
    result
}

// ==================== Store Vector Operations ====================

impl Store {
    /// VADD - Add or update a vector in the vector set
    /// Returns (added: bool, level: u8) where added is true if new element added, false if updated
    /// level is the HNSW level assigned to the node (used for replication determinism)
    /// dim: Optional REDUCE dimension - if provided, vectors will be projected to this dimension
    /// fixed_level: Optional fixed level for replication determinism - if provided, uses this level instead of random
    pub fn vadd(
        &self,
        key: Bytes,
        reduce_dim: Option<usize>,
        vector: Vec<f32>,
        element: Bytes,
        quant: VectorQuantization,
        ef_construction: Option<usize>,
        m: Option<usize>,
        attrs: Option<Box<sonic_rs::Value>>,
        fixed_level: Option<u8>,
    ) -> Result<(bool, u8)> {
        let original_dim = vector.len();

        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Vacant(e) => {
                // Create new vector set
                let mut vs = if let Some(reduced_dim) = reduce_dim {
                    // Create with dimension reduction
                    VectorSetData::with_reduction(original_dim, reduced_dim)
                } else {
                    VectorSetData::new(original_dim)
                };

                if let Some(m_val) = m {
                    vs.m = m_val.clamp(2, 128);
                    vs.m0 = vs.m * 2;
                    vs.level_mult = 1.0 / (vs.m as f64).ln();
                }
                if let Some(ef) = ef_construction {
                    vs.ef_construction = ef.max(1);
                }
                vs.quant = quant;

                // Project vector if using dimension reduction
                let final_vector = if let Some(projected) = vs.project_vector(&vector) {
                    projected
                } else {
                    vector
                };

                // Initialize global Q8 stats for the first vector
                if quant == VectorQuantization::Q8 {
                    vs.q8_stats = Some(crate::storage::types::Q8GlobalStats::from_vector(
                        &final_vector,
                    ));
                }

                // Use fixed_level if provided, otherwise generate random level
                let level = fixed_level.unwrap_or_else(|| vs.random_level());
                let mut new_node = VectorNode::new(element.clone(), final_vector, level, quant);
                if attrs.is_some() {
                    new_node.attributes = attrs;
                }
                let new_idx = 0;
                vs.nodes.push(new_node);
                vs.element_index.insert(element, new_idx);
                vs.entry_point = Some(new_idx);
                vs.max_level = level;

                e.insert((key, Entry::new(DataType::VectorSet(Box::new(vs)))));
                self.key_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok((true, level))
            }
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                let DataType::VectorSet(ref mut vs) = entry.data else {
                    return Ok((false, 0));
                };

                // Check dimension compatibility
                // If using reduction, check against original_dim; otherwise check against dim
                let expected_input_dim = vs.original_dim.unwrap_or(vs.dim);
                if expected_input_dim != 0 && expected_input_dim != original_dim {
                    // Return error for dimension mismatch
                    return Err(crate::error::Error::Custom(if vs.original_dim.is_some() {
                        "Input dimension mismatch for projection".to_string()
                    } else {
                        format!(
                            "ERR Vector dimension mismatch. Expected {}, got {}",
                            expected_input_dim, original_dim
                        )
                    }));
                }
                if vs.dim == 0 {
                    vs.dim = original_dim;
                }

                // Project vector if using dimension reduction
                let final_vector = if let Some(projected) = vs.project_vector(&vector) {
                    projected
                } else {
                    vector.clone()
                };

                if let Some(&existing_idx) = vs.element_index.get(&element) {
                    // Update the vector data and recompute norm
                    let node_level = if let Some(node) = vs.nodes.get_mut(existing_idx as usize) {
                        // Recompute norm
                        let sum_sq: f32 = final_vector.iter().map(|&x| x * x).sum();
                        node.norm = sum_sq.sqrt();
                        node.inv_norm = if node.norm > 1e-10 {
                            1.0 / node.norm
                        } else {
                            0.0
                        };
                        node.vector = final_vector.clone();

                        // Handle quantization with global stats for Q8
                        if quant == VectorQuantization::Q8 {
                            if let Some(ref stats) = vs.q8_stats {
                                node.vector_q8 = Some(stats.quantize(&node.vector));
                            } else {
                                node.vector_q8 = Some(VectorNode::quantize_q8_static(&node.vector));
                            }
                        }
                        if quant == VectorQuantization::Binary {
                            node.vector_bin =
                                Some(VectorNode::quantize_binary_static(&node.vector));
                        }
                        if attrs.is_some() {
                            node.attributes = attrs;
                        }
                        node.level
                    } else {
                        entry.bump_version();
                        return Ok((false, 0));
                    };

                    // INCREMENTAL EDGE UPDATE: Only update edges that need to change
                    // Save old connections for differential update
                    let old_connections: Vec<HashSet<u32>> = vs
                        .nodes
                        .get(existing_idx as usize)
                        .map(|n| {
                            n.connections
                                .iter()
                                .map(|c| c.iter().copied().collect())
                                .collect()
                        })
                        .unwrap_or_default();

                    // Precompute query norms for SIMD-optimized distance computations
                    let query = &final_vector;
                    let query_sum_sq: f32 = query.iter().map(|&x| x * x).sum();
                    let query_norm = query_sum_sq.sqrt();
                    let query_inv_norm = if query_norm > 1e-10 {
                        1.0 / query_norm
                    } else {
                        0.0
                    };

                    // Find entry point (use existing entry_point, but not self)
                    let entry_point = vs.entry_point.unwrap_or(0);
                    let mut curr_ep = if entry_point == existing_idx {
                        (0..vs.nodes.len() as u32)
                            .find(|&i| i != existing_idx)
                            .unwrap_or(existing_idx)
                    } else {
                        entry_point
                    };

                    // Search from top layers down to node's level + 1
                    let ef = ef_construction.unwrap_or(vs.ef_construction);
                    for lc in (node_level as usize + 1..=vs.max_level as usize).rev() {
                        if curr_ep != existing_idx {
                            curr_ep = search_layer_single_impl(
                                vs,
                                query,
                                query_norm,
                                query_inv_norm,
                                curr_ep,
                                lc,
                            );
                        }
                    }

                    // Re-establish connections at each layer using INCREMENTAL approach
                    for lc in (0..=node_level as usize).rev() {
                        let m_max = if lc == 0 { vs.m0 } else { vs.m };

                        // Search for new neighbors (excluding self)
                        let neighbors = if curr_ep != existing_idx {
                            search_layer_impl(
                                vs,
                                query,
                                query_norm,
                                query_inv_norm,
                                curr_ep,
                                ef,
                                lc,
                            )
                        } else {
                            vec![]
                        };

                        let new_neighbors: HashSet<u32> = neighbors
                            .iter()
                            .filter(|c| c.idx != existing_idx)
                            .take(m_max)
                            .map(|c| c.idx)
                            .collect();

                        let old_neighbors = if lc < old_connections.len() {
                            &old_connections[lc]
                        } else {
                            &HashSet::new()
                        };

                        // INCREMENTAL: Only remove from neighbors that are no longer connected
                        for &old_neighbor in old_neighbors {
                            if !new_neighbors.contains(&old_neighbor) {
                                if let Some(neighbor) = vs.nodes.get_mut(old_neighbor as usize) {
                                    if lc < neighbor.connections.len() {
                                        neighbor.connections[lc].retain(|&x| x != existing_idx);
                                    }
                                }
                            }
                        }

                        // Update this node's connections
                        let selected: Vec<u32> = new_neighbors.iter().copied().collect();
                        if let Some(node) = vs.nodes.get_mut(existing_idx as usize) {
                            if lc < node.connections.len() {
                                node.connections[lc] = selected.clone();
                            }
                        }

                        // INCREMENTAL: Only add to neighbors that are newly connected
                        let mut prune_needed: Vec<(u32, usize)> = Vec::new();
                        for &neighbor_idx in &new_neighbors {
                            if !old_neighbors.contains(&neighbor_idx) {
                                // This is a NEW connection - add bidirectional link
                                if let Some(neighbor) = vs.nodes.get_mut(neighbor_idx as usize)
                                    && lc < neighbor.connections.len()
                                {
                                    neighbor.connections[lc].push(existing_idx);
                                    if neighbor.connections[lc].len() > m_max {
                                        prune_needed.push((neighbor_idx, lc));
                                    }
                                }
                            }
                        }

                        // Prune over-connected neighbors using precomputed norms
                        for (neighbor_idx, layer) in prune_needed {
                            if let Some(neighbor) = vs.nodes.get(neighbor_idx as usize) {
                                let n_norm = neighbor.norm;
                                let n_inv_norm = neighbor.inv_norm;
                                let nv = &neighbor.vector;
                                let conns = neighbor.connections[layer].clone();
                                let mut scored: Vec<_> = conns
                                    .iter()
                                    .filter_map(|&idx| {
                                        vs.nodes.get(idx as usize).map(|n| {
                                            let dist =
                                                cosine_distance_fast(nv, n_norm, n_inv_norm, n);
                                            (dist, idx)
                                        })
                                    })
                                    .collect();
                                scored.sort_by(|a, b| {
                                    a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal)
                                });
                                let new_conns: Vec<_> =
                                    scored.into_iter().take(m_max).map(|(_, idx)| idx).collect();
                                if let Some(neighbor) = vs.nodes.get_mut(neighbor_idx as usize) {
                                    neighbor.connections[layer] = new_conns;
                                }
                            }
                        }

                        if !neighbors.is_empty() && neighbors[0].idx != existing_idx {
                            curr_ep = neighbors[0].idx;
                        }
                    }

                    entry.bump_version();
                    return Ok((false, node_level));
                }

                // Update global Q8 stats with new vector
                if quant == VectorQuantization::Q8 {
                    if let Some(ref mut stats) = vs.q8_stats {
                        stats.update(&final_vector);
                    } else {
                        vs.q8_stats = Some(crate::storage::types::Q8GlobalStats::from_vector(
                            &final_vector,
                        ));
                    }
                }

                // Use fixed_level if provided (for replication), otherwise generate random level
                let level = fixed_level.unwrap_or_else(|| vs.random_level());
                let mut new_node =
                    VectorNode::new(element.clone(), final_vector.clone(), level, quant);
                if attrs.is_some() {
                    new_node.attributes = attrs;
                }
                let new_idx = vs.nodes.len() as u32;

                if vs.nodes.is_empty() {
                    vs.nodes.push(new_node);
                    vs.element_index.insert(element, new_idx);
                    vs.entry_point = Some(new_idx);
                    vs.max_level = level;
                    entry.bump_version();
                    return Ok((true, level));
                }

                vs.nodes.push(new_node);
                vs.element_index.insert(element, new_idx);

                let ef = ef_construction.unwrap_or(vs.ef_construction);
                let entry_point = vs.entry_point.unwrap();
                let query = final_vector;
                let mut curr_ep = entry_point;

                // Precompute query norms for SIMD-optimized distance computations
                let query_sum_sq: f32 = query.iter().map(|&x| x * x).sum();
                let query_norm = query_sum_sq.sqrt();
                let query_inv_norm = if query_norm > 1e-10 {
                    1.0 / query_norm
                } else {
                    0.0
                };

                for lc in (level as usize + 1..=vs.max_level as usize).rev() {
                    curr_ep = search_layer_single_impl(
                        vs,
                        &query,
                        query_norm,
                        query_inv_norm,
                        curr_ep,
                        lc,
                    );
                }

                for lc in (0..=level as usize).rev() {
                    let m_max = if lc == 0 { vs.m0 } else { vs.m };
                    let neighbors =
                        search_layer_impl(vs, &query, query_norm, query_inv_norm, curr_ep, ef, lc);
                    let selected: Vec<u32> = neighbors.iter().take(m_max).map(|c| c.idx).collect();

                    if lc < vs.nodes[new_idx as usize].connections.len() {
                        vs.nodes[new_idx as usize].connections[lc] = selected.clone();
                    }

                    let mut prune_needed: Vec<(u32, usize)> = Vec::new();
                    for &neighbor_idx in &selected {
                        if neighbor_idx == new_idx {
                            continue;
                        }
                        if let Some(neighbor) = vs.nodes.get_mut(neighbor_idx as usize)
                            && lc < neighbor.connections.len()
                        {
                            neighbor.connections[lc].push(new_idx);
                            if neighbor.connections[lc].len() > m_max {
                                prune_needed.push((neighbor_idx, lc));
                            }
                        }
                    }

                    // Prune over-connected neighbors using precomputed norms
                    for (neighbor_idx, layer) in prune_needed {
                        if let Some(neighbor) = vs.nodes.get(neighbor_idx as usize) {
                            let n_norm = neighbor.norm;
                            let n_inv_norm = neighbor.inv_norm;
                            let nv = &neighbor.vector;
                            let conns = neighbor.connections[layer].clone();
                            let mut scored: Vec<_> = conns
                                .iter()
                                .filter_map(|&idx| {
                                    vs.nodes.get(idx as usize).map(|n| {
                                        let dist = cosine_distance_fast(nv, n_norm, n_inv_norm, n);
                                        (dist, idx)
                                    })
                                })
                                .collect();
                            scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
                            let new_conns: Vec<_> =
                                scored.into_iter().take(m_max).map(|(_, idx)| idx).collect();
                            if let Some(neighbor) = vs.nodes.get_mut(neighbor_idx as usize) {
                                neighbor.connections[layer] = new_conns;
                            }
                        }
                    }

                    if !neighbors.is_empty() {
                        curr_ep = neighbors[0].idx;
                    }
                }

                if level > vs.max_level {
                    vs.entry_point = Some(new_idx);
                    vs.max_level = level;
                }

                entry.bump_version();
                Ok((true, level))
            }
        }
    }

    /// VSIM - Similarity search
    /// epsilon: maximum distance (1 - similarity) to include. E.g., epsilon=0.5 means similarity >= 0.5
    pub fn vsim(
        &self,
        key: &[u8],
        query: &[f32],
        count: usize,
        ef: Option<usize>,
        with_scores: bool,
        with_attribs: bool,
        filter: Option<&str>,
        epsilon: Option<f32>,
    ) -> crate::error::Result<Vec<(Bytes, Option<f32>, Option<String>)>> {
        let Some(entry_ref) = self.data_get(key) else {
            return Ok(vec![]);
        };

        let entry = &entry_ref.1;
        let DataType::VectorSet(ref vs) = entry.data else {
            return Ok(vec![]);
        };

        if vs.is_empty() {
            return Ok(vec![]);
        }

        let Some(ep) = vs.entry_point else {
            return Ok(vec![]);
        };

        // Handle dimension reduction: project query if needed
        let query_vec: std::borrow::Cow<'_, [f32]> = if let Some(original_dim) = vs.original_dim {
            // Vector set uses dimension reduction
            if query.len() == original_dim {
                // Query is in original dimension, project it
                if let Some(projected) = vs.project_vector(query) {
                    std::borrow::Cow::Owned(projected)
                } else {
                    return Ok(vec![]);
                }
            } else if query.len() == vs.dim {
                // Query is already in reduced dimension
                std::borrow::Cow::Borrowed(query)
            } else {
                // Dimension mismatch error
                return Err(crate::error::Error::Custom(
                    "Input dimension mismatch for projection".to_string(),
                ));
            }
        } else {
            // No dimension reduction
            if query.len() != vs.dim {
                return Err(crate::error::Error::Custom(format!(
                    "ERR Vector dimension mismatch. Expected {}, got {}",
                    vs.dim,
                    query.len()
                )));
            }
            std::borrow::Cow::Borrowed(query)
        };

        let query = query_vec.as_ref();

        // Precompute query norms for SIMD-optimized distance computations
        let query_sum_sq: f32 = query.iter().map(|&x| x * x).sum();
        let query_norm = query_sum_sq.sqrt();
        let query_inv_norm = if query_norm > 1e-10 {
            1.0 / query_norm
        } else {
            0.0
        };

        let search_ef = ef.unwrap_or(count.max(vs.ef_construction));
        let mut curr_ep = ep;
        for lc in (1..=vs.max_level as usize).rev() {
            curr_ep = search_layer_single_impl(vs, query, query_norm, query_inv_norm, curr_ep, lc);
        }

        let candidates =
            search_layer_impl(vs, query, query_norm, query_inv_norm, curr_ep, search_ef, 0);
        let mut results = Vec::with_capacity(count);

        for cand in candidates {
            // EPSILON filtering: filter by minimum similarity score
            // EPSILON represents max allowed distance from perfect similarity
            // score = 1 - distance/2, so we want score >= (1 - epsilon)
            // This means: distance <= 2 * epsilon
            if let Some(eps) = epsilon {
                if cand.distance > 2.0 * eps {
                    continue;
                }
            }

            if let Some(node) = vs.nodes.get(cand.idx as usize) {
                if let Some(filter_expr) = filter
                    && !eval_filter(node, filter_expr)
                {
                    continue;
                }

                let score = if with_scores {
                    // Redis formula: similarity = 1.0 - distance/2.0
                    // Distance is in range [0, 2], so similarity is in range [0, 1]
                    // Clamp to handle floating point precision
                    Some((1.0 - cand.distance / 2.0).clamp(0.0, 1.0))
                } else {
                    None
                };

                let attrs = if with_attribs {
                    node.attributes
                        .as_ref()
                        .and_then(|a| sonic_rs::to_string(a.as_ref()).ok())
                } else {
                    None
                };

                results.push((node.element.clone(), score, attrs));
                if results.len() >= count {
                    break;
                }
            }
        }

        Ok(results)
    }

    /// VREM - Remove element from vector set
    /// Deletes the key entirely if the last element is removed
    pub fn vrem(&self, key: &[u8], element: &[u8]) -> bool {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let should_delete;
                {
                    let entry = &mut e.get_mut().1;
                    let DataType::VectorSet(ref mut vs) = entry.data else {
                        return false;
                    };

                    let Some(&idx) = vs.element_index.get(element) else {
                        return false;
                    };

                    vs.element_index.remove(element);

                    let connections = match vs.nodes.get(idx as usize) {
                        Some(node) => node.connections.clone(),
                        None => {
                            should_delete = vs.element_index.is_empty();
                            if !should_delete {
                                entry.bump_version();
                            }
                            if should_delete {
                                e.remove();
                                self.key_count
                                    .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                            }
                            return true;
                        }
                    };

                    for (layer, neighbors) in connections.iter().enumerate() {
                        for &neighbor_idx in neighbors {
                            if let Some(neighbor) = vs.nodes.get_mut(neighbor_idx as usize)
                                && layer < neighbor.connections.len()
                            {
                                neighbor.connections[layer].retain(|&n| n != idx);
                            }
                        }
                    }

                    if vs.entry_point == Some(idx) {
                        vs.entry_point = vs
                            .nodes
                            .iter()
                            .enumerate()
                            .filter(|(i, _)| {
                                *i as u32 != idx
                                    && vs.element_index.values().any(|&v| v == *i as u32)
                            })
                            .max_by_key(|(_, n)| n.level)
                            .map(|(i, _)| i as u32);

                        if let Some(ep) = vs.entry_point {
                            vs.max_level = vs.nodes.get(ep as usize).map(|n| n.level).unwrap_or(0);
                        } else {
                            vs.max_level = 0;
                        }
                    }

                    should_delete = vs.element_index.is_empty();
                    if !should_delete {
                        entry.bump_version();
                    }
                }

                // Delete the key if no elements remain
                if should_delete {
                    e.remove();
                    self.key_count
                        .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                }

                true
            }
            crate::storage::dashtable::Entry::Vacant(_) => false,
        }
    }

    /// VCARD - Get number of elements
    pub fn vcard(&self, key: &[u8]) -> i64 {
        match self.data_get(key) {
            Some(entry_ref) => match entry_ref.1.data.as_vectorset() {
                Some(vs) => vs.element_index.len() as i64,
                None => 0,
            },
            None => 0,
        }
    }

    /// VDIM - Get vector dimension
    pub fn vdim(&self, key: &[u8]) -> Option<usize> {
        match self.data_get(key) {
            Some(entry_ref) => entry_ref.1.data.as_vectorset().map(|vs| vs.dim),
            None => None,
        }
    }

    /// VEMB - Get vector for element
    pub fn vemb(&self, key: &[u8], element: &[u8], _raw: bool) -> Option<Vec<f32>> {
        let entry_ref = self.data_get(key)?;
        let vs = entry_ref.1.data.as_vectorset()?;
        let &idx = vs.element_index.get(element)?;
        let node = vs.nodes.get(idx as usize)?;
        Some(node.vector.clone())
    }

    /// VEMB - Get full vector information for element (supports RAW mode)
    pub fn vemb_full(
        &self,
        key: &[u8],
        element: &[u8],
        raw: bool,
    ) -> Option<super::vector_ops::VembResult> {
        use super::vector_ops::VembResult;

        let entry_ref = self.data_get(key)?;
        let vs = entry_ref.1.data.as_vectorset()?;
        let &idx = vs.element_index.get(element)?;
        let node = vs.nodes.get(idx as usize)?;

        // Calculate norm from vector
        let norm = magnitude_sq(&node.vector).sqrt();

        // Determine quantization type and raw data
        let (quant_type, raw_data, quant_range) = if raw {
            match vs.quant {
                VectorQuantization::NoQuant => {
                    // FP32: raw data is the vector as little-endian bytes
                    let mut bytes = Vec::with_capacity(node.vector.len() * 4);
                    for &v in &node.vector {
                        bytes.extend_from_slice(&v.to_le_bytes());
                    }
                    ("fp32".to_string(), Bytes::from(bytes), None)
                }
                VectorQuantization::Q8 => {
                    // Q8: raw data is the quantized bytes
                    let q8_data = node
                        .vector_q8
                        .as_ref()
                        .map(|q| Bytes::from(q.iter().map(|&b| b as u8).collect::<Vec<_>>()))
                        .unwrap_or_else(|| Bytes::from(vec![]));
                    // Calculate quantization range
                    let (min, max) = node
                        .vector
                        .iter()
                        .fold((f32::INFINITY, f32::NEG_INFINITY), |(min, max), &v| {
                            (min.min(v), max.max(v))
                        });
                    let range = if (max - min).abs() < 1e-10 {
                        1.0
                    } else {
                        (max - min) / 255.0
                    };
                    ("q8".to_string(), q8_data, Some(range))
                }
                VectorQuantization::Binary => {
                    // Binary: raw data is the bitmap
                    let bin_data = node
                        .vector_bin
                        .as_ref()
                        .map(|b| {
                            let mut bytes = Vec::with_capacity(b.len() * 8);
                            for &word in b {
                                bytes.extend_from_slice(&word.to_le_bytes());
                            }
                            Bytes::from(bytes)
                        })
                        .unwrap_or_else(|| Bytes::from(vec![]));
                    ("bin".to_string(), bin_data, None)
                }
            }
        } else {
            (vs.quant.as_str().to_string(), Bytes::new(), None)
        };

        Some(VembResult {
            vector: node.vector.clone(),
            quant_type,
            raw_data,
            norm,
            quant_range,
        })
    }

    /// VGETATTR - Get JSON attributes
    pub fn vgetattr(&self, key: &[u8], element: &[u8]) -> Option<String> {
        let entry_ref = self.data_get(key)?;
        let vs = entry_ref.1.data.as_vectorset()?;
        let &idx = vs.element_index.get(element)?;
        let node = vs.nodes.get(idx as usize)?;
        node.attributes
            .as_ref()
            .and_then(|a| sonic_rs::to_string(a.as_ref()).ok())
    }

    /// VSETATTR - Set JSON attributes
    pub fn vsetattr(
        &self,
        key: &[u8],
        element: &[u8],
        attrs: Option<Box<sonic_rs::Value>>,
    ) -> bool {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if let DataType::VectorSet(ref mut vs) = entry.data {
                    let result = vs.set_attributes(element, attrs);
                    if result {
                        entry.bump_version();
                    }
                    result
                } else {
                    false
                }
            }
            crate::storage::dashtable::Entry::Vacant(_) => false,
        }
    }

    /// VISMEMBER - Check if element exists
    pub fn vismember(&self, key: &[u8], element: &[u8]) -> bool {
        match self.data_get(key) {
            Some(entry_ref) => match entry_ref.1.data.as_vectorset() {
                Some(vs) => vs.contains(element),
                None => false,
            },
            None => false,
        }
    }

    /// VLINKS - Get neighbors at each layer
    pub fn vlinks(
        &self,
        key: &[u8],
        element: &[u8],
        with_scores: bool,
    ) -> Option<Vec<Vec<(Bytes, Option<f32>)>>> {
        let entry_ref = self.data_get(key)?;
        let vs = entry_ref.1.data.as_vectorset()?;
        let &idx = vs.element_index.get(element)?;
        let node = vs.nodes.get(idx as usize)?;
        let node_vector = node.vector.clone();

        let result: Vec<Vec<(Bytes, Option<f32>)>> = node
            .connections
            .iter()
            .map(|layer| {
                layer
                    .iter()
                    .filter_map(|&neighbor_idx| {
                        let neighbor = vs.nodes.get(neighbor_idx as usize)?;
                        let score = if with_scores {
                            cosine_similarity(&node_vector, &neighbor.vector)
                        } else {
                            None
                        };
                        Some((neighbor.element.clone(), score))
                    })
                    .collect()
            })
            .collect();

        Some(result)
    }

    /// VRANDMEMBER - Get random members
    pub fn vrandmember(&self, key: &[u8], count: Option<i64>) -> Vec<Bytes> {
        let Some(entry_ref) = self.data_get(key) else {
            return vec![];
        };
        let Some(vs) = entry_ref.1.data.as_vectorset() else {
            return vec![];
        };

        let count = count.unwrap_or(1);
        if count == 0 {
            return vec![];
        }

        let allow_dups = count < 0;
        let n = count.unsigned_abs() as usize;

        if allow_dups {
            let keys: Vec<_> = vs.element_index.keys().cloned().collect();
            if keys.is_empty() {
                return vec![];
            }
            (0..n)
                .map(|_| keys[fastrand::usize(..keys.len())].clone())
                .collect()
        } else {
            vs.random_members(n)
        }
    }

    /// VRANGE - Lexicographical range with Redis-style bounds
    /// Supports: [value (inclusive), (value (exclusive), - (min), + (max)
    pub fn vrange(&self, key: &[u8], start: &[u8], end: &[u8], count: Option<usize>) -> Vec<Bytes> {
        match self.data_get(key) {
            Some(entry_ref) => match entry_ref.1.data.as_vectorset() {
                Some(vs) => vs.lex_range(start, end, count),
                None => vec![],
            },
            None => vec![],
        }
    }

    /// VINFO - Get vector set info
    /// Returns metadata matching Redis's exact field names:
    /// quant-type, vector-dim, size, max-level, vset-uid, hnsw-max-node-uid
    pub fn vinfo(&self, key: &[u8]) -> Option<Vec<(String, VInfoValue)>> {
        let entry_ref = self.data_get(key)?;
        let vs = entry_ref.1.data.as_vectorset()?;

        let mut info = vec![
            // Quantization type: int8 (for Q8), fp32 (for NoQuant), bin (for Binary)
            (
                "quant-type".into(),
                VInfoValue::Str(match vs.quant {
                    VectorQuantization::Q8 => "int8".to_string(),
                    VectorQuantization::NoQuant => "fp32".to_string(),
                    VectorQuantization::Binary => "bin".to_string(),
                }),
            ),
            ("vector-dim".into(), VInfoValue::Int(vs.dim as i64)),
            (
                "size".into(),
                VInfoValue::Int(vs.element_index.len() as i64),
            ),
            ("max-level".into(), VInfoValue::Int(vs.max_level as i64)),
            // vset-uid: unique identifier for this vector set (use 1 as placeholder)
            ("vset-uid".into(), VInfoValue::Int(1)),
            // hnsw-max-node-uid: highest node UID in the graph
            (
                "hnsw-max-node-uid".into(),
                VInfoValue::Int(vs.nodes.len() as i64),
            ),
        ];

        // Add projection info if dimension reduction is used
        if let Some(original_dim) = vs.original_dim {
            info.push((
                "projection-input-dim".into(),
                VInfoValue::Int(original_dim as i64),
            ));
        }

        Some(info)
    }
}

// ==================== High-Performance Filter Expression Parser ====================

/// Token types for filter expressions
#[derive(Debug, Clone, PartialEq)]
enum Token {
    // Literals
    Number(f64),
    String(String),
    Bool(bool),
    Null,
    // Field access
    Field(String),
    // Operators
    Eq,  // ==
    Ne,  // !=
    Lt,  // <
    Le,  // <=
    Gt,  // >
    Ge,  // >=
    And, // and, &&
    Or,  // or, ||
    Not, // !, not
    In,  // in
    Add, // +
    Sub, // -
    Mul, // *
    Div, // /
    Mod, // %
    Pow, // **
    // Grouping
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    Eof,
}

/// Tokenizer for filter expressions
struct Tokenizer<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Tokenizer<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn peek_char(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn next_char(&mut self) -> Option<char> {
        let c = self.peek_char()?;
        self.pos += c.len_utf8();
        Some(c)
    }

    fn skip_whitespace(&mut self) {
        while let Some(c) = self.peek_char() {
            if c.is_whitespace() {
                self.next_char();
            } else {
                break;
            }
        }
    }

    fn read_string(&mut self, quote: char) -> String {
        let mut s = String::new();
        while let Some(c) = self.next_char() {
            if c == quote {
                break;
            } else if c == '\\' {
                if let Some(escaped) = self.next_char() {
                    match escaped {
                        'n' => s.push('\n'),
                        't' => s.push('\t'),
                        'r' => s.push('\r'),
                        _ => s.push(escaped),
                    }
                }
            } else {
                s.push(c);
            }
        }
        s
    }

    fn read_number(&mut self, first: char) -> f64 {
        let mut s = String::new();
        s.push(first);
        while let Some(c) = self.peek_char() {
            if c.is_ascii_digit() || c == '.' || c == 'e' || c == 'E' || c == '-' || c == '+' {
                if (c == '-' || c == '+') && !s.ends_with(['e', 'E']) {
                    break;
                }
                s.push(c);
                self.next_char();
            } else {
                break;
            }
        }
        s.parse().unwrap_or(0.0)
    }

    fn read_identifier(&mut self, first: char) -> String {
        let mut s = String::new();
        s.push(first);
        while let Some(c) = self.peek_char() {
            if c.is_alphanumeric() || c == '_' {
                s.push(c);
                self.next_char();
            } else {
                break;
            }
        }
        s
    }

    fn next_token(&mut self) -> Token {
        self.skip_whitespace();

        let Some(c) = self.next_char() else {
            return Token::Eof;
        };

        match c {
            '(' => Token::LParen,
            ')' => Token::RParen,
            '[' => Token::RBracket, // Note: used for array literals
            ']' => Token::LBracket,
            ',' => Token::Comma,
            '+' => Token::Add,
            '-' => {
                // Could be negative number or subtraction
                if let Some(nc) = self.peek_char() {
                    if nc.is_ascii_digit() {
                        let first = self.next_char().unwrap();
                        Token::Number(-self.read_number(first))
                    } else {
                        Token::Sub
                    }
                } else {
                    Token::Sub
                }
            }
            '*' => {
                if self.peek_char() == Some('*') {
                    self.next_char();
                    Token::Pow
                } else {
                    Token::Mul
                }
            }
            '/' => Token::Div,
            '%' => Token::Mod,
            '=' => {
                if self.peek_char() == Some('=') {
                    self.next_char();
                }
                Token::Eq
            }
            '!' => {
                if self.peek_char() == Some('=') {
                    self.next_char();
                    Token::Ne
                } else {
                    Token::Not
                }
            }
            '<' => {
                if self.peek_char() == Some('=') {
                    self.next_char();
                    Token::Le
                } else {
                    Token::Lt
                }
            }
            '>' => {
                if self.peek_char() == Some('=') {
                    self.next_char();
                    Token::Ge
                } else {
                    Token::Gt
                }
            }
            '&' => {
                if self.peek_char() == Some('&') {
                    self.next_char();
                }
                Token::And
            }
            '|' => {
                if self.peek_char() == Some('|') {
                    self.next_char();
                }
                Token::Or
            }
            '"' | '\'' => Token::String(self.read_string(c)),
            '.' => {
                // Field access: .fieldname
                let first = self.next_char().unwrap_or('_');
                let field = self.read_identifier(first);
                Token::Field(field)
            }
            _ if c.is_ascii_digit() => Token::Number(self.read_number(c)),
            _ if c.is_alphabetic() || c == '_' => {
                let ident = self.read_identifier(c);
                match ident.to_lowercase().as_str() {
                    "true" => Token::Bool(true),
                    "false" => Token::Bool(false),
                    "null" => Token::Null,
                    "and" => Token::And,
                    "or" => Token::Or,
                    "not" => Token::Not,
                    "in" => Token::In,
                    _ => Token::String(ident), // Treat unknown identifiers as strings
                }
            }
            _ => Token::Eof,
        }
    }
}

/// Value type for expression evaluation
#[derive(Debug, Clone)]
enum Value {
    Number(f64),
    String(String),
    Bool(bool),
    Array(Vec<Value>),
    Null,
}

impl Value {
    fn from_json(v: &sonic_rs::Value) -> Self {
        use sonic_rs::{JsonContainerTrait, JsonValueTrait};
        if v.is_null() {
            Value::Null
        } else if let Some(b) = v.as_bool() {
            Value::Bool(b)
        } else if let Some(n) = v.as_i64() {
            Value::Number(n as f64)
        } else if let Some(n) = v.as_u64() {
            Value::Number(n as f64)
        } else if let Some(n) = v.as_f64() {
            Value::Number(n)
        } else if let Some(s) = v.as_str() {
            Value::String(s.to_string())
        } else if v.is_array() {
            let arr: Vec<Value> = v
                .as_array()
                .map(|a: &sonic_rs::Array| a.iter().map(Value::from_json).collect())
                .unwrap_or_default();
            Value::Array(arr)
        } else {
            Value::Null
        }
    }

    fn as_number(&self) -> Option<f64> {
        match self {
            Value::Number(n) => Some(*n),
            Value::String(s) => s.parse().ok(),
            Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            _ => None,
        }
    }

    fn as_bool(&self) -> bool {
        match self {
            Value::Bool(b) => *b,
            Value::Number(n) => *n != 0.0,
            Value::String(s) => !s.is_empty(),
            Value::Array(a) => !a.is_empty(),
            Value::Null => false,
        }
    }

    fn eq(&self, other: &Value) -> bool {
        match (self, other) {
            (Value::Number(a), Value::Number(b)) => (a - b).abs() < 1e-9,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Null, Value::Null) => true,
            (Value::Number(a), Value::String(b)) | (Value::String(b), Value::Number(a)) => b
                .parse::<f64>()
                .map(|n| (n - a).abs() < 1e-9)
                .unwrap_or(false),
            _ => false,
        }
    }
}

/// Expression parser and evaluator
struct FilterExpr<'a> {
    tokenizer: Tokenizer<'a>,
    current: Token,
}

impl<'a> FilterExpr<'a> {
    fn new(input: &'a str) -> Self {
        let mut tokenizer = Tokenizer::new(input);
        let current = tokenizer.next_token();
        Self { tokenizer, current }
    }

    fn advance(&mut self) {
        self.current = self.tokenizer.next_token();
    }

    fn eval(&mut self, attrs: &sonic_rs::Value) -> Value {
        self.parse_or(attrs)
    }

    fn parse_or(&mut self, attrs: &sonic_rs::Value) -> Value {
        let mut left = self.parse_and(attrs);
        while self.current == Token::Or {
            self.advance();
            let right = self.parse_and(attrs);
            left = Value::Bool(left.as_bool() || right.as_bool());
        }
        left
    }

    fn parse_and(&mut self, attrs: &sonic_rs::Value) -> Value {
        let mut left = self.parse_not(attrs);
        while self.current == Token::And {
            self.advance();
            let right = self.parse_not(attrs);
            left = Value::Bool(left.as_bool() && right.as_bool());
        }
        left
    }

    fn parse_not(&mut self, attrs: &sonic_rs::Value) -> Value {
        if self.current == Token::Not {
            self.advance();
            let val = self.parse_not(attrs);
            Value::Bool(!val.as_bool())
        } else {
            self.parse_comparison(attrs)
        }
    }

    fn parse_comparison(&mut self, attrs: &sonic_rs::Value) -> Value {
        let left = self.parse_in(attrs);

        match &self.current {
            Token::Eq => {
                self.advance();
                let right = self.parse_in(attrs);
                Value::Bool(left.eq(&right))
            }
            Token::Ne => {
                self.advance();
                let right = self.parse_in(attrs);
                Value::Bool(!left.eq(&right))
            }
            Token::Lt => {
                self.advance();
                let right = self.parse_in(attrs);
                match (left.as_number(), right.as_number()) {
                    (Some(l), Some(r)) => Value::Bool(l < r),
                    _ => Value::Bool(false),
                }
            }
            Token::Le => {
                self.advance();
                let right = self.parse_in(attrs);
                match (left.as_number(), right.as_number()) {
                    (Some(l), Some(r)) => Value::Bool(l <= r),
                    _ => Value::Bool(false),
                }
            }
            Token::Gt => {
                self.advance();
                let right = self.parse_in(attrs);
                match (left.as_number(), right.as_number()) {
                    (Some(l), Some(r)) => Value::Bool(l > r),
                    _ => Value::Bool(false),
                }
            }
            Token::Ge => {
                self.advance();
                let right = self.parse_in(attrs);
                match (left.as_number(), right.as_number()) {
                    (Some(l), Some(r)) => Value::Bool(l >= r),
                    _ => Value::Bool(false),
                }
            }
            _ => left,
        }
    }

    fn parse_in(&mut self, attrs: &sonic_rs::Value) -> Value {
        let left = self.parse_additive(attrs);

        if self.current == Token::In {
            self.advance();
            let right = self.parse_additive(attrs);

            // Check if left is in right
            match (&left, &right) {
                // String contains substring: "abc" in .field
                (Value::String(needle), Value::String(haystack)) => {
                    Value::Bool(haystack.contains(needle.as_str()))
                }
                // Value in array: .field in [1, 2, 3]
                (val, Value::Array(arr)) => Value::Bool(arr.iter().any(|item| val.eq(item))),
                _ => Value::Bool(false),
            }
        } else {
            left
        }
    }

    fn parse_additive(&mut self, attrs: &sonic_rs::Value) -> Value {
        let mut left = self.parse_multiplicative(attrs);

        loop {
            match &self.current {
                Token::Add => {
                    self.advance();
                    let right = self.parse_multiplicative(attrs);
                    match (left.as_number(), right.as_number()) {
                        (Some(l), Some(r)) => left = Value::Number(l + r),
                        _ => left = Value::Null,
                    }
                }
                Token::Sub => {
                    self.advance();
                    let right = self.parse_multiplicative(attrs);
                    match (left.as_number(), right.as_number()) {
                        (Some(l), Some(r)) => left = Value::Number(l - r),
                        _ => left = Value::Null,
                    }
                }
                _ => break,
            }
        }
        left
    }

    fn parse_multiplicative(&mut self, attrs: &sonic_rs::Value) -> Value {
        let mut left = self.parse_power(attrs);

        loop {
            match &self.current {
                Token::Mul => {
                    self.advance();
                    let right = self.parse_power(attrs);
                    match (left.as_number(), right.as_number()) {
                        (Some(l), Some(r)) => left = Value::Number(l * r),
                        _ => left = Value::Null,
                    }
                }
                Token::Div => {
                    self.advance();
                    let right = self.parse_power(attrs);
                    match (left.as_number(), right.as_number()) {
                        (Some(l), Some(r)) if r != 0.0 => left = Value::Number(l / r),
                        _ => left = Value::Null,
                    }
                }
                Token::Mod => {
                    self.advance();
                    let right = self.parse_power(attrs);
                    match (left.as_number(), right.as_number()) {
                        (Some(l), Some(r)) if r != 0.0 => left = Value::Number(l % r),
                        _ => left = Value::Null,
                    }
                }
                _ => break,
            }
        }
        left
    }

    fn parse_power(&mut self, attrs: &sonic_rs::Value) -> Value {
        let left = self.parse_unary(attrs);

        if self.current == Token::Pow {
            self.advance();
            let right = self.parse_power(attrs); // Right associative
            match (left.as_number(), right.as_number()) {
                (Some(l), Some(r)) => Value::Number(l.powf(r)),
                _ => Value::Null,
            }
        } else {
            left
        }
    }

    fn parse_unary(&mut self, attrs: &sonic_rs::Value) -> Value {
        if self.current == Token::Sub {
            self.advance();
            let val = self.parse_unary(attrs);
            match val.as_number() {
                Some(n) => Value::Number(-n),
                None => Value::Null,
            }
        } else {
            self.parse_primary(attrs)
        }
    }

    fn parse_primary(&mut self, attrs: &sonic_rs::Value) -> Value {
        match std::mem::replace(&mut self.current, Token::Eof) {
            Token::Number(n) => {
                self.advance();
                Value::Number(n)
            }
            Token::String(s) => {
                self.advance();
                Value::String(s)
            }
            Token::Bool(b) => {
                self.advance();
                Value::Bool(b)
            }
            Token::Null => {
                self.advance();
                Value::Null
            }
            Token::Field(name) => {
                self.advance();
                let v = attrs.get(name.as_str());
                match v {
                    Some(val) => Value::from_json(val),
                    None => Value::Null,
                }
            }
            Token::LParen => {
                self.advance();
                let val = self.parse_or(attrs);
                if self.current == Token::RParen {
                    self.advance();
                }
                val
            }
            Token::RBracket => {
                // Array literal: [1, 2, 3]
                self.advance();
                let mut arr = Vec::new();
                while self.current != Token::LBracket && self.current != Token::Eof {
                    arr.push(self.parse_or(attrs));
                    if self.current == Token::Comma {
                        self.advance();
                    }
                }
                if self.current == Token::LBracket {
                    self.advance();
                }
                Value::Array(arr)
            }
            other => {
                self.current = other;
                Value::Null
            }
        }
    }
}

/// Evaluate filter expression against node attributes
fn eval_filter(node: &VectorNode, filter: &str) -> bool {
    let Some(attrs) = &node.attributes else {
        return false; // No attributes means filter fails
    };

    // Check if attrs is actually a JSON object (not a raw string stored for invalid JSON)
    use sonic_rs::JsonValueTrait;
    if !attrs.is_object() {
        return false; // Invalid JSON stored as string, filter fails
    }

    let mut expr = FilterExpr::new(filter);
    expr.eval(attrs).as_bool()
}

impl VectorNode {
    #[inline]
    pub fn quantize_q8_static(vector: &[f32]) -> Vec<i8> {
        let (min, max) = vector
            .iter()
            .fold((f32::INFINITY, f32::NEG_INFINITY), |(min, max), &v| {
                (min.min(v), max.max(v))
            });
        let scale = if (max - min).abs() < 1e-10 {
            1.0
        } else {
            255.0 / (max - min)
        };

        vector
            .iter()
            .map(|&v| ((v - min) * scale - 128.0).clamp(-128.0, 127.0) as i8)
            .collect()
    }

    #[inline]
    pub fn quantize_binary_static(vector: &[f32]) -> Vec<u64> {
        let mut result = vec![0u64; vector.len().div_ceil(64)];
        for (i, &v) in vector.iter().enumerate() {
            if v > 0.0 {
                result[i / 64] |= 1u64 << (i % 64);
            }
        }
        result
    }
}
