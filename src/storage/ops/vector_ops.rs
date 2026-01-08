//! Vector set operations for HNSW-based similarity search
//!
//! Performance optimizations:
//! - SIMD-friendly auto-vectorized distance calculations
//! - Efficient HNSW graph traversal
//! - Quantization support (Q8, Binary)
//! - Lock-free concurrent access patterns

use bytes::Bytes;
use sonic_rs::JsonValueTrait;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};

use crate::error::Result;
use crate::storage::Store;
use crate::storage::types::{DataType, Entry, VectorNode, VectorQuantization, VectorSetData};

// ==================== SIMD-Friendly Distance Functions ====================
// These are written to auto-vectorize on modern compilers (LLVM/GCC)

/// Dot product of two vectors (auto-vectorized)
#[inline]
fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    a.iter().zip(b.iter()).map(|(&x, &y)| x * y).sum()
}

/// Magnitude squared of a vector
#[inline]
fn magnitude_sq(v: &[f32]) -> f32 {
    v.iter().map(|&x| x * x).sum()
}

/// Cosine similarity (1 = identical, 0 = orthogonal, -1 = opposite)
#[inline]
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot = dot_product(a, b);
    let mag_a = magnitude_sq(a).sqrt();
    let mag_b = magnitude_sq(b).sqrt();
    if mag_a < 1e-10 || mag_b < 1e-10 {
        return 0.0;
    }
    dot / (mag_a * mag_b)
}

/// Cosine distance (0 = identical, 2 = opposite)
#[inline]
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    1.0 - cosine_similarity(a, b)
}

/// Euclidean distance squared (faster, avoids sqrt)
#[inline]
#[allow(dead_code)]
pub fn euclidean_distance_sq(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    a.iter()
        .zip(b.iter())
        .map(|(&x, &y)| {
            let diff = x - y;
            diff * diff
        })
        .sum()
}

// ==================== HNSW Helper Types ====================

/// Candidate for priority queue (distance, node index)
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
        // Min-heap: smaller distance = higher priority
        other
            .distance
            .partial_cmp(&self.distance)
            .unwrap_or(Ordering::Equal)
    }
}

/// Max-heap candidate (for keeping furthest)
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
        // Max-heap: larger distance = higher priority
        self.distance
            .partial_cmp(&other.distance)
            .unwrap_or(Ordering::Equal)
    }
}

// ==================== HNSW Helper Functions ====================
// These operate on VectorSetData directly to avoid borrow issues

/// Search single neighbor at a layer (greedy)
fn search_layer_single_impl(vs: &VectorSetData, query: &[f32], ep: u32, layer: usize) -> u32 {
    let mut curr = ep;
    let mut curr_dist = match vs.nodes.get(curr as usize) {
        Some(n) => cosine_distance(query, &n.vector),
        None => return ep,
    };

    loop {
        let mut changed = false;
        if let Some(node) = vs.nodes.get(curr as usize)
            && layer < node.connections.len()
        {
            for &neighbor in &node.connections[layer] {
                if let Some(n) = vs.nodes.get(neighbor as usize) {
                    let dist = cosine_distance(query, &n.vector);
                    if dist < curr_dist {
                        curr = neighbor;
                        curr_dist = dist;
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

/// Search layer with ef candidates (beam search)
fn search_layer_impl(
    vs: &VectorSetData,
    query: &[f32],
    ep: u32,
    ef: usize,
    layer: usize,
) -> Vec<Candidate> {
    let mut visited = HashSet::new();
    let mut candidates = BinaryHeap::new();
    let mut results = BinaryHeap::new();

    let ep_dist = match vs.nodes.get(ep as usize) {
        Some(n) => cosine_distance(query, &n.vector),
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
                    let dist = cosine_distance(query, &n.vector);
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
    /// Returns true if new element added, false if updated
    pub fn vadd(
        &self,
        key: Bytes,
        dim: Option<usize>,
        vector: Vec<f32>,
        element: Bytes,
        quant: VectorQuantization,
        ef_construction: Option<usize>,
        m: Option<usize>,
        attrs: Option<Box<sonic_rs::Value>>,
    ) -> Result<bool> {
        let actual_dim = dim.unwrap_or(vector.len());

        // Validate dimension
        if vector.len() != actual_dim {
            return Ok(false);
        }

        let mut entry_ref = self.data.entry(key.clone()).or_insert_with(|| {
            let mut vs = VectorSetData::new(actual_dim);
            if let Some(m_val) = m {
                vs.m = m_val.clamp(2, 128);
                vs.m0 = vs.m * 2;
                vs.level_mult = 1.0 / (vs.m as f64).ln();
            }
            if let Some(ef) = ef_construction {
                vs.ef_construction = ef.max(1);
            }
            vs.quant = quant;
            Entry::new(DataType::VectorSet(Box::new(vs)))
        });

        let entry = entry_ref.value_mut();

        if let DataType::VectorSet(ref mut vs) = entry.data {
            // Check dimension consistency
            if vs.dim != 0 && vs.dim != vector.len() {
                return Ok(false);
            }
            if vs.dim == 0 {
                vs.dim = vector.len();
            }

            // Check if element exists (update case)
            if let Some(&existing_idx) = vs.element_index.get(&element) {
                // Update existing vector
                if let Some(node) = vs.nodes.get_mut(existing_idx as usize) {
                    node.vector = vector;
                    if quant == VectorQuantization::Q8 {
                        node.vector_q8 = Some(VectorNode::quantize_q8_static(&node.vector));
                    }
                    if quant == VectorQuantization::Binary {
                        node.vector_bin = Some(VectorNode::quantize_binary_static(&node.vector));
                    }
                    if attrs.is_some() {
                        node.attributes = attrs;
                    }
                }
                entry.bump_version();
                return Ok(false);
            }

            // New element - HNSW insertion
            let level = vs.random_level();
            let mut new_node = VectorNode::new(element.clone(), vector.clone(), level, quant);
            if attrs.is_some() {
                new_node.attributes = attrs;
            }

            let new_idx = vs.nodes.len() as u32;

            // If first node, just add it
            if vs.nodes.is_empty() {
                vs.nodes.push(new_node);
                vs.element_index.insert(element, new_idx);
                vs.entry_point = Some(new_idx);
                vs.max_level = level;
                entry.bump_version();
                return Ok(true);
            }

            // Add node first
            vs.nodes.push(new_node);
            vs.element_index.insert(element, new_idx);

            let ef = ef_construction.unwrap_or(vs.ef_construction);
            let entry_point = vs.entry_point.unwrap();

            // Clone the query vector for searching
            let query = vector;

            // Search from top to layer level+1
            let mut curr_ep = entry_point;

            for lc in (level as usize + 1..=vs.max_level as usize).rev() {
                curr_ep = search_layer_single_impl(vs, &query, curr_ep, lc);
            }

            // Search and connect at each layer from level down to 0
            for lc in (0..=level as usize).rev() {
                let m_max = if lc == 0 { vs.m0 } else { vs.m };
                let neighbors = search_layer_impl(vs, &query, curr_ep, ef, lc);

                // Collect neighbor indices
                let selected: Vec<u32> = neighbors.iter().take(m_max).map(|c| c.idx).collect();

                // Connect new node to neighbors
                if lc < vs.nodes[new_idx as usize].connections.len() {
                    vs.nodes[new_idx as usize].connections[lc] = selected.clone();
                }

                // Connect neighbors back to new node (bidirectional)
                // Collect info needed for pruning first
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

                // Prune over-connected neighbors
                for (neighbor_idx, layer) in prune_needed {
                    if let Some(neighbor) = vs.nodes.get(neighbor_idx as usize) {
                        let nv = neighbor.vector.clone();
                        let conns = neighbor.connections[layer].clone();

                        let mut scored: Vec<_> = conns
                            .iter()
                            .filter_map(|&idx| {
                                vs.nodes
                                    .get(idx as usize)
                                    .map(|n| (cosine_distance(&nv, &n.vector), idx))
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

            // Update entry point if new node has higher level
            if level > vs.max_level {
                vs.entry_point = Some(new_idx);
                vs.max_level = level;
            }

            entry.bump_version();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// VSIM - Similarity search
    pub fn vsim(
        &self,
        key: &[u8],
        query: &[f32],
        count: usize,
        ef: Option<usize>,
        with_scores: bool,
        with_attribs: bool,
        filter: Option<&str>,
    ) -> Vec<(Bytes, Option<f32>, Option<String>)> {
        let Some(entry_ref) = self.data.get(key) else {
            return vec![];
        };

        let entry = entry_ref.value();
        let DataType::VectorSet(ref vs) = entry.data else {
            return vec![];
        };

        if vs.is_empty() {
            return vec![];
        }

        let Some(ep) = vs.entry_point else {
            return vec![];
        };

        // Validate query dimension
        if query.len() != vs.dim {
            return vec![];
        }

        let search_ef = ef.unwrap_or(count.max(vs.ef_construction));

        // Search from top layer
        let mut curr_ep = ep;

        // Greedy search on upper layers
        for lc in (1..=vs.max_level as usize).rev() {
            curr_ep = search_layer_single_impl(vs, query, curr_ep, lc);
        }

        // Beam search on layer 0
        let candidates = search_layer_impl(vs, query, curr_ep, search_ef, 0);

        // Apply filter and collect results
        let mut results = Vec::with_capacity(count);

        for cand in candidates {
            if let Some(node) = vs.nodes.get(cand.idx as usize) {
                // Apply filter if present
                if let Some(filter_expr) = filter
                    && !eval_filter(node, filter_expr)
                {
                    continue;
                }

                let score = if with_scores {
                    Some(1.0 - cand.distance) // Convert distance back to similarity
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

        results
    }

    /// VREM - Remove element from vector set
    pub fn vrem(&self, key: &[u8], element: &[u8]) -> bool {
        let Some(mut entry_ref) = self.data.get_mut(key) else {
            return false;
        };

        let entry = entry_ref.value_mut();
        let DataType::VectorSet(ref mut vs) = entry.data else {
            return false;
        };

        let Some(&idx) = vs.element_index.get(element) else {
            return false;
        };

        // Remove from index
        vs.element_index.remove(element);

        // Get node's connections to repair neighbors
        let connections = match vs.nodes.get(idx as usize) {
            Some(node) => node.connections.clone(),
            None => return true,
        };

        // Remove references to this node from neighbors
        for (layer, neighbors) in connections.iter().enumerate() {
            for &neighbor_idx in neighbors {
                if let Some(neighbor) = vs.nodes.get_mut(neighbor_idx as usize)
                    && layer < neighbor.connections.len()
                {
                    neighbor.connections[layer].retain(|&n| n != idx);
                }
            }
        }

        // Update entry point if needed
        if vs.entry_point == Some(idx) {
            // Find new entry point (node with highest level)
            vs.entry_point = vs
                .nodes
                .iter()
                .enumerate()
                .filter(|(i, _)| {
                    *i as u32 != idx && vs.element_index.values().any(|&v| v == *i as u32)
                })
                .max_by_key(|(_, n)| n.level)
                .map(|(i, _)| i as u32);

            if let Some(ep) = vs.entry_point {
                vs.max_level = vs.nodes.get(ep as usize).map(|n| n.level).unwrap_or(0);
            } else {
                vs.max_level = 0;
            }
        }

        entry.bump_version();
        true
    }

    /// VCARD - Get number of elements
    pub fn vcard(&self, key: &[u8]) -> i64 {
        match self.data.get(key) {
            Some(entry_ref) => match entry_ref.value().data.as_vectorset() {
                Some(vs) => vs.element_index.len() as i64,
                None => 0,
            },
            None => 0,
        }
    }

    /// VDIM - Get vector dimension
    pub fn vdim(&self, key: &[u8]) -> Option<usize> {
        match self.data.get(key) {
            Some(entry_ref) => entry_ref.value().data.as_vectorset().map(|vs| vs.dim),
            None => None,
        }
    }

    /// VEMB - Get vector for element
    pub fn vemb(&self, key: &[u8], element: &[u8], _raw: bool) -> Option<Vec<f32>> {
        let entry_ref = self.data.get(key)?;
        let vs = entry_ref.value().data.as_vectorset()?;
        let &idx = vs.element_index.get(element)?;
        let node = vs.nodes.get(idx as usize)?;
        Some(node.vector.clone())
    }

    /// VGETATTR - Get JSON attributes
    pub fn vgetattr(&self, key: &[u8], element: &[u8]) -> Option<String> {
        let entry_ref = self.data.get(key)?;
        let vs = entry_ref.value().data.as_vectorset()?;
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
        let Some(mut entry_ref) = self.data.get_mut(key) else {
            return false;
        };

        let entry = entry_ref.value_mut();
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

    /// VISMEMBER - Check if element exists
    pub fn vismember(&self, key: &[u8], element: &[u8]) -> bool {
        match self.data.get(key) {
            Some(entry_ref) => match entry_ref.value().data.as_vectorset() {
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
        let entry_ref = self.data.get(key)?;
        let vs = entry_ref.value().data.as_vectorset()?;
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
                            Some(cosine_similarity(&node_vector, &neighbor.vector))
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
        let Some(entry_ref) = self.data.get(key) else {
            return vec![];
        };
        let Some(vs) = entry_ref.value().data.as_vectorset() else {
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

    /// VRANGE - Lexicographical range
    pub fn vrange(&self, key: &[u8], start: &[u8], end: &[u8], count: Option<usize>) -> Vec<Bytes> {
        match self.data.get(key) {
            Some(entry_ref) => match entry_ref.value().data.as_vectorset() {
                Some(vs) => vs.range(start, end, count),
                None => vec![],
            },
            None => vec![],
        }
    }

    /// VINFO - Get vector set info
    pub fn vinfo(&self, key: &[u8]) -> Option<Vec<(String, String)>> {
        let entry_ref = self.data.get(key)?;
        let vs = entry_ref.value().data.as_vectorset()?;

        Some(vec![
            ("elements".into(), vs.element_index.len().to_string()),
            ("dimension".into(), vs.dim.to_string()),
            ("m".into(), vs.m.to_string()),
            ("ef_construction".into(), vs.ef_construction.to_string()),
            ("max_level".into(), vs.max_level.to_string()),
            ("quantization".into(), vs.quant.as_str().to_string()),
        ])
    }
}

/// Simple filter evaluation (supports basic JSON path checks)
fn eval_filter(node: &VectorNode, filter: &str) -> bool {
    // Basic implementation: check if filter matches attributes
    // Format: "field=value" or "field!=value"
    if let Some(attrs) = &node.attributes
        && let Some((field, value)) = filter.split_once('=')
    {
        let (field, negate) = if let Some(stripped) = field.strip_suffix('!') {
            (stripped, true)
        } else {
            (field, false)
        };

        // Try to access field from attributes
        // sonic_rs Value indexing with string key
        let attr_val = &attrs[field];
        if !attr_val.is_null() {
            let matches = attr_val.as_str().map(|s| s == value).unwrap_or(false);
            return if negate { !matches } else { matches };
        }
        return negate;
    }
    true // No attrs = no filter match, but pass through
}

// Helper methods for VectorNode
impl VectorNode {
    /// Quantize to 8-bit signed integers (static version)
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

    /// Quantize to binary (sign bits) - static version
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
