use crate::storage::dashtable::{DashTable, Entry, calculate_hash};
use bytes::Bytes;
use dashmap::DashMap;
use roaring::RoaringBitmap;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::RwLock;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::error::{Error, Result};
use crate::storage::Store;

// ==================== Search Index Types ====================

/// Field types for indexing
#[derive(Debug, Clone)]
pub enum FieldType {
    Text {
        weight: f64,
        nostem: bool,
        phonetic: Option<String>,
        sortable: bool,
        noindex: bool,
    },
    Tag {
        separator: char,
        casesensitive: bool,
        sortable: bool,
        noindex: bool,
    },
    Numeric {
        sortable: bool,
        noindex: bool,
    },
    Geo {
        sortable: bool,
        noindex: bool,
    },
    Vector {
        algorithm: VectorAlgorithm,
        dim: usize,
        distance_metric: DistanceMetric,
        initial_cap: usize,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorAlgorithm {
    Flat,
    Hnsw,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceMetric {
    L2,
    IP, // Inner Product
    Cosine,
}

impl DistanceMetric {
    pub fn from_bytes(b: &[u8]) -> Option<Self> {
        match b.to_ascii_uppercase().as_slice() {
            b"L2" => Some(Self::L2),
            b"IP" => Some(Self::IP),
            b"COSINE" => Some(Self::Cosine),
            _ => None,
        }
    }
}

/// Schema field definition
#[derive(Debug, Clone)]
pub struct SchemaField {
    pub name: Bytes,
    pub alias: Option<Bytes>,
    pub field_type: FieldType,
}

/// Inverted index posting with RoaringBitmap
#[derive(Debug, Default, Clone)]
struct PostingList {
    /// doc_ids as compressed RoaringBitmap
    docs: RoaringBitmap,
    /// Term frequencies: doc_id -> tf (stored separately for scoring)
    tfs: HashMap<u32, u32>,
}

/// Search index
#[derive(Debug)]
pub struct SearchIndex {
    pub name: Bytes,
    pub key_prefixes: Vec<Bytes>,
    pub on_hash: bool, // true = HASH, false = JSON
    pub schema: Vec<SchemaField>,
    pub filter: Option<String>,
    pub language: String,
    pub stopwords: HashSet<String>,

    /// Inverted index: field_name:term -> PostingList
    inverted_index: DashMap<Bytes, PostingList>,
    /// Numeric index: field_name -> BTreeMap<value, doc_ids>
    numeric_index: DashMap<Bytes, BTreeMap<i64, HashSet<u32>>>,
    /// Tag index: field_name:tag -> doc_ids
    tag_index: DashMap<Bytes, HashSet<u32>>,
    /// Geo index: field_name -> geohash prefix tree (simplified)
    geo_index: DashMap<Bytes, HashMap<String, HashSet<u32>>>,
    /// Vector index: field_name -> (vectors, doc_ids)
    vector_index: DashMap<Bytes, VectorIndex>,

    /// Reverse index: doc_id -> term keys (for efficient removal)
    doc_terms: DashMap<u32, Vec<Bytes>>,
    /// Document metadata
    doc_keys: RwLock<Vec<Bytes>>,
    doc_id_map: DashMap<Bytes, u32>,
    next_doc_id: AtomicU32,

    /// Total docs indexed
    pub num_docs: AtomicU32,
}

/// Vector index for similarity search
#[derive(Debug, Default)]
pub struct VectorIndex {
    /// Stored vectors: doc_id -> vector
    vectors: HashMap<u32, Vec<f32>>,
    dim: usize,
}

impl VectorIndex {
    pub fn new(dim: usize) -> Self {
        Self {
            vectors: HashMap::new(),
            dim,
        }
    }

    pub fn add(&mut self, doc_id: u32, vector: Vec<f32>) {
        if vector.len() == self.dim {
            self.vectors.insert(doc_id, vector);
        }
    }

    /// KNN search using brute-force with optional SIMD
    #[inline]
    pub fn knn(&self, query: &[f32], k: usize, metric: DistanceMetric) -> Vec<(u32, f32)> {
        if query.len() != self.dim {
            return Vec::new();
        }

        let mut results: Vec<(u32, f32)> = self
            .vectors
            .iter()
            .map(|(&doc_id, vec)| {
                let dist = match metric {
                    DistanceMetric::L2 => l2_distance(query, vec),
                    DistanceMetric::IP => -inner_product(query, vec), // Negate for min-heap
                    DistanceMetric::Cosine => 1.0 - cosine_similarity(query, vec),
                };
                (doc_id, dist)
            })
            .collect();

        // Partial sort for top-k
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(k);
        results
    }

    /// Range search
    #[inline]
    pub fn range(&self, query: &[f32], radius: f32, metric: DistanceMetric) -> Vec<(u32, f32)> {
        if query.len() != self.dim {
            return Vec::new();
        }

        self.vectors
            .iter()
            .filter_map(|(&doc_id, vec)| {
                let dist = match metric {
                    DistanceMetric::L2 => l2_distance(query, vec),
                    DistanceMetric::IP => -inner_product(query, vec),
                    DistanceMetric::Cosine => 1.0 - cosine_similarity(query, vec),
                };
                if dist <= radius {
                    Some((doc_id, dist))
                } else {
                    None
                }
            })
            .collect()
    }
}

// ==================== Multi-Architecture Optimized Distance Functions ====================
// These use portable patterns that LLVM auto-vectorizes for any architecture (x86 AVX, ARM NEON, etc.)

/// L2 (Euclidean) distance - optimized for auto-vectorization
#[inline]
fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());

    // Process in chunks of 8 for optimal SIMD utilization
    let chunks = a.len() / 8;
    let mut sum0 = 0.0f32;
    let mut sum1 = 0.0f32;
    let mut sum2 = 0.0f32;
    let mut sum3 = 0.0f32;

    // Unrolled loop for better auto-vectorization
    for i in 0..chunks {
        let base = i * 8;
        let d0 = a[base] - b[base];
        let d1 = a[base + 1] - b[base + 1];
        let d2 = a[base + 2] - b[base + 2];
        let d3 = a[base + 3] - b[base + 3];
        let d4 = a[base + 4] - b[base + 4];
        let d5 = a[base + 5] - b[base + 5];
        let d6 = a[base + 6] - b[base + 6];
        let d7 = a[base + 7] - b[base + 7];
        sum0 += d0 * d0 + d1 * d1;
        sum1 += d2 * d2 + d3 * d3;
        sum2 += d4 * d4 + d5 * d5;
        sum3 += d6 * d6 + d7 * d7;
    }

    let mut sum = sum0 + sum1 + sum2 + sum3;

    // Handle remainder
    for i in (chunks * 8)..a.len() {
        let d = a[i] - b[i];
        sum += d * d;
    }

    sum.sqrt()
}

/// Inner product (dot product) - optimized for auto-vectorization
#[inline]
fn inner_product(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());

    let chunks = a.len() / 8;
    let mut sum0 = 0.0f32;
    let mut sum1 = 0.0f32;
    let mut sum2 = 0.0f32;
    let mut sum3 = 0.0f32;

    for i in 0..chunks {
        let base = i * 8;
        sum0 += a[base] * b[base] + a[base + 1] * b[base + 1];
        sum1 += a[base + 2] * b[base + 2] + a[base + 3] * b[base + 3];
        sum2 += a[base + 4] * b[base + 4] + a[base + 5] * b[base + 5];
        sum3 += a[base + 6] * b[base + 6] + a[base + 7] * b[base + 7];
    }

    let mut sum = sum0 + sum1 + sum2 + sum3;

    for i in (chunks * 8)..a.len() {
        sum += a[i] * b[i];
    }

    sum
}

/// Cosine similarity
#[inline]
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot = inner_product(a, b);
    let norm_a = inner_product(a, a).sqrt();
    let norm_b = inner_product(b, b).sqrt();
    let denom = (norm_a * norm_b).max(1e-10);
    dot / denom
}

impl SearchIndex {
    pub fn new(name: Bytes, on_hash: bool) -> Self {
        Self {
            name,
            key_prefixes: Vec::new(),
            on_hash,
            schema: Vec::new(),
            filter: None,
            language: "english".to_string(),
            stopwords: default_stopwords(),
            inverted_index: DashMap::new(),
            numeric_index: DashMap::new(),
            tag_index: DashMap::new(),
            geo_index: DashMap::new(),
            vector_index: DashMap::new(),
            doc_terms: DashMap::new(),
            doc_keys: RwLock::new(Vec::new()),
            doc_id_map: DashMap::new(),
            next_doc_id: AtomicU32::new(0),
            num_docs: AtomicU32::new(0),
        }
    }

    /// Get or assign document ID
    fn get_or_create_doc_id(&self, key: &Bytes) -> u32 {
        if let Some(id) = self.doc_id_map.get(key) {
            return *id;
        }

        let id = self.next_doc_id.fetch_add(1, Ordering::Relaxed);
        self.doc_id_map.insert(key.clone(), id);

        let mut keys = self.doc_keys.write().unwrap();
        if keys.len() <= id as usize {
            keys.resize(id as usize + 1, Bytes::new());
        }
        keys[id as usize] = key.clone();

        self.num_docs.fetch_add(1, Ordering::Relaxed);
        id
    }

    /// Index a document (HASH or JSON)
    pub fn index_document(&self, key: Bytes, fields: &HashMap<Bytes, Bytes>) {
        let doc_id = self.get_or_create_doc_id(&key);

        for field_def in &self.schema {
            let field_name = field_def.alias.as_ref().unwrap_or(&field_def.name);

            if let Some(value) = fields.get(&field_def.name) {
                match &field_def.field_type {
                    FieldType::Text { noindex, .. } if !noindex => {
                        self.index_text_field(field_name, value, doc_id);
                    }
                    FieldType::Tag {
                        separator,
                        casesensitive,
                        noindex,
                        ..
                    } if !noindex => {
                        self.index_tag_field(field_name, value, doc_id, *separator, *casesensitive);
                    }
                    FieldType::Numeric { noindex, .. } if !noindex => {
                        self.index_numeric_field(field_name, value, doc_id);
                    }
                    FieldType::Geo { noindex, .. } if !noindex => {
                        self.index_geo_field(field_name, value, doc_id);
                    }
                    FieldType::Vector { dim, .. } => {
                        self.index_vector_field(field_name, value, doc_id, *dim);
                    }
                    _ => {}
                }
            }
        }
    }

    fn index_text_field(&self, field_name: &Bytes, value: &Bytes, doc_id: u32) {
        let text = match std::str::from_utf8(value) {
            Ok(s) => s.to_lowercase(),
            Err(_) => return,
        };

        // Simple tokenization
        for word in text.split(|c: char| !c.is_alphanumeric()) {
            let word = word.trim();
            if word.is_empty() || self.stopwords.contains(word) {
                continue;
            }

            let term_key = format!("{}:{}", String::from_utf8_lossy(field_name), word);
            let term_bytes = Bytes::from(term_key);

            // Track term key for this document (for efficient removal)
            self.doc_terms
                .entry(doc_id)
                .or_default()
                .push(term_bytes.clone());

            let mut posting = self.inverted_index.entry(term_bytes).or_default();
            posting.docs.insert(doc_id);
            *posting.tfs.entry(doc_id).or_insert(0) += 1;
        }
    }

    fn index_tag_field(
        &self,
        field_name: &Bytes,
        value: &Bytes,
        doc_id: u32,
        separator: char,
        casesensitive: bool,
    ) {
        let text = match std::str::from_utf8(value) {
            Ok(s) => s,
            Err(_) => return,
        };

        for tag in text.split(separator) {
            let tag = tag.trim();
            if tag.is_empty() {
                continue;
            }

            let tag_normalized = if casesensitive {
                tag.to_string()
            } else {
                tag.to_lowercase()
            };

            let tag_key = format!("{}:{}", String::from_utf8_lossy(field_name), tag_normalized);
            let tag_bytes = Bytes::from(tag_key);

            let mut set = self.tag_index.entry(tag_bytes).or_default();
            set.insert(doc_id);
        }
    }

    fn index_numeric_field(&self, field_name: &Bytes, value: &Bytes, doc_id: u32) {
        let num: i64 = match std::str::from_utf8(value) {
            Ok(s) => match s.parse::<f64>() {
                Ok(f) => (f * 1000.0) as i64, // Store as fixed-point for range queries
                Err(_) => return,
            },
            Err(_) => return,
        };

        let mut tree = self.numeric_index.entry(field_name.clone()).or_default();
        tree.entry(num).or_default().insert(doc_id);
    }

    fn index_geo_field(&self, field_name: &Bytes, value: &Bytes, doc_id: u32) {
        // Parse "lon,lat" format
        let text = match std::str::from_utf8(value) {
            Ok(s) => s,
            Err(_) => return,
        };

        let parts: Vec<&str> = text.split(',').collect();
        if parts.len() != 2 {
            return;
        }

        let lon: f64 = match parts[0].trim().parse() {
            Ok(v) => v,
            Err(_) => return,
        };
        let lat: f64 = match parts[1].trim().parse() {
            Ok(v) => v,
            Err(_) => return,
        };

        // Simple geohash (first 6 chars for ~1km precision)
        let geohash = encode_geohash(lat, lon, 6);

        let mut geo_map = self.geo_index.entry(field_name.clone()).or_default();
        geo_map.entry(geohash).or_default().insert(doc_id);
    }

    fn index_vector_field(&self, field_name: &Bytes, value: &Bytes, doc_id: u32, dim: usize) {
        // Parse vector from binary blob or JSON array
        let vector = parse_vector(value, dim);
        if let Some(vec) = vector {
            let mut idx = self
                .vector_index
                .entry(field_name.clone())
                .or_insert_with(|| VectorIndex::new(dim));
            idx.add(doc_id, vec);
        }
    }

    /// Search the index with a query
    pub fn search(&self, query: &str, offset: usize, limit: usize) -> Vec<(Bytes, f64)> {
        let tokens = tokenize_query(query.to_lowercase().as_str(), &self.stopwords);

        if tokens.is_empty() {
            // Return all docs
            let keys = self.doc_keys.read().unwrap();
            return keys
                .iter()
                .skip(offset)
                .take(limit)
                .filter(|k| !k.is_empty())
                .map(|k| (k.clone(), 1.0))
                .collect();
        }

        // Simple intersection of posting lists
        let mut doc_scores: HashMap<u32, f64> = HashMap::new();

        for token in &tokens {
            // Search across all text fields
            for field in &self.schema {
                if matches!(field.field_type, FieldType::Text { .. }) {
                    let field_name = field.alias.as_ref().unwrap_or(&field.name);
                    let term_key = format!("{}:{}", String::from_utf8_lossy(field_name), token);

                    if let Some(posting) = self.inverted_index.get(&Bytes::from(term_key)) {
                        for doc_id in posting.docs.iter() {
                            let tf = posting.tfs.get(&doc_id).copied().unwrap_or(1);
                            *doc_scores.entry(doc_id).or_insert(0.0) += tf as f64;
                        }
                    }
                }
            }
        }

        // Sort by score descending
        let mut results: Vec<(u32, f64)> = doc_scores.into_iter().collect();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Return doc keys
        let keys = self.doc_keys.read().unwrap();
        results
            .into_iter()
            .skip(offset)
            .take(limit)
            .filter_map(|(doc_id, score)| keys.get(doc_id as usize).map(|k| (k.clone(), score)))
            .collect()
    }

    /// Get document key by internal ID
    pub fn get_doc_key(&self, doc_id: u32) -> Option<Bytes> {
        let keys = self.doc_keys.read().unwrap();
        keys.get(doc_id as usize).cloned()
    }

    /// Remove a document from the index with full cleanup
    pub fn remove_document(&self, key: &Bytes) {
        if let Some((_, doc_id)) = self.doc_id_map.remove(key) {
            // Clean up inverted index entries using tracked term keys
            if let Some((_, term_keys)) = self.doc_terms.remove(&doc_id) {
                for term_key in term_keys {
                    if let Some(mut posting) = self.inverted_index.get_mut(&term_key) {
                        posting.docs.remove(doc_id);
                        posting.tfs.remove(&doc_id);
                    }
                }
            }

            // Clean up tag index
            for mut entry in self.tag_index.iter_mut() {
                entry.value_mut().remove(&doc_id);
            }

            // Clean up numeric index
            for mut field_entry in self.numeric_index.iter_mut() {
                for doc_set in field_entry.value_mut().values_mut() {
                    doc_set.remove(&doc_id);
                }
            }

            // Clean up vector index
            for mut entry in self.vector_index.iter_mut() {
                entry.value_mut().vectors.remove(&doc_id);
            }

            // Clear the doc key slot
            if let Ok(mut keys) = self.doc_keys.write()
                && let Some(slot) = keys.get_mut(doc_id as usize)
            {
                *slot = Bytes::new();
            }

            self.num_docs.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Get field by name
    pub fn get_field(&self, name: &[u8]) -> Option<&SchemaField> {
        self.schema
            .iter()
            .find(|f| f.name.as_ref() == name || f.alias.as_deref() == Some(name))
    }
}

// ==================== Helper Functions ====================

fn default_stopwords() -> HashSet<String> {
    [
        "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is",
        "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there",
        "these", "they", "this", "to", "was", "will", "with",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

fn tokenize_query(query: &str, stopwords: &HashSet<String>) -> Vec<String> {
    query
        .split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty() && !stopwords.contains(*s))
        .map(|s| s.to_string())
        .collect()
}

fn parse_vector(value: &Bytes, dim: usize) -> Option<Vec<f32>> {
    // Try parsing as comma-separated floats first
    if let Ok(s) = std::str::from_utf8(value) {
        let parts: Vec<f32> = s.split(',').filter_map(|p| p.trim().parse().ok()).collect();
        if parts.len() == dim {
            return Some(parts);
        }
    }

    // Try as binary f32 array
    if value.len() == dim * 4 {
        let mut vec = Vec::with_capacity(dim);
        for i in 0..dim {
            let bytes: [u8; 4] = [
                value[i * 4],
                value[i * 4 + 1],
                value[i * 4 + 2],
                value[i * 4 + 3],
            ];
            vec.push(f32::from_le_bytes(bytes));
        }
        return Some(vec);
    }

    None
}

/// Simple geohash encoding
fn encode_geohash(lat: f64, lon: f64, precision: usize) -> String {
    const BASE32: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";
    let mut geohash = String::with_capacity(precision);

    let mut lat_range = (-90.0, 90.0);
    let mut lon_range = (-180.0, 180.0);
    let mut is_lon = true;
    let mut bit = 0u8;
    let mut ch = 0u8;

    for _ in 0..(precision * 5) {
        if is_lon {
            let mid = (lon_range.0 + lon_range.1) / 2.0;
            if lon >= mid {
                ch |= 1 << (4 - bit);
                lon_range.0 = mid;
            } else {
                lon_range.1 = mid;
            }
        } else {
            let mid = (lat_range.0 + lat_range.1) / 2.0;
            if lat >= mid {
                ch |= 1 << (4 - bit);
                lat_range.0 = mid;
            } else {
                lat_range.1 = mid;
            }
        }
        is_lon = !is_lon;
        bit += 1;

        if bit == 5 {
            geohash.push(BASE32[ch as usize] as char);
            ch = 0;
            bit = 0;
            if geohash.len() >= precision {
                break;
            }
        }
    }

    geohash
}

// ==================== Store Integration ====================

/// Search indexes stored in Store
impl Store {
    /// Get search indexes map from Store field
    #[inline]
    fn search_indexes(&self) -> &DashTable<(Bytes, SearchIndex)> {
        &self.search_indexes
    }

    /// Create a new search index
    pub fn ft_create(
        &self,
        name: Bytes,
        on_hash: bool,
        prefixes: Vec<Bytes>,
        schema: Vec<SchemaField>,
    ) -> Result<()> {
        let indexes = self.search_indexes();

        let h = calculate_hash(&name);
        if indexes.get(h, |kv| kv.0 == name).is_some() {
            return Err(Error::Custom("Index already exists".into()));
        }

        let mut index = SearchIndex::new(name.clone(), on_hash);
        index.key_prefixes = prefixes;
        index.schema = schema;

        let h = calculate_hash(&name);
        indexes.insert(
            h,
            (name.clone(), index),
            |kv| kv.0 == name,
            |kv| calculate_hash(&kv.0),
        );
        Ok(())
    }

    /// Alter an existing search index
    pub fn ft_alter(
        &self,
        index_name: &[u8],
        _skip_initial_scan: bool,
        schema_fields: Vec<SchemaField>,
    ) -> Result<()> {
        let indexes = self.search_indexes();
        let h = calculate_hash(index_name);

        match indexes.entry(h, |kv| kv.0 == index_name, |kv| calculate_hash(&kv.0)) {
            Entry::Occupied(mut entry) => {
                let index = &mut entry.get_mut().1;

                for new_field in schema_fields {
                    // Check name collision
                    if index.schema.iter().any(|f| f.name == new_field.name) {
                        return Err(Error::Custom(
                            format!(
                                "Duplicate field name: {}",
                                String::from_utf8_lossy(&new_field.name)
                            )
                            .into(),
                        ));
                    }
                    // Check alias collision
                    if let Some(ref alias) = new_field.alias {
                        if index.schema.iter().any(|f| f.alias.as_ref() == Some(alias)) {
                            return Err(Error::Custom(
                                format!(
                                    "Duplicate field alias: {}",
                                    String::from_utf8_lossy(alias)
                                )
                                .into(),
                            ));
                        }
                    }

                    index.schema.push(new_field);
                }
                Ok(())
            }
            Entry::Vacant(_) => Err(Error::Custom("Unknown Index name".into())),
        }
    }

    /// Drop a search index
    pub fn ft_dropindex(&self, name: &[u8], delete_docs: bool) -> Result<()> {
        let indexes = self.search_indexes();

        let h = calculate_hash(name);
        let removed = indexes.remove(h, |kv| kv.0 == name);
        if removed.is_none() {
            return Err(Error::Custom("Unknown Index name".into()));
        }

        // Delete underlying HASH/JSON keys if DD option is set
        if delete_docs && let Some((_, index)) = removed {
            let doc_keys = index.doc_keys.read().unwrap();
            for key in doc_keys.iter() {
                if !key.is_empty() {
                    // Delete the underlying key from storage
                    self.del(key.as_ref());
                }
            }
        }

        Ok(())
    }

    /// List all search indexes
    pub fn ft_list(&self) -> Vec<Bytes> {
        let indexes = self.search_indexes();
        indexes.iter().map(|r| r.0.clone()).collect()
    }

    /// Get index info
    pub fn ft_info(&self, name: &[u8]) -> Result<SearchIndex> {
        let indexes = self.search_indexes();
        let h = calculate_hash(name);

        indexes
            .get(h, |kv| kv.0 == name)
            .map(|r| {
                let r = &r.1;
                // Return a minimal clone for info purposes
                SearchIndex {
                    name: r.name.clone(),
                    key_prefixes: r.key_prefixes.clone(),
                    on_hash: r.on_hash,
                    schema: r.schema.clone(),
                    filter: r.filter.clone(),
                    language: r.language.clone(),
                    stopwords: r.stopwords.clone(),
                    inverted_index: DashMap::new(),
                    numeric_index: DashMap::new(),
                    tag_index: DashMap::new(),
                    geo_index: DashMap::new(),
                    vector_index: DashMap::new(),
                    doc_terms: DashMap::new(),
                    doc_keys: RwLock::new(Vec::new()),
                    doc_id_map: DashMap::new(),
                    next_doc_id: AtomicU32::new(r.next_doc_id.load(Ordering::Relaxed)),
                    num_docs: AtomicU32::new(r.num_docs.load(Ordering::Relaxed)),
                }
            })
            .ok_or(Error::Custom("Unknown Index name".into()))
    }

    /// Search an index
    pub fn ft_search(
        &self,
        index_name: &[u8],
        query: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<(Bytes, f64)>> {
        let indexes = self.search_indexes();
        let h = calculate_hash(index_name);

        indexes
            .get(h, |kv| kv.0 == index_name)
            .map(|idx| idx.1.search(query, offset, limit))
            .ok_or(Error::Custom("Unknown Index name".into()))
    }

    /// Index a document manually (called when HSET/JSON.SET happens)
    pub fn ft_index_document(&self, key: &Bytes, fields: &HashMap<Bytes, Bytes>) {
        let indexes = self.search_indexes();

        for entry in indexes.iter() {
            let index = &entry.1;
            // Check if key matches any prefix
            let matches_prefix = index.key_prefixes.is_empty()
                || index
                    .key_prefixes
                    .iter()
                    .any(|p| key.starts_with(p.as_ref()));

            if matches_prefix {
                index.index_document(key.clone(), fields);
            }
        }
    }

    /// Get alias map from Store field
    #[inline]
    fn search_aliases(&self) -> &DashTable<(Bytes, Bytes)> {
        &self.search_aliases
    }

    /// Add alias for an index
    pub fn ft_aliasadd(&self, alias: Bytes, index_name: &[u8]) -> Result<()> {
        let indexes = self.search_indexes();
        let aliases = self.search_aliases();

        let h_idx = calculate_hash(index_name);
        if indexes.get(h_idx, |kv| kv.0 == index_name).is_none() {
            return Err(Error::Custom("Unknown Index name".into()));
        }

        let h_alias = calculate_hash(&alias);
        if aliases.get(h_alias, |kv| kv.0 == alias).is_some() {
            return Err(Error::Custom("Alias already exists".into()));
        }

        aliases.insert(
            h_alias,
            (alias.clone(), Bytes::copy_from_slice(index_name)),
            |kv| kv.0 == alias,
            |kv| calculate_hash(&kv.0),
        );
        Ok(())
    }

    /// Delete alias
    pub fn ft_aliasdel(&self, alias: &[u8]) -> Result<()> {
        let aliases = self.search_aliases();
        let h = calculate_hash(alias);
        if aliases.remove(h, |kv| kv.0 == alias).is_none() {
            return Err(Error::Custom("Alias does not exist".into()));
        }
        Ok(())
    }

    /// Update alias
    pub fn ft_aliasupdate(&self, alias: Bytes, index_name: &[u8]) -> Result<()> {
        let indexes = self.search_indexes();
        let aliases = self.search_aliases();

        let h_idx = calculate_hash(index_name);
        if indexes.get(h_idx, |kv| kv.0 == index_name).is_none() {
            return Err(Error::Custom("Unknown Index name".into()));
        }

        let h_alias = calculate_hash(&alias);
        aliases.insert(
            h_alias,
            (alias.clone(), Bytes::copy_from_slice(index_name)),
            |kv| kv.0 == alias,
            |kv| calculate_hash(&kv.0),
        );
        Ok(())
    }

    /// Resolve alias to index name
    pub fn ft_resolve_alias(&self, name: &[u8]) -> Option<Bytes> {
        let aliases = self.search_aliases();
        let h = calculate_hash(name);
        aliases.get(h, |kv| kv.0 == name).map(|r| r.1.clone())
    }

    /// Get unique tag values for a field in an index
    pub fn ft_tagvals(&self, index_name: &[u8], field_name: &[u8]) -> Result<Vec<Bytes>> {
        let indexes = self.search_indexes();
        let h = calculate_hash(index_name);

        let index_ref = indexes
            .get(h, |kv| kv.0 == index_name)
            .ok_or_else(|| Error::Custom("Unknown Index name".into()))?;
        let index = &index_ref.1;

        // Find the field to verify it's a TAG field
        let field = index
            .schema
            .iter()
            .find(|f| f.name.as_ref() == field_name || f.alias.as_deref() == Some(field_name));

        let field_def = match field {
            Some(f) => f,
            None => return Err(Error::Custom("Unknown field".into())),
        };

        if !matches!(field_def.field_type, FieldType::Tag { .. }) {
            return Err(Error::Custom("Field is not a TAG field".into()));
        }

        // Get the field key used in tag_index
        let field_key = field_def.alias.as_ref().unwrap_or(&field_def.name);
        let prefix = format!("{}:", String::from_utf8_lossy(field_key));

        // Collect unique tag values from the tag_index
        let mut tags = Vec::new();
        for entry in index.tag_index.iter() {
            let key_str = String::from_utf8_lossy(entry.key());
            if key_str.starts_with(&prefix) && !entry.value().is_empty() {
                // Extract the tag value (part after the prefix)
                let tag_value = &key_str[prefix.len()..];
                tags.push(Bytes::from(tag_value.to_string()));
            }
        }

        tags.sort();
        tags.dedup();
        Ok(tags)
    }
}
