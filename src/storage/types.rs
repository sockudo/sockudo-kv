use bytes::Bytes;
use dashmap::{DashMap, DashSet};
use sonic_rs::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use super::value::now_ms;

/// All Redis data types
#[derive(Debug)]
pub enum DataType {
    /// String - raw bytes
    String(Bytes),
    /// List - doubly-linked list (VecDeque for O(1) push/pop both ends)
    List(VecDeque<Bytes>),
    /// Set - unordered unique strings
    Set(DashSet<Bytes>),
    /// Hash - field-value pairs
    Hash(DashMap<Bytes, Bytes>),
    /// Sorted Set - unique strings ordered by score
    SortedSet(SortedSetData),
    /// Stream - append-only log
    Stream(StreamData),
    /// HyperLogLog - cardinality estimation
    HyperLogLog(HyperLogLogData),
    /// JSON - sonic-rs high-performance JSON value
    Json(Box<JsonValue>),
    /// TimeSeries - time-ordered samples with labels
    TimeSeries(Box<TimeSeriesData>),
    /// VectorSet - HNSW-based vector similarity search
    VectorSet(Box<VectorSetData>),
}

impl DataType {
    #[inline]
    pub fn type_name(&self) -> &'static str {
        match self {
            DataType::String(_) => "string",
            DataType::List(_) => "list",
            DataType::Set(_) => "set",
            DataType::Hash(_) => "hash",
            DataType::SortedSet(_) => "zset",
            DataType::Stream(_) => "stream",
            DataType::HyperLogLog(_) => "string", // HLL is stored as string in Redis
            DataType::Json(_) => "ReJSON-RL",
            DataType::TimeSeries(_) => "TSDB-TYPE",
            DataType::VectorSet(_) => "vectorset",
        }
    }

    #[inline]
    pub fn as_string(&self) -> Option<&Bytes> {
        match self {
            DataType::String(s) => Some(s),
            _ => None,
        }
    }

    #[inline]
    pub fn as_string_mut(&mut self) -> Option<&mut Bytes> {
        match self {
            DataType::String(s) => Some(s),
            _ => None,
        }
    }

    #[inline]
    pub fn as_list(&self) -> Option<&VecDeque<Bytes>> {
        match self {
            DataType::List(l) => Some(l),
            _ => None,
        }
    }

    #[inline]
    pub fn as_list_mut(&mut self) -> Option<&mut VecDeque<Bytes>> {
        match self {
            DataType::List(l) => Some(l),
            _ => None,
        }
    }

    #[inline]
    pub fn as_set(&self) -> Option<&DashSet<Bytes>> {
        match self {
            DataType::Set(s) => Some(s),
            _ => None,
        }
    }

    #[inline]
    pub fn as_set_mut(&mut self) -> Option<&mut DashSet<Bytes>> {
        match self {
            DataType::Set(s) => Some(s),
            _ => None,
        }
    }

    #[inline]
    pub fn as_hash(&self) -> Option<&DashMap<Bytes, Bytes>> {
        match self {
            DataType::Hash(h) => Some(h),
            _ => None,
        }
    }

    #[inline]
    pub fn as_sorted_set(&self) -> Option<&SortedSetData> {
        match self {
            DataType::SortedSet(z) => Some(z),
            _ => None,
        }
    }

    #[inline]
    pub fn as_sorted_set_mut(&mut self) -> Option<&mut SortedSetData> {
        match self {
            DataType::SortedSet(z) => Some(z),
            _ => None,
        }
    }

    #[inline]
    pub fn as_stream(&self) -> Option<&StreamData> {
        match self {
            DataType::Stream(s) => Some(s),
            _ => None,
        }
    }

    #[inline]
    pub fn as_stream_mut(&mut self) -> Option<&mut StreamData> {
        match self {
            DataType::Stream(s) => Some(s),
            _ => None,
        }
    }

    #[inline]
    pub fn as_hyperloglog(&self) -> Option<&HyperLogLogData> {
        match self {
            DataType::HyperLogLog(h) => Some(h),
            _ => None,
        }
    }

    #[inline]
    pub fn as_hyperloglog_mut(&mut self) -> Option<&mut HyperLogLogData> {
        match self {
            DataType::HyperLogLog(h) => Some(h),
            _ => None,
        }
    }

    #[inline]
    pub fn as_json(&self) -> Option<&JsonValue> {
        match self {
            DataType::Json(j) => Some(j),
            _ => None,
        }
    }

    #[inline]
    pub fn as_json_mut(&mut self) -> Option<&mut JsonValue> {
        match self {
            DataType::Json(j) => Some(j),
            _ => None,
        }
    }

    #[inline]
    pub fn as_timeseries(&self) -> Option<&TimeSeriesData> {
        match self {
            DataType::TimeSeries(ts) => Some(ts),
            _ => None,
        }
    }

    #[inline]
    pub fn as_timeseries_mut(&mut self) -> Option<&mut TimeSeriesData> {
        match self {
            DataType::TimeSeries(ts) => Some(ts),
            _ => None,
        }
    }

    #[inline]
    pub fn as_vectorset(&self) -> Option<&VectorSetData> {
        match self {
            DataType::VectorSet(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_vectorset_mut(&mut self) -> Option<&mut VectorSetData> {
        match self {
            DataType::VectorSet(v) => Some(v),
            _ => None,
        }
    }
}

/// Sorted set: maintains both score->member and member->score mappings
#[derive(Debug, Default)]
pub struct SortedSetData {
    /// member -> score mapping for O(1) score lookup
    pub scores: std::collections::HashMap<Bytes, f64>,
    /// (score, member) for ordered iteration - BTreeSet for O(log n) range queries
    pub by_score: BTreeSet<(OrderedFloat, Bytes)>,
}

impl SortedSetData {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add or update a member. Returns true if new member added.
    #[inline]
    pub fn insert(&mut self, member: Bytes, score: f64) -> bool {
        if let Some(&old_score) = self.scores.get(&member) {
            // Update existing
            self.by_score
                .remove(&(OrderedFloat(old_score), member.clone()));
            self.by_score.insert((OrderedFloat(score), member.clone()));
            self.scores.insert(member, score);
            false
        } else {
            // New member
            self.scores.insert(member.clone(), score);
            self.by_score.insert((OrderedFloat(score), member));
            true
        }
    }

    /// Remove a member. Returns true if existed.
    #[inline]
    pub fn remove(&mut self, member: &Bytes) -> bool {
        if let Some(score) = self.scores.remove(member) {
            self.by_score.remove(&(OrderedFloat(score), member.clone()));
            true
        } else {
            false
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.scores.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.scores.is_empty()
    }

    #[inline]
    pub fn score(&self, member: &[u8]) -> Option<f64> {
        self.scores.get(member).copied()
    }

    /// Get rank (0-indexed position by score ascending)
    pub fn rank(&self, member: &[u8]) -> Option<usize> {
        let score = self.scores.get(member)?;
        let member_bytes = Bytes::copy_from_slice(member);
        let target = (OrderedFloat(*score), member_bytes);
        Some(self.by_score.iter().take_while(|e| *e < &target).count())
    }

    /// Get reverse rank (0-indexed position by score descending)
    pub fn rev_rank(&self, member: &[u8]) -> Option<usize> {
        self.rank(member).map(|r| self.len() - 1 - r)
    }
}

/// Wrapper for f64 that implements Ord for BTreeSet
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OrderedFloat(pub f64);

impl Eq for OrderedFloat {}

impl PartialOrd for OrderedFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Stream entry ID
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub const ZERO: StreamId = StreamId { ms: 0, seq: 0 };
    pub const MAX: StreamId = StreamId {
        ms: u64::MAX,
        seq: u64::MAX,
    };
    pub const MIN: StreamId = StreamId { ms: 0, seq: 0 };

    #[inline]
    pub fn new(ms: u64, seq: u64) -> Self {
        Self { ms, seq }
    }

    /// Parse stream ID from bytes. Supports:
    /// - "*" -> None (auto-generate)
    /// - "ms-seq" -> Some(StreamId)
    /// - "ms-*" -> Some(StreamId) with seq=0, caller handles auto-seq
    /// - "-" -> MIN (for range queries)
    /// - "+" -> MAX (for range queries)
    #[inline]
    pub fn parse(s: &[u8]) -> Option<Self> {
        if s.is_empty() {
            return None;
        }
        if s == b"*" {
            return None; // Auto-generate
        }
        if s == b"-" {
            return Some(Self::MIN);
        }
        if s == b"+" {
            return Some(Self::MAX);
        }

        // Find the dash separator
        let dash_pos = s.iter().position(|&b| b == b'-')?;
        let ms_str = std::str::from_utf8(&s[..dash_pos]).ok()?;
        let ms: u64 = ms_str.parse().ok()?;

        let seq_part = &s[dash_pos + 1..];
        if seq_part == b"*" {
            // Partial ID - caller should handle seq generation
            Some(Self { ms, seq: 0 })
        } else {
            let seq_str = std::str::from_utf8(seq_part).ok()?;
            let seq: u64 = seq_str.parse().ok()?;
            Some(Self { ms, seq })
        }
    }

    /// Parse ID that must be greater than given ID (for XADD validation)
    #[inline]
    pub fn parse_for_add(s: &[u8], last_id: StreamId) -> Option<Self> {
        if s == b"*" {
            return None; // Auto-generate
        }

        let dash_pos = s.iter().position(|&b| b == b'-')?;
        let ms_str = std::str::from_utf8(&s[..dash_pos]).ok()?;
        let ms: u64 = ms_str.parse().ok()?;

        let seq_part = &s[dash_pos + 1..];
        if seq_part == b"*" {
            // Auto-seq: if same ms as last_id, use last_id.seq + 1, else 0
            let seq = if ms == last_id.ms { last_id.seq + 1 } else { 0 };
            Some(Self { ms, seq })
        } else {
            let seq_str = std::str::from_utf8(seq_part).ok()?;
            let seq: u64 = seq_str.parse().ok()?;
            Some(Self { ms, seq })
        }
    }

    /// Check if this ID is the special ">" for XREADGROUP
    #[inline]
    pub fn is_new_entries_marker(s: &[u8]) -> bool {
        s == b">"
    }

    /// Increment to next possible ID
    #[inline]
    pub fn next(&self) -> Self {
        if self.seq == u64::MAX {
            Self {
                ms: self.ms + 1,
                seq: 0,
            }
        } else {
            Self {
                ms: self.ms,
                seq: self.seq + 1,
            }
        }
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

/// Stream data structure
#[derive(Debug)]
pub struct StreamData {
    /// Entries: ID -> fields
    pub entries: BTreeMap<StreamId, Vec<(Bytes, Bytes)>>,
    /// Last generated/added ID
    pub last_id: StreamId,
    /// Consumer groups
    pub groups: std::collections::HashMap<Bytes, ConsumerGroup>,
    /// Total entries ever added (for XINFO)
    pub entries_added: u64,
    /// Maximum deleted entry ID (for XSETID)
    pub max_deleted_id: StreamId,
    /// First entry ID (cached for performance)
    pub first_id: Option<StreamId>,
}

impl Default for StreamData {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamData {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            last_id: StreamId::ZERO,
            groups: std::collections::HashMap::new(),
            entries_added: 0,
            max_deleted_id: StreamId::ZERO,
            first_id: None,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get first entry ID
    #[inline]
    pub fn first_entry_id(&self) -> Option<StreamId> {
        self.entries.keys().next().copied()
    }

    /// Get last entry ID
    #[inline]
    pub fn last_entry_id(&self) -> Option<StreamId> {
        self.entries.keys().next_back().copied()
    }

    /// Generate next ID based on current time and last ID
    #[inline]
    pub fn generate_id(&self) -> StreamId {
        let now = now_ms() as u64;
        if now > self.last_id.ms {
            StreamId { ms: now, seq: 0 }
        } else {
            StreamId {
                ms: self.last_id.ms,
                seq: self.last_id.seq + 1,
            }
        }
    }

    /// Add entry with given ID. Returns true if successful.
    #[inline]
    pub fn add_entry(&mut self, id: StreamId, fields: Vec<(Bytes, Bytes)>) -> bool {
        // ID must be greater than last_id
        if id <= self.last_id && self.last_id != StreamId::ZERO {
            // Allow 0-0 as special first entry
            if !(id == StreamId::ZERO && self.entries.is_empty()) {
                return false;
            }
        }

        self.entries.insert(id, fields);
        self.last_id = id;
        self.entries_added += 1;

        if self.first_id.is_none() {
            self.first_id = Some(id);
        }

        true
    }

    /// Trim by MAXLEN - remove oldest entries until len <= maxlen
    pub fn trim_maxlen(&mut self, maxlen: usize, approx: bool) -> usize {
        let current_len = self.entries.len();
        if current_len <= maxlen {
            return 0;
        }

        let to_remove = current_len - maxlen;
        // For approximate trimming, we might remove slightly fewer
        let actual_remove = if approx {
            to_remove.min(100)
        } else {
            to_remove
        };

        let mut removed = 0;
        while removed < actual_remove && !self.entries.is_empty() {
            if let Some((&id, _)) = self.entries.iter().next() {
                self.entries.remove(&id);
                if id > self.max_deleted_id {
                    self.max_deleted_id = id;
                }
                removed += 1;
            } else {
                break;
            }
        }

        // Update first_id cache
        self.first_id = self.first_entry_id();

        removed
    }

    /// Trim by MINID - remove entries with ID < minid
    pub fn trim_minid(&mut self, minid: StreamId, approx: bool) -> usize {
        let ids_to_remove: Vec<StreamId> = self
            .entries
            .range(..minid)
            .map(|(&id, _)| id)
            .take(if approx { 100 } else { usize::MAX })
            .collect();

        let count = ids_to_remove.len();
        for id in ids_to_remove {
            self.entries.remove(&id);
            if id > self.max_deleted_id {
                self.max_deleted_id = id;
            }
        }

        // Update first_id cache
        self.first_id = self.first_entry_id();

        count
    }
}

/// Consumer group for streams
#[derive(Debug, Default)]
pub struct ConsumerGroup {
    pub last_delivered_id: StreamId,
    pub pending: BTreeMap<StreamId, PendingEntry>,
    pub consumers: std::collections::HashMap<Bytes, Consumer>,
    /// Total entries read by this group (for XINFO)
    pub entries_read: Option<u64>,
}

impl ConsumerGroup {
    pub fn new(last_delivered_id: StreamId) -> Self {
        Self {
            last_delivered_id,
            pending: BTreeMap::new(),
            consumers: std::collections::HashMap::new(),
            entries_read: Some(0),
        }
    }

    /// Get or create a consumer
    #[inline]
    pub fn get_or_create_consumer(&mut self, name: Bytes) -> &mut Consumer {
        self.consumers.entry(name).or_insert_with(|| Consumer {
            pending: HashSet::new(),
            last_seen: now_ms(),
        })
    }

    /// Calculate lag (entries behind)
    #[inline]
    pub fn lag(&self, stream_entries_added: u64) -> Option<u64> {
        self.entries_read
            .map(|read| stream_entries_added.saturating_sub(read))
    }
}

/// Stream entry - convenience type for external API usage
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct StreamEntry {
    pub id: StreamId,
    pub fields: Vec<(Bytes, Bytes)>,
}

#[derive(Debug, Clone)]
pub struct PendingEntry {
    pub consumer: Bytes,
    pub delivery_time: i64,
    pub delivery_count: u32,
}

#[derive(Debug, Default)]
pub struct Consumer {
    pub pending: HashSet<StreamId>,
    pub last_seen: i64,
}

/// HyperLogLog for cardinality estimation
/// Uses 16384 registers (2^14) with 6 bits each
#[derive(Debug)]
pub struct HyperLogLogData {
    pub registers: [u8; 16384],
}

impl Default for HyperLogLogData {
    fn default() -> Self {
        Self::new()
    }
}

impl HyperLogLogData {
    const P: usize = 14; // log2(16384)
    const M: usize = 16384;
    const ALPHA: f64 = 0.7213 / (1.0 + 1.079 / 16384.0);

    pub fn new() -> Self {
        Self {
            registers: [0; 16384],
        }
    }

    /// Add an element, returns true if cardinality estimate changed
    pub fn add(&mut self, data: &[u8]) -> bool {
        let hash = Self::hash(data);
        let index = (hash >> (64 - Self::P)) as usize;
        let remaining = (hash << Self::P) | (1 << (Self::P - 1));
        let zeros = remaining.leading_zeros() as u8 + 1;

        if zeros > self.registers[index] {
            self.registers[index] = zeros;
            true
        } else {
            false
        }
    }

    /// Estimate cardinality
    pub fn count(&self) -> u64 {
        let mut sum = 0.0f64;
        let mut zeros = 0usize;

        for &reg in &self.registers {
            sum += 2.0f64.powi(-(reg as i32));
            if reg == 0 {
                zeros += 1;
            }
        }

        let estimate = Self::ALPHA * (Self::M as f64) * (Self::M as f64) / sum;

        // Small range correction
        if estimate <= 2.5 * Self::M as f64 && zeros > 0 {
            return (Self::M as f64 * (Self::M as f64 / zeros as f64).ln()) as u64;
        }

        // Large range correction
        let two_pow_32 = 2.0f64.powi(32);
        if estimate > two_pow_32 / 30.0 {
            return (-two_pow_32 * (1.0 - estimate / two_pow_32).ln()) as u64;
        }

        estimate as u64
    }

    /// Merge another HLL into this one
    pub fn merge(&mut self, other: &HyperLogLogData) {
        for i in 0..Self::M {
            self.registers[i] = self.registers[i].max(other.registers[i]);
        }
    }

    /// Simple hash function (MurmurHash3 finalizer)
    fn hash(data: &[u8]) -> u64 {
        let mut h: u64 = 0;
        for chunk in data.chunks(8) {
            let mut k: u64 = 0;
            for (i, &b) in chunk.iter().enumerate() {
                k |= (b as u64) << (i * 8);
            }
            k = k.wrapping_mul(0xc4ceb9fe1a85ec53);
            k ^= k >> 33;
            k = k.wrapping_mul(0xff51afd7ed558ccd);
            k ^= k >> 33;
            h ^= k;
            h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
        }
        h ^= data.len() as u64;
        h ^= h >> 33;
        h = h.wrapping_mul(0xff51afd7ed558ccd);
        h ^= h >> 33;
        h
    }
}

// ======================== TimeSeries Data Structures ========================

/// Duplicate policy for TimeSeries
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DuplicatePolicy {
    /// Block duplicate timestamps
    #[default]
    Block,
    /// Keep first value
    First,
    /// Keep last value
    Last,
    /// Keep minimum value
    Min,
    /// Keep maximum value
    Max,
    /// Sum the values
    Sum,
}

impl DuplicatePolicy {
    pub fn from_bytes(b: &[u8]) -> Option<Self> {
        match b.to_ascii_uppercase().as_slice() {
            b"BLOCK" => Some(Self::Block),
            b"FIRST" => Some(Self::First),
            b"LAST" => Some(Self::Last),
            b"MIN" => Some(Self::Min),
            b"MAX" => Some(Self::Max),
            b"SUM" => Some(Self::Sum),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Block => "block",
            Self::First => "first",
            Self::Last => "last",
            Self::Min => "min",
            Self::Max => "max",
            Self::Sum => "sum",
        }
    }
}

/// Aggregation types for TimeSeries
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Aggregation {
    Avg,
    First,
    Last,
    Min,
    Max,
    Sum,
    Range,
    Count,
    StdP,
    StdS,
    VarP,
    VarS,
    Twa,
}

impl Aggregation {
    pub fn from_bytes(b: &[u8]) -> Option<Self> {
        match b.to_ascii_uppercase().as_slice() {
            b"AVG" => Some(Self::Avg),
            b"FIRST" => Some(Self::First),
            b"LAST" => Some(Self::Last),
            b"MIN" => Some(Self::Min),
            b"MAX" => Some(Self::Max),
            b"SUM" => Some(Self::Sum),
            b"RANGE" => Some(Self::Range),
            b"COUNT" => Some(Self::Count),
            b"STD.P" => Some(Self::StdP),
            b"STD.S" => Some(Self::StdS),
            b"VAR.P" => Some(Self::VarP),
            b"VAR.S" => Some(Self::VarS),
            b"TWA" => Some(Self::Twa),
            _ => None,
        }
    }
}

/// Compaction rule for downsampling
#[derive(Debug, Clone)]
pub struct CompactionRule {
    pub dest_key: Bytes,
    pub aggregation: Aggregation,
    pub bucket_duration: i64,
    pub align_timestamp: i64,
}

/// TimeSeries data - time-ordered samples with labels
#[derive(Debug)]
pub struct TimeSeriesData {
    /// Samples: timestamp (ms) -> value
    /// BTreeMap provides O(log n) range queries
    pub samples: BTreeMap<i64, f64>,
    /// Labels for filtering
    pub labels: std::collections::HashMap<String, String>,
    /// Retention period in ms (0 = infinite)
    pub retention: i64,
    /// Chunk size (for compatibility, we use BTreeMap internally)
    pub chunk_size: usize,
    /// Duplicate policy
    pub duplicate_policy: DuplicatePolicy,
    /// Ignore max time diff for dedup
    pub ignore_max_time_diff: i64,
    /// Ignore max value diff for dedup
    pub ignore_max_val_diff: f64,
    /// Compaction rules
    pub rules: Vec<CompactionRule>,
    /// Total sample count (for quick INFO)
    pub total_samples: u64,
    /// First timestamp
    pub first_timestamp: i64,
    /// Last timestamp
    pub last_timestamp: i64,
}

impl Default for TimeSeriesData {
    fn default() -> Self {
        Self::new()
    }
}

impl TimeSeriesData {
    pub fn new() -> Self {
        Self {
            samples: BTreeMap::new(),
            labels: std::collections::HashMap::new(),
            retention: 0,
            chunk_size: 4096,
            duplicate_policy: DuplicatePolicy::Block,
            ignore_max_time_diff: 0,
            ignore_max_val_diff: 0.0,
            rules: Vec::new(),
            total_samples: 0,
            first_timestamp: 0,
            last_timestamp: 0,
        }
    }

    /// Add a sample, applying duplicate policy
    #[inline]
    pub fn add_sample(&mut self, timestamp: i64, value: f64) -> Result<(), &'static str> {
        // Check duplicate
        if let Some(&existing) = self.samples.get(&timestamp) {
            match self.duplicate_policy {
                DuplicatePolicy::Block => return Err("TSDB: duplicate sample"),
                DuplicatePolicy::First => return Ok(()), // Keep existing
                DuplicatePolicy::Last => {
                    self.samples.insert(timestamp, value);
                }
                DuplicatePolicy::Min => {
                    self.samples.insert(timestamp, existing.min(value));
                }
                DuplicatePolicy::Max => {
                    self.samples.insert(timestamp, existing.max(value));
                }
                DuplicatePolicy::Sum => {
                    self.samples.insert(timestamp, existing + value);
                }
            }
        } else {
            self.samples.insert(timestamp, value);
            self.total_samples += 1;
        }

        // Update first/last
        if self.first_timestamp == 0 || timestamp < self.first_timestamp {
            self.first_timestamp = timestamp;
        }
        if timestamp > self.last_timestamp {
            self.last_timestamp = timestamp;
        }

        // Apply retention
        if self.retention > 0 {
            let cutoff = timestamp - self.retention;
            self.samples = self.samples.split_off(&cutoff);
        }

        Ok(())
    }

    /// Get latest sample
    #[inline]
    pub fn get_latest(&self) -> Option<(i64, f64)> {
        self.samples.iter().next_back().map(|(&t, &v)| (t, v))
    }

    /// Get range of samples
    #[inline]
    pub fn range(&self, from: i64, to: i64) -> impl Iterator<Item = (&i64, &f64)> {
        self.samples.range(from..=to)
    }

    /// Get range in reverse
    #[inline]
    pub fn rev_range(&self, from: i64, to: i64) -> impl DoubleEndedIterator<Item = (&i64, &f64)> {
        self.samples.range(from..=to)
    }

    /// Delete samples in range
    pub fn delete_range(&mut self, from: i64, to: i64) -> usize {
        let keys: Vec<i64> = self.samples.range(from..=to).map(|(&k, _)| k).collect();
        let count = keys.len();
        for key in keys {
            self.samples.remove(&key);
        }
        self.total_samples = self.total_samples.saturating_sub(count as u64);
        count
    }

    /// Apply aggregation to a range
    pub fn aggregate(
        &self,
        from: i64,
        to: i64,
        agg: Aggregation,
        bucket_duration: i64,
    ) -> Vec<(i64, f64)> {
        if bucket_duration <= 0 {
            return vec![];
        }

        let mut result = Vec::new();
        let mut bucket_start = (from / bucket_duration) * bucket_duration;

        while bucket_start <= to {
            let bucket_end = bucket_start + bucket_duration - 1;
            let samples: Vec<f64> = self
                .samples
                .range(bucket_start..=bucket_end.min(to))
                .map(|(_, &v)| v)
                .collect();

            if !samples.is_empty() {
                let value = match agg {
                    Aggregation::Avg => samples.iter().sum::<f64>() / samples.len() as f64,
                    Aggregation::First => samples[0],
                    Aggregation::Last => *samples.last().unwrap(),
                    Aggregation::Min => samples.iter().cloned().fold(f64::INFINITY, f64::min),
                    Aggregation::Max => samples.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
                    Aggregation::Sum => samples.iter().sum(),
                    Aggregation::Range => {
                        let min = samples.iter().cloned().fold(f64::INFINITY, f64::min);
                        let max = samples.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
                        max - min
                    }
                    Aggregation::Count => samples.len() as f64,
                    Aggregation::StdP | Aggregation::StdS => {
                        let mean = samples.iter().sum::<f64>() / samples.len() as f64;
                        let variance: f64 = samples.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                            / if matches!(agg, Aggregation::StdP) {
                                samples.len() as f64
                            } else {
                                (samples.len() - 1).max(1) as f64
                            };
                        variance.sqrt()
                    }
                    Aggregation::VarP | Aggregation::VarS => {
                        let mean = samples.iter().sum::<f64>() / samples.len() as f64;
                        samples.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                            / if matches!(agg, Aggregation::VarP) {
                                samples.len() as f64
                            } else {
                                (samples.len() - 1).max(1) as f64
                            }
                    }
                    Aggregation::Twa => {
                        // Time-weighted average - simplified
                        samples.iter().sum::<f64>() / samples.len() as f64
                    }
                };
                result.push((bucket_start, value));
            }

            bucket_start += bucket_duration;
        }

        result
    }
}

/// Entry stored in the main database
#[derive(Debug)]
pub struct Entry {
    pub data: DataType,
    /// Expiration time in milliseconds since UNIX epoch.
    /// -1 means no expiration.
    expire_at: AtomicI64,
    /// Version number for optimistic locking (WATCH)
    version: AtomicU64,
    /// Last access time in seconds since UNIX epoch (for LRU eviction)
    /// Uses i32 stored as i64 for atomic access, wraps after 2038
    lru_time: AtomicI64,
    /// LFU counter (logarithmic, 0-255) for LFU eviction
    /// Uses probabilistic increment like Redis
    lfu_counter: std::sync::atomic::AtomicU8,
}

impl Entry {
    pub const NO_EXPIRE: i64 = -1;

    #[inline]
    pub fn new(data: DataType) -> Self {
        let now_secs = now_ms() / 1000;
        Self {
            data,
            expire_at: AtomicI64::new(Self::NO_EXPIRE),
            version: AtomicU64::new(1),
            lru_time: AtomicI64::new(now_secs),
            lfu_counter: std::sync::atomic::AtomicU8::new(5), // Initial counter
        }
    }

    #[inline]
    pub fn with_expire(data: DataType, expire_at_ms: i64) -> Self {
        let now_secs = now_ms() / 1000;
        Self {
            data,
            expire_at: AtomicI64::new(expire_at_ms),
            version: AtomicU64::new(1),
            lru_time: AtomicI64::new(now_secs),
            lfu_counter: std::sync::atomic::AtomicU8::new(5),
        }
    }

    /// Get current version
    #[inline]
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Relaxed)
    }

    /// Increment version (call after any modification)
    #[inline]
    pub fn bump_version(&self) {
        self.version.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn is_expired(&self) -> bool {
        let exp = self.expire_at.load(Ordering::Relaxed);
        exp != Self::NO_EXPIRE && now_ms() >= exp
    }

    #[inline]
    pub fn expire_at_ms(&self) -> Option<i64> {
        let exp = self.expire_at.load(Ordering::Relaxed);
        if exp == Self::NO_EXPIRE {
            None
        } else {
            Some(exp)
        }
    }

    #[inline]
    pub fn set_expire_at(&self, ms: i64) {
        self.expire_at.store(ms, Ordering::Relaxed);
    }

    #[inline]
    pub fn set_expire_in(&self, ms: i64) {
        self.expire_at.store(now_ms() + ms, Ordering::Relaxed);
    }

    #[inline]
    pub fn persist(&self) {
        self.expire_at.store(Self::NO_EXPIRE, Ordering::Relaxed);
    }

    #[inline]
    pub fn ttl_ms(&self) -> Option<i64> {
        let exp = self.expire_at.load(Ordering::Relaxed);
        if exp == Self::NO_EXPIRE {
            None
        } else {
            Some((exp - now_ms()).max(0))
        }
    }

    // ==================== LRU/LFU Methods ====================

    /// Update LRU access time to now
    #[inline]
    pub fn touch_lru(&self) {
        let now_secs = now_ms() / 1000;
        self.lru_time.store(now_secs, Ordering::Relaxed);
    }

    /// Get LRU access time in seconds since epoch
    #[inline]
    pub fn lru_time(&self) -> i64 {
        self.lru_time.load(Ordering::Relaxed)
    }

    /// Get idle time in seconds since last access
    #[inline]
    pub fn idle_time(&self) -> i64 {
        let now_secs = now_ms() / 1000;
        (now_secs - self.lru_time.load(Ordering::Relaxed)).max(0)
    }

    /// Increment LFU counter using probabilistic increment (Redis algorithm)
    /// Counter grows logarithmically to prevent saturation
    #[inline]
    pub fn increment_lfu(&self) {
        let counter = self.lfu_counter.load(Ordering::Relaxed);
        if counter < 255 {
            // Probabilistic increment: P = 1/(counter-LFU_INIT_VAL+1)
            let base_val = counter.saturating_sub(5) as u32;
            let p = 1.0 / ((base_val as f64) + 1.0);
            if fastrand::f64() < p {
                let _ = self.lfu_counter.compare_exchange(
                    counter,
                    counter.saturating_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                );
            }
        }
    }

    /// Decay LFU counter based on time since last access
    /// Called periodically to decrease counters of idle keys
    #[inline]
    pub fn decay_lfu(&self, lfu_decay_time: u32) {
        if lfu_decay_time == 0 {
            return;
        }
        let idle_secs = self.idle_time() as u32;
        let num_decays = idle_secs / lfu_decay_time;
        if num_decays > 0 {
            let counter = self.lfu_counter.load(Ordering::Relaxed);
            let new_counter = counter.saturating_sub(num_decays.min(255) as u8);
            self.lfu_counter.store(new_counter, Ordering::Relaxed);
        }
    }

    /// Get LFU counter value
    #[inline]
    pub fn lfu_counter(&self) -> u8 {
        self.lfu_counter.load(Ordering::Relaxed)
    }

    /// Check if key has expiration set (for volatile-* eviction)
    #[inline]
    pub fn has_expire(&self) -> bool {
        self.expire_at.load(Ordering::Relaxed) != Self::NO_EXPIRE
    }
}

// ======================== VectorSet Data Structures ========================

/// Vector quantization type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VectorQuantization {
    #[default]
    NoQuant, // Full FP32 precision
    Q8,     // 8-bit quantization
    Binary, // Binary quantization
}

impl VectorQuantization {
    pub fn from_bytes(b: &[u8]) -> Option<Self> {
        match b.to_ascii_uppercase().as_slice() {
            b"NOQUANT" => Some(Self::NoQuant),
            b"Q8" => Some(Self::Q8),
            b"BIN" | b"BINARY" => Some(Self::Binary),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NoQuant => "noquant",
            Self::Q8 => "q8",
            Self::Binary => "bin",
        }
    }
}

/// HNSW node with vector and connections
#[derive(Debug)]
pub struct VectorNode {
    /// The element name
    pub element: Bytes,
    /// Vector data (stored as f32 for SIMD efficiency)
    pub vector: Vec<f32>,
    /// Quantized vector for Q8 (optional)
    pub vector_q8: Option<Vec<i8>>,
    /// Binary quantized vector (optional)
    pub vector_bin: Option<Vec<u64>>,
    /// Optional JSON attributes
    pub attributes: Option<Box<JsonValue>>,
    /// HNSW connections per layer: layer -> [neighbor indices]
    pub connections: Vec<Vec<u32>>,
    /// Node level in HNSW (0 = bottom layer, always present)
    pub level: u8,
}

impl VectorNode {
    pub fn new(element: Bytes, vector: Vec<f32>, level: u8, quant: VectorQuantization) -> Self {
        let vector_q8 = if quant == VectorQuantization::Q8 {
            Some(Self::quantize_q8(&vector))
        } else {
            None
        };

        let vector_bin = if quant == VectorQuantization::Binary {
            Some(Self::quantize_binary(&vector))
        } else {
            None
        };

        Self {
            element,
            vector,
            vector_q8,
            vector_bin,
            attributes: None,
            connections: vec![Vec::new(); level as usize + 1],
            level,
        }
    }

    /// Quantize to 8-bit signed integers
    #[inline]
    fn quantize_q8(vector: &[f32]) -> Vec<i8> {
        // Find min/max for normalization
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

    /// Quantize to binary (sign bits)
    #[inline]
    fn quantize_binary(vector: &[f32]) -> Vec<u64> {
        let mut result = vec![0u64; vector.len().div_ceil(64)];
        for (i, &v) in vector.iter().enumerate() {
            if v > 0.0 {
                result[i / 64] |= 1u64 << (i % 64);
            }
        }
        result
    }
}

/// HNSW-based vector set for similarity search
#[derive(Debug)]
pub struct VectorSetData {
    /// Vector dimension
    pub dim: usize,
    /// Reduced dimension for queries (optional PCA/random projection)
    pub reduced_dim: Option<usize>,
    /// Quantization type
    pub quant: VectorQuantization,
    /// All nodes indexed by internal ID
    pub nodes: Vec<VectorNode>,
    /// Element name -> node index mapping for O(1) lookup
    pub element_index: std::collections::HashMap<Bytes, u32>,
    /// Entry point (index of highest-level node)
    pub entry_point: Option<u32>,
    /// Max level currently in graph
    pub max_level: u8,
    /// M parameter (max connections per layer, default 16)
    pub m: usize,
    /// M0 parameter (max connections at layer 0, typically 2*M)
    pub m0: usize,
    /// EF construction parameter (default 200)
    pub ef_construction: usize,
    /// Level multiplier for probabilistic layer assignment
    pub level_mult: f64,
}

impl Default for VectorSetData {
    fn default() -> Self {
        Self::new(0)
    }
}

impl VectorSetData {
    /// Default M value for HNSW (as per Redis)
    pub const DEFAULT_M: usize = 16;
    /// Default EF construction value
    pub const DEFAULT_EF_CONSTRUCTION: usize = 200;

    pub fn new(dim: usize) -> Self {
        let m = Self::DEFAULT_M;
        Self {
            dim,
            reduced_dim: None,
            quant: VectorQuantization::NoQuant,
            nodes: Vec::new(),
            element_index: std::collections::HashMap::new(),
            entry_point: None,
            max_level: 0,
            m,
            m0: m * 2,
            ef_construction: Self::DEFAULT_EF_CONSTRUCTION,
            level_mult: 1.0 / (m as f64).ln(),
        }
    }

    pub fn with_options(
        dim: usize,
        m: usize,
        ef_construction: usize,
        quant: VectorQuantization,
    ) -> Self {
        let m = m.clamp(2, 128); // Clamp M to reasonable range
        Self {
            dim,
            reduced_dim: None,
            quant,
            nodes: Vec::new(),
            element_index: std::collections::HashMap::new(),
            entry_point: None,
            max_level: 0,
            m,
            m0: m * 2,
            ef_construction: ef_construction.max(1),
            level_mult: 1.0 / (m as f64).ln(),
        }
    }

    /// Get number of elements
    #[inline]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Check if element exists
    #[inline]
    pub fn contains(&self, element: &[u8]) -> bool {
        self.element_index.contains_key(element)
    }

    /// Get node index for element
    #[inline]
    pub fn get_index(&self, element: &[u8]) -> Option<u32> {
        self.element_index.get(element).copied()
    }

    /// Get node by index
    #[inline]
    pub fn get_node(&self, idx: u32) -> Option<&VectorNode> {
        self.nodes.get(idx as usize)
    }

    /// Get mutable node by index
    #[inline]
    pub fn get_node_mut(&mut self, idx: u32) -> Option<&mut VectorNode> {
        self.nodes.get_mut(idx as usize)
    }

    /// Generate random level for new node (geometric distribution)
    #[inline]
    pub fn random_level(&self) -> u8 {
        let mut level = 0u8;
        let max_level = 16u8; // Cap at reasonable level
        while fastrand::f64() < (1.0 / self.m as f64) && level < max_level {
            level += 1;
        }
        level
    }

    /// Get vector for an element
    pub fn get_vector(&self, element: &[u8]) -> Option<&[f32]> {
        self.element_index
            .get(element)
            .and_then(|&idx| self.nodes.get(idx as usize))
            .map(|node| node.vector.as_slice())
    }

    /// Get attributes for an element
    pub fn get_attributes(&self, element: &[u8]) -> Option<&JsonValue> {
        self.element_index
            .get(element)
            .and_then(|&idx| self.nodes.get(idx as usize))
            .and_then(|node| node.attributes.as_deref())
    }

    /// Set attributes for an element
    pub fn set_attributes(&mut self, element: &[u8], attrs: Option<Box<JsonValue>>) -> bool {
        if let Some(&idx) = self.element_index.get(element)
            && let Some(node) = self.nodes.get_mut(idx as usize)
        {
            node.attributes = attrs;
            return true;
        }
        false
    }

    /// Get all elements in lexicographical order within range
    pub fn range(&self, start: &[u8], end: &[u8], count: Option<usize>) -> Vec<Bytes> {
        let mut elements: Vec<Bytes> = self
            .element_index
            .keys()
            .filter(|e| {
                let e_slice = e.as_ref();
                (start.is_empty() || e_slice >= start) && (end.is_empty() || e_slice <= end)
            })
            .cloned()
            .collect();

        elements.sort();

        if let Some(n) = count {
            elements.truncate(n);
        }

        elements
    }

    /// Get random members
    pub fn random_members(&self, count: usize) -> Vec<Bytes> {
        if self.nodes.is_empty() {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(count.min(self.nodes.len()));
        let mut seen = HashSet::new();

        while result.len() < count && seen.len() < self.nodes.len() {
            let idx = fastrand::usize(..self.nodes.len());
            if seen.insert(idx) {
                result.push(self.nodes[idx].element.clone());
            }
        }

        result
    }
}
