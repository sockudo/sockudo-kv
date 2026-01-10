//! TimeSeries operations for the Store
//!
//! Performance optimizations:
//! - BTreeMap for O(log n) range queries
//! - Efficient aggregation with iterators
//! - Zero-copy where possible

use bytes::Bytes;
use dashmap::mapref::entry::Entry as DashEntry;
use std::sync::atomic::Ordering;

use crate::error::{Error, Result};
use crate::storage::Store;
use crate::storage::timeseries::TimeSeries;
use crate::storage::types::{
    Aggregation, CompactionRule, DataType, DuplicatePolicy, Entry, TimeSeriesInfo,
};

/// Label index for efficient filtering
use dashmap::{DashMap, DashSet};
use std::sync::LazyLock;

/// Global label index: label:value -> set of keys
static LABEL_INDEX: LazyLock<DashMap<String, DashSet<Bytes>>> = LazyLock::new(DashMap::new);

impl Store {
    // ==================== TimeSeries Core Operations ====================

    /// TS.CREATE key [RETENTION retentionPeriod] [ENCODING ...] [CHUNK SIZE size]
    /// [DUPLICATE POLICY policy] [IGNORE ...] [LABELS label value ...]
    pub fn ts_create(
        &self,
        key: Bytes,
        retention: i64,
        duplicate_policy: DuplicatePolicy,
        labels: Vec<(String, String)>,
    ) -> Result<()> {
        match self.data.entry(key.clone()) {
            DashEntry::Occupied(_) => Err(Error::Other("TSDB: key already exists")),
            DashEntry::Vacant(e) => {
                let mut ts = TimeSeries::new();
                ts.retention_ms = retention;
                ts.duplicate_policy = duplicate_policy;
                // chunk_size, ignore_* are not supported or handled differently

                // Add labels and update index
                for (label, value) in labels {
                    let index_key = format!("{}={}", label, value);
                    LABEL_INDEX
                        .entry(index_key)
                        .or_default()
                        .insert(key.clone());
                    ts.labels.insert(label, value);
                }

                e.insert(Entry::new(DataType::TimeSeries(Box::new(ts))));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        }
    }

    /// TS.ADD key timestamp value [options...]
    pub fn ts_add(&self, key: Bytes, timestamp: i64, value: f64) -> Result<i64> {
        match self.data.entry(key.clone()) {
            DashEntry::Occupied(mut e) => {
                let entry = e.get_mut();
                match &mut entry.data {
                    DataType::TimeSeries(ts) => {
                        ts.add(timestamp, value).map_err(Error::Other)?;
                        Ok(timestamp)
                    }
                    _ => Err(Error::WrongType),
                }
            }
            DashEntry::Vacant(e) => {
                // Auto-create timeseries
                let mut ts = TimeSeries::new();
                ts.add(timestamp, value).map_err(Error::Other)?;
                e.insert(Entry::new(DataType::TimeSeries(Box::new(ts))));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(timestamp)
            }
        }
    }

    /// TS.MADD key timestamp value [key timestamp value ...]
    pub fn ts_madd(&self, samples: Vec<(Bytes, i64, f64)>) -> Vec<Result<i64>> {
        samples
            .into_iter()
            .map(|(key, ts, val)| self.ts_add(key, ts, val))
            .collect()
    }

    /// TS.GET key [LATEST]
    pub fn ts_get(&self, key: &[u8]) -> Option<(i64, f64)> {
        self.data.get(key).and_then(|e| {
            if e.is_expired() {
                None
            } else {
                e.data.as_timeseries().and_then(|ts| ts.get_latest())
            }
        })
    }

    /// TS.DEL key fromTimestamp toTimestamp
    pub fn ts_del(&self, key: &[u8], from: i64, to: i64) -> Result<usize> {
        match self.data.get_mut(key) {
            Some(mut e) => match &mut e.value_mut().data {
                DataType::TimeSeries(ts) => Ok(ts.delete_range(from, to)),
                _ => Err(Error::WrongType),
            },
            None => Ok(0),
        }
    }

    /// TS.INCRBY / TS.DECRBY
    pub fn ts_incrby(&self, key: Bytes, value: f64, timestamp: Option<i64>) -> Result<i64> {
        let ts_val = timestamp.unwrap_or_else(crate::storage::value::now_ms);

        match self.data.entry(key.clone()) {
            DashEntry::Occupied(mut e) => {
                let entry = e.get_mut();
                match &mut entry.data {
                    DataType::TimeSeries(ts) => {
                        ts.incrby(value, ts_val).map_err(Error::Other)?;
                        Ok(ts_val)
                    }
                    _ => Err(Error::WrongType),
                }
            }
            DashEntry::Vacant(e) => {
                let mut tsdata = TimeSeries::new();
                tsdata.add(ts_val, value).map_err(Error::Other)?;
                e.insert(Entry::new(DataType::TimeSeries(Box::new(tsdata))));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(ts_val)
            }
        }
    }

    /// TS.ALTER key [options...]
    pub fn ts_alter(
        &self,
        key: &[u8],
        retention: Option<i64>,
        duplicate_policy: Option<DuplicatePolicy>,
        labels: Option<Vec<(String, String)>>,
    ) -> Result<()> {
        match self.data.get_mut(key) {
            Some(mut e) => {
                let entry = e.value_mut();
                match &mut entry.data {
                    DataType::TimeSeries(ts) => {
                        if let Some(r) = retention {
                            ts.retention_ms = r;
                        }
                        // chunk_size not directly supported for modification
                        if let Some(d) = duplicate_policy {
                            ts.duplicate_policy = d;
                        }
                        if let Some(new_labels) = labels {
                            // Update label index
                            let key_bytes = Bytes::copy_from_slice(key);
                            for (label, value) in &ts.labels {
                                let index_key = format!("{}={}", label, value);
                                if let Some(set) = LABEL_INDEX.get(&index_key) {
                                    set.remove(&key_bytes);
                                }
                            }
                            ts.labels.clear();
                            for (label, value) in new_labels {
                                let index_key = format!("{}={}", label, value);
                                LABEL_INDEX
                                    .entry(index_key)
                                    .or_default()
                                    .insert(key_bytes.clone());
                                ts.labels.insert(label, value);
                            }
                        }
                        Ok(())
                    }
                    _ => Err(Error::WrongType),
                }
            }
            None => Err(Error::Other("TSDB: the key does not exist")),
        }
    }

    /// TS.INFO key [DEBUG]
    pub fn ts_info(&self, key: &[u8]) -> Option<TimeSeriesInfo> {
        self.data.get(key).and_then(|e| {
            if e.is_expired() {
                None
            } else {
                e.data.as_timeseries().map(|ts| ts.info())
            }
        })
    }

    // ==================== Range Queries ====================

    /// TS.RANGE key fromTimestamp toTimestamp [options...]
    pub fn ts_range(
        &self,
        key: &[u8],
        from: i64,
        to: i64,
        count: Option<usize>,
        aggregation: Option<(Aggregation, i64)>,
    ) -> Option<Vec<(i64, f64)>> {
        self.data.get(key).and_then(|e| {
            if e.is_expired() {
                return None;
            }
            e.data.as_timeseries().map(|ts| {
                if let Some((agg, bucket)) = aggregation {
                    let mut result = ts.aggregate(from, to, agg, bucket);
                    if let Some(c) = count {
                        result.truncate(c);
                    }
                    result
                } else {
                    let mut result = ts.range(from, to);
                    if let Some(c) = count {
                        result.truncate(c);
                    }
                    result
                }
            })
        })
    }

    /// TS.REVRANGE key fromTimestamp toTimestamp [options...]
    pub fn ts_revrange(
        &self,
        key: &[u8],
        from: i64,
        to: i64,
        count: Option<usize>,
        aggregation: Option<(Aggregation, i64)>,
    ) -> Option<Vec<(i64, f64)>> {
        self.data.get(key).and_then(|e| {
            if e.is_expired() {
                return None;
            }
            e.data.as_timeseries().map(|ts| {
                if let Some((agg, bucket)) = aggregation {
                    let mut result = ts.aggregate(from, to, agg, bucket);
                    result.reverse();
                    if let Some(c) = count {
                        result.truncate(c);
                    }
                    result
                } else {
                    let mut result = ts.rev_range(from, to);
                    if let Some(c) = count {
                        result.truncate(c);
                    }
                    result
                }
            })
        })
    }

    /// TS.MGET - get latest from multiple series by filter
    pub fn ts_mget(&self, filters: &[(String, String)]) -> Vec<(Bytes, Option<(i64, f64)>)> {
        let matching_keys = self.ts_query_index(filters);
        matching_keys
            .into_iter()
            .map(|key| {
                let sample = self.ts_get(&key);
                (key, sample)
            })
            .collect()
    }

    /// TS.MRANGE - range query on multiple series
    pub fn ts_mrange(
        &self,
        from: i64,
        to: i64,
        filters: &[(String, String)],
        count: Option<usize>,
        aggregation: Option<(Aggregation, i64)>,
    ) -> Vec<(Bytes, Vec<(i64, f64)>)> {
        let matching_keys = self.ts_query_index(filters);
        matching_keys
            .into_iter()
            .filter_map(|key| {
                self.ts_range(&key, from, to, count, aggregation)
                    .map(|samples| (key, samples))
            })
            .collect()
    }

    /// TS.QUERYINDEX - get keys matching filters
    pub fn ts_query_index(&self, filters: &[(String, String)]) -> Vec<Bytes> {
        if filters.is_empty() {
            return vec![];
        }

        // Start with first filter
        let first = &filters[0];
        let index_key = format!("{}={}", first.0, first.1);

        let mut result: Option<DashSet<Bytes>> = LABEL_INDEX.get(&index_key).map(|s| {
            let set = DashSet::new();
            for key in s.iter() {
                set.insert(key.clone());
            }
            set
        });

        // Intersect with remaining filters
        for filter in &filters[1..] {
            let index_key = format!("{}={}", filter.0, filter.1);
            if let Some(current) = &result {
                if let Some(filter_set) = LABEL_INDEX.get(&index_key) {
                    let new_set = DashSet::new();
                    for key in current.iter() {
                        if filter_set.contains(&*key) {
                            new_set.insert(key.clone());
                        }
                    }
                    result = Some(new_set);
                } else {
                    result = None;
                    break;
                }
            }
        }

        result
            .map(|s| s.iter().map(|k| k.clone()).collect())
            .unwrap_or_default()
    }

    // ==================== Compaction Rules ====================

    /// TS.CREATERULE sourceKey destKey AGGREGATION aggregationType timeBucket
    pub fn ts_createrule(
        &self,
        source_key: &[u8],
        dest_key: Bytes,
        aggregation: Aggregation,
        bucket_duration: i64,
        align_timestamp: i64,
    ) -> Result<()> {
        // Verify dest key exists
        if !self.data.contains_key(dest_key.as_ref()) {
            return Err(Error::Other("TSDB: the key does not exist"));
        }

        match self.data.get_mut(source_key) {
            Some(mut e) => match &mut e.value_mut().data {
                DataType::TimeSeries(ts) => {
                    // Check if rule already exists
                    if ts.rules.iter().any(|r| r.dest_key == dest_key) {
                        return Err(Error::Other("TSDB: compaction rule already exists"));
                    }

                    ts.rules.push(CompactionRule {
                        dest_key,
                        aggregation,
                        bucket_duration,
                        align_timestamp,
                    });
                    Ok(())
                }
                _ => Err(Error::WrongType),
            },
            None => Err(Error::Other("TSDB: the key does not exist")),
        }
    }

    /// TS.DELETERULE sourceKey destKey
    pub fn ts_deleterule(&self, source_key: &[u8], dest_key: &[u8]) -> Result<()> {
        match self.data.get_mut(source_key) {
            Some(mut e) => match &mut e.value_mut().data {
                DataType::TimeSeries(ts) => {
                    let original_len = ts.rules.len();
                    ts.rules.retain(|r| r.dest_key.as_ref() != dest_key);
                    if ts.rules.len() == original_len {
                        Err(Error::Other("TSDB: compaction rule does not exist"))
                    } else {
                        Ok(())
                    }
                }
                _ => Err(Error::WrongType),
            },
            None => Err(Error::Other("TSDB: the key does not exist")),
        }
    }
}
