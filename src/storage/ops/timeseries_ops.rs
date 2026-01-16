//! TimeSeries operations for the Store
//!
//! Performance optimizations:
//! - BTreeMap for O(log n) range queries
//! - Efficient aggregation with iterators
//! - Zero-copy where possible

use bytes::Bytes;
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
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(_) => {
                Err(Error::Other("TSDB: key already exists"))
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                let mut ts = TimeSeries::new();
                ts.retention_ms = retention;
                ts.duplicate_policy = duplicate_policy;

                for (label, value) in labels {
                    let index_key = format!("{}={}", label, value);
                    LABEL_INDEX
                        .entry(index_key)
                        .or_default()
                        .insert(key.clone());
                    ts.labels.insert(label, value);
                }

                e.insert((key, Entry::new(DataType::TimeSeries(Box::new(ts)))));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        }
    }

    /// TS.ADD key timestamp value [options...]
    pub fn ts_add(&self, key: Bytes, timestamp: i64, value: f64) -> Result<i64> {
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                match &mut entry.data {
                    DataType::TimeSeries(ts) => {
                        ts.add(timestamp, value).map_err(Error::Other)?;
                        entry.bump_version();
                        Ok(timestamp)
                    }
                    _ => Err(Error::WrongType),
                }
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                let mut ts = TimeSeries::new();
                ts.add(timestamp, value).map_err(Error::Other)?;
                e.insert((key, Entry::new(DataType::TimeSeries(Box::new(ts)))));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(timestamp)
            }
        }
    }

    /// TS.ADD key timestamp value [RETENTION retentionPeriod] [ON_DUPLICATE policy] [LABELS label value ...]
    /// Extended version with options for auto-created series
    pub fn ts_add_with_options(
        &self,
        key: Bytes,
        timestamp: i64,
        value: f64,
        retention: Option<i64>,
        on_duplicate: Option<DuplicatePolicy>,
        labels: Option<Vec<(String, String)>>,
    ) -> Result<i64> {
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                match &mut entry.data {
                    DataType::TimeSeries(ts) => {
                        // Apply on_duplicate policy for this sample if specified
                        let prev_policy = ts.duplicate_policy;
                        if let Some(policy) = on_duplicate {
                            ts.duplicate_policy = policy;
                        }
                        let result = ts.add(timestamp, value).map_err(Error::Other);
                        // Restore original policy
                        if on_duplicate.is_some() {
                            ts.duplicate_policy = prev_policy;
                        }
                        if result.is_ok() {
                            entry.bump_version();
                        }
                        result.map(|_| timestamp)
                    }
                    _ => Err(Error::WrongType),
                }
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                // Create new time series with options
                let mut ts = TimeSeries::new();
                if let Some(r) = retention {
                    ts.retention_ms = r;
                }
                if let Some(policy) = on_duplicate {
                    ts.duplicate_policy = policy;
                }
                if let Some(new_labels) = labels {
                    for (label, lvalue) in new_labels {
                        let index_key = format!("{}={}", label, lvalue);
                        LABEL_INDEX
                            .entry(index_key)
                            .or_default()
                            .insert(key.clone());
                        ts.labels.insert(label, lvalue);
                    }
                }
                ts.add(timestamp, value).map_err(Error::Other)?;
                e.insert((key, Entry::new(DataType::TimeSeries(Box::new(ts)))));
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
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    None
                } else {
                    entry_ref
                        .1
                        .data
                        .as_timeseries()
                        .and_then(|ts| ts.get_latest())
                }
            }
            None => None,
        }
    }

    /// TS.DEL key fromTimestamp toTimestamp
    pub fn ts_del(&self, key: &[u8], from: i64, to: i64) -> Result<usize> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                match &mut entry.data {
                    DataType::TimeSeries(ts) => {
                        let deleted = ts.delete_range(from, to);
                        if deleted > 0 {
                            entry.bump_version();
                        }
                        Ok(deleted)
                    }
                    _ => Err(Error::WrongType),
                }
            }
            crate::storage::dashtable::Entry::Vacant(_) => Ok(0),
        }
    }

    /// TS.INCRBY / TS.DECRBY
    pub fn ts_incrby(&self, key: Bytes, value: f64, timestamp: Option<i64>) -> Result<i64> {
        let ts_val = timestamp.unwrap_or_else(crate::storage::value::now_ms);

        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                match &mut entry.data {
                    DataType::TimeSeries(ts) => {
                        ts.incrby(value, ts_val).map_err(Error::Other)?;
                        entry.bump_version();
                        Ok(ts_val)
                    }
                    _ => Err(Error::WrongType),
                }
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                let mut tsdata = TimeSeries::new();
                tsdata.add(ts_val, value).map_err(Error::Other)?;
                e.insert((key, Entry::new(DataType::TimeSeries(Box::new(tsdata)))));
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
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                match &mut entry.data {
                    DataType::TimeSeries(ts) => {
                        if let Some(r) = retention {
                            ts.retention_ms = r;
                        }
                        if let Some(d) = duplicate_policy {
                            ts.duplicate_policy = d;
                        }
                        if let Some(new_labels) = labels {
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
                        entry.bump_version();
                        Ok(())
                    }
                    _ => Err(Error::WrongType),
                }
            }
            crate::storage::dashtable::Entry::Vacant(_) => {
                Err(Error::Other("TSDB: the key does not exist"))
            }
        }
    }

    /// TS.INFO key [DEBUG]
    pub fn ts_info(&self, key: &[u8]) -> Option<TimeSeriesInfo> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    None
                } else {
                    entry_ref.1.data.as_timeseries().map(|ts| ts.info())
                }
            }
            None => None,
        }
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
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return None;
                }
                entry_ref.1.data.as_timeseries().map(|ts| {
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
            }
            None => None,
        }
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
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return None;
                }
                entry_ref.1.data.as_timeseries().map(|ts| {
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
            }
            None => None,
        }
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

        let first = &filters[0];
        let index_key = format!("{}={}", first.0, first.1);

        let mut result: Option<DashSet<Bytes>> = LABEL_INDEX.get(&index_key).map(|s| {
            let set = DashSet::new();
            for key in s.iter() {
                set.insert(key.clone());
            }
            set
        });

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
        if self.data_get(dest_key.as_ref()).is_none() {
            return Err(Error::Other("TSDB: the key does not exist"));
        }

        match self.data_entry(source_key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                match &mut entry.data {
                    DataType::TimeSeries(ts) => {
                        if ts.rules.iter().any(|r| r.dest_key == dest_key) {
                            return Err(Error::Other("TSDB: compaction rule already exists"));
                        }

                        ts.rules.push(CompactionRule {
                            dest_key,
                            aggregation,
                            bucket_duration,
                            align_timestamp,
                        });
                        entry.bump_version();
                        Ok(())
                    }
                    _ => Err(Error::WrongType),
                }
            }
            crate::storage::dashtable::Entry::Vacant(_) => {
                Err(Error::Other("TSDB: the key does not exist"))
            }
        }
    }

    /// TS.DELETERULE sourceKey destKey
    pub fn ts_deleterule(&self, source_key: &[u8], dest_key: &[u8]) -> Result<()> {
        match self.data_entry(source_key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                match &mut entry.data {
                    DataType::TimeSeries(ts) => {
                        let original_len = ts.rules.len();
                        ts.rules.retain(|r| r.dest_key.as_ref() != dest_key);
                        if ts.rules.len() == original_len {
                            Err(Error::Other("TSDB: compaction rule does not exist"))
                        } else {
                            entry.bump_version();
                            Ok(())
                        }
                    }
                    _ => Err(Error::WrongType),
                }
            }
            crate::storage::dashtable::Entry::Vacant(_) => {
                Err(Error::Other("TSDB: the key does not exist"))
            }
        }
    }
}
