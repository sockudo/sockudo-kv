use bytes::Bytes;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry as DashEntry;

use crate::error::{Error, Result};
use crate::storage::Store;
use crate::storage::types::{DataType, Entry};

use std::sync::atomic::Ordering;

/// Hash operations for the Store
impl Store {
    // ==================== Hash operations ====================

    /// Set field in hash
    #[inline]
    pub fn hset(&self, key: Bytes, fields: Vec<(Bytes, Bytes)>) -> Result<usize> {
        match self.data.entry(key) {
            DashEntry::Occupied(mut e) => {
                let entry = e.get_mut();
                if entry.is_expired() {
                    let hash = DashMap::new();
                    for (field, value) in &fields {
                        hash.insert(field.clone(), value.clone());
                    }
                    let count = hash.len();
                    entry.data = DataType::Hash(hash);
                    entry.persist();
                    entry.bump_version();
                    return Ok(count);
                }

                match &entry.data {
                    DataType::Hash(hash) => {
                        let mut new_fields = 0;
                        for (field, value) in &fields {
                            if hash.insert(field.clone(), value.clone()).is_none() {
                                new_fields += 1;
                            }
                        }
                        entry.bump_version();
                        Ok(new_fields)
                    }
                    _ => Err(Error::WrongType),
                }
            }
            DashEntry::Vacant(e) => {
                let hash = DashMap::new();
                for (field, value) in &fields {
                    hash.insert(field.clone(), value.clone());
                }
                let count = hash.len();
                e.insert(Entry::new(DataType::Hash(hash)));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(count)
            }
        }
    }

    /// Get field from hash
    #[inline]
    pub fn hget(&self, key: &[u8], field: &[u8]) -> Option<Bytes> {
        self.data.get(key).and_then(|e| {
            if e.is_expired() {
                None
            } else {
                e.data
                    .as_hash()
                    .and_then(|h| h.get(field).map(|v| v.clone()))
            }
        })
    }

    /// Delete fields from hash
    #[inline]
    pub fn hdel(&self, key: &[u8], fields: &[Bytes]) -> usize {
        match self.data.get_mut(key) {
            Some(mut e) => {
                let entry = e.value_mut();
                if entry.is_expired() {
                    drop(e);
                    self.del(key);
                    return 0;
                }

                match &entry.data {
                    DataType::Hash(hash) => {
                        let mut count = 0;
                        for field in fields {
                            if hash.remove(field).is_some() {
                                count += 1;
                            }
                        }
                        if count > 0 {
                            entry.bump_version();
                        }
                        if hash.is_empty() {
                            drop(e);
                            self.del(key);
                        }
                        count
                    }
                    _ => 0,
                }
            }
            None => 0,
        }
    }

    /// Get all fields and values from hash
    #[inline]
    pub fn hgetall(&self, key: &[u8]) -> Vec<(Bytes, Bytes)> {
        match self.data.get(key) {
            Some(e) => {
                if e.is_expired() {
                    return vec![];
                }
                match e.data.as_hash() {
                    Some(hash) => hash
                        .iter()
                        .map(|kv| (kv.key().clone(), kv.value().clone()))
                        .collect(),
                    None => vec![],
                }
            }
            None => vec![],
        }
    }

    /// Get hash length
    #[inline]
    pub fn hlen(&self, key: &[u8]) -> usize {
        match self.data.get(key) {
            Some(e) => {
                if e.is_expired() {
                    return 0;
                }
                match e.data.as_hash() {
                    Some(hash) => hash.len(),
                    None => 0,
                }
            }
            None => 0,
        }
    }

    /// Check if hash field exists
    #[inline]
    pub fn hexists(&self, key: &[u8], field: &[u8]) -> bool {
        match self.data.get(key) {
            Some(e) => {
                if e.is_expired() {
                    return false;
                }
                match e.data.as_hash() {
                    Some(hash) => hash.contains_key(field),
                    None => false,
                }
            }
            None => false,
        }
    }

    /// Increment hash field by integer
    #[inline]
    pub fn hincrby(&self, key: Bytes, field: Bytes, delta: i64) -> Result<i64> {
        match self.data.entry(key) {
            DashEntry::Occupied(mut e) => {
                let entry = e.get_mut();
                if entry.is_expired() {
                    let hash = DashMap::new();
                    hash.insert(field, Bytes::from(delta.to_string()));
                    entry.data = DataType::Hash(hash);
                    entry.persist();
                    entry.bump_version();
                    return Ok(delta);
                }

                match &entry.data {
                    DataType::Hash(hash) => match hash.get(&field) {
                        Some(val) => {
                            let current: i64 = std::str::from_utf8(&val)
                                .map_err(|_| Error::NotInteger)?
                                .parse()
                                .map_err(|_| Error::NotInteger)?;
                            let new_val = current.checked_add(delta).ok_or(Error::Overflow)?;
                            drop(val);
                            hash.insert(field, Bytes::from(new_val.to_string()));
                            entry.bump_version();
                            Ok(new_val)
                        }
                        None => {
                            hash.insert(field, Bytes::from(delta.to_string()));
                            entry.bump_version();
                            Ok(delta)
                        }
                    },
                    _ => Err(Error::WrongType),
                }
            }
            DashEntry::Vacant(e) => {
                let hash = DashMap::new();
                hash.insert(field, Bytes::from(delta.to_string()));
                e.insert(Entry::new(DataType::Hash(hash)));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(delta)
            }
        }
    }
    #[inline]
    pub fn hincrbyfloat(&self, key: Bytes, field: Bytes, delta: f64) -> Result<f64> {
        match self.data.entry(key) {
            DashEntry::Occupied(mut e) => {
                let entry = e.get_mut();
                if entry.is_expired() {
                    let hash = DashMap::new();
                    hash.insert(field, Bytes::from(delta.to_string()));
                    entry.data = DataType::Hash(hash);
                    entry.persist();
                    entry.bump_version();
                    return Ok(delta);
                }

                match &entry.data {
                    DataType::Hash(hash) => {
                        let new_val = if let Some(val) = hash.get(&field) {
                            let current: f64 = std::str::from_utf8(&val)
                                .map_err(|_| Error::NotFloat)?
                                .parse()
                                .map_err(|_| Error::NotFloat)?;
                            let result = current + delta;
                            // Check for overflow (infinity) or NaN
                            if result.is_infinite() || result.is_nan() {
                                return Err(Error::Overflow);
                            }
                            result
                        } else {
                            delta
                        };
                        hash.insert(field, Bytes::from(new_val.to_string()));
                        entry.bump_version();
                        Ok(new_val)
                    }
                    _ => Err(Error::WrongType),
                }
            }
            DashEntry::Vacant(e) => {
                let hash = DashMap::new();
                hash.insert(field, Bytes::from(delta.to_string()));
                e.insert(Entry::new(DataType::Hash(hash)));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(delta)
            }
        }
    }
}
