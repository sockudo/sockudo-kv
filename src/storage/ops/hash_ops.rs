use crate::storage::dashtable::{DashTable, calculate_hash};
use bytes::Bytes;

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
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    let hash = DashTable::new();
                    for (field, value) in &fields {
                        let h = calculate_hash(field);
                        hash.insert_unique(h, (field.clone(), value.clone()), |kv| {
                            calculate_hash(&kv.0)
                        });
                    }
                    let count = hash.len();
                    entry.data = DataType::Hash(hash);
                    entry.persist();
                    entry.bump_version();
                    return Ok(count);
                }

                match &mut entry.data {
                    DataType::Hash(hash) => {
                        let mut new_fields = 0;
                        for (field, value) in &fields {
                            let h = calculate_hash(field);
                            match hash.entry(h, |kv| kv.0 == *field, |kv| calculate_hash(&kv.0)) {
                                crate::storage::dashtable::Entry::Occupied(mut e) => {
                                    e.get_mut().1 = value.clone();
                                }
                                crate::storage::dashtable::Entry::Vacant(e) => {
                                    e.insert((field.clone(), value.clone()));
                                    new_fields += 1;
                                }
                            }
                        }
                        entry.bump_version();
                        Ok(new_fields)
                    }
                    _ => Err(Error::WrongType),
                }
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                let hash = DashTable::new();
                for (field, value) in &fields {
                    let h = calculate_hash(field);
                    hash.insert_unique(h, (field.clone(), value.clone()), |kv| {
                        calculate_hash(&kv.0)
                    });
                }
                let count = hash.len();
                e.insert((key, Entry::new(DataType::Hash(hash))));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(count)
            }
        }
    }

    /// Get field from hash
    #[inline]
    pub fn hget(&self, key: &[u8], field: &[u8]) -> Option<Bytes> {
        self.data_get(key).and_then(|e| {
            if e.1.is_expired() {
                None
            } else {
                e.1.data.as_hash().and_then(|h| {
                    let h_val = calculate_hash(field);
                    match h.get(h_val, |kv| kv.0 == field) {
                        Some(kv) => Some(kv.1.clone()),
                        None => None,
                    }
                })
            }
        })
    }

    /// Delete fields from hash
    #[inline]
    pub fn hdel(&self, key: &[u8], fields: &[Bytes]) -> usize {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return 0;
                }

                let (count, is_empty) = match &mut entry.data {
                    DataType::Hash(hash) => {
                        let mut count = 0;
                        for field in fields {
                            let h = calculate_hash(field);
                            if let crate::storage::dashtable::Entry::Occupied(e) =
                                hash.entry(h, |kv| kv.0 == *field, |kv| calculate_hash(&kv.0))
                            {
                                e.remove();
                                count += 1;
                            }
                        }
                        (count, hash.is_empty())
                    }
                    _ => (0, false),
                };

                if count > 0 {
                    entry.bump_version();
                }
                if is_empty {
                    e.remove();
                }
                count
            }
            crate::storage::dashtable::Entry::Vacant(_) => 0,
        }
    }

    /// Get all fields and values from hash
    #[inline]
    pub fn hgetall(&self, key: &[u8]) -> Vec<(Bytes, Bytes)> {
        match self.data_get(key) {
            Some(e) => {
                if e.1.is_expired() {
                    return vec![];
                }
                match e.1.data.as_hash() {
                    Some(hash) => hash.iter().map(|kv| (kv.0.clone(), kv.1.clone())).collect(),
                    None => vec![],
                }
            }
            None => vec![],
        }
    }

    /// Get hash length
    #[inline]
    pub fn hlen(&self, key: &[u8]) -> usize {
        match self.data_get(key) {
            Some(e) => {
                if e.1.is_expired() {
                    return 0;
                }
                match e.1.data.as_hash() {
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
        match self.data_get(key) {
            Some(e) => {
                if e.1.is_expired() {
                    return false;
                }
                match e.1.data.as_hash() {
                    Some(hash) => {
                        let h = calculate_hash(field);
                        hash.get(h, |kv| kv.0 == field).is_some()
                    }
                    None => false,
                }
            }
            None => false,
        }
    }

    #[inline]
    pub fn hincrby(&self, key: Bytes, field: Bytes, delta: i64) -> Result<i64> {
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    let hash = DashTable::new();
                    let h = calculate_hash(&field);
                    hash.insert_unique(h, (field, Bytes::from(delta.to_string())), |kv| {
                        calculate_hash(&kv.0)
                    });
                    entry.data = DataType::Hash(hash);
                    entry.persist();
                    entry.bump_version();
                    return Ok(delta);
                }

                let result = match &mut entry.data {
                    DataType::Hash(hash) => {
                        let h = calculate_hash(&field);
                        match hash.entry(h, |kv| kv.0 == field, |kv| calculate_hash(&kv.0)) {
                            crate::storage::dashtable::Entry::Occupied(mut e) => {
                                let kv = e.get_mut();
                                let current: i64 = std::str::from_utf8(&kv.1)
                                    .map_err(|_| Error::NotInteger)?
                                    .parse()
                                    .map_err(|_| Error::NotInteger)?;
                                let new_val = current.checked_add(delta).ok_or(Error::Overflow)?;
                                kv.1 = Bytes::from(new_val.to_string());
                                Ok(new_val)
                            }
                            crate::storage::dashtable::Entry::Vacant(e) => {
                                e.insert((field, Bytes::from(delta.to_string())));
                                Ok(delta)
                            }
                        }
                    }
                    _ => Err(Error::WrongType),
                };

                if result.is_ok() {
                    entry.bump_version();
                }
                result
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                let hash = DashTable::new();
                let h = calculate_hash(&field);
                hash.insert_unique(h, (field, Bytes::from(delta.to_string())), |kv| {
                    calculate_hash(&kv.0)
                });
                e.insert((key, Entry::new(DataType::Hash(hash))));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(delta)
            }
        }
    }

    #[inline]
    pub fn hincrbyfloat(&self, key: Bytes, field: Bytes, delta: f64) -> Result<f64> {
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    let hash = DashTable::new();
                    let h = calculate_hash(&field);
                    hash.insert_unique(h, (field, Bytes::from(delta.to_string())), |kv| {
                        calculate_hash(&kv.0)
                    });
                    entry.data = DataType::Hash(hash);
                    entry.persist();
                    entry.bump_version();
                    return Ok(delta);
                }

                let result = match &mut entry.data {
                    DataType::Hash(hash) => {
                        let h = calculate_hash(&field);
                        match hash.entry(h, |kv| kv.0 == field, |kv| calculate_hash(&kv.0)) {
                            crate::storage::dashtable::Entry::Occupied(mut e) => {
                                let kv = e.get_mut();
                                let current: f64 = std::str::from_utf8(&kv.1)
                                    .map_err(|_| Error::NotFloat)?
                                    .parse()
                                    .map_err(|_| Error::NotFloat)?;
                                let result = current + delta;
                                if result.is_infinite() || result.is_nan() {
                                    return Err(Error::Overflow);
                                }
                                kv.1 = Bytes::from(result.to_string());
                                Ok(result)
                            }
                            crate::storage::dashtable::Entry::Vacant(e) => {
                                e.insert((field, Bytes::from(delta.to_string())));
                                Ok(delta)
                            }
                        }
                    }
                    _ => Err(Error::WrongType),
                };

                if result.is_ok() {
                    entry.bump_version();
                }
                result
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                let hash = DashTable::new();
                let h = calculate_hash(&field);
                hash.insert_unique(h, (field, Bytes::from(delta.to_string())), |kv| {
                    calculate_hash(&kv.0)
                });
                e.insert((key, Entry::new(DataType::Hash(hash))));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(delta)
            }
        }
    }
}
