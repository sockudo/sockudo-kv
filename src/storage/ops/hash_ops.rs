use crate::storage::dashtable::{DashTable, calculate_hash};
use crate::storage::listpack::{LISTPACK_HASH_MAX_ENTRIES, LISTPACK_MAX_ENTRY_SIZE, Listpack};
use bytes::Bytes;

use crate::error::{Error, Result};
use crate::storage::Store;
use crate::storage::types::{DataType, Entry};

use std::sync::atomic::Ordering;

/// Check if a field-value pair should use packed encoding
#[inline]
fn should_use_packed(field: &[u8], value: &[u8]) -> bool {
    field.len() <= LISTPACK_MAX_ENTRY_SIZE && value.len() <= LISTPACK_MAX_ENTRY_SIZE
}

/// Upgrade a HashPacked to a full Hash (DashTable)
#[inline]
fn upgrade_hash_packed_to_dashtable(lp: &Listpack) -> DashTable<(Bytes, Bytes)> {
    let hash = DashTable::new();
    for (field, value) in lp.iter() {
        let h = calculate_hash(&field);
        hash.insert_unique(h, (field, value), |kv| calculate_hash(&kv.0));
    }
    hash
}

/// Hash operations for the Store
impl Store {
    // ==================== Hash operations ====================

    /// Set field in hash. Uses packed encoding for small hashes.
    #[inline]
    pub fn hset(&self, key: Bytes, fields: Vec<(Bytes, Bytes)>) -> Result<usize> {
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    // Expired entry: create new
                    let should_pack = fields.len() < LISTPACK_HASH_MAX_ENTRIES
                        && fields.iter().all(|(f, v)| should_use_packed(f, v));

                    if should_pack {
                        let mut lp = Listpack::with_capacity(fields.len());
                        for (field, value) in &fields {
                            lp.insert(field, value);
                        }
                        let _count = lp.len();
                        entry.data = DataType::HashPacked(lp);
                    } else {
                        let hash = DashTable::new();
                        for (field, value) in &fields {
                            let h = calculate_hash(field);
                            hash.insert_unique(h, (field.clone(), value.clone()), |kv| {
                                calculate_hash(&kv.0)
                            });
                        }
                        entry.data = DataType::Hash(hash);
                    }
                    entry.persist();
                    entry.bump_version();
                    return Ok(fields.len());
                }

                match &mut entry.data {
                    DataType::HashPacked(lp) => {
                        let mut new_fields = 0;

                        // Check if any field is too large or if we'll exceed capacity
                        let needs_upgrade = fields.iter().any(|(f, v)| !should_use_packed(f, v))
                            || lp.len() + fields.len() > LISTPACK_HASH_MAX_ENTRIES;

                        if needs_upgrade {
                            // Upgrade to DashTable
                            let hash = upgrade_hash_packed_to_dashtable(lp);
                            for (field, value) in &fields {
                                let h = calculate_hash(field);
                                match hash.entry(h, |kv| kv.0 == *field, |kv| calculate_hash(&kv.0))
                                {
                                    crate::storage::dashtable::Entry::Occupied(mut e) => {
                                        e.get_mut().1 = value.clone();
                                    }
                                    crate::storage::dashtable::Entry::Vacant(e) => {
                                        e.insert((field.clone(), value.clone()));
                                        new_fields += 1;
                                    }
                                }
                            }
                            entry.data = DataType::Hash(hash);
                        } else {
                            // Stay packed
                            for (field, value) in &fields {
                                if lp.insert(field, value) {
                                    new_fields += 1;
                                }
                            }
                        }
                        entry.bump_version();
                        Ok(new_fields)
                    }
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
                // New key: use packed if small enough
                let should_pack = fields.len() < LISTPACK_HASH_MAX_ENTRIES
                    && fields.iter().all(|(f, v)| should_use_packed(f, v));

                let count = fields.len();
                if should_pack {
                    let mut lp = Listpack::with_capacity(fields.len());
                    for (field, value) in &fields {
                        lp.insert(field, value);
                    }
                    e.insert((key, Entry::new(DataType::HashPacked(lp))));
                } else {
                    let hash = DashTable::new();
                    for (field, value) in &fields {
                        let h = calculate_hash(field);
                        hash.insert_unique(h, (field.clone(), value.clone()), |kv| {
                            calculate_hash(&kv.0)
                        });
                    }
                    e.insert((key, Entry::new(DataType::Hash(hash))));
                }
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
                match &e.1.data {
                    DataType::HashPacked(lp) => lp.get(field),
                    DataType::Hash(h) => {
                        let h_val = calculate_hash(field);
                        h.get(h_val, |kv| kv.0 == field).map(|kv| kv.1.clone())
                    }
                    _ => None,
                }
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
                    DataType::HashPacked(lp) => {
                        let mut count = 0;
                        for field in fields {
                            if lp.remove(field).is_some() {
                                count += 1;
                            }
                        }
                        (count, lp.is_empty())
                    }
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
                match &e.1.data {
                    DataType::HashPacked(lp) => lp.iter().collect(),
                    DataType::Hash(hash) => {
                        hash.iter().map(|kv| (kv.0.clone(), kv.1.clone())).collect()
                    }
                    _ => vec![],
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
                match &e.1.data {
                    DataType::HashPacked(lp) => lp.len(),
                    DataType::Hash(hash) => hash.len(),
                    _ => 0,
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
                match &e.1.data {
                    DataType::HashPacked(lp) => lp.contains_key(field),
                    DataType::Hash(hash) => {
                        let h = calculate_hash(field);
                        hash.get(h, |kv| kv.0 == field).is_some()
                    }
                    _ => false,
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
                    // Create new packed hash with single field
                    let value_str = delta.to_string();
                    if should_use_packed(&field, value_str.as_bytes()) {
                        let mut lp = Listpack::new();
                        lp.insert(&field, value_str.as_bytes());
                        entry.data = DataType::HashPacked(lp);
                    } else {
                        let hash = DashTable::new();
                        let h = calculate_hash(&field);
                        hash.insert_unique(h, (field, Bytes::from(value_str)), |kv| {
                            calculate_hash(&kv.0)
                        });
                        entry.data = DataType::Hash(hash);
                    }
                    entry.persist();
                    entry.bump_version();
                    return Ok(delta);
                }

                let result = match &mut entry.data {
                    DataType::HashPacked(lp) => {
                        let current: i64 = match lp.get(&field) {
                            Some(v) => std::str::from_utf8(&v)
                                .map_err(|_| Error::NotInteger)?
                                .parse()
                                .map_err(|_| Error::NotInteger)?,
                            None => 0,
                        };
                        let new_val = current.checked_add(delta).ok_or(Error::Overflow)?;
                        let value_str = new_val.to_string();

                        // Check if we need to upgrade
                        if !should_use_packed(&field, value_str.as_bytes())
                            || (!lp.contains_key(&field) && lp.hash_at_capacity())
                        {
                            // Upgrade to DashTable
                            let hash = upgrade_hash_packed_to_dashtable(lp);
                            let h = calculate_hash(&field);
                            match hash.entry(h, |kv| kv.0 == field, |kv| calculate_hash(&kv.0)) {
                                crate::storage::dashtable::Entry::Occupied(mut e) => {
                                    e.get_mut().1 = Bytes::from(value_str);
                                }
                                crate::storage::dashtable::Entry::Vacant(e) => {
                                    e.insert((field, Bytes::from(value_str)));
                                }
                            }
                            entry.data = DataType::Hash(hash);
                        } else {
                            lp.insert(&field, value_str.as_bytes());
                        }
                        Ok(new_val)
                    }
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
                let value_str = delta.to_string();
                if should_use_packed(&field, value_str.as_bytes()) {
                    let mut lp = Listpack::new();
                    lp.insert(&field, value_str.as_bytes());
                    e.insert((key, Entry::new(DataType::HashPacked(lp))));
                } else {
                    let hash = DashTable::new();
                    let h = calculate_hash(&field);
                    hash.insert_unique(h, (field, Bytes::from(value_str)), |kv| {
                        calculate_hash(&kv.0)
                    });
                    e.insert((key, Entry::new(DataType::Hash(hash))));
                }
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
                    let value_str = delta.to_string();
                    if should_use_packed(&field, value_str.as_bytes()) {
                        let mut lp = Listpack::new();
                        lp.insert(&field, value_str.as_bytes());
                        entry.data = DataType::HashPacked(lp);
                    } else {
                        let hash = DashTable::new();
                        let h = calculate_hash(&field);
                        hash.insert_unique(h, (field, Bytes::from(value_str)), |kv| {
                            calculate_hash(&kv.0)
                        });
                        entry.data = DataType::Hash(hash);
                    }
                    entry.persist();
                    entry.bump_version();
                    return Ok(delta);
                }

                let result = match &mut entry.data {
                    DataType::HashPacked(lp) => {
                        let current: f64 = match lp.get(&field) {
                            Some(v) => std::str::from_utf8(&v)
                                .map_err(|_| Error::NotFloat)?
                                .parse()
                                .map_err(|_| Error::NotFloat)?,
                            None => 0.0,
                        };
                        let new_val = current + delta;
                        if new_val.is_infinite() || new_val.is_nan() {
                            return Err(Error::Overflow);
                        }
                        let value_str = new_val.to_string();

                        // Check if we need to upgrade
                        if !should_use_packed(&field, value_str.as_bytes())
                            || (!lp.contains_key(&field) && lp.hash_at_capacity())
                        {
                            let hash = upgrade_hash_packed_to_dashtable(lp);
                            let h = calculate_hash(&field);
                            match hash.entry(h, |kv| kv.0 == field, |kv| calculate_hash(&kv.0)) {
                                crate::storage::dashtable::Entry::Occupied(mut e) => {
                                    e.get_mut().1 = Bytes::from(value_str);
                                }
                                crate::storage::dashtable::Entry::Vacant(e) => {
                                    e.insert((field, Bytes::from(value_str)));
                                }
                            }
                            entry.data = DataType::Hash(hash);
                        } else {
                            lp.insert(&field, value_str.as_bytes());
                        }
                        Ok(new_val)
                    }
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
                let value_str = delta.to_string();
                if should_use_packed(&field, value_str.as_bytes()) {
                    let mut lp = Listpack::new();
                    lp.insert(&field, value_str.as_bytes());
                    e.insert((key, Entry::new(DataType::HashPacked(lp))));
                } else {
                    let hash = DashTable::new();
                    let h = calculate_hash(&field);
                    hash.insert_unique(h, (field, Bytes::from(value_str)), |kv| {
                        calculate_hash(&kv.0)
                    });
                    e.insert((key, Entry::new(DataType::Hash(hash))));
                }
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(delta)
            }
        }
    }
}
