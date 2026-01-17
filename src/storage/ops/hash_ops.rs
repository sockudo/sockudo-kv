use crate::storage::dashtable::{DashTable, calculate_hash};
use crate::storage::listpack::Listpack;
use bytes::Bytes;

use crate::error::{Error, Result};
use crate::storage::Store;
use crate::storage::types::{DataType, Entry};

use std::sync::atomic::Ordering;

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
                    let max_entries = self.encoding.read().hash_max_listpack_entries;
                    let max_value = self.encoding.read().hash_max_listpack_value;
                    let should_pack = fields.len() < max_entries
                        && fields
                            .iter()
                            .all(|(f, v)| f.len() <= max_value && v.len() <= max_value);

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
                        let max_entries = self.encoding.read().hash_max_listpack_entries;
                        let max_value = self.encoding.read().hash_max_listpack_value;
                        let needs_upgrade = fields
                            .iter()
                            .any(|(f, v)| f.len() > max_value || v.len() > max_value)
                            || lp.len() + fields.len() > max_entries;

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
                let max_entries = self.encoding.read().hash_max_listpack_entries;
                let max_value = self.encoding.read().hash_max_listpack_value;
                let should_pack = fields.len() < max_entries
                    && fields
                        .iter()
                        .all(|(f, v)| f.len() <= max_value && v.len() <= max_value);

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
    /// Get field from hash
    #[inline]
    pub fn hget(&self, key: &[u8], field: &[u8]) -> Result<Option<Bytes>> {
        self.data_get(key).map_or(Ok(None), |e| {
            if e.1.is_expired() {
                Ok(None)
            } else {
                match &e.1.data {
                    DataType::HashPacked(lp) => Ok(lp.get(field)),
                    DataType::Hash(h) => {
                        let h_val = calculate_hash(field);
                        Ok(h.get(h_val, |kv| kv.0 == field).map(|kv| kv.1.clone()))
                    }
                    _ => Err(Error::WrongType),
                }
            }
        })
    }

    /// Delete fields from hash
    #[inline]
    pub fn hdel(&self, key: &[u8], fields: &[Bytes]) -> Result<usize> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return Ok(0);
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
                    _ => return Err(Error::WrongType),
                };

                if count > 0 {
                    entry.bump_version();
                }
                if is_empty {
                    e.remove();
                }
                Ok(count)
            }
            crate::storage::dashtable::Entry::Vacant(_) => Ok(0),
        }
    }

    /// Get all fields and values from hash
    #[inline]
    pub fn hgetall(&self, key: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        match self.data_get(key) {
            Some(e) => {
                if e.1.is_expired() {
                    return Ok(vec![]);
                }
                match &e.1.data {
                    DataType::HashPacked(lp) => Ok(lp.iter().collect()),
                    DataType::Hash(hash) => {
                        Ok(hash.iter().map(|kv| (kv.0.clone(), kv.1.clone())).collect())
                    }
                    _ => Err(Error::WrongType),
                }
            }
            None => Ok(vec![]),
        }
    }

    /// Get hash length
    #[inline]
    pub fn hlen(&self, key: &[u8]) -> Result<usize> {
        match self.data_get(key) {
            Some(e) => {
                if e.1.is_expired() {
                    return Ok(0);
                }
                match &e.1.data {
                    DataType::HashPacked(lp) => Ok(lp.len()),
                    DataType::Hash(hash) => Ok(hash.len()),
                    _ => Err(Error::WrongType),
                }
            }
            None => Ok(0),
        }
    }

    /// Check if hash field exists
    #[inline]
    pub fn hexists(&self, key: &[u8], field: &[u8]) -> Result<bool> {
        match self.data_get(key) {
            Some(e) => {
                if e.1.is_expired() {
                    return Ok(false);
                }
                match &e.1.data {
                    DataType::HashPacked(lp) => Ok(lp.contains_key(field)),
                    DataType::Hash(hash) => {
                        let h = calculate_hash(field);
                        Ok(hash.get(h, |kv| kv.0 == field).is_some())
                    }
                    _ => Err(Error::WrongType),
                }
            }
            None => Ok(false),
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
                    let max_value = self.encoding.read().hash_max_listpack_value;
                    if field.len() <= max_value && value_str.len() <= max_value {
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
                        let max_value = self.encoding.read().hash_max_listpack_value;
                        let max_entries = self.encoding.read().hash_max_listpack_entries;
                        if !(field.len() <= max_value && value_str.len() <= max_value)
                            || (!lp.contains_key(&field) && lp.len() >= max_entries)
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
                let max_value = self.encoding.read().hash_max_listpack_value;
                if field.len() <= max_value && value_str.len() <= max_value {
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
                    let max_value = self.encoding.read().hash_max_listpack_value;
                    if field.len() <= max_value && value_str.len() <= max_value {
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
                            return Err(Error::Custom(
                                "ERR increment would produce NaN or Infinity".into(),
                            ));
                        }
                        let value_str = new_val.to_string();

                        // Check if we need to upgrade
                        let max_value = self.encoding.read().hash_max_listpack_value;
                        let max_entries = self.encoding.read().hash_max_listpack_entries;
                        if !(field.len() <= max_value && value_str.len() <= max_value)
                            || (!lp.contains_key(&field) && lp.len() >= max_entries)
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
                                    return Err(Error::Custom(
                                        "ERR increment would produce NaN or Infinity".into(),
                                    ));
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
                let max_value = self.encoding.read().hash_max_listpack_value;
                if field.len() <= max_value && value_str.len() <= max_value {
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

    /// HRANDFIELD - return random field(s) from hash, with optional values
    /// count: positive = unique fields, negative = allow duplicates
    /// with_values: if true, return field-value pairs
    pub fn hrandfield(&self, key: &[u8], count: i64, with_values: bool) -> Result<Vec<Bytes>> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return Ok(vec![]);
                }
                match &entry_ref.1.data {
                    DataType::Hash(hash) => {
                        if hash.is_empty() {
                            return Ok(vec![]);
                        }
                        let allow_duplicates = count < 0;
                        let want = count.unsigned_abs() as usize;

                        if allow_duplicates {
                            // Allow duplicates: sample with replacement
                            let fields: Vec<(Bytes, Bytes)> =
                                hash.iter().map(|r| r.clone()).collect();
                            let mut result =
                                Vec::with_capacity(if with_values { want * 2 } else { want });
                            for _ in 0..want {
                                let idx = fastrand::usize(..fields.len());
                                result.push(fields[idx].0.clone());
                                if with_values {
                                    result.push(fields[idx].1.clone());
                                }
                            }
                            Ok(result)
                        } else {
                            // Unique fields only - use Fisher-Yates sampling
                            let hash_len = hash.len();
                            let take = want.min(hash_len);
                            let mut result =
                                Vec::with_capacity(if with_values { take * 2 } else { take });

                            if take == hash_len {
                                // Return all fields
                                for (field, value) in hash.iter() {
                                    result.push(field.clone());
                                    if with_values {
                                        result.push(value.clone());
                                    }
                                }
                            } else {
                                // Random sampling without replacement
                                let all_fields: Vec<(Bytes, Bytes)> =
                                    hash.iter().map(|r| r.clone()).collect();
                                let mut indices: Vec<usize> = (0..hash_len).collect();

                                // Fisher-Yates shuffle first `take` elements
                                for i in 0..take {
                                    let j = fastrand::usize(i..hash_len);
                                    indices.swap(i, j);
                                }

                                for i in 0..take {
                                    let idx = indices[i];
                                    result.push(all_fields[idx].0.clone());
                                    if with_values {
                                        result.push(all_fields[idx].1.clone());
                                    }
                                }
                            }
                            Ok(result)
                        }
                    }
                    DataType::HashPacked(lp) => {
                        if lp.len() == 0 {
                            return Ok(vec![]);
                        }
                        let allow_duplicates = count < 0;
                        let want = count.unsigned_abs() as usize;

                        if allow_duplicates {
                            // Allow duplicates: sample with replacement
                            let fields: Vec<(Bytes, Bytes)> = lp.iter().collect();
                            let mut result =
                                Vec::with_capacity(if with_values { want * 2 } else { want });
                            for _ in 0..want {
                                let idx = fastrand::usize(..fields.len());
                                result.push(fields[idx].0.clone());
                                if with_values {
                                    result.push(fields[idx].1.clone());
                                }
                            }
                            Ok(result)
                        } else {
                            // Unique fields only - use Fisher-Yates sampling
                            let lp_len = lp.len();
                            let take = want.min(lp_len);
                            let mut result =
                                Vec::with_capacity(if with_values { take * 2 } else { take });

                            if take == lp_len {
                                // Return all fields
                                for (field, value) in lp.iter() {
                                    result.push(field);
                                    if with_values {
                                        result.push(value);
                                    }
                                }
                            } else {
                                // Random sampling without replacement
                                let all_fields: Vec<(Bytes, Bytes)> = lp.iter().collect();
                                let mut indices: Vec<usize> = (0..lp_len).collect();

                                // Fisher-Yates shuffle first `take` elements
                                for i in 0..take {
                                    let j = fastrand::usize(i..lp_len);
                                    indices.swap(i, j);
                                }

                                for i in 0..take {
                                    let idx = indices[i];
                                    result.push(all_fields[idx].0.clone());
                                    if with_values {
                                        result.push(all_fields[idx].1.clone());
                                    }
                                }
                            }
                            Ok(result)
                        }
                    }
                    _ => Err(Error::WrongType),
                }
            }
            None => Ok(vec![]),
        }
    }
    /// Get and delete fields from hash (HGETDEL multi-field)
    #[inline]
    pub fn hgetdel(&self, key: &[u8], fields: &[Bytes]) -> Result<Vec<Option<Bytes>>> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return Ok(vec![None; fields.len()]);
                }

                let mut results = Vec::with_capacity(fields.len());
                let mut deletions = 0;

                match &mut entry.data {
                    DataType::HashPacked(lp) => {
                        for field in fields {
                            if let Some(val) = lp.remove(field) {
                                results.push(Some(val));
                                deletions += 1;
                            } else {
                                results.push(None);
                            }
                        }
                    }
                    DataType::Hash(hash) => {
                        for field in fields {
                            let h = calculate_hash(field);
                            if let crate::storage::dashtable::Entry::Occupied(e) =
                                hash.entry(h, |kv| kv.0 == *field, |kv| calculate_hash(&kv.0))
                            {
                                let (_, val) = e.remove();
                                results.push(Some(val));
                                deletions += 1;
                            } else {
                                results.push(None);
                            }
                        }
                    }
                    _ => return Err(Error::WrongType),
                }

                if deletions > 0 {
                    entry.bump_version();
                    let is_empty = match &entry.data {
                        DataType::HashPacked(lp) => lp.is_empty(),
                        DataType::Hash(hash) => hash.is_empty(),
                        _ => false,
                    };

                    if is_empty {
                        e.remove();
                    }
                }
                Ok(results)
            }
            crate::storage::dashtable::Entry::Vacant(_) => Ok(vec![None; fields.len()]),
        }
    }

    /// Scan hash fields
    pub fn hscan(
        &self,
        key: &[u8],
        cursor: u64,
        pattern: Option<&[u8]>,
        count: usize,
        no_values: bool,
    ) -> Result<(u64, Vec<Bytes>)> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return Ok((0, vec![]));
                }

                let mut result = Vec::new();
                match &entry_ref.1.data {
                    DataType::HashPacked(lp) => {
                        // Linear scan for small packed hashes
                        for (field, value) in lp.iter() {
                            let matches =
                                pattern.map_or(true, |p| crate::storage::match_pattern(p, &field));
                            if matches {
                                result.push(field);
                                if !no_values {
                                    result.push(value);
                                }
                            }
                        }
                        Ok((0, result))
                    }
                    DataType::Hash(hash) => {
                        // Naive implementation: collect all fields and paginate
                        // TODO: Implement proper cursor-based scanning for DashTable
                        let all_fields: Vec<(Bytes, Bytes)> =
                            hash.iter().map(|kv| (kv.0.clone(), kv.1.clone())).collect();

                        let total = all_fields.len();
                        let start = cursor as usize;
                        if start >= total {
                            return Ok((0, vec![]));
                        }

                        let end = (start + count).min(total);
                        for i in start..end {
                            let (field, value) = &all_fields[i];
                            let matches =
                                pattern.map_or(true, |p| crate::storage::match_pattern(p, field));
                            if matches {
                                result.push(field.clone());
                                if !no_values {
                                    result.push(value.clone());
                                }
                            }
                        }

                        let next_cursor = if end >= total { 0 } else { end as u64 };
                        Ok((next_cursor, result))
                    }
                    _ => Err(Error::WrongType),
                }
            }
            None => Ok((0, vec![])),
        }
    }
}
