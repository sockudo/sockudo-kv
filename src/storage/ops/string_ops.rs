use bytes::Bytes;

use crate::error::{Error, Result};
use crate::storage::Store;
use crate::storage::types::{DataType, Entry};
use crate::storage::value::now_ms;

use std::sync::atomic::Ordering;

/// String operations for the Store
impl Store {
    // ==================== String operations ====================

    /// Get string value
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.data_get(key).and_then(|e| {
            if e.1.is_expired() {
                None
            } else {
                e.1.data.as_string().cloned()
            }
        })
    }

    /// Get string value with TTL
    #[inline]
    pub fn get_with_ttl(&self, key: &[u8]) -> Option<(Bytes, Option<i64>)> {
        self.data_get(key).and_then(|e| {
            if e.1.is_expired() {
                None
            } else {
                e.1.data.as_string().cloned().map(|v| (v, e.1.ttl_ms()))
            }
        })
    }

    /// Set string value
    #[inline]
    pub fn set(&self, key: Bytes, value: Bytes) {
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Vacant(e) => {
                e.insert((key, Entry::new(DataType::String(value))));
                self.key_count.fetch_add(1, Ordering::Relaxed);
            }
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                match &mut entry.data {
                    DataType::String(s) => {
                        *s = value;
                    }
                    _ => {
                        entry.data = DataType::String(value);
                    }
                }
                entry.persist();
                entry.bump_version();
            }
        }
    }

    /// Set with expiration (milliseconds)
    #[inline]
    pub fn set_ex(&self, key: Bytes, value: Bytes, expire_ms: i64) {
        let expire_at = now_ms() + expire_ms;
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Vacant(e) => {
                e.insert((
                    key.clone(),
                    Entry::with_expire(DataType::String(value), expire_at),
                ));
                self.key_count.fetch_add(1, Ordering::Relaxed);

                // Add to expiration index
                self.expiration_index.add(key, expire_at);
            }
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                let old_expire = entry.expire_at_ms();

                match &mut entry.data {
                    DataType::String(s) => {
                        *s = value;
                    }
                    _ => {
                        entry.data = DataType::String(value);
                    }
                }
                entry.set_expire_at(expire_at);
                entry.bump_version();

                // Update expiration index
                let key_bytes = e.get().0.clone();
                self.expiration_index
                    .update(key_bytes, old_expire, expire_at);
            }
        }
    }

    /// Set with expiration at timestamp (milliseconds)
    #[inline]
    pub fn set_exat(&self, key: Bytes, value: Bytes, expire_at_ms: i64) {
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Vacant(e) => {
                e.insert((
                    key.clone(),
                    Entry::with_expire(DataType::String(value), expire_at_ms),
                ));
                self.key_count.fetch_add(1, Ordering::Relaxed);

                // Add to expiration index
                self.expiration_index.add(key, expire_at_ms);
            }
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                let old_expire = entry.expire_at_ms();

                match &mut entry.data {
                    DataType::String(s) => {
                        *s = value;
                    }
                    _ => {
                        entry.data = DataType::String(value);
                    }
                }
                entry.set_expire_at(expire_at_ms);
                entry.bump_version();

                // Update expiration index
                let key_bytes = e.get().0.clone();
                self.expiration_index
                    .update(key_bytes, old_expire, expire_at_ms);
            }
        }
    }

    /// Set if not exists
    #[inline]
    pub fn setnx(&self, key: Bytes, value: Bytes) -> bool {
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                if e.get().1.is_expired() {
                    let entry = &mut e.get_mut().1;
                    entry.data = DataType::String(value);
                    entry.persist();
                    entry.bump_version();
                    true
                } else {
                    false
                }
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                e.insert((key, Entry::new(DataType::String(value))));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                true
            }
        }
    }

    /// Get and delete
    #[inline]
    pub fn getdel(&self, key: &[u8]) -> Option<Bytes> {
        self.data_remove(key).and_then(|(_, e)| {
            self.key_count.fetch_sub(1, Ordering::Relaxed);
            if e.is_expired() {
                None
            } else {
                e.data.as_string().cloned()
            }
        })
    }

    /// Append to string
    #[inline]
    pub fn append(&self, key: Bytes, value: &[u8]) -> Result<usize> {
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    entry.data = DataType::String(Bytes::copy_from_slice(value));
                    entry.persist();
                    entry.bump_version();
                    return Ok(value.len());
                }

                let result = match &mut entry.data {
                    DataType::String(s) => {
                        let mut new_val = Vec::with_capacity(s.len() + value.len());
                        new_val.extend_from_slice(s);
                        new_val.extend_from_slice(value);
                        *s = Bytes::from(new_val);
                        Ok(s.len())
                    }
                    _ => Err(Error::WrongType),
                };
                if result.is_ok() {
                    entry.bump_version();
                }
                result
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                let len = value.len();
                e.insert((
                    key,
                    Entry::new(DataType::String(Bytes::copy_from_slice(value))),
                ));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(len)
            }
        }
    }

    /// Get string length
    #[inline]
    pub fn strlen(&self, key: &[u8]) -> usize {
        self.data_get(key)
            .and_then(|e| {
                if e.1.is_expired() {
                    None
                } else {
                    e.1.data.as_string().map(|s| s.len())
                }
            })
            .unwrap_or(0)
    }

    /// Increment integer value
    #[inline]
    pub fn incr(&self, key: Bytes, delta: i64) -> Result<i64> {
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    entry.data = DataType::String(Bytes::from(delta.to_string()));
                    entry.persist();
                    entry.bump_version();
                    return Ok(delta);
                }

                match &mut entry.data {
                    DataType::String(s) => {
                        let val: i64 = std::str::from_utf8(s)
                            .map_err(|_| Error::NotInteger)?
                            .parse()
                            .map_err(|_| Error::NotInteger)?;
                        let new_val = val.checked_add(delta).ok_or(Error::Overflow)?;
                        *s = Bytes::from(new_val.to_string());
                        entry.bump_version();
                        Ok(new_val)
                    }
                    _ => Err(Error::WrongType),
                }
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                e.insert((
                    key,
                    Entry::new(DataType::String(Bytes::from(delta.to_string()))),
                ));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(delta)
            }
        }
    }

    /// Increment float value
    #[inline]
    pub fn incr_float(&self, key: Bytes, delta: f64) -> Result<f64> {
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    let result = format_float(delta);
                    entry.data = DataType::String(Bytes::from(result.clone()));
                    entry.persist();
                    entry.bump_version();
                    return Ok(delta);
                }

                match &mut entry.data {
                    DataType::String(s) => {
                        let val: f64 = std::str::from_utf8(s)
                            .map_err(|_| Error::NotFloat)?
                            .parse()
                            .map_err(|_| Error::NotFloat)?;
                        let new_val = val + delta;
                        if new_val.is_infinite() || new_val.is_nan() {
                            return Err(Error::Overflow);
                        }
                        let result = format_float(new_val);
                        *s = Bytes::from(result);
                        entry.bump_version();
                        Ok(new_val)
                    }
                    _ => Err(Error::WrongType),
                }
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                let result = format_float(delta);
                e.insert((key, Entry::new(DataType::String(Bytes::from(result)))));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(delta)
            }
        }
    }

    /// Get substring
    #[inline]
    pub fn getrange(&self, key: &[u8], start: i64, end: i64) -> Bytes {
        self.data_get(key)
            .and_then(|e| {
                if e.1.is_expired() {
                    return None;
                }
                e.1.data.as_string().map(|s| {
                    let len = s.len() as i64;
                    let start = normalize_index(start, len);
                    let end = normalize_index(end, len);

                    if start > end || start >= len {
                        return Bytes::new();
                    }

                    let start = start.max(0) as usize;
                    let end = (end + 1).min(len) as usize;
                    s.slice(start..end)
                })
            })
            .unwrap_or_default()
    }

    /// Set substring
    #[inline]
    pub fn setrange(&self, key: Bytes, offset: usize, value: &[u8]) -> Result<usize> {
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    let mut bytes = vec![0u8; offset + value.len()];
                    bytes[offset..offset + value.len()].copy_from_slice(value);
                    entry.data = DataType::String(Bytes::from(bytes));
                    entry.persist();
                    entry.bump_version();
                    return Ok(entry.data.as_string().unwrap().len());
                }

                let result = match &mut entry.data {
                    DataType::String(s) => {
                        let mut bytes = s.to_vec();
                        let new_len = offset + value.len();
                        if new_len > bytes.len() {
                            bytes.resize(new_len, 0);
                        }
                        bytes[offset..offset + value.len()].copy_from_slice(value);
                        *s = Bytes::from(bytes);
                        Ok(s.len())
                    }
                    _ => Err(Error::WrongType),
                };
                if result.is_ok() {
                    entry.bump_version();
                }
                result
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                let mut bytes = vec![0u8; offset + value.len()];
                bytes[offset..offset + value.len()].copy_from_slice(value);
                let len = bytes.len();
                e.insert((key, Entry::new(DataType::String(Bytes::from(bytes)))));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(len)
            }
        }
    }

    /// Multi-get
    #[inline]
    pub fn mget(&self, keys: &[Bytes]) -> Vec<Option<Bytes>> {
        keys.iter().map(|k| self.get(k)).collect()
    }

    /// Multi-set
    #[inline]
    pub fn mset(&self, pairs: Vec<(Bytes, Bytes)>) {
        for (key, value) in pairs {
            self.set(key, value);
        }
    }

    /// Multi-set with shared expiration (MSETEX)
    /// NX: Only set if none of the keys exist
    /// XX: Only set if all keys exist
    /// Returns true if operation was performed
    #[inline]
    pub fn mset_ex(&self, pairs: Vec<(Bytes, Bytes)>, expire_ms: i64, nx: bool, xx: bool) -> bool {
        // Pre-check NX/XX conditions
        if nx || xx {
            for (key, _) in &pairs {
                let exists = self.exists(key);
                if nx && exists {
                    return false; // NX: one key exists, abort
                }
                if xx && !exists {
                    return false; // XX: one key doesn't exist, abort
                }
            }
        }

        // Set all keys with expiration
        let expire_at = now_ms() + expire_ms;
        for (key, value) in pairs {
            match self.data_entry(&key) {
                crate::storage::dashtable::Entry::Vacant(e) => {
                    e.insert((
                        key.clone(),
                        Entry::with_expire(DataType::String(value), expire_at),
                    ));
                    self.key_count.fetch_add(1, Ordering::Relaxed);

                    // Add to expiration index
                    self.expiration_index.add(key, expire_at);
                }
                crate::storage::dashtable::Entry::Occupied(mut e) => {
                    let entry = &mut e.get_mut().1;
                    let old_expire = entry.expire_at_ms();

                    match &mut entry.data {
                        DataType::String(s) => {
                            *s = value;
                        }
                        _ => {
                            entry.data = DataType::String(value);
                        }
                    }
                    entry.set_expire_at(expire_at);
                    entry.bump_version();

                    // Update expiration index
                    let key_bytes = e.get().0.clone();
                    self.expiration_index
                        .update(key_bytes, old_expire, expire_at);
                }
            }
        }
        true
    }

    /// Multi-set with expiration at timestamp (for EXAT/PXAT)
    #[inline]
    pub fn mset_exat(
        &self,
        pairs: Vec<(Bytes, Bytes)>,
        expire_at_ms: i64,
        nx: bool,
        xx: bool,
    ) -> bool {
        // Pre-check NX/XX conditions
        if nx || xx {
            for (key, _) in &pairs {
                let exists = self.exists(key);
                if nx && exists {
                    return false;
                }
                if xx && !exists {
                    return false;
                }
            }
        }

        // Set all keys with expiration at
        for (key, value) in pairs {
            match self.data_entry(&key) {
                crate::storage::dashtable::Entry::Vacant(e) => {
                    e.insert((
                        key.clone(),
                        Entry::with_expire(DataType::String(value), expire_at_ms),
                    ));
                    self.key_count.fetch_add(1, Ordering::Relaxed);

                    // Add to expiration index
                    self.expiration_index.add(key, expire_at_ms);
                }
                crate::storage::dashtable::Entry::Occupied(mut e) => {
                    let entry = &mut e.get_mut().1;
                    let old_expire = entry.expire_at_ms();

                    match &mut entry.data {
                        DataType::String(s) => {
                            *s = value;
                        }
                        _ => {
                            entry.data = DataType::String(value);
                        }
                    }
                    entry.set_expire_at(expire_at_ms);
                    entry.bump_version();

                    // Update expiration index
                    let key_bytes = e.get().0.clone();
                    self.expiration_index
                        .update(key_bytes, old_expire, expire_at_ms);
                }
            }
        }
        true
    }
}

/// Normalize negative indices for string operations
fn normalize_index(idx: i64, len: i64) -> i64 {
    if idx < 0 {
        (len + idx).max(0)
    } else {
        idx.min(len - 1)
    }
}

/// Format float like Redis does
fn format_float(f: f64) -> String {
    if f.fract() == 0.0 && f.abs() < 1e17 {
        format!("{:.1}", f)
    } else {
        f.to_string()
    }
}
