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

    /// Get string value, returning WRONGTYPE error if key exists but is not a string
    /// This is used by SET ... GET to properly return errors for non-string types
    #[inline]
    pub fn get_or_wrongtype(&self, key: &[u8]) -> Result<Option<Bytes>> {
        match self.data_get(key) {
            Some(e) => {
                if e.1.is_expired() {
                    Ok(None)
                } else {
                    match &e.1.data {
                        DataType::String(s) => Ok(Some(s.clone())),
                        _ => Err(Error::WrongType),
                    }
                }
            }
            None => Ok(None),
        }
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
    #[inline]
    fn update_string_value(entry: &mut Entry, value: Bytes) {
        match &mut entry.data {
            DataType::String(s) => {
                *s = value;
            }
            _ => {
                entry.data = DataType::String(value);
            }
        }
    }

    #[inline]
    fn set_with_expire_at(&self, key: Bytes, value: Bytes, expire_at_ms: i64) {
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Vacant(e) => {
                let key_for_index = key.clone();
                e.insert((
                    key,
                    Entry::with_expire(DataType::String(value), expire_at_ms),
                ));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                self.expiration_index.add(key_for_index, expire_at_ms);
            }
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                let old_expire = entry.expire_at_ms();

                Self::update_string_value(entry, value);
                entry.set_expire_at(expire_at_ms);
                entry.bump_version();

                let key_bytes = e.get().0.clone();
                self.expiration_index
                    .update(key_bytes, old_expire, expire_at_ms);
            }
        }
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
                Self::update_string_value(entry, value);
                entry.persist();
                entry.bump_version();
            }
        }
    }

    /// Set with expiration (milliseconds)
    #[inline]
    pub fn set_ex(&self, key: Bytes, value: Bytes, expire_ms: i64) {
        let expire_at = now_ms() + expire_ms;
        self.set_with_expire_at(key, value, expire_at);
    }

    /// Set with expiration at timestamp (milliseconds)
    #[inline]
    pub fn set_exat(&self, key: Bytes, value: Bytes, expire_at_ms: i64) {
        self.set_with_expire_at(key, value, expire_at_ms);
    }

    /// Set if not exists
    #[inline]
    pub fn setnx(&self, key: Bytes, value: Bytes) -> bool {
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                if e.get().1.is_expired() {
                    let entry = &mut e.get_mut().1;
                    Self::update_string_value(entry, value);
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
                    // Use RawString for appended strings
                    entry.data = DataType::RawString(Bytes::copy_from_slice(value));
                    entry.persist();
                    entry.bump_version();
                    return Ok(value.len());
                }

                let result = match &mut entry.data {
                    DataType::String(s) | DataType::RawString(s) => {
                        let mut new_val = Vec::with_capacity(s.len() + value.len());
                        new_val.extend_from_slice(s);
                        new_val.extend_from_slice(value);
                        // Convert to RawString to mark as modified
                        let new_bytes = Bytes::from(new_val);
                        let len = new_bytes.len();
                        entry.data = DataType::RawString(new_bytes);
                        Ok(len)
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
                // Use RawString for new strings created by APPEND
                e.insert((
                    key,
                    Entry::new(DataType::RawString(Bytes::copy_from_slice(value))),
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

    /// Decrement integer value
    #[inline]
    pub fn decr(&self, key: Bytes, delta: i64) -> Result<i64> {
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    // New value is 0 - delta
                    let val = 0i64.checked_sub(delta).ok_or(Error::Overflow)?;
                    entry.data = DataType::String(Bytes::from(val.to_string()));
                    entry.persist();
                    entry.bump_version();
                    return Ok(val);
                }

                match &mut entry.data {
                    DataType::String(s) => {
                        let val: i64 = std::str::from_utf8(s)
                            .map_err(|_| Error::NotInteger)?
                            .parse()
                            .map_err(|_| Error::NotInteger)?;
                        let new_val = val.checked_sub(delta).ok_or(Error::Overflow)?;
                        *s = Bytes::from(new_val.to_string());
                        entry.bump_version();
                        Ok(new_val)
                    }
                    _ => Err(Error::WrongType),
                }
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                // New value is 0 - delta
                let val = 0i64.checked_sub(delta).ok_or(Error::Overflow)?;
                e.insert((
                    key,
                    Entry::new(DataType::String(Bytes::from(val.to_string()))),
                ));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(val)
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

    /// Get substring - Redis-compatible GETRANGE implementation
    /// Ported from Redis t_string.c getrangeCommand()
    #[inline]
    pub fn getrange(&self, key: &[u8], start: i64, end: i64) -> crate::error::Result<Bytes> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return Ok(Bytes::new());
                }
                match &entry_ref.1.data {
                    DataType::String(s) => {
                        let strlen = s.len() as i64;

                        // Redis: Convert negative indexes
                        // Special case: if both are negative and start > end, return empty
                        if start < 0 && end < 0 && start > end {
                            return Ok(Bytes::new());
                        }

                        let mut real_start = if start < 0 { strlen + start } else { start };
                        let mut real_end = if end < 0 { strlen + end } else { end };

                        // Clamp to valid range
                        if real_start < 0 {
                            real_start = 0;
                        }
                        if real_end < 0 {
                            real_end = 0;
                        }
                        if real_end >= strlen {
                            real_end = strlen - 1;
                        }

                        // Check for empty result conditions
                        if real_start > real_end || strlen == 0 {
                            return Ok(Bytes::new());
                        }

                        Ok(Bytes::copy_from_slice(
                            &s[real_start as usize..=real_end as usize],
                        ))
                    }
                    _ => Err(crate::error::Error::WrongType),
                }
            }
            None => Ok(Bytes::new()),
        }
    }

    /// Set substring - Redis-compatible SETRANGE implementation
    /// Ported from Redis t_string.c setrangeCommand()
    #[inline]
    pub fn setrange(&self, key: Bytes, offset: usize, value: &[u8]) -> Result<usize> {
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    // Redis: If key doesn't exist (or is expired) and value is empty, return 0
                    if value.is_empty() {
                        return Ok(0);
                    }
                    let mut bytes = vec![0u8; offset + value.len()];
                    bytes[offset..offset + value.len()].copy_from_slice(value);
                    // Use RawString for modified strings
                    entry.data = DataType::RawString(Bytes::from(bytes));
                    entry.persist();
                    entry.bump_version();
                    return Ok(entry.data.as_string().unwrap().len());
                }

                let result = match &mut entry.data {
                    DataType::String(s) | DataType::RawString(s) => {
                        // Redis: If value is empty, just return current length
                        if value.is_empty() {
                            return Ok(s.len());
                        }
                        let mut bytes = s.to_vec();
                        let new_len = offset + value.len();
                        if new_len > bytes.len() {
                            bytes.resize(new_len, 0);
                        }
                        bytes[offset..offset + value.len()].copy_from_slice(value);
                        // Convert to RawString to mark as modified
                        let new_bytes = Bytes::from(bytes);
                        let len = new_bytes.len();
                        entry.data = DataType::RawString(new_bytes);
                        Ok(len)
                    }
                    _ => Err(Error::WrongType),
                };
                if result.is_ok() {
                    entry.bump_version();
                }
                result
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                // Redis: If key doesn't exist and value is empty, return 0 without creating key
                if value.is_empty() {
                    return Ok(0);
                }
                let mut bytes = vec![0u8; offset + value.len()];
                bytes[offset..offset + value.len()].copy_from_slice(value);
                let len = bytes.len();
                // Use RawString for new strings created by SETRANGE
                e.insert((key, Entry::new(DataType::RawString(Bytes::from(bytes)))));
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

    /// Multi-set keeping existing TTL (MSET KEEPTTL)
    #[inline]
    pub fn mset_keepttl(&self, pairs: Vec<(Bytes, Bytes)>, nx: bool, xx: bool) -> bool {
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

        // Set all keys keeping existing expiration
        for (key, value) in pairs {
            match self.data_entry(&key) {
                crate::storage::dashtable::Entry::Vacant(e) => {
                    // New key - no expiration to keep, so just insert
                    e.insert((key, Entry::new(DataType::String(value))));
                    self.key_count.fetch_add(1, Ordering::Relaxed);
                }
                crate::storage::dashtable::Entry::Occupied(mut e) => {
                    let entry = &mut e.get_mut().1;

                    // If entry is expired, it's effectively new, so we treat it as new (clearing expiration)
                    if entry.is_expired() {
                        entry.data = DataType::String(value);
                        entry.persist(); // Clear expiration since it was expired
                        entry.bump_version();
                    } else {
                        // Alive entry - update value but keep metadata (expiration)
                        match &mut entry.data {
                            DataType::String(s) => {
                                *s = value;
                            }
                            _ => {
                                entry.data = DataType::String(value);
                            }
                        }
                        // Explicitly NOT calling persist() here to preserve expiration
                        entry.bump_version();
                    }
                }
            }
        }
        true
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
