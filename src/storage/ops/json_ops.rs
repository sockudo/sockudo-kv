//! JSON operations for the Store using sonic-rs for hyper-performance
//!
//! Performance optimizations:
//! - sonic-rs SIMD parsing (2-3x faster than serde_json)
//! - Lazy parsing when possible
//! - Zero-copy string references
//! - In-place mutations

use bytes::Bytes;
use dashmap::mapref::entry::Entry as DashEntry;
use sonic_rs::{JsonContainerTrait, JsonValueMutTrait, JsonValueTrait, Value, from_slice, to_vec};

use crate::error::{Error, Result};
use crate::storage::Store;
use crate::storage::types::{DataType, Entry};

use std::sync::atomic::Ordering;

/// Helper to get JSON type name
#[inline]
fn json_type_name(v: &Value) -> &'static str {
    if v.is_null() {
        "null"
    } else if v.is_boolean() {
        "boolean"
    } else if v.is_number() {
        "number"
    } else if v.is_str() {
        "string"
    } else if v.is_array() {
        "array"
    } else if v.is_object() {
        "object"
    } else {
        "unknown"
    }
}

/// Normalize negative array index
#[inline]
fn normalize_array_index(idx: i64, len: usize) -> Option<usize> {
    if idx >= 0 {
        let idx = idx as usize;
        if idx < len { Some(idx) } else { None }
    } else {
        let abs_idx = (-idx) as usize;
        if abs_idx <= len {
            Some(len - abs_idx)
        } else {
            None
        }
    }
}

/// JSON operations for the Store
impl Store {
    // ==================== Core JSON operations ====================

    /// JSON.SET key path value [NX|XX]
    /// NX: Only set if path doesn't exist
    /// XX: Only set if path exists
    #[inline]
    pub fn json_set(
        &self,
        key: Bytes,
        path: &str,
        value: &[u8],
        nx: bool,
        xx: bool,
    ) -> Result<bool> {
        // Parse the JSON value first
        let json_value: Value = from_slice(value).map_err(|_| Error::JsonParse)?;

        let is_root = path == "$" || path == "." || path.is_empty();

        match self.data.entry(key) {
            DashEntry::Vacant(e) => {
                if xx {
                    return Ok(false);
                }
                if !is_root {
                    return Err(Error::JsonNewAtRoot);
                }
                e.insert(Entry::new(DataType::Json(Box::new(json_value))));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(true)
            }
            DashEntry::Occupied(mut e) => {
                let entry = e.get_mut();
                if entry.is_expired() {
                    if xx {
                        return Ok(false);
                    }
                    if !is_root {
                        return Err(Error::JsonNewAtRoot);
                    }
                    entry.data = DataType::Json(Box::new(json_value));
                    entry.persist();
                    entry.bump_version();
                    return Ok(true);
                }

                let root = match &mut entry.data {
                    DataType::Json(j) => j,
                    _ => return Err(Error::WrongType),
                };

                if is_root {
                    if nx && !root.is_null() {
                        return Ok(false);
                    }
                    **root = json_value;
                    entry.bump_version();
                    return Ok(true);
                }

                // Handle nested path
                let path_set = json_path_set(root, path, json_value, nx, xx)?;
                if path_set {
                    entry.bump_version();
                }
                Ok(path_set)
            }
        }
    }

    /// JSON.GET key [path ...]
    /// Returns JSON string for the value(s) at path(s)
    #[inline]
    pub fn json_get(&self, key: &[u8], paths: &[&str]) -> Option<Bytes> {
        let entry = self.data.get(key)?;
        if entry.is_expired() {
            return None;
        }

        let root = entry.data.as_json()?;

        if paths.is_empty()
            || (paths.len() == 1 && (paths[0] == "$" || paths[0] == "." || paths[0].is_empty()))
        {
            // Return entire JSON
            let bytes = to_vec(root).ok()?;
            return Some(Bytes::from(bytes));
        }

        if paths.len() == 1 {
            // Single path - return value directly
            let result = json_path_get(root, paths[0])?;
            if result.len() == 1 {
                let bytes = to_vec(&result[0]).ok()?;
                return Some(Bytes::from(bytes));
            }
            // Multiple matches - return array
            let arr: Vec<&Value> = result.iter().collect();
            let bytes = to_vec(&arr).ok()?;
            return Some(Bytes::from(bytes));
        }

        // Multiple paths - return object with path -> array mapping
        let mut result_map: Vec<(&str, Vec<Value>)> = Vec::new();
        for path in paths {
            if let Some(values) = json_path_get(root, path) {
                result_map.push((path, values));
            }
        }
        let bytes = to_vec(&result_map).ok()?;
        Some(Bytes::from(bytes))
    }

    /// JSON.DEL / JSON.FORGET key [path]
    /// Returns number of paths deleted
    #[inline]
    pub fn json_del(&self, key: &[u8], path: Option<&str>) -> Result<usize> {
        let is_root = match path {
            None => true,
            Some(p) if p == "$" || p == "." || p.is_empty() => true,
            _ => false,
        };

        if is_root {
            // Delete entire key
            return Ok(if self.del(key) { 1 } else { 0 });
        }

        let mut entry = match self.data.get_mut(key) {
            Some(e) => e,
            None => return Ok(0),
        };

        if entry.is_expired() {
            drop(entry);
            self.del(key);
            return Ok(0);
        }

        let root = match &mut entry.data {
            DataType::Json(j) => j,
            _ => return Err(Error::WrongType),
        };

        let deleted = json_path_delete(root, path.unwrap())?;
        if deleted > 0 {
            entry.bump_version();
        }
        Ok(deleted)
    }

    /// JSON.TYPE key [path]
    #[inline]
    pub fn json_type(&self, key: &[u8], path: Option<&str>) -> Option<Vec<&'static str>> {
        let entry = self.data.get(key)?;
        if entry.is_expired() {
            return None;
        }

        let root = entry.data.as_json()?;

        let is_root = match path {
            None => true,
            Some(p) if p == "$" || p == "." || p.is_empty() => true,
            _ => false,
        };

        if is_root {
            return Some(vec![json_type_name(root)]);
        }

        let values = json_path_get(root, path?)?;
        Some(values.iter().map(|v| json_type_name(v)).collect())
    }

    // ==================== Array operations ====================

    /// JSON.ARRAPPEND key path value [value ...]
    #[inline]
    pub fn json_arrappend(
        &self,
        key: &[u8],
        path: &str,
        values: Vec<Value>,
    ) -> Result<Vec<Option<i64>>> {
        let mut entry = match self.data.get_mut(key) {
            Some(e) => e,
            None => return Err(Error::JsonKeyNotFound),
        };

        if entry.is_expired() {
            drop(entry);
            self.del(key);
            return Err(Error::JsonKeyNotFound);
        }

        let e = entry.value_mut();
        let root = match &mut e.data {
            DataType::Json(j) => j,
            _ => return Err(Error::WrongType),
        };

        let result = json_array_append(root, path, values)?;
        if result.iter().any(|r| r.is_some()) {
            e.bump_version();
        }
        Ok(result)
    }

    /// JSON.ARRINDEX key path value [start [stop]]
    #[inline]
    pub fn json_arrindex(
        &self,
        key: &[u8],
        path: &str,
        value: &Value,
        start: i64,
        stop: i64,
    ) -> Result<Vec<Option<i64>>> {
        let entry = match self.data.get(key) {
            Some(e) => e,
            None => return Err(Error::JsonKeyNotFound),
        };

        if entry.is_expired() {
            return Err(Error::JsonKeyNotFound);
        }

        let root = match &entry.data {
            DataType::Json(j) => j,
            _ => return Err(Error::WrongType),
        };

        json_array_index(root, path, value, start, stop)
    }

    /// JSON.ARRINSERT key path index value [value ...]
    #[inline]
    pub fn json_arrinsert(
        &self,
        key: &[u8],
        path: &str,
        index: i64,
        values: Vec<Value>,
    ) -> Result<Vec<Option<i64>>> {
        let mut entry = match self.data.get_mut(key) {
            Some(e) => e,
            None => return Err(Error::JsonKeyNotFound),
        };

        if entry.is_expired() {
            drop(entry);
            self.del(key);
            return Err(Error::JsonKeyNotFound);
        }

        let root = match &mut entry.data {
            DataType::Json(j) => j,
            _ => return Err(Error::WrongType),
        };

        json_array_insert(root, path, index, values)
    }

    /// JSON.ARRLEN key [path]
    #[inline]
    pub fn json_arrlen(&self, key: &[u8], path: Option<&str>) -> Result<Vec<Option<i64>>> {
        let entry = match self.data.get(key) {
            Some(e) => e,
            None => return Ok(vec![None]),
        };

        if entry.is_expired() {
            return Ok(vec![None]);
        }

        let root = match &entry.data {
            DataType::Json(j) => j,
            _ => return Err(Error::WrongType),
        };

        let path = path.unwrap_or("$");
        json_array_len(root, path)
    }

    /// JSON.ARRPOP key [path [index]]
    #[inline]
    pub fn json_arrpop(
        &self,
        key: &[u8],
        path: Option<&str>,
        index: i64,
    ) -> Result<Vec<Option<Value>>> {
        let mut entry = match self.data.get_mut(key) {
            Some(e) => e,
            None => return Ok(vec![None]),
        };

        if entry.is_expired() {
            drop(entry);
            self.del(key);
            return Ok(vec![None]);
        }

        let root = match &mut entry.data {
            DataType::Json(j) => j,
            _ => return Err(Error::WrongType),
        };

        let path = path.unwrap_or("$");
        json_array_pop(root, path, index)
    }

    /// JSON.ARRTRIM key path start stop
    #[inline]
    pub fn json_arrtrim(
        &self,
        key: &[u8],
        path: &str,
        start: i64,
        stop: i64,
    ) -> Result<Vec<Option<i64>>> {
        let mut entry = match self.data.get_mut(key) {
            Some(e) => e,
            None => return Err(Error::JsonKeyNotFound),
        };

        if entry.is_expired() {
            drop(entry);
            self.del(key);
            return Err(Error::JsonKeyNotFound);
        }

        let root = match &mut entry.data {
            DataType::Json(j) => j,
            _ => return Err(Error::WrongType),
        };

        json_array_trim(root, path, start, stop)
    }

    // ==================== Object operations ====================

    /// JSON.OBJKEYS key [path]
    #[inline]
    pub fn json_objkeys(&self, key: &[u8], path: Option<&str>) -> Result<Vec<Option<Vec<String>>>> {
        let entry = match self.data.get(key) {
            Some(e) => e,
            None => return Ok(vec![None]),
        };

        if entry.is_expired() {
            return Ok(vec![None]);
        }

        let root = match &entry.data {
            DataType::Json(j) => j,
            _ => return Err(Error::WrongType),
        };

        let path = path.unwrap_or("$");
        json_object_keys(root, path)
    }

    /// JSON.OBJLEN key [path]
    #[inline]
    pub fn json_objlen(&self, key: &[u8], path: Option<&str>) -> Result<Vec<Option<i64>>> {
        let entry = match self.data.get(key) {
            Some(e) => e,
            None => return Ok(vec![None]),
        };

        if entry.is_expired() {
            return Ok(vec![None]);
        }

        let root = match &entry.data {
            DataType::Json(j) => j,
            _ => return Err(Error::WrongType),
        };

        let path = path.unwrap_or("$");
        json_object_len(root, path)
    }

    // ==================== Number operations ====================

    /// JSON.NUMINCRBY key path value
    #[inline]
    pub fn json_numincrby(&self, key: &[u8], path: &str, value: f64) -> Result<Vec<Option<f64>>> {
        let mut entry = match self.data.get_mut(key) {
            Some(e) => e,
            None => return Err(Error::JsonKeyNotFound),
        };

        if entry.is_expired() {
            drop(entry);
            self.del(key);
            return Err(Error::JsonKeyNotFound);
        }

        let root = match &mut entry.data {
            DataType::Json(j) => j,
            _ => return Err(Error::WrongType),
        };

        json_num_op(root, path, value, |a, b| a + b)
    }

    /// JSON.NUMMULTBY key path value
    #[inline]
    pub fn json_nummultby(&self, key: &[u8], path: &str, value: f64) -> Result<Vec<Option<f64>>> {
        let mut entry = match self.data.get_mut(key) {
            Some(e) => e,
            None => return Err(Error::JsonKeyNotFound),
        };

        if entry.is_expired() {
            drop(entry);
            self.del(key);
            return Err(Error::JsonKeyNotFound);
        }

        let root = match &mut entry.data {
            DataType::Json(j) => j,
            _ => return Err(Error::WrongType),
        };

        json_num_op(root, path, value, |a, b| a * b)
    }

    // ==================== String operations ====================

    /// JSON.STRAPPEND key [path] value
    #[inline]
    pub fn json_strappend(&self, key: &[u8], path: &str, value: &str) -> Result<Vec<Option<i64>>> {
        let mut entry = match self.data.get_mut(key) {
            Some(e) => e,
            None => return Err(Error::JsonKeyNotFound),
        };

        if entry.is_expired() {
            drop(entry);
            self.del(key);
            return Err(Error::JsonKeyNotFound);
        }

        let root = match &mut entry.data {
            DataType::Json(j) => j,
            _ => return Err(Error::WrongType),
        };

        json_str_append(root, path, value)
    }

    /// JSON.STRLEN key [path]
    #[inline]
    pub fn json_strlen(&self, key: &[u8], path: Option<&str>) -> Result<Vec<Option<i64>>> {
        let entry = match self.data.get(key) {
            Some(e) => e,
            None => return Ok(vec![None]),
        };

        if entry.is_expired() {
            return Ok(vec![None]);
        }

        let root = match &entry.data {
            DataType::Json(j) => j,
            _ => return Err(Error::WrongType),
        };

        let path = path.unwrap_or("$");
        json_str_len(root, path)
    }

    // ==================== Utility operations ====================

    /// JSON.TOGGLE key path
    #[inline]
    pub fn json_toggle(&self, key: &[u8], path: &str) -> Result<Vec<Option<bool>>> {
        let mut entry = match self.data.get_mut(key) {
            Some(e) => e,
            None => return Err(Error::JsonKeyNotFound),
        };

        if entry.is_expired() {
            drop(entry);
            self.del(key);
            return Err(Error::JsonKeyNotFound);
        }

        let root = match &mut entry.data {
            DataType::Json(j) => j,
            _ => return Err(Error::WrongType),
        };

        json_toggle_impl(root, path)
    }

    /// JSON.CLEAR key [path]
    #[inline]
    pub fn json_clear(&self, key: &[u8], path: Option<&str>) -> Result<usize> {
        let mut entry = match self.data.get_mut(key) {
            Some(e) => e,
            None => return Ok(0),
        };

        if entry.is_expired() {
            drop(entry);
            self.del(key);
            return Ok(0);
        }

        let root = match &mut entry.data {
            DataType::Json(j) => j,
            _ => return Err(Error::WrongType),
        };

        let path = path.unwrap_or("$");
        json_clear_impl(root, path)
    }

    /// JSON.MERGE key path value
    #[inline]
    pub fn json_merge(&self, key: Bytes, path: &str, value: &[u8]) -> Result<()> {
        let merge_value: Value = from_slice(value).map_err(|_| Error::JsonParse)?;

        match self.data.entry(key) {
            DashEntry::Vacant(e) => {
                if path == "$" || path == "." || path.is_empty() {
                    e.insert(Entry::new(DataType::Json(Box::new(merge_value))));
                    self.key_count.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                } else {
                    Err(Error::JsonNewAtRoot)
                }
            }
            DashEntry::Occupied(mut e) => {
                let entry = e.get_mut();
                if entry.is_expired() {
                    if path == "$" || path == "." || path.is_empty() {
                        entry.data = DataType::Json(Box::new(merge_value));
                        entry.persist();
                        return Ok(());
                    } else {
                        return Err(Error::JsonNewAtRoot);
                    }
                }

                let root = match &mut entry.data {
                    DataType::Json(j) => j,
                    _ => return Err(Error::WrongType),
                };

                json_merge_impl(root, path, merge_value)
            }
        }
    }

    /// JSON.MGET key [key ...] path
    #[inline]
    pub fn json_mget(&self, keys: &[Bytes], path: &str) -> Vec<Option<Bytes>> {
        keys.iter()
            .map(|key| {
                let entry = self.data.get(key.as_ref())?;
                if entry.is_expired() {
                    return None;
                }
                let root = entry.data.as_json()?;
                let values = json_path_get(root, path)?;
                if values.is_empty() {
                    return None;
                }
                let result = if values.len() == 1 {
                    to_vec(&values[0]).ok()?
                } else {
                    to_vec(&values).ok()?
                };
                Some(Bytes::from(result))
            })
            .collect()
    }

    /// JSON.MSET key path value [key path value ...]
    #[inline]
    pub fn json_mset(&self, items: Vec<(Bytes, &str, &[u8])>) -> Result<()> {
        for (key, path, value) in items {
            self.json_set(key, path, value, false, false)?;
        }
        Ok(())
    }

    /// JSON.RESP key [path]
    #[inline]
    pub fn json_resp(&self, key: &[u8], path: Option<&str>) -> Result<Vec<Option<Value>>> {
        let entry = match self.data.get(key) {
            Some(e) => e,
            None => return Ok(vec![None]),
        };

        if entry.is_expired() {
            return Ok(vec![None]);
        }

        let root = match &entry.data {
            DataType::Json(j) => j,
            _ => return Err(Error::WrongType),
        };

        let path = path.unwrap_or("$");
        Ok(json_path_get(root, path)
            .unwrap_or_default()
            .into_iter()
            .map(Some)
            .collect())
    }

    /// JSON.DEBUG MEMORY key [path]
    #[inline]
    pub fn json_debug_memory(&self, key: &[u8], path: Option<&str>) -> Result<Vec<Option<usize>>> {
        let entry = match self.data.get(key) {
            Some(e) => e,
            None => return Ok(vec![None]),
        };

        if entry.is_expired() {
            return Ok(vec![None]);
        }

        let root = match &entry.data {
            DataType::Json(j) => j,
            _ => return Err(Error::WrongType),
        };

        let path = path.unwrap_or("$");
        let values = json_path_get(root, path).unwrap_or_default();

        Ok(values
            .iter()
            .map(|v| {
                let bytes = to_vec(v).ok()?;
                Some(bytes.len())
            })
            .collect())
    }
}

// ==================== JSONPath Implementation ====================

/// Parse simple JSONPath and get matching values
/// Supports: $, .field, [index], [*]
fn json_path_get(root: &Value, path: &str) -> Option<Vec<Value>> {
    if path == "$" || path == "." || path.is_empty() {
        return Some(vec![root.clone()]);
    }

    let path = path.trim_start_matches('$').trim_start_matches('.');
    if path.is_empty() {
        return Some(vec![root.clone()]);
    }

    let mut results = vec![root.clone()];

    for part in split_path(path) {
        let mut new_results = Vec::new();

        for value in &results {
            if part == "*" {
                // Wildcard - get all children
                if let Some(arr) = value.as_array() {
                    for item in arr {
                        new_results.push(item.clone());
                    }
                } else if let Some(obj) = value.as_object() {
                    for (_, v) in obj.iter() {
                        new_results.push(v.clone());
                    }
                }
            } else if part.starts_with('[') && part.ends_with(']') {
                // Array index or quoted key
                let inner = &part[1..part.len() - 1];
                if let Ok(idx) = inner.parse::<i64>() {
                    if let Some(arr) = value.as_array()
                        && let Some(normalized) = normalize_array_index(idx, arr.len())
                        && let Some(v) = arr.get(normalized)
                    {
                        new_results.push(v.clone());
                    }
                } else {
                    // Quoted key like ['field']
                    let key = inner.trim_matches(|c| c == '\'' || c == '"');
                    if let Some(v) = value.get(key) {
                        new_results.push(v.clone());
                    }
                }
            } else {
                // Object field
                if let Some(v) = value.get(part) {
                    new_results.push(v.clone());
                }
            }
        }

        results = new_results;
        if results.is_empty() {
            return None;
        }
    }

    Some(results)
}

/// Set value at path
fn json_path_set(root: &mut Value, path: &str, value: Value, nx: bool, xx: bool) -> Result<bool> {
    let path = path.trim_start_matches('$').trim_start_matches('.');
    if path.is_empty() {
        *root = value;
        return Ok(true);
    }

    let parts: Vec<&str> = split_path(path);
    set_nested(root, &parts, value, nx, xx)
}

/// Helper to set nested value
fn set_nested(
    current: &mut Value,
    parts: &[&str],
    value: Value,
    nx: bool,
    xx: bool,
) -> Result<bool> {
    if parts.is_empty() {
        if nx && !current.is_null() {
            return Ok(false);
        }
        *current = value;
        return Ok(true);
    }

    let part = parts[0];
    let remaining = &parts[1..];

    if part == "*" {
        // Set all children
        let mut count = 0;
        if let Some(arr) = current.as_array_mut() {
            for item in arr.iter_mut() {
                if set_nested(item, remaining, value.clone(), nx, xx)? {
                    count += 1;
                }
            }
        } else if let Some(obj) = current.as_object_mut() {
            for (_, v) in obj.iter_mut() {
                if set_nested(v, remaining, value.clone(), nx, xx)? {
                    count += 1;
                }
            }
        }
        return Ok(count > 0);
    }

    if part.starts_with('[') && part.ends_with(']') {
        let inner = &part[1..part.len() - 1];
        if let Ok(idx) = inner.parse::<i64>() {
            if let Some(arr) = current.as_array_mut()
                && let Some(normalized) = normalize_array_index(idx, arr.len())
            {
                if remaining.is_empty() {
                    if xx && arr.get(normalized).map(|v| v.is_null()).unwrap_or(true) {
                        return Ok(false);
                    }
                    if nx && arr.get(normalized).map(|v| !v.is_null()).unwrap_or(false) {
                        return Ok(false);
                    }
                    if let Some(elem) = arr.get_mut(normalized) {
                        *elem = value;
                        return Ok(true);
                    }
                } else if let Some(elem) = arr.get_mut(normalized) {
                    return set_nested(elem, remaining, value, nx, xx);
                }
            }
            return Ok(false);
        }
    }

    // Object field
    if let Some(obj) = current.as_object_mut() {
        if remaining.is_empty() {
            let exists = obj
                .get(&part.to_string())
                .map(|v| !v.is_null())
                .unwrap_or(false);
            if xx && !exists {
                return Ok(false);
            }
            if nx && exists {
                return Ok(false);
            }
            obj.insert(&part.to_string(), value);
            return Ok(true);
        } else {
            // Navigate or create intermediate object
            if obj.get(&part.to_string()).is_none() {
                obj.insert(&part.to_string(), Value::new_object());
            }
            if let Some(child) = obj.get_mut(&part.to_string()) {
                return set_nested(child, remaining, value, nx, xx);
            }
        }
    }

    Ok(false)
}

/// Delete value at path, returns count of deleted
fn json_path_delete(root: &mut Value, path: &str) -> Result<usize> {
    let path = path.trim_start_matches('$').trim_start_matches('.');
    if path.is_empty() {
        return Ok(0); // Can't delete root with path delete
    }

    let parts: Vec<&str> = split_path(path);
    delete_nested(root, &parts)
}

fn delete_nested(current: &mut Value, parts: &[&str]) -> Result<usize> {
    if parts.is_empty() {
        return Ok(0);
    }

    if parts.len() == 1 {
        let part = parts[0];
        if part == "*" {
            if let Some(arr) = current.as_array_mut() {
                let len = arr.len();
                arr.clear();
                return Ok(len);
            } else if let Some(obj) = current.as_object_mut() {
                let len = obj.len();
                obj.clear();
                return Ok(len);
            }
        } else if part.starts_with('[') && part.ends_with(']') {
            let inner = &part[1..part.len() - 1];
            if let Ok(idx) = inner.parse::<i64>()
                && let Some(arr) = current.as_array_mut()
                && let Some(normalized) = normalize_array_index(idx, arr.len())
            {
                arr.remove(normalized);
                return Ok(1);
            }
        } else if let Some(obj) = current.as_object_mut()
            && obj.remove(&part.to_string()).is_some()
        {
            return Ok(1);
        }
        return Ok(0);
    }

    let part = parts[0];
    let remaining = &parts[1..];

    let mut count = 0;
    if part == "*" {
        if let Some(arr) = current.as_array_mut() {
            for item in arr.iter_mut() {
                count += delete_nested(item, remaining)?;
            }
        } else if let Some(obj) = current.as_object_mut() {
            for (_, v) in obj.iter_mut() {
                count += delete_nested(v, remaining)?;
            }
        }
    } else if part.starts_with('[') && part.ends_with(']') {
        let inner = &part[1..part.len() - 1];
        if let Ok(idx) = inner.parse::<i64>()
            && let Some(arr) = current.as_array_mut()
            && let Some(normalized) = normalize_array_index(idx, arr.len())
            && let Some(elem) = arr.get_mut(normalized)
        {
            count += delete_nested(elem, remaining)?;
        }
    } else if let Some(obj) = current.as_object_mut()
        && let Some(child) = obj.get_mut(&part.to_string())
    {
        count += delete_nested(child, remaining)?;
    }

    Ok(count)
}

/// Split JSONPath into parts
fn split_path(path: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut current_start = 0;
    let mut in_bracket = false;
    let bytes = path.as_bytes();

    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'[' => in_bracket = true,
            b']' => in_bracket = false,
            b'.' if !in_bracket => {
                if i > current_start {
                    parts.push(&path[current_start..i]);
                }
                current_start = i + 1;
            }
            _ => {}
        }
    }

    if current_start < path.len() {
        parts.push(&path[current_start..]);
    }

    parts
}

// ==================== Array Operation Helpers ====================

fn json_array_append(root: &mut Value, path: &str, values: Vec<Value>) -> Result<Vec<Option<i64>>> {
    let targets = get_mutable_targets(root, path)?;
    let mut results = Vec::new();

    for target in targets {
        if let Some(arr) = target.as_array_mut() {
            for v in &values {
                arr.push(v.clone());
            }
            results.push(Some(arr.len() as i64));
        } else {
            results.push(None);
        }
    }

    Ok(results)
}

fn json_array_index(
    root: &Value,
    path: &str,
    value: &Value,
    start: i64,
    stop: i64,
) -> Result<Vec<Option<i64>>> {
    let values = json_path_get(root, path).unwrap_or_default();
    let mut results = Vec::new();

    for v in values {
        if let Some(arr) = v.as_array() {
            let len = arr.len();
            let start_idx = if start >= 0 {
                start as usize
            } else {
                (len as i64 + start).max(0) as usize
            };
            let stop_idx = if stop == 0 || stop >= len as i64 {
                len
            } else if stop < 0 {
                (len as i64 + stop).max(0) as usize
            } else {
                stop as usize
            };

            let mut found = -1i64;
            for (i, elem) in arr
                .iter()
                .enumerate()
                .skip(start_idx)
                .take(stop_idx.saturating_sub(start_idx))
            {
                if elem == value {
                    found = i as i64;
                    break;
                }
            }
            results.push(Some(found));
        } else {
            results.push(None);
        }
    }

    Ok(results)
}

fn json_array_insert(
    root: &mut Value,
    path: &str,
    index: i64,
    values: Vec<Value>,
) -> Result<Vec<Option<i64>>> {
    let targets = get_mutable_targets(root, path)?;
    let mut results = Vec::new();

    for target in targets {
        if let Some(arr) = target.as_array_mut() {
            let idx = if index >= 0 {
                (index as usize).min(arr.len())
            } else {
                let abs = (-index) as usize;
                arr.len().saturating_sub(abs)
            };

            for (i, v) in values.iter().enumerate() {
                arr.insert(idx + i, v.clone());
            }
            results.push(Some(arr.len() as i64));
        } else {
            results.push(None);
        }
    }

    Ok(results)
}

fn json_array_len(root: &Value, path: &str) -> Result<Vec<Option<i64>>> {
    let values = json_path_get(root, path).unwrap_or_default();
    Ok(values
        .iter()
        .map(|v| v.as_array().map(|a| a.len() as i64))
        .collect())
}

fn json_array_pop(root: &mut Value, path: &str, index: i64) -> Result<Vec<Option<Value>>> {
    let targets = get_mutable_targets(root, path)?;
    let mut results = Vec::new();

    for target in targets {
        if let Some(arr) = target.as_array_mut() {
            if arr.is_empty() {
                results.push(None);
                continue;
            }

            let idx = if index == -1 {
                arr.len() - 1
            } else if let Some(normalized) = normalize_array_index(index, arr.len()) {
                normalized
            } else {
                results.push(None);
                continue;
            };

            // Get value before removing (sonic-rs remove returns ())
            let removed_value = arr.get(idx).cloned();
            arr.remove(idx);
            results.push(removed_value);
        } else {
            results.push(None);
        }
    }

    Ok(results)
}

fn json_array_trim(
    root: &mut Value,
    path: &str,
    start: i64,
    stop: i64,
) -> Result<Vec<Option<i64>>> {
    let targets = get_mutable_targets(root, path)?;
    let mut results = Vec::new();

    for target in targets {
        if let Some(arr) = target.as_array_mut() {
            let len = arr.len();
            let start_idx = if start >= 0 {
                (start as usize).min(len)
            } else {
                (len as i64 + start).max(0) as usize
            };
            let stop_idx = if stop >= 0 {
                ((stop + 1) as usize).min(len)
            } else {
                (len as i64 + stop + 1).max(0) as usize
            };

            if start_idx >= stop_idx || start_idx >= len {
                arr.clear();
                results.push(Some(0));
            } else {
                // Create new array with only the elements in range
                let new_arr: Vec<Value> = arr
                    .iter()
                    .skip(start_idx)
                    .take(stop_idx - start_idx)
                    .cloned()
                    .collect();
                arr.clear();
                for item in new_arr {
                    arr.push(item);
                }
                results.push(Some(arr.len() as i64));
            }
        } else {
            results.push(None);
        }
    }

    Ok(results)
}

// ==================== Object Operation Helpers ====================

fn json_object_keys(root: &Value, path: &str) -> Result<Vec<Option<Vec<String>>>> {
    let values = json_path_get(root, path).unwrap_or_default();
    Ok(values
        .iter()
        .map(|v| {
            v.as_object()
                .map(|o| o.iter().map(|(k, _)| k.to_string()).collect())
        })
        .collect())
}

fn json_object_len(root: &Value, path: &str) -> Result<Vec<Option<i64>>> {
    let values = json_path_get(root, path).unwrap_or_default();
    Ok(values
        .iter()
        .map(|v| v.as_object().map(|o| o.len() as i64))
        .collect())
}

// ==================== Number Operation Helpers ====================

fn json_num_op<F>(root: &mut Value, path: &str, value: f64, op: F) -> Result<Vec<Option<f64>>>
where
    F: Fn(f64, f64) -> f64,
{
    let targets = get_mutable_targets(root, path)?;
    let mut results = Vec::new();

    for target in targets {
        if target.is_i64() {
            let current = target.as_i64().unwrap() as f64;
            let new_val = op(current, value);
            if new_val.fract() == 0.0 && new_val >= i64::MIN as f64 && new_val <= i64::MAX as f64 {
                *target = sonic_rs::json!(new_val as i64);
            } else {
                *target = sonic_rs::json!(new_val);
            }
            results.push(Some(new_val));
        } else if target.is_f64() {
            let current = target.as_f64().unwrap();
            let new_val = op(current, value);
            *target = sonic_rs::json!(new_val);
            results.push(Some(new_val));
        } else {
            results.push(None);
        }
    }

    Ok(results)
}

// ==================== String Operation Helpers ====================

fn json_str_append(root: &mut Value, path: &str, value: &str) -> Result<Vec<Option<i64>>> {
    let targets = get_mutable_targets(root, path)?;
    let mut results = Vec::new();

    for target in targets {
        if let Some(s) = target.as_str() {
            let new_str = format!("{}{}", s, value);
            let len = new_str.len() as i64;
            *target = sonic_rs::json!(new_str);
            results.push(Some(len));
        } else {
            results.push(None);
        }
    }

    Ok(results)
}

fn json_str_len(root: &Value, path: &str) -> Result<Vec<Option<i64>>> {
    let values = json_path_get(root, path).unwrap_or_default();
    Ok(values
        .iter()
        .map(|v| v.as_str().map(|s| s.len() as i64))
        .collect())
}

// ==================== Utility Operation Helpers ====================

fn json_toggle_impl(root: &mut Value, path: &str) -> Result<Vec<Option<bool>>> {
    let targets = get_mutable_targets(root, path)?;
    let mut results = Vec::new();

    for target in targets {
        if let Some(b) = target.as_bool() {
            let new_val = !b;
            *target = sonic_rs::json!(new_val);
            results.push(Some(new_val));
        } else {
            results.push(None);
        }
    }

    Ok(results)
}

fn json_clear_impl(root: &mut Value, path: &str) -> Result<usize> {
    let targets = get_mutable_targets(root, path)?;
    let mut count = 0;

    for target in targets {
        if target.is_array() {
            if let Some(arr) = target.as_array_mut()
                && !arr.is_empty()
            {
                arr.clear();
                count += 1;
            }
        } else if target.is_object() {
            if let Some(obj) = target.as_object_mut()
                && !obj.is_empty()
            {
                obj.clear();
                count += 1;
            }
        } else if target.is_number() {
            *target = sonic_rs::json!(0);
            count += 1;
        }
    }

    Ok(count)
}

fn json_merge_impl(root: &mut Value, path: &str, merge_value: Value) -> Result<()> {
    if path == "$" || path == "." || path.is_empty() {
        merge_values(root, merge_value);
        return Ok(());
    }

    let targets = get_mutable_targets(root, path)?;
    for target in targets {
        merge_values(target, merge_value.clone());
    }

    Ok(())
}

fn merge_values(target: &mut Value, source: Value) {
    if source.is_null() {
        // null means delete
        *target = Value::new_null();
    } else if target.is_object() && source.is_object() {
        if let (Some(target_obj), Some(source_obj)) = (target.as_object_mut(), source.as_object()) {
            for (k, v) in source_obj.iter() {
                if v.is_null() {
                    target_obj.remove(&k);
                } else if let Some(existing) = target_obj.get_mut(&k) {
                    merge_values(existing, v.clone());
                } else {
                    target_obj.insert(&k, v.clone());
                }
            }
        }
    } else {
        *target = source;
    }
}

/// Get mutable references to values at path
fn get_mutable_targets<'a>(root: &'a mut Value, path: &str) -> Result<Vec<&'a mut Value>> {
    if path == "$" || path == "." || path.is_empty() {
        return Ok(vec![root]);
    }

    let path = path.trim_start_matches('$').trim_start_matches('.');
    if path.is_empty() {
        return Ok(vec![root]);
    }

    let parts: Vec<&str> = split_path(path);
    get_mutable_nested(root, &parts)
}

fn get_mutable_nested<'a>(current: &'a mut Value, parts: &[&str]) -> Result<Vec<&'a mut Value>> {
    if parts.is_empty() {
        return Ok(vec![current]);
    }

    let part = parts[0];
    let remaining = &parts[1..];

    if part.starts_with('[') && part.ends_with(']') {
        let inner = &part[1..part.len() - 1];
        if let Ok(idx) = inner.parse::<i64>() {
            if let Some(arr) = current.as_array_mut()
                && let Some(normalized) = normalize_array_index(idx, arr.len())
                && let Some(elem) = arr.get_mut(normalized)
            {
                return get_mutable_nested(elem, remaining);
            }
            return Ok(vec![]);
        }
    }

    // Object field
    if let Some(obj) = current.as_object_mut()
        && let Some(child) = obj.get_mut(&part.to_string())
    {
        return get_mutable_nested(child, remaining);
    }

    Ok(vec![])
}
