//! JSON operations for the Store using sonic-rs for hyper-performance

use bytes::Bytes;
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
        let json_value: Value = from_slice(value).map_err(|_| Error::JsonParse)?;
        let is_root = path == "$" || path == "." || path.is_empty();

        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Vacant(e) => {
                if xx {
                    return Ok(false);
                }
                if !is_root {
                    return Err(Error::JsonNewAtRoot);
                }
                e.insert((key, Entry::new(DataType::Json(Box::new(json_value)))));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(true)
            }
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
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
        let entry_ref = self.data_get(key)?;
        if entry_ref.1.is_expired() {
            return None;
        }

        let root = entry_ref.1.data.as_json()?;

        if paths.is_empty()
            || (paths.len() == 1 && (paths[0] == "$" || paths[0] == "." || paths[0].is_empty()))
        {
            let bytes = to_vec(root).ok()?;
            return Some(Bytes::from(bytes));
        }

        if paths.len() == 1 {
            let result = json_path_get(root, paths[0])?;
            if result.len() == 1 {
                let bytes = to_vec(&result[0]).ok()?;
                return Some(Bytes::from(bytes));
            }
            let arr: Vec<&Value> = result.iter().collect();
            let bytes = to_vec(&arr).ok()?;
            return Some(Bytes::from(bytes));
        }

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
            return Ok(if self.del(key) { 1 } else { 0 });
        }

        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
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
            crate::storage::dashtable::Entry::Vacant(_) => Ok(0),
        }
    }

    /// JSON.TYPE key [path]
    #[inline]
    pub fn json_type(&self, key: &[u8], path: Option<&str>) -> Option<Vec<&'static str>> {
        let entry_ref = self.data_get(key)?;
        if entry_ref.1.is_expired() {
            return None;
        }

        let root = entry_ref.1.data.as_json()?;

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
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return Err(Error::JsonKeyNotFound);
                }

                let root = match &mut entry.data {
                    DataType::Json(j) => j,
                    _ => return Err(Error::WrongType),
                };

                let result = json_array_append(root, path, values)?;
                if result.iter().any(|r| r.is_some()) {
                    entry.bump_version();
                }
                Ok(result)
            }
            crate::storage::dashtable::Entry::Vacant(_) => Err(Error::JsonKeyNotFound),
        }
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
        let entry_ref = match self.data_get(key) {
            Some(e) => e,
            None => return Err(Error::JsonKeyNotFound),
        };

        if entry_ref.1.is_expired() {
            return Err(Error::JsonKeyNotFound);
        }

        let root = match &entry_ref.1.data {
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
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return Err(Error::JsonKeyNotFound);
                }

                let root = match &mut entry.data {
                    DataType::Json(j) => j,
                    _ => return Err(Error::WrongType),
                };

                let result = json_array_insert(root, path, index, values);
                if result.is_ok() {
                    entry.bump_version();
                }
                result
            }
            crate::storage::dashtable::Entry::Vacant(_) => Err(Error::JsonKeyNotFound),
        }
    }

    /// JSON.ARRLEN key [path]
    #[inline]
    pub fn json_arrlen(&self, key: &[u8], path: Option<&str>) -> Result<Vec<Option<i64>>> {
        let entry_ref = match self.data_get(key) {
            Some(e) => e,
            None => return Ok(vec![None]),
        };

        if entry_ref.1.is_expired() {
            return Ok(vec![None]);
        }

        let root = match &entry_ref.1.data {
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
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return Ok(vec![None]);
                }

                let root = match &mut entry.data {
                    DataType::Json(j) => j,
                    _ => return Err(Error::WrongType),
                };

                let path = path.unwrap_or("$");
                let result = json_array_pop(root, path, index);
                if result.is_ok() {
                    entry.bump_version();
                }
                result
            }
            crate::storage::dashtable::Entry::Vacant(_) => Ok(vec![None]),
        }
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
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return Err(Error::JsonKeyNotFound);
                }

                let root = match &mut entry.data {
                    DataType::Json(j) => j,
                    _ => return Err(Error::WrongType),
                };

                let result = json_array_trim(root, path, start, stop);
                if result.is_ok() {
                    entry.bump_version();
                }
                result
            }
            crate::storage::dashtable::Entry::Vacant(_) => Err(Error::JsonKeyNotFound),
        }
    }

    // ==================== Object operations ====================

    /// JSON.OBJKEYS key [path]
    #[inline]
    pub fn json_objkeys(&self, key: &[u8], path: Option<&str>) -> Result<Vec<Option<Vec<String>>>> {
        let entry_ref = match self.data_get(key) {
            Some(e) => e,
            None => return Ok(vec![None]),
        };

        if entry_ref.1.is_expired() {
            return Ok(vec![None]);
        }

        let root = match &entry_ref.1.data {
            DataType::Json(j) => j,
            _ => return Err(Error::WrongType),
        };

        let path = path.unwrap_or("$");
        json_object_keys(root, path)
    }

    /// JSON.OBJLEN key [path]
    #[inline]
    pub fn json_objlen(&self, key: &[u8], path: Option<&str>) -> Result<Vec<Option<i64>>> {
        let entry_ref = match self.data_get(key) {
            Some(e) => e,
            None => return Ok(vec![None]),
        };

        if entry_ref.1.is_expired() {
            return Ok(vec![None]);
        }

        let root = match &entry_ref.1.data {
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
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return Err(Error::JsonKeyNotFound);
                }

                let root = match &mut entry.data {
                    DataType::Json(j) => j,
                    _ => return Err(Error::WrongType),
                };

                let result = json_num_op(root, path, value, |a, b| a + b);
                if result.is_ok() {
                    entry.bump_version();
                }
                result
            }
            crate::storage::dashtable::Entry::Vacant(_) => Err(Error::JsonKeyNotFound),
        }
    }

    /// JSON.NUMMULTBY key path value
    #[inline]
    pub fn json_nummultby(&self, key: &[u8], path: &str, value: f64) -> Result<Vec<Option<f64>>> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return Err(Error::JsonKeyNotFound);
                }

                let root = match &mut entry.data {
                    DataType::Json(j) => j,
                    _ => return Err(Error::WrongType),
                };

                let result = json_num_op(root, path, value, |a, b| a * b);
                if result.is_ok() {
                    entry.bump_version();
                }
                result
            }
            crate::storage::dashtable::Entry::Vacant(_) => Err(Error::JsonKeyNotFound),
        }
    }

    // ==================== String operations ====================

    /// JSON.STRAPPEND key [path] value
    #[inline]
    pub fn json_strappend(&self, key: &[u8], path: &str, value: &str) -> Result<Vec<Option<i64>>> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return Err(Error::JsonKeyNotFound);
                }

                let root = match &mut entry.data {
                    DataType::Json(j) => j,
                    _ => return Err(Error::WrongType),
                };

                let result = json_str_append(root, path, value);
                if result.is_ok() {
                    entry.bump_version();
                }
                result
            }
            crate::storage::dashtable::Entry::Vacant(_) => Err(Error::JsonKeyNotFound),
        }
    }

    /// JSON.STRLEN key [path]
    #[inline]
    pub fn json_strlen(&self, key: &[u8], path: Option<&str>) -> Result<Vec<Option<i64>>> {
        let entry_ref = match self.data_get(key) {
            Some(e) => e,
            None => return Ok(vec![None]),
        };

        if entry_ref.1.is_expired() {
            return Ok(vec![None]);
        }

        let root = match &entry_ref.1.data {
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
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return Err(Error::JsonKeyNotFound);
                }

                let root = match &mut entry.data {
                    DataType::Json(j) => j,
                    _ => return Err(Error::WrongType),
                };

                let result = json_toggle_impl(root, path);
                if result.is_ok() {
                    entry.bump_version();
                }
                result
            }
            crate::storage::dashtable::Entry::Vacant(_) => Err(Error::JsonKeyNotFound),
        }
    }

    /// JSON.CLEAR key [path]
    #[inline]
    pub fn json_clear(&self, key: &[u8], path: Option<&str>) -> Result<usize> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return Ok(0);
                }

                let root = match &mut entry.data {
                    DataType::Json(j) => j,
                    _ => return Err(Error::WrongType),
                };

                let path = path.unwrap_or("$");
                let result = json_clear_impl(root, path);
                if result.is_ok() {
                    entry.bump_version();
                }
                result
            }
            crate::storage::dashtable::Entry::Vacant(_) => Ok(0),
        }
    }

    /// JSON.MERGE key path value
    #[inline]
    pub fn json_merge(&self, key: Bytes, path: &str, value: &[u8]) -> Result<()> {
        let merge_value: Value = from_slice(value).map_err(|_| Error::JsonParse)?;

        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Vacant(e) => {
                if path == "$" || path == "." || path.is_empty() {
                    e.insert((key, Entry::new(DataType::Json(Box::new(merge_value)))));
                    self.key_count.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                } else {
                    Err(Error::JsonNewAtRoot)
                }
            }
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    if path == "$" || path == "." || path.is_empty() {
                        entry.data = DataType::Json(Box::new(merge_value));
                        entry.persist();
                        entry.bump_version();
                        return Ok(());
                    } else {
                        return Err(Error::JsonNewAtRoot);
                    }
                }

                let root = match &mut entry.data {
                    DataType::Json(j) => j,
                    _ => return Err(Error::WrongType),
                };

                let result = json_merge_impl(root, path, merge_value);
                if result.is_ok() {
                    entry.bump_version();
                }
                result
            }
        }
    }

    /// JSON.MGET key [key ...] path
    #[inline]
    pub fn json_mget(&self, keys: &[Bytes], path: &str) -> Vec<Option<Bytes>> {
        keys.iter()
            .map(|key| {
                let entry_ref = self.data_get(key.as_ref())?;
                if entry_ref.1.is_expired() {
                    return None;
                }
                let root = entry_ref.1.data.as_json()?;
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
        let entry_ref = match self.data_get(key) {
            Some(e) => e,
            None => return Ok(vec![None]),
        };

        if entry_ref.1.is_expired() {
            return Ok(vec![None]);
        }

        let root = match &entry_ref.1.data {
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
        let entry_ref = match self.data_get(key) {
            Some(e) => e,
            None => return Ok(vec![None]),
        };

        if entry_ref.1.is_expired() {
            return Ok(vec![None]);
        }

        let root = match &entry_ref.1.data {
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
                let inner = &part[1..part.len() - 1];
                if let Ok(idx) = inner.parse::<i64>() {
                    if let Some(arr) = value.as_array()
                        && let Some(normalized) = normalize_array_index(idx, arr.len())
                        && let Some(v) = arr.get(normalized)
                    {
                        new_results.push(v.clone());
                    }
                } else {
                    let key = inner.trim_matches(|c| c == '\'' || c == '"');
                    if let Some(v) = value.get(key) {
                        new_results.push(v.clone());
                    }
                }
            } else {
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

    if part.starts_with('[') && part.ends_with(']') {
        let inner = &part[1..part.len() - 1];
        if let Ok(idx) = inner.parse::<i64>() {
            if !current.is_array() {
                if xx {
                    return Ok(false);
                }
                *current = Value::from(Vec::<Value>::new());
            }
            let arr = current.as_array_mut().unwrap();
            let len = arr.len();
            if let Some(normalized) = normalize_array_index(idx, len) {
                return set_nested(&mut arr[normalized], remaining, value, nx, xx);
            } else if !xx {
                // For NX/normal, append if index is out of bounds at positive end
                if idx >= 0 {
                    let mut new_val = Value::from(());
                    let res = set_nested(&mut new_val, remaining, value, nx, xx);
                    if let Ok(true) = res {
                        arr.push(new_val);
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        } else {
            let key = inner.trim_matches(|c| c == '\'' || c == '"');
            if !current.is_object() {
                if xx {
                    return Ok(false);
                }
                *current = Value::from(sonic_rs::Object::new());
            }
            let obj = current.as_object_mut().unwrap();
            if !obj.contains_key(&key) && xx {
                return Ok(false);
            }
            if !obj.contains_key(&key) {
                let mut new_val = Value::from(());
                let res = set_nested(&mut new_val, remaining, value, nx, xx);
                if let Ok(true) = res {
                    obj.insert(key, new_val);
                    return Ok(true);
                }
                return res;
            }
            set_nested(obj.get_mut(&key).unwrap(), remaining, value, nx, xx)
        }
    } else {
        if !current.is_object() {
            if xx {
                return Ok(false);
            }
            *current = Value::from(sonic_rs::Object::new());
        }
        let obj = current.as_object_mut().unwrap();
        if !obj.contains_key(&part) && xx {
            return Ok(false);
        }
        if !obj.contains_key(&part) {
            let mut new_val = Value::from(());
            let res = set_nested(&mut new_val, remaining, value, nx, xx);
            if let Ok(true) = res {
                obj.insert(part, new_val);
                return Ok(true);
            }
            return res;
        }
        set_nested(obj.get_mut(&part).unwrap(), remaining, value, nx, xx)
    }
}

/// Implementation of JSON.DEL/FORGET for paths
fn json_path_delete(root: &mut Value, path: &str) -> Result<usize> {
    let path = path.trim_start_matches('$').trim_start_matches('.');
    if path.is_empty() {
        *root = Value::from(());
        return Ok(1);
    }

    let parts = split_path(path);
    delete_nested(root, &parts)
}

fn delete_nested(current: &mut Value, parts: &[&str]) -> Result<usize> {
    if parts.is_empty() {
        return Ok(0);
    }

    let part = parts[0];
    let remaining = &parts[1..];

    if remaining.is_empty() {
        if part == "*" {
            if let Some(arr) = current.as_array_mut() {
                let count = arr.len();
                arr.clear();
                return Ok(count);
            } else if let Some(obj) = current.as_object_mut() {
                let count = obj.len();
                obj.clear();
                return Ok(count);
            }
            return Ok(0);
        }

        if part.starts_with('[') && part.ends_with(']') {
            let inner = &part[1..part.len() - 1];
            if let Ok(idx) = inner.parse::<i64>() {
                if let Some(arr) = current.as_array_mut() {
                    if let Some(normalized) = normalize_array_index(idx, arr.len()) {
                        arr.remove(normalized);
                        return Ok(1);
                    }
                }
            } else {
                let key = inner.trim_matches(|c| c == '\'' || c == '"');
                if let Some(obj) = current.as_object_mut() {
                    if obj.remove(&key).is_some() {
                        return Ok(1);
                    }
                }
            }
        } else {
            if let Some(obj) = current.as_object_mut() {
                if obj.remove(&part).is_some() {
                    return Ok(1);
                }
            }
        }
        return Ok(0);
    }

    // Traverse deeper
    let mut deleted = 0;
    if part == "*" {
        if let Some(arr) = current.as_array_mut() {
            for val in arr {
                deleted += delete_nested(val, remaining)?;
            }
        } else if let Some(obj) = current.as_object_mut() {
            for (_, val) in obj.iter_mut() {
                deleted += delete_nested(val, remaining)?;
            }
        }
    } else if part.starts_with('[') && part.ends_with(']') {
        let inner = &part[1..part.len() - 1];
        if let Ok(idx) = inner.parse::<i64>() {
            if let Some(arr) = current.as_array_mut() {
                if let Some(normalized) = normalize_array_index(idx, arr.len()) {
                    deleted += delete_nested(&mut arr[normalized], remaining)?;
                }
            }
        } else {
            let key = inner.trim_matches(|c| c == '\'' || c == '"');
            if let Some(obj) = current.as_object_mut() {
                if let Some(val) = obj.get_mut(&key) {
                    deleted += delete_nested(val, remaining)?;
                }
            }
        }
    } else {
        if let Some(obj) = current.as_object_mut() {
            if let Some(val) = obj.get_mut(&part) {
                deleted += delete_nested(val, remaining)?;
            }
        }
    }
    Ok(deleted)
}

fn json_array_append(root: &mut Value, path: &str, values: Vec<Value>) -> Result<Vec<Option<i64>>> {
    let mut results = Vec::new();
    let matches = json_path_get_mut(root, path)?;
    if matches.is_empty() {
        return Ok(vec![None]);
    }
    for val in matches {
        if let Some(arr) = val.as_array_mut() {
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
    let matches = json_path_get(root, path).unwrap_or_default();
    if matches.is_empty() {
        return Ok(vec![None]);
    }
    let mut results = Vec::new();
    for v in matches {
        if let Some(arr) = v.as_array() {
            let len = arr.len();
            let start_idx = if start >= 0 {
                start as usize
            } else {
                (len as i64 + start).max(0) as usize
            };
            let stop_idx = if stop == 0 {
                len
            } else if stop < 0 {
                (len as i64 + stop).max(0) as usize
            } else {
                stop as usize
            };

            let mut found = -1;
            for i in start_idx..stop_idx.min(len) {
                if &arr[i] == value {
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
    let mut results = Vec::new();
    let matches = json_path_get_mut(root, path)?;
    if matches.is_empty() {
        return Ok(vec![None]);
    }
    for val in matches {
        if let Some(arr) = val.as_array_mut() {
            let len = arr.len();
            let idx = if index >= 0 {
                index as usize
            } else {
                (len as i64 + index).max(0) as usize
            };
            if idx > len {
                results.push(None);
                continue;
            }
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
    let matches = json_path_get(root, path).unwrap_or_default();
    if matches.is_empty() {
        return Ok(vec![None]);
    }
    Ok(matches
        .iter()
        .map(|v| v.as_array().map(|a| a.len() as i64))
        .collect())
}

fn json_array_pop(root: &mut Value, path: &str, index: i64) -> Result<Vec<Option<Value>>> {
    let mut results: Vec<Option<Value>> = Vec::new();
    let matches = json_path_get_mut(root, path)?;
    if matches.is_empty() {
        return Ok(vec![None]);
    }
    for val in matches {
        if let Some(arr) = val.as_array_mut() {
            let len = arr.len();
            if len == 0 {
                results.push(None);
                continue;
            }
            let idx = if index == -1 {
                len - 1
            } else if index >= 0 {
                index as usize
            } else {
                (len as i64 + index).max(0) as usize
            };
            if idx >= len {
                results.push(None);
            } else {
                // sonic-rs remove returns (), so we get and clone first
                let val = arr.get(idx).map(|v| v.clone());
                arr.remove(idx);
                results.push(val);
            }
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
    let mut results = Vec::new();
    let matches = json_path_get_mut(root, path)?;
    if matches.is_empty() {
        return Ok(vec![None]);
    }
    for val in matches {
        if let Some(arr) = val.as_array_mut() {
            let len = arr.len();
            let start_idx = if start >= 0 {
                start as usize
            } else {
                (len as i64 + start).max(0) as usize
            };
            let stop_idx = if stop < 0 {
                (len as i64 + stop).max(0) as usize
            } else {
                stop as usize
            };

            if start_idx >= len || start_idx > stop_idx {
                arr.clear();
            } else {
                let stop_idx = (stop_idx + 1).min(len);
                arr.truncate(stop_idx);
                for _ in 0..start_idx {
                    arr.remove(0);
                }
            }
            results.push(Some(arr.len() as i64));
        } else {
            results.push(None);
        }
    }
    Ok(results)
}

fn json_object_keys(root: &Value, path: &str) -> Result<Vec<Option<Vec<String>>>> {
    let matches = json_path_get(root, path).unwrap_or_default();
    if matches.is_empty() {
        return Ok(vec![None]);
    }
    Ok(matches
        .iter()
        .map(|v| {
            v.as_object()
                .map(|o| o.iter().map(|(k, _)| k.to_string()).collect())
        })
        .collect())
}

fn json_object_len(root: &Value, path: &str) -> Result<Vec<Option<i64>>> {
    let matches = json_path_get(root, path).unwrap_or_default();
    if matches.is_empty() {
        return Ok(vec![None]);
    }
    Ok(matches
        .iter()
        .map(|v| v.as_object().map(|o| o.len() as i64))
        .collect())
}

fn json_num_op<F>(root: &mut Value, path: &str, value: f64, op: F) -> Result<Vec<Option<f64>>>
where
    F: Fn(f64, f64) -> f64,
{
    let mut results = Vec::new();
    let matches = json_path_get_mut(root, path)?;
    if matches.is_empty() {
        return Ok(vec![None]);
    }
    for val in matches {
        if let Some(n) = val.as_f64() {
            let new_val = op(n, value);
            *val = sonic_rs::to_value(&new_val).unwrap_or(Value::from(()));
            results.push(Some(new_val));
        } else if let Some(n) = val.as_i64() {
            let new_val = op(n as f64, value);
            *val = sonic_rs::to_value(&new_val).unwrap_or(Value::from(()));
            results.push(Some(new_val));
        } else if let Some(n) = val.as_u64() {
            let new_val = op(n as f64, value);
            *val = sonic_rs::to_value(&new_val).unwrap_or(Value::from(()));
            results.push(Some(new_val));
        } else {
            results.push(None);
        }
    }
    Ok(results)
}

fn json_str_append(root: &mut Value, path: &str, value: &str) -> Result<Vec<Option<i64>>> {
    let mut results = Vec::new();
    let matches = json_path_get_mut(root, path)?;
    if matches.is_empty() {
        return Ok(vec![None]);
    }
    for val in matches {
        if let Some(s) = val.as_str() {
            let mut new_s = s.to_string();
            new_s.push_str(value);
            let len = new_s.len();
            *val = Value::from(new_s.as_str());
            results.push(Some(len as i64));
        } else {
            results.push(None);
        }
    }
    Ok(results)
}

fn json_str_len(root: &Value, path: &str) -> Result<Vec<Option<i64>>> {
    let matches = json_path_get(root, path).unwrap_or_default();
    if matches.is_empty() {
        return Ok(vec![None]);
    }
    Ok(matches
        .iter()
        .map(|v| v.as_str().map(|s| s.len() as i64))
        .collect())
}

fn json_toggle_impl(root: &mut Value, path: &str) -> Result<Vec<Option<bool>>> {
    let mut results = Vec::new();
    let matches = json_path_get_mut(root, path)?;
    if matches.is_empty() {
        return Ok(vec![None]);
    }
    for val in matches {
        if let Some(b) = val.as_bool() {
            let new_b = !b;
            *val = Value::from(new_b);
            results.push(Some(new_b));
        } else {
            results.push(None);
        }
    }
    Ok(results)
}

fn json_clear_impl(root: &mut Value, path: &str) -> Result<usize> {
    let matches = json_path_get_mut(root, path)?;
    let mut cleared = 0;
    for val in matches {
        if let Some(arr) = val.as_array_mut() {
            if !arr.is_empty() {
                arr.clear();
                cleared += 1;
            }
        } else if let Some(obj) = val.as_object_mut() {
            if !obj.is_empty() {
                obj.clear();
                cleared += 1;
            }
        } else if val.is_number() {
            *val = Value::from(0);
            cleared += 1;
        }
    }
    Ok(cleared)
}

fn json_merge_impl(root: &mut Value, path: &str, value: Value) -> Result<()> {
    let matches = json_path_get_mut(root, path)?;
    for val in matches {
        if let Some(obj) = val.as_object_mut() {
            if let Some(merge_obj) = value.as_object() {
                for (k, v) in merge_obj.iter() {
                    if v.is_null() {
                        obj.remove(&k);
                    } else {
                        obj.insert(k, v.clone());
                    }
                }
            } else {
                *val = value.clone();
            }
        } else {
            *val = value.clone();
        }
    }
    Ok(())
}

fn split_path(path: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut in_bracket = false;

    for (i, c) in path.chars().enumerate() {
        match c {
            '.' if !in_bracket => {
                if i > start {
                    parts.push(&path[start..i]);
                }
                start = i + 1;
            }
            '[' => {
                if i > start {
                    parts.push(&path[start..i]);
                }
                start = i;
                in_bracket = true;
            }
            ']' => {
                in_bracket = false;
                parts.push(&path[start..i + 1]);
                start = i + 1;
            }
            _ => {}
        }
    }

    if start < path.len() {
        parts.push(&path[start..]);
    }
    parts
}

fn json_path_get_mut<'a>(root: &'a mut Value, path: &str) -> Result<Vec<&'a mut Value>> {
    let path = path.trim_start_matches('$').trim_start_matches('.');
    if path.is_empty() {
        return Ok(vec![root]);
    }

    let mut results = vec![root];
    for part in split_path(path) {
        let mut next_results = Vec::new();
        for val in results {
            if part == "*" {
                if val.is_array() {
                    if let Some(arr) = val.as_array_mut() {
                        for item in arr {
                            next_results.push(item);
                        }
                    }
                } else if val.is_object() {
                    if let Some(obj) = val.as_object_mut() {
                        for (_, v) in obj.iter_mut() {
                            next_results.push(v);
                        }
                    }
                }
            } else if part.starts_with('[') && part.ends_with(']') {
                let inner = &part[1..part.len() - 1];
                if let Ok(idx) = inner.parse::<i64>() {
                    if let Some(arr) = val.as_array_mut() {
                        if let Some(normalized) = normalize_array_index(idx, arr.len()) {
                            next_results.push(&mut arr[normalized]);
                        }
                    }
                } else {
                    let key = inner.trim_matches(|c| c == '\'' || c == '"');
                    if let Some(obj) = val.as_object_mut() {
                        if let Some(v) = obj.get_mut(&key) {
                            next_results.push(v);
                        }
                    }
                }
            } else {
                if let Some(obj) = val.as_object_mut() {
                    if let Some(v) = obj.get_mut(&part) {
                        next_results.push(v);
                    }
                }
            }
        }
        results = next_results;
        if results.is_empty() {
            break;
        }
    }
    Ok(results)
}
