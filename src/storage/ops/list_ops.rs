use bytes::Bytes;
use dashmap::mapref::entry::Entry as DashEntry;

use crate::error::{Error, Result};
use crate::storage::Store;
use crate::storage::types::{DataType, Entry};

use std::collections::VecDeque;
use std::sync::atomic::Ordering;

/// List operations for the Store
impl Store {
    // ==================== List operations ====================

    /// Push elements to the left (head) of the list
    #[inline]
    pub fn lpush(&self, key: Bytes, values: Vec<Bytes>) -> Result<usize> {
        match self.data.entry(key) {
            DashEntry::Occupied(mut e) => {
                let entry = e.get_mut();
                if entry.is_expired() {
                    let mut list = VecDeque::new();
                    for val in values.iter().rev() {
                        list.push_front(val.clone());
                    }
                    let len = list.len();
                    entry.data = DataType::List(list);
                    entry.persist();
                    entry.bump_version();
                    return Ok(len);
                }

                let result = match &mut entry.data {
                    DataType::List(list) => {
                        for val in values.iter().rev() {
                            list.push_front(val.clone());
                        }
                        Ok(list.len())
                    }
                    _ => Err(Error::WrongType),
                };
                if result.is_ok() {
                    entry.bump_version();
                }
                result
            }
            DashEntry::Vacant(e) => {
                let mut list = VecDeque::new();
                for val in values.iter().rev() {
                    list.push_front(val.clone());
                }
                let len = list.len();
                e.insert(Entry::new(DataType::List(list)));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(len)
            }
        }
    }

    /// Push elements to the right (tail) of the list
    #[inline]
    pub fn rpush(&self, key: Bytes, values: Vec<Bytes>) -> Result<usize> {
        match self.data.entry(key) {
            DashEntry::Occupied(mut e) => {
                let entry = e.get_mut();
                if entry.is_expired() {
                    let mut list = VecDeque::new();
                    for val in &values {
                        list.push_back(val.clone());
                    }
                    let len = list.len();
                    entry.data = DataType::List(list);
                    entry.persist();
                    entry.bump_version();
                    return Ok(len);
                }

                let result = match &mut entry.data {
                    DataType::List(list) => {
                        for val in &values {
                            list.push_back(val.clone());
                        }
                        Ok(list.len())
                    }
                    _ => Err(Error::WrongType),
                };
                if result.is_ok() {
                    entry.bump_version();
                }
                result
            }
            DashEntry::Vacant(e) => {
                let mut list = VecDeque::new();
                for val in &values {
                    list.push_back(val.clone());
                }
                let len = list.len();
                e.insert(Entry::new(DataType::List(list)));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(len)
            }
        }
    }

    /// Pop element from the left (head) of the list
    #[inline]
    pub fn lpop(&self, key: &[u8], count: usize) -> Option<Vec<Bytes>> {
        let mut entry = self.data.get_mut(key)?;
        if entry.is_expired() {
            drop(entry);
            self.del(key);
            return None;
        }

        let (result, is_empty) = match &mut entry.data {
            DataType::List(list) => {
                let mut result = Vec::new();
                for _ in 0..count {
                    if let Some(val) = list.pop_front() {
                        result.push(val);
                    } else {
                        break;
                    }
                }
                (result, list.is_empty())
            }
            _ => return None,
        };
        if !result.is_empty() {
            entry.bump_version();
        }
        if is_empty {
            drop(entry);
            self.del(key);
        }
        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    /// Pop element from the right (tail) of the list
    #[inline]
    pub fn rpop(&self, key: &[u8], count: usize) -> Option<Vec<Bytes>> {
        let mut entry = self.data.get_mut(key)?;
        if entry.is_expired() {
            drop(entry);
            self.del(key);
            return None;
        }

        let (result, is_empty) = match &mut entry.data {
            DataType::List(list) => {
                let mut result = Vec::new();
                for _ in 0..count {
                    if let Some(val) = list.pop_back() {
                        result.push(val);
                    } else {
                        break;
                    }
                }
                (result, list.is_empty())
            }
            _ => return None,
        };
        if !result.is_empty() {
            entry.bump_version();
        }
        if is_empty {
            drop(entry);
            self.del(key);
        }
        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    /// Get list length
    #[inline]
    pub fn llen(&self, key: &[u8]) -> usize {
        match self.data.get(key) {
            Some(e) => {
                if e.is_expired() {
                    return 0;
                }
                match e.data.as_list() {
                    Some(list) => list.len(),
                    None => 0,
                }
            }
            None => 0,
        }
    }

    /// Get list range
    #[inline]
    pub fn lrange(&self, key: &[u8], start: i64, stop: i64) -> Vec<Bytes> {
        match self.data.get(key) {
            Some(e) => {
                if e.is_expired() {
                    return vec![];
                }
                match e.data.as_list() {
                    Some(list) => {
                        let len = list.len() as i64;
                        let start = normalize_index(start, len);
                        let stop = normalize_index(stop, len);

                        if start > stop || start >= len {
                            return vec![];
                        }

                        let start = start.max(0) as usize;
                        let stop = (stop + 1).min(len) as usize;

                        list.iter()
                            .skip(start)
                            .take(stop - start)
                            .cloned()
                            .collect()
                    }
                    None => vec![],
                }
            }
            None => vec![],
        }
    }

    /// Get element at index
    #[inline]
    pub fn lindex(&self, key: &[u8], index: i64) -> Option<Bytes> {
        match self.data.get(key) {
            Some(e) => {
                if e.is_expired() {
                    return None;
                }
                match e.data.as_list() {
                    Some(list) => {
                        let len = list.len() as i64;
                        let index = normalize_index(index, len);
                        if index < 0 || index >= len {
                            None
                        } else {
                            list.get(index as usize).cloned()
                        }
                    }
                    None => None,
                }
            }
            None => None,
        }
    }

    /// Set element at index
    #[inline]
    pub fn lset(&self, key: &[u8], index: i64, value: Bytes) -> Result<()> {
        match self.data.get_mut(key) {
            Some(mut e) => {
                let entry = e.value_mut();
                if entry.is_expired() {
                    return Err(Error::NoSuchKey);
                }
                match entry.data.as_list_mut() {
                    Some(list) => {
                        let len = list.len() as i64;
                        let index = normalize_index(index, len);
                        if index < 0 || index >= len {
                            Err(Error::IndexOutOfRange)
                        } else {
                            list[index as usize] = value;
                            entry.bump_version();
                            Ok(())
                        }
                    }
                    None => Err(Error::WrongType),
                }
            }
            None => Err(Error::NoSuchKey),
        }
    }

    /// Remove elements equal to element. Count specifies direction and limit:
    /// count > 0: Remove from head to tail, up to count elements
    /// count < 0: Remove from tail to head, up to abs(count) elements
    /// count = 0: Remove all matching elements
    /// Returns number of removed elements
    #[inline]
    pub fn lrem(&self, key: &[u8], count: i64, element: &[u8]) -> usize {
        let mut entry = match self.data.get_mut(key) {
            Some(e) => e,
            None => return 0,
        };

        if entry.is_expired() {
            drop(entry);
            self.del(key);
            return 0;
        }

        let (removed, is_empty) = {
            let list = match entry.data.as_list_mut() {
                Some(l) => l,
                None => return 0,
            };

            let limit = if count == 0 {
                usize::MAX
            } else {
                count.unsigned_abs() as usize
            };
            let mut removed = 0;

            if count >= 0 {
                // Remove from head to tail
                let mut i = 0;
                while i < list.len() && removed < limit {
                    if list[i].as_ref() == element {
                        list.remove(i);
                        removed += 1;
                    } else {
                        i += 1;
                    }
                }
            } else {
                // Remove from tail to head (iterate backwards)
                let mut i = list.len();
                while i > 0 && removed < limit {
                    i -= 1;
                    if list[i].as_ref() == element {
                        list.remove(i);
                        removed += 1;
                    }
                }
            }
            (removed, list.is_empty())
        };

        if removed > 0 {
            entry.bump_version();
        }

        // Clean up empty list
        if is_empty {
            drop(entry);
            self.del(key);
        }

        removed
    }

    /// Trim list to specified range (inclusive)
    /// Returns Ok if list exists and is a list, Err otherwise
    #[inline]
    pub fn ltrim(&self, key: &[u8], start: i64, stop: i64) -> Result<()> {
        let mut entry = match self.data.get_mut(key) {
            Some(e) => e,
            None => return Ok(()), // Non-existent key is valid (no-op)
        };

        if entry.is_expired() {
            drop(entry);
            self.del(key);
            return Ok(());
        }

        let is_empty = {
            let list = match entry.data.as_list_mut() {
                Some(l) => l,
                None => return Err(Error::WrongType),
            };

            let len = list.len() as i64;
            let start = normalize_index(start, len);
            let stop = normalize_index(stop, len);

            // If range is invalid or empty, clear the list
            if start > stop || start >= len || stop < 0 {
                list.clear();
                true
            } else {
                let start = start.max(0) as usize;
                let stop = (stop + 1).min(len) as usize;

                // Remove elements from the end first (more efficient for VecDeque)
                let keep_end = stop;
                let remove_end = list.len() - keep_end;
                for _ in 0..remove_end {
                    list.pop_back();
                }

                // Remove elements from the front
                for _ in 0..start {
                    list.pop_front();
                }
                list.is_empty()
            }
        };

        entry.bump_version();

        // Clean up empty list
        if is_empty {
            drop(entry);
            self.del(key);
        }

        Ok(())
    }

    /// Insert element before or after pivot
    /// Returns list length after insert, or -1 if pivot not found, or 0 if key doesn't exist
    #[inline]
    pub fn linsert(&self, key: &[u8], before: bool, pivot: &[u8], element: Bytes) -> Result<i64> {
        let mut entry = match self.data.get_mut(key) {
            Some(e) => e,
            None => return Ok(0),
        };

        if entry.is_expired() {
            drop(entry);
            self.del(key);
            return Ok(0);
        }

        let result = {
            let list = match entry.data.as_list_mut() {
                Some(l) => l,
                None => return Err(Error::WrongType),
            };

            // Find pivot position
            let pivot_pos = list.iter().position(|x| x.as_ref() == pivot);

            match pivot_pos {
                Some(pos) => {
                    let insert_pos = if before { pos } else { pos + 1 };
                    list.insert(insert_pos, element);
                    Some(list.len() as i64)
                }
                None => None, // Pivot not found
            }
        };

        match result {
            Some(len) => {
                entry.bump_version();
                Ok(len)
            }
            None => Ok(-1),
        }
    }

    /// Find position of element in list
    /// rank: which occurrence to return (1 = first, -1 = last, 2 = second, etc.)
    /// count: how many positions to return (0 = all)
    /// maxlen: limit search to first/last maxlen elements (0 = no limit)
    /// Returns positions as Vec, empty if not found
    #[inline]
    pub fn lpos(
        &self,
        key: &[u8],
        element: &[u8],
        rank: i64,
        count: usize,
        maxlen: usize,
    ) -> Option<Vec<i64>> {
        let entry = self.data.get(key)?;

        if entry.is_expired() {
            return None;
        }

        let list = entry.data.as_list()?;

        let len = list.len();
        if len == 0 {
            return Some(vec![]);
        }

        let limit = if maxlen == 0 { len } else { maxlen.min(len) };
        let want_count = if count == 0 { usize::MAX } else { count };
        let reverse = rank < 0;
        let target_rank = if rank == 0 {
            1
        } else {
            rank.unsigned_abs() as usize
        };

        let mut positions = Vec::new();
        let mut found_count = 0usize;

        if reverse {
            // Search from tail to head
            let start = len.saturating_sub(limit);
            for i in (start..len).rev() {
                if list[i].as_ref() == element {
                    found_count += 1;
                    if found_count >= target_rank {
                        positions.push(i as i64);
                        if positions.len() >= want_count {
                            break;
                        }
                    }
                }
            }
        } else {
            // Search from head to tail
            for (i, item) in list.iter().enumerate().take(limit) {
                if item.as_ref() == element {
                    found_count += 1;
                    if found_count >= target_rank {
                        positions.push(i as i64);
                        if positions.len() >= want_count {
                            break;
                        }
                    }
                }
            }
        }

        Some(positions)
    }
}

/// Normalize negative indices for list operations
fn normalize_index(idx: i64, len: i64) -> i64 {
    if idx < 0 { len + idx } else { idx }
}
