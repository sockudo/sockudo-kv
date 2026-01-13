use bytes::Bytes;

use crate::error::{Error, Result};
use crate::storage::Store;
use crate::storage::types::{DataType, Entry};

use crate::storage::quicklist::QuickList;
use std::sync::atomic::Ordering;

/// List operations for the Store
impl Store {
    // ==================== List operations ====================

    /// Push elements to the left (head) of the list
    #[inline]
    pub fn lpush(&self, key: Bytes, values: Vec<Bytes>) -> Result<usize> {
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    let mut list = QuickList::new();
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
            crate::storage::dashtable::Entry::Vacant(e) => {
                let mut list = QuickList::new();
                for val in values.iter().rev() {
                    list.push_front(val.clone());
                }
                let len = list.len();
                e.insert((key, Entry::new(DataType::List(list))));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(len)
            }
        }
    }

    /// Push elements to the right (tail) of the list
    #[inline]
    pub fn rpush(&self, key: Bytes, values: Vec<Bytes>) -> Result<usize> {
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    let mut list = QuickList::new();
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
            crate::storage::dashtable::Entry::Vacant(e) => {
                let mut list = QuickList::new();
                for val in &values {
                    list.push_back(val.clone());
                }
                let len = list.len();
                e.insert((key, Entry::new(DataType::List(list))));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(len)
            }
        }
    }

    /// Pop element from the left (head) of the list
    #[inline]
    pub fn lpop(&self, key: &[u8], count: usize) -> Option<Vec<Bytes>> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
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
                    e.remove();
                }

                if result.is_empty() {
                    None
                } else {
                    Some(result)
                }
            }
            crate::storage::dashtable::Entry::Vacant(_) => None,
        }
    }

    /// Pop element from the right (tail) of the list
    #[inline]
    pub fn rpop(&self, key: &[u8], count: usize) -> Option<Vec<Bytes>> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
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
                    e.remove();
                }

                if result.is_empty() {
                    None
                } else {
                    Some(result)
                }
            }
            crate::storage::dashtable::Entry::Vacant(_) => None,
        }
    }

    /// Get list length
    #[inline]
    pub fn llen(&self, key: &[u8]) -> usize {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return 0;
                }
                match entry_ref.1.data.as_list() {
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
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return vec![];
                }
                match entry_ref.1.data.as_list() {
                    Some(list) => {
                        let len = list.len() as i64;
                        let start = normalize_index(start, len);
                        let stop = normalize_index(stop, len);

                        if start > stop || start >= len {
                            return vec![];
                        }

                        let start = start.max(0) as usize;
                        let stop = (stop + 1).min(len) as usize;

                        list.iter().skip(start).take(stop - start).collect()
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
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return None;
                }
                match entry_ref.1.data.as_list() {
                    Some(list) => {
                        let len = list.len() as i64;
                        let index = normalize_index(index, len);
                        if index < 0 || index >= len {
                            None
                        } else {
                            list.get(index as usize)
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
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
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
                            list.set(index as usize, value);
                            entry.bump_version();
                            Ok(())
                        }
                    }
                    None => Err(Error::WrongType),
                }
            }
            crate::storage::dashtable::Entry::Vacant(_) => Err(Error::NoSuchKey),
        }
    }

    /// Remove elements equal to element. Count specifies direction and limit:
    /// count > 0: Remove from head to tail, up to count elements
    /// count < 0: Remove from tail to head, up to abs(count) elements
    /// count = 0: Remove all matching elements
    /// Returns number of removed elements
    #[inline]
    pub fn lrem(&self, key: &[u8], count: i64, element: &[u8]) -> usize {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
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
                        let mut i = 0;
                        while i < list.len() && removed < limit {
                            if let Some(val) = list.get(i) {
                                if val.as_ref() == element {
                                    list.remove(i);
                                    removed += 1;
                                    continue;
                                }
                            }
                            i += 1;
                        }
                    } else {
                        let mut i = list.len();
                        while i > 0 && removed < limit {
                            i -= 1;
                            if let Some(val) = list.get(i) {
                                if val.as_ref() == element {
                                    list.remove(i);
                                    removed += 1;
                                }
                            }
                        }
                    }
                    (removed, list.is_empty())
                };

                if removed > 0 {
                    entry.bump_version();
                }
                if is_empty {
                    e.remove();
                }
                removed
            }
            crate::storage::dashtable::Entry::Vacant(_) => 0,
        }
    }

    /// Trim list to specified range (inclusive)
    /// Returns Ok if list exists and is a list, Err otherwise
    #[inline]
    pub fn ltrim(&self, key: &[u8], start: i64, stop: i64) -> Result<()> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
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

                    if start > stop || start >= len || stop < 0 {
                        list.clear();
                        true
                    } else {
                        let start_idx = start.max(0) as usize;
                        let stop_idx = (stop + 1).min(len) as usize;

                        let keep_end = stop_idx;
                        let remove_end = list.len() - keep_end;
                        for _ in 0..remove_end {
                            list.pop_back();
                        }

                        for _ in 0..start_idx {
                            list.pop_front();
                        }
                        list.is_empty()
                    }
                };

                entry.bump_version();
                if is_empty {
                    e.remove();
                }
                Ok(())
            }
            crate::storage::dashtable::Entry::Vacant(_) => Ok(()),
        }
    }

    /// Insert element before or after pivot
    /// Returns list length after insert, or -1 if pivot not found, or 0 if key doesn't exist
    #[inline]
    pub fn linsert(&self, key: &[u8], before: bool, pivot: &[u8], element: Bytes) -> Result<i64> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return Ok(0);
                }

                let result = {
                    let list = match entry.data.as_list_mut() {
                        Some(l) => l,
                        None => return Err(Error::WrongType),
                    };

                    let pivot_pos = list.iter().position(|x| x.as_ref() == pivot);

                    match pivot_pos {
                        Some(pos) => {
                            let insert_pos = if before { pos } else { pos + 1 };
                            list.insert(insert_pos, element);
                            Some(list.len() as i64)
                        }
                        None => None,
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
            crate::storage::dashtable::Entry::Vacant(_) => Ok(0),
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
        let entry_ref = self.data_get(key)?;
        if entry_ref.1.is_expired() {
            return None;
        }

        let list = entry_ref.1.data.as_list()?;
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
            let start = len.saturating_sub(limit);
            for i in (start..len).rev() {
                if let Some(item) = list.get(i) {
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
        } else {
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
