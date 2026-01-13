use bytes::Bytes;
use dashmap::DashSet;

use crate::error::{Error, Result};
use crate::storage::Store;
use crate::storage::types::{DataType, Entry};

use std::sync::atomic::Ordering;

/// Set operations for the Store
impl Store {
    // ==================== Set operations ====================

    /// Add members to set
    #[inline]
    pub fn sadd(&self, key: Bytes, members: Vec<Bytes>) -> Result<usize> {
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    let set = DashSet::new();
                    for member in &members {
                        set.insert(member.clone());
                    }
                    let count = set.len();
                    entry.data = DataType::Set(set);
                    entry.persist();
                    entry.bump_version();
                    return Ok(count);
                }

                match &mut entry.data {
                    DataType::Set(set) => {
                        let mut added = 0;
                        for member in &members {
                            if set.insert(member.clone()) {
                                added += 1;
                            }
                        }
                        if added > 0 {
                            entry.bump_version();
                        }
                        Ok(added)
                    }
                    _ => Err(Error::WrongType),
                }
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                let set = DashSet::new();
                for member in &members {
                    set.insert(member.clone());
                }
                let count = set.len();
                e.insert((key, Entry::new(DataType::Set(set))));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(count)
            }
        }
    }

    /// Remove members from set
    #[inline]
    pub fn srem(&self, key: &[u8], members: &[Bytes]) -> usize {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return 0;
                }

                let (removed, is_empty) = match &mut entry.data {
                    DataType::Set(set) => {
                        let mut removed = 0;
                        for member in members {
                            if set.remove(member).is_some() {
                                removed += 1;
                            }
                        }
                        (removed, set.is_empty())
                    }
                    _ => return 0,
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

    /// Check if member is in set
    #[inline]
    pub fn sismember(&self, key: &[u8], member: &[u8]) -> bool {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return false;
                }
                match entry_ref.1.data.as_set() {
                    Some(set) => set.contains(member),
                    None => false,
                }
            }
            None => false,
        }
    }

    /// Get all members of set
    #[inline]
    pub fn smembers(&self, key: &[u8]) -> Vec<Bytes> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return vec![];
                }
                match entry_ref.1.data.as_set() {
                    Some(set) => set.iter().map(|r| r.clone()).collect(),
                    None => vec![],
                }
            }
            None => vec![],
        }
    }

    /// Get set cardinality (number of members)
    #[inline]
    pub fn scard(&self, key: &[u8]) -> usize {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return 0;
                }
                match entry_ref.1.data.as_set() {
                    Some(set) => set.len(),
                    None => 0,
                }
            }
            None => 0,
        }
    }

    /// Pop one or more random members from set
    /// Returns the popped members
    #[inline]
    pub fn spop(&self, key: &[u8], count: usize) -> Vec<Bytes> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return vec![];
                }

                let (result, is_empty) = match &mut entry.data {
                    DataType::Set(set) => {
                        if set.is_empty() {
                            (vec![], true)
                        } else {
                            let mut result = Vec::with_capacity(count.min(set.len()));
                            let to_remove: Vec<Bytes> =
                                set.iter().take(count).map(|r| r.clone()).collect();
                            for member in &to_remove {
                                if let Some(removed) = set.remove(member) {
                                    result.push(removed);
                                }
                            }
                            (result, set.is_empty())
                        }
                    }
                    _ => (vec![], false),
                };

                if !result.is_empty() {
                    entry.bump_version();
                }
                if is_empty {
                    e.remove();
                }
                result
            }
            crate::storage::dashtable::Entry::Vacant(_) => vec![],
        }
    }

    /// Get one or more random members from set without removing
    /// count > 0: return up to count unique members
    /// count < 0: return abs(count) members, possibly with duplicates
    #[inline]
    pub fn srandmember(&self, key: &[u8], count: i64) -> Vec<Bytes> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return vec![];
                }
                match entry_ref.1.data.as_set() {
                    Some(set) => {
                        if set.is_empty() {
                            return vec![];
                        }
                        let allow_duplicates = count < 0;
                        let want = count.unsigned_abs() as usize;

                        if allow_duplicates {
                            let members: Vec<Bytes> = set.iter().map(|r| r.clone()).collect();
                            let mut result = Vec::with_capacity(want);
                            for _ in 0..want {
                                let idx = fastrand::usize(..members.len());
                                result.push(members[idx].clone());
                            }
                            result
                        } else {
                            let take = want.min(set.len());
                            set.iter().take(take).map(|r| r.clone()).collect()
                        }
                    }
                    None => vec![],
                }
            }
            None => vec![],
        }
    }

    /// Get difference of sets (first set minus all others)
    #[inline]
    pub fn sdiff(&self, keys: &[Bytes]) -> DashSet<Bytes> {
        if keys.is_empty() {
            return DashSet::new();
        }

        let first = match self.get_set(&keys[0]) {
            Some(s) => s,
            None => return DashSet::new(),
        };

        if keys.len() == 1 {
            return first;
        }

        let result = first;
        for key in &keys[1..] {
            if let Some(other) = self.get_set(key) {
                for member in other.iter() {
                    result.remove(&*member);
                }
            }
        }

        result
    }

    /// Get intersection of sets
    #[inline]
    pub fn sinter(&self, keys: &[Bytes]) -> DashSet<Bytes> {
        if keys.is_empty() {
            return DashSet::new();
        }

        let mut sets: Vec<DashSet<Bytes>> = Vec::with_capacity(keys.len());
        for key in keys {
            match self.get_set(key) {
                Some(s) => sets.push(s),
                None => return DashSet::new(),
            }
        }

        sets.sort_by_key(|s| s.len());

        let first = sets.remove(0);
        let result = DashSet::new();

        for member in first.iter() {
            let in_all = sets.iter().all(|s| s.contains(&*member));
            if in_all {
                result.insert(member.clone());
            }
        }

        result
    }

    /// Get union of sets
    #[inline]
    pub fn sunion(&self, keys: &[Bytes]) -> DashSet<Bytes> {
        let result = DashSet::new();
        for key in keys {
            if let Some(set) = self.get_set(key) {
                for member in set.iter() {
                    result.insert(member.clone());
                }
            }
        }
        result
    }

    /// Move member from source set to destination set
    /// Returns true if member was moved
    #[inline]
    pub fn smove(&self, src: &[u8], dst: Bytes, member: Bytes) -> Result<bool> {
        let removed = match self.data_entry(src) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    false
                } else {
                    let src_set = match entry.data.as_set_mut() {
                        Some(s) => s,
                        None => return Err(Error::WrongType),
                    };

                    let r = src_set.remove(&member).is_some();
                    if r && src_set.is_empty() {
                        e.remove();
                    }
                    r
                }
            }
            crate::storage::dashtable::Entry::Vacant(_) => false,
        };

        if removed {
            self.sadd(dst, vec![member])?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Helper to get a copy of a set for read operations
    fn get_set(&self, key: &[u8]) -> Option<DashSet<Bytes>> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return None;
                }
                entry_ref.1.data.as_set().cloned()
            }
            None => None,
        }
    }

    /// Store set operation result into destination key
    pub fn set_store(&self, dst: Bytes, set: DashSet<Bytes>) -> usize {
        let count = set.len();
        if set.is_empty() {
            self.data_remove(&dst);
        } else {
            match self.data_entry(&dst) {
                crate::storage::dashtable::Entry::Occupied(mut e) => {
                    let entry = &mut e.get_mut().1;
                    entry.data = DataType::Set(set);
                    entry.persist();
                    entry.bump_version();
                }
                crate::storage::dashtable::Entry::Vacant(e) => {
                    e.insert((dst, Entry::new(DataType::Set(set))));
                    self.key_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        count
    }
}
