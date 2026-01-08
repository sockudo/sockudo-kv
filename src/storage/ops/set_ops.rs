use bytes::Bytes;
use dashmap::DashSet;
use dashmap::mapref::entry::Entry as DashEntry;

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
        match self.data.entry(key) {
            DashEntry::Occupied(mut e) => {
                let entry = e.get_mut();
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
            DashEntry::Vacant(e) => {
                let set = DashSet::new();
                for member in &members {
                    set.insert(member.clone());
                }
                let count = set.len();
                e.insert(Entry::new(DataType::Set(set)));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(count)
            }
        }
    }

    /// Remove members from set
    #[inline]
    pub fn srem(&self, key: &[u8], members: &[Bytes]) -> usize {
        match self.data.get_mut(key) {
            Some(mut e) => {
                let entry = e.value_mut();
                if entry.is_expired() {
                    drop(e);
                    self.del(key);
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
                    drop(e);
                    self.del(key);
                }
                removed
            }
            None => 0,
        }
    }

    /// Check if member is in set
    #[inline]
    pub fn sismember(&self, key: &[u8], member: &[u8]) -> bool {
        match self.data.get(key) {
            Some(e) => {
                if e.is_expired() {
                    return false;
                }
                match e.data.as_set() {
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
        match self.data.get(key) {
            Some(e) => {
                if e.is_expired() {
                    return vec![];
                }
                match e.data.as_set() {
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
        match self.data.get(key) {
            Some(e) => {
                if e.is_expired() {
                    return 0;
                }
                match e.data.as_set() {
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
        let mut entry = match self.data.get_mut(key) {
            Some(e) => e,
            None => return vec![],
        };

        if entry.is_expired() {
            drop(entry);
            self.del(key);
            return vec![];
        }

        let (result, is_empty) = {
            let set = match entry.data.as_set_mut() {
                Some(s) => s,
                None => return vec![],
            };

            if set.is_empty() {
                return vec![];
            }

            let mut result = Vec::with_capacity(count.min(set.len()));

            // For efficiency, we collect keys to remove then remove them
            // Since DashSet doesn't have random access, we iterate
            let to_remove: Vec<Bytes> = set.iter().take(count).map(|r| r.clone()).collect();

            for member in &to_remove {
                if let Some(removed) = set.remove(member) {
                    result.push(removed);
                }
            }

            (result, set.is_empty())
        };

        if !result.is_empty() {
            entry.bump_version();
        }

        if is_empty {
            drop(entry);
            self.del(key);
        }

        result
    }

    /// Get one or more random members from set without removing
    /// count > 0: return up to count unique members
    /// count < 0: return abs(count) members, possibly with duplicates
    #[inline]
    pub fn srandmember(&self, key: &[u8], count: i64) -> Vec<Bytes> {
        let entry = match self.data.get(key) {
            Some(e) => e,
            None => return vec![],
        };

        if entry.is_expired() {
            return vec![];
        }

        let set = match entry.data.as_set() {
            Some(s) => s,
            None => return vec![],
        };

        if set.is_empty() {
            return vec![];
        }

        let allow_duplicates = count < 0;
        let want = count.unsigned_abs() as usize;

        if allow_duplicates {
            // With duplicates, we can return more than the set size
            // Use simple random selection
            let members: Vec<Bytes> = set.iter().map(|r| r.clone()).collect();
            let mut result = Vec::with_capacity(want);
            for _ in 0..want {
                let idx = fastrand::usize(..members.len());
                result.push(members[idx].clone());
            }
            result
        } else {
            // Without duplicates, return up to min(count, set.len())
            let take = want.min(set.len());
            set.iter().take(take).map(|r| r.clone()).collect()
        }
    }

    /// Get difference of sets (first set minus all others)
    #[inline]
    pub fn sdiff(&self, keys: &[Bytes]) -> DashSet<Bytes> {
        if keys.is_empty() {
            return DashSet::new();
        }

        // Get first set
        let first = match self.get_set(&keys[0]) {
            Some(s) => s,
            None => return DashSet::new(),
        };

        if keys.len() == 1 {
            return first;
        }

        // Subtract all other sets
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
    /// Optimized to start with smallest set
    #[inline]
    pub fn sinter(&self, keys: &[Bytes]) -> DashSet<Bytes> {
        if keys.is_empty() {
            return DashSet::new();
        }

        // Collect all sets and find smallest
        let mut sets: Vec<DashSet<Bytes>> = Vec::with_capacity(keys.len());
        for key in keys {
            match self.get_set(key) {
                Some(s) => sets.push(s),
                None => return DashSet::new(), // Empty set means empty intersection
            }
        }

        if sets.is_empty() {
            return DashSet::new();
        }

        // Sort by size to start with smallest (optimization)
        sets.sort_by_key(|s| s.len());

        let first = sets.remove(0);
        let result = DashSet::new();

        // Only keep members that exist in all sets
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
        // First check if member exists in source
        {
            let mut src_entry = match self.data.get_mut(src) {
                Some(e) => e,
                None => return Ok(false),
            };

            if src_entry.is_expired() {
                drop(src_entry);
                self.del(src);
                return Ok(false);
            }

            let src_set = match src_entry.data.as_set_mut() {
                Some(s) => s,
                None => return Err(Error::WrongType),
            };

            if src_set.remove(&member).is_none() {
                return Ok(false); // Member not in source
            }

            // Clean up empty source set
            if src_set.is_empty() {
                drop(src_entry);
                self.del(src);
            }
        }

        // Add to destination
        self.sadd(dst, vec![member])?;
        Ok(true)
    }

    /// Helper to get a copy of a set for read operations
    fn get_set(&self, key: &[u8]) -> Option<DashSet<Bytes>> {
        match self.data.get(key) {
            Some(e) => {
                if e.is_expired() {
                    return None;
                }
                e.data.as_set().cloned()
            }
            None => None,
        }
    }

    /// Store set operation result into destination key
    pub fn set_store(&self, dst: Bytes, set: DashSet<Bytes>) -> usize {
        let count = set.len();
        if set.is_empty() {
            self.del(&dst);
        } else {
            match self.data.entry(dst) {
                DashEntry::Occupied(mut e) => {
                    let entry = e.get_mut();
                    entry.data = DataType::Set(set);
                    entry.persist();
                    entry.bump_version();
                }
                DashEntry::Vacant(e) => {
                    e.insert(Entry::new(DataType::Set(set)));
                    self.key_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        count
    }
}
