use bytes::Bytes;
use dashmap::DashSet;

use crate::error::{Error, Result};
use crate::storage::Store;
use crate::storage::listpack::Listpack;
use crate::storage::types::{DataType, Entry};

use std::sync::atomic::Ordering;

/// Check if a string is a valid integer for intset storage.
/// Returns None if string has leading zeros (except "0" itself) or can't be parsed.
/// This ensures that "013" is NOT treated as integer 13, preserving the original string.
#[inline]
fn parse_int_for_set(s: &str) -> Option<i64> {
    if s.is_empty() {
        return None;
    }

    let bytes = s.as_bytes();

    // Check for leading zeros
    if bytes[0] == b'0' && bytes.len() > 1 {
        // "0" is valid, but "00", "01", "007" etc are not valid integers
        return None;
    }

    // Check for negative numbers with leading zeros after the minus sign
    if bytes[0] == b'-' && bytes.len() > 2 && bytes[1] == b'0' {
        // "-0" is valid (equals 0), but "-00", "-01" etc are not
        return None;
    }

    s.parse::<i64>().ok()
}

/// Set operations for the Store
impl Store {
    // ==================== Set operations ====================

    /// Add members to set
    #[inline]
    pub fn sadd(&self, key: Bytes, members: Vec<Bytes>) -> Result<usize> {
        let max_intset_entries = self.encoding.read().set_max_intset_entries;
        let max_listpack_entries = self.encoding.read().set_max_listpack_entries;
        let max_listpack_value = self.encoding.read().set_max_listpack_value;

        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    // Start fresh - choose best encoding
                    let (data, count) = Self::create_set_data(
                        &members,
                        max_intset_entries,
                        max_listpack_entries,
                        max_listpack_value,
                    );
                    entry.data = data;
                    entry.persist();
                    entry.bump_version();
                    return Ok(count);
                }

                // Existing entry
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
                    DataType::SetPacked(lp) => {
                        let mut added = 0;

                        // Redis/Dragonfly approach: check if any member exceeds value size limit
                        let has_large_value = members.iter().any(|m| m.len() > max_listpack_value);

                        if has_large_value {
                            // Immediate upgrade due to large value
                            let new_set = DashSet::new();
                            for member in lp.siter() {
                                new_set.insert(member);
                            }
                            for member in &members {
                                if new_set.insert(member.clone()) {
                                    added += 1;
                                }
                            }
                            entry.data = DataType::Set(new_set);
                        } else {
                            // Add members one by one, upgrade only when actually needed
                            for member in &members {
                                if lp.scontains(member) {
                                    continue; // Already exists, skip
                                }
                                // Check if adding this member would exceed limit
                                if lp.len() >= max_listpack_entries {
                                    // Need to upgrade to hashtable
                                    let new_set = DashSet::new();
                                    for existing in lp.siter() {
                                        new_set.insert(existing);
                                    }
                                    new_set.insert(member.clone());
                                    added += 1;
                                    // Add remaining members
                                    for remaining in members
                                        .iter()
                                        .skip(members.iter().position(|m| m == member).unwrap() + 1)
                                    {
                                        if new_set.insert(remaining.clone()) {
                                            added += 1;
                                        }
                                    }
                                    entry.data = DataType::Set(new_set);
                                    if added > 0 {
                                        entry.bump_version();
                                    }
                                    return Ok(added);
                                }
                                if lp.sinsert(member) {
                                    added += 1;
                                }
                            }
                        }
                        if added > 0 {
                            entry.bump_version();
                        }
                        Ok(added)
                    }
                    DataType::IntSet(intset) => {
                        let mut added = 0;
                        let mut must_convert = false;
                        let mut non_int_members = false;

                        // Check if any members are non-integers
                        for m in &members {
                            if let Ok(s) = std::str::from_utf8(m) {
                                if parse_int_for_set(s).is_none() {
                                    non_int_members = true;
                                    break;
                                }
                            } else {
                                non_int_members = true;
                                break;
                            }
                        }

                        // Check if adding would exceed intset limit
                        // We need to count how many unique new members we'd add
                        let mut potential_new_count = 0;
                        if !non_int_members {
                            for m in &members {
                                if let Ok(s) = std::str::from_utf8(m) {
                                    if let Some(i) = parse_int_for_set(s) {
                                        if !intset.contains(i) {
                                            potential_new_count += 1;
                                        }
                                    }
                                }
                            }
                            if intset.len() + potential_new_count > max_intset_entries {
                                must_convert = true;
                            }
                        }

                        if non_int_members || must_convert {
                            // Convert to SetPacked or DashSet based on size
                            let total_size = intset.len() + members.len();
                            let any_large = members.iter().any(|m| m.len() > max_listpack_value);

                            if total_size <= max_listpack_entries && !any_large && !must_convert {
                                // Convert to SetPacked (only if we have non-int members, not size overflow)
                                let mut lp = Listpack::with_capacity(total_size);
                                for i in intset.iter() {
                                    lp.sinsert(i.to_string().as_bytes());
                                }
                                for member in &members {
                                    if lp.sinsert(member) {
                                        added += 1;
                                    }
                                }
                                entry.data = DataType::SetPacked(lp);
                            } else {
                                // Convert to DashSet (hashtable)
                                let new_set = DashSet::new();
                                for i in intset.iter() {
                                    new_set.insert(Bytes::from(i.to_string()));
                                }
                                for member in &members {
                                    if new_set.insert(member.clone()) {
                                        added += 1;
                                    }
                                }
                                entry.data = DataType::Set(new_set);
                            }
                        } else {
                            // Stay as intset, just add the integers
                            for m in &members {
                                if let Ok(s) = std::str::from_utf8(m) {
                                    if let Some(i) = parse_int_for_set(s) {
                                        if intset.insert(i) {
                                            added += 1;
                                        }
                                    }
                                }
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
                let (data, count) = Self::create_set_data(
                    &members,
                    max_intset_entries,
                    max_listpack_entries,
                    max_listpack_value,
                );
                e.insert((key, Entry::new(data)));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(count)
            }
        }
    }

    /// Create optimal set data structure for given members
    fn create_set_data(
        members: &[Bytes],
        max_intset_entries: usize,
        max_listpack_entries: usize,
        max_listpack_value: usize,
    ) -> (DataType, usize) {
        // Check if all integers (use IntSet)
        let mut all_ints = true;
        let mut int_members = Vec::with_capacity(members.len());
        for m in members {
            if let Ok(s) = std::str::from_utf8(m) {
                if let Some(i) = parse_int_for_set(s) {
                    int_members.push(i);
                } else {
                    all_ints = false;
                    break;
                }
            } else {
                all_ints = false;
                break;
            }
        }

        // Use IntSet only if all integers AND within size limit
        if all_ints && int_members.len() <= max_intset_entries {
            let mut intset = crate::storage::intset::IntSet::new();
            for i in int_members {
                intset.insert(i);
            }
            let count = intset.len();
            return (DataType::IntSet(intset), count);
        }

        // Check if we can use SetPacked (listpack)
        let can_use_listpack = members.len() <= max_listpack_entries
            && members.iter().all(|m| m.len() <= max_listpack_value);

        if can_use_listpack {
            let mut lp = Listpack::with_capacity(members.len());
            for member in members {
                lp.sinsert(member);
            }
            let count = lp.len();
            (DataType::SetPacked(lp), count)
        } else {
            let set = DashSet::new();
            for member in members {
                set.insert(member.clone());
            }
            let count = set.len();
            (DataType::Set(set), count)
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
                    DataType::SetPacked(lp) => {
                        let mut removed = 0;
                        for member in members {
                            if lp.sremove(member) {
                                removed += 1;
                            }
                        }
                        (removed, lp.is_empty())
                    }
                    DataType::IntSet(intset) => {
                        let mut removed = 0;
                        for member in members {
                            if let Ok(s) = std::str::from_utf8(member) {
                                if let Some(i) = parse_int_for_set(s) {
                                    if intset.remove(i) {
                                        removed += 1;
                                    }
                                }
                            }
                        }
                        (removed, intset.is_empty())
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
    /// Returns Err(WrongType) if key exists but is not a set
    #[inline]
    pub fn sismember(&self, key: &[u8], member: &[u8]) -> Result<bool> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return Ok(false);
                }
                match &entry_ref.1.data {
                    DataType::Set(set) => Ok(set.contains(member)),
                    DataType::SetPacked(lp) => Ok(lp.scontains(member)),
                    DataType::IntSet(intset) => {
                        if let Ok(s) = std::str::from_utf8(member) {
                            if let Some(i) = parse_int_for_set(s) {
                                return Ok(intset.contains(i));
                            }
                        }
                        Ok(false)
                    }
                    _ => Err(Error::WrongType),
                }
            }
            None => Ok(false),
        }
    }

    /// Get all members of set
    /// Returns Err(WrongType) if key exists but is not a set
    #[inline]
    pub fn smembers(&self, key: &[u8]) -> Result<Vec<Bytes>> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return Ok(vec![]);
                }
                match &entry_ref.1.data {
                    DataType::Set(set) => Ok(set.iter().map(|r| r.clone()).collect()),
                    DataType::SetPacked(lp) => Ok(lp.siter().collect()),
                    DataType::IntSet(intset) => {
                        Ok(intset.iter().map(|i| Bytes::from(i.to_string())).collect())
                    }
                    _ => Err(Error::WrongType),
                }
            }
            None => Ok(vec![]),
        }
    }

    /// Get set cardinality (number of members)
    /// Returns Err(WrongType) if key exists but is not a set
    #[inline]
    pub fn scard(&self, key: &[u8]) -> Result<usize> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return Ok(0);
                }
                match &entry_ref.1.data {
                    DataType::Set(set) => Ok(set.len()),
                    DataType::SetPacked(lp) => Ok(lp.len()),
                    DataType::IntSet(intset) => Ok(intset.len()),
                    _ => Err(Error::WrongType),
                }
            }
            None => Ok(0),
        }
    }

    /// Pop one or more random members from set
    /// Returns the popped members
    #[inline]
    /// Pop members from set
    /// Returns (popped_members, key_was_deleted)
    pub fn spop(&self, key: &[u8], count: usize) -> (Vec<Bytes>, bool) {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return (vec![], false);
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
                    DataType::SetPacked(lp) => {
                        if lp.is_empty() {
                            (vec![], true)
                        } else {
                            let members: Vec<Bytes> = lp.siter().collect();
                            let n = count.min(members.len());
                            let to_pop: Vec<Bytes> = members.into_iter().take(n).collect();
                            for member in &to_pop {
                                lp.sremove(member);
                            }
                            (to_pop, lp.is_empty())
                        }
                    }
                    DataType::IntSet(intset) => {
                        if intset.is_empty() {
                            (vec![], true)
                        } else {
                            let mut popped = Vec::new();
                            let target_len = intset.len();
                            let n = count.min(target_len);

                            if n == target_len {
                                // Pop all
                                let members =
                                    intset.iter().map(|i| Bytes::from(i.to_string())).collect();
                                (members, true)
                            } else {
                                // Pop N random items using select(rank)
                                for _ in 0..n {
                                    // Note: Repeatedly removing by logical index 0..len is not uniform if we just pick random?
                                    // If we pick random index, we get uniform.
                                    // We need to pick random index in range [0, current_len)
                                    // current_len decreases.
                                    let current_len = intset.len() as u64;
                                    if current_len == 0 {
                                        break;
                                    }
                                    let rank = fastrand::u64(0..current_len);
                                    if let Some(val_u64) = intset.inner.select(rank) {
                                        // Remove it
                                        intset.inner.remove(val_u64);
                                        let val = val_u64 as i64;
                                        popped.push(Bytes::from(val.to_string()));
                                    }
                                }
                                (popped, intset.is_empty())
                            }
                        }
                    }
                    _ => (vec![], false),
                };

                if !result.is_empty() {
                    entry.bump_version();
                }
                let key_deleted = is_empty && !result.is_empty();
                if is_empty {
                    e.remove();
                }
                (result, key_deleted)
            }
            crate::storage::dashtable::Entry::Vacant(_) => (vec![], false),
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
                match &entry_ref.1.data {
                    DataType::Set(set) => {
                        if set.is_empty() {
                            return vec![];
                        }
                        let allow_duplicates = count < 0;
                        let want = count.unsigned_abs() as usize;

                        let members: Vec<Bytes> = set.iter().map(|r| r.clone()).collect();
                        if allow_duplicates {
                            let mut result = Vec::with_capacity(want);
                            for _ in 0..want {
                                let idx = fastrand::usize(..members.len());
                                result.push(members[idx].clone());
                            }
                            result
                        } else {
                            let take = want.min(members.len());
                            if take >= members.len() {
                                members
                            } else {
                                // Shuffle indices and take the first `take` elements
                                let mut indices: Vec<usize> = (0..members.len()).collect();
                                fastrand::shuffle(&mut indices);
                                indices
                                    .into_iter()
                                    .take(take)
                                    .map(|i| members[i].clone())
                                    .collect()
                            }
                        }
                    }

                    DataType::SetPacked(lp) => {
                        if lp.is_empty() {
                            return vec![];
                        }
                        let members: Vec<Bytes> = lp.siter().collect();
                        let allow_duplicates = count < 0;
                        let want = count.unsigned_abs() as usize;

                        if allow_duplicates {
                            let mut result = Vec::with_capacity(want);
                            for _ in 0..want {
                                let idx = fastrand::usize(..members.len());
                                result.push(members[idx].clone());
                            }
                            result
                        } else {
                            let take = want.min(members.len());
                            if take >= members.len() {
                                members
                            } else {
                                // Shuffle indices and take the first `take` elements
                                let mut indices: Vec<usize> = (0..members.len()).collect();
                                fastrand::shuffle(&mut indices);
                                indices
                                    .into_iter()
                                    .take(take)
                                    .map(|i| members[i].clone())
                                    .collect()
                            }
                        }
                    }

                    DataType::IntSet(intset) => {
                        if intset.is_empty() {
                            return vec![];
                        }
                        let allow_duplicates = count < 0;
                        let want = count.unsigned_abs() as usize;

                        if allow_duplicates {
                            let mut result = Vec::with_capacity(want);
                            let len = intset.len() as u64;
                            for _ in 0..want {
                                let rank = fastrand::u64(0..len);
                                if let Some(val) = intset.inner.select(rank) {
                                    result.push(Bytes::from((val as i64).to_string()));
                                }
                            }
                            result
                        } else {
                            if want >= intset.len() {
                                intset.iter().map(|i| Bytes::from(i.to_string())).collect()
                            } else {
                                // Pick unique indices
                                // Rejection sampling or Fisher-Yates if access to vector?
                                // We don't have vector.
                                // We have select(rank).
                                // We can sample ranks.
                                let len = intset.len();
                                let mut indices: Vec<usize> = (0..len).collect();
                                fastrand::shuffle(&mut indices);
                                indices
                                    .into_iter()
                                    .take(want)
                                    .map(|rank| {
                                        let val = intset.inner.select(rank as u64).unwrap_or(0);
                                        Bytes::from((val as i64).to_string())
                                    })
                                    .collect()
                            }
                        }
                    }

                    _ => vec![],
                }
            }
            None => vec![],
        }
    }

    /// Get difference of sets (first set minus all others)
    /// Returns Err(WrongType) if any key exists but is not a set
    #[inline]
    pub fn sdiff(&self, keys: &[Bytes]) -> Result<DashSet<Bytes>> {
        if keys.is_empty() {
            return Ok(DashSet::new());
        }

        // First, check all keys for type errors
        for key in keys {
            self.get_set_or_error(key)?;
        }

        let first = match self.get_set_or_error(&keys[0])? {
            Some(s) => s,
            None => return Ok(DashSet::new()),
        };

        if keys.len() == 1 {
            return Ok(first);
        }

        let result = first;
        for key in &keys[1..] {
            if let Some(other) = self.get_set_or_error(key)? {
                for member in other.iter() {
                    result.remove(&*member);
                }
            }
        }

        Ok(result)
    }

    /// Get intersection of sets
    /// Returns Err(WrongType) if any key exists but is not a set
    #[inline]
    pub fn sinter(&self, keys: &[Bytes]) -> Result<DashSet<Bytes>> {
        if keys.is_empty() {
            return Ok(DashSet::new());
        }

        // First, check all keys for type errors
        for key in keys {
            self.get_set_or_error(key)?;
        }

        let mut sets: Vec<DashSet<Bytes>> = Vec::with_capacity(keys.len());
        for key in keys {
            match self.get_set_or_error(key)? {
                Some(s) => sets.push(s),
                None => return Ok(DashSet::new()),
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

        Ok(result)
    }

    /// Get cardinality of set intersection with optional limit
    /// SINTERCARD numkeys key [key ...] [LIMIT limit]
    /// Returns Err(WrongType) if any key exists but is not a set
    #[inline]
    pub fn sintercard(&self, keys: &[Bytes], limit: usize) -> Result<usize> {
        if keys.is_empty() {
            return Ok(0);
        }

        let mut sets: Vec<DashSet<Bytes>> = Vec::with_capacity(keys.len());
        for key in keys {
            match self.get_set_or_error(key)? {
                Some(s) => sets.push(s),
                None => return Ok(0), // Empty set means intersection is empty
            }
        }

        // Sort by size for efficiency (smallest first)
        sets.sort_by_key(|s| s.len());

        let first = sets.remove(0);
        let mut count = 0;
        let effective_limit = if limit == 0 { usize::MAX } else { limit };

        for member in first.iter() {
            let in_all = sets.iter().all(|s| s.contains(&*member));
            if in_all {
                count += 1;
                if count >= effective_limit {
                    break;
                }
            }
        }

        Ok(count)
    }

    /// Get union of sets
    /// Returns Err(WrongType) if any key exists but is not a set
    #[inline]
    pub fn sunion(&self, keys: &[Bytes]) -> Result<DashSet<Bytes>> {
        // First, check all keys for type errors
        for key in keys {
            self.get_set_or_error(key)?;
        }

        let result = DashSet::new();
        for key in keys {
            if let Some(set) = self.get_set_or_error(key)? {
                for member in set.iter() {
                    result.insert(member.clone());
                }
            }
        }
        Ok(result)
    }

    /// Move member from source set to destination set
    /// Returns true if member was moved
    #[inline]
    pub fn smove(&self, src: &[u8], dst: Bytes, member: Bytes) -> Result<bool> {
        // Check destination type upfront (Redis behavior)
        if let Some(entry_ref) = self.data_get(&dst) {
            if !entry_ref.1.is_expired() {
                match &entry_ref.1.data {
                    DataType::Set(_) | DataType::SetPacked(_) | DataType::IntSet(_) => {}
                    _ => return Err(Error::WrongType),
                }
            }
        }

        let removed = match self.data_entry(src) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    false
                } else {
                    match &mut entry.data {
                        DataType::Set(set) => {
                            let r = set.remove(&member).is_some();
                            if r && set.is_empty() {
                                e.remove();
                            }
                            r
                        }
                        DataType::SetPacked(lp) => {
                            let r = lp.sremove(&member);
                            if r && lp.is_empty() {
                                e.remove();
                            }
                            r
                        }
                        DataType::IntSet(intset) => {
                            let mut r = false;
                            if let Ok(s) = std::str::from_utf8(&member) {
                                if let Some(i) = parse_int_for_set(s) {
                                    r = intset.remove(i);
                                }
                            }
                            if r && intset.is_empty() {
                                e.remove();
                            }
                            r
                        }
                        _ => return Err(Error::WrongType),
                    }
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

    /// Helper to get a copy of a set for read operations, returning WRONGTYPE error if key exists but is not a set
    fn get_set_or_error(&self, key: &[u8]) -> Result<Option<DashSet<Bytes>>> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return Ok(None);
                }
                match &entry_ref.1.data {
                    DataType::Set(set) => Ok(Some(set.clone())),
                    DataType::SetPacked(lp) => {
                        let set = DashSet::new();
                        for member in lp.siter() {
                            set.insert(member);
                        }
                        Ok(Some(set))
                    }
                    DataType::IntSet(intset) => {
                        let set = DashSet::new();
                        for i in intset.iter() {
                            set.insert(Bytes::from(i.to_string()));
                        }
                        Ok(Some(set))
                    }
                    _ => Err(Error::WrongType),
                }
            }
            None => Ok(None),
        }
    }

    /// Store set operation result into destination key using optimal encoding
    pub fn set_store(&self, dst: Bytes, set: DashSet<Bytes>) -> usize {
        let count = set.len();
        if set.is_empty() {
            self.data_remove(&dst);
        } else {
            let max_intset_entries = self.encoding.read().set_max_intset_entries;
            let max_listpack_entries = self.encoding.read().set_max_listpack_entries;
            let max_listpack_value = self.encoding.read().set_max_listpack_value;

            // Convert DashSet to Vec for processing
            let members: Vec<Bytes> = set.into_iter().collect();

            // Use optimal encoding based on content
            let (data, _) = Self::create_set_data(
                &members,
                max_intset_entries,
                max_listpack_entries,
                max_listpack_value,
            );

            match self.data_entry(&dst) {
                crate::storage::dashtable::Entry::Occupied(mut e) => {
                    let entry = &mut e.get_mut().1;
                    entry.data = data;
                    entry.persist();
                    entry.bump_version();
                }
                crate::storage::dashtable::Entry::Vacant(e) => {
                    e.insert((dst, Entry::new(data)));
                    self.key_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        count
    }

    /// SSCAN key cursor [MATCH pattern] [COUNT count]
    /// High-performance scan using hash-based cursor iteration.
    /// Uses reverse binary iteration to survive rehashing during iteration.
    pub fn sscan(
        &self,
        key: &[u8],
        cursor: u64,
        pattern: Option<&[u8]>,
        count: usize,
    ) -> Result<(u64, Vec<Bytes>)> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return Ok((0, vec![]));
                }

                match &entry_ref.1.data {
                    DataType::Set(set) => {
                        // Hash-based cursor scan for DashSet
                        // Use high bits of hash as bucket index for stable iteration
                        Self::scan_dashset(set, cursor, pattern, count)
                    }
                    DataType::SetPacked(lp) => {
                        // For small listpack sets, simple linear scan is efficient
                        // Cursor is just an index
                        let all_members: Vec<Bytes> = lp.siter().collect();
                        let total = all_members.len();
                        let start = cursor as usize;

                        if start >= total {
                            return Ok((0, vec![]));
                        }

                        let end = (start + count).min(total);
                        let mut result = Vec::new();

                        for member in &all_members[start..end] {
                            let matches =
                                pattern.map_or(true, |p| crate::storage::match_pattern(p, member));
                            if matches {
                                result.push(member.clone());
                            }
                        }

                        let next_cursor = if end >= total { 0 } else { end as u64 };
                        Ok((next_cursor, result))
                    }
                    DataType::IntSet(intset) => {
                        // For intsets, use index-based cursor (stable ordering)
                        let all_members: Vec<Bytes> =
                            intset.iter().map(|i| Bytes::from(i.to_string())).collect();

                        let total = all_members.len();
                        let start = cursor as usize;
                        if start >= total {
                            return Ok((0, vec![]));
                        }

                        let end = (start + count).min(total);
                        let mut result = Vec::new();

                        for member in &all_members[start..end] {
                            let matches =
                                pattern.map_or(true, |p| crate::storage::match_pattern(p, member));
                            if matches {
                                result.push(member.clone());
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

    /// High-performance hash-based scan for DashSet using reverse binary iteration.
    /// This algorithm ensures all elements present throughout the scan are returned,
    /// even if elements are added or removed during iteration.
    fn scan_dashset(
        set: &DashSet<Bytes>,
        cursor: u64,
        pattern: Option<&[u8]>,
        count: usize,
    ) -> Result<(u64, Vec<Bytes>)> {
        use crate::storage::dashtable::calculate_hash;

        let total = set.len();
        if total == 0 {
            return Ok((0, vec![]));
        }

        // Determine table size (round up to power of 2 for masking)
        let table_size = total.next_power_of_two().max(16) as u64;
        let mask = table_size - 1;

        let mut result = Vec::with_capacity(count);
        let mut current_cursor = cursor;
        let mut iterations = 0;
        let max_iterations = table_size.max(count as u64 * 10); // Safety limit

        loop {
            // Scan elements whose hash falls into the current cursor bucket
            for member_ref in set.iter() {
                let member = member_ref.key();
                let hash = calculate_hash(member);
                let bucket = hash & mask;

                // Check if this element belongs to the current cursor position
                if bucket == (current_cursor & mask) {
                    let matches =
                        pattern.map_or(true, |p| crate::storage::match_pattern(p, member));
                    if matches {
                        result.push(member.clone());
                    }
                }
            }

            // Advance cursor using reverse binary iteration
            // This ensures we visit all buckets even if table resizes
            current_cursor = reverse_bits_increment(current_cursor, mask);

            iterations += 1;

            // Stop if we've collected enough or completed a full cycle
            if result.len() >= count || current_cursor == 0 || iterations >= max_iterations {
                break;
            }
        }

        Ok((current_cursor, result))
    }
}

/// Reverse binary increment for cursor iteration.
/// This is the key to Redis's SCAN algorithm that survives rehashing.
/// It increments the cursor by reversing bits, adding 1, then reversing back.
#[inline]
fn reverse_bits_increment(cursor: u64, mask: u64) -> u64 {
    // Get the number of bits in the mask
    let bits = (mask + 1).trailing_zeros();
    if bits == 0 {
        return 0;
    }

    // Reverse the bits within the mask range
    let mut v = cursor & mask;
    v = v.reverse_bits() >> (64 - bits);

    // Increment
    v = v.wrapping_add(1);

    // Reverse back
    v = v.reverse_bits() >> (64 - bits);

    // If we've wrapped around, return 0
    if v > mask { 0 } else { v }
}
