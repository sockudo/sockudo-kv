use bytes::Bytes;

use crate::error::{Error, Result};
use crate::storage::Store;
use crate::storage::listpack::Listpack;
use crate::storage::types::{DataType, Entry, SortedSetData};

use std::sync::atomic::Ordering;

/// Upgrade a SortedSetPacked to a full SortedSet
#[inline]
fn upgrade_zset_packed_to_full(lp: &Listpack) -> SortedSetData {
    let mut zset = SortedSetData::new();
    for (member, score) in lp.ziter() {
        zset.insert(member, score);
    }
    zset
}

/// Get sorted entries from listpack (needed for range operations)
#[inline]
fn get_sorted_from_packed(lp: &Listpack) -> Vec<(Bytes, f64)> {
    lp.zrange_sorted()
}

/// Get scores HashMap from listpack (needed for set operations)
#[inline]
fn get_scores_from_packed(lp: &Listpack) -> std::collections::HashMap<Bytes, f64> {
    lp.ziter().collect()
}

/// Sorted set operations for the Store
impl Store {
    // ==================== Sorted Set operations ====================

    /// Add members with scores to sorted set. Uses packed encoding for small sets.
    #[inline]
    pub fn zadd(&self, key: Bytes, members: Vec<(f64, Bytes)>) -> Result<usize> {
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    // Expired entry: create new
                    let max_entries = self.encoding.read().zset_max_listpack_entries;
                    let max_value = self.encoding.read().zset_max_listpack_value;
                    let should_pack = members.len() < max_entries
                        && members.iter().all(|(_, m)| m.len() <= max_value);

                    if should_pack {
                        let mut lp = Listpack::with_capacity(members.len());
                        for (score, member) in &members {
                            lp.zinsert(member, *score);
                        }
                        let count = lp.len();
                        entry.data = DataType::SortedSetPacked(lp);
                        entry.persist();
                        entry.bump_version();
                        return Ok(count);
                    } else {
                        let mut zset = SortedSetData::new();
                        for (score, member) in &members {
                            zset.insert(member.clone(), *score);
                        }
                        let count = zset.len();
                        entry.data = DataType::SortedSet(zset);
                        entry.persist();
                        entry.bump_version();
                        return Ok(count);
                    }
                }

                match &mut entry.data {
                    DataType::SortedSetPacked(lp) => {
                        // Check if we need to upgrade
                        let max_entries = self.encoding.read().zset_max_listpack_entries;
                        let max_value = self.encoding.read().zset_max_listpack_value;
                        let needs_upgrade = members.iter().any(|(_, m)| m.len() > max_value)
                            || lp.len() + members.len() > max_entries;

                        if needs_upgrade {
                            // Upgrade to full SortedSet
                            let mut zset = upgrade_zset_packed_to_full(lp);
                            let mut added = 0;
                            for (score, member) in &members {
                                if zset.insert(member.clone(), *score) {
                                    added += 1;
                                }
                            }
                            entry.data = DataType::SortedSet(zset);
                            entry.bump_version();
                            Ok(added)
                        } else {
                            // Stay packed
                            let mut added = 0;
                            for (score, member) in &members {
                                if lp.zinsert(member, *score) {
                                    added += 1;
                                }
                            }
                            entry.bump_version();
                            Ok(added)
                        }
                    }
                    DataType::SortedSet(zset) => {
                        let mut added = 0;
                        for (score, member) in &members {
                            if zset.insert(member.clone(), *score) {
                                added += 1;
                            }
                        }
                        entry.bump_version();
                        Ok(added)
                    }
                    _ => Err(Error::WrongType),
                }
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                // New key: use packed if small enough
                let max_entries = self.encoding.read().zset_max_listpack_entries;
                let max_value = self.encoding.read().zset_max_listpack_value;
                let should_pack = members.len() < max_entries
                    && members.iter().all(|(_, m)| m.len() <= max_value);

                if should_pack {
                    let mut lp = Listpack::with_capacity(members.len());
                    for (score, member) in &members {
                        lp.zinsert(member, *score);
                    }
                    let count = lp.len();
                    e.insert((key, Entry::new(DataType::SortedSetPacked(lp))));
                    self.key_count.fetch_add(1, Ordering::Relaxed);
                    Ok(count)
                } else {
                    let mut zset = SortedSetData::new();
                    for (score, member) in &members {
                        zset.insert(member.clone(), *score);
                    }
                    let count = zset.len();
                    e.insert((key, Entry::new(DataType::SortedSet(zset))));
                    self.key_count.fetch_add(1, Ordering::Relaxed);
                    Ok(count)
                }
            }
        }
    }

    /// Get score of member in sorted set
    #[inline]
    pub fn zscore(&self, key: &[u8], member: &[u8]) -> Option<f64> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return None;
                }
                match &entry_ref.1.data {
                    DataType::SortedSetPacked(lp) => lp.zscore(member),
                    DataType::SortedSet(zset) => zset.score(member),
                    _ => None,
                }
            }
            None => None,
        }
    }

    /// Get sorted set cardinality (number of members)
    #[inline]
    pub fn zcard(&self, key: &[u8]) -> usize {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return 0;
                }
                match &entry_ref.1.data {
                    DataType::SortedSetPacked(lp) => lp.len(),
                    DataType::SortedSet(zset) => zset.len(),
                    _ => 0,
                }
            }
            None => 0,
        }
    }

    /// Get rank of member in sorted set (0-indexed, ascending order)
    #[inline]
    pub fn zrank(&self, key: &[u8], member: &[u8]) -> Option<usize> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return None;
                }
                match &entry_ref.1.data {
                    DataType::SortedSetPacked(lp) => lp.zrank(member),
                    DataType::SortedSet(zset) => zset.rank(member),
                    _ => None,
                }
            }
            None => None,
        }
    }

    /// Get range of members by index
    #[inline]
    pub fn zrange(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
        with_scores: bool,
    ) -> Vec<(Bytes, Option<f64>)> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return vec![];
                }

                let (len, sorted): (i64, Vec<(Bytes, f64)>) = match &entry_ref.1.data {
                    DataType::SortedSetPacked(lp) => (lp.len() as i64, get_sorted_from_packed(lp)),
                    DataType::SortedSet(zset) => {
                        let len = zset.len() as i64;
                        if len == 0 {
                            return vec![];
                        }
                        let start_idx = normalize_index(start, len);
                        let stop_idx = normalize_index(stop, len);
                        if start_idx > stop_idx || start_idx >= len {
                            return vec![];
                        }
                        let start_idx = start_idx.max(0) as usize;
                        let stop_idx = (stop_idx + 1).min(len) as usize;

                        return zset
                            .by_score
                            .iter()
                            .skip(start_idx)
                            .take(stop_idx - start_idx)
                            .map(|((score, member), _)| {
                                let score_opt = if with_scores { Some(score.0) } else { None };
                                (member.clone(), score_opt)
                            })
                            .collect();
                    }
                    _ => return vec![],
                };

                if len == 0 {
                    return vec![];
                }

                let start_idx = normalize_index(start, len);
                let stop_idx = normalize_index(stop, len);

                if start_idx > stop_idx || start_idx >= len {
                    return vec![];
                }

                let start_idx = start_idx.max(0) as usize;
                let stop_idx = (stop_idx + 1).min(len) as usize;

                sorted
                    .into_iter()
                    .skip(start_idx)
                    .take(stop_idx - start_idx)
                    .map(|(member, score)| {
                        let score_opt = if with_scores { Some(score) } else { None };
                        (member, score_opt)
                    })
                    .collect()
            }
            None => vec![],
        }
    }

    /// Remove members from sorted set. Returns number removed.
    #[inline]
    pub fn zrem(&self, key: &[u8], members: &[Bytes]) -> usize {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return 0;
                }

                let (removed, is_empty) = match &mut entry.data {
                    DataType::SortedSetPacked(lp) => {
                        let mut removed = 0;
                        for member in members {
                            if lp.zremove(member).is_some() {
                                removed += 1;
                            }
                        }
                        (removed, lp.is_empty())
                    }
                    DataType::SortedSet(zset) => {
                        let mut removed = 0;
                        for member in members {
                            if zset.remove(member) {
                                removed += 1;
                            }
                        }
                        (removed, zset.is_empty())
                    }
                    _ => (0, false),
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

    /// Increment score of member by delta. Returns new score.
    #[inline]
    pub fn zincrby(&self, key: Bytes, member: Bytes, delta: f64) -> Result<f64> {
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    // Create new packed
                    let max_value = self.encoding.read().zset_max_listpack_value;
                    if member.len() <= max_value {
                        let mut lp = Listpack::new();
                        lp.zinsert(&member, delta);
                        entry.data = DataType::SortedSetPacked(lp);
                    } else {
                        let mut zset = SortedSetData::new();
                        zset.insert(member, delta);
                        entry.data = DataType::SortedSet(zset);
                    }
                    entry.persist();
                    entry.bump_version();
                    return Ok(delta);
                }

                let result = match &mut entry.data {
                    DataType::SortedSetPacked(lp) => {
                        let current = lp.zscore(&member).unwrap_or(0.0);
                        let new_score = current + delta;
                        if !new_score.is_finite() {
                            return Err(Error::NotFloat);
                        }

                        // Check if we need to upgrade
                        let max_value = self.encoding.read().zset_max_listpack_value;
                        let max_entries = self.encoding.read().zset_max_listpack_entries;
                        if member.len() > max_value
                            || (!lp.contains_key(&member) && lp.len() >= max_entries)
                        {
                            let mut zset = upgrade_zset_packed_to_full(lp);
                            zset.insert(member, new_score);
                            entry.data = DataType::SortedSet(zset);
                        } else {
                            lp.zinsert(&member, new_score);
                        }
                        Ok(new_score)
                    }
                    DataType::SortedSet(zset) => {
                        let new_score = zset.scores.get(&member).copied().unwrap_or(0.0) + delta;
                        if !new_score.is_finite() {
                            return Err(Error::NotFloat);
                        }
                        zset.insert(member, new_score);
                        Ok(new_score)
                    }
                    _ => Err(Error::WrongType),
                };
                if result.is_ok() {
                    entry.bump_version();
                }
                result
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                let max_value = self.encoding.read().zset_max_listpack_value;
                if member.len() <= max_value {
                    let mut lp = Listpack::new();
                    lp.zinsert(&member, delta);
                    e.insert((key, Entry::new(DataType::SortedSetPacked(lp))));
                } else {
                    let mut zset = SortedSetData::new();
                    zset.insert(member, delta);
                    e.insert((key, Entry::new(DataType::SortedSet(zset))));
                }
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(delta)
            }
        }
    }

    /// Count members with scores in [min, max]
    #[inline]
    pub fn zcount(&self, key: &[u8], min: f64, max: f64) -> usize {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return 0;
                }
                match &entry_ref.1.data {
                    DataType::SortedSetPacked(lp) => lp
                        .ziter()
                        .filter(|(_, score)| *score >= min && *score <= max)
                        .count(),
                    DataType::SortedSet(zset) => zset
                        .by_score
                        .iter()
                        .filter(|((score, _), _)| score.0 >= min && score.0 <= max)
                        .count(),
                    _ => 0,
                }
            }
            None => 0,
        }
    }

    /// Get reverse rank (0-indexed position by score descending)
    #[inline]
    pub fn zrevrank(&self, key: &[u8], member: &[u8]) -> Option<usize> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return None;
                }
                match &entry_ref.1.data {
                    DataType::SortedSetPacked(lp) => lp.zrank(member).map(|r| lp.len() - 1 - r),
                    DataType::SortedSet(zset) => zset.rev_rank(member),
                    _ => None,
                }
            }
            None => None,
        }
    }

    /// Get range of members in reverse order (high to low score)
    #[inline]
    pub fn zrevrange(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
        with_scores: bool,
    ) -> Vec<(Bytes, Option<f64>)> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return vec![];
                }

                let (len, sorted): (i64, Vec<(Bytes, f64)>) = match &entry_ref.1.data {
                    DataType::SortedSetPacked(lp) => {
                        let mut s = get_sorted_from_packed(lp);
                        s.reverse();
                        (lp.len() as i64, s)
                    }
                    DataType::SortedSet(zset) => {
                        let len = zset.len() as i64;
                        if len == 0 {
                            return vec![];
                        }
                        let start_idx = normalize_index(start, len);
                        let stop_idx = normalize_index(stop, len);
                        if start_idx > stop_idx || start_idx >= len {
                            return vec![];
                        }
                        let start_idx = start_idx.max(0) as usize;
                        let stop_idx = (stop_idx + 1).min(len) as usize;

                        return zset
                            .by_score
                            .iter()
                            .rev()
                            .skip(start_idx)
                            .take(stop_idx - start_idx)
                            .map(|((score, member), _)| {
                                let score_opt = if with_scores { Some(score.0) } else { None };
                                (member.clone(), score_opt)
                            })
                            .collect();
                    }
                    _ => return vec![],
                };

                if len == 0 {
                    return vec![];
                }

                let start_idx = normalize_index(start, len);
                let stop_idx = normalize_index(stop, len);

                if start_idx > stop_idx || start_idx >= len {
                    return vec![];
                }

                let start_idx = start_idx.max(0) as usize;
                let stop_idx = (stop_idx + 1).min(len) as usize;

                sorted
                    .into_iter()
                    .skip(start_idx)
                    .take(stop_idx - start_idx)
                    .map(|(member, score)| {
                        let score_opt = if with_scores { Some(score) } else { None };
                        (member, score_opt)
                    })
                    .collect()
            }
            None => vec![],
        }
    }

    /// Get members with scores in [min, max], optionally with LIMIT offset count
    #[inline]
    pub fn zrangebyscore(
        &self,
        key: &[u8],
        min: f64,
        max: f64,
        with_scores: bool,
        offset: usize,
        count: usize,
    ) -> Vec<(Bytes, Option<f64>)> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return vec![];
                }
                match &entry_ref.1.data {
                    DataType::SortedSetPacked(lp) => {
                        let sorted = get_sorted_from_packed(lp);
                        let iter = sorted
                            .into_iter()
                            .filter(|(_, score)| *score >= min && *score <= max)
                            .skip(offset);

                        let result: Vec<_> = if count > 0 {
                            iter.take(count).collect()
                        } else {
                            iter.collect()
                        };

                        result
                            .into_iter()
                            .map(|(member, score)| {
                                let score_opt = if with_scores { Some(score) } else { None };
                                (member, score_opt)
                            })
                            .collect()
                    }
                    DataType::SortedSet(zset) => {
                        let iter = zset
                            .by_score
                            .iter()
                            .filter(|((score, _), _)| score.0 >= min && score.0 <= max)
                            .skip(offset);

                        let iter: Box<dyn Iterator<Item = _>> = if count > 0 {
                            Box::new(iter.take(count))
                        } else {
                            Box::new(iter)
                        };

                        iter.map(|((score, member), _)| {
                            let score_opt = if with_scores { Some(score.0) } else { None };
                            (member.clone(), score_opt)
                        })
                        .collect()
                    }
                    _ => vec![],
                }
            }
            None => vec![],
        }
    }

    /// Get members with scores in [max, min] in reverse, optionally with LIMIT
    #[inline]
    pub fn zrevrangebyscore(
        &self,
        key: &[u8],
        max: f64,
        min: f64,
        with_scores: bool,
        offset: usize,
        count: usize,
    ) -> Vec<(Bytes, Option<f64>)> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return vec![];
                }
                match &entry_ref.1.data {
                    DataType::SortedSetPacked(lp) => {
                        let mut sorted = get_sorted_from_packed(lp);
                        sorted.reverse();
                        let iter = sorted
                            .into_iter()
                            .filter(|(_, score)| *score >= min && *score <= max)
                            .skip(offset);

                        let result: Vec<_> = if count > 0 {
                            iter.take(count).collect()
                        } else {
                            iter.collect()
                        };

                        result
                            .into_iter()
                            .map(|(member, score)| {
                                let score_opt = if with_scores { Some(score) } else { None };
                                (member, score_opt)
                            })
                            .collect()
                    }
                    DataType::SortedSet(zset) => {
                        let iter = zset
                            .by_score
                            .iter()
                            .rev()
                            .filter(|((score, _), _)| score.0 >= min && score.0 <= max)
                            .skip(offset);

                        let iter: Box<dyn Iterator<Item = _>> = if count > 0 {
                            Box::new(iter.take(count))
                        } else {
                            Box::new(iter)
                        };

                        iter.map(|((score, member), _)| {
                            let score_opt = if with_scores { Some(score.0) } else { None };
                            (member.clone(), score_opt)
                        })
                        .collect()
                    }
                    _ => vec![],
                }
            }
            None => vec![],
        }
    }

    /// Pop members with lowest scores
    #[inline]
    pub fn zpopmin(&self, key: &[u8], count: usize) -> Vec<(Bytes, f64)> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return vec![];
                }

                let (result, is_empty) = match &mut entry.data {
                    DataType::SortedSetPacked(lp) => {
                        let sorted = get_sorted_from_packed(lp);
                        let to_pop: Vec<_> = sorted.into_iter().take(count).collect();
                        let result: Vec<(Bytes, f64)> =
                            to_pop.iter().map(|(m, s)| (m.clone(), *s)).collect();
                        for (member, _) in &to_pop {
                            lp.zremove(member);
                        }
                        (result, lp.is_empty())
                    }
                    DataType::SortedSet(zset) => {
                        let mut result = Vec::with_capacity(count.min(zset.len()));
                        for _ in 0..count {
                            if let Some(((score, member), _)) = zset.by_score.iter().next() {
                                let member_clone = member.clone();
                                let score_val = score.0;
                                zset.remove(&member_clone);
                                result.push((member_clone, score_val));
                            } else {
                                break;
                            }
                        }
                        (result, zset.is_empty())
                    }
                    _ => return vec![],
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

    /// Pop members with highest scores
    #[inline]
    pub fn zpopmax(&self, key: &[u8], count: usize) -> Vec<(Bytes, f64)> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return vec![];
                }

                let (result, is_empty) = match &mut entry.data {
                    DataType::SortedSetPacked(lp) => {
                        let mut sorted = get_sorted_from_packed(lp);
                        sorted.reverse();
                        let to_pop: Vec<_> = sorted.into_iter().take(count).collect();
                        let result: Vec<(Bytes, f64)> =
                            to_pop.iter().map(|(m, s)| (m.clone(), *s)).collect();
                        for (member, _) in &to_pop {
                            lp.zremove(member);
                        }
                        (result, lp.is_empty())
                    }
                    DataType::SortedSet(zset) => {
                        let mut result = Vec::with_capacity(count.min(zset.len()));
                        for _ in 0..count {
                            if let Some(((score, member), _)) = zset.by_score.iter().next_back() {
                                let member_clone = member.clone();
                                let score_val = score.0;
                                zset.remove(&member_clone);
                                result.push((member_clone, score_val));
                            } else {
                                break;
                            }
                        }
                        (result, zset.is_empty())
                    }
                    _ => return vec![],
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

    /// Get scores for multiple members
    #[inline]
    pub fn zmscore(&self, key: &[u8], members: &[Bytes]) -> Vec<Option<f64>> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return members.iter().map(|_| None).collect();
                }
                match &entry_ref.1.data {
                    DataType::SortedSetPacked(lp) => members.iter().map(|m| lp.zscore(m)).collect(),
                    DataType::SortedSet(zset) => members.iter().map(|m| zset.score(m)).collect(),
                    _ => members.iter().map(|_| None).collect(),
                }
            }
            None => members.iter().map(|_| None).collect(),
        }
    }

    // ==================== Set Operations (ZDIFF, ZINTER, ZUNION) ====================

    /// ZDIFF - Return difference of first set minus all others
    pub fn zdiff(&self, keys: &[Bytes], with_scores: bool) -> Vec<(Bytes, Option<f64>)> {
        if keys.is_empty() {
            return vec![];
        }

        let first = match self.data_get(&keys[0]) {
            Some(entry_ref) if !entry_ref.1.is_expired() => match &entry_ref.1.data {
                DataType::SortedSetPacked(lp) => get_scores_from_packed(lp),
                DataType::SortedSet(ss) => ss.scores.clone(),
                _ => return vec![],
            },
            _ => return vec![],
        };

        let mut result = first;
        for key in &keys[1..] {
            if let Some(entry_ref) = self.data_get(key.as_ref())
                && !entry_ref.1.is_expired()
            {
                let other_members: Vec<Bytes> = match &entry_ref.1.data {
                    DataType::SortedSetPacked(lp) => lp.ziter().map(|(m, _)| m).collect(),
                    DataType::SortedSet(ss) => ss.scores.keys().cloned().collect(),
                    _ => continue,
                };
                for member in other_members {
                    result.remove(&member);
                }
            }
        }

        result
            .into_iter()
            .map(|(m, s)| (m, if with_scores { Some(s) } else { None }))
            .collect()
    }

    /// ZINTER - Return intersection of all sets
    pub fn zinter(
        &self,
        keys: &[Bytes],
        weights: Option<&[f64]>,
        aggregate: &str,
        with_scores: bool,
    ) -> Vec<(Bytes, Option<f64>)> {
        if keys.is_empty() {
            return vec![];
        }

        let first = match self.data_get(&keys[0]) {
            Some(entry_ref) if !entry_ref.1.is_expired() => match &entry_ref.1.data {
                DataType::SortedSetPacked(lp) => get_scores_from_packed(lp),
                DataType::SortedSet(ss) => ss.scores.clone(),
                _ => return vec![],
            },
            _ => return vec![],
        };

        let w0 = weights
            .map(|w| w.first().copied().unwrap_or(1.0))
            .unwrap_or(1.0);

        let mut result: Vec<(Bytes, f64)> = vec![];
        'outer: for (member, score) in first {
            let mut final_score = score * w0;

            for (i, key) in keys[1..].iter().enumerate() {
                if let Some(entry_ref) = self.data_get(key.as_ref()) {
                    if !entry_ref.1.is_expired() {
                        let other_score = match &entry_ref.1.data {
                            DataType::SortedSetPacked(lp) => lp.zscore(&member),
                            DataType::SortedSet(ss) => ss.scores.get(&member).copied(),
                            _ => continue 'outer,
                        };

                        if let Some(s) = other_score {
                            let w = weights
                                .map(|ws| ws.get(i + 1).copied().unwrap_or(1.0))
                                .unwrap_or(1.0);
                            let weighted = s * w;
                            final_score = match aggregate.to_uppercase().as_str() {
                                "MIN" => final_score.min(weighted),
                                "MAX" => final_score.max(weighted),
                                _ => final_score + weighted, // SUM
                            };
                        } else {
                            continue 'outer;
                        }
                    } else {
                        continue 'outer;
                    }
                } else {
                    continue 'outer;
                }
            }

            result.push((member, final_score));
        }

        result
            .into_iter()
            .map(|(m, s)| (m, if with_scores { Some(s) } else { None }))
            .collect()
    }

    /// ZINTERCARD - Count intersection members (with optional limit)
    pub fn zintercard(&self, keys: &[Bytes], limit: Option<usize>) -> usize {
        if keys.is_empty() {
            return 0;
        }

        let first: Vec<Bytes> = match self.data_get(&keys[0]) {
            Some(entry_ref) if !entry_ref.1.is_expired() => match &entry_ref.1.data {
                DataType::SortedSetPacked(lp) => lp.ziter().map(|(m, _)| m).collect(),
                DataType::SortedSet(ss) => ss.scores.keys().cloned().collect(),
                _ => return 0,
            },
            _ => return 0,
        };

        let mut count = 0usize;
        let limit_val = limit.unwrap_or(usize::MAX);

        'outer: for member in first {
            for key in &keys[1..] {
                if let Some(entry_ref) = self.data_get(key.as_ref()) {
                    if !entry_ref.1.is_expired() {
                        let contains = match &entry_ref.1.data {
                            DataType::SortedSetPacked(lp) => lp.contains_key(&member),
                            DataType::SortedSet(ss) => ss.scores.contains_key(&member),
                            _ => continue 'outer,
                        };
                        if !contains {
                            continue 'outer;
                        }
                    } else {
                        continue 'outer;
                    }
                } else {
                    continue 'outer;
                }
            }
            count += 1;
            if count >= limit_val {
                break;
            }
        }

        count
    }

    /// ZUNION - Return union of all sets
    pub fn zunion(
        &self,
        keys: &[Bytes],
        weights: Option<&[f64]>,
        aggregate: &str,
        with_scores: bool,
    ) -> Vec<(Bytes, Option<f64>)> {
        use std::collections::HashMap;
        let mut result: HashMap<Bytes, f64> = HashMap::new();

        for (i, key) in keys.iter().enumerate() {
            if let Some(entry_ref) = self.data_get(key.as_ref())
                && !entry_ref.1.is_expired()
            {
                let scores: Vec<(Bytes, f64)> = match &entry_ref.1.data {
                    DataType::SortedSetPacked(lp) => lp.ziter().collect(),
                    DataType::SortedSet(ss) => {
                        ss.scores.iter().map(|(m, &s)| (m.clone(), s)).collect()
                    }
                    _ => continue,
                };

                let w = weights
                    .map(|ws| ws.get(i).copied().unwrap_or(1.0))
                    .unwrap_or(1.0);
                for (member, score) in scores {
                    let weighted = score * w;
                    result
                        .entry(member)
                        .and_modify(|existing| {
                            *existing = match aggregate.to_uppercase().as_str() {
                                "MIN" => existing.min(weighted),
                                "MAX" => existing.max(weighted),
                                _ => *existing + weighted, // SUM
                            };
                        })
                        .or_insert(weighted);
                }
            }
        }

        result
            .into_iter()
            .map(|(m, s)| (m, if with_scores { Some(s) } else { None }))
            .collect()
    }

    // ==================== Lexicographic Operations ====================

    /// ZLEXCOUNT - Count members in lexicographic range
    pub fn zlexcount(&self, key: &[u8], min: &[u8], max: &[u8]) -> usize {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return 0;
                }
                match &entry_ref.1.data {
                    DataType::SortedSetPacked(lp) => lp
                        .ziter()
                        .filter(|(m, _)| lex_in_range(m, min, max))
                        .count(),
                    DataType::SortedSet(ss) => ss
                        .scores
                        .keys()
                        .filter(|m| lex_in_range(m, min, max))
                        .count(),
                    _ => 0,
                }
            }
            None => 0,
        }
    }

    /// ZRANGEBYLEX - Get members in lexicographic range
    pub fn zrangebylex(
        &self,
        key: &[u8],
        min: &[u8],
        max: &[u8],
        offset: usize,
        count: usize,
    ) -> Vec<Bytes> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return vec![];
                }
                match &entry_ref.1.data {
                    DataType::SortedSetPacked(lp) => {
                        let mut members: Vec<_> = lp
                            .ziter()
                            .map(|(m, _)| m)
                            .filter(|m| lex_in_range(m, min, max))
                            .collect();
                        members.sort();
                        members.into_iter().skip(offset).take(count).collect()
                    }
                    DataType::SortedSet(ss) => {
                        let mut members: Vec<_> = ss
                            .scores
                            .keys()
                            .filter(|m| lex_in_range(m, min, max))
                            .cloned()
                            .collect();
                        members.sort();
                        members.into_iter().skip(offset).take(count).collect()
                    }
                    _ => vec![],
                }
            }
            None => vec![],
        }
    }

    /// ZREVRANGEBYLEX - Get members in reverse lexicographic range
    pub fn zrevrangebylex(
        &self,
        key: &[u8],
        max: &[u8],
        min: &[u8],
        offset: usize,
        count: usize,
    ) -> Vec<Bytes> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return vec![];
                }
                match &entry_ref.1.data {
                    DataType::SortedSetPacked(lp) => {
                        let mut members: Vec<_> = lp
                            .ziter()
                            .map(|(m, _)| m)
                            .filter(|m| lex_in_range(m, min, max))
                            .collect();
                        members.sort();
                        members.reverse();
                        members.into_iter().skip(offset).take(count).collect()
                    }
                    DataType::SortedSet(ss) => {
                        let mut members: Vec<_> = ss
                            .scores
                            .keys()
                            .filter(|m| lex_in_range(m, min, max))
                            .cloned()
                            .collect();
                        members.sort();
                        members.reverse();
                        members.into_iter().skip(offset).take(count).collect()
                    }
                    _ => vec![],
                }
            }
            None => vec![],
        }
    }

    /// ZREMRANGEBYLEX - Remove members in lexicographic range
    pub fn zremrangebylex(&self, key: &[u8], min: &[u8], max: &[u8]) -> usize {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return 0;
                }

                let (removed, is_empty) = match &mut entry.data {
                    DataType::SortedSetPacked(lp) => {
                        let to_remove: Vec<_> = lp
                            .ziter()
                            .filter(|(m, _)| lex_in_range(m, min, max))
                            .map(|(m, _)| m)
                            .collect();
                        let count = to_remove.len();
                        for m in to_remove {
                            lp.zremove(&m);
                        }
                        (count, lp.is_empty())
                    }
                    DataType::SortedSet(ss) => {
                        let to_remove: Vec<_> = ss
                            .scores
                            .keys()
                            .filter(|m| lex_in_range(m, min, max))
                            .cloned()
                            .collect();
                        let count = to_remove.len();
                        for m in to_remove {
                            ss.remove(&m);
                        }
                        (count, ss.is_empty())
                    }
                    _ => (0, false),
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

    /// ZREMRANGEBYRANK - Remove members in rank range
    pub fn zremrangebyrank(&self, key: &[u8], start: i64, stop: i64) -> usize {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return 0;
                }

                let (removed, is_empty) = match &mut entry.data {
                    DataType::SortedSetPacked(lp) => {
                        let len = lp.len() as i64;
                        let start_idx = normalize_index(start, len);
                        let stop_idx = normalize_index(stop, len);

                        if start_idx > stop_idx || start_idx >= len {
                            (0, lp.is_empty())
                        } else {
                            let start_idx = start_idx.max(0) as usize;
                            let stop_idx = (stop_idx + 1).min(len) as usize;

                            let sorted = get_sorted_from_packed(lp);
                            let to_remove: Vec<_> = sorted
                                .into_iter()
                                .skip(start_idx)
                                .take(stop_idx - start_idx)
                                .map(|(m, _)| m)
                                .collect();

                            let count = to_remove.len();
                            for m in to_remove {
                                lp.zremove(&m);
                            }
                            (count, lp.is_empty())
                        }
                    }
                    DataType::SortedSet(ss) => {
                        let len = ss.len() as i64;
                        let start_idx = normalize_index(start, len);
                        let stop_idx = normalize_index(stop, len);

                        if start_idx > stop_idx || start_idx >= len {
                            (0, ss.is_empty())
                        } else {
                            let start_idx = start_idx.max(0) as usize;
                            let stop_idx = (stop_idx + 1).min(len) as usize;

                            let to_remove: Vec<_> = ss
                                .by_score
                                .iter()
                                .skip(start_idx)
                                .take(stop_idx - start_idx)
                                .map(|((_, m), _)| m.clone())
                                .collect();

                            let count = to_remove.len();
                            for m in to_remove {
                                ss.remove(&m);
                            }
                            (count, ss.is_empty())
                        }
                    }
                    _ => (0, false),
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

    /// ZREMRANGEBYSCORE - Remove members in score range
    pub fn zremrangebyscore(&self, key: &[u8], min: f64, max: f64) -> usize {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return 0;
                }

                let (removed, is_empty) = match &mut entry.data {
                    DataType::SortedSetPacked(lp) => {
                        let to_remove: Vec<_> = lp
                            .ziter()
                            .filter(|(_, score)| *score >= min && *score <= max)
                            .map(|(m, _)| m)
                            .collect();

                        let count = to_remove.len();
                        for m in to_remove {
                            lp.zremove(&m);
                        }
                        (count, lp.is_empty())
                    }
                    DataType::SortedSet(ss) => {
                        let to_remove: Vec<_> = ss
                            .by_score
                            .iter()
                            .filter(|((score, _), _)| score.0 >= min && score.0 <= max)
                            .map(|((_, m), _)| m.clone())
                            .collect();

                        let count = to_remove.len();
                        for m in to_remove {
                            ss.remove(&m);
                        }
                        (count, ss.is_empty())
                    }
                    _ => (0, false),
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

    /// ZSCAN key cursor [MATCH pattern] [COUNT count]
    /// Incrementally iterate sorted set elements
    pub fn zscan(
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
                    DataType::SortedSet(ss) => {
                        // Collect all members with scores and paginate
                        // by_score stores ((OrderedFloat(score), member), ())
                        let all_members: Vec<(Bytes, f64)> = ss
                            .by_score
                            .iter()
                            .map(|((score, member), _)| (member.clone(), score.0))
                            .collect();

                        let total = all_members.len();
                        let start = cursor as usize;
                        if start >= total {
                            return Ok((0, vec![]));
                        }

                        let end = (start + count).min(total);
                        let mut result: Vec<Bytes> = Vec::new();

                        for (member, score) in &all_members[start..end] {
                            let matches =
                                pattern.map_or(true, |p| crate::storage::match_pattern(p, member));
                            if matches {
                                result.push(member.clone());
                                result.push(Bytes::from(score.to_string()));
                            }
                        }

                        let next_cursor = if end >= total { 0 } else { end as u64 };
                        Ok((next_cursor, result))
                    }
                    DataType::SortedSetPacked(lp) => {
                        // Collect all members with scores using ziter for proper decoding
                        let all_members: Vec<(Bytes, f64)> = lp.ziter().collect();

                        let total = all_members.len();
                        let start = cursor as usize;
                        if start >= total {
                            return Ok((0, vec![]));
                        }

                        let end = (start + count).min(total);
                        let mut result: Vec<Bytes> = Vec::new();

                        for (member, score) in &all_members[start..end] {
                            let matches =
                                pattern.map_or(true, |p| crate::storage::match_pattern(p, member));
                            if matches {
                                result.push(member.clone());
                                result.push(Bytes::from(score.to_string()));
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

/// Helper for lexicographic range matching
fn lex_in_range(member: &[u8], min: &[u8], max: &[u8]) -> bool {
    fn check_bound(m: &[u8], bound: &[u8], is_min: bool) -> bool {
        if bound.is_empty() {
            return true;
        }

        match bound[0] {
            b'[' => {
                let val = &bound[1..];
                if is_min { m >= val } else { m <= val }
            }
            b'(' => {
                let val = &bound[1..];
                if is_min { m > val } else { m < val }
            }
            b'-' => is_min,  // -inf is Always less than m
            b'+' => !is_min, // +inf is Always more than m
            _ => {
                if is_min {
                    m >= bound
                } else {
                    m <= bound
                }
            }
        }
    }

    check_bound(member, min, true) && check_bound(member, max, false)
}

/// Normalize negative indices for sorted set operations
fn normalize_index(idx: i64, len: i64) -> i64 {
    if idx < 0 { len + idx } else { idx }
}
