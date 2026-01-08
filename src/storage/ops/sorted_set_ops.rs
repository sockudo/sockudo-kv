use bytes::Bytes;
use dashmap::mapref::entry::Entry as DashEntry;

use crate::error::{Error, Result};
use crate::storage::Store;
use crate::storage::types::{DataType, Entry, SortedSetData};

use std::sync::atomic::Ordering;

/// Sorted set operations for the Store
impl Store {
    // ==================== Sorted Set operations ====================

    /// Add members with scores to sorted set
    #[inline]
    pub fn zadd(&self, key: Bytes, members: Vec<(f64, Bytes)>) -> Result<usize> {
        match self.data.entry(key) {
            DashEntry::Occupied(mut e) => {
                let entry = e.get_mut();
                if entry.is_expired() {
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

                let result = match &mut entry.data {
                    DataType::SortedSet(zset) => {
                        let mut added = 0;
                        for (score, member) in &members {
                            if zset.insert(member.clone(), *score) {
                                added += 1;
                            }
                        }
                        Ok(added)
                    }
                    _ => Err(Error::WrongType),
                };
                if result.is_ok() {
                    entry.bump_version();
                }
                result
            }
            DashEntry::Vacant(e) => {
                let mut zset = SortedSetData::new();
                for (score, member) in &members {
                    zset.insert(member.clone(), *score);
                }
                let count = zset.len();
                e.insert(Entry::new(DataType::SortedSet(zset)));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(count)
            }
        }
    }

    /// Get score of member in sorted set
    #[inline]
    pub fn zscore(&self, key: &[u8], member: &[u8]) -> Option<f64> {
        match self.data.get(key) {
            Some(e) => {
                if e.is_expired() {
                    return None;
                }
                match e.data.as_sorted_set() {
                    Some(zset) => zset.score(member),
                    None => None,
                }
            }
            None => None,
        }
    }

    /// Get sorted set cardinality (number of members)
    #[inline]
    pub fn zcard(&self, key: &[u8]) -> usize {
        match self.data.get(key) {
            Some(e) => {
                if e.is_expired() {
                    return 0;
                }
                match e.data.as_sorted_set() {
                    Some(zset) => zset.len(),
                    None => 0,
                }
            }
            None => 0,
        }
    }

    /// Get rank of member in sorted set (0-indexed, ascending order)
    #[inline]
    pub fn zrank(&self, key: &[u8], member: &[u8]) -> Option<usize> {
        match self.data.get(key) {
            Some(e) => {
                if e.is_expired() {
                    return None;
                }
                match e.data.as_sorted_set() {
                    Some(zset) => zset.rank(member),
                    None => None,
                }
            }
            None => None,
        }
    }

    /// Get range of members by score
    #[inline]
    pub fn zrange(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
        with_scores: bool,
    ) -> Vec<(Bytes, Option<f64>)> {
        match self.data.get(key) {
            Some(e) => {
                if e.is_expired() {
                    return vec![];
                }
                match e.data.as_sorted_set() {
                    Some(zset) => {
                        let len = zset.len() as i64;
                        if len == 0 {
                            return vec![];
                        }

                        let start = normalize_index(start, len);
                        let stop = normalize_index(stop, len);

                        if start > stop || start >= len {
                            return vec![];
                        }

                        let start = start.max(0) as usize;
                        let stop = (stop + 1).min(len) as usize;

                        zset.by_score
                            .iter()
                            .skip(start)
                            .take(stop - start)
                            .map(|(score, member)| {
                                let score_opt = if with_scores { Some(score.0) } else { None };
                                (member.clone(), score_opt)
                            })
                            .collect()
                    }
                    None => vec![],
                }
            }
            None => vec![],
        }
    }

    /// Remove members from sorted set. Returns number removed.
    #[inline]
    pub fn zrem(&self, key: &[u8], members: &[Bytes]) -> usize {
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
            let zset = match entry.data.as_sorted_set_mut() {
                Some(z) => z,
                None => return 0,
            };

            let mut removed = 0;
            for member in members {
                if zset.remove(member) {
                    removed += 1;
                }
            }
            (removed, zset.is_empty())
        };

        if removed > 0 {
            entry.bump_version();
        }

        if is_empty {
            drop(entry);
            self.del(key);
        }

        removed
    }

    /// Increment score of member by delta. Returns new score.
    #[inline]
    pub fn zincrby(&self, key: Bytes, member: Bytes, delta: f64) -> Result<f64> {
        match self.data.entry(key) {
            DashEntry::Occupied(mut e) => {
                let entry = e.get_mut();
                if entry.is_expired() {
                    entry.data = DataType::SortedSet(SortedSetData::new());
                    entry.persist();
                }

                let result = match &mut entry.data {
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
            DashEntry::Vacant(e) => {
                let mut zset = SortedSetData::new();
                zset.insert(member, delta);
                e.insert(Entry::new(DataType::SortedSet(zset)));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(delta)
            }
        }
    }

    /// Count members with scores in [min, max]
    #[inline]
    pub fn zcount(&self, key: &[u8], min: f64, max: f64) -> usize {
        match self.data.get(key) {
            Some(e) => {
                if e.is_expired() {
                    return 0;
                }
                match e.data.as_sorted_set() {
                    Some(zset) => zset
                        .by_score
                        .iter()
                        .filter(|(score, _)| score.0 >= min && score.0 <= max)
                        .count(),
                    None => 0,
                }
            }
            None => 0,
        }
    }

    /// Get reverse rank (0-indexed position by score descending)
    #[inline]
    pub fn zrevrank(&self, key: &[u8], member: &[u8]) -> Option<usize> {
        match self.data.get(key) {
            Some(e) => {
                if e.is_expired() {
                    return None;
                }
                match e.data.as_sorted_set() {
                    Some(zset) => zset.rev_rank(member),
                    None => None,
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
        match self.data.get(key) {
            Some(e) => {
                if e.is_expired() {
                    return vec![];
                }
                match e.data.as_sorted_set() {
                    Some(zset) => {
                        let len = zset.len() as i64;
                        if len == 0 {
                            return vec![];
                        }

                        let start = normalize_index(start, len);
                        let stop = normalize_index(stop, len);

                        if start > stop || start >= len {
                            return vec![];
                        }

                        let start = start.max(0) as usize;
                        let stop = (stop + 1).min(len) as usize;

                        // Collect in reverse order
                        zset.by_score
                            .iter()
                            .rev()
                            .skip(start)
                            .take(stop - start)
                            .map(|(score, member)| {
                                let score_opt = if with_scores { Some(score.0) } else { None };
                                (member.clone(), score_opt)
                            })
                            .collect()
                    }
                    None => vec![],
                }
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
        match self.data.get(key) {
            Some(e) => {
                if e.is_expired() {
                    return vec![];
                }
                match e.data.as_sorted_set() {
                    Some(zset) => {
                        let iter = zset
                            .by_score
                            .iter()
                            .filter(|(score, _)| score.0 >= min && score.0 <= max)
                            .skip(offset);

                        let iter: Box<dyn Iterator<Item = _>> = if count > 0 {
                            Box::new(iter.take(count))
                        } else {
                            Box::new(iter)
                        };

                        iter.map(|(score, member)| {
                            let score_opt = if with_scores { Some(score.0) } else { None };
                            (member.clone(), score_opt)
                        })
                        .collect()
                    }
                    None => vec![],
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
        match self.data.get(key) {
            Some(e) => {
                if e.is_expired() {
                    return vec![];
                }
                match e.data.as_sorted_set() {
                    Some(zset) => {
                        let iter = zset
                            .by_score
                            .iter()
                            .rev()
                            .filter(|(score, _)| score.0 >= min && score.0 <= max)
                            .skip(offset);

                        let iter: Box<dyn Iterator<Item = _>> = if count > 0 {
                            Box::new(iter.take(count))
                        } else {
                            Box::new(iter)
                        };

                        iter.map(|(score, member)| {
                            let score_opt = if with_scores { Some(score.0) } else { None };
                            (member.clone(), score_opt)
                        })
                        .collect()
                    }
                    None => vec![],
                }
            }
            None => vec![],
        }
    }

    /// Pop members with lowest scores
    #[inline]
    pub fn zpopmin(&self, key: &[u8], count: usize) -> Vec<(Bytes, f64)> {
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
            let zset = match entry.data.as_sorted_set_mut() {
                Some(z) => z,
                None => return vec![],
            };

            let mut result = Vec::with_capacity(count.min(zset.len()));

            for _ in 0..count {
                if let Some((score, member)) = zset.by_score.iter().next().cloned() {
                    zset.remove(&member);
                    result.push((member, score.0));
                } else {
                    break;
                }
            }
            (result, zset.is_empty())
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

    /// Pop members with highest scores
    #[inline]
    pub fn zpopmax(&self, key: &[u8], count: usize) -> Vec<(Bytes, f64)> {
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
            let zset = match entry.data.as_sorted_set_mut() {
                Some(z) => z,
                None => return vec![],
            };

            let mut result = Vec::with_capacity(count.min(zset.len()));

            for _ in 0..count {
                if let Some((score, member)) = zset.by_score.iter().next_back().cloned() {
                    zset.remove(&member);
                    result.push((member, score.0));
                } else {
                    break;
                }
            }
            (result, zset.is_empty())
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

    /// Get scores for multiple members
    #[inline]
    pub fn zmscore(&self, key: &[u8], members: &[Bytes]) -> Vec<Option<f64>> {
        match self.data.get(key) {
            Some(e) => {
                if e.is_expired() {
                    return members.iter().map(|_| None).collect();
                }
                match e.data.as_sorted_set() {
                    Some(zset) => members.iter().map(|m| zset.score(m)).collect(),
                    None => members.iter().map(|_| None).collect(),
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

        // Get first set as base
        let first = match self.data.get(&keys[0]) {
            Some(e) if !e.is_expired() => {
                if let Some(ss) = e.data.as_sorted_set() {
                    ss.scores.clone()
                } else {
                    return vec![];
                }
            }
            _ => return vec![],
        };

        // Remove members that exist in other sets
        let mut result = first;
        for key in &keys[1..] {
            if let Some(e) = self.data.get(key)
                && !e.is_expired()
                    && let Some(ss) = e.data.as_sorted_set() {
                        for member in ss.scores.keys() {
                            result.remove(member);
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

        // Get first set
        let first = match self.data.get(&keys[0]) {
            Some(e) if !e.is_expired() => {
                if let Some(ss) = e.data.as_sorted_set() {
                    ss.scores.clone()
                } else {
                    return vec![];
                }
            }
            _ => return vec![],
        };

        let w0 = weights
            .map(|w| w.first().copied().unwrap_or(1.0))
            .unwrap_or(1.0);

        // For each member in first set, check if exists in all others
        let mut result: Vec<(Bytes, f64)> = vec![];
        'outer: for (member, score) in first {
            let mut final_score = score * w0;

            for (i, key) in keys[1..].iter().enumerate() {
                if let Some(e) = self.data.get(key) {
                    if !e.is_expired() {
                        if let Some(ss) = e.data.as_sorted_set() {
                            if let Some(&s) = ss.scores.get(&member) {
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
                                continue 'outer; // Not in this set
                            }
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

        let first = match self.data.get(&keys[0]) {
            Some(e) if !e.is_expired() => {
                if let Some(ss) = e.data.as_sorted_set() {
                    ss.scores.keys().cloned().collect::<Vec<_>>()
                } else {
                    return 0;
                }
            }
            _ => return 0,
        };

        let mut count = 0usize;
        let limit = limit.unwrap_or(usize::MAX);

        'outer: for member in first {
            for key in &keys[1..] {
                if let Some(e) = self.data.get(key) {
                    if !e.is_expired() {
                        if let Some(ss) = e.data.as_sorted_set() {
                            if !ss.scores.contains_key(&member) {
                                continue 'outer;
                            }
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
            count += 1;
            if count >= limit {
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
            if let Some(e) = self.data.get(key)
                && !e.is_expired()
                    && let Some(ss) = e.data.as_sorted_set() {
                        let w = weights
                            .map(|ws| ws.get(i).copied().unwrap_or(1.0))
                            .unwrap_or(1.0);
                        for (member, &score) in &ss.scores {
                            let weighted = score * w;
                            result
                                .entry(member.clone())
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
        self.data
            .get(key)
            .and_then(|e| {
                if e.is_expired() {
                    return None;
                }
                e.data.as_sorted_set().map(|ss| {
                    ss.scores
                        .keys()
                        .filter(|m| lex_in_range(m, min, max))
                        .count()
                })
            })
            .unwrap_or(0)
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
        self.data
            .get(key)
            .and_then(|e| {
                if e.is_expired() {
                    return None;
                }
                e.data.as_sorted_set().map(|ss| {
                    let mut members: Vec<_> = ss
                        .scores
                        .keys()
                        .filter(|m| lex_in_range(m, min, max))
                        .cloned()
                        .collect();
                    members.sort();
                    members.into_iter().skip(offset).take(count).collect()
                })
            })
            .unwrap_or_default()
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
        self.data
            .get(key)
            .and_then(|e| {
                if e.is_expired() {
                    return None;
                }
                e.data.as_sorted_set().map(|ss| {
                    let mut members: Vec<_> = ss
                        .scores
                        .keys()
                        .filter(|m| lex_in_range(m, min, max))
                        .cloned()
                        .collect();
                    members.sort();
                    members.reverse();
                    members.into_iter().skip(offset).take(count).collect()
                })
            })
            .unwrap_or_default()
    }

    // ==================== Remove Range Operations ====================

    /// ZREMRANGEBYRANK - Remove members by rank range
    pub fn zremrangebyrank(&self, key: &[u8], start: i64, stop: i64) -> usize {
        match self.data.get_mut(key) {
            Some(mut e) => {
                if e.is_expired() {
                    return 0;
                }
                let entry = e.value_mut();
                let count = match &mut entry.data {
                    DataType::SortedSet(ss) => {
                        let len = ss.len() as i64;
                        let start = normalize_index(start, len).max(0) as usize;
                        let stop = (normalize_index(stop, len) + 1).min(len).max(0) as usize;

                        // Collect members to remove by rank
                        let to_remove: Vec<Bytes> = ss
                            .by_score
                            .iter()
                            .map(|(_, member)| member.clone())
                            .skip(start)
                            .take(stop.saturating_sub(start))
                            .collect();

                        let count = to_remove.len();
                        for member in to_remove {
                            ss.remove(&member);
                        }
                        count
                    }
                    _ => 0,
                };
                if count > 0 {
                    entry.bump_version();
                }
                count
            }
            None => 0,
        }
    }

    /// ZREMRANGEBYSCORE - Remove members by score range
    pub fn zremrangebyscore(&self, key: &[u8], min: f64, max: f64) -> usize {
        match self.data.get_mut(key) {
            Some(mut e) => {
                if e.is_expired() {
                    return 0;
                }
                let entry = e.value_mut();
                let count = match &mut entry.data {
                    DataType::SortedSet(ss) => {
                        let to_remove: Vec<Bytes> = ss
                            .scores
                            .iter()
                            .filter(|(_, score)| **score >= min && **score <= max)
                            .map(|(m, _)| m.clone())
                            .collect();

                        let count = to_remove.len();
                        for member in to_remove {
                            ss.remove(&member);
                        }
                        count
                    }
                    _ => 0,
                };
                if count > 0 {
                    entry.bump_version();
                }
                count
            }
            None => 0,
        }
    }

    /// ZREMRANGEBYLEX - Remove members by lexicographic range
    pub fn zremrangebylex(&self, key: &[u8], min: &[u8], max: &[u8]) -> usize {
        match self.data.get_mut(key) {
            Some(mut e) => {
                if e.is_expired() {
                    return 0;
                }
                let entry = e.value_mut();
                let count = match &mut entry.data {
                    DataType::SortedSet(ss) => {
                        let to_remove: Vec<Bytes> = ss
                            .scores
                            .keys()
                            .filter(|m| lex_in_range(m, min, max))
                            .cloned()
                            .collect();

                        let count = to_remove.len();
                        for member in to_remove {
                            ss.remove(&member);
                        }
                        count
                    }
                    _ => 0,
                };
                if count > 0 {
                    entry.bump_version();
                }
                count
            }
            None => 0,
        }
    }

    /// ZRANDMEMBER - Get random members
    pub fn zrandmember(
        &self,
        key: &[u8],
        count: i64,
        with_scores: bool,
    ) -> Vec<(Bytes, Option<f64>)> {
        self.data
            .get(key)
            .and_then(|e| {
                if e.is_expired() {
                    return None;
                }
                e.data.as_sorted_set().map(|ss| {
                    let members: Vec<_> = ss.scores.iter().map(|(m, &s)| (m.clone(), s)).collect();
                    if members.is_empty() {
                        return vec![];
                    }

                    let abs_count = count.unsigned_abs() as usize;
                    let allow_duplicates = count < 0;

                    let mut result = Vec::with_capacity(abs_count.min(members.len()));

                    if allow_duplicates {
                        // With duplicates allowed
                        for _ in 0..abs_count {
                            let idx = fastrand::usize(..members.len());
                            let (m, s) = &members[idx];
                            result.push((m.clone(), if with_scores { Some(*s) } else { None }));
                        }
                    } else {
                        // Without duplicates
                        let take = abs_count.min(members.len());
                        let mut indices: Vec<usize> = (0..members.len()).collect();
                        fastrand::shuffle(&mut indices);
                        for i in 0..take {
                            let (m, s) = &members[indices[i]];
                            result.push((m.clone(), if with_scores { Some(*s) } else { None }));
                        }
                    }

                    result
                })
            })
            .unwrap_or_default()
    }

    /// ZSCAN - Scan sorted set members
    pub fn zscan(
        &self,
        key: &[u8],
        cursor: usize,
        pattern: Option<&[u8]>,
        count: usize,
    ) -> (usize, Vec<(Bytes, f64)>) {
        self.data
            .get(key)
            .and_then(|e| {
                if e.is_expired() {
                    return None;
                }
                e.data.as_sorted_set().map(|ss| {
                    let all: Vec<_> = ss.scores.iter().map(|(m, &s)| (m.clone(), s)).collect();
                    let len = all.len();

                    if cursor >= len {
                        return (0, vec![]);
                    }

                    let mut result = Vec::new();
                    let mut i = cursor;

                    while result.len() < count && i < len {
                        let (member, score) = &all[i];
                        let matches = pattern.map(|p| glob_match(p, member)).unwrap_or(true);
                        if matches {
                            result.push((member.clone(), *score));
                        }
                        i += 1;
                    }

                    let next_cursor = if i >= len { 0 } else { i };
                    (next_cursor, result)
                })
            })
            .unwrap_or((0, vec![]))
    }
}

/// Check if member is in lexicographic range
fn lex_in_range(member: &[u8], min: &[u8], max: &[u8]) -> bool {
    let min_ok = if min == b"-" {
        true
    } else if min.starts_with(b"(") {
        member > &min[1..]
    } else if min.starts_with(b"[") {
        member >= &min[1..]
    } else {
        member >= min
    };

    let max_ok = if max == b"+" {
        true
    } else if max.starts_with(b"(") {
        member < &max[1..]
    } else if max.starts_with(b"[") {
        member <= &max[1..]
    } else {
        member <= max
    };

    min_ok && max_ok
}

/// Simple glob pattern match
fn glob_match(pattern: &[u8], member: &[u8]) -> bool {
    if pattern == b"*" {
        return true;
    }
    // Simple prefix/suffix matching
    if pattern.starts_with(b"*") && pattern.ends_with(b"*") && pattern.len() > 2 {
        let inner = &pattern[1..pattern.len() - 1];
        return member.windows(inner.len()).any(|w| w == inner);
    }
    if pattern.starts_with(b"*") {
        let suffix = &pattern[1..];
        return member.ends_with(suffix);
    }
    if pattern.ends_with(b"*") {
        let prefix = &pattern[..pattern.len() - 1];
        return member.starts_with(prefix);
    }
    member == pattern
}

/// Normalize negative indices for sorted set operations
fn normalize_index(idx: i64, len: i64) -> i64 {
    if idx < 0 { len + idx } else { idx }
}
