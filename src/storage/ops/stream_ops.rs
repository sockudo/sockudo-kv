//! Stream operations for the Store
//!
//! Implements all Redis stream commands with high-performance O(1) and O(log n) operations
//! using BTreeMap for ordered entries and HashMap for consumer groups.

use bytes::Bytes;
use dashmap::mapref::entry::Entry as DashEntry;
use std::sync::atomic::Ordering;

use crate::error::{Error, Result};
use crate::storage::Store;
use crate::storage::types::{
    Consumer, ConsumerGroup, DataType, Entry, PendingEntry, StreamData, StreamId,
};
use crate::storage::value::now_ms;

/// Trim options for XADD and XTRIM
#[derive(Debug, Clone)]
pub enum TrimStrategy {
    /// MAXLEN [=|~] count
    MaxLen { threshold: usize, approx: bool },
    /// MINID [=|~] id
    MinId { threshold: StreamId, approx: bool },
}

/// Result of XAUTOCLAIM
#[derive(Debug)]
pub struct AutoClaimResult {
    pub next_start_id: StreamId,
    pub claimed: Vec<(StreamId, Vec<(Bytes, Bytes)>)>,
    pub deleted_ids: Vec<StreamId>,
}

/// Pending entries summary for XPENDING without range
#[derive(Debug)]
pub struct PendingSummary {
    pub count: usize,
    pub min_id: Option<StreamId>,
    pub max_id: Option<StreamId>,
    pub consumers: Vec<(Bytes, usize)>,
}

/// Stream operations for the Store
impl Store {
    // ==================== Basic Stream Operations ====================

    /// XADD - Add entry to stream
    /// Returns the generated ID on success
    #[inline]
    pub fn xadd(
        &self,
        key: Bytes,
        id: Option<StreamId>,
        fields: Vec<(Bytes, Bytes)>,
        nomkstream: bool,
        trim: Option<TrimStrategy>,
    ) -> Result<Option<StreamId>> {
        use DashEntry::*;

        match self.data.entry(key) {
            Occupied(mut entry) => {
                if entry.get().is_expired() {
                    // Key expired, treat as new
                    if nomkstream {
                        entry.remove();
                        self.key_count.fetch_sub(1, Ordering::Relaxed);
                        return Ok(None);
                    }
                    let mut stream = StreamData::new();
                    let new_id = id.unwrap_or_else(|| stream.generate_id());
                    if !stream.add_entry(new_id, fields) {
                        return Err(Error::Stream(
                            "The ID specified in XADD is equal or smaller than the target stream top item".into()
                        ));
                    }
                    if let Some(trim_strategy) = trim {
                        apply_trim(&mut stream, trim_strategy);
                    }
                    let new_entry = Entry::new(DataType::Stream(stream));
                    entry.insert(new_entry);
                    Ok(Some(new_id))
                } else {
                    let e = entry.get_mut();
                    let result = match e.data.as_stream_mut() {
                        Some(stream) => {
                            let new_id = id.unwrap_or_else(|| stream.generate_id());
                            if !stream.add_entry(new_id, fields) {
                                return Err(Error::Stream(
                                    "The ID specified in XADD is equal or smaller than the target stream top item".into()
                                ));
                            }
                            if let Some(trim_strategy) = trim {
                                apply_trim(stream, trim_strategy);
                            }
                            Ok(Some(new_id))
                        }
                        None => Err(Error::WrongType),
                    };
                    if result.is_ok() {
                        e.bump_version();
                    }
                    result
                }
            }
            Vacant(entry) => {
                if nomkstream {
                    return Ok(None);
                }
                let mut stream = StreamData::new();
                let new_id = id.unwrap_or_else(|| stream.generate_id());
                if !stream.add_entry(new_id, fields) {
                    return Err(Error::Stream(
                        "The ID specified in XADD is equal or smaller than the target stream top item".into()
                    ));
                }
                if let Some(trim_strategy) = trim {
                    apply_trim(&mut stream, trim_strategy);
                }
                entry.insert(Entry::new(DataType::Stream(stream)));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(Some(new_id))
            }
        }
    }

    /// XLEN - Get stream length
    #[inline]
    pub fn xlen(&self, key: &[u8]) -> usize {
        self.data
            .get(key)
            .and_then(|e| {
                if e.is_expired() {
                    None
                } else {
                    e.data.as_stream().map(|s| s.len())
                }
            })
            .unwrap_or(0)
    }

    /// XDEL - Delete entries by ID
    /// Returns number of deleted entries
    #[inline]
    pub fn xdel(&self, key: &[u8], ids: &[StreamId]) -> Result<usize> {
        match self.data.get_mut(key) {
            Some(mut entry) => {
                if entry.is_expired() {
                    return Ok(0);
                }
                let e = entry.value_mut();
                let result = match e.data.as_stream_mut() {
                    Some(stream) => {
                        let mut deleted = 0;
                        for id in ids {
                            if stream.entries.remove(id).is_some() {
                                deleted += 1;
                                if *id > stream.max_deleted_id {
                                    stream.max_deleted_id = *id;
                                }
                            }
                        }
                        stream.first_id = stream.first_entry_id();
                        Ok(deleted)
                    }
                    None => Err(Error::WrongType),
                };
                if let Ok(deleted) = result
                    && deleted > 0
                {
                    e.bump_version();
                }
                result
            }
            None => Ok(0),
        }
    }

    /// XRANGE - Get entries in ID range
    #[inline]
    pub fn xrange(
        &self,
        key: &[u8],
        start: StreamId,
        end: StreamId,
        count: Option<usize>,
    ) -> Result<Vec<(StreamId, Vec<(Bytes, Bytes)>)>> {
        match self.data.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    return Ok(vec![]);
                }
                match entry.data.as_stream() {
                    Some(stream) => {
                        let iter = stream.entries.range(start..=end);
                        let result: Vec<_> = match count {
                            Some(n) => iter
                                .take(n)
                                .map(|(&id, fields)| (id, fields.clone()))
                                .collect(),
                            None => iter.map(|(&id, fields)| (id, fields.clone())).collect(),
                        };
                        Ok(result)
                    }
                    None => Err(Error::WrongType),
                }
            }
            None => Ok(vec![]),
        }
    }

    /// XREVRANGE - Get entries in reverse ID range
    #[inline]
    pub fn xrevrange(
        &self,
        key: &[u8],
        end: StreamId,
        start: StreamId,
        count: Option<usize>,
    ) -> Result<Vec<(StreamId, Vec<(Bytes, Bytes)>)>> {
        match self.data.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    return Ok(vec![]);
                }
                match entry.data.as_stream() {
                    Some(stream) => {
                        let iter = stream.entries.range(start..=end).rev();
                        let result: Vec<_> = match count {
                            Some(n) => iter
                                .take(n)
                                .map(|(&id, fields)| (id, fields.clone()))
                                .collect(),
                            None => iter.map(|(&id, fields)| (id, fields.clone())).collect(),
                        };
                        Ok(result)
                    }
                    None => Err(Error::WrongType),
                }
            }
            None => Ok(vec![]),
        }
    }

    /// XTRIM - Trim stream
    /// Returns number of entries removed
    #[inline]
    pub fn xtrim(&self, key: &[u8], strategy: TrimStrategy) -> Result<usize> {
        match self.data.get_mut(key) {
            Some(mut entry) => {
                if entry.is_expired() {
                    return Ok(0);
                }
                let e = entry.value_mut();
                let result = match e.data.as_stream_mut() {
                    Some(stream) => Ok(apply_trim(stream, strategy)),
                    None => Err(Error::WrongType),
                };
                if let Ok(trimmed) = result
                    && trimmed > 0
                {
                    e.bump_version();
                }
                result
            }
            None => Ok(0),
        }
    }

    /// XREAD - Read from multiple streams
    /// Returns entries from streams with IDs greater than specified
    #[inline]
    pub fn xread(
        &self,
        keys: &[Bytes],
        ids: &[StreamId],
        count: Option<usize>,
    ) -> Result<Vec<(Bytes, Vec<(StreamId, Vec<(Bytes, Bytes)>)>)>> {
        let mut results = Vec::with_capacity(keys.len());

        for (key, start_id) in keys.iter().zip(ids.iter()) {
            if let Some(entry) = self.data.get(key.as_ref()) {
                if entry.is_expired() {
                    continue;
                }
                if let Some(stream) = entry.data.as_stream() {
                    // Get entries with ID > start_id
                    let next_id = start_id.next();
                    let iter = stream.entries.range(next_id..);
                    let entries: Vec<_> = match count {
                        Some(n) => iter
                            .take(n)
                            .map(|(&id, fields)| (id, fields.clone()))
                            .collect(),
                        None => iter.map(|(&id, fields)| (id, fields.clone())).collect(),
                    };
                    if !entries.is_empty() {
                        results.push((key.clone(), entries));
                    }
                }
            }
        }

        Ok(results)
    }

    // ==================== Consumer Group Operations ====================

    /// XGROUP CREATE - Create a consumer group
    #[inline]
    pub fn xgroup_create(
        &self,
        key: Bytes,
        group: Bytes,
        id: StreamId,
        mkstream: bool,
        entries_read: Option<u64>,
    ) -> Result<bool> {
        use DashEntry::*;

        match self.data.entry(key.clone()) {
            Occupied(mut entry) => {
                if entry.get().is_expired() {
                    if !mkstream {
                        return Err(Error::Stream(
                            "ERR The XGROUP subcommand requires the key to exist".into(),
                        ));
                    }
                    let mut stream = StreamData::new();
                    let mut group_data = ConsumerGroup::new(id);
                    group_data.entries_read = entries_read;
                    stream.groups.insert(group, group_data);
                    entry.insert(Entry::new(DataType::Stream(stream)));
                    return Ok(true);
                }
                let e = entry.get_mut();
                let result = match e.data.as_stream_mut() {
                    Some(stream) => {
                        if stream.groups.contains_key(&group) {
                            return Err(Error::Stream(
                                "BUSYGROUP Consumer Group name already exists".into(),
                            ));
                        }
                        let mut group_data = ConsumerGroup::new(id);
                        group_data.entries_read = entries_read;
                        stream.groups.insert(group, group_data);
                        Ok(true)
                    }
                    None => Err(Error::WrongType),
                };
                if result.is_ok() {
                    e.bump_version();
                }
                result
            }
            Vacant(entry) => {
                if !mkstream {
                    return Err(Error::Stream(
                        "ERR The XGROUP subcommand requires the key to exist".into(),
                    ));
                }
                let mut stream = StreamData::new();
                let mut group_data = ConsumerGroup::new(id);
                group_data.entries_read = entries_read;
                stream.groups.insert(group, group_data);
                entry.insert(Entry::new(DataType::Stream(stream)));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(true)
            }
        }
    }

    /// XGROUP CREATECONSUMER - Create a consumer in a group
    #[inline]
    pub fn xgroup_createconsumer(&self, key: &[u8], group: &[u8], consumer: Bytes) -> Result<bool> {
        match self.data.get_mut(key) {
            Some(mut entry) => {
                if entry.is_expired() {
                    return Err(Error::Stream(
                        "ERR The XGROUP subcommand requires the key to exist".into(),
                    ));
                }
                let e = entry.value_mut();
                let result = match e.data.as_stream_mut() {
                    Some(stream) => {
                        match stream.groups.get_mut(group) {
                            Some(group_data) => {
                                if let std::collections::hash_map::Entry::Vacant(e) =
                                    group_data.consumers.entry(consumer)
                                {
                                    e.insert(Consumer::default());
                                    Ok(true)
                                } else {
                                    Ok(false) // Already exists
                                }
                            }
                            None => Err(Error::Stream("NOGROUP No such consumer group".into())),
                        }
                    }
                    None => Err(Error::WrongType),
                };
                if let Ok(true) = result {
                    e.bump_version();
                }
                result
            }
            None => Err(Error::Stream(
                "ERR The XGROUP subcommand requires the key to exist".into(),
            )),
        }
    }

    /// XGROUP DELCONSUMER - Delete a consumer from a group
    /// Returns the number of pending messages that were removed
    #[inline]
    pub fn xgroup_delconsumer(&self, key: &[u8], group: &[u8], consumer: &[u8]) -> Result<usize> {
        match self.data.get_mut(key) {
            Some(mut entry) => {
                if entry.is_expired() {
                    return Err(Error::Stream(
                        "ERR The XGROUP subcommand requires the key to exist".into(),
                    ));
                }
                let e = entry.value_mut();
                let result = match e.data.as_stream_mut() {
                    Some(stream) => {
                        match stream.groups.get_mut(group) {
                            Some(group_data) => {
                                if let Some(removed_consumer) =
                                    group_data.consumers.remove(consumer)
                                {
                                    let pending_count = removed_consumer.pending.len();
                                    // Remove from global pending
                                    for id in &removed_consumer.pending {
                                        group_data.pending.remove(id);
                                    }
                                    Ok(pending_count)
                                } else {
                                    Ok(0)
                                }
                            }
                            None => Err(Error::Stream("NOGROUP No such consumer group".into())),
                        }
                    }
                    None => Err(Error::WrongType),
                };
                if result.is_ok() {
                    e.bump_version();
                }
                result
            }
            None => Err(Error::Stream(
                "ERR The XGROUP subcommand requires the key to exist".into(),
            )),
        }
    }

    /// XGROUP DESTROY - Destroy a consumer group
    #[inline]
    pub fn xgroup_destroy(&self, key: &[u8], group: &[u8]) -> Result<bool> {
        match self.data.get_mut(key) {
            Some(mut entry) => {
                if entry.is_expired() {
                    return Ok(false);
                }
                let e = entry.value_mut();
                let result = match e.data.as_stream_mut() {
                    Some(stream) => Ok(stream.groups.remove(group).is_some()),
                    None => Err(Error::WrongType),
                };
                if let Ok(true) = result {
                    e.bump_version();
                }
                result
            }
            None => Ok(false),
        }
    }

    /// XGROUP SETID - Set the last delivered ID of a consumer group
    #[inline]
    pub fn xgroup_setid(
        &self,
        key: &[u8],
        group: &[u8],
        id: StreamId,
        entries_read: Option<u64>,
    ) -> Result<bool> {
        match self.data.get_mut(key) {
            Some(mut entry) => {
                if entry.is_expired() {
                    return Err(Error::Stream(
                        "ERR The XGROUP subcommand requires the key to exist".into(),
                    ));
                }
                let e = entry.value_mut();
                let result = match e.data.as_stream_mut() {
                    Some(stream) => match stream.groups.get_mut(group) {
                        Some(group_data) => {
                            group_data.last_delivered_id = id;
                            if let Some(er) = entries_read {
                                group_data.entries_read = Some(er);
                            }
                            Ok(true)
                        }
                        None => Err(Error::Stream("NOGROUP No such consumer group".into())),
                    },
                    None => Err(Error::WrongType),
                };
                if result.is_ok() {
                    e.bump_version();
                }
                result
            }
            None => Err(Error::Stream(
                "ERR The XGROUP subcommand requires the key to exist".into(),
            )),
        }
    }

    /// XREADGROUP - Read entries for a consumer group
    #[inline]
    pub fn xreadgroup(
        &self,
        group: &[u8],
        consumer: Bytes,
        keys: &[Bytes],
        ids: &[Bytes],
        count: Option<usize>,
        noack: bool,
    ) -> Result<Vec<(Bytes, Vec<(StreamId, Vec<(Bytes, Bytes)>)>)>> {
        let mut results = Vec::with_capacity(keys.len());
        let now = now_ms();

        for (key, id_bytes) in keys.iter().zip(ids.iter()) {
            let is_new_entries = StreamId::is_new_entries_marker(id_bytes);

            if let Some(mut entry) = self.data.get_mut(key.as_ref()) {
                if entry.is_expired() {
                    continue;
                }
                let e = entry.value_mut();
                let mut modified = false;
                if let Some(stream) = e.data.as_stream_mut() {
                    let group_data = match stream.groups.get_mut(group) {
                        Some(g) => g,
                        None => continue, // No such group
                    };

                    // Ensure consumer exists
                    let consumer_entry = group_data.get_or_create_consumer(consumer.clone());
                    consumer_entry.last_seen = now;

                    let entries: Vec<(StreamId, Vec<(Bytes, Bytes)>)>;

                    if is_new_entries {
                        // Get new entries (after last_delivered_id)
                        let start_id = group_data.last_delivered_id.next();
                        let iter = stream.entries.range(start_id..);
                        entries = match count {
                            Some(n) => iter
                                .take(n)
                                .map(|(&id, fields)| (id, fields.clone()))
                                .collect(),
                            None => iter.map(|(&id, fields)| (id, fields.clone())).collect(),
                        };

                        // Update last_delivered_id and add to pending
                        if let Some((last_id, _)) = entries.last() {
                            group_data.last_delivered_id = *last_id;
                            if let Some(er) = group_data.entries_read.as_mut() {
                                *er += entries.len() as u64;
                            }
                            modified = true;
                        }

                        if !noack {
                            for (id, _) in &entries {
                                group_data.pending.insert(
                                    *id,
                                    PendingEntry {
                                        consumer: consumer.clone(),
                                        delivery_time: now,
                                        delivery_count: 1,
                                    },
                                );
                                if let Some(c) = group_data.consumers.get_mut(&consumer) {
                                    c.pending.insert(*id);
                                }
                            }
                        }
                    } else {
                        // Get pending entries for this consumer (history)
                        let start_id = StreamId::parse(id_bytes).unwrap_or(StreamId::ZERO);
                        entries = group_data
                            .pending
                            .range(start_id..)
                            .filter(|(_, pe)| pe.consumer == consumer)
                            .take(count.unwrap_or(usize::MAX))
                            .filter_map(|(&id, _pe)| {
                                stream.entries.get(&id).map(|fields| {
                                    // Update delivery info
                                    (id, fields.clone())
                                })
                            })
                            .collect();

                        // Update delivery count
                        for (id, _) in &entries {
                            if let Some(pe) = group_data.pending.get_mut(id) {
                                pe.delivery_count += 1;
                                pe.delivery_time = now;
                                modified = true;
                            }
                        }
                    }

                    if !entries.is_empty() {
                        results.push((key.clone(), entries));
                    }
                }
                if modified {
                    e.bump_version();
                }
            }
        }

        Ok(results)
    }

    /// XACK - Acknowledge messages
    /// Returns number of acknowledged messages
    #[inline]
    pub fn xack(&self, key: &[u8], group: &[u8], ids: &[StreamId]) -> Result<usize> {
        match self.data.get_mut(key) {
            Some(mut entry) => {
                if entry.is_expired() {
                    return Ok(0);
                }
                let e = entry.value_mut();
                let result = match e.data.as_stream_mut() {
                    Some(stream) => {
                        match stream.groups.get_mut(group) {
                            Some(group_data) => {
                                let mut acked = 0;
                                for id in ids {
                                    if let Some(pe) = group_data.pending.remove(id) {
                                        if let Some(c) = group_data.consumers.get_mut(&pe.consumer)
                                        {
                                            c.pending.remove(id);
                                        }
                                        acked += 1;
                                    }
                                }
                                Ok(acked)
                            }
                            None => Ok(0), // No such group
                        }
                    }
                    None => Err(Error::WrongType),
                };
                if let Ok(acked) = result
                    && acked > 0
                {
                    e.bump_version();
                }
                result
            }
            None => Ok(0),
        }
    }

    /// XCLAIM - Claim messages for a consumer
    #[inline]
    pub fn xclaim(
        &self,
        key: &[u8],
        group: &[u8],
        consumer: Bytes,
        min_idle_time: i64,
        ids: &[StreamId],
        idle: Option<i64>,
        time: Option<i64>,
        retry_count: Option<u32>,
        force: bool,
        justid: bool,
    ) -> Result<Vec<(StreamId, Option<Vec<(Bytes, Bytes)>>)>> {
        let now = now_ms();

        match self.data.get_mut(key) {
            Some(mut entry) => {
                if entry.is_expired() {
                    return Ok(vec![]);
                }
                let e = entry.value_mut();
                let result = match e.data.as_stream_mut() {
                    Some(stream) => {
                        let group_data = match stream.groups.get_mut(group) {
                            Some(g) => g,
                            None => {
                                return Err(Error::Stream("NOGROUP No such consumer group".into()));
                            }
                        };

                        // Ensure consumer exists
                        group_data.get_or_create_consumer(consumer.clone());

                        let mut claimed = Vec::new();

                        for id in ids {
                            let should_claim = if let Some(pe) = group_data.pending.get(id) {
                                let idle_time = now - pe.delivery_time;
                                idle_time >= min_idle_time
                            } else {
                                force // Only claim non-pending if FORCE
                            };

                            if !should_claim {
                                continue;
                            }

                            // Get entry from stream
                            let entry_exists = stream.entries.contains_key(id);
                            if !entry_exists && !force {
                                continue;
                            }

                            // Remove from old consumer's pending
                            if let Some(pe) = group_data.pending.get(id)
                                && let Some(old_consumer) =
                                    group_data.consumers.get_mut(&pe.consumer)
                            {
                                old_consumer.pending.remove(id);
                            }

                            // Add to new consumer
                            let delivery_time = time.unwrap_or_else(|| now - idle.unwrap_or(0));
                            let new_delivery_count = retry_count.unwrap_or_else(|| {
                                group_data
                                    .pending
                                    .get(id)
                                    .map(|pe| pe.delivery_count + 1)
                                    .unwrap_or(1)
                            });

                            group_data.pending.insert(
                                *id,
                                PendingEntry {
                                    consumer: consumer.clone(),
                                    delivery_time,
                                    delivery_count: new_delivery_count,
                                },
                            );

                            if let Some(c) = group_data.consumers.get_mut(&consumer) {
                                c.pending.insert(*id);
                            }

                            // Add to result
                            if justid {
                                claimed.push((*id, None));
                            } else if let Some(fields) = stream.entries.get(id) {
                                claimed.push((*id, Some(fields.clone())));
                            } else {
                                claimed.push((*id, Some(vec![]))); // Deleted entry
                            }
                        }

                        Ok(claimed)
                    }
                    None => Err(Error::WrongType),
                };
                if let Ok(ref claimed) = result
                    && !claimed.is_empty()
                {
                    e.bump_version();
                }
                result
            }
            None => Ok(vec![]),
        }
    }

    /// XAUTOCLAIM - Automatically claim idle messages
    #[inline]
    pub fn xautoclaim(
        &self,
        key: &[u8],
        group: &[u8],
        consumer: Bytes,
        min_idle_time: i64,
        start: StreamId,
        count: usize,
        justid: bool,
    ) -> Result<AutoClaimResult> {
        let now = now_ms();

        match self.data.get_mut(key) {
            Some(mut entry) => {
                if entry.is_expired() {
                    return Ok(AutoClaimResult {
                        next_start_id: StreamId::ZERO,
                        claimed: vec![],
                        deleted_ids: vec![],
                    });
                }
                let e = entry.value_mut();
                let result = match e.data.as_stream_mut() {
                    Some(stream) => {
                        let group_data = match stream.groups.get_mut(group) {
                            Some(g) => g,
                            None => {
                                return Err(Error::Stream("NOGROUP No such consumer group".into()));
                            }
                        };

                        // Ensure consumer exists
                        group_data.get_or_create_consumer(consumer.clone());

                        let mut claimed = Vec::new();
                        let mut deleted_ids = Vec::new();
                        let mut next_start_id = StreamId::ZERO;
                        let mut checked = 0;

                        // Iterate pending entries from start
                        let ids_to_check: Vec<_> = group_data
                            .pending
                            .range(start..)
                            .take(count * 2) // Check more to account for skips
                            .map(|(&id, _)| id)
                            .collect();

                        for id in ids_to_check {
                            if claimed.len() >= count {
                                next_start_id = id;
                                break;
                            }

                            checked += 1;
                            let pe = match group_data.pending.get(&id) {
                                Some(pe) => pe.clone(),
                                None => continue,
                            };

                            let idle_time = now - pe.delivery_time;
                            if idle_time < min_idle_time {
                                continue;
                            }

                            // Check if entry still exists
                            if !stream.entries.contains_key(&id) {
                                deleted_ids.push(id);
                                group_data.pending.remove(&id);
                                if let Some(c) = group_data.consumers.get_mut(&pe.consumer) {
                                    c.pending.remove(&id);
                                }
                                continue;
                            }

                            // Claim the entry
                            // Remove from old consumer
                            if let Some(old_consumer) = group_data.consumers.get_mut(&pe.consumer) {
                                old_consumer.pending.remove(&id);
                            }

                            // Add to new consumer
                            group_data.pending.insert(
                                id,
                                PendingEntry {
                                    consumer: consumer.clone(),
                                    delivery_time: now,
                                    delivery_count: pe.delivery_count + 1,
                                },
                            );

                            if let Some(c) = group_data.consumers.get_mut(&consumer) {
                                c.pending.insert(id);
                            }

                            // Add to result
                            if justid {
                                claimed.push((id, vec![]));
                            } else if let Some(fields) = stream.entries.get(&id) {
                                claimed.push((id, fields.clone()));
                            }

                            next_start_id = id.next();
                        }

                        if checked > 0 && claimed.len() < count && next_start_id == StreamId::ZERO {
                            next_start_id = StreamId::ZERO; // Wrapped around
                        }

                        Ok(AutoClaimResult {
                            next_start_id,
                            claimed,
                            deleted_ids,
                        })
                    }
                    None => Err(Error::WrongType),
                };
                if let Ok(ref res) = result
                    && (!res.claimed.is_empty() || !res.deleted_ids.is_empty())
                {
                    e.bump_version();
                }
                result
            }
            None => Ok(AutoClaimResult {
                next_start_id: StreamId::ZERO,
                claimed: vec![],
                deleted_ids: vec![],
            }),
        }
    }

    /// XPENDING - Get pending entries info
    #[inline]
    pub fn xpending_summary(&self, key: &[u8], group: &[u8]) -> Result<PendingSummary> {
        match self.data.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    return Ok(PendingSummary {
                        count: 0,
                        min_id: None,
                        max_id: None,
                        consumers: vec![],
                    });
                }
                match entry.data.as_stream() {
                    Some(stream) => {
                        match stream.groups.get(group) {
                            Some(group_data) => {
                                let count = group_data.pending.len();
                                let min_id = group_data.pending.keys().next().copied();
                                let max_id = group_data.pending.keys().next_back().copied();

                                // Count per consumer
                                let mut consumer_counts: std::collections::HashMap<Bytes, usize> =
                                    std::collections::HashMap::new();
                                for pe in group_data.pending.values() {
                                    *consumer_counts.entry(pe.consumer.clone()).or_insert(0) += 1;
                                }
                                let consumers: Vec<_> = consumer_counts.into_iter().collect();

                                Ok(PendingSummary {
                                    count,
                                    min_id,
                                    max_id,
                                    consumers,
                                })
                            }
                            None => Err(Error::Stream("NOGROUP No such consumer group".into())),
                        }
                    }
                    None => Err(Error::WrongType),
                }
            }
            None => Err(Error::Stream("ERR no such key".into())),
        }
    }

    /// XPENDING with range - Get detailed pending entries
    #[inline]
    pub fn xpending_range(
        &self,
        key: &[u8],
        group: &[u8],
        start: StreamId,
        end: StreamId,
        count: usize,
        consumer: Option<&[u8]>,
        idle: Option<i64>,
    ) -> Result<Vec<(StreamId, Bytes, i64, u32)>> {
        let now = now_ms();

        match self.data.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    return Ok(vec![]);
                }
                match entry.data.as_stream() {
                    Some(stream) => match stream.groups.get(group) {
                        Some(group_data) => {
                            let result: Vec<_> = group_data
                                .pending
                                .range(start..=end)
                                .filter(|(_, pe)| {
                                    let matches_consumer =
                                        consumer.is_none_or(|c| pe.consumer.as_ref() == c);
                                    let matches_idle = idle.is_none_or(|min_idle| {
                                        (now - pe.delivery_time) >= min_idle
                                    });
                                    matches_consumer && matches_idle
                                })
                                .take(count)
                                .map(|(&id, pe)| {
                                    let idle_time = now - pe.delivery_time;
                                    (id, pe.consumer.clone(), idle_time, pe.delivery_count)
                                })
                                .collect();
                            Ok(result)
                        }
                        None => Err(Error::Stream("NOGROUP No such consumer group".into())),
                    },
                    None => Err(Error::WrongType),
                }
            }
            None => Err(Error::Stream("ERR no such key".into())),
        }
    }

    // ==================== Info Operations ====================

    /// XINFO STREAM - Get stream info
    #[inline]
    pub fn xinfo_stream(&self, key: &[u8], full: bool, count: Option<usize>) -> Result<StreamInfo> {
        match self.data.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    return Err(Error::Stream("ERR no such key".into()));
                }
                match entry.data.as_stream() {
                    Some(stream) => {
                        let first_entry = stream
                            .entries
                            .iter()
                            .next()
                            .map(|(&id, fields)| (id, fields.clone()));
                        let last_entry = stream
                            .entries
                            .iter()
                            .next_back()
                            .map(|(&id, fields)| (id, fields.clone()));

                        let groups = if full {
                            let limit = count.unwrap_or(10);
                            stream
                                .groups
                                .iter()
                                .take(limit)
                                .map(|(name, g)| GroupInfo {
                                    name: name.clone(),
                                    consumers: g.consumers.len(),
                                    pending: g.pending.len(),
                                    last_delivered_id: g.last_delivered_id,
                                    entries_read: g.entries_read,
                                    lag: g.lag(stream.entries_added),
                                })
                                .collect()
                        } else {
                            vec![]
                        };

                        Ok(StreamInfo {
                            length: stream.len(),
                            radix_tree_keys: 1, // Simplified
                            radix_tree_nodes: stream.len(),
                            last_generated_id: stream.last_id,
                            max_deleted_entry_id: stream.max_deleted_id,
                            entries_added: stream.entries_added,
                            first_entry,
                            last_entry,
                            groups: stream.groups.len(),
                            group_details: groups,
                        })
                    }
                    None => Err(Error::WrongType),
                }
            }
            None => Err(Error::Stream("ERR no such key".into())),
        }
    }

    /// XINFO GROUPS - Get consumer groups info
    #[inline]
    pub fn xinfo_groups(&self, key: &[u8]) -> Result<Vec<GroupInfo>> {
        match self.data.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    return Err(Error::Stream("ERR no such key".into()));
                }
                match entry.data.as_stream() {
                    Some(stream) => {
                        let groups: Vec<_> = stream
                            .groups
                            .iter()
                            .map(|(name, g)| GroupInfo {
                                name: name.clone(),
                                consumers: g.consumers.len(),
                                pending: g.pending.len(),
                                last_delivered_id: g.last_delivered_id,
                                entries_read: g.entries_read,
                                lag: g.lag(stream.entries_added),
                            })
                            .collect();
                        Ok(groups)
                    }
                    None => Err(Error::WrongType),
                }
            }
            None => Err(Error::Stream("ERR no such key".into())),
        }
    }

    /// XINFO CONSUMERS - Get consumers in a group
    #[inline]
    pub fn xinfo_consumers(&self, key: &[u8], group: &[u8]) -> Result<Vec<ConsumerInfo>> {
        let now = now_ms();

        match self.data.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    return Err(Error::Stream("ERR no such key".into()));
                }
                match entry.data.as_stream() {
                    Some(stream) => match stream.groups.get(group) {
                        Some(group_data) => {
                            let consumers: Vec<_> = group_data
                                .consumers
                                .iter()
                                .map(|(name, c)| ConsumerInfo {
                                    name: name.clone(),
                                    pending: c.pending.len(),
                                    idle: now - c.last_seen,
                                    inactive: now - c.last_seen,
                                })
                                .collect();
                            Ok(consumers)
                        }
                        None => Err(Error::Stream("NOGROUP No such consumer group".into())),
                    },
                    None => Err(Error::WrongType),
                }
            }
            None => Err(Error::Stream("ERR no such key".into())),
        }
    }

    /// XSETID - Set stream last ID (internal/replication)
    #[inline]
    pub fn xsetid(
        &self,
        key: &[u8],
        last_id: StreamId,
        entries_added: Option<u64>,
        max_deleted_id: Option<StreamId>,
    ) -> Result<bool> {
        match self.data.get_mut(key) {
            Some(mut entry) => {
                if entry.is_expired() {
                    return Err(Error::Stream("ERR no such key".into()));
                }
                let e = entry.value_mut();
                let result = match e.data.as_stream_mut() {
                    Some(stream) => {
                        if last_id < stream.last_id {
                            return Err(Error::Stream(
                                "ERR The ID specified in XSETID is smaller than the target stream top item".into()
                            ));
                        }
                        stream.last_id = last_id;
                        if let Some(ea) = entries_added {
                            stream.entries_added = ea;
                        }
                        if let Some(md) = max_deleted_id {
                            stream.max_deleted_id = md;
                        }
                        Ok(true)
                    }
                    None => Err(Error::WrongType),
                };
                if result.is_ok() {
                    e.bump_version();
                }
                result
            }
            None => Err(Error::Stream("ERR no such key".into())),
        }
    }
}

/// Apply trim strategy
#[inline]
fn apply_trim(stream: &mut StreamData, strategy: TrimStrategy) -> usize {
    match strategy {
        TrimStrategy::MaxLen { threshold, approx } => stream.trim_maxlen(threshold, approx),
        TrimStrategy::MinId { threshold, approx } => stream.trim_minid(threshold, approx),
    }
}

// ==================== Info Types ====================

#[derive(Debug)]
pub struct StreamInfo {
    pub length: usize,
    pub radix_tree_keys: usize,
    pub radix_tree_nodes: usize,
    pub last_generated_id: StreamId,
    pub max_deleted_entry_id: StreamId,
    pub entries_added: u64,
    pub first_entry: Option<(StreamId, Vec<(Bytes, Bytes)>)>,
    pub last_entry: Option<(StreamId, Vec<(Bytes, Bytes)>)>,
    pub groups: usize,
    pub group_details: Vec<GroupInfo>,
}

#[derive(Debug)]
pub struct GroupInfo {
    pub name: Bytes,
    pub consumers: usize,
    pub pending: usize,
    pub last_delivered_id: StreamId,
    pub entries_read: Option<u64>,
    pub lag: Option<u64>,
}

#[derive(Debug)]
pub struct ConsumerInfo {
    pub name: Bytes,
    pub pending: usize,
    pub idle: i64,
    pub inactive: i64,
}
