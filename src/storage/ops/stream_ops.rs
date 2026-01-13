//! Stream operations for the Store
//!
//! Implements all Redis stream commands with high-performance O(1) and O(log n) operations
//! using BTreeMap for ordered entries and HashMap for consumer groups.

use bytes::Bytes;
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

pub struct StreamInfo {
    pub length: usize,
    pub radix_tree_keys: usize,
    pub radix_tree_nodes: usize,
    pub last_generated_id: StreamId,
    pub max_deleted_entry_id: StreamId,
    pub entries_added: u64,
    pub groups: usize,
    pub first_entry: Option<(StreamId, Vec<(Bytes, Bytes)>)>,
    pub last_entry: Option<(StreamId, Vec<(Bytes, Bytes)>)>,
}

pub struct StreamGroupInfo {
    pub name: Bytes,
    pub consumers: usize,
    pub pending: usize,
    pub last_delivered_id: StreamId,
    pub entries_read: Option<u64>,
    pub lag: Option<u64>,
}

pub struct StreamConsumerInfo {
    pub name: Bytes,
    pub pending: usize,
    pub idle: i64,
    pub inactive: i64,
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
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    if nomkstream {
                        e.remove();
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
                    entry.data = DataType::Stream(stream);
                    entry.persist();
                    entry.bump_version();
                    Ok(Some(new_id))
                } else {
                    let result = match &mut entry.data {
                        DataType::Stream(stream) => {
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
                        _ => Err(Error::WrongType),
                    };
                    if result.is_ok() {
                        entry.bump_version();
                    }
                    result
                }
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
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
                e.insert((key, Entry::new(DataType::Stream(stream))));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(Some(new_id))
            }
        }
    }

    /// XLEN - Get stream length
    #[inline]
    pub fn xlen(&self, key: &[u8]) -> usize {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    0
                } else {
                    entry_ref.1.data.as_stream().map(|s| s.len()).unwrap_or(0)
                }
            }
            None => 0,
        }
    }

    /// XDEL - Delete entries by ID
    /// Returns number of deleted entries
    #[inline]
    pub fn xdel(&self, key: &[u8], ids: &[StreamId]) -> Result<usize> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return Ok(0);
                }
                let result = match &mut entry.data {
                    DataType::Stream(stream) => {
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
                    _ => Err(Error::WrongType),
                };
                if let Ok(deleted) = result
                    && deleted > 0
                {
                    entry.bump_version();
                }
                result
            }
            crate::storage::dashtable::Entry::Vacant(_) => Ok(0),
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
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return Ok(vec![]);
                }
                match entry_ref.1.data.as_stream() {
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
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return Ok(vec![]);
                }
                match entry_ref.1.data.as_stream() {
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
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return Ok(0);
                }
                let result = match &mut entry.data {
                    DataType::Stream(stream) => Ok(apply_trim(stream, strategy)),
                    _ => Err(Error::WrongType),
                };
                if let Ok(trimmed) = result
                    && trimmed > 0
                {
                    entry.bump_version();
                }
                result
            }
            crate::storage::dashtable::Entry::Vacant(_) => Ok(0),
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
            if let Some(entry_ref) = self.data_get(key.as_ref()) {
                if entry_ref.1.is_expired() {
                    continue;
                }
                if let Some(stream) = entry_ref.1.data.as_stream() {
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
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    if !mkstream {
                        return Err(Error::Stream(
                            "ERR The XGROUP subcommand requires the key to exist".into(),
                        ));
                    }
                    let mut stream = StreamData::new();
                    let mut group_data = ConsumerGroup::new(id);
                    group_data.entries_read = entries_read;
                    stream.groups.insert(group, group_data);
                    entry.data = DataType::Stream(stream);
                    entry.persist();
                    entry.bump_version();
                    return Ok(true);
                }
                let result = match &mut entry.data {
                    DataType::Stream(stream) => {
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
                    _ => Err(Error::WrongType),
                };
                if result.is_ok() {
                    entry.bump_version();
                }
                result
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                if !mkstream {
                    return Err(Error::Stream(
                        "ERR The XGROUP subcommand requires the key to exist".into(),
                    ));
                }
                let mut stream = StreamData::new();
                let mut group_data = ConsumerGroup::new(id);
                group_data.entries_read = entries_read;
                stream.groups.insert(group, group_data);
                e.insert((key, Entry::new(DataType::Stream(stream))));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(true)
            }
        }
    }

    /// XGROUP CREATECONSUMER - Create a consumer in a group
    #[inline]
    pub fn xgroup_createconsumer(&self, key: &[u8], group: &[u8], consumer: Bytes) -> Result<bool> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    return Err(Error::Stream(
                        "ERR The XGROUP subcommand requires the key to exist".into(),
                    ));
                }
                let result = match &mut entry.data {
                    DataType::Stream(stream) => {
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
                    _ => Err(Error::WrongType),
                };
                if let Ok(true) = result {
                    entry.bump_version();
                }
                result
            }
            crate::storage::dashtable::Entry::Vacant(_) => Err(Error::Stream(
                "ERR The XGROUP subcommand requires the key to exist".into(),
            )),
        }
    }

    /// XGROUP DELCONSUMER - Delete a consumer from a group
    /// Returns the number of pending messages that were removed
    #[inline]
    pub fn xgroup_delconsumer(&self, key: &[u8], group: &[u8], consumer: &[u8]) -> Result<usize> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    return Err(Error::Stream(
                        "ERR The XGROUP subcommand requires the key to exist".into(),
                    ));
                }
                let result = match &mut entry.data {
                    DataType::Stream(stream) => match stream.groups.get_mut(group) {
                        Some(group_data) => {
                            if let Some(removed_consumer) = group_data.consumers.remove(consumer) {
                                let pending_count = removed_consumer.pending.len();
                                for id in &removed_consumer.pending {
                                    group_data.pending.remove(id);
                                }
                                Ok(pending_count)
                            } else {
                                Ok(0)
                            }
                        }
                        None => Err(Error::Stream("NOGROUP No such consumer group".into())),
                    },
                    _ => Err(Error::WrongType),
                };
                if result.is_ok() {
                    entry.bump_version();
                }
                result
            }
            crate::storage::dashtable::Entry::Vacant(_) => Err(Error::Stream(
                "ERR The XGROUP subcommand requires the key to exist".into(),
            )),
        }
    }

    /// XGROUP DESTROY - Destroy a consumer group
    #[inline]
    pub fn xgroup_destroy(&self, key: &[u8], group: &[u8]) -> Result<bool> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return Ok(false);
                }
                let result = match &mut entry.data {
                    DataType::Stream(stream) => Ok(stream.groups.remove(group).is_some()),
                    _ => Err(Error::WrongType),
                };
                if let Ok(true) = result {
                    entry.bump_version();
                }
                result
            }
            crate::storage::dashtable::Entry::Vacant(_) => Ok(false),
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
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    return Err(Error::Stream(
                        "ERR The XGROUP subcommand requires the key to exist".into(),
                    ));
                }
                let result = match &mut entry.data {
                    DataType::Stream(stream) => match stream.groups.get_mut(group) {
                        Some(group_data) => {
                            group_data.last_delivered_id = id;
                            if let Some(er) = entries_read {
                                group_data.entries_read = Some(er);
                            }
                            Ok(true)
                        }
                        None => Err(Error::Stream("NOGROUP No such consumer group".into())),
                    },
                    _ => Err(Error::WrongType),
                };
                if result.is_ok() {
                    entry.bump_version();
                }
                result
            }
            crate::storage::dashtable::Entry::Vacant(_) => Err(Error::Stream(
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

            match self.data_entry(key.as_ref()) {
                crate::storage::dashtable::Entry::Occupied(mut e) => {
                    let entry = &mut e.get_mut().1;
                    if entry.is_expired() {
                        continue;
                    }
                    let mut modified = false;
                    if let Some(stream) = entry.data.as_stream_mut() {
                        let group_data = match stream.groups.get_mut(group) {
                            Some(g) => g,
                            None => continue,
                        };

                        let consumer_entry = group_data.get_or_create_consumer(consumer.clone());
                        consumer_entry.last_seen = now;

                        let entries: Vec<(StreamId, Vec<(Bytes, Bytes)>)>;

                        if is_new_entries {
                            let start_id = group_data.last_delivered_id.next();
                            let iter = stream.entries.range(start_id..);
                            entries = match count {
                                Some(n) => iter
                                    .take(n)
                                    .map(|(&id, fields)| (id, fields.clone()))
                                    .collect(),
                                None => iter.map(|(&id, fields)| (id, fields.clone())).collect(),
                            };

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
                            let start_id = StreamId::parse(id_bytes).unwrap_or(StreamId::ZERO);
                            entries = group_data
                                .pending
                                .range(start_id..)
                                .filter(|(_, pe)| pe.consumer == consumer)
                                .take(count.unwrap_or(usize::MAX))
                                .filter_map(|(&id, _pe)| {
                                    stream.entries.get(&id).map(|fields| (id, fields.clone()))
                                })
                                .collect();

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
                        entry.bump_version();
                    }
                }
                crate::storage::dashtable::Entry::Vacant(_) => {}
            }
        }

        Ok(results)
    }

    /// XACK - Acknowledge messages
    /// Returns number of acknowledged messages
    #[inline]
    pub fn xack(&self, key: &[u8], group: &[u8], ids: &[StreamId]) -> Result<usize> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return Ok(0);
                }
                let result = match &mut entry.data {
                    DataType::Stream(stream) => match stream.groups.get_mut(group) {
                        Some(group_data) => {
                            let mut acked = 0;
                            for id in ids {
                                if let Some(pe) = group_data.pending.remove(id) {
                                    if let Some(c) = group_data.consumers.get_mut(&pe.consumer) {
                                        c.pending.remove(id);
                                    }
                                    acked += 1;
                                }
                            }
                            Ok(acked)
                        }
                        None => Ok(0),
                    },
                    _ => Err(Error::WrongType),
                };
                if let Ok(acked) = result
                    && acked > 0
                {
                    entry.bump_version();
                }
                result
            }
            crate::storage::dashtable::Entry::Vacant(_) => Ok(0),
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

        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return Ok(vec![]);
                }
                let result = match &mut entry.data {
                    DataType::Stream(stream) => {
                        let group_data = match stream.groups.get_mut(group) {
                            Some(g) => g,
                            None => {
                                return Err(Error::Stream("NOGROUP No such consumer group".into()));
                            }
                        };

                        group_data.get_or_create_consumer(consumer.clone());

                        let mut claimed = Vec::new();

                        for id in ids {
                            let should_claim = if let Some(pe) = group_data.pending.get(id) {
                                let idle_time = now - pe.delivery_time;
                                idle_time >= min_idle_time
                            } else {
                                force
                            };

                            if !should_claim {
                                continue;
                            }

                            let entry_exists = stream.entries.contains_key(id);
                            if !entry_exists && !force {
                                continue;
                            }

                            if let Some(pe) = group_data.pending.get(id)
                                && let Some(old_consumer) =
                                    group_data.consumers.get_mut(&pe.consumer)
                            {
                                old_consumer.pending.remove(id);
                            }

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

                            if justid {
                                claimed.push((*id, None));
                            } else if let Some(fields) = stream.entries.get(id) {
                                claimed.push((*id, Some(fields.clone())));
                            } else {
                                claimed.push((*id, Some(vec![])));
                            }
                        }

                        Ok(claimed)
                    }
                    _ => Err(Error::WrongType),
                };
                if let Ok(ref claimed) = result
                    && !claimed.is_empty()
                {
                    entry.bump_version();
                }
                result
            }
            crate::storage::dashtable::Entry::Vacant(_) => Ok(vec![]),
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

        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    e.remove();
                    return Ok(AutoClaimResult {
                        next_start_id: StreamId::ZERO,
                        claimed: vec![],
                        deleted_ids: vec![],
                    });
                }
                let result = match &mut entry.data {
                    DataType::Stream(stream) => {
                        let group_data = match stream.groups.get_mut(group) {
                            Some(g) => g,
                            None => {
                                return Err(Error::Stream("NOGROUP No such consumer group".into()));
                            }
                        };

                        group_data.get_or_create_consumer(consumer.clone());

                        let mut claimed = Vec::new();
                        let mut deleted_ids = Vec::new();
                        let mut next_start_id = StreamId::ZERO;

                        let consumers = &mut group_data.consumers;
                        for (&id, pe) in group_data.pending.range_mut(start..) {
                            if claimed.len() >= count {
                                next_start_id = id;
                                break;
                            }

                            let idle_time = now - pe.delivery_time;
                            if idle_time >= min_idle_time {
                                if let Some(fields) = stream.entries.get(&id) {
                                    if !justid {
                                        claimed.push((id, fields.clone()));
                                    } else {
                                        claimed.push((id, vec![]));
                                    }
                                } else {
                                    deleted_ids.push(id);
                                }

                                if let Some(old_consumer) = consumers.get_mut(&pe.consumer) {
                                    old_consumer.pending.remove(&id);
                                }

                                if let Some(c) = consumers.get_mut(&consumer) {
                                    c.pending.insert(id);
                                }

                                pe.consumer = consumer.clone();
                                pe.delivery_time = now;
                                pe.delivery_count += 1;
                            }
                        }

                        Ok(AutoClaimResult {
                            next_start_id,
                            claimed,
                            deleted_ids,
                        })
                    }
                    _ => Err(Error::WrongType),
                };
                if result.is_ok() {
                    entry.bump_version();
                }
                result
            }
            crate::storage::dashtable::Entry::Vacant(_) => Ok(AutoClaimResult {
                next_start_id: StreamId::ZERO,
                claimed: vec![],
                deleted_ids: vec![],
            }),
        }
    }

    /// XPENDING - Get information about pending messages
    #[inline]
    /// XPENDING summary mode
    pub fn xpending_summary(&self, key: &[u8], group: &[u8]) -> Result<PendingSummary> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return Err(Error::Custom("ERR no such key".into()));
                }
                match entry_ref.1.data.as_stream() {
                    Some(stream) => {
                        let group_data = stream.groups.get(group).ok_or_else(|| {
                            Error::Stream("NOGROUP No such consumer group".into())
                        })?;

                        let consumers = group_data
                            .consumers
                            .iter()
                            .map(|(k, v)| (k.clone(), v.pending.len()))
                            .collect();

                        Ok(PendingSummary {
                            count: group_data.pending.len(),
                            min_id: group_data.pending.keys().next().copied(),
                            max_id: group_data.pending.keys().next_back().copied(),
                            consumers,
                        })
                    }
                    None => Err(Error::WrongType),
                }
            }
            None => Err(Error::Custom("ERR no such key".into())),
        }
    }

    /// XPENDING range mode
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
        match self.data_get(key) {
            Some(entry_ref) => {
                let now = now_ms();
                match entry_ref.1.data.as_stream() {
                    Some(stream) => {
                        let group_data = stream.groups.get(group).ok_or_else(|| {
                            Error::Stream("NOGROUP No such consumer group".into())
                        })?;

                        let mut result = Vec::new();
                        for (&id, pe) in group_data.pending.range(start..=end) {
                            if result.len() >= count {
                                break;
                            }
                            if let Some(c_filter) = consumer {
                                if pe.consumer != c_filter {
                                    continue;
                                }
                            }
                            let idle_time = now - pe.delivery_time;
                            if let Some(i_filter) = idle {
                                if idle_time < i_filter {
                                    continue;
                                }
                            }
                            result.push((id, pe.consumer.clone(), idle_time, pe.delivery_count));
                        }
                        Ok(result)
                    }
                    None => Err(Error::WrongType),
                }
            }
            None => Ok(vec![]),
        }
    }

    /// Helper to get stream groups info
    pub fn xinfo_groups(&self, key: &[u8]) -> Result<Vec<StreamGroupInfo>> {
        match self.data_get(key) {
            Some(entry_ref) => match entry_ref.1.data.as_stream() {
                Some(stream) => {
                    let mut result = Vec::new();
                    for (name, group) in &stream.groups {
                        result.push(StreamGroupInfo {
                            name: name.clone(),
                            consumers: group.consumers.len(),
                            pending: group.pending.len(),
                            last_delivered_id: group.last_delivered_id,
                            entries_read: group.entries_read,
                            lag: group.lag(stream.entries_added),
                        });
                    }
                    Ok(result)
                }
                None => Err(Error::WrongType),
            },
            None => Err(Error::Custom("ERR no such key".into())),
        }
    }

    /// Helper to get stream consumers info
    pub fn xinfo_consumers(&self, key: &[u8], group: &[u8]) -> Result<Vec<StreamConsumerInfo>> {
        match self.data_get(key) {
            Some(entry_ref) => {
                let now = now_ms();
                match entry_ref.1.data.as_stream() {
                    Some(stream) => {
                        let g = stream.groups.get(group).ok_or_else(|| {
                            Error::Stream("NOGROUP No such consumer group".into())
                        })?;
                        let mut result = Vec::new();
                        for (name, consumer) in &g.consumers {
                            result.push(StreamConsumerInfo {
                                name: name.clone(),
                                pending: consumer.pending.len(),
                                idle: now - consumer.last_seen,
                                inactive: -1, // Not tracking inactive specifically
                            });
                        }
                        Ok(result)
                    }
                    None => Err(Error::WrongType),
                }
            }
            None => Err(Error::Custom("ERR no such key".into())),
        }
    }

    /// Helper to get stream info
    pub fn xinfo_stream(
        &self,
        key: &[u8],
        _full: bool,
        _count: Option<usize>,
    ) -> Result<StreamInfo> {
        match self.data_get(key) {
            Some(entry_ref) => match entry_ref.1.data.as_stream() {
                Some(stream) => {
                    let first = stream.entries.iter().next().map(|(k, v)| (*k, v.clone()));
                    let last = stream
                        .entries
                        .iter()
                        .next_back()
                        .map(|(k, v)| (*k, v.clone()));

                    Ok(StreamInfo {
                        length: stream.entries.len(),
                        radix_tree_keys: 0,
                        radix_tree_nodes: 0,
                        last_generated_id: stream.last_id,
                        max_deleted_entry_id: stream.max_deleted_id,
                        entries_added: stream.entries_added,
                        groups: stream.groups.len(),
                        first_entry: first,
                        last_entry: last,
                    })
                }
                None => Err(Error::WrongType),
            },
            None => Err(Error::Custom("ERR no such key".into())),
        }
    }

    pub fn xsetid(
        &self,
        key: &[u8],
        last_id: StreamId,
        entries_added: Option<u64>,
        max_deleted_id: Option<StreamId>,
    ) -> Result<()> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                match &mut entry.data {
                    DataType::Stream(stream) => {
                        stream.last_id = last_id;
                        if let Some(ea) = entries_added {
                            stream.entries_added = ea;
                        }
                        if let Some(md) = max_deleted_id {
                            stream.max_deleted_id = md;
                        }
                        entry.bump_version();
                        Ok(())
                    }
                    _ => Err(Error::WrongType),
                }
            }
            crate::storage::dashtable::Entry::Vacant(_) => {
                Err(Error::Custom("ERR no such key".into()))
            }
        }
    }
}

/// Helper to apply trim strategy to stream
fn apply_trim(stream: &mut StreamData, strategy: TrimStrategy) -> usize {
    let initial_len = stream.len();
    match strategy {
        TrimStrategy::MaxLen { threshold, .. } => {
            while stream.len() > threshold {
                if let Some(id) = stream.first_id {
                    stream.entries.remove(&id);
                    stream.first_id = stream.first_entry_id();
                } else {
                    break;
                }
            }
        }
        TrimStrategy::MinId { threshold, .. } => {
            let to_remove: Vec<_> = stream
                .entries
                .range(..threshold)
                .map(|(&id, _)| id)
                .collect();
            for id in to_remove {
                stream.entries.remove(&id);
            }
            stream.first_id = stream.first_entry_id();
        }
    }
    initial_len - stream.len()
}
