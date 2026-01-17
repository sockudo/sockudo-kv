//! Cuckoo Filter storage operations
//!
//! Implements CF.* commands for probabilistic set membership with deletion support.

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::storage::cuckoofilter::{CuckooFilterConfig, ScalableCuckooFilter};
use crate::storage::{DataType, Entry, Store};

/// Cuckoo Filter storage operations
impl Store {
    /// CF.RESERVE - Create a new Cuckoo Filter with specified parameters
    ///
    /// Follows Redis/RedisBloom CF.RESERVE conventions:
    /// - capacity: Number of items the filter is expected to hold
    /// - bucket_size: Number of items per bucket (default: 2 per RedisBloom)
    /// - max_iterations: Maximum cuckoo kicks before giving up (default: 500)
    /// - expansion: Expansion factor for sub-filters when scaling (default: 1)
    pub fn cf_reserve(
        &self,
        key: Bytes,
        capacity: usize,
        bucket_size: Option<usize>,
        max_iterations: Option<usize>,
        expansion: Option<u32>,
    ) -> Result<bool> {
        if self.exists(&key) {
            return Err(Error::Other("ERR item exists"));
        }

        // Use RedisBloom-compatible defaults
        let defaults = CuckooFilterConfig::default();
        let config = CuckooFilterConfig {
            capacity,
            bucket_size: bucket_size.unwrap_or(defaults.bucket_size),
            max_iterations: max_iterations.unwrap_or(defaults.max_iterations),
            expansion: expansion.unwrap_or(defaults.expansion),
            fingerprint_bits: defaults.fingerprint_bits,
        };

        let cf = ScalableCuckooFilter::new(config);
        self.data_insert(key, Entry::new(DataType::CuckooFilter(Box::new(cf))));
        Ok(true)
    }

    /// CF.ADD - Add an item to the Cuckoo Filter
    /// Creates filter with defaults if it doesn't exist
    /// Returns true if added successfully
    pub fn cf_add(&self, key: &Bytes, item: &[u8]) -> Result<bool> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if let Some(cf) = entry.data.as_cuckoofilter_mut() {
                    if cf.add(item) {
                        entry.bump_version();
                        Ok(true)
                    } else {
                        // Filter is full
                        Err(Error::Other("ERR filter is full"))
                    }
                } else {
                    Err(Error::WrongType)
                }
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                let mut cf = ScalableCuckooFilter::new(CuckooFilterConfig::default());
                let added = cf.add(item);
                e.insert((
                    key.clone(),
                    Entry::new(DataType::CuckooFilter(Box::new(cf))),
                ));
                if added {
                    Ok(true)
                } else {
                    Err(Error::Other("ERR filter is full"))
                }
            }
        }
    }

    /// CF.ADDNX - Add an item only if it doesn't already exist
    /// Returns true if added, false if already exists
    pub fn cf_addnx(&self, key: &Bytes, item: &[u8]) -> Result<bool> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if let Some(cf) = entry.data.as_cuckoofilter_mut() {
                    let added = cf.add_nx(item);
                    if added {
                        entry.bump_version();
                    }
                    Ok(added)
                } else {
                    Err(Error::WrongType)
                }
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                let mut cf = ScalableCuckooFilter::new(CuckooFilterConfig::default());
                let added = cf.add_nx(item);
                e.insert((
                    key.clone(),
                    Entry::new(DataType::CuckooFilter(Box::new(cf))),
                ));
                Ok(added)
            }
        }
    }

    /// CF.INSERT - Batch add items with options
    #[allow(clippy::too_many_arguments)]
    pub fn cf_insert(
        &self,
        key: Bytes,
        items: &[&[u8]],
        capacity: Option<usize>,
        nocreate: bool,
    ) -> Result<Option<Vec<i64>>> {
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if let Some(cf) = entry.data.as_cuckoofilter_mut() {
                    let results: Vec<i64> = items
                        .iter()
                        .map(|item| if cf.add(item) { 1 } else { -1 })
                        .collect();
                    entry.bump_version();
                    Ok(Some(results))
                } else {
                    Err(Error::WrongType)
                }
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                if nocreate {
                    return Err(Error::Other("ERR not found"));
                }

                let config = CuckooFilterConfig {
                    capacity: capacity.unwrap_or(1024),
                    ..Default::default()
                };

                let mut cf = ScalableCuckooFilter::new(config);
                let results: Vec<i64> = items
                    .iter()
                    .map(|item| if cf.add(item) { 1 } else { -1 })
                    .collect();
                e.insert((
                    key.clone(),
                    Entry::new(DataType::CuckooFilter(Box::new(cf))),
                ));
                Ok(Some(results))
            }
        }
    }

    /// CF.INSERTNX - Batch add items only if they don't exist
    #[allow(clippy::too_many_arguments)]
    pub fn cf_insertnx(
        &self,
        key: Bytes,
        items: &[&[u8]],
        capacity: Option<usize>,
        nocreate: bool,
    ) -> Result<Option<Vec<i64>>> {
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if let Some(cf) = entry.data.as_cuckoofilter_mut() {
                    let results: Vec<i64> = items
                        .iter()
                        .map(|item| if cf.add_nx(item) { 1 } else { 0 })
                        .collect();
                    if results.iter().any(|&r| r == 1) {
                        entry.bump_version();
                    }
                    Ok(Some(results))
                } else {
                    Err(Error::WrongType)
                }
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                if nocreate {
                    return Err(Error::Other("ERR not found"));
                }

                let config = CuckooFilterConfig {
                    capacity: capacity.unwrap_or(1024),
                    ..Default::default()
                };

                let mut cf = ScalableCuckooFilter::new(config);
                let results: Vec<i64> = items
                    .iter()
                    .map(|item| if cf.add_nx(item) { 1 } else { 0 })
                    .collect();
                e.insert((
                    key.clone(),
                    Entry::new(DataType::CuckooFilter(Box::new(cf))),
                ));
                Ok(Some(results))
            }
        }
    }

    /// CF.EXISTS - Check if an item exists in the Cuckoo Filter
    pub fn cf_exists(&self, key: &[u8], item: &[u8]) -> Result<bool> {
        match self.data_get(key) {
            Some(entry) => {
                if let Some(cf) = entry.1.data.as_cuckoofilter() {
                    Ok(cf.exists(item))
                } else {
                    Err(Error::WrongType)
                }
            }
            None => Ok(false),
        }
    }

    /// CF.MEXISTS - Check if multiple items exist
    pub fn cf_mexists(&self, key: &[u8], items: &[&[u8]]) -> Result<Vec<bool>> {
        match self.data_get(key) {
            Some(entry) => {
                if let Some(cf) = entry.1.data.as_cuckoofilter() {
                    Ok(cf.exists_many(items))
                } else {
                    Err(Error::WrongType)
                }
            }
            None => Ok(vec![false; items.len()]),
        }
    }

    /// CF.COUNT - Returns estimated count of item occurrences
    pub fn cf_count(&self, key: &[u8], item: &[u8]) -> Result<usize> {
        match self.data_get(key) {
            Some(entry) => {
                if let Some(cf) = entry.1.data.as_cuckoofilter() {
                    Ok(cf.count_item(item))
                } else {
                    Err(Error::WrongType)
                }
            }
            None => Ok(0),
        }
    }

    /// CF.DEL - Delete one occurrence of an item
    pub fn cf_del(&self, key: &Bytes, item: &[u8]) -> Result<bool> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if let Some(cf) = entry.data.as_cuckoofilter_mut() {
                    let deleted = cf.delete(item);
                    if deleted {
                        entry.bump_version();
                    }
                    Ok(deleted)
                } else {
                    Err(Error::WrongType)
                }
            }
            crate::storage::dashtable::Entry::Vacant(_) => Ok(false),
        }
    }

    /// CF.INFO - Get information about the Cuckoo Filter
    /// Returns: (capacity, size_bytes, num_buckets, num_filters, num_items, bucket_size, expansion, max_iterations)
    pub fn cf_info(
        &self,
        key: &[u8],
    ) -> Result<Option<(usize, usize, usize, usize, usize, usize, u32, usize)>> {
        match self.data_get(key) {
            Some(entry) => {
                if let Some(cf) = entry.1.data.as_cuckoofilter() {
                    Ok(Some((
                        cf.capacity(),
                        cf.size_bytes(),
                        cf.total_buckets(),
                        cf.num_filters(),
                        cf.len(),
                        cf.bucket_size(),
                        cf.expansion(),
                        cf.max_iterations(),
                    )))
                } else {
                    Err(Error::WrongType)
                }
            }
            None => Err(Error::Other("ERR not found")),
        }
    }

    /// CF.SCANDUMP - Get serialized data for migration
    pub fn cf_scandump(&self, key: &[u8], iterator: usize) -> Result<Option<(usize, Vec<u8>)>> {
        if iterator != 0 {
            // We return everything at once for simplicity
            return Ok(None);
        }

        match self.data_get(key) {
            Some(entry) => {
                if let Some(cf) = entry.1.data.as_cuckoofilter() {
                    let data = cf.to_bytes();
                    Ok(Some((0, data)))
                } else {
                    Err(Error::WrongType)
                }
            }
            None => Err(Error::Other("ERR not found")),
        }
    }

    /// CF.LOADCHUNK - Load serialized data
    pub fn cf_loadchunk(&self, key: Bytes, _iterator: usize, data: &[u8]) -> Result<()> {
        let cf = ScalableCuckooFilter::from_bytes(data)
            .ok_or_else(|| Error::Other("ERR invalid data"))?;

        self.data_insert(key, Entry::new(DataType::CuckooFilter(Box::new(cf))));
        Ok(())
    }
}
