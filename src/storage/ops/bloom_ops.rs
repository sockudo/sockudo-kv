use bytes::Bytes;

use crate::error::{Error, Result};
use crate::storage::bloomfilter::{BloomFilterConfig, ScalableBloomFilter};
use crate::storage::{DataType, Entry, Store};

/// Bloom Filter storage operations
impl Store {
    /// Create a new Bloom Filter with specified parameters
    pub fn bf_reserve(
        &self,
        key: Bytes,
        error_rate: f64,
        capacity: usize,
        expansion: u32,
        nonscaling: bool,
    ) -> Result<bool> {
        if self.exists(&key) {
            return Err(Error::Other("ERR item exists"));
        }

        let config = BloomFilterConfig {
            error_rate,
            capacity,
            expansion,
            nonscaling,
        };

        let bf = ScalableBloomFilter::new(config);
        self.data_insert(key, Entry::new(DataType::BloomFilter(Box::new(bf))));
        Ok(true)
    }

    /// Add an item to the Bloom Filter
    /// Returns true if the item was added (not present), false if it was already present
    pub fn bf_add(&self, key: &Bytes, item: &[u8]) -> Result<bool> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if let Some(bf) = entry.data.as_bloomfilter_mut() {
                    Ok(bf.add(item))
                } else {
                    Err(Error::WrongType)
                }
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                // Auto-create with defaults
                let mut bf = ScalableBloomFilter::new(BloomFilterConfig::default());
                let added = bf.add(item);
                e.insert((key.clone(), Entry::new(DataType::BloomFilter(Box::new(bf)))));
                Ok(added)
            }
        }
    }

    /// Add multiple items to the Bloom Filter
    /// Returns a list of booleans indicating if each item was added
    pub fn bf_madd(&self, key: &Bytes, items: &[&[u8]]) -> Result<Vec<bool>> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if let Some(bf) = entry.data.as_bloomfilter_mut() {
                    Ok(items.iter().map(|item| bf.add(item)).collect())
                } else {
                    Err(Error::WrongType)
                }
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                // Auto-create with defaults
                let mut bf = ScalableBloomFilter::new(BloomFilterConfig::default());
                let results = items.iter().map(|item| bf.add(item)).collect();
                e.insert((key.clone(), Entry::new(DataType::BloomFilter(Box::new(bf)))));
                Ok(results)
            }
        }
    }

    /// Check if an item exists in the Bloom Filter
    pub fn bf_exists(&self, key: &[u8], item: &[u8]) -> Result<bool> {
        match self.data_get(key) {
            Some(entry) => {
                if let Some(bf) = entry.1.data.as_bloomfilter() {
                    Ok(bf.exists(item))
                } else {
                    Err(Error::WrongType)
                }
            }
            None => Ok(false),
        }
    }

    /// Check if multiple items exist in the Bloom Filter
    pub fn bf_mexists(&self, key: &[u8], items: &[&[u8]]) -> Result<Vec<bool>> {
        match self.data_get(key) {
            Some(entry) => {
                if let Some(bf) = entry.1.data.as_bloomfilter() {
                    Ok(bf.exists_many(items))
                } else {
                    Err(Error::WrongType)
                }
            }
            None => Ok(vec![false; items.len()]),
        }
    }

    /// Get information about the Bloom Filter
    pub fn bf_info(&self, key: &[u8]) -> Result<Option<(usize, usize, usize, usize, u32)>> {
        match self.data_get(key) {
            Some(entry) => {
                if let Some(bf) = entry.1.data.as_bloomfilter() {
                    Ok(Some((
                        bf.capacity(),
                        bf.size_bytes(),
                        bf.num_filters(),
                        bf.len(),
                        bf.expansion(),
                    )))
                } else {
                    Err(Error::WrongType)
                }
            }
            None => Err(Error::Other("ERR item not found")),
        }
    }

    /// Estimate the cardinality of the Bloom Filter
    pub fn bf_card(&self, key: &[u8]) -> Result<usize> {
        match self.data_get(key) {
            Some(entry) => {
                if let Some(bf) = entry.1.data.as_bloomfilter() {
                    Ok(bf.estimate_cardinality())
                } else {
                    Err(Error::WrongType)
                }
            }
            None => Ok(0),
        }
    }

    /// Insert items with custom configuration
    #[allow(clippy::too_many_arguments)]
    pub fn bf_insert(
        &self,
        key: Bytes,
        items: &[&[u8]],
        capacity: Option<usize>,
        error_rate: Option<f64>,
        expansion: Option<u32>,
        nonscaling: Option<bool>,
        nocreate: bool,
    ) -> Result<Option<Vec<bool>>> {
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if let Some(bf) = entry.data.as_bloomfilter_mut() {
                    Ok(Some(items.iter().map(|item| bf.add(item)).collect()))
                } else {
                    Err(Error::WrongType)
                }
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                if nocreate {
                    return Err(Error::Other("ERR not found"));
                }

                let config = BloomFilterConfig {
                    capacity: capacity.unwrap_or(100),
                    error_rate: error_rate.unwrap_or(0.01),
                    expansion: expansion.unwrap_or(2),
                    nonscaling: nonscaling.unwrap_or(false),
                };

                let mut bf = ScalableBloomFilter::new(config);
                let results = items.iter().map(|item| bf.add(item)).collect();
                e.insert((key.clone(), Entry::new(DataType::BloomFilter(Box::new(bf)))));
                Ok(Some(results))
            }
        }
    }

    /// Get serialized data for SCANDUMP
    pub fn bf_scandump(&self, key: &[u8], iterator: usize) -> Result<Option<(usize, Vec<u8>)>> {
        if iterator != 0 {
            // We don't support partial iteration for now, simpler to just return everything at once
            // Redis does chunking, but for now we'll do 0 = start, 1 = done
            return Ok(None);
        }

        match self.data_get(key) {
            Some(entry) => {
                if let Some(bf) = entry.1.data.as_bloomfilter() {
                    let data = bf.to_bytes();
                    // Return (iterator=1, data) to signal we have more... wait.
                    // Redis SCANDUMP usage:
                    // 1. call with iter=0
                    // 2. returns (next_iter, data)
                    // 3. call with next_iter
                    // 4. if next_iter=0, done

                    // Since we return everything at once:
                    // return (0, data) acts as "here is data, and we are done"
                    Ok(Some((0, data)))
                } else {
                    Err(Error::WrongType)
                }
            }
            None => Err(Error::Other("ERR item not found")),
        }
    }

    /// Load chunk for LOADCHUNK
    pub fn bf_loadchunk(&self, key: Bytes, _iterator: usize, data: &[u8]) -> Result<()> {
        let bf = ScalableBloomFilter::from_bytes(data)
            .ok_or_else(|| Error::Other("ERR invalid data"))?;

        self.data_insert(key, Entry::new(DataType::BloomFilter(Box::new(bf))));
        Ok(())
    }
}
