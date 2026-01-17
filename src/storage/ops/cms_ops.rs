//! Count-Min Sketch storage operations
//!
//! Implements CMS.* commands for frequency estimation.

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::storage::cms::CountMinSketch;
use crate::storage::{DataType, Entry, Store};

/// Count-Min Sketch storage operations
impl Store {
    /// CMS.INITBYDIM - Create a CMS by specifying dimensions
    pub fn cms_initbydim(&self, key: Bytes, width: usize, depth: usize) -> Result<bool> {
        if self.exists(&key) {
            return Err(Error::Other("ERR item exists"));
        }

        let cms = CountMinSketch::new(width, depth);
        self.data_insert(key, Entry::new(DataType::CountMinSketch(Box::new(cms))));
        Ok(true)
    }

    /// CMS.INITBYPROB - Create a CMS by specifying error rate and probability
    pub fn cms_initbyprob(&self, key: Bytes, error: f64, probability: f64) -> Result<bool> {
        if self.exists(&key) {
            return Err(Error::Other("ERR item exists"));
        }

        if error <= 0.0 || error >= 1.0 {
            return Err(Error::Other("ERR error must be between 0 and 1 exclusive"));
        }
        if probability <= 0.0 || probability >= 1.0 {
            return Err(Error::Other(
                "ERR probability must be between 0 and 1 exclusive",
            ));
        }

        let cms = CountMinSketch::from_error_prob(error, probability);
        self.data_insert(key, Entry::new(DataType::CountMinSketch(Box::new(cms))));
        Ok(true)
    }

    /// CMS.INCRBY - Increment the count of one or more items
    /// Takes pairs of (item, increment)
    pub fn cms_incrby(&self, key: &Bytes, items: &[(&[u8], u64)]) -> Result<Vec<u64>> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if let Some(cms) = entry.data.as_cms_mut() {
                    let results: Vec<u64> = items
                        .iter()
                        .map(|(item, incr)| {
                            cms.incrby(item, *incr);
                            cms.query(item)
                        })
                        .collect();
                    Ok(results)
                } else {
                    Err(Error::WrongType)
                }
            }
            crate::storage::dashtable::Entry::Vacant(_) => Err(Error::Other("ERR not found")),
        }
    }

    /// CMS.QUERY - Get estimated counts for one or more items
    pub fn cms_query(&self, key: &[u8], items: &[&[u8]]) -> Result<Vec<u64>> {
        match self.data_get(key) {
            Some(entry) => {
                if let Some(cms) = entry.1.data.as_cms() {
                    Ok(items.iter().map(|item| cms.query(item)).collect())
                } else {
                    Err(Error::WrongType)
                }
            }
            None => Ok(vec![0; items.len()]),
        }
    }

    /// CMS.MERGE - Merge multiple CMS sketches into a destination
    pub fn cms_merge(
        &self,
        dest_key: Bytes,
        source_keys: &[Bytes],
        weights: Option<&[u64]>,
    ) -> Result<()> {
        // Collect source CMS sketches
        let mut sources: Vec<(CountMinSketch, u64)> = Vec::with_capacity(source_keys.len());

        for (i, key) in source_keys.iter().enumerate() {
            match self.data_get(key.as_ref()) {
                Some(entry) => {
                    if let Some(cms) = entry.1.data.as_cms() {
                        let weight = weights.map(|w| w.get(i).copied().unwrap_or(1)).unwrap_or(1);
                        sources.push((cms.clone(), weight));
                    } else {
                        return Err(Error::WrongType);
                    }
                }
                None => {
                    return Err(Error::Other("ERR source key not found"));
                }
            }
        }

        if sources.is_empty() {
            return Err(Error::Other("ERR at least one source key required"));
        }

        // Get or create destination
        // Use dimensions from first source
        let (first_width, first_depth) = {
            let first = &sources[0].0;
            (first.width(), first.depth())
        };

        // Check all sources have same dimensions
        for (cms, _) in &sources {
            if cms.width() != first_width || cms.depth() != first_depth {
                return Err(Error::Other("ERR CMS dimensions must match for merge"));
            }
        }

        // Create or get destination
        match self.data_entry(&dest_key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if let Some(dest_cms) = entry.data.as_cms_mut() {
                    // Check dimensions match
                    if dest_cms.width() != first_width || dest_cms.depth() != first_depth {
                        return Err(Error::Other("ERR CMS dimensions must match for merge"));
                    }
                    // Reset and merge
                    dest_cms.reset();
                    for (src, weight) in &sources {
                        dest_cms.merge(src, *weight);
                    }
                    Ok(())
                } else {
                    Err(Error::WrongType)
                }
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                // Create new CMS with same dimensions
                let mut dest_cms = CountMinSketch::new(first_width, first_depth);
                for (src, weight) in &sources {
                    dest_cms.merge(src, *weight);
                }
                e.insert((
                    dest_key.clone(),
                    Entry::new(DataType::CountMinSketch(Box::new(dest_cms))),
                ));
                Ok(())
            }
        }
    }

    /// CMS.INFO - Get information about the CMS
    /// Returns (width, depth, count)
    pub fn cms_info(&self, key: &[u8]) -> Result<Option<(usize, usize, u64)>> {
        match self.data_get(key) {
            Some(entry) => {
                if let Some(cms) = entry.1.data.as_cms() {
                    let info = cms.info();
                    Ok(Some((info.width, info.depth, info.count)))
                } else {
                    Err(Error::WrongType)
                }
            }
            None => Err(Error::Other("ERR not found")),
        }
    }
}
