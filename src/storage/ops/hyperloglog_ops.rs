use bytes::Bytes;
use dashmap::mapref::entry::Entry as DashEntry;

use crate::error::{Error, Result};
use crate::storage::Store;
use crate::storage::types::{DataType, Entry, HyperLogLogData};

use std::sync::atomic::Ordering;

/// HyperLogLog operations for the Store
impl Store {
    // ==================== HyperLogLog operations ====================

    /// Add elements to HyperLogLog
    #[inline]
    pub fn pfadd(&self, key: Bytes, elements: &[Bytes]) -> Result<bool> {
        match self.data.entry(key) {
            DashEntry::Occupied(mut e) => {
                let entry = e.get_mut();
                if entry.is_expired() {
                    let mut hll = HyperLogLogData::new();
                    let mut changed = false;
                    for element in elements {
                        if hll.add(element) {
                            changed = true;
                        }
                    }
                    entry.data = DataType::HyperLogLog(hll);
                    entry.persist();
                    entry.bump_version();
                    return Ok(changed);
                }

                let result = match &mut entry.data {
                    DataType::HyperLogLog(hll) => {
                        let mut changed = false;
                        for element in elements {
                            if hll.add(element) {
                                changed = true;
                            }
                        }
                        Ok(changed)
                    }
                    _ => Err(Error::WrongType),
                };
                if let Ok(true) = result {
                    entry.bump_version();
                }
                result
            }
            DashEntry::Vacant(e) => {
                let mut hll = HyperLogLogData::new();
                let mut changed = false;
                for element in elements {
                    if hll.add(element) {
                        changed = true;
                    }
                }
                e.insert(Entry::new(DataType::HyperLogLog(hll)));
                self.key_count.fetch_add(1, Ordering::Relaxed);
                Ok(changed)
            }
        }
    }

    /// Get cardinality estimate from HyperLogLog
    #[inline]
    pub fn pfcount(&self, keys: &[Bytes]) -> u64 {
        if keys.is_empty() {
            return 0;
        }

        if keys.len() == 1 {
            // Single key - simple case
            match self.data.get(keys[0].as_ref()) {
                Some(e) => {
                    if e.is_expired() {
                        return 0;
                    }
                    match e.data.as_hyperloglog() {
                        Some(hll) => hll.count(),
                        None => 0,
                    }
                }
                None => 0,
            }
        } else {
            // Multiple keys - merge HLLs
            let mut merged = HyperLogLogData::new();
            for key in keys {
                if let Some(e) = self.data.get(key.as_ref())
                    && !e.is_expired()
                {
                    match e.data.as_hyperloglog() {
                        Some(hll) => merged.merge(hll),
                        None => return 0,
                    }
                }
            }
            merged.count()
        }
    }

    /// Merge HyperLogLogs into destination key
    #[inline]
    pub fn pfmerge(&self, dest: Bytes, sources: &[Bytes]) -> Result<()> {
        let mut merged = HyperLogLogData::new();

        for source in sources {
            if let Some(e) = self.data.get(source.as_ref())
                && !e.is_expired()
            {
                match e.data.as_hyperloglog() {
                    Some(hll) => merged.merge(hll),
                    None => return Err(Error::WrongType),
                }
            }
        }

        // Store merged HLL in destination
        match self.data.entry(dest) {
            DashEntry::Occupied(mut e) => {
                let entry = e.get_mut();
                entry.data = DataType::HyperLogLog(merged);
                entry.persist();
                entry.bump_version();
            }
            DashEntry::Vacant(e) => {
                e.insert(Entry::new(DataType::HyperLogLog(merged)));
                self.key_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        Ok(())
    }
}
