use bytes::Bytes;

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
        match self.data_entry(&key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
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
            crate::storage::dashtable::Entry::Vacant(e) => {
                let mut hll = HyperLogLogData::new();
                let mut changed = false;
                for element in elements {
                    if hll.add(element) {
                        changed = true;
                    }
                }
                e.insert((key, Entry::new(DataType::HyperLogLog(hll))));
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
            match self.data_get(keys[0].as_ref()) {
                Some(entry_ref) => {
                    if entry_ref.1.is_expired() {
                        return 0;
                    }
                    match entry_ref.1.data.as_hyperloglog() {
                        Some(hll) => hll.count(),
                        None => 0,
                    }
                }
                None => 0,
            }
        } else {
            let mut merged = HyperLogLogData::new();
            for key in keys {
                if let Some(entry_ref) = self.data_get(key.as_ref())
                    && !entry_ref.1.is_expired()
                {
                    match entry_ref.1.data.as_hyperloglog() {
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
            if let Some(entry_ref) = self.data_get(source.as_ref())
                && !entry_ref.1.is_expired()
            {
                match entry_ref.1.data.as_hyperloglog() {
                    Some(hll) => merged.merge(hll),
                    None => return Err(Error::WrongType),
                }
            }
        }

        match self.data_entry(&dest) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                entry.data = DataType::HyperLogLog(merged);
                entry.persist();
                entry.bump_version();
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                e.insert((dest, Entry::new(DataType::HyperLogLog(merged))));
                self.key_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        Ok(())
    }

    /// Get HyperLogLog registers (for PFDEBUG GETREG)
    #[inline]
    pub fn pf_get_registers(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return Ok(None);
                }
                match entry_ref.1.data.as_hyperloglog() {
                    Some(hll) => Ok(Some(hll.get_registers())),
                    None => Err(Error::WrongType),
                }
            }
            None => Ok(None),
        }
    }

    /// Check if HyperLogLog uses sparse encoding (for PFDEBUG)
    #[inline]
    pub fn pf_is_sparse(&self, key: &[u8]) -> Result<Option<bool>> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return Ok(None);
                }
                match entry_ref.1.data.as_hyperloglog() {
                    Some(hll) => Ok(Some(hll.is_sparse())),
                    None => Err(Error::WrongType),
                }
            }
            None => Ok(None),
        }
    }

    /// Get HyperLogLog encoding name (for PFDEBUG ENCODING)
    #[inline]
    pub fn pf_encoding(&self, key: &[u8]) -> Result<Option<&'static str>> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return Ok(None);
                }
                match entry_ref.1.data.as_hyperloglog() {
                    Some(hll) => Ok(Some(hll.encoding_name())),
                    None => Err(Error::WrongType),
                }
            }
            None => Ok(None),
        }
    }

    /// Promote HyperLogLog to dense encoding (for PFDEBUG TODENSE)
    #[inline]
    pub fn pf_to_dense(&self, key: &[u8]) -> Result<Option<bool>> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if entry.is_expired() {
                    return Ok(None);
                }
                match &mut entry.data {
                    DataType::HyperLogLog(hll) => {
                        let was_sparse = hll.is_sparse();
                        hll.promote_to_dense();
                        Ok(Some(was_sparse))
                    }
                    _ => Err(Error::WrongType),
                }
            }
            crate::storage::dashtable::Entry::Vacant(_) => Ok(None),
        }
    }

    /// Get sparse decode string (for PFDEBUG DECODE)
    #[inline]
    pub fn pf_decode_sparse(&self, key: &[u8]) -> Result<Option<Option<String>>> {
        match self.data_get(key) {
            Some(entry_ref) => {
                if entry_ref.1.is_expired() {
                    return Ok(None);
                }
                match entry_ref.1.data.as_hyperloglog() {
                    Some(hll) => Ok(Some(hll.decode_sparse())),
                    None => Err(Error::WrongType),
                }
            }
            None => Ok(None),
        }
    }
}
