//! Top-K storage operations
//!
//! Implements TOPK.* commands for heavy hitters tracking.

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::storage::topk::{TopK, TopKConfig};
use crate::storage::{DataType, Entry, Store};

/// Top-K storage operations
impl Store {
    /// TOPK.RESERVE - Create a new Top-K sketch
    pub fn topk_reserve(
        &self,
        key: Bytes,
        topk: usize,
        width: Option<usize>,
        depth: Option<usize>,
        decay: Option<f64>,
    ) -> Result<bool> {
        if self.exists(&key) {
            return Err(Error::Other("ERR item exists"));
        }

        let config = TopKConfig {
            k: topk,
            width: width.unwrap_or(8),
            depth: depth.unwrap_or(7),
            decay: decay.unwrap_or(0.9),
        };

        let tk = TopK::new(config);
        self.data_insert(key, Entry::new(DataType::TopK(Box::new(tk))));
        Ok(true)
    }

    /// TOPK.ADD - Add one or more items to the Top-K
    /// Returns a list of evicted items (null if not evicted)
    pub fn topk_add(&self, key: &Bytes, items: &[&[u8]]) -> Result<Vec<Option<Bytes>>> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if let Some(tk) = entry.data.as_topk_mut() {
                    let results: Vec<Option<Bytes>> =
                        items.iter().map(|item| tk.add(item)).collect();
                    Ok(results)
                } else {
                    Err(Error::WrongType)
                }
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                // Auto-create with default k=10
                let mut tk = TopK::with_k(10);
                let results: Vec<Option<Bytes>> = items.iter().map(|item| tk.add(item)).collect();
                e.insert((key.clone(), Entry::new(DataType::TopK(Box::new(tk)))));
                Ok(results)
            }
        }
    }

    /// TOPK.INCRBY - Increment the count of one or more items
    /// Returns a list of evicted items (null if not evicted)
    pub fn topk_incrby(&self, key: &Bytes, items: &[(&[u8], u64)]) -> Result<Vec<Option<Bytes>>> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                if let Some(tk) = entry.data.as_topk_mut() {
                    let results: Vec<Option<Bytes>> = items
                        .iter()
                        .map(|(item, incr)| tk.incrby(item, *incr))
                        .collect();
                    Ok(results)
                } else {
                    Err(Error::WrongType)
                }
            }
            crate::storage::dashtable::Entry::Vacant(e) => {
                // Auto-create with default k=10
                let mut tk = TopK::with_k(10);
                let results: Vec<Option<Bytes>> = items
                    .iter()
                    .map(|(item, incr)| tk.incrby(item, *incr))
                    .collect();
                e.insert((key.clone(), Entry::new(DataType::TopK(Box::new(tk)))));
                Ok(results)
            }
        }
    }

    /// TOPK.QUERY - Check if one or more items are in the Top-K list
    pub fn topk_query(&self, key: &[u8], items: &[&[u8]]) -> Result<Vec<bool>> {
        match self.data_get(key) {
            Some(entry) => {
                if let Some(tk) = entry.1.data.as_topk() {
                    Ok(items.iter().map(|item| tk.query(item)).collect())
                } else {
                    Err(Error::WrongType)
                }
            }
            None => Ok(vec![false; items.len()]),
        }
    }

    /// TOPK.COUNT - Get estimated counts for one or more items
    pub fn topk_count(&self, key: &[u8], items: &[&[u8]]) -> Result<Vec<u64>> {
        match self.data_get(key) {
            Some(entry) => {
                if let Some(tk) = entry.1.data.as_topk() {
                    Ok(items.iter().map(|item| tk.count(item)).collect())
                } else {
                    Err(Error::WrongType)
                }
            }
            None => Ok(vec![0; items.len()]),
        }
    }

    /// TOPK.LIST - Get the full list of items in the Top-K
    /// If with_count is true, returns (item, count) pairs
    pub fn topk_list(&self, key: &[u8], with_count: bool) -> Result<TopKListResult> {
        match self.data_get(key) {
            Some(entry) => {
                if let Some(tk) = entry.1.data.as_topk() {
                    if with_count {
                        Ok(TopKListResult::WithCount(tk.list_with_count()))
                    } else {
                        Ok(TopKListResult::Items(tk.list()))
                    }
                } else {
                    Err(Error::WrongType)
                }
            }
            None => Err(Error::Other("ERR not found")),
        }
    }

    /// TOPK.INFO - Get information about the Top-K
    pub fn topk_info(&self, key: &[u8]) -> Result<Option<(usize, usize, usize, f64)>> {
        match self.data_get(key) {
            Some(entry) => {
                if let Some(tk) = entry.1.data.as_topk() {
                    let info = tk.info();
                    Ok(Some((info.k, info.width, info.depth, info.decay)))
                } else {
                    Err(Error::WrongType)
                }
            }
            None => Err(Error::Other("ERR not found")),
        }
    }
}

/// Result type for TOPK.LIST
#[derive(Debug)]
pub enum TopKListResult {
    Items(Vec<Bytes>),
    WithCount(Vec<(Bytes, u64)>),
}
