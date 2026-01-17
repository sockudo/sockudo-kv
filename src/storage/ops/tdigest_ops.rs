//! T-Digest storage operations
//!
//! Implements all TDIGEST.* commands for quantile estimation.

use crate::storage::Store;
use crate::storage::tdigest::TDigest;
use crate::storage::types::DataType;
use bytes::Bytes;

impl Store {
    /// TDIGEST.CREATE - Create a new T-Digest sketch
    /// Returns true if created, false if already exists
    pub fn tdigest_create(&self, key: &Bytes, compression: f64) -> Result<bool, &'static str> {
        // Check if key exists
        if self.exists(key) {
            // Check if it's a TDigest
            if let Some(entry) = self.data_get(key.as_ref()) {
                if entry.1.data.as_tdigest().is_some() {
                    return Ok(false); // Already exists as TDigest
                }
                return Err("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
        }

        let td = TDigest::new(compression);
        let entry = crate::storage::Entry::new(DataType::TDigest(Box::new(td)));
        self.data_insert(key.clone(), entry);
        Ok(true)
    }

    /// TDIGEST.ADD - Add observations to T-Digest
    /// Returns OK on success
    pub fn tdigest_add(&self, key: &Bytes, values: &[f64]) -> Result<(), &'static str> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                let td = entry
                    .data
                    .as_tdigest_mut()
                    .ok_or("WRONGTYPE Operation against a key holding the wrong kind of value")?;

                td.add_many(values);
                entry.bump_version();
                Ok(())
            }
            crate::storage::dashtable::Entry::Vacant(_) => Err("ERR T-Digest: key does not exist"),
        }
    }

    /// TDIGEST.MERGE - Merge multiple T-Digests into destination
    /// If destination exists and override is false, merge into it
    /// If override is true or destination doesn't exist, create new
    pub fn tdigest_merge(
        &self,
        dest_key: &Bytes,
        source_keys: &[Bytes],
        compression: Option<f64>,
        override_dest: bool,
    ) -> Result<(), &'static str> {
        // Collect source T-Digests
        let mut sources = Vec::new();
        for key in source_keys {
            if let Some(entry) = self.data_get(key.as_ref()) {
                if let Some(td) = entry.1.data.as_tdigest() {
                    sources.push(td.clone());
                } else {
                    return Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                }
            }
            // Skip non-existent keys
        }

        if sources.is_empty() {
            return Err("ERR T-Digest: at least one source key must exist");
        }

        // Determine compression
        let comp = compression.unwrap_or_else(|| {
            sources
                .iter()
                .map(|td| td.compression())
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap_or(100.0)
        });

        // Get or create destination
        let create_new = override_dest
            || !self.exists(dest_key)
            || self
                .data_get(dest_key.as_ref())
                .map(|e| e.1.data.as_tdigest().is_none())
                .unwrap_or(true);

        if create_new {
            let mut td = TDigest::new(comp);
            for source in &sources {
                td.merge(source);
            }
            let entry = crate::storage::Entry::new(DataType::TDigest(Box::new(td)));
            self.data_insert(dest_key.clone(), entry);
        } else {
            match self.data_entry(dest_key) {
                crate::storage::dashtable::Entry::Occupied(mut e) => {
                    let entry = &mut e.get_mut().1;
                    let td = entry.data.as_tdigest_mut().unwrap();
                    for source in &sources {
                        td.merge(source);
                    }
                    entry.bump_version();
                }
                crate::storage::dashtable::Entry::Vacant(_) => {}
            }
        }

        Ok(())
    }

    /// TDIGEST.RESET - Reset a T-Digest to empty state
    pub fn tdigest_reset(&self, key: &Bytes) -> Result<(), &'static str> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                let td = entry
                    .data
                    .as_tdigest_mut()
                    .ok_or("WRONGTYPE Operation against a key holding the wrong kind of value")?;

                td.reset();
                entry.bump_version();
                Ok(())
            }
            crate::storage::dashtable::Entry::Vacant(_) => Err("ERR T-Digest: key does not exist"),
        }
    }

    /// TDIGEST.QUANTILE - Get values at specified quantiles
    pub fn tdigest_quantile(
        &self,
        key: &Bytes,
        quantiles: &[f64],
    ) -> Result<Vec<f64>, &'static str> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                let td = entry
                    .data
                    .as_tdigest_mut()
                    .ok_or("WRONGTYPE Operation against a key holding the wrong kind of value")?;

                Ok(td.quantiles(quantiles))
            }
            crate::storage::dashtable::Entry::Vacant(_) => Err("ERR T-Digest: key does not exist"),
        }
    }

    /// TDIGEST.CDF - Get cumulative distribution function values
    pub fn tdigest_cdf(&self, key: &Bytes, values: &[f64]) -> Result<Vec<f64>, &'static str> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                let td = entry
                    .data
                    .as_tdigest_mut()
                    .ok_or("WRONGTYPE Operation against a key holding the wrong kind of value")?;

                Ok(td.cdfs(values))
            }
            crate::storage::dashtable::Entry::Vacant(_) => Err("ERR T-Digest: key does not exist"),
        }
    }

    /// TDIGEST.RANK - Get rank of values
    pub fn tdigest_rank(&self, key: &Bytes, values: &[f64]) -> Result<Vec<i64>, &'static str> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                let td = entry
                    .data
                    .as_tdigest_mut()
                    .ok_or("WRONGTYPE Operation against a key holding the wrong kind of value")?;

                Ok(td.ranks(values))
            }
            crate::storage::dashtable::Entry::Vacant(_) => Err("ERR T-Digest: key does not exist"),
        }
    }

    /// TDIGEST.REVRANK - Get reverse rank of values
    pub fn tdigest_revrank(&self, key: &Bytes, values: &[f64]) -> Result<Vec<i64>, &'static str> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                let td = entry
                    .data
                    .as_tdigest_mut()
                    .ok_or("WRONGTYPE Operation against a key holding the wrong kind of value")?;

                Ok(td.rev_ranks(values))
            }
            crate::storage::dashtable::Entry::Vacant(_) => Err("ERR T-Digest: key does not exist"),
        }
    }

    /// TDIGEST.BYRANK - Get values at specified ranks
    pub fn tdigest_byrank(&self, key: &Bytes, ranks: &[i64]) -> Result<Vec<f64>, &'static str> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                let td = entry
                    .data
                    .as_tdigest_mut()
                    .ok_or("WRONGTYPE Operation against a key holding the wrong kind of value")?;

                Ok(td.by_ranks(ranks))
            }
            crate::storage::dashtable::Entry::Vacant(_) => Err("ERR T-Digest: key does not exist"),
        }
    }

    /// TDIGEST.BYREVRANK - Get values at specified reverse ranks
    pub fn tdigest_byrevrank(&self, key: &Bytes, ranks: &[i64]) -> Result<Vec<f64>, &'static str> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                let td = entry
                    .data
                    .as_tdigest_mut()
                    .ok_or("WRONGTYPE Operation against a key holding the wrong kind of value")?;

                Ok(td.by_rev_ranks(ranks))
            }
            crate::storage::dashtable::Entry::Vacant(_) => Err("ERR T-Digest: key does not exist"),
        }
    }

    /// TDIGEST.MIN - Get minimum value
    pub fn tdigest_min(&self, key: &Bytes) -> Result<f64, &'static str> {
        if let Some(entry) = self.data_get(key.as_ref()) {
            let td = entry
                .1
                .data
                .as_tdigest()
                .ok_or("WRONGTYPE Operation against a key holding the wrong kind of value")?;

            Ok(td.min())
        } else {
            Err("ERR T-Digest: key does not exist")
        }
    }

    /// TDIGEST.MAX - Get maximum value
    pub fn tdigest_max(&self, key: &Bytes) -> Result<f64, &'static str> {
        if let Some(entry) = self.data_get(key.as_ref()) {
            let td = entry
                .1
                .data
                .as_tdigest()
                .ok_or("WRONGTYPE Operation against a key holding the wrong kind of value")?;

            Ok(td.max())
        } else {
            Err("ERR T-Digest: key does not exist")
        }
    }

    /// TDIGEST.TRIMMED_MEAN - Get trimmed mean between quantiles
    pub fn tdigest_trimmed_mean(
        &self,
        key: &Bytes,
        low_quantile: f64,
        high_quantile: f64,
    ) -> Result<f64, &'static str> {
        match self.data_entry(key) {
            crate::storage::dashtable::Entry::Occupied(mut e) => {
                let entry = &mut e.get_mut().1;
                let td = entry
                    .data
                    .as_tdigest_mut()
                    .ok_or("WRONGTYPE Operation against a key holding the wrong kind of value")?;

                Ok(td.trimmed_mean(low_quantile, high_quantile))
            }
            crate::storage::dashtable::Entry::Vacant(_) => Err("ERR T-Digest: key does not exist"),
        }
    }

    /// TDIGEST.INFO - Get information about a T-Digest
    pub fn tdigest_info(&self, key: &Bytes) -> Result<TDigestInfo, &'static str> {
        if let Some(entry) = self.data_get(key.as_ref()) {
            let td = entry
                .1
                .data
                .as_tdigest()
                .ok_or("WRONGTYPE Operation against a key holding the wrong kind of value")?;

            Ok(TDigestInfo {
                compression: td.compression(),
                capacity: td.capacity(),
                merged_nodes: td.num_centroids(),
                unmerged_nodes: td.num_unmerged(),
                merged_weight: td.merged_weight(),
                unmerged_weight: td.unmerged_weight(),
                total_compressions: td.total_compressions(),
                memory_usage: td.memory_usage(),
            })
        } else {
            Err("ERR T-Digest: key does not exist")
        }
    }
}

/// Information about a T-Digest
#[derive(Debug, Clone)]
pub struct TDigestInfo {
    pub compression: f64,
    pub capacity: usize,
    pub merged_nodes: usize,
    pub unmerged_nodes: usize,
    pub merged_weight: f64,
    pub unmerged_weight: f64,
    pub total_compressions: u64,
    pub memory_usage: usize,
}
