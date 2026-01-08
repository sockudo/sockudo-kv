//! Multi-database storage wrapper
//!
//! Provides Redis-compatible multiple database support (default 16 databases).
//! Uses Arc pointer swapping for O(1) SWAPDB - same technique as Dragonfly.

use super::Store;
use parking_lot::RwLock;
use std::sync::Arc;

/// Default number of databases (like Redis)
pub const DEFAULT_DB_COUNT: usize = 16;

/// Multi-database store wrapper
/// Uses RwLock<Vec<Arc<Store>>> for O(1) SWAPDB via Arc pointer swapping
pub struct MultiStore {
    /// Array of databases - wrapped in RwLock for atomic SWAPDB
    databases: RwLock<Vec<Arc<Store>>>,
    /// Number of databases (immutable after creation)
    db_count: usize,
}

impl MultiStore {
    /// Create new multi-store with default 16 databases
    #[inline]
    pub fn new() -> Self {
        Self::with_db_count(DEFAULT_DB_COUNT)
    }

    /// Create new multi-store with specified database count
    pub fn with_db_count(db_count: usize) -> Self {
        let databases: Vec<Arc<Store>> = (0..db_count).map(|_| Arc::new(Store::new())).collect();

        Self {
            databases: RwLock::new(databases),
            db_count,
        }
    }

    /// Create with capacity per database
    pub fn with_capacity(capacity_per_db: usize) -> Self {
        Self::with_capacity_and_count(capacity_per_db, DEFAULT_DB_COUNT)
    }

    /// Create with capacity and database count
    pub fn with_capacity_and_count(capacity_per_db: usize, db_count: usize) -> Self {
        let databases: Vec<Arc<Store>> = (0..db_count)
            .map(|_| Arc::new(Store::with_capacity(capacity_per_db)))
            .collect();

        Self {
            databases: RwLock::new(databases),
            db_count,
        }
    }

    /// Get number of databases
    #[inline]
    pub fn db_count(&self) -> usize {
        self.db_count
    }

    /// Get a specific database by index (clone Arc for minimal lock time)
    /// Returns None if index is out of range
    #[inline]
    pub fn get_db(&self, index: usize) -> Option<Arc<Store>> {
        let dbs = self.databases.read();
        dbs.get(index).cloned()
    }

    /// Get database 0 (default) - optimized fast path
    #[inline]
    pub fn db0(&self) -> Arc<Store> {
        self.databases.read()[0].clone()
    }

    /// Get database by index, panics if out of range
    #[inline]
    pub fn db(&self, index: usize) -> Arc<Store> {
        self.databases.read()[index].clone()
    }

    /// SWAPDB - O(1) atomic swap of two databases
    /// Returns true if successful, false if indices are out of range
    #[inline]
    pub fn swap_db(&self, db1: usize, db2: usize) -> bool {
        if db1 >= self.db_count || db2 >= self.db_count {
            return false;
        }
        if db1 == db2 {
            return true; // No-op
        }

        let mut dbs = self.databases.write();
        dbs.swap(db1, db2);
        true
    }

    /// Get total key count across all databases
    pub fn total_keys(&self) -> usize {
        let dbs = self.databases.read();
        dbs.iter().map(|db| db.len()).sum()
    }

    /// Get key count for each database
    pub fn keys_per_db(&self) -> Vec<usize> {
        let dbs = self.databases.read();
        dbs.iter().map(|db| db.len()).collect()
    }

    /// Flush a specific database
    pub fn flush_db(&self, index: usize) -> bool {
        if let Some(db) = self.get_db(index) {
            db.flush();
            true
        } else {
            false
        }
    }

    /// Flush all databases
    pub fn flush_all(&self) {
        let dbs = self.databases.read();
        for db in dbs.iter() {
            db.flush();
        }
    }

    /// Get all databases (for RDB export)
    pub fn all_databases(&self) -> Vec<Arc<Store>> {
        self.databases.read().clone()
    }
}

impl Default for MultiStore {
    fn default() -> Self {
        Self::new()
    }
}
