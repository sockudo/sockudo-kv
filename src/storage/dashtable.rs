use ahash::RandomState;
use crossbeam_utils::CachePadded;
use hashbrown::{HashTable, hash_table};
use parking_lot::{RwLock, RwLockReadGuard};
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};

static HASHER: RandomState = RandomState::with_seeds(1, 2, 3, 4);

/// Calculate hash for a value using stable ahash seeds.
#[inline(always)]
pub fn calculate_hash<T: Hash + ?Sized>(val: &T) -> u64 {
    HASHER.hash_one(val)
}

/// A custom concurrent hash table optimized for sockudo-kv.
/// Supports manual sharding and efficient entry-based access.
pub struct DashTable<T> {
    shift: u32,
    shards: Box<[CachePadded<RwLock<HashTable<T>>>]>,
    size: AtomicUsize,
}

impl<T> Default for DashTable<T> {
    fn default() -> Self {
        Self::with_shard_amount(1)
    }
}

impl<T> DashTable<T> {
    pub fn new() -> Self {
        Self::with_shard_amount(16)
    }

    /// Create a new DashTable with a specific number of shards.
    /// shard_amount must be a power of two.
    pub fn with_shard_amount(shard_amount: usize) -> Self {
        assert!(shard_amount.is_power_of_two());
        let shard_shift = shard_amount.ilog2();
        let shift = usize::BITS - shard_shift;

        let shards = (0..shard_amount)
            .map(|_| CachePadded::new(RwLock::new(HashTable::new())))
            .collect();

        Self {
            shift,
            shards,
            size: AtomicUsize::new(0),
        }
    }

    /// Create a new DashTable with a specific capacity and shard amount.
    pub fn with_capacity_and_shard_amount(capacity: usize, shard_amount: usize) -> Self {
        assert!(shard_amount.is_power_of_two());
        let shard_shift = shard_amount.ilog2();
        let shift = usize::BITS - shard_shift;

        let cps = (capacity + (shard_amount - 1)) >> shard_shift;

        let shards = (0..shard_amount)
            .map(|_| CachePadded::new(RwLock::new(HashTable::with_capacity(cps))))
            .collect();

        Self {
            shift,
            shards,
            size: AtomicUsize::new(0),
        }
    }

    /// Determine which shard a hash belongs to.
    #[inline(always)]
    fn determine_shard(&self, hash: u64) -> usize {
        if self.shards.len() == 1 {
            return 0;
        }
        // Use high bits for sharding to keep low bits for hashbrown
        (hash as usize) >> self.shift
    }

    /// Get the current number of elements in the table.
    pub fn len(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    /// Check if the table is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Access an entry in the table.
    pub fn entry<'a>(
        &'a self,
        hash: u64,
        eq: impl FnMut(&T) -> bool,
        hasher: impl Fn(&T) -> u64,
    ) -> Entry<'a, T> {
        let shard_idx = self.determine_shard(hash);
        let mut guard = self.shards[shard_idx].write();

        // Safety: We manage the lifetime via our own wrapper
        let shard: *mut HashTable<T> = &mut *guard;
        let shard: &'a mut HashTable<T> = unsafe { &mut *shard };

        match shard.entry(hash, eq, hasher) {
            hash_table::Entry::Occupied(e) => Entry::Occupied(OccupiedEntry {
                _guard: guard,
                entry: e,
                size: &self.size,
            }),
            hash_table::Entry::Vacant(e) => Entry::Vacant(VacantEntry {
                _guard: guard,
                entry: e,
                size: &self.size,
            }),
        }
    }

    /// Insert a value if it doesn't exist, using pre-computed hash.
    pub fn insert_unique(&self, hash: u64, value: T, hasher: impl Fn(&T) -> u64) {
        let shard_idx = self.determine_shard(hash);
        let mut guard = self.shards[shard_idx].write();
        guard.insert_unique(hash, value, hasher);
        self.size.fetch_add(1, Ordering::Relaxed);
    }

    /// Get a read-only reference to an entry.
    pub fn get<'a>(
        &'a self,
        hash: u64,
        mut eq: impl FnMut(&T) -> bool,
    ) -> Option<ReadOnlyRef<'a, T>> {
        let shard_idx = self.determine_shard(hash);
        let guard = self.shards[shard_idx].read();

        // Safety: Extend lifetime of the reference since we hold the guard.
        let shard: &HashTable<T> = &guard;
        let value = shard.find(hash, |v| eq(v))?;
        let value: &'a T = unsafe { &*(value as *const T) };

        Some(ReadOnlyRef {
            _guard: guard,
            value,
        })
    }

    /// Insert a value, replacing existing if it matches.
    pub fn insert(
        &self,
        hash: u64,
        value: T,
        eq: impl FnMut(&T) -> bool,
        hasher: impl Fn(&T) -> u64,
    ) -> Option<T> {
        let shard_idx = self.determine_shard(hash);
        let mut guard = self.shards[shard_idx].write();
        match guard.entry(hash, eq, hasher) {
            hash_table::Entry::Occupied(mut e) => Some(std::mem::replace(e.get_mut(), value)),
            hash_table::Entry::Vacant(e) => {
                e.insert(value);
                self.size.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Clear all entries.
    pub fn clear(&self) {
        for shard in &self.shards {
            let mut guard = shard.write();
            guard.clear();
        }
        self.size.store(0, Ordering::Relaxed);
    }

    /// Number of shards
    pub fn shards_len(&self) -> usize {
        self.shards.len()
    }

    /// Remove an entry and return its value.
    pub fn remove(
        &self,
        hash: u64,
        eq: impl FnMut(&T) -> bool,
        hasher: impl Fn(&T) -> u64,
    ) -> Option<T> {
        let shard_idx = self.determine_shard(hash);
        let mut guard = self.shards[shard_idx].write();

        match guard.entry(hash, eq, hasher) {
            hash_table::Entry::Occupied(e) => {
                let val = e.remove().0;
                self.size.fetch_sub(1, Ordering::Relaxed);
                Some(val)
            }
            hash_table::Entry::Vacant(_) => None,
        }
    }

    /// Parallel iteration (locked per shard).
    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(&T),
    {
        for shard_lock in self.shards.iter() {
            let guard = shard_lock.read();
            for item in guard.iter() {
                f(item);
            }
        }
    }

    /// Iterate over a specific shard
    pub fn for_each_in_shard<F>(&self, shard_idx: usize, mut f: F)
    where
        F: FnMut(&T),
    {
        if shard_idx < self.shards.len() {
            let guard = self.shards[shard_idx].read();
            for item in guard.iter() {
                f(item);
            }
        }
    }

    /// Provides an iterator over the shards.
    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            table: self,
            shard_idx: 0,
            current_guard: None,
        }
    }
}

pub struct Iter<'a, T> {
    table: &'a DashTable<T>,
    shard_idx: usize,
    current_guard: Option<(RwLockReadGuard<'a, HashTable<T>>, hash_table::Iter<'a, T>)>,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((_, iter)) = &mut self.current_guard {
                // We have a shard locked, try to get next element
                // Safety: hashbrown::hash_table::Iter is safe as long as the HashTable is not modified.
                // We hold the read guard, so it's safe.
                // However, hashbrown's Iter doesn't have the same lifetime as the guard in the struct easily.
                // We'll use a slightly different approach for the iterator to be safe.
                if let Some(item) =
                    unsafe { std::mem::transmute::<Option<&T>, Option<&'a T>>(iter.next()) }
                {
                    return Some(item);
                }
            }

            // Move to next shard
            if self.shard_idx >= self.table.shards.len() {
                return None;
            }

            let guard = self.table.shards[self.shard_idx].read();
            // Safety: Transmuting to extend lifetime because we hold the guard.
            #[allow(clippy::missing_transmute_annotations)]
            let iter = unsafe { std::mem::transmute(guard.iter()) };
            self.current_guard = Some((guard, iter));
            self.shard_idx += 1;
        }
    }
}

pub enum Entry<'a, T> {
    Occupied(OccupiedEntry<'a, T>),
    Vacant(VacantEntry<'a, T>),
}

pub struct OccupiedEntry<'a, T> {
    _guard: parking_lot::RwLockWriteGuard<'a, HashTable<T>>,
    entry: hash_table::OccupiedEntry<'a, T>,
    size: &'a AtomicUsize,
}

impl<'a, T> OccupiedEntry<'a, T> {
    pub fn get(&self) -> &T {
        self.entry.get()
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.entry.get_mut()
    }

    pub fn remove(self) -> T {
        let val = self.entry.remove().0;
        self.size.fetch_sub(1, Ordering::Relaxed);
        val
    }
}

pub struct VacantEntry<'a, T> {
    _guard: parking_lot::RwLockWriteGuard<'a, HashTable<T>>,
    entry: hash_table::VacantEntry<'a, T>,
    size: &'a AtomicUsize,
}

impl<'a, T> VacantEntry<'a, T> {
    pub fn insert(self, value: T) -> &'a mut T {
        self.size.fetch_add(1, Ordering::Relaxed);
        self.entry.insert(value).into_mut()
    }
}

pub struct ReadOnlyRef<'a, T> {
    pub(crate) _guard: RwLockReadGuard<'a, HashTable<T>>,
    pub(crate) value: &'a T,
}

impl<'a, T> std::ops::Deref for ReadOnlyRef<'a, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.value
    }
}
