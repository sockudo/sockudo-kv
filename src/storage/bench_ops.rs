#[cfg(test)]
mod tests {
    use crate::storage::intset::IntSet;
    use crate::storage::quicklist::QuickList;
    use bytes::Bytes;
    use fastrand;
    use std::collections::{HashSet, VecDeque};
    use std::time::Instant;

    const COUNT: usize = 100_000;

    #[test]
    fn bench_optimizations() {
        println!("\n=== Data Structure Benchmarks ({} items) ===", COUNT);

        bench_list();
        bench_set();
    }

    fn bench_list() {
        println!("\n--- List Benchmarks ---");

        let items: Vec<Bytes> = (0..COUNT).map(|i| Bytes::from(i.to_string())).collect();

        // --- VecDeque ---
        let start = Instant::now();
        let mut deque = VecDeque::new();
        for item in &items {
            deque.push_back(item.clone());
        }
        let push_deque = start.elapsed();

        let start = Instant::now();
        let mut dummy = 0;
        for i in 0..COUNT {
            if let Some(val) = deque.get(i) {
                dummy += val.len();
            }
        }
        let get_deque = start.elapsed();
        println!(
            "VecDeque:  Push: {:?}, Get: {:?} (checksum: {})",
            push_deque, get_deque, dummy
        );

        // --- QuickList ---
        let start = Instant::now();
        let mut qlist = QuickList::new();
        for item in &items {
            qlist.push_back(item.clone());
        }
        let push_qlist = start.elapsed();

        let start = Instant::now();
        let mut dummy = 0;
        for i in 0..COUNT {
            if let Some(val) = qlist.get(i) {
                dummy += val.len();
            }
        }
        let get_qlist = start.elapsed();
        println!(
            "QuickList: Push: {:?}, Get: {:?} (checksum: {})",
            push_qlist, get_qlist, dummy
        );

        // Iteration
        let start = Instant::now();
        let mut count = 0;
        for _ in qlist.iter() {
            count += 1;
        }
        let count_elapsed = start.elapsed();
        println!("QuickList: Iter: {:?} (count: {})", count_elapsed, count);
    }

    fn bench_set() {
        println!("\n--- IntSet Benchmarks ---");

        // Use integers that fit in 16-bit, then 32-bit, then 64-bit
        let mut keys: Vec<i64> = (0..COUNT as i64).collect();
        fastrand::shuffle(&mut keys);

        // --- HashSet ---
        let start = Instant::now();
        let mut hashset = HashSet::new();
        for &k in &keys {
            hashset.insert(k);
        }
        let insert_hash = start.elapsed();

        let start = Instant::now();
        let mut found = 0;
        for &k in &keys {
            if hashset.contains(&k) {
                found += 1;
            }
        }
        let get_hash = start.elapsed();
        println!(
            "HashSet:   Insert: {:?}, Contains: {:?} (found: {})",
            insert_hash, get_hash, found
        );

        // --- IntSet ---
        let start = Instant::now();
        let mut intset = IntSet::new();
        for &k in &keys {
            intset.insert(k);
        }
        let insert_intset = start.elapsed();

        let start = Instant::now();
        let mut found = 0;
        for &k in &keys {
            if intset.contains(k) {
                found += 1;
            }
        }
        let get_intset = start.elapsed();
        println!(
            "IntSet:    Insert: {:?}, Contains: {:?} (found: {})",
            insert_intset, get_intset, found
        );
    }
}
