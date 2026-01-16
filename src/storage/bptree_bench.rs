#[cfg(test)]
mod tests {
    use crate::storage::bptree::BPTree;
    use fastrand;
    use std::collections::BTreeMap;
    use std::time::Instant;
    use sweep_bptree::BPlusTreeMap;

    const COUNT: usize = 1_000_000;

    #[test]
    fn bench_comparison() {
        let mut keys: Vec<i64> = (0..COUNT as i64).collect();
        fastrand::shuffle(&mut keys);

        println!("\n=== B+Tree Comparison ({} items) ===", COUNT);

        // --- Setup ---
        let mut btree = BTreeMap::new();
        let mut local_bptree = BPTree::new();
        let mut sweep_bptree: BPlusTreeMap<i64, i64, ()> = BPlusTreeMap::new();

        // Used for sequential tests
        let seq_keys: Vec<i64> = (0..COUNT as i64).collect();

        // ====================================================================
        // INSERT RANDOM
        // ====================================================================
        println!("\n--- Insert (Random) ---");

        // Std BTreeMap
        let start = Instant::now();
        for &k in &keys {
            btree.insert(k, k);
        }
        println!("std::BTreeMap:   {:?}", start.elapsed());

        // Local BPTree
        let start = Instant::now();
        for &k in &keys {
            local_bptree.insert(k, k);
        }
        println!("Local BPTree:    {:?}", start.elapsed());

        // sweep-bptree
        let start = Instant::now();
        for &k in &keys {
            sweep_bptree.insert(k, k);
        }
        println!("sweep-bptree:    {:?}", start.elapsed());

        // ====================================================================
        // INSERT SEQUENTIAL
        // ====================================================================
        // Re-create trees for sequential insert
        let mut btree_seq = BTreeMap::new();
        let mut local_seq = BPTree::new();
        let mut sweep_seq: BPlusTreeMap<i64, i64, ()> = BPlusTreeMap::new();

        println!("\n--- Insert (Sequential) ---");

        // Std BTreeMap
        let start = Instant::now();
        for &k in &seq_keys {
            btree_seq.insert(k, k);
        }
        println!("std::BTreeMap:   {:?}", start.elapsed());

        // Local BPTree
        let start = Instant::now();
        for &k in &seq_keys {
            local_seq.insert(k, k);
        }
        println!("Local BPTree:    {:?}", start.elapsed());

        // sweep-bptree
        let start = Instant::now();
        for &k in &seq_keys {
            sweep_seq.insert(k, k);
        }
        println!("sweep-bptree:    {:?}", start.elapsed());

        // ====================================================================
        // GET RANDOM
        // ====================================================================
        println!("\n--- Get (Random) ---");

        // Std BTreeMap
        let start = Instant::now();
        let mut dummy = 0;
        for &k in &keys {
            if let Some(v) = btree.get(&k) {
                dummy += v;
            }
        }
        println!("std::BTreeMap:   {:?}", start.elapsed());

        // Local BPTree
        let start = Instant::now();
        for &k in &keys {
            if let Some(v) = local_bptree.get(&k) {
                dummy += v;
            }
        }
        println!("Local BPTree:    {:?}", start.elapsed());

        // sweep-bptree
        let start = Instant::now();
        for &k in &keys {
            if let Some(v) = sweep_bptree.get(&k) {
                dummy += v;
            }
        }
        println!("sweep-bptree:    {:?}", start.elapsed());

        // ====================================================================
        // GET SEQUENTIAL
        // ====================================================================
        println!("\n--- Get (Sequential) ---");

        // Std BTreeMap
        let start = Instant::now();
        for &k in &seq_keys {
            if let Some(v) = btree.get(&k) {
                dummy += v;
            }
        }
        println!("std::BTreeMap:   {:?}", start.elapsed());

        // Local BPTree
        let start = Instant::now();
        for &k in &seq_keys {
            if let Some(v) = local_bptree.get(&k) {
                dummy += v;
            }
        }
        println!("Local BPTree:    {:?}", start.elapsed());

        // sweep-bptree
        let start = Instant::now();
        for &k in &seq_keys {
            if let Some(v) = sweep_bptree.get(&k) {
                dummy += v;
            }
        }
        println!("sweep-bptree:    {:?}", start.elapsed());

        // Avoid optimizing out
        if dummy == 0 {
            println!(" ");
        }

        // ====================================================================
        // REMOVE RANDOM
        // ====================================================================
        println!("\n--- Remove (Random) ---");

        let mut remove_keys = keys.clone();
        fastrand::shuffle(&mut remove_keys);

        // Std BTreeMap
        let start = Instant::now();
        for &k in &remove_keys {
            btree.remove(&k);
        }
        println!("std::BTreeMap:   {:?}", start.elapsed());

        // Local BPTree
        let start = Instant::now();
        for &k in &remove_keys {
            local_bptree.remove(&k);
        }
        println!("Local BPTree:    {:?}", start.elapsed());

        // sweep-bptree
        let start = Instant::now();
        for &k in &remove_keys {
            sweep_bptree.remove(&k);
        }
        println!("sweep-bptree:    {:?}", start.elapsed());
    }
}
