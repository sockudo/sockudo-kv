use sweep_bptree::BPlusTreeMap;

fn main() {
    let mut tree: BPlusTreeMap<i32, i32, ()> = BPlusTreeMap::new();
    tree.insert(1, 1);
    println!("Tree len: {}", tree.len());
}
