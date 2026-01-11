//! High-performance B+Tree implementation inspired by Dragonfly's design.
//!
//! This B+Tree is optimized for:
//! - Fast range queries with O(log n) seek + O(k) iteration
//! - Cache-friendly node layout
//! - Safe Rust implementation using Vec-based storage
//!
//! Design decisions:
//! - Uses Vec for node storage (safe, properly aligned)
//! - Separate leaf and inner node types for clarity
//! - Path-based traversal (no parent pointers)
//! - Binary search within nodes

// ============================================================================
// Constants
// ============================================================================

/// Maximum keys per leaf node (tuned for cache efficiency)
const MAX_LEAF_KEYS: usize = 15;

/// Minimum keys per leaf node
const MIN_LEAF_KEYS: usize = MAX_LEAF_KEYS / 2;

/// Maximum keys per inner node
const MAX_INNER_KEYS: usize = 15;

/// Minimum keys per inner node
const MIN_INNER_KEYS: usize = MAX_INNER_KEYS / 2;

// ============================================================================
// Node Types
// ============================================================================

/// A leaf node containing key-value pairs
#[derive(Debug, Clone)]
struct LeafNode<K, V> {
    keys: Vec<K>,
    values: Vec<V>,
}

impl<K: Clone + Ord, V: Clone> LeafNode<K, V> {
    fn new() -> Self {
        Self {
            keys: Vec::with_capacity(MAX_LEAF_KEYS),
            values: Vec::with_capacity(MAX_LEAF_KEYS),
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.keys.len()
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.keys.len() >= MAX_LEAF_KEYS
    }

    #[inline]
    fn is_underfull(&self) -> bool {
        self.keys.len() < MIN_LEAF_KEYS
    }

    /// Binary search for key, returns (index, found)
    fn search(&self, key: &K) -> (usize, bool) {
        match self.keys.binary_search(key) {
            Ok(i) => (i, true),
            Err(i) => (i, false),
        }
    }

    fn insert(&mut self, pos: usize, key: K, value: V) {
        self.keys.insert(pos, key);
        self.values.insert(pos, value);
    }

    fn remove(&mut self, pos: usize) -> (K, V) {
        let key = self.keys.remove(pos);
        let value = self.values.remove(pos);
        (key, value)
    }

    /// Split this node, returning the new right node and median key
    fn split(&mut self) -> (Self, K) {
        let mid = self.keys.len() / 2;
        let median = self.keys[mid].clone();

        let mut right = Self::new();
        right.keys = self.keys.split_off(mid);
        right.values = self.values.split_off(mid);

        (right, median)
    }
}

/// An inner node containing keys and child indices
#[derive(Debug, Clone)]
struct InnerNode<K> {
    keys: Vec<K>,
    children: Vec<usize>,       // Indices into node storage
    subtree_counts: Vec<usize>, // Count of items in each child's subtree
}

impl<K: Clone + Ord> InnerNode<K> {
    fn new() -> Self {
        Self {
            keys: Vec::with_capacity(MAX_INNER_KEYS),
            children: Vec::with_capacity(MAX_INNER_KEYS + 1),
            subtree_counts: Vec::with_capacity(MAX_INNER_KEYS + 1),
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.keys.len()
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.keys.len() >= MAX_INNER_KEYS
    }

    #[inline]
    fn is_underfull(&self) -> bool {
        self.keys.len() < MIN_INNER_KEYS
    }

    /// Binary search for key, returns (index, found)
    fn search(&self, key: &K) -> (usize, bool) {
        match self.keys.binary_search(key) {
            Ok(i) => (i, true),
            Err(i) => (i, false),
        }
    }

    fn insert(&mut self, pos: usize, key: K, right_child: usize, right_count: usize) {
        self.keys.insert(pos, key);
        self.children.insert(pos + 1, right_child);
        self.subtree_counts.insert(pos + 1, right_count);
    }

    fn remove(&mut self, pos: usize) -> K {
        let key = self.keys.remove(pos);
        self.children.remove(pos + 1);
        self.subtree_counts.remove(pos + 1);
        key
    }

    /// Split this node, returning the new right node and median key
    fn split(&mut self) -> (Self, K) {
        let mid = self.keys.len() / 2;
        let median = self.keys[mid].clone();

        let mut right = Self::new();
        right.keys = self.keys.split_off(mid + 1);
        right.children = self.children.split_off(mid + 1);
        right.subtree_counts = self.subtree_counts.split_off(mid + 1);

        // Remove the median from left
        self.keys.pop();

        (right, median)
    }
}

/// A node in the B+Tree (either leaf or inner)
#[derive(Debug, Clone)]
enum Node<K, V> {
    Leaf(LeafNode<K, V>),
    Inner(InnerNode<K>),
}

impl<K: Clone + Ord, V: Clone> Node<K, V> {
    fn is_leaf(&self) -> bool {
        matches!(self, Node::Leaf(_))
    }

    fn len(&self) -> usize {
        match self {
            Node::Leaf(leaf) => leaf.len(),
            Node::Inner(inner) => inner.len(),
        }
    }

    fn is_underfull(&self) -> bool {
        match self {
            Node::Leaf(leaf) => leaf.is_underfull(),
            Node::Inner(inner) => inner.is_underfull(),
        }
    }

    fn tree_count(&self) -> usize {
        match self {
            Node::Leaf(leaf) => leaf.len(),
            Node::Inner(inner) => inner.subtree_counts.iter().sum(),
        }
    }
}

// ============================================================================
// Path for Traversal
// ============================================================================

/// Maximum tree depth
const MAX_DEPTH: usize = 32;

/// A path from root to a node, tracking position at each level
#[derive(Clone)]
struct BPTreePath {
    /// Node indices at each level
    nodes: [usize; MAX_DEPTH],
    /// Position within each node
    positions: [usize; MAX_DEPTH],
    /// Current depth
    depth: usize,
}

impl BPTreePath {
    fn new() -> Self {
        Self {
            nodes: [0; MAX_DEPTH],
            positions: [0; MAX_DEPTH],
            depth: 0,
        }
    }

    fn push(&mut self, node_idx: usize, pos: usize) {
        debug_assert!(self.depth < MAX_DEPTH);
        self.nodes[self.depth] = node_idx;
        self.positions[self.depth] = pos;
        self.depth += 1;
    }

    fn pop(&mut self) -> Option<(usize, usize)> {
        if self.depth == 0 {
            return None;
        }
        self.depth -= 1;
        Some((self.nodes[self.depth], self.positions[self.depth]))
    }

    fn last(&self) -> Option<(usize, usize)> {
        if self.depth == 0 {
            return None;
        }
        Some((self.nodes[self.depth - 1], self.positions[self.depth - 1]))
    }

    fn set_last_pos(&mut self, pos: usize) {
        if self.depth > 0 {
            self.positions[self.depth - 1] = pos;
        }
    }

    fn is_empty(&self) -> bool {
        self.depth == 0
    }
}

// ============================================================================
// BPTree
// ============================================================================

/// A high-performance B+Tree map
#[derive(Debug)]
pub struct BPTree<K: Clone + Ord, V: Clone> {
    /// All nodes stored in a Vec for cache-friendly access
    nodes: Vec<Node<K, V>>,
    /// Index of root node (None if empty)
    root: Option<usize>,
    /// Total number of entries
    count: usize,
    /// Free list for reusing node slots
    free_list: Vec<usize>,
}

impl<K: Clone + Ord, V: Clone> Default for BPTree<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Clone + Ord, V: Clone> BPTree<K, V> {
    /// Create a new empty B+Tree
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            root: None,
            count: 0,
            free_list: Vec::new(),
        }
    }

    /// Get the number of entries
    #[inline]
    pub fn len(&self) -> usize {
        self.count
    }

    /// Check if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Allocate a new node, returns its index
    fn alloc_node(&mut self, node: Node<K, V>) -> usize {
        if let Some(idx) = self.free_list.pop() {
            self.nodes[idx] = node;
            idx
        } else {
            let idx = self.nodes.len();
            self.nodes.push(node);
            idx
        }
    }

    /// Free a node slot for reuse
    fn free_node(&mut self, idx: usize) {
        self.free_list.push(idx);
    }

    /// Insert a key-value pair, returns true if inserted (false if key existed)
    pub fn insert(&mut self, key: K, value: V) -> bool {
        if self.root.is_none() {
            let mut leaf = LeafNode::new();
            leaf.insert(0, key, value);
            let idx = self.alloc_node(Node::Leaf(leaf));
            self.root = Some(idx);
            self.count = 1;
            return true;
        }

        let mut path = BPTreePath::new();
        if self.locate(&key, &mut path) {
            return false; // Key already exists
        }

        let (leaf_idx, pos) = path.last().unwrap();

        // Check if leaf is full
        let is_full = match &self.nodes[leaf_idx] {
            Node::Leaf(leaf) => leaf.is_full(),
            _ => unreachable!(),
        };

        if !is_full {
            // Simple insert
            if let Node::Leaf(leaf) = &mut self.nodes[leaf_idx] {
                leaf.insert(pos, key, value);
            }
            self.update_tree_counts(&path, 1);
            self.count += 1;
            return true;
        }

        // Leaf is full, need to split
        self.insert_full_leaf(key, value, path);
        self.count += 1;
        true
    }

    /// Get value for key
    pub fn get(&self, key: &K) -> Option<V> {
        let root_idx = self.root?;
        let mut node_idx = root_idx;

        loop {
            match &self.nodes[node_idx] {
                Node::Leaf(leaf) => {
                    let (pos, found) = leaf.search(key);
                    return if found {
                        Some(leaf.values[pos].clone())
                    } else {
                        None
                    };
                }
                Node::Inner(inner) => {
                    let (pos, found) = inner.search(key);
                    let child_idx = if found { pos + 1 } else { pos };
                    node_idx = inner.children[child_idx];
                }
            }
        }
    }

    /// Check if key exists
    pub fn contains(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    /// Remove a key, returning its value if it existed
    pub fn remove(&mut self, key: &K) -> Option<V> {
        let mut path = BPTreePath::new();
        if !self.locate(key, &mut path) {
            return None;
        }

        let (leaf_idx, pos) = path.last().unwrap();
        let value = if let Node::Leaf(leaf) = &mut self.nodes[leaf_idx] {
            leaf.remove(pos).1
        } else {
            unreachable!()
        };

        self.count -= 1;
        self.update_tree_counts(&path, -1);
        self.rebalance_after_remove(&mut path);

        Some(value)
    }

    /// Iterate over a range of keys [from, to] inclusive
    pub fn range<'a>(&'a self, from: &K, to: &K) -> RangeIter<'a, K, V> {
        RangeIter::new(self, from.clone(), to.clone())
    }

    /// Iterate over all entries in order
    pub fn iter(&self) -> Iter<'_, K, V> {
        Iter::new(self)
    }

    /// Find the first key >= given key
    pub fn lower_bound(&self, key: &K) -> Option<(K, V)> {
        let mut path = BPTreePath::new();
        self.locate_geq(key, &mut path);

        if path.is_empty() {
            return None;
        }

        let (node_idx, pos) = path.last().unwrap();
        if let Node::Leaf(leaf) = &self.nodes[node_idx]
            && pos < leaf.len()
        {
            return Some((leaf.keys[pos].clone(), leaf.values[pos].clone()));
        }
        None
    }

    /// Clear all entries
    pub fn clear(&mut self) {
        self.nodes.clear();
        self.root = None;
        self.count = 0;
        self.free_list.clear();
    }

    /// Get the first (smallest) key-value pair
    pub fn first(&self) -> Option<(K, V)> {
        let root_idx = self.root?;
        let mut node_idx = root_idx;

        loop {
            match &self.nodes[node_idx] {
                Node::Leaf(leaf) => {
                    if leaf.len() > 0 {
                        return Some((leaf.keys[0].clone(), leaf.values[0].clone()));
                    }
                    return None;
                }
                Node::Inner(inner) => {
                    node_idx = inner.children[0];
                }
            }
        }
    }

    /// Get the last (largest) key-value pair
    pub fn last(&self) -> Option<(K, V)> {
        let root_idx = self.root?;
        let mut node_idx = root_idx;

        loop {
            match &self.nodes[node_idx] {
                Node::Leaf(leaf) => {
                    let len = leaf.len();
                    if len > 0 {
                        return Some((leaf.keys[len - 1].clone(), leaf.values[len - 1].clone()));
                    }
                    return None;
                }
                Node::Inner(inner) => {
                    node_idx = inner.children[inner.children.len() - 1];
                }
            }
        }
    }

    /// Get the first (smallest) key
    #[inline]
    pub fn first_key(&self) -> Option<K> {
        self.first().map(|(k, _)| k)
    }

    /// Get the last (largest) key
    #[inline]
    pub fn last_key(&self) -> Option<K> {
        self.last().map(|(k, _)| k)
    }

    /// Get rank of a key (0-indexed)
    pub fn rank(&self, key: &K) -> Option<usize> {
        let root_idx = self.root?;

        let mut node_idx = root_idx;
        let mut rank = 0;

        loop {
            match &self.nodes[node_idx] {
                Node::Leaf(leaf) => {
                    let (pos, found) = leaf.search(key);
                    return if found { Some(rank + pos) } else { None };
                }
                Node::Inner(inner) => {
                    let (pos, found) = inner.search(key);
                    let child_idx = if found { pos + 1 } else { pos };

                    // Add counts of all left siblings
                    for i in 0..child_idx {
                        rank += inner.subtree_counts[i];
                    }

                    node_idx = inner.children[child_idx];
                }
            }
        }
    }

    /// Get entry by rank (0-indexed)
    pub fn select(&self, mut rank: usize) -> Option<(K, V)> {
        let root_idx = self.root?;

        if rank >= self.count {
            return None;
        }

        let mut node_idx = root_idx;

        loop {
            match &self.nodes[node_idx] {
                Node::Leaf(leaf) => {
                    if rank < leaf.len() {
                        return Some((leaf.keys[rank].clone(), leaf.values[rank].clone()));
                    } else {
                        return None; // Should not happen if self.count is correct
                    }
                }
                Node::Inner(inner) => {
                    for (i, &count) in inner.subtree_counts.iter().enumerate() {
                        if rank < count {
                            node_idx = inner.children[i];
                            break;
                        }
                        rank -= count;
                    }
                }
            }
        }
    }

    // ==================== Internal Methods ====================

    /// Locate a key, returning true if found
    fn locate(&self, key: &K, path: &mut BPTreePath) -> bool {
        let Some(root_idx) = self.root else {
            return false;
        };

        let mut node_idx = root_idx;
        loop {
            match &self.nodes[node_idx] {
                Node::Leaf(leaf) => {
                    let (pos, found) = leaf.search(key);
                    path.push(node_idx, pos);
                    return found;
                }
                Node::Inner(inner) => {
                    let (pos, found) = inner.search(key);
                    let child_idx = if found { pos + 1 } else { pos };
                    path.push(node_idx, child_idx);
                    node_idx = inner.children[child_idx];
                }
            }
        }
    }

    /// Locate first key >= given key
    fn locate_geq(&self, key: &K, path: &mut BPTreePath) {
        let Some(root_idx) = self.root else {
            return;
        };

        let mut node_idx = root_idx;
        loop {
            match &self.nodes[node_idx] {
                Node::Leaf(leaf) => {
                    let (pos, _) = leaf.search(key);
                    path.push(node_idx, pos);
                    return;
                }
                Node::Inner(inner) => {
                    let (pos, found) = inner.search(key);
                    let child_idx = if found { pos + 1 } else { pos };
                    path.push(node_idx, child_idx);
                    node_idx = inner.children[child_idx];
                }
            }
        }
    }

    /// Insert into a full leaf, splitting as needed
    fn insert_full_leaf(&mut self, key: K, value: V, mut path: BPTreePath) {
        let (leaf_idx, insert_pos) = path.pop().unwrap();

        // Split the leaf
        let (right_leaf, median) = if let Node::Leaf(leaf) = &mut self.nodes[leaf_idx] {
            // First insert, then split
            leaf.insert(insert_pos, key, value);
            leaf.split()
        } else {
            unreachable!()
        };

        let right_idx = self.alloc_node(Node::Leaf(right_leaf));

        // Propagate split up the tree
        self.propagate_split(median, right_idx, &mut path);
    }

    /// Propagate a split up the tree
    fn propagate_split(&mut self, mut median: K, mut right_idx: usize, path: &mut BPTreePath) {
        while let Some((parent_idx, pos)) = path.pop() {
            let is_full = match &self.nodes[parent_idx] {
                Node::Inner(inner) => inner.is_full(),
                _ => unreachable!(),
            };

            if !is_full {
                let right_count = self.nodes[right_idx].tree_count();

                // Get left child count (it changed due to split)
                let left_idx = if let Node::Inner(inner) = &self.nodes[parent_idx] {
                    inner.children[pos]
                } else {
                    unreachable!()
                };
                let left_count = self.nodes[left_idx].tree_count();

                // Simple insert into parent
                if let Node::Inner(inner) = &mut self.nodes[parent_idx] {
                    inner.insert(pos, median, right_idx, right_count);
                    inner.subtree_counts[pos] = left_count;
                }
                return;
            }

            // Parent is full, need to split it too
            let right_count = self.nodes[right_idx].tree_count();
            let (new_right_inner, new_median) =
                if let Node::Inner(inner) = &mut self.nodes[parent_idx] {
                    inner.insert(pos, median, right_idx, right_count);
                    inner.split()
                } else {
                    unreachable!()
                };

            // Recalculate tree counts for both sides
            self.recalc_tree_count(parent_idx);
            let new_right_idx = self.alloc_node(Node::Inner(new_right_inner));
            self.recalc_tree_count(new_right_idx);

            median = new_median;
            right_idx = new_right_idx;
        }

        // Need a new root
        let mut new_root = InnerNode::<K>::new();
        new_root.keys.push(median);
        new_root.children.push(self.root.unwrap());
        new_root.children.push(right_idx);

        // Root counts
        let left_count = self.nodes[self.root.unwrap()].tree_count();
        let right_count = self.nodes[right_idx].tree_count();
        new_root.subtree_counts.push(left_count);
        new_root.subtree_counts.push(right_count);

        let new_root_idx = self.alloc_node(Node::Inner(new_root));
        self.root = Some(new_root_idx);
    }

    /// Recalculate subtree counts for an inner node
    fn recalc_tree_count(&mut self, node_idx: usize) {
        let children = if let Node::Inner(inner) = &self.nodes[node_idx] {
            inner.children.clone()
        } else {
            return;
        };

        let mut counts = Vec::with_capacity(children.len());
        for &child_idx in &children {
            counts.push(self.nodes[child_idx].tree_count());
        }

        if let Node::Inner(inner) = &mut self.nodes[node_idx] {
            inner.subtree_counts = counts;
        }
    }

    /// Update tree counts along path
    fn update_tree_counts(&mut self, path: &BPTreePath, delta: i32) {
        // Iterate path, find which child index we descended from, update that count
        for i in 0..path.depth.saturating_sub(1) {
            let node_idx = path.nodes[i];
            let child_pos = path.positions[i]; // Position in parent (should be index in children/subtree_counts)

            if let Node::Inner(inner) = &mut self.nodes[node_idx]
                && child_pos < inner.subtree_counts.len()
            {
                let new_count = (inner.subtree_counts[child_pos] as i32 + delta) as usize;
                inner.subtree_counts[child_pos] = new_count;
            }
        }
    }

    /// Rebalance after removal
    fn rebalance_after_remove(&mut self, path: &mut BPTreePath) {
        loop {
            let Some((node_idx, _)) = path.last() else {
                break;
            };

            // Check if node is the root
            if path.depth == 1 {
                let node = &self.nodes[node_idx];
                if !node.is_leaf() && node.len() == 0 {
                    // Root has no keys, make its only child the new root
                    if let Node::Inner(inner) = node {
                        let new_root = inner.children[0];
                        self.free_node(node_idx);
                        self.root = Some(new_root);
                    }
                } else if node.is_leaf() && node.len() == 0 {
                    // Tree is now empty
                    self.free_node(node_idx);
                    self.root = None;
                }
                break;
            }

            if !self.nodes[node_idx].is_underfull() {
                break;
            }

            // Node is underfull, try to rebalance with siblings
            path.pop();
            let (parent_idx, child_pos) = path.last().unwrap();

            // Try to borrow or merge
            let parent_len = self.nodes[parent_idx].len();
            let can_borrow_left = child_pos > 0 && {
                if let Node::Inner(p) = &self.nodes[parent_idx] {
                    let left_idx = p.children[child_pos - 1];
                    self.nodes[left_idx].len() > MIN_LEAF_KEYS
                } else {
                    false
                }
            };

            let can_borrow_right = child_pos < parent_len && {
                if let Node::Inner(p) = &self.nodes[parent_idx] {
                    let right_idx = p.children[child_pos + 1];
                    self.nodes[right_idx].len() > MIN_LEAF_KEYS
                } else {
                    false
                }
            };

            if can_borrow_left {
                self.borrow_from_left(parent_idx, child_pos);
                break;
            } else if can_borrow_right {
                self.borrow_from_right(parent_idx, child_pos);
                break;
            } else if child_pos > 0 {
                // Merge with left sibling
                self.merge_with_left(parent_idx, child_pos);
            } else {
                // Merge with right sibling
                self.merge_with_right(parent_idx, child_pos);
            }
        }
    }

    fn borrow_from_left(&mut self, parent_idx: usize, child_pos: usize) {
        let (left_idx, node_idx, separator_pos) = {
            if let Node::Inner(parent) = &self.nodes[parent_idx] {
                (
                    parent.children[child_pos - 1],
                    parent.children[child_pos],
                    child_pos - 1,
                )
            } else {
                return;
            }
        };

        let is_leaf = self.nodes[node_idx].is_leaf();

        if is_leaf {
            // Borrow from left leaf
            let (key, value) = if let Node::Leaf(left) = &mut self.nodes[left_idx] {
                left.remove(left.len() - 1)
            } else {
                return;
            };

            let new_separator = key.clone();

            if let Node::Leaf(node) = &mut self.nodes[node_idx] {
                node.insert(0, key, value);
            }

            if let Node::Inner(parent) = &mut self.nodes[parent_idx] {
                parent.keys[separator_pos] = new_separator;
                // Update subtree counts
                parent.subtree_counts[child_pos - 1] -= 1;
                parent.subtree_counts[child_pos] += 1;
            }
        } else {
            // Borrow from left inner node
            let (key, child, count) = if let Node::Inner(left) = &mut self.nodes[left_idx] {
                let k = left.keys.pop().unwrap();
                let c = left.children.pop().unwrap();
                let cnt = left.subtree_counts.pop().unwrap();
                (k, c, cnt)
            } else {
                unreachable!()
            };

            // Separator from parent
            let separator = if let Node::Inner(parent) = &self.nodes[parent_idx] {
                parent.keys[separator_pos].clone()
            } else {
                unreachable!()
            };

            if let Node::Inner(node) = &mut self.nodes[node_idx] {
                node.keys.insert(0, separator);
                node.children.insert(0, child);
                node.subtree_counts.insert(0, count);
            }

            if let Node::Inner(parent) = &mut self.nodes[parent_idx] {
                parent.keys[separator_pos] = key;
                // Update subtree counts: `left` lost `count`, `node` gained `count`
                // But wait, the key also moved? Inner keys don't count towards size?
                // In B+Tree, only leaves contain data. Inner keys are separators.
                // So total size of subtree depends on sum of children counts.
                // `left` lost `child` (size `count`). `node` gained `child` (size `count`).
                // So we just subtract `count` from left and add `count` to node in parent logic.
                let left_cnt = &mut parent.subtree_counts[child_pos - 1];
                *left_cnt -= count;
                let node_cnt = &mut parent.subtree_counts[child_pos];
                *node_cnt += count;
            }
        }
    }

    fn borrow_from_right(&mut self, parent_idx: usize, child_pos: usize) {
        let (node_idx, right_idx, separator_pos) = {
            if let Node::Inner(parent) = &self.nodes[parent_idx] {
                (
                    parent.children[child_pos],
                    parent.children[child_pos + 1],
                    child_pos,
                )
            } else {
                return;
            }
        };

        let is_leaf = self.nodes[node_idx].is_leaf();

        if is_leaf {
            // Borrow from right leaf
            let (key, value) = if let Node::Leaf(right) = &mut self.nodes[right_idx] {
                right.remove(0)
            } else {
                return;
            };

            if let Node::Leaf(node) = &mut self.nodes[node_idx] {
                node.insert(node.len(), key, value);
            }

            // Update separator
            let new_separator = if let Node::Leaf(right) = &self.nodes[right_idx] {
                right.keys[0].clone()
            } else {
                return;
            };

            if let Node::Inner(parent) = &mut self.nodes[parent_idx] {
                parent.keys[separator_pos] = new_separator;
                // Update subtree counts
                parent.subtree_counts[child_pos + 1] -= 1;
                parent.subtree_counts[child_pos] += 1;
            }
        } else {
            // Borrow from right inner node
            let (key, child, count) = if let Node::Inner(right) = &mut self.nodes[right_idx] {
                let k = right.keys.remove(0);
                let c = right.children.remove(0);
                let cnt = right.subtree_counts.remove(0);
                (k, c, cnt)
            } else {
                unreachable!()
            };

            // Separator from parent
            let separator = if let Node::Inner(parent) = &self.nodes[parent_idx] {
                parent.keys[separator_pos].clone()
            } else {
                unreachable!()
            };

            if let Node::Inner(node) = &mut self.nodes[node_idx] {
                node.keys.push(separator);
                node.children.push(child);
                node.subtree_counts.push(count);
            }

            if let Node::Inner(parent) = &mut self.nodes[parent_idx] {
                parent.keys[separator_pos] = key;
                // Update subtree counts: `right` lost `count`, `node` gained `count`
                let right_cnt = &mut parent.subtree_counts[child_pos + 1];
                *right_cnt -= count;
                let node_cnt = &mut parent.subtree_counts[child_pos];
                *node_cnt += count;
            }
        }
    }

    fn merge_with_left(&mut self, parent_idx: usize, child_pos: usize) {
        let (left_idx, node_idx, separator) = if let Node::Inner(parent) = &self.nodes[parent_idx] {
            (
                parent.children[child_pos - 1],
                parent.children[child_pos],
                parent.keys[child_pos - 1].clone(),
            )
        } else {
            return;
        };

        // Remove separator from parent - this handles keys and children removal
        if let Node::Inner(parent) = &mut self.nodes[parent_idx] {
            parent.remove(child_pos - 1);
        }

        let is_leaf = self.nodes[node_idx].is_leaf();

        if is_leaf {
            // Get the node's data
            let (keys, values) = if let Node::Leaf(node) = &self.nodes[node_idx] {
                (node.keys.clone(), node.values.clone())
            } else {
                return;
            };

            // Merge into left
            if let Node::Leaf(left) = &mut self.nodes[left_idx] {
                left.keys.push(separator);
                left.values.push(values[0].clone()); // Dummy value for separator
                left.keys.pop(); // Actually, leaf merge doesn't use separator
                left.values.pop();
                left.keys.extend(keys);
                left.values.extend(values);
            }

            self.free_node(node_idx);
        } else {
            // Inner node merge
            let (keys, children, counts) = if let Node::Inner(node) = &self.nodes[node_idx] {
                (
                    node.keys.clone(),
                    node.children.clone(),
                    node.subtree_counts.clone(),
                )
            } else {
                return;
            };

            if let Node::Inner(left) = &mut self.nodes[left_idx] {
                left.keys.push(separator);
                left.keys.extend(keys);
                left.children.extend(children);
                left.subtree_counts.extend(counts);
            }

            self.recalc_tree_count(left_idx);
            self.free_node(node_idx);
        }

        // Update parent tree count
        // Update parent's count for the merged child (left_idx)
        let new_child_count = self.nodes[left_idx].tree_count();
        if let Node::Inner(parent) = &mut self.nodes[parent_idx] {
            // The merged node is at child_pos - 1
            parent.subtree_counts[child_pos - 1] = new_child_count;
        }
    }

    fn merge_with_right(&mut self, parent_idx: usize, child_pos: usize) {
        let (node_idx, right_idx, separator) = if let Node::Inner(parent) = &self.nodes[parent_idx]
        {
            (
                parent.children[child_pos],
                parent.children[child_pos + 1],
                parent.keys[child_pos].clone(),
            )
        } else {
            return;
        };

        // Remove separator from parent
        if let Node::Inner(parent) = &mut self.nodes[parent_idx] {
            parent.remove(child_pos);
        }

        let is_leaf = self.nodes[node_idx].is_leaf();

        if is_leaf {
            // Get right's data
            let (keys, values) = if let Node::Leaf(right) = &self.nodes[right_idx] {
                (right.keys.clone(), right.values.clone())
            } else {
                return;
            };

            // Merge right into node (not using separator for leaf)
            if let Node::Leaf(node) = &mut self.nodes[node_idx] {
                node.keys.extend(keys);
                node.values.extend(values);
            }

            self.free_node(right_idx);
        } else {
            // Inner node merge
            let (keys, children, counts) = if let Node::Inner(right) = &self.nodes[right_idx] {
                (
                    right.keys.clone(),
                    right.children.clone(),
                    right.subtree_counts.clone(),
                )
            } else {
                return;
            };

            if let Node::Inner(node) = &mut self.nodes[node_idx] {
                node.keys.push(separator);
                node.keys.extend(keys);
                node.children.extend(children);
                node.subtree_counts.extend(counts);
            }

            self.recalc_tree_count(node_idx);
            self.free_node(right_idx);
        }

        // Update parent tree count
        // Update parent's count for the merged child (node_idx)
        let new_child_count = self.nodes[node_idx].tree_count();
        if let Node::Inner(parent) = &mut self.nodes[parent_idx] {
            // The merged node is at child_pos
            parent.subtree_counts[child_pos] = new_child_count;
        }
    }

    /// Advance path to next entry
    fn path_next(&self, path: &mut BPTreePath) -> bool {
        if path.is_empty() {
            return false;
        }

        let (node_idx, pos) = path.last().unwrap();
        let node = &self.nodes[node_idx];

        match node {
            Node::Leaf(leaf) => {
                // Try to advance within leaf
                if pos + 1 < leaf.len() {
                    path.set_last_pos(pos + 1);
                    return true;
                }

                // Need to go up
                loop {
                    path.pop();
                    if path.is_empty() {
                        return false;
                    }

                    let (parent_idx, parent_pos) = path.last().unwrap();
                    if let Node::Inner(parent) = &self.nodes[parent_idx]
                        && parent_pos < parent.children.len() - 1
                    {
                        // Descend to leftmost leaf of next child
                        path.set_last_pos(parent_pos + 1);
                        let next_child = parent.children[parent_pos + 1];
                        self.descend_left(path, next_child);
                        return true;
                    }
                }
            }
            Node::Inner(_) => false,
        }
    }

    /// Descend to leftmost leaf from a node
    fn descend_left(&self, path: &mut BPTreePath, mut node_idx: usize) {
        loop {
            match &self.nodes[node_idx] {
                Node::Leaf(_) => {
                    path.push(node_idx, 0);
                    return;
                }
                Node::Inner(inner) => {
                    path.push(node_idx, 0);
                    node_idx = inner.children[0];
                }
            }
        }
    }

    /// Descend to rightmost leaf from a node
    fn descend_right(&self, path: &mut BPTreePath, mut node_idx: usize) {
        loop {
            match &self.nodes[node_idx] {
                Node::Leaf(_) => {
                    let len = self.nodes[node_idx].len();
                    let pos = if len > 0 { len - 1 } else { 0 };
                    path.push(node_idx, pos);
                    return;
                }
                Node::Inner(inner) => {
                    let child_pos = inner.children.len() - 1;
                    path.push(node_idx, child_pos);
                    node_idx = inner.children[child_pos];
                }
            }
        }
    }

    /// Advance path to previous entry
    fn path_prev(&self, path: &mut BPTreePath) -> bool {
        if path.is_empty() {
            return false;
        }

        let (node_idx, pos) = path.last().unwrap();

        // Try to move left in current node
        if pos > 0 {
            path.set_last_pos(pos - 1);

            if let Node::Leaf(_) = &self.nodes[node_idx] {
                return true;
            }
            // Logic for inner node descent below
        } else {
            // Need to go up
            loop {
                path.pop();
                if path.is_empty() {
                    return false;
                }
                let (_, parent_pos) = path.last().unwrap();
                if parent_pos > 0 {
                    break;
                }
            }
            // Now at parent where pos > 0. Decrement pos.
            let (_, parent_pos) = path.last().unwrap();
            path.set_last_pos(parent_pos - 1);
        }

        // Now we are at an inner node (or just moved in one).
        // We need to descend to the rightmost leaf of the child we just moved to.
        loop {
            let (node_idx, pos) = path.last().unwrap();
            match &self.nodes[node_idx] {
                Node::Leaf(_) => return true,
                Node::Inner(inner) => {
                    let child_idx = inner.children[pos];
                    // Descend to rightmost child of this child
                    let child_node = &self.nodes[child_idx];
                    match child_node {
                        Node::Leaf(l) => {
                            let p = if l.len() > 0 { l.len() - 1 } else { 0 };
                            path.push(child_idx, p);
                            return true;
                        }
                        Node::Inner(i) => {
                            let p = i.children.len() - 1;
                            path.push(child_idx, p);
                        }
                    }
                }
            }
        }
    }
}

// ============================================================================
// Iterators
// ============================================================================

/// Iterator over all entries
pub struct Iter<'a, K: Clone + Ord, V: Clone> {
    tree: &'a BPTree<K, V>,
    start_path: BPTreePath,
    end_path: BPTreePath,
    yielded_count: usize,
    total_count: usize,
}

impl<'a, K: Clone + Ord, V: Clone> Iter<'a, K, V> {
    fn new(tree: &'a BPTree<K, V>) -> Self {
        let mut start_path = BPTreePath::new();
        let mut end_path = BPTreePath::new();

        if let Some(root_idx) = tree.root {
            tree.descend_left(&mut start_path, root_idx);
            tree.descend_right(&mut end_path, root_idx);
        }

        Self {
            tree,
            start_path,
            end_path,
            yielded_count: 0,
            total_count: tree.len(),
        }
    }
}

impl<'a, K: Clone + Ord, V: Clone> Iterator for Iter<'a, K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.yielded_count >= self.total_count {
            return None;
        }

        let (node_idx, pos) = self.start_path.last()?;
        let result = if let Node::Leaf(leaf) = &self.tree.nodes[node_idx]
            && pos < leaf.len()
        {
            Some((leaf.keys[pos].clone(), leaf.values[pos].clone()))
        } else {
            None
        };

        if result.is_some() {
            self.yielded_count += 1;
            // Advance start path
            if self.yielded_count < self.total_count {
                self.tree.path_next(&mut self.start_path);
            }
        }

        result
    }
}

impl<'a, K: Clone + Ord, V: Clone> DoubleEndedIterator for Iter<'a, K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.yielded_count >= self.total_count {
            return None;
        }

        let (node_idx, pos) = self.end_path.last()?;
        let result = if let Node::Leaf(leaf) = &self.tree.nodes[node_idx]
            && pos < leaf.len()
        {
            Some((leaf.keys[pos].clone(), leaf.values[pos].clone()))
        } else {
            None
        };

        if result.is_some() {
            self.yielded_count += 1;
            // Retreat end path
            if self.yielded_count < self.total_count {
                self.tree.path_prev(&mut self.end_path);
            }
        }

        result
    }
}

/// Iterator over a range of keys
pub struct RangeIter<'a, K: Clone + Ord, V: Clone> {
    tree: &'a BPTree<K, V>,
    path: BPTreePath,
    to: K,
    started: bool,
    finished: bool,
}

impl<'a, K: Clone + Ord, V: Clone> RangeIter<'a, K, V> {
    fn new(tree: &'a BPTree<K, V>, from: K, to: K) -> Self {
        let mut path = BPTreePath::new();
        tree.locate_geq(&from, &mut path);

        let finished = path.is_empty();

        Self {
            tree,
            path,
            to,
            started: false,
            finished,
        }
    }
}

impl<'a, K: Clone + Ord, V: Clone> Iterator for RangeIter<'a, K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        if self.started {
            if !self.tree.path_next(&mut self.path) {
                self.finished = true;
                return None;
            }
        } else {
            self.started = true;
        }

        let (node_idx, pos) = self.path.last()?;
        if let Node::Leaf(leaf) = &self.tree.nodes[node_idx]
            && pos < leaf.len()
        {
            let key = &leaf.keys[pos];
            if key > &self.to {
                self.finished = true;
                return None;
            }
            return Some((key.clone(), leaf.values[pos].clone()));
        }

        self.finished = true;
        None
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_insert_get() {
        let mut tree: BPTree<i64, i64> = BPTree::new();

        tree.insert(5, 50);
        tree.insert(3, 30);
        tree.insert(7, 70);
        tree.insert(1, 10);
        tree.insert(9, 90);

        assert_eq!(tree.len(), 5);
        assert_eq!(tree.get(&5), Some(50));
        assert_eq!(tree.get(&3), Some(30));
        assert_eq!(tree.get(&7), Some(70));
        assert_eq!(tree.get(&1), Some(10));
        assert_eq!(tree.get(&9), Some(90));
        assert_eq!(tree.get(&4), None);
    }

    #[test]
    fn test_sequential_insert() {
        let mut tree: BPTree<i64, i64> = BPTree::new();

        for i in 0..1000 {
            tree.insert(i, i * 10);
        }

        assert_eq!(tree.len(), 1000);

        for i in 0..1000 {
            assert_eq!(tree.get(&i), Some(i * 10));
        }
    }

    #[test]
    fn test_reverse_insert() {
        let mut tree: BPTree<i64, i64> = BPTree::new();

        for i in (0..1000).rev() {
            tree.insert(i, i * 10);
        }

        assert_eq!(tree.len(), 1000);

        for i in 0..1000 {
            assert_eq!(tree.get(&i), Some(i * 10));
        }
    }

    #[test]
    fn test_rank_and_select() {
        let mut tree: BPTree<i64, i64> = BPTree::new();
        let count = 100;

        for i in 0..count {
            tree.insert(i * 2, i * 20); // 0, 2, 4, ...
        }

        assert_eq!(tree.len(), count as usize);

        for i in 0..count {
            // Rank of i*2 should be i
            assert_eq!(
                tree.rank(&(i * 2)),
                Some(i as usize),
                "Rank of key {}",
                i * 2
            );
            // Select of rank i should be key i*2
            let (k, v) = tree.select(i as usize).unwrap();
            assert_eq!(k, i * 2);
            assert_eq!(v, i * 20);
        }

        // Rank of non-existent key
        assert_eq!(tree.rank(&1), None);

        // Select out of bounds
        assert_eq!(tree.select(count as usize), None);
    }

    #[test]
    fn test_remove() {
        let mut tree: BPTree<i64, i64> = BPTree::new();

        for i in 0..100 {
            tree.insert(i, i * 10);
        }

        assert_eq!(tree.remove(&50), Some(500));
        assert_eq!(tree.len(), 99);
        assert_eq!(tree.get(&50), None);

        // Remove all
        for i in 0..100 {
            if i != 50 {
                tree.remove(&i);
            }
        }

        assert!(tree.is_empty());
    }

    #[test]
    fn test_range_query() {
        let mut tree: BPTree<i64, i64> = BPTree::new();

        for i in 0..100 {
            tree.insert(i, i * 10);
        }

        let results: Vec<_> = tree.range(&20, &30).collect();
        assert_eq!(results.len(), 11);
        assert_eq!(results[0], (20, 200));
        assert_eq!(results[10], (30, 300));
    }

    #[test]
    fn test_iter() {
        let mut tree: BPTree<i64, i64> = BPTree::new();

        tree.insert(5, 50);
        tree.insert(3, 30);
        tree.insert(7, 70);
        tree.insert(1, 10);
        tree.insert(9, 90);

        let entries: Vec<_> = tree.iter().collect();
        assert_eq!(entries, vec![(1, 10), (3, 30), (5, 50), (7, 70), (9, 90)]);
    }

    #[test]
    fn test_duplicate_insert() {
        let mut tree: BPTree<i64, i64> = BPTree::new();

        assert!(tree.insert(5, 50));
        assert!(!tree.insert(5, 100)); // Should not insert
        assert_eq!(tree.get(&5), Some(50)); // Original value
        assert_eq!(tree.len(), 1);
    }

    #[test]
    fn test_first_last() {
        let mut tree: BPTree<i64, i64> = BPTree::new();

        assert_eq!(tree.first(), None);
        assert_eq!(tree.last(), None);

        tree.insert(5, 50);
        assert_eq!(tree.first(), Some((5, 50)));
        assert_eq!(tree.last(), Some((5, 50)));

        tree.insert(3, 30);
        tree.insert(7, 70);
        assert_eq!(tree.first(), Some((3, 30)));
        assert_eq!(tree.last(), Some((7, 70)));

        tree.insert(1, 10);
        tree.insert(9, 90);
        assert_eq!(tree.first(), Some((1, 10)));
        assert_eq!(tree.last(), Some((9, 90)));
        assert_eq!(tree.first_key(), Some(1));
        assert_eq!(tree.last_key(), Some(9));
    }

    #[test]
    fn test_bounds() {
        let mut tree: BPTree<i64, i64> = BPTree::new();

        for i in (0..100).step_by(10) {
            tree.insert(i, i);
        }

        // lower_bound for existing key
        assert_eq!(tree.lower_bound(&30), Some((30, 30)));

        // lower_bound for non-existing key
        assert_eq!(tree.lower_bound(&35), Some((40, 40)));
    }

    #[test]
    fn bench_bptree_vs_btreemap_streams() {
        use std::collections::BTreeMap;
        use std::time::Instant;

        // Simulate Stream IDs: (ms, seq) as u128
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
        struct StreamId {
            ms: u64,
            seq: u64,
        }

        let count = 100_000u64;

        // ========== INSERT BENCHMARK ==========
        let mut bptree: BPTree<StreamId, u32> = BPTree::new();
        let start = Instant::now();
        for i in 0..count {
            let id = StreamId { ms: i, seq: 0 };
            bptree.insert(id, i as u32);
        }
        let bptree_insert = start.elapsed();

        let mut btree: BTreeMap<StreamId, u32> = BTreeMap::new();
        let start = Instant::now();
        for i in 0..count {
            let id = StreamId { ms: i, seq: 0 };
            btree.insert(id, i as u32);
        }
        let btree_insert = start.elapsed();

        // ========== POINT LOOKUP BENCHMARK ==========
        let iterations = 100_000;

        let start = Instant::now();
        for i in 0..iterations {
            let id = StreamId {
                ms: i % count,
                seq: 0,
            };
            let _ = bptree.get(&id);
        }
        let bptree_get = start.elapsed();

        let start = Instant::now();
        for i in 0..iterations {
            let id = StreamId {
                ms: i % count,
                seq: 0,
            };
            let _ = btree.get(&id);
        }
        let btree_get = start.elapsed();

        // ========== FIRST/LAST BENCHMARK (Stream needs this) ==========
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = bptree.first();
            let _ = bptree.last();
        }
        let bptree_first_last = start.elapsed();

        let start = Instant::now();
        for _ in 0..iterations {
            let _ = btree.first_key_value();
            let _ = btree.last_key_value();
        }
        let btree_first_last = start.elapsed();

        // ========== RANGE QUERY BENCHMARK (proper range, not from MIN) ==========
        let range_iterations = 10_000;
        let range_size = 100u64;

        let start = Instant::now();
        for i in 0..range_iterations {
            let from = StreamId {
                ms: (i * 10) % (count - range_size),
                seq: 0,
            };
            let to = StreamId {
                ms: from.ms + range_size,
                seq: u64::MAX,
            };
            let cnt: usize = bptree.range(&from, &to).count();
            std::hint::black_box(cnt);
        }
        let bptree_range = start.elapsed();

        let start = Instant::now();
        for i in 0..range_iterations {
            let from = StreamId {
                ms: (i * 10) % (count - range_size),
                seq: 0,
            };
            let to = StreamId {
                ms: from.ms + range_size,
                seq: u64::MAX,
            };
            let cnt: usize = btree.range(from..=to).count();
            std::hint::black_box(cnt);
        }
        let btree_range = start.elapsed();

        // ========== SEQUENTIAL REMOVE (XDEL from front) ==========
        let mut bptree_del: BPTree<StreamId, u32> = BPTree::new();
        let mut btree_del: BTreeMap<StreamId, u32> = BTreeMap::new();
        for i in 0..10_000u64 {
            let id = StreamId { ms: i, seq: 0 };
            bptree_del.insert(id, i as u32);
            btree_del.insert(id, i as u32);
        }

        let start = Instant::now();
        for i in 0..5_000u64 {
            let id = StreamId { ms: i, seq: 0 };
            bptree_del.remove(&id);
        }
        let bptree_remove = start.elapsed();

        let start = Instant::now();
        for i in 0..5_000u64 {
            let id = StreamId { ms: i, seq: 0 };
            btree_del.remove(&id);
        }
        let btree_remove = start.elapsed();

        println!("\n=== BPTree vs BTreeMap for Stream Operations ===");
        println!("Items: {}", count);
        println!();
        println!("Insert ({} items):", count);
        println!("  BPTree:   {:?}", bptree_insert);
        println!("  BTreeMap: {:?}", btree_insert);
        println!(
            "  Ratio:    {:.2}x",
            bptree_insert.as_secs_f64() / btree_insert.as_secs_f64()
        );
        println!();
        println!("Point Lookup ({} ops):", iterations);
        println!("  BPTree:   {:?}", bptree_get);
        println!("  BTreeMap: {:?}", btree_get);
        println!(
            "  Ratio:    {:.2}x",
            bptree_get.as_secs_f64() / btree_get.as_secs_f64()
        );
        println!();
        println!("First/Last ({} ops each):", iterations);
        println!("  BPTree:   {:?}", bptree_first_last);
        println!("  BTreeMap: {:?}", btree_first_last);
        println!(
            "  Ratio:    {:.2}x",
            bptree_first_last.as_secs_f64() / btree_first_last.as_secs_f64()
        );
        println!();
        println!(
            "Range Query ({} ops, {} items each):",
            range_iterations, range_size
        );
        println!("  BPTree:   {:?}", bptree_range);
        println!("  BTreeMap: {:?}", btree_range);
        println!(
            "  Ratio:    {:.2}x",
            bptree_range.as_secs_f64() / btree_range.as_secs_f64()
        );
        println!();
        println!("Remove from front (5000 ops):");
        println!("  BPTree:   {:?}", bptree_remove);
        println!("  BTreeMap: {:?}", btree_remove);
        println!(
            "  Ratio:    {:.2}x",
            bptree_remove.as_secs_f64() / btree_remove.as_secs_f64()
        );
        println!();

        // Summary
        let dominated = [
            bptree_insert < btree_insert,
            bptree_get < btree_get,
            bptree_first_last < btree_first_last,
            bptree_range < btree_range,
            bptree_remove < btree_remove,
        ];
        let wins = dominated.iter().filter(|&&x| x).count();
        println!("BPTree wins: {}/5 benchmarks", wins);
        if wins >= 3 {
            println!(">>> BPTree is RECOMMENDED for Streams");
        } else {
            println!(">>> BTreeMap is RECOMMENDED for Streams");
        }
    }

    #[test]
    fn bench_bptree_vs_btreeset_sorted_sets() {
        use std::collections::BTreeSet;
        use std::time::Instant;

        // Simulate SortedSet: (score, member) pairs
        // This is exactly how Dragonfly uses BPTree for ZSET
        #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
        struct ScoreMember {
            score: i64, // Using i64 to represent OrderedFloat
            member: u64,
        }

        let count = 100_000u64;

        // ========== INSERT BENCHMARK (ZADD) ==========
        let mut bptree: BPTree<ScoreMember, ()> = BPTree::new();
        let start = Instant::now();
        for i in 0..count {
            let sm = ScoreMember {
                score: (i % 1000) as i64,
                member: i,
            };
            bptree.insert(sm, ());
        }
        let bptree_insert = start.elapsed();

        let mut btreeset: BTreeSet<ScoreMember> = BTreeSet::new();
        let start = Instant::now();
        for i in 0..count {
            let sm = ScoreMember {
                score: (i % 1000) as i64,
                member: i,
            };
            btreeset.insert(sm);
        }
        let btreeset_insert = start.elapsed();

        // ========== RANK QUERY (ZRANK) - BPTree's key advantage ==========
        let rank_iterations = 50_000;

        let start = Instant::now();
        for i in 0..rank_iterations {
            let sm = ScoreMember {
                score: (i % 1000) as i64,
                member: i % count,
            };
            let _ = bptree.rank(&sm);
        }
        let bptree_rank = start.elapsed();

        // BTreeSet doesn't have rank, must iterate to count
        let start = Instant::now();
        for i in 0..rank_iterations {
            let sm = ScoreMember {
                score: (i % 1000) as i64,
                member: i % count,
            };
            // Simulate rank by counting elements less than target
            let _rank: usize = btreeset.range(..&sm).count();
        }
        let btreeset_rank = start.elapsed();

        // ========== SELECT BY RANK (ZRANGE by index) - BPTree advantage ==========
        let select_iterations = 50_000;

        let start = Instant::now();
        for i in 0..select_iterations {
            let rank = (i as usize) % bptree.len();
            let _ = bptree.select(rank);
        }
        let bptree_select = start.elapsed();

        // BTreeSet must iterate to find nth element
        let start = Instant::now();
        for i in 0..select_iterations {
            let rank = (i as usize) % btreeset.len();
            let _ = btreeset.iter().nth(rank);
        }
        let btreeset_select = start.elapsed();

        // ========== RANGE BY SCORE (ZRANGEBYSCORE) ==========
        let range_iterations = 10_000;

        let start = Instant::now();
        for i in 0..range_iterations {
            let from = ScoreMember {
                score: (i % 900) as i64,
                member: 0,
            };
            let to = ScoreMember {
                score: (i % 900) as i64 + 100,
                member: u64::MAX,
            };
            let cnt: usize = bptree.range(&from, &to).count();
            std::hint::black_box(cnt);
        }
        let bptree_range = start.elapsed();

        let start = Instant::now();
        for i in 0..range_iterations {
            let from = ScoreMember {
                score: (i % 900) as i64,
                member: 0,
            };
            let to = ScoreMember {
                score: (i % 900) as i64 + 100,
                member: u64::MAX,
            };
            let cnt: usize = btreeset.range(from..=to).count();
            std::hint::black_box(cnt);
        }
        let btreeset_range = start.elapsed();

        // ========== REMOVE (ZREM) ==========
        let mut bptree_del: BPTree<ScoreMember, ()> = BPTree::new();
        let mut btreeset_del: BTreeSet<ScoreMember> = BTreeSet::new();
        for i in 0..10_000u64 {
            let sm = ScoreMember {
                score: i as i64,
                member: i,
            };
            bptree_del.insert(sm.clone(), ());
            btreeset_del.insert(sm);
        }

        let start = Instant::now();
        for i in 0..5_000u64 {
            let sm = ScoreMember {
                score: i as i64,
                member: i,
            };
            bptree_del.remove(&sm);
        }
        let bptree_remove = start.elapsed();

        let start = Instant::now();
        for i in 0..5_000u64 {
            let sm = ScoreMember {
                score: i as i64,
                member: i,
            };
            btreeset_del.remove(&sm);
        }
        let btreeset_remove = start.elapsed();

        println!("\n=== BPTree vs BTreeSet for SortedSet (ZSET) Operations ===");
        println!("Items: {}", count);
        println!();
        println!("ZADD - Insert ({} items):", count);
        println!("  BPTree:   {:?}", bptree_insert);
        println!("  BTreeSet: {:?}", btreeset_insert);
        println!(
            "  Ratio:    {:.2}x",
            bptree_insert.as_secs_f64() / btreeset_insert.as_secs_f64()
        );
        println!();
        println!("ZRANK - Get rank ({} ops):", rank_iterations);
        println!("  BPTree:   {:?}", bptree_rank);
        println!("  BTreeSet: {:?}", btreeset_rank);
        println!(
            "  Ratio:    {:.2}x",
            bptree_rank.as_secs_f64() / btreeset_rank.as_secs_f64()
        );
        println!();
        println!(
            "ZRANGE by index - Select by rank ({} ops):",
            select_iterations
        );
        println!("  BPTree:   {:?}", bptree_select);
        println!("  BTreeSet: {:?}", btreeset_select);
        println!(
            "  Ratio:    {:.2}x",
            bptree_select.as_secs_f64() / btreeset_select.as_secs_f64()
        );
        println!();
        println!("ZRANGEBYSCORE - Range by score ({} ops):", range_iterations);
        println!("  BPTree:   {:?}", bptree_range);
        println!("  BTreeSet: {:?}", btreeset_range);
        println!(
            "  Ratio:    {:.2}x",
            bptree_range.as_secs_f64() / btreeset_range.as_secs_f64()
        );
        println!();
        println!("ZREM - Remove (5000 ops):");
        println!("  BPTree:   {:?}", bptree_remove);
        println!("  BTreeSet: {:?}", btreeset_remove);
        println!(
            "  Ratio:    {:.2}x",
            bptree_remove.as_secs_f64() / btreeset_remove.as_secs_f64()
        );
        println!();

        // Summary - for ZSET, rank and select are critical operations
        let bptree_rank_wins = bptree_rank < btreeset_rank;
        let bptree_select_wins = bptree_select < btreeset_select;

        println!("=== CRITICAL ZSET OPERATIONS ===");
        println!(
            "ZRANK:  BPTree is {:.1}x {}",
            if bptree_rank_wins {
                btreeset_rank.as_secs_f64() / bptree_rank.as_secs_f64()
            } else {
                bptree_rank.as_secs_f64() / btreeset_rank.as_secs_f64()
            },
            if bptree_rank_wins { "FASTER" } else { "SLOWER" }
        );
        println!(
            "ZRANGE: BPTree is {:.1}x {}",
            if bptree_select_wins {
                btreeset_select.as_secs_f64() / bptree_select.as_secs_f64()
            } else {
                bptree_select.as_secs_f64() / btreeset_select.as_secs_f64()
            },
            if bptree_select_wins {
                "FASTER"
            } else {
                "SLOWER"
            }
        );

        if bptree_rank_wins && bptree_select_wins {
            println!(">>> BPTree is RECOMMENDED for SortedSets (rank/select operations)");
        } else if bptree_rank_wins || bptree_select_wins {
            println!(">>> BPTree has MIXED results - consider workload");
        } else {
            println!(">>> BTreeSet is RECOMMENDED for SortedSets");
        }
    }

    #[test]
    fn test_large_tree() {
        let mut tree: BPTree<i64, i64> = BPTree::new();
        let n = 100_000i64;

        for i in 0..n {
            tree.insert(i, i);
        }

        assert_eq!(tree.len(), n as usize);

        // Verify some random lookups
        for i in (0..n).step_by(1000) {
            assert_eq!(tree.get(&i), Some(i));
        }

        // Verify range query
        let results: Vec<_> = tree.range(&50000, &50100).collect();
        assert_eq!(results.len(), 101);
    }
}
