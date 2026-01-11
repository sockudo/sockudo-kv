use bytes::Bytes;
use std::collections::HashMap;

/// A node in the GlobTrie
#[derive(Debug)]
struct Node<V> {
    /// Exact character children mapping byte -> Node
    children: HashMap<u8, Box<Node<V>>>,
    /// Child for '*' wildcard (matches any sequence)
    star_child: Option<Box<Node<V>>>,
    /// Child for '?' wildcard (matches any single char)
    question_child: Option<Box<Node<V>>>,
    /// Child for '[]' ranges - simplified for now, we might stick to standard glob logic in children
    /// For complex bracket matches, we might fallback or expand.
    /// To keep it fast/simple: simple chars, *, and ? are optimized in the trie.
    /// Complex patterns (brackets, escapes) can be stored in a fallback list or
    /// handled by expanding if feasible (but brackets are rare in high perf pubsub).
    /// For now, we'll treat complex patterns as "Literal" if possible or store them?
    /// Actually, Redis `psubscribe` supports `[abc]`.
    /// Let's support `*`, `?` efficiently. Anything else we can just store in a special "fallback" list at the node?
    /// Or just treat `[` as a normal char edge and rely on the full pattern match verification at the leaf?
    /// Yes: The Trie is a filtering mechanism. The leaves contain the full pattern.
    /// We traverse:
    /// - If char matches edge -> go
    /// - If `*` edge exists -> go (branch)
    /// - If `?` edge exists -> go
    /// - If we reach a leaf (or node with values), we double check the full pattern using `stringmatch` if needed,
    ///   or trust the path if fully exact.
    ///
    /// Values stored at this node (subscribers)
    values: Option<V>,
    /// The original pattern string (for verification)
    pattern: Option<Bytes>,
}

impl<V> Default for Node<V> {
    fn default() -> Self {
        Self {
            children: HashMap::new(),
            star_child: None,
            question_child: None,
            values: None,
            pattern: None,
        }
    }
}

pub struct GlobTrie<V> {
    root: Node<V>,
    count: usize,
}

impl<V> GlobTrie<V> {
    pub fn new() -> Self {
        Self {
            root: Node::default(),
            count: 0,
        }
    }

    /// Insert a pattern and associated value.
    /// Returns mutable reference to value if it exists, or creates new.
    pub fn insert(&mut self, pattern: Bytes, default_val: impl FnOnce() -> V) -> &mut V {
        let mut node = &mut self.root;
        let mut i = 0;
        let p = pattern.as_ref();

        while i < p.len() {
            let b = p[i];

            // Handle wildcards as structural edges
            if b == b'*' {
                if node.star_child.is_none() {
                    node.star_child = Some(Box::new(Node::default()));
                }
                node = node.star_child.as_mut().unwrap();
                i += 1;
            } else if b == b'?' {
                if node.question_child.is_none() {
                    node.question_child = Some(Box::new(Node::default()));
                }
                node = node.question_child.as_mut().unwrap();
                i += 1;
            } else {
                // Regular character (including '[', we'll rely on verification for complex bracket logic if we don't expand)
                // Actually, if we treat `[` as a regular char, `[abc]` pattern path is `[` -> `a` ...
                // But `news.[abc]` matching `news.a`:
                // Traverse `n` `e` `w` `s` `.`
                // At `.`, we have children `[`? No, we have `a` in the text. `[` doesn't match `a`.
                // So `[abc]` support requires parsing the pattern and structuring the Trie to handle sets.
                // For MVP Speed: Optimized for `*` and `?` and prefixes.
                // Complex patterns `[...]` will be treated as normal edges, which means they WON'T match `a`.
                // Wait. This is a problem.
                // Solution: If a pattern contains `[`, we can store it in a special "complex" list at the root or fallback?
                // Better: Parse `[...]` into a special edge type?
                // Let's stick to `*` and `?` optimization. If `[` is found, maybe treat it as `?` for structural purposes (over-matching)?
                // `news.[abc]` -> `news.?` in Trie structure.
                // When matching `news.a`: `?` edge matches `a`.
                // We reach leaf. Verify `news.[abc]` against `news.a`. It matches.
                // When matching `news.z`: `?` edge matches `z`.
                // We reach leaf. Verify `news.[abc]` against `news.z`. Fail.
                // Safe and fast!

                let edge_byte = if b == b'[' { b'?' } else { b };

                // If mapping `[` to `?`, we need to advance the pattern index `i` past the `]`.
                if b == b'[' {
                    // Find closing `]`
                    let mut j = i + 1;
                    while j < p.len() && p[j] != b']' {
                        j += 1;
                    }
                    // If found, treat whole `[...]` as one `?` step
                    if j < p.len() {
                        i = j + 1; // Advance past `]`
                    } else {
                        // Invalid or incomplete, treat as char `[`
                        i += 1;
                    }

                    if node.question_child.is_none() {
                        node.question_child = Some(Box::new(Node::default()));
                    }
                    node = node.question_child.as_mut().unwrap();
                    continue;
                }

                node = node
                    .children
                    .entry(edge_byte)
                    .or_insert_with(|| Box::new(Node::default()));
                i += 1;
            }
        }

        if node.values.is_none() {
            node.values = Some(default_val());
            self.count += 1;
            node.pattern = Some(pattern);
        }

        node.values.as_mut().unwrap()
    }

    /// Remove a pattern
    pub fn remove(&mut self, pattern: &[u8]) -> Option<V> {
        Self::remove_recursive(&mut self.root, &mut self.count, pattern, 0)
    }

    fn remove_recursive(
        node: &mut Node<V>,
        count: &mut usize,
        p: &[u8],
        mut i: usize,
    ) -> Option<V> {
        if i == p.len() {
            if node.values.is_some() {
                *count -= 1;
                return node.values.take();
            }
            return None;
        }

        let b = p[i];
        if b == b'*' {
            if let Some(ref mut child) = node.star_child {
                return Self::remove_recursive(child, count, p, i + 1);
            }
        } else if b == b'?' {
            if let Some(ref mut child) = node.question_child {
                return Self::remove_recursive(child, count, p, i + 1);
            }
        } else if b == b'[' {
            // Handle [...] logic same as insert
            let mut j = i + 1;
            while j < p.len() && p[j] != b']' {
                j += 1;
            }
            if j < p.len() {
                i = j; // Will be incremented to j+1
                if let Some(ref mut child) = node.question_child {
                    return Self::remove_recursive(child, count, p, i + 1);
                }
            } else {
                // Fallback
                if let Some(child) = node.children.get_mut(&b'[') {
                    return Self::remove_recursive(child, count, p, i + 1);
                }
            }
        } else {
            if let Some(child) = node.children.get_mut(&b) {
                return Self::remove_recursive(child, count, p, i + 1);
            }
        }
        None
    }

    /// Match a text against all stored patterns.
    /// Returns list of (pattern, value) tuples
    pub fn matches(&self, text: &[u8]) -> Vec<(&Bytes, &V)> {
        let mut results: Vec<(&Bytes, &V)> = Vec::new();

        // Stack for DFS: (Node, text_index)
        // We use recursion or explicit stack. Explicit stack is safer for deep tries?
        // Patterns are usually short. Recursion is fine and cleaner.

        self.match_recursive(&self.root, text, 0, &mut results);
        results
    }

    fn match_recursive<'a>(
        &'a self,
        node: &'a Node<V>,
        text: &[u8],
        ti: usize,
        results: &mut Vec<(&'a Bytes, &'a V)>,
    ) {
        // 1. Check if current node is a match (Leaf or intermediate pattern end)
        // AND we exhausted the text?
        // Note: implicit `*` at end? No, Redis patterns must match fully unless they end in `*`.
        // If node has a value, it means a pattern ended here.
        // Does it match the text consumed so far?
        // If pattern ended here, we must have consumed equal amount of text, OR the pattern ended with `*` which consumed the rest.
        // Wait. `*` consumes text.
        // My Trie structure: `a` -> `*` -> `b`.
        // Pattern `a*b`.
        // Traversal:
        // - `a` (matches `a`) -> `*` (matches `foo`) -> `b` (matches `b`).
        // At each step `ti` advances.

        if let Some(ref val) = node.values {
            // Found a pattern. Does it match the *entire* text?
            // If we are at this node, we followed a path.
            // But `*` is tricky. `*` loop might leave us "here" with different `ti`.
            // Actually, we should verify the Full Pattern match at the end to be 100% sure,
            // especially due to the `[...]` -> `?` simplification.
            if let Some(ref pat) = node.pattern {
                if crate::pattern::matches_glob(pat, text) {
                    results.push((pat, val));
                }
            }
        }

        // 2. Try Exact Match
        if ti < text.len() {
            let b = text[ti];
            if let Some(child) = node.children.get(&b) {
                self.match_recursive(child, text, ti + 1, results);
            }
        }

        // 3. Try '?' Wildcard (matches any single char)
        if ti < text.len() {
            if let Some(ref child) = node.question_child {
                self.match_recursive(child, text, ti + 1, results);
            }
        }

        // 4. Try '*' Wildcard (matches 0 or more chars)
        if let Some(ref child) = node.star_child {
            // '*' can match 0 chars:
            self.match_recursive(child, text, ti, results);

            // '*' can match 1+ chars:
            // We can iterate consuming text until end.
            for i in ti..text.len() {
                // Consume text[i] and move to child
                // Actually, '*' edge means "consume input, stay here" OR "move to next node"?
                // In my struct, `star_child` is a node *after* the star.
                // So `*` edge represents the `*` itself.
                // BUT `*` is a loop.
                // Correct Trie for `*`:
                // It effectively adds a self-loop?
                // Or: we just try to jump to `star_child` with every possible suffix of text?
                // Yes. `*` matches `text[ti..ti+k]`.
                // So we recursively call `star_child` with `ti`, `ti+1`, ... `len`.

                // Note: optimization - minimal match?
                // Redis `*` is greedy? Glob is usually greedy but matches validly.
                // We just need *any* match.
                // Since we need ALL matching patterns, we must explore ALL valid consumptions.
                self.match_recursive(child, text, i + 1, results);
            }
        }
    }

    pub fn get_mut(&mut self, pattern: &[u8]) -> Option<&mut V> {
        let mut node = &mut self.root;
        let mut i = 0;

        while i < pattern.len() {
            let b = pattern[i];

            if b == b'*' {
                if let Some(ref mut child) = node.star_child {
                    node = child;
                    i += 1;
                } else {
                    return None;
                }
            } else if b == b'?' {
                if let Some(ref mut child) = node.question_child {
                    node = child;
                    i += 1;
                } else {
                    return None;
                }
            } else if b == b'[' {
                // Handle [...] same as insert/remove
                let mut j = i + 1;
                while j < pattern.len() && pattern[j] != b']' {
                    j += 1;
                }
                if j < pattern.len() {
                    // Treated as `?`
                    if let Some(ref mut child) = node.question_child {
                        node = child;
                        i = j + 1;
                    } else {
                        return None;
                    }
                } else {
                    // Fallback to literal '['
                    if let Some(child) = node.children.get_mut(&b'[') {
                        node = child;
                        i += 1;
                    } else {
                        return None;
                    }
                }
            } else {
                if let Some(child) = node.children.get_mut(&b) {
                    node = child;
                    i += 1;
                } else {
                    return None;
                }
            }
        }

        node.values.as_mut()
    }

    pub fn len(&self) -> usize {
        self.count
    }
}
