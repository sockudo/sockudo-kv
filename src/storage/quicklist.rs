use bytes::Bytes;
use std::collections::VecDeque;

/// Default sizes for different fill values (negative means size-based)
/// -1 = 4KB, -2 = 8KB, -3 = 16KB, -4 = 32KB, -5 = 64KB
const SIZE_4KB: usize = 4 * 1024;
const SIZE_8KB: usize = 8 * 1024;
const SIZE_16KB: usize = 16 * 1024;
const SIZE_32KB: usize = 32 * 1024;
const SIZE_64KB: usize = 64 * 1024;

/// Convert fill value to max size per node
fn fill_to_max_size(fill: i32) -> usize {
    match fill {
        -1 => SIZE_4KB,
        -2 => SIZE_8KB,
        -3 => SIZE_16KB,
        -4 => SIZE_32KB,
        -5 => SIZE_64KB,
        n if n < -5 => SIZE_64KB, // Treat < -5 as -5
        _ => usize::MAX,          // Positive fill means count-based, not size-based
    }
}

/// Convert fill value to max entries per node (for positive fill values)
fn fill_to_max_entries(fill: i32) -> usize {
    if fill <= 0 {
        usize::MAX // Size-based, no entry limit
    } else {
        fill as usize
    }
}

/// A packed block of list items (listpack node)
/// Format: [count: u16] [item1_len: varint] [item1_data] ...
#[derive(Clone, Debug)]
pub struct PackedBlock {
    data: Vec<u8>,
    count: u16,
}

impl Default for PackedBlock {
    fn default() -> Self {
        let mut data = Vec::with_capacity(512);
        // Reserve 2 bytes for count
        data.extend_from_slice(&0u16.to_le_bytes());
        Self { data, count: 0 }
    }
}

impl PackedBlock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.count as usize
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn size_bytes(&self) -> usize {
        self.data.len()
    }

    /// Check if block can accept more items given the fill constraints
    pub fn can_insert(&self, item_size: usize, fill: i32) -> bool {
        let max_size = fill_to_max_size(fill);
        let max_entries = fill_to_max_entries(fill);

        // Check entry count limit
        if self.count as usize >= max_entries {
            return false;
        }

        // Check size limit (estimate: varint overhead max 9 bytes + item size)
        let needed = item_size + 9;
        if self.data.len() + needed > max_size {
            return false;
        }

        true
    }

    /// Push item to back
    pub fn push_back(&mut self, item: &[u8]) {
        write_varint(&mut self.data, item.len() as u64);
        self.data.extend_from_slice(item);
        self.count += 1;
        self.update_count();
    }

    /// Push item to front
    pub fn push_front(&mut self, item: &[u8]) {
        // Must shift data
        // Header is 2 bytes
        // Move [2..] to [2 + item_size..]
        let mut buf = Vec::new();
        write_varint(&mut buf, item.len() as u64);
        buf.extend_from_slice(item);
        let item_size = buf.len();

        let old_len = self.data.len();
        self.data.resize(old_len + item_size, 0);
        self.data.copy_within(2..old_len, 2 + item_size);
        self.data[2..2 + item_size].copy_from_slice(&buf);

        self.count += 1;
        self.update_count();
    }

    /// Pop item from back
    pub fn pop_back(&mut self) -> Option<Bytes> {
        if self.count == 0 {
            return None;
        }

        // We need to find the last item start position
        // This is slow: O(N) to walk from start.
        let mut pos = 2;
        for _ in 0..self.count - 1 {
            let (len, next) = read_varint(&self.data, pos).ok()?;
            pos = next + len as usize;
        }

        // At last item
        let (len, next) = read_varint(&self.data, pos).ok()?;
        let val = Bytes::copy_from_slice(&self.data[next..next + len as usize]);

        // Truncate
        self.data.truncate(pos);
        self.count -= 1;
        self.update_count();

        Some(val)
    }

    /// Pop item from front
    pub fn pop_front(&mut self) -> Option<Bytes> {
        if self.count == 0 {
            return None;
        }

        let (len, next) = read_varint(&self.data, 2).ok()?;
        let val = Bytes::copy_from_slice(&self.data[next..next + len as usize]);
        let item_size = next + len as usize - 2;

        // Shift rest left
        let total_len = self.data.len();
        self.data.copy_within(next + len as usize..total_len, 2);
        self.data.truncate(total_len - item_size);

        self.count -= 1;
        self.update_count();
        Some(val)
    }

    fn update_count(&mut self) {
        let b = self.count.to_le_bytes();
        self.data[0] = b[0];
        self.data[1] = b[1];
    }

    pub fn get(&self, idx: usize) -> Option<Bytes> {
        if idx >= self.count as usize {
            return None;
        }
        let mut pos = 2;
        for _ in 0..idx {
            let (len, next) = read_varint(&self.data, pos).ok()?;
            pos = next + len as usize;
        }
        let (len, next) = read_varint(&self.data, pos).ok()?;
        Some(Bytes::copy_from_slice(
            &self.data[next..next + len as usize],
        ))
    }

    pub fn iter(&self) -> PackedBlockIter<'_> {
        PackedBlockIter {
            block: self,
            pos: 2,
            idx: 0,
        }
    }
}

pub struct PackedBlockIter<'a> {
    block: &'a PackedBlock,
    pos: usize,
    idx: u16,
}

impl<'a> Iterator for PackedBlockIter<'a> {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.block.count {
            return None;
        }
        let (len, next) = read_varint(&self.block.data, self.pos).ok()?;
        let val = Bytes::copy_from_slice(&self.block.data[next..next + len as usize]);
        self.pos = next + len as usize;
        self.idx += 1;
        Some(val)
    }
}

/// A double-linked list of packed blocks (quicklist)
///
/// The `fill` parameter controls how nodes are sized:
/// - Positive values (1, 2, 3, ...): max number of entries per node
/// - Negative values: max size per node (-1=4KB, -2=8KB, -3=16KB, -4=32KB, -5=64KB)
/// - 0 is treated as 1
#[derive(Clone, Debug)]
pub struct QuickList {
    blocks: VecDeque<PackedBlock>,
    len: usize,
    fill: i32,
}

impl Default for QuickList {
    fn default() -> Self {
        Self {
            blocks: VecDeque::new(),
            len: 0,
            fill: -2, // Default: 8KB per node
        }
    }
}

impl QuickList {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new QuickList with a specific fill value
    pub fn with_fill(fill: i32) -> Self {
        // Normalize fill: 0 is treated as 1
        let fill = if fill == 0 { 1 } else { fill };
        Self {
            blocks: VecDeque::new(),
            len: 0,
            fill,
        }
    }

    /// Set the fill value (used when config changes)
    pub fn set_fill(&mut self, fill: i32) {
        self.fill = if fill == 0 { 1 } else { fill };
    }

    /// Get the current fill value
    pub fn fill(&self) -> i32 {
        self.fill
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the number of internal blocks (nodes) in the quicklist.
    pub fn node_count(&self) -> usize {
        self.blocks.len()
    }

    /// Check if this list should be considered "listpack" encoding.
    /// In Redis 7.0+:
    /// - If there's only 1 node AND the node doesn't exceed size limits, it's "listpack"
    /// - If there are multiple nodes OR a single oversized node, it's "quicklist"
    pub fn is_listpack_encoding(&self) -> bool {
        if self.blocks.len() > 1 {
            return false;
        }
        if self.blocks.len() == 1 {
            let block = &self.blocks[0];
            let max_size = fill_to_max_size(self.fill);
            let max_entries = fill_to_max_entries(self.fill);
            // If the block exceeds limits, it's a "plain node" -> quicklist encoding
            if block.size_bytes() > max_size || block.len() > max_entries {
                return false;
            }
        }
        true
    }

    pub fn push_back(&mut self, item: Bytes) {
        if let Some(tail) = self.blocks.back_mut() {
            if tail.can_insert(item.len(), self.fill) {
                tail.push_back(&item);
                self.len += 1;
                return;
            }
        }
        // New block needed
        let mut block = PackedBlock::new();
        block.push_back(&item);
        self.blocks.push_back(block);
        self.len += 1;
    }

    pub fn push_front(&mut self, item: Bytes) {
        if let Some(head) = self.blocks.front_mut() {
            if head.can_insert(item.len(), self.fill) {
                head.push_front(&item);
                self.len += 1;
                return;
            }
        }
        // New block needed
        let mut block = PackedBlock::new();
        block.push_front(&item);
        self.blocks.push_front(block);
        self.len += 1;
    }

    pub fn pop_back(&mut self) -> Option<Bytes> {
        if self.len == 0 {
            return None;
        }

        let mut val = None;
        if let Some(tail) = self.blocks.back_mut() {
            val = tail.pop_back();
        }

        if let Some(v) = val {
            self.len -= 1;
            if self.blocks.back().is_some_and(|b| b.is_empty()) {
                self.blocks.pop_back();
            }
            Some(v)
        } else {
            // Tail was empty? Should be swept.
            self.blocks.pop_back();
            self.pop_back()
        }
    }

    pub fn pop_front(&mut self) -> Option<Bytes> {
        if self.len == 0 {
            return None;
        }

        let mut val = None;
        if let Some(head) = self.blocks.front_mut() {
            val = head.pop_front();
        }

        if let Some(v) = val {
            self.len -= 1;
            if self.blocks.front().is_some_and(|b| b.is_empty()) {
                self.blocks.pop_front();
            }
            Some(v)
        } else {
            self.blocks.pop_front();
            self.pop_front()
        }
    }

    pub fn get(&self, mut index: usize) -> Option<Bytes> {
        if index >= self.len {
            return None;
        }

        // Find block
        for block in &self.blocks {
            let count = block.len();
            if index < count {
                return block.get(index);
            }
            index -= count;
        }
        None
    }

    // Quick iterator over all elements
    pub fn iter(&self) -> QuickListIter<'_> {
        QuickListIter {
            list: self,
            block_idx: 0,
            inner_iter: if self.blocks.is_empty() {
                None
            } else {
                Some(self.blocks[0].iter())
            },
        }
    }

    // Clear list
    pub fn clear(&mut self) {
        self.blocks.clear();
        self.len = 0;
    }

    // Insert at index
    pub fn insert(&mut self, index: usize, item: Bytes) {
        if index >= self.len {
            self.push_back(item);
            return;
        }
        if index == 0 {
            self.push_front(item);
            return;
        }

        // Find block containing the index
        let mut block_idx = 0;
        let mut offset = index;

        for (i, block) in self.blocks.iter().enumerate() {
            let count = block.len();
            if offset <= count {
                block_idx = i;
                break;
            }
            offset -= count;
        }

        // Extract all items from the block
        let block = &mut self.blocks[block_idx];
        let mut items: Vec<Bytes> = block.iter().collect();
        items.insert(offset, item);

        // Rebuild block(s) respecting fill constraints
        self.rebuild_block(block_idx, items);
        self.len += 1;
    }

    /// Rebuild a block from items, potentially splitting into multiple blocks
    fn rebuild_block(&mut self, block_idx: usize, items: Vec<Bytes>) {
        if items.is_empty() {
            self.blocks.remove(block_idx);
            return;
        }

        // Create new blocks from items
        let mut new_blocks: Vec<PackedBlock> = Vec::new();
        let mut current_block = PackedBlock::new();

        for item in items {
            if current_block.can_insert(item.len(), self.fill) {
                current_block.push_back(&item);
            } else {
                // Current block is full, start a new one
                if !current_block.is_empty() {
                    new_blocks.push(current_block);
                }
                current_block = PackedBlock::new();
                current_block.push_back(&item);
            }
        }

        if !current_block.is_empty() {
            new_blocks.push(current_block);
        }

        // Replace the old block with new blocks
        self.blocks.remove(block_idx);
        for (i, block) in new_blocks.into_iter().enumerate() {
            self.blocks.insert(block_idx + i, block);
        }
    }

    pub fn set(&mut self, index: usize, item: Bytes) -> bool {
        if index >= self.len {
            return false;
        }

        // Find block
        let mut block_idx = 0;
        let mut offset = index;
        for (i, block) in self.blocks.iter().enumerate() {
            let count = block.len();
            if offset < count {
                block_idx = i;
                break;
            }
            offset -= count;
        }

        // Update in block - rebuild to respect fill constraints
        let block = &mut self.blocks[block_idx];
        let mut items: Vec<Bytes> = block.iter().collect();
        items[offset] = item;

        self.rebuild_block(block_idx, items);
        true
    }

    pub fn remove(&mut self, index: usize) -> Option<Bytes> {
        if index >= self.len {
            return None;
        }

        let mut block_idx = 0;
        let mut offset = index;
        for (i, block) in self.blocks.iter().enumerate() {
            let count = block.len();
            if offset < count {
                block_idx = i;
                break;
            }
            offset -= count;
        }

        // Remove from block
        let block = &mut self.blocks[block_idx];
        let mut items: Vec<Bytes> = block.iter().collect();
        let val = items.remove(offset);

        if items.is_empty() {
            self.blocks.remove(block_idx);
        } else {
            let mut new_block = PackedBlock::new();
            for it in items {
                new_block.push_back(&it);
            }
            self.blocks[block_idx] = new_block;
        }
        self.len -= 1;
        Some(val)
    }
}

pub struct QuickListIter<'a> {
    list: &'a QuickList,
    block_idx: usize,
    inner_iter: Option<PackedBlockIter<'a>>,
}

impl<'a> Iterator for QuickListIter<'a> {
    type Item = Bytes;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(iter) = &mut self.inner_iter {
            if let Some(val) = iter.next() {
                return Some(val);
            }
        }

        // Next block
        self.block_idx += 1;
        if self.block_idx < self.list.blocks.len() {
            self.inner_iter = Some(self.list.blocks[self.block_idx].iter());
            return self.next();
        }

        None
    }
}

// --- Varint Utils ---

fn write_varint(buf: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        buf.push((n as u8) | 0x80);
        n >>= 7;
    }
    buf.push(n as u8);
}

fn read_varint(data: &[u8], mut pos: usize) -> Result<(u64, usize), ()> {
    let mut result = 0u64;
    let mut shift = 0;
    while pos < data.len() {
        let byte = data[pos];
        pos += 1;
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok((result, pos));
        }
        shift += 7;
    }
    Err(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quicklist_with_fill_1() {
        // fill=1 means max 1 entry per node
        let mut list = QuickList::with_fill(1);
        list.push_back(Bytes::from("a"));
        list.push_back(Bytes::from("b"));
        list.push_back(Bytes::from("c"));

        assert_eq!(list.len(), 3);
        assert_eq!(list.node_count(), 3); // Each item in its own node
        assert!(!list.is_listpack_encoding()); // quicklist encoding
    }

    #[test]
    fn test_quicklist_with_fill_negative() {
        // fill=-2 means 8KB per node
        let mut list = QuickList::with_fill(-2);
        list.push_back(Bytes::from("a"));
        list.push_back(Bytes::from("b"));
        list.push_back(Bytes::from("c"));

        assert_eq!(list.len(), 3);
        assert_eq!(list.node_count(), 1); // All fit in one node
        assert!(list.is_listpack_encoding()); // listpack encoding
    }

    #[test]
    fn test_quicklist_fill_0_treated_as_1() {
        let mut list = QuickList::with_fill(0);
        list.push_back(Bytes::from("a"));
        list.push_back(Bytes::from("b"));

        assert_eq!(list.node_count(), 2); // fill=0 treated as fill=1
    }
}
