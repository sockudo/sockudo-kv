use bytes::Bytes;
use std::collections::VecDeque;

const BLOCK_SIZE: usize = 8 * 1024; // 8KB config

/// A packed block of list items
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

    pub fn free_space(&self) -> usize {
        if self.data.len() >= BLOCK_SIZE {
            0
        } else {
            BLOCK_SIZE - self.data.len()
        }
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
        // For optimization, we could store offsets?
        // For now, walk.
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

/// A double-linked list of packed blocks
#[derive(Clone, Debug, Default)]
pub struct QuickList {
    blocks: VecDeque<PackedBlock>,
    len: usize,
}

impl QuickList {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn push_back(&mut self, item: Bytes) {
        if let Some(tail) = self.blocks.back_mut() {
            // Estimate size: varint overhead (max 9) + len
            let needed = item.len() + 9;
            if tail.free_space() >= needed {
                tail.push_back(&item);
                self.len += 1;
                return;
            }
        }
        // New block
        let mut block = PackedBlock::new();
        block.push_back(&item);
        self.blocks.push_back(block);
        self.len += 1;
    }

    pub fn push_front(&mut self, item: Bytes) {
        if let Some(head) = self.blocks.front_mut() {
            let needed = item.len() + 9;
            if head.free_space() >= needed {
                head.push_front(&item);
                self.len += 1;
                return;
            }
        }
        // New block
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
            if self.blocks.back().unwrap().is_empty() {
                self.blocks.pop_back();
            }
            Some(v)
        } else {
            // Tail was empty? Should be swept.
            self.blocks.pop_back();
            // Try again recurse?
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
            if self.blocks.front().unwrap().is_empty() {
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

    // Insert at index (slow)
    pub fn insert(&mut self, index: usize, item: Bytes) {
        if index >= self.len {
            self.push_back(item);
            return;
        }
        if index == 0 {
            self.push_front(item);
            return;
        }

        // Find block and split it? or insert into it?
        // Simplest MVP: Convert to VecDeque, insert, convert back? No.
        // Split block at index.
        let mut block_idx = 0;
        let mut offset = index;

        for (i, block) in self.blocks.iter().enumerate() {
            let count = block.len();
            if offset <= count {
                // Found block
                block_idx = i;
                break;
            }
            offset -= count;
        }

        // We are at `offset` inside `block_idx`.
        // Check if block has space
        // This is complex for MVP.
        // FALLBACK: Deconstruct whole block, insert, reconstruct.
        // Since block size is small (8KB), this is fine.

        let block = &mut self.blocks[block_idx];
        // Extract all items
        let mut items: Vec<Bytes> = block.iter().collect();
        items.insert(offset, item);

        // Rebuild block(s) from these items
        let mut new_block = PackedBlock::new();
        // If items overflow 8KB, we might need multiple blocks?
        // For now assume they fit or we split.
        // Let's just create one block. If it overflows, `push_back` will resize buffer (Vec does).
        // Wait, PackedBlock logic uses `BLOCK_SIZE` only for `free_space` check.
        // `push_back` just appends.
        // So we can make an oversized block temporarily.

        for it in items {
            new_block.push_back(&it);
        }

        self.blocks[block_idx] = new_block;
        self.len += 1;
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

        // Update in block
        // Deconstruct/reconstruct
        let block = &mut self.blocks[block_idx];
        let mut items: Vec<Bytes> = block.iter().collect();
        items[offset] = item;

        let mut new_block = PackedBlock::new();
        for it in items {
            new_block.push_back(&it);
        }
        self.blocks[block_idx] = new_block;
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
        // Since we don't have random remove in PackedBlock yet
        // We deconstruct/reconstruct
        let block = &mut self.blocks[block_idx];
        let mut items: Vec<Bytes> = block.iter().collect();
        let val = items.remove(offset);

        let mut new_block = PackedBlock::new();
        for it in items {
            new_block.push_back(&it);
        }
        if new_block.is_empty() {
            self.blocks.remove(block_idx);
        } else {
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
