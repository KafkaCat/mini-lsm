// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use nom::Slice;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        if block.offsets.is_empty() {
            return BlockIterator::new(block);
        }
        let mut iter = BlockIterator::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        if block.offsets.is_empty() {
            return BlockIterator::new(block);
        }
        let mut iter = BlockIterator::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice<'_> {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Initialize first_key by reading the first entry. Must be called before any seek operation.
    fn init_first_key(&mut self) {
        if !self.first_key.is_empty() || self.block.offsets.is_empty() {
            // Either first key is already initialized or the block is empty
            return;
        }
        let key_len = u16::from_be_bytes([self.block.data[0], self.block.data[1]]) as usize;
        self.first_key = KeyVec::from_vec(self.block.data[2..2 + key_len].to_vec());
    }

    fn decode_key(&self, start: usize) -> (KeyVec, usize) {
        if start == 0 {
            let key_len =
                u16::from_be_bytes([self.block.data[start], self.block.data[start + 1]]) as usize;
            (
                KeyVec::from_vec(self.block.data[start + 2..start + 2 + key_len].to_vec()),
                key_len,
            )
        } else {
            let key_overlap_len =
                u16::from_be_bytes([self.block.data[start], self.block.data[start + 1]]) as usize;
            let key_rest_len =
                u16::from_be_bytes([self.block.data[start + 2], self.block.data[start + 3]])
                    as usize;
            let rest_key = &self.block.data[start + 4..start + 4 + key_rest_len];
            let mut prefix = self.first_key.raw_ref().slice(0..key_overlap_len).to_vec();
            prefix.extend_from_slice(rest_key);
            (KeyVec::from_vec(prefix), key_rest_len)
        }
    }

    fn set_current_key_value(&mut self) {
        let start = self.block.offsets[self.idx] as usize;
        if self.idx == 0 {
            debug_assert_eq!(start, 0, "First entry offset should always start from 0");
        }
        let (key, key_len) = self.decode_key(start);
        self.key = key;

        if start == 0 {
            let value_len = u16::from_be_bytes([
                self.block.data[start + 2 + key_len],
                self.block.data[start + 3 + key_len],
            ]) as usize;
            self.value_range = (start + key_len + 4, start + key_len + 4 + value_len);
        } else {
            let value_len = u16::from_be_bytes([
                self.block.data[start + 4 + key_len],
                self.block.data[start + 5 + key_len],
            ]) as usize;
            self.value_range = (start + key_len + 6, start + key_len + 6 + value_len);
        }
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        if self.block.offsets.is_empty() {
            self.key = KeyVec::new();
            return;
        }

        self.init_first_key();
        self.idx = 0;
        self.set_current_key_value();
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;

        // Out of bondary: we reached to the end of block
        if self.idx >= self.block.offsets.len() {
            self.key = KeyVec::new();
            return;
        }

        self.set_current_key_value();
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        if self.block.offsets.is_empty() {
            self.key = KeyVec::new();
            return;
        }

        self.init_first_key();

        // Binary search on offsets to find first key >= target
        let idx = self.block.offsets.partition_point(|&offset| {
            let start = offset as usize;
            let curr_key = self.decode_key(start).0;
            curr_key.raw_ref() < key.raw_ref()
        });

        if idx >= self.block.offsets.len() {
            self.key = KeyVec::new();
            return;
        }

        self.idx = idx;
        self.set_current_key_value();
    }

    /// Creates a block iterator and seek to the last entry.
    pub fn create_and_seek_to_last(block: Arc<Block>) -> Self {
        if block.offsets.is_empty() {
            return BlockIterator::new(block);
        }
        let mut iter = BlockIterator::new(block);
        iter.seek_to_last();
        iter
    }

    /// Seeks to the last key in the block.
    pub fn seek_to_last(&mut self) {
        if self.block.offsets.is_empty() {
            self.key = KeyVec::new();
            return;
        }

        self.init_first_key();

        // Then seek to the last entry
        self.idx = self.block.offsets.len() - 1;
        self.set_current_key_value();
    }

    /// Move to the previous key in the block.
    pub fn prev(&mut self) {
        if self.idx == 0 {
            self.key = KeyVec::new();
            return;
        }

        self.idx -= 1;
        self.set_current_key_value();
    }
}
