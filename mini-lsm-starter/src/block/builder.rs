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

use crate::key::{Key, KeyBytes, KeySlice, KeyVec};
use bytes::BufMut;
use nom::Slice;

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
    /// The last key in the block
    last_key: KeyVec,
}

fn overlap_len(key: &[u8], first_key: &[u8]) -> usize {
    first_key
        .iter()
        .zip(key) // Pairs them up: (f[0], k[0]), (f[1], k[1])...
        .take_while(|(a, b)| a == b) // Stop as soon as they differ
        .count() // Return the number of matches
}

fn append_entry(buf: &mut Vec<u8>, key: &[u8], first_key: &[u8], value: &[u8]) {
    let overlap_len = overlap_len(key, first_key);
    let rest_key_len = key.len() - overlap_len;
    let rest_bytes = key.slice(overlap_len..);
    buf.put_u16(overlap_len as u16);
    buf.put_u16(rest_key_len as u16);
    buf.put(rest_bytes);
    buf.put_u16(value.len() as u16);
    buf.put(value);
}

fn append_first_entry(buf: &mut Vec<u8>, key: &[u8], value: &[u8]) {
    buf.put_u16(key.len() as u16);
    buf.put(key);
    buf.put_u16(value.len() as u16);
    buf.put(value);
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: Key::new(),
            last_key: Key::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let start = self.data.len() as u16;

        // key_len(2) + key_bytes_len + value_len(2) + value_bytes_len
        let entry_size = 2 + key.len() + 2 + value.len();

        // Always insert the first key-value entry regardless of size limit as per tutorial
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
            self.last_key = key.to_key_vec();
            append_first_entry(&mut self.data, key.raw_ref(), value);
            self.offsets.push(0);
            return true;
        }

        // If adding the upcoming key-value pair would breach the limit we stop early and return false
        if self.current_block_size() + entry_size + 2 > self.block_size {
            return false;
        }
        append_entry(
            &mut self.data,
            key.raw_ref(),
            self.first_key.raw_ref(),
            value,
        );
        self.last_key = key.to_key_vec();
        self.offsets.push(start);

        true
    }

    // entrys_len + offsets_len * 2 (because each offset is u16) + number_of_elements
    fn current_block_size(&self) -> usize {
        self.data.len() + self.offsets.len() * 2 + 2
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }

    pub fn first_key_bytes(&self) -> KeyBytes {
        self.first_key.clone().into_key_bytes()
    }

    pub fn last_key_bytes(&self) -> KeyBytes {
        self.last_key.clone().into_key_bytes()
    }
}
