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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let data_size = self.data.len();
        let offsets_size = self.offsets.len() * 2;
        let mut res = BytesMut::with_capacity(data_size + offsets_size + 2);
        res.put(&*self.data);
        for offset in &self.offsets {
            res.put_u16(*offset);
        }
        res.put_u16(self.offsets.len() as u16);
        res.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let mut end: usize = data.len() - 1;
        let mut number_of_elements = u16::from_be_bytes([data[end - 1], data[end]]);
        end -= 2;

        let mut offsets: Vec<u16> = Vec::new();
        while number_of_elements > 0 {
            let offset = u16::from_be_bytes([data[end - 1], data[end]]);
            offsets.push(offset);
            end -= 2;
            number_of_elements -= 1;
        }

        let data_block = &data[0..=end];
        offsets.reverse();

        Self {
            data: data_block.to_vec(),
            offsets,
        }
    }
}
