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

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;

use super::{BlockMeta, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeySlice, KeyVec},
    lsm_storage::BlockCache,
    table::FileObject,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key = key.raw_ref().to_vec();
        }

        if !self.builder.add(key, value) {
            // Create a new block builder and make it current
            let mut new_builder = BlockBuilder::new(self.block_size);
            let res = new_builder.add(key, value);
            debug_assert!(res, "first insert of key-value must succeed");
            let old_block_builder = std::mem::replace(&mut self.builder, new_builder);

            self.seal_block(old_block_builder);
        }

        self.last_key = key.raw_ref().to_vec();
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    fn seal_block(&mut self, block_builder: BlockBuilder) {
        let block_meta = BlockMeta {
            offset: self.data.len(),
            first_key: block_builder.first_key_bytes(),
            last_key: block_builder.last_key_bytes(),
        };
        let sealed_block = block_builder.build().encode();

        self.data.extend_from_slice(&sealed_block);
        self.meta.push(block_meta);
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.build_with_options(id, block_cache, path, false)
    }

    /// Builds the SSTable with direct I/O option.
    /// When `direct_io` is true, uses O_DIRECT (Linux) or F_NOCACHE (macOS) to bypass OS page cache.
    pub fn build_with_options(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
        direct_io: bool,
    ) -> Result<SsTable> {
        // Seal the last and current block
        let block_meta = BlockMeta {
            offset: self.data.len(),
            first_key: self.builder.first_key_bytes(),
            last_key: self.builder.last_key_bytes(),
        };
        let sealed_block = self.builder.build().encode();
        self.data.extend_from_slice(&sealed_block);
        self.meta.push(block_meta);

        let offset = self.data.len();
        BlockMeta::encode_block_meta(&self.meta, &mut self.data);
        self.data.put_u64(offset as u64);

        let file = if direct_io {
            FileObject::create_direct_io(path.as_ref(), self.data)?
        } else {
            FileObject::create(path.as_ref(), self.data)?
        };

        Ok(SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset: offset,
            id,
            block_cache,
            first_key: KeyVec::from_vec(self.first_key).into_key_bytes(),
            last_key: KeyVec::from_vec(self.last_key).into_key_bytes(),
            bloom: None,
            max_ts: 0_u64,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
