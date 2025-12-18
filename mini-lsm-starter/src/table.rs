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

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::alloc::{Layout, alloc, dealloc};
use std::fs::File;
use std::io::Write;
#[cfg(target_os = "linux")]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(target_os = "macos")]
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::ptr::NonNull;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

/// Block size for direct I/O alignment (4KB)
pub const BLOCK_ALIGNMENT: usize = 4096;

/// An aligned buffer for direct I/O operations.
/// Guarantees memory is aligned to BLOCK_ALIGNMENT bytes.
pub(crate) struct AlignedBuffer {
    ptr: NonNull<u8>,
    len: usize,
    capacity: usize,
}

impl AlignedBuffer {
    /// Create a new aligned buffer with the given capacity (rounded up to BLOCK_ALIGNMENT).
    fn new(capacity: usize) -> Result<Self> {
        let capacity = (capacity + BLOCK_ALIGNMENT - 1) & !(BLOCK_ALIGNMENT - 1);
        let layout = Layout::from_size_align(capacity, BLOCK_ALIGNMENT)
            .map_err(|e| anyhow::anyhow!("Invalid layout for aligned buffer: {}", e))?;
        let ptr = unsafe { alloc(layout) };
        let ptr = NonNull::new(ptr).ok_or_else(|| {
            anyhow::anyhow!("Failed to allocate aligned buffer of {} bytes", capacity)
        })?;
        Ok(Self {
            ptr,
            len: 0,
            capacity,
        })
    }

    /// Create an aligned buffer with specific size, zero-initialized.
    /// Note: size should be aligned to BLOCK_ALIGNMENT for direct I/O operations.
    pub(crate) fn with_size(size: usize) -> Result<Self> {
        debug_assert!(
            size.is_multiple_of(BLOCK_ALIGNMENT),
            "with_size() expects aligned size, got {}",
            size
        );
        let mut buf = Self::new(size)?;
        buf.len = size;
        unsafe {
            std::ptr::write_bytes(buf.ptr.as_ptr(), 0, buf.capacity);
        }
        Ok(buf)
    }

    /// Get a mutable slice to the buffer
    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    /// Get a slice to the buffer
    pub(crate) fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    /// Copy data from a slice into the aligned buffer
    fn copy_from_slice(&mut self, data: &[u8]) {
        debug_assert!(data.len() <= self.capacity, "Data too large for buffer");
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr.as_ptr(), data.len());
        }
        self.len = data.len();
    }

    /// Create an aligned buffer from Vec, padding to alignment for direct I/O writes.
    pub(crate) fn from_vec_padded(data: Vec<u8>) -> Result<Self> {
        let padded_len = (data.len() + BLOCK_ALIGNMENT - 1) & !(BLOCK_ALIGNMENT - 1);
        let mut buf = Self::new(padded_len)?;
        buf.copy_from_slice(&data);
        buf.len = padded_len; // Set len to padded size for aligned writes
        Ok(buf)
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        if let Ok(layout) = Layout::from_size_align(self.capacity, BLOCK_ALIGNMENT) {
            unsafe {
                dealloc(self.ptr.as_ptr(), layout);
            }
        }
    }
}

unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        for meta in block_meta {
            buf.put_u16(meta.offset as u16);
            buf.put_u16(meta.first_key.raw_ref().len() as u16);
            buf.put(meta.first_key.raw_ref());
            buf.put_u16(meta.last_key.raw_ref().len() as u16);
            buf.put(meta.last_key.raw_ref());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut block_meta = Vec::new();
        while buf.has_remaining() {
            let offset = buf.get_u16() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key = KeyBytes::from_bytes(buf.copy_to_bytes(first_key_len));
            let last_key_len = buf.get_u16() as usize;
            let last_key = KeyBytes::from_bytes(buf.copy_to_bytes(last_key_len));
            block_meta.push(BlockMeta {
                offset,
                first_key,
                last_key,
            });
        }
        block_meta
    }
}

/// A file object with optional Direct I/O support.
pub struct FileObject {
    file: Option<File>,
    size: u64,
    direct_io: bool,
}

impl FileObject {
    /// Enable direct I/O for the given file descriptor.
    /// - macOS: Uses fcntl(F_NOCACHE) to disable page cache for this fd
    /// - Linux: No-op because O_DIRECT is set via custom_flags() during open()
    #[cfg(target_os = "macos")]
    fn enable_direct_io(file: &File) -> Result<()> {
        let fd = file.as_raw_fd();
        let ret = unsafe { libc::fcntl(fd, libc::F_NOCACHE, 1) };
        if ret == -1 {
            anyhow::bail!(
                "Failed to set F_NOCACHE: {}",
                std::io::Error::last_os_error()
            );
        }
        Ok(())
    }

    /// Enable direct I/O for the given file descriptor.
    /// On Linux, O_DIRECT is set via custom_flags() during open(), so this is a no-op.
    #[cfg(target_os = "linux")]
    fn enable_direct_io(_file: &File) -> Result<()> {
        Ok(())
    }

    /// Fallback for other Unix systems - direct I/O not supported
    #[cfg(all(unix, not(target_os = "macos"), not(target_os = "linux")))]
    fn enable_direct_io(_file: &File) -> Result<()> {
        Ok(())
    }

    /// Read data from file using aligned buffer for direct I/O
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;

        if self.direct_io {
            let aligned_offset = offset & !(BLOCK_ALIGNMENT as u64 - 1);
            let offset_diff = (offset - aligned_offset) as usize;

            // Calculate aligned length, but cap at file size to avoid reading past EOF
            let max_readable = self.size.saturating_sub(aligned_offset) as usize;
            let desired_aligned_len =
                (len as usize + offset_diff + BLOCK_ALIGNMENT - 1) & !(BLOCK_ALIGNMENT - 1);
            let aligned_len = desired_aligned_len
                .min((max_readable + BLOCK_ALIGNMENT - 1) & !(BLOCK_ALIGNMENT - 1));

            // For reads near EOF, we may need to read less than aligned_len
            // Use read_at which returns actual bytes read, then verify we got enough
            let mut buf = AlignedBuffer::with_size(aligned_len)?;
            let bytes_to_read = max_readable.min(aligned_len);
            self.file
                .as_ref()
                .unwrap()
                .read_exact_at(&mut buf.as_mut_slice()[..bytes_to_read], aligned_offset)?;

            Ok(buf.as_slice()[offset_diff..offset_diff + len as usize].to_vec())
        } else {
            let mut data = vec![0; len as usize];
            self.file
                .as_ref()
                .unwrap()
                .read_exact_at(&mut data[..], offset)?;
            Ok(data)
        }
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    /// Create a new file object with direct I/O disabled (for compatibility)
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject {
            file: Some(File::options().read(true).write(false).open(path)?),
            size: data.len() as u64,
            direct_io: false,
        })
    }

    /// Create a new file object with Direct I/O enabled.
    ///
    /// The write process:
    /// 1. Write padded data (aligned to BLOCK_ALIGNMENT) with direct I/O
    /// 2. Truncate file to actual size (removes padding)
    /// 3. Sync to disk
    /// 4. Reopen as read-only for the FileObject
    pub fn create_direct_io(path: &Path, data: Vec<u8>) -> Result<Self> {
        let actual_size = data.len() as u64;
        let aligned_buf = AlignedBuffer::from_vec_padded(data)?;

        #[cfg(target_os = "linux")]
        {
            // Write with O_DIRECT (requires aligned writes)
            let mut file = File::options()
                .write(true)
                .create(true)
                .truncate(true)
                .custom_flags(libc::O_DIRECT)
                .open(path)?;
            file.write_all(aligned_buf.as_slice())?;
            // Truncate to actual size before sync (removes padding)
            file.set_len(actual_size)?;
            file.sync_all()?;
            drop(file);

            // Reopen as read-only with O_DIRECT
            let file = File::options()
                .read(true)
                .write(false)
                .custom_flags(libc::O_DIRECT)
                .open(path)?;
            return Ok(FileObject {
                file: Some(file),
                size: actual_size,
                direct_io: true,
            });
        }

        #[cfg(target_os = "macos")]
        {
            // macOS: F_NOCACHE is set via fcntl after open
            let mut file = File::options()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)?;
            Self::enable_direct_io(&file)?;
            file.write_all(aligned_buf.as_slice())?;
            // Truncate to actual size before sync (removes padding)
            file.set_len(actual_size)?;
            file.sync_all()?;
            drop(file);

            // Reopen as read-only with F_NOCACHE
            let file = File::options().read(true).write(false).open(path)?;
            Self::enable_direct_io(&file)?;
            Ok(FileObject {
                file: Some(file),
                size: actual_size,
                direct_io: true,
            })
        }

        #[cfg(all(unix, not(target_os = "macos"), not(target_os = "linux")))]
        {
            // Fallback: no direct I/O support, write normally
            let mut file = File::options()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)?;
            file.write_all(&aligned_buf.as_slice()[..actual_size as usize])?;
            file.sync_all()?;
            drop(file);
            let file = File::options().read(true).write(false).open(path)?;
            return Ok(FileObject {
                file: Some(file),
                size: actual_size,
                direct_io: false, // No actual direct I/O on this platform
            });
        }
    }

    /// Open an existing file without direct I/O (for compatibility)
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject {
            file: Some(file),
            size,
            direct_io: false,
        })
    }

    /// Open an existing file with Direct I/O enabled
    pub fn open_direct_io(path: &Path) -> Result<Self> {
        #[cfg(target_os = "linux")]
        let file = File::options()
            .read(true)
            .write(false)
            .custom_flags(libc::O_DIRECT)
            .open(path)?;

        #[cfg(target_os = "macos")]
        let file = {
            let file = File::options().read(true).write(false).open(path)?;
            Self::enable_direct_io(&file)?;
            file
        };

        #[cfg(all(unix, not(target_os = "macos"), not(target_os = "linux")))]
        let file = File::options().read(true).write(false).open(path)?;

        let size = file.metadata()?.len();
        Ok(FileObject {
            file: Some(file),
            size,
            direct_io: true,
        })
    }

    /// Check if direct I/O is enabled
    pub fn is_direct_io(&self) -> bool {
        self.direct_io
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let bytes: [u8; 8] = file.read(file.size() - 8, 8)?[..].try_into().unwrap();
        let meta_block_offset = u64::from_be_bytes(bytes);
        let bytes = file.read(meta_block_offset, file.size() - 8 - meta_block_offset)?;
        let meta = BlockMeta::decode_block_meta(&*bytes);
        let first_key = meta.first().expect("meta first is empty").first_key.clone();
        let last_key = meta.last().expect("meta last is empty").last_key.clone();

        Ok(Self {
            file,
            block_meta: meta,
            block_meta_offset: meta_block_offset as usize,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject {
                file: None,
                size: file_size,
                direct_io: false,
            },
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        if block_idx == self.block_meta.len() - 1 {
            let len = self.block_meta_offset - self.block_meta[block_idx].offset;
            let block_data_raw = self
                .file
                .read(self.block_meta[block_idx].offset as u64, len as u64)?;
            Ok(Arc::new(Block::decode(&block_data_raw)))
        } else if block_idx < self.block_meta.len() - 1 {
            let len = self.block_meta[block_idx + 1].offset - self.block_meta[block_idx].offset;
            let block_data_raw = self
                .file
                .read(self.block_meta[block_idx].offset as u64, len as u64)?;
            Ok(Arc::new(Block::decode(&block_data_raw)))
        } else {
            anyhow::bail!("{block_idx} is out of boundary")
        }
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(ref block_cache) = self.block_cache {
            let blk = block_cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow::anyhow!("{}", e))?;
            Ok(blk)
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        let idx = self
            .block_meta
            .partition_point(|meta| meta.first_key.as_key_slice() <= key);
        idx.saturating_sub(1)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.size
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
