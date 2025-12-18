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

use std::collections::HashMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::{Ok, Result, anyhow};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::StorageIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::{Key, KeySlice};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::{MemTable, map_bound};
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.flush_notifier.send(()).ok();
        let mut flush_thread = self.flush_thread.lock();
        if let Some(flush_thread) = flush_thread.take() {
            flush_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan_prefix(prefix)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        if !path.as_ref().exists() {
            std::fs::create_dir_all(path.as_ref())?;
        }

        let path = path.as_ref();
        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; // drop global lock here

        let value = snapshot.memtable.get(key);
        if let Some(v) = value {
            if !v.is_empty() {
                return Ok(Some(v));
            } else {
                return Ok(None);
            }
        }
        for imm_memtable in snapshot.imm_memtables.iter() {
            let value = imm_memtable.get(key);
            if let Some(v) = value {
                if !v.is_empty() {
                    return Ok(Some(v));
                } else {
                    return Ok(None);
                }
            }
        }

        // L0 ssts - create iterators in parallel
        let l0_ssts: Vec<_> = snapshot
            .l0_sstables
            .iter()
            .map(|sst_id| {
                snapshot
                    .sstables
                    .get(sst_id)
                    .cloned()
                    .ok_or_else(|| anyhow!("Sstable {sst_id} doesn't exist in the snapshot"))
            })
            .collect::<Result<Vec<_>>>()?;

        let l0_iters: Vec<Box<SsTableIterator>> = std::thread::scope(|s| {
            let handles: Vec<_> = l0_ssts
                .into_iter()
                .filter_map(|sst| {
                    if skip_sst(
                        Bound::Included(key),
                        Bound::Included(key),
                        sst.first_key(),
                        sst.last_key(),
                    ) {
                        None
                    } else if sst.bloom.is_some()
                        && !sst
                            .bloom
                            .as_ref()
                            .unwrap()
                            .may_contain(farmhash::fingerprint32(key))
                    {
                        // Bloom filter says key definitely NOT in this SST, skip it
                        None
                    } else {
                        Some(s.spawn(move || {
                            SsTableIterator::create_and_seek_to_key(sst, KeySlice::from_slice(key))
                        }))
                    }
                })
                .collect();

            handles
                .into_iter()
                .map(|h| h.join().expect("Thread panicked").map(Box::new))
                .collect::<Result<Vec<_>>>()
        })?;
        let l0_iter = MergeIterator::create(l0_iters);

        // L1+ ssts - create iterators in parallel
        let level_ssts: Vec<_> = snapshot
            .levels
            .iter()
            .flat_map(|(_, sst_level)| sst_level.iter())
            .map(|sst_id| {
                snapshot
                    .sstables
                    .get(sst_id)
                    .cloned()
                    .ok_or_else(|| anyhow!("Sstable {sst_id} doesn't exist in the snapshot"))
            })
            .collect::<Result<Vec<_>>>()?;

        let sst_iters: Vec<Box<SsTableIterator>> = std::thread::scope(|s| {
            let handles: Vec<_> = level_ssts
                .into_iter()
                .filter_map(|sst| {
                    if skip_sst(
                        Bound::Included(key),
                        Bound::Included(key),
                        sst.first_key(),
                        sst.last_key(),
                    ) {
                        None
                    } else if sst.bloom.is_some()
                        && !sst
                            .bloom
                            .as_ref()
                            .unwrap()
                            .may_contain(farmhash::fingerprint32(key))
                    {
                        // Bloom filter says key definitely NOT in this SST, skip it
                        None
                    } else {
                        Some(s.spawn(move || {
                            SsTableIterator::create_and_seek_to_key(sst, KeySlice::from_slice(key))
                        }))
                    }
                })
                .collect();

            handles
                .into_iter()
                .map(|h| h.join().expect("Thread panicked").map(Box::new))
                .collect::<Result<Vec<_>>>()
        })?;
        let sst_iter = MergeIterator::create(sst_iters);
        let sst_merge_iterator = TwoMergeIterator::create(l0_iter, sst_iter)?;

        if sst_merge_iterator.is_valid()
            && sst_merge_iterator.key().raw_ref() == key
            && !sst_merge_iterator.value().is_empty()
        {
            return Ok(Some(Bytes::copy_from_slice(sst_merge_iterator.value())));
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let lock = self.state.write();
        let _ = lock.memtable.put(key, value);
        drop(lock);

        if self.state.read().memtable.approximate_size() > self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            if self.state.read().memtable.approximate_size() > self.options.target_sst_size {
                let _ = self.force_freeze_memtable(&state_lock);
            }
        }
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let lock = self.state.write();
        let _ = lock.memtable.put(key, b"");
        drop(lock);

        if self.state.read().memtable.approximate_size() > self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            if self.state.read().memtable.approximate_size() > self.options.target_sst_size {
                let _ = self.force_freeze_memtable(&state_lock);
            }
        }
        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let new_memtable = Arc::new(MemTable::create(self.next_sst_id()));
        {
            let mut guard = self.state.write();
            let mut state = guard.as_ref().clone();
            state.imm_memtables.insert(0, guard.memtable.clone());
            state.memtable = new_memtable;
            *guard = Arc::new(state);
        }
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let _state_lock = self.state_lock.lock();

        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; // drop global lock here

        if snapshot.imm_memtables.is_empty() {
            return Ok(());
        }

        // Flush to new sst
        let memtable_to_flush = snapshot
            .imm_memtables
            .last()
            .ok_or(anyhow!(
                "Unknown erro getting the earliest immutable memtables"
            ))?
            .clone();
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        memtable_to_flush.flush(&mut sst_builder)?;
        let sst = sst_builder.build(
            memtable_to_flush.id(),
            Some(self.block_cache.clone()),
            self.path_of_sst(memtable_to_flush.id()),
        )?;

        // Mutate the state
        {
            let mut guard = self.state.write();
            let mut state = guard.as_ref().clone();
            state.imm_memtables.pop().unwrap();
            if self.compaction_controller.flush_to_l0() {
                // newer sst are always at front of the list
                state.l0_sstables.insert(0, sst.sst_id());
            }
            state.sstables.insert(sst.sst_id(), Arc::new(sst));
            *guard = Arc::new(state);
        }

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; // drop global lock here

        let mut memtable_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        memtable_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));
        for memtable in snapshot.imm_memtables.iter() {
            memtable_iters.push(Box::new(memtable.scan(lower, upper)));
        }
        let memtable_iter = MergeIterator::create(memtable_iters);

        // At this stage we only consider L0-ssts. Will expand to further levels in future chapters
        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for sst_id in &snapshot.l0_sstables {
            if let Some(sst) = snapshot.sstables.get(sst_id) {
                if skip_sst(lower, upper, sst.first_key(), sst.last_key()) {
                    continue;
                }

                let iter = match lower {
                    Bound::Included(low) => SsTableIterator::create_and_seek_to_key(
                        sst.clone(),
                        KeySlice::from_slice(low),
                    )?,
                    Bound::Excluded(low) => {
                        let mut iter = SsTableIterator::create_and_seek_to_key(
                            sst.clone(),
                            KeySlice::from_slice(low),
                        )?;
                        if iter.is_valid() && iter.key().raw_ref() == low {
                            iter.next()?;
                        }
                        iter
                    }
                    Bound::Unbounded => SsTableIterator::create_and_seek_to_first(sst.clone())?,
                };
                l0_iters.push(Box::new(iter));
            } else {
                return Err(anyhow!("Sstable {sst_id} doesn't exist in the snapshot"));
            }
        }
        let l0_iter = MergeIterator::create(l0_iters);

        let two_merge_iterator = TwoMergeIterator::create(memtable_iter, l0_iter)?;
        Ok(FusedIterator::new(LsmIterator::new(
            two_merge_iterator,
            map_bound(upper),
        )?))
    }
}

impl LsmStorageInner {
    /// Scan all keys with the given prefix.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<FusedIterator<LsmIterator>> {
        let lower = Bound::Included(prefix);
        // Compute upper bound: find rightmost non-0xFF byte and increment it
        let mut upper_buf = prefix.to_vec();
        let upper = loop {
            if let Some(&last) = upper_buf.last() {
                if last == 0xFF {
                    upper_buf.pop();
                } else {
                    *upper_buf.last_mut().unwrap() += 1;
                    break Bound::Excluded(upper_buf.as_slice());
                }
            } else {
                break Bound::Unbounded;
            }
        };
        self.scan(lower, upper)
    }
}

fn skip_sst(
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
    first_key: &Key<bytes::Bytes>,
    last_key: &Key<bytes::Bytes>,
) -> bool {
    let skip_low = match lower {
        Bound::Included(low) => last_key.raw_ref() < low,
        Bound::Excluded(low) => last_key.raw_ref() <= low,
        Bound::Unbounded => false,
    };
    let skip_up = match upper {
        Bound::Included(up) => first_key.raw_ref() > up,
        Bound::Excluded(up) => first_key.raw_ref() >= up,
        Bound::Unbounded => false,
    };

    skip_low || skip_up
}
