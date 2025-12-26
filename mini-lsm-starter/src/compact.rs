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

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Ok, Result};
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; // drop global lock here

        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let l0_ssts: Vec<_> = l0_sstables
                    .iter()
                    .map(|sst_id| {
                        snapshot.sstables.get(sst_id).cloned().ok_or_else(|| {
                            anyhow::anyhow!("Sstable {sst_id} doesn't exist in the snapshot")
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let l0_iters: Vec<Box<SsTableIterator>> = std::thread::scope(|s| {
                    let handles: Vec<_> = l0_ssts
                        .into_iter()
                        .map(|sst| s.spawn(move || SsTableIterator::create_and_seek_to_first(sst)))
                        .collect();

                    handles
                        .into_iter()
                        .map(|h| h.join().expect("Thread panicked").map(Box::new))
                        .collect::<Result<Vec<_>>>()
                })?;
                let l0_merge_iterator = MergeIterator::create(l0_iters);

                let l1_ssts: Vec<_> = l1_sstables
                    .iter()
                    .map(|sst_id| {
                        snapshot.sstables.get(sst_id).cloned().ok_or_else(|| {
                            anyhow::anyhow!("Sstable {sst_id} doesn't exist in the snapshot")
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let l1_concat_iterator = SstConcatIterator::create_and_seek_to_first(l1_ssts)?;

                let mut merge_iterator =
                    TwoMergeIterator::create(l0_merge_iterator, l1_concat_iterator)?;
                let mut result_ssts: Vec<Arc<SsTable>> = Vec::new();
                let mut sst_builder = SsTableBuilder::new(self.options.block_size);
                let mut pending_entries = 0usize;
                while merge_iterator.is_valid() {
                    if merge_iterator.value().is_empty() {
                        merge_iterator.next()?;
                        continue;
                    }
                    sst_builder.add(merge_iterator.key(), merge_iterator.value());
                    pending_entries += 1;
                    if sst_builder.estimated_size() >= self.options.target_sst_size {
                        let sst_id = self.next_sst_id();
                        let sst = sst_builder.build(
                            sst_id,
                            Some(self.block_cache.clone()),
                            self.path_of_sst(sst_id),
                        )?;
                        debug_assert!(sst.first_key() <= sst.last_key());
                        if let Some(prev) = result_ssts.last() {
                            debug_assert!(prev.last_key() < sst.first_key());
                        }
                        result_ssts.push(Arc::new(sst));
                        sst_builder = SsTableBuilder::new(self.options.block_size);
                        pending_entries = 0;
                    }
                    merge_iterator.next()?;
                }

                // Do the final flush
                if pending_entries > 0 {
                    let sst_id = self.next_sst_id();
                    let sst = sst_builder.build(
                        sst_id,
                        Some(self.block_cache.clone()),
                        self.path_of_sst(sst_id),
                    )?;
                    debug_assert!(sst.first_key() <= sst.last_key());
                    if let Some(prev) = result_ssts.last() {
                        debug_assert!(prev.last_key() < sst.first_key());
                    }
                    result_ssts.push(Arc::new(sst));
                }

                Ok(result_ssts)
            }
            CompactionTask::Leveled(leveled_compaction_task) => todo!(),
            CompactionTask::Tiered(tiered_compaction_task) => todo!(),
            CompactionTask::Simple(simple_leveled_compaction_task) => todo!(),
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; // drop global lock here

        let l0_sstables = snapshot.l0_sstables.to_owned();
        let l1_sstables = snapshot.levels.first().unwrap_or(&(0, vec![])).1.to_owned();
        let compaction_task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };

        println!("force full compaction: {:?}", compaction_task);
        let compaction_result = self.compact(&compaction_task)?;

        {
            let _lock = self.state_lock.lock();
            let mut guard = self.state.write();
            let mut state = guard.as_ref().clone();

            // remove L0 ssts that were compacted (becasue there will be concurrent flush from memtable)
            // because new flushed ssts are inserted to the front of l0_sstables so we can just truncate the remaining by finding the
            // index of first element in the original list.
            let stop_sst_id = l0_sstables.first().unwrap_or(&usize::MAX);
            if let Some(idx) = state.l0_sstables.iter().position(|it| it == stop_sst_id) {
                state.l0_sstables.truncate(idx);
            }
            for old_l0_sst_id in &l0_sstables {
                state.sstables.remove(old_l0_sst_id);
            }
            // remove L1 ssts that were compacted. It's safe to clear because we assume no other compaction is writing to L1
            for old_l1_sst_id in &l1_sstables {
                state.sstables.remove(old_l1_sst_id);
            }
            state.levels[0].1.clear();
            // add new ssts to L1
            for new_sst in compaction_result.iter() {
                state.levels[0].1.push(new_sst.sst_id());
                state.sstables.insert(new_sst.sst_id(), new_sst.clone());
            }

            *guard = Arc::new(state);
        }
        for sst_id in [l0_sstables, l1_sstables].concat() {
            let path = self.path_of_sst(sst_id);
            std::fs::remove_file(path)?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        self.force_full_compaction()
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let do_flush = {
            let snapshot = self.state.read();
            snapshot.imm_memtables.len() >= self.options.num_memtable_limit
        };
        if do_flush {
            self.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
