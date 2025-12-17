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

//! Dynamic dispatch version of TwoMergeIterator for performance comparison.
//!
//! This module demonstrates the cost of dynamic dispatch vs monomorphization.
//! The `StorageIterator` trait has a GAT (Generic Associated Type) `KeyType<'a>`,
//! which makes it not object-safe. To enable dynamic dispatch, we create an
//! object-safe trait `DynStorageIterator` with a fixed key type.

use anyhow::Result;
use bytes::Bytes;

use crate::key::KeySlice;

/// Object-safe version of StorageIterator with fixed key type.
/// This enables dynamic dispatch via `Box<dyn DynStorageIterator>`.
pub trait DynStorageIterator {
    /// Get the current key as bytes (owned to avoid lifetime issues with dyn)
    fn key_bytes(&self) -> Bytes;

    /// Get the current key as a slice (for comparison)
    fn key_slice(&self) -> KeySlice<'_>;

    /// Get the current value
    fn value(&self) -> &[u8];

    /// Check if the iterator is valid
    fn is_valid(&self) -> bool;

    /// Move to the next position
    fn next(&mut self) -> Result<()>;

    /// Number of active iterators
    fn num_active_iterators(&self) -> usize {
        1
    }
}

/// Wrapper to convert any StorageIterator<KeyType = KeySlice> into DynStorageIterator
pub struct DynIteratorWrapper<I> {
    inner: I,
}

impl<I> DynIteratorWrapper<I> {
    pub fn new(inner: I) -> Self {
        Self { inner }
    }
}

impl<I> DynStorageIterator for DynIteratorWrapper<I>
where
    I: 'static + for<'a> super::StorageIterator<KeyType<'a> = KeySlice<'a>>,
{
    fn key_bytes(&self) -> Bytes {
        Bytes::copy_from_slice(self.inner.key().raw_ref())
    }

    fn key_slice(&self) -> KeySlice<'_> {
        self.inner.key()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// Dynamic dispatch version of TwoMergeIterator.
/// Uses `Box<dyn DynStorageIterator>` instead of generics.
pub struct DynTwoMergeIterator {
    a: Box<dyn DynStorageIterator>,
    b: Box<dyn DynStorageIterator>,
    use_a: bool,
}

impl DynTwoMergeIterator {
    fn update_use_a(&mut self) {
        if !self.a.is_valid() {
            self.use_a = false;
            return;
        }
        if !self.b.is_valid() {
            self.use_a = true;
            return;
        }

        self.use_a = self.a.key_slice() < self.b.key_slice();
    }

    fn skip_b(&mut self) -> Result<()> {
        if self.a.is_valid() && self.b.is_valid() && self.a.key_slice() == self.b.key_slice() {
            self.b.next()?;
        }
        Ok(())
    }

    pub fn create(a: Box<dyn DynStorageIterator>, b: Box<dyn DynStorageIterator>) -> Result<Self> {
        let mut iter = Self { a, b, use_a: true };
        iter.skip_b()?;
        iter.update_use_a();
        Ok(iter)
    }
}

impl DynStorageIterator for DynTwoMergeIterator {
    fn key_bytes(&self) -> Bytes {
        if self.use_a {
            self.a.key_bytes()
        } else {
            self.b.key_bytes()
        }
    }

    fn key_slice(&self) -> KeySlice<'_> {
        if self.use_a {
            debug_assert!(self.a.is_valid());
            self.a.key_slice()
        } else {
            debug_assert!(self.b.is_valid());
            self.b.key_slice()
        }
    }

    fn value(&self) -> &[u8] {
        if self.use_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        if self.use_a {
            self.a.is_valid()
        } else {
            self.b.is_valid()
        }
    }

    fn next(&mut self) -> Result<()> {
        if self.use_a {
            self.a.next()?;
        } else {
            self.b.next()?;
        }
        self.skip_b()?;
        self.update_use_a();
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iterators::StorageIterator;
    use crate::iterators::two_merge_iterator::TwoMergeIterator;
    use crate::key::KeySlice;
    use std::time::Instant;

    /// A simple in-memory iterator for benchmarking
    struct MockIterator {
        data: Vec<(Vec<u8>, Vec<u8>)>,
        index: usize,
    }

    impl MockIterator {
        fn new(data: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
            Self { data, index: 0 }
        }
    }

    impl StorageIterator for MockIterator {
        type KeyType<'a> = KeySlice<'a>;

        fn key(&self) -> KeySlice<'_> {
            KeySlice::from_slice(&self.data[self.index].0)
        }

        fn value(&self) -> &[u8] {
            &self.data[self.index].1
        }

        fn is_valid(&self) -> bool {
            self.index < self.data.len()
        }

        fn next(&mut self) -> Result<()> {
            self.index += 1;
            Ok(())
        }
    }

    fn generate_test_data(n: usize, prefix: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..n)
            .map(|i| {
                let key = format!("{}{:08}", prefix, i * 2).into_bytes();
                let value = format!("value_{:08}", i).into_bytes();
                (key, value)
            })
            .collect()
    }

    #[test]
    fn test_dyn_two_merge_iterator_correctness() {
        let data_a = generate_test_data(100, "a_key_");
        let data_b = generate_test_data(100, "b_key_");

        let iter_a = MockIterator::new(data_a.clone());
        let iter_b = MockIterator::new(data_b.clone());

        let dyn_iter = DynTwoMergeIterator::create(
            Box::new(DynIteratorWrapper::new(iter_a)),
            Box::new(DynIteratorWrapper::new(iter_b)),
        )
        .unwrap();

        let iter_a2 = MockIterator::new(data_a);
        let iter_b2 = MockIterator::new(data_b);
        let static_iter = TwoMergeIterator::create(iter_a2, iter_b2).unwrap();

        // Compare results
        let mut dyn_results = Vec::new();
        let mut dyn_iter = dyn_iter;
        while dyn_iter.is_valid() {
            dyn_results.push((dyn_iter.key_bytes(), dyn_iter.value().to_vec()));
            dyn_iter.next().unwrap();
        }

        let mut static_results = Vec::new();
        let mut static_iter = static_iter;
        while static_iter.is_valid() {
            static_results.push((
                Bytes::copy_from_slice(static_iter.key().raw_ref()),
                static_iter.value().to_vec(),
            ));
            static_iter.next().unwrap();
        }

        assert_eq!(dyn_results.len(), static_results.len());
        for (d, s) in dyn_results.iter().zip(static_results.iter()) {
            assert_eq!(d.0, s.0);
            assert_eq!(d.1, s.1);
        }
    }

    #[test]
    fn bench_static_vs_dynamic_dispatch() {
        const N: usize = 10_000;
        const ITERATIONS: usize = 10;

        let data_a = generate_test_data(N, "a_key_");
        let data_b = generate_test_data(N, "b_key_");

        // Warm up and benchmark static dispatch
        let mut static_times = Vec::new();
        for _ in 0..ITERATIONS {
            let iter_a = MockIterator::new(data_a.clone());
            let iter_b = MockIterator::new(data_b.clone());

            let start = Instant::now();
            let mut iter = TwoMergeIterator::create(iter_a, iter_b).unwrap();
            let mut count = 0;
            while iter.is_valid() {
                let _ = iter.key();
                let _ = iter.value();
                iter.next().unwrap();
                count += 1;
            }
            let elapsed = start.elapsed();
            static_times.push(elapsed);
            assert_eq!(count, N * 2);
        }

        // Warm up and benchmark dynamic dispatch
        let mut dyn_times = Vec::new();
        for _ in 0..ITERATIONS {
            let iter_a = MockIterator::new(data_a.clone());
            let iter_b = MockIterator::new(data_b.clone());

            let start = Instant::now();
            let mut iter = DynTwoMergeIterator::create(
                Box::new(DynIteratorWrapper::new(iter_a)),
                Box::new(DynIteratorWrapper::new(iter_b)),
            )
            .unwrap();
            let mut count = 0;
            while iter.is_valid() {
                let _ = iter.key_slice();
                let _ = iter.value();
                iter.next().unwrap();
                count += 1;
            }
            let elapsed = start.elapsed();
            dyn_times.push(elapsed);
            assert_eq!(count, N * 2);
        }

        // Calculate averages (skip first iteration as warm-up)
        let static_avg: u128 =
            static_times[1..].iter().map(|d| d.as_nanos()).sum::<u128>() / (ITERATIONS - 1) as u128;
        let dyn_avg: u128 =
            dyn_times[1..].iter().map(|d| d.as_nanos()).sum::<u128>() / (ITERATIONS - 1) as u128;

        eprintln!("\n========== TwoMergeIterator Benchmark ==========");
        eprintln!("Items per iterator: {}", N);
        eprintln!("Total items merged: {}", N * 2);
        eprintln!("Iterations: {}", ITERATIONS);
        eprintln!("------------------------------------------------");
        eprintln!("Static dispatch (generics): {:>10} ns avg", static_avg);
        eprintln!("Dynamic dispatch (Box<dyn>): {:>10} ns avg", dyn_avg);
        eprintln!("------------------------------------------------");
        if dyn_avg > static_avg {
            let overhead = ((dyn_avg as f64 / static_avg as f64) - 1.0) * 100.0;
            eprintln!("Dynamic dispatch overhead: {:.2}%", overhead);
        } else {
            eprintln!("Dynamic dispatch was faster (unexpected)");
        }
        eprintln!("=================================================\n");
    }
}
