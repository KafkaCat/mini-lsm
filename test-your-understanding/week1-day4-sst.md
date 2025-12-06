# Week 1 Day 4: Sorted String Table (SST)

## Test Your Understanding

### Q1: What is the time complexity of seeking a key in the SST?

**A:** `O(logN + logM)` where N = number of blocks and M = number of entries per block. We use binary search (`partition_point`) on block metadata to find the right block, and binary search within the block using the offsets array. Both operations are logarithmic.

---

### Q2: Where does the cursor stop when you seek a non-existent key in your implementation?

**A:** It stops at the first key that is greater than or equal to the target key. If no such key exists (i.e., seeking past the last key), the iterator becomes invalid.

---

### Q3: Is it possible (or necessary) to do in-place updates of SST files?

**A:** It's not recommended. SSTs are designed to be immutable - this simplifies concurrency (no locks needed for reads), enables easy caching, and allows for atomic operations. In-place updates would require complex locking, invalidate caches, and risk data corruption on crashes. The LSM design uses compaction to handle updates instead.

---

### Q4: An SST is usually large (i.e., 256MB). In this case, the cost of copying/expanding the `Vec` would be significant. Does your implementation allocate enough space for your SST builder in advance? How did you implement it?

**A:** Currently no, we don't pre-allocate. We could use `Vec::with_capacity()` to reserve space upfront based on the target SST size. For example, if target is 256MB, we could reserve that much for the data buffer to avoid reallocation during building.

---

### Q5: Looking at the `moka` block cache, why does it return `Arc<Error>` instead of the original `Error`?

**A:** Because `try_get_with` can be called concurrently by multiple threads for the same key. If there's an error, all waiters need to receive the error. Since `Error` types typically don't implement `Clone`, wrapping in `Arc` allows sharing the same error instance across all callers without copying.

---

### Q6: Does the usage of a block cache guarantee that there will be at most a fixed number of blocks in memory? For example, if you have a `moka` block cache of 4GB and block size of 4KB, will there be more than 4GB/4KB number of blocks in memory at the same time?

**A:** No, it doesn't guarantee a fixed number. The cache only controls what it caches, but blocks can also be held by active iterators. If you have many concurrent iterators, each holding references to blocks, those blocks stay in memory even if evicted from cache. So actual memory usage can exceed the cache size.

---

### Q7: Is it possible to store columnar data (i.e., a table of 100 integer columns) in an LSM engine? Is the current SST format still a good choice?

**A:** It's possible but not optimal. The current row-oriented SST format stores all columns together. For columnar workloads (like analytics that read few columns), this wastes I/O reading unnecessary data. Better approaches: store each column in separate SSTs, use column groups, or adopt formats like Parquet that are designed for columnar access.

---

### Q8: Consider the case that the LSM engine is built on object store services (i.e., S3). How would you optimize/change the SST format/parameters and the block cache to make it suitable for such services?

**A:** 
1. Increase block size significantly (maybe 1-4MB) to reduce number of API calls since S3 charges per request
2. Increase SST size (maybe 256MB-1GB) for better sequential read performance
3. Use larger cache since S3 latency is much higher than local disk
4. Consider prefetching blocks since S3 has high latency but good throughput
5. Store block index/footer at the beginning of file to avoid extra read for metadata

---

### Q9: For now, we load the index of all SSTs into the memory. Assume you have a 16GB memory reserved for the indexes, can you estimate the maximum size of the database your LSM system can support? (That's why you need an index cache!)

**A:** Rough estimate: Each BlockMeta has offset (8 bytes) + first_key + last_key. If keys are ~20 bytes each, that's ~50 bytes per block. With 4KB blocks and 256MB SSTs, each SST has ~65K blocks = ~3.2MB of index. With 16GB for indexes, we can hold ~5000 SSTs = ~1.25TB of data. For larger databases, we'd need an index cache to load indexes on demand.

---

## Bonus Tasks

- **Explore different SST encoding and layout.** For example, in the Lethe paper, the author adds secondary key support to SST. Or you can use B+ Tree as the SST format instead of sorted blocks.
- **Index Blocks.** Split block indexes and block metadata into index blocks, and load them on-demand.
- **Index Cache.** Use a separate cache for indexes apart from the data block cache.
- **I/O Optimizations.** Align blocks to 4KB boundary and use direct I/O to bypass the system page cache.
