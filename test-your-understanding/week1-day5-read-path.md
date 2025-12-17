# Week 1 Day 5: Read Path

## Test Your Understanding

### Q1: Consider the case that a user has an iterator that iterates the whole storage engine, and the storage engine is 1TB large, so that it takes ~1 hour to scan all the data. What would be the problems if the user does so? (This is a good question and we will ask it several times at different points of the course...)

**A:**

- The performance is obvious not acceptable. We should be able to parallelize things like batch processing with multiple batches.
- If anything crashes we then have to start again from scratches.
- Space amplification: The scan holds the sst `Arc` thus compaction cannot proceed with deleting the old sst files. This might lead to a write stall eventually.
- Metables are not allowed to flushed into disk as scan is in progress thus we might OOM.

---

### Q2: Another popular interface provided by some LSM-tree storage engines is multi-get (or vectored get). The user can pass a list of keys that they want to retrieve. The interface returns the value of each of the key. For example, `multi_get(vec!["a", "b", "c", "d"]) -> a=1,b=2,c=3,d=4`. Obviously, an easy implementation is to simply doing a single get for each of the key. How will you implement the multi-get interface, and what optimizations you can do to make it more efficient? (Hint: some operations during the get process will only need to be done once for all keys, and besides that, you can think of an improved disk I/O interface to better support this multi-get interface).

**A:**

- First thing NOT to do is to do a `for` loop because that's just too inefficient
- First thing to do is to look for multiple targets in memory first.
- Then you want to compute based on the sst stats which sst files you want to open and split the multi-keys into groups that belong to each sst
- For bloom-filter you might want to issue lookup for multi keys at same time to speed up cpu efficiency
- And you want to take only one snapshot for all keys at one time
- You probably want asyncIO or `io_uring` for better performance when fetching from multiple ssts.

---

## Bonus Tasks

- **The Cost of Dynamic Dispatch.** Implement a `Box<dyn StorageIterator>` version of merge iterators and benchmark to see the performance differences.
- **Parallel Seek.** Creating a merge iterator requires loading the first block of all underlying SSTs (when you create `SSTIterator`). You may parallelize the process of creating iterators.
