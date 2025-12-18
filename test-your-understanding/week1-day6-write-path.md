# Week 1 Day 6: Write Path

## Test Your Understanding

### Q1: What happens if a user requests to delete a key twice?

**A:**

We will just append two tombstone entries where both have empty value. It's like UPSERT - LSM trees never modify data in place, they only append. When read, we will see the latest tombstone and return None. The duplicate tombstones will eventually be cleaned up during compaction.

---

### Q2: How much memory (or number of blocks) will be loaded into memory at the same time when the iterator is initialized?

**A:**

One block per sst is loaded (when there is no sst pruning)

---

### Q3: Some crazy users want to *fork* their LSM tree. They want to start the engine to ingest some data, and then fork it, so that they get two identical dataset and then operate on them separately. An easy but not efficient way to implement is to simply copy all SSTs and the in-memory structures to a new directory and start the engine. However, note that we never modify the on-disk files, and we can actually reuse the SST files from the parent engine. How do you think you can implement this fork functionality efficiently without copying data? (Check out [Neon Branching](https://neon.tech/docs/introduction/branching))

**A:**

The key insight is that SST files are **immutable** - we never modify them after creation. This enables a **copy-on-write** strategy:

- **Memtables**: It's better to just copy them (small in size, reduces lock contention between branches). If spawning on a different machine, this is straightforward.
- **SST files**: Don't copy! Use reference counting or a shared storage layer. Both branches point to the same SST files. New writes create new SSTs in each branch independently.
- **Manifest**: Each branch gets its own manifest file tracking which SSTs belong to it, starting from the fork point.

**Compaction challenge**: Since SSTs are shared, deletion during compaction requires reference counting. An SST can only be deleted when ALL branches have finished compacting it away (similar to a "low watermark" concept). This increases space amplification temporarily but doesn't hurt performance.

This is exactly how Neon implements database branching - branches share underlying storage pages and only allocate new storage when data is modified (copy-on-write).

---

### Q4: Imagine you are building a multi-tenant LSM system where you host 10k databases on a single 128GB memory machine. The memtable size limit is set to 256MB. How much memory for memtable do you need for this setup? Obviously, you don't have enough memory for all these memtables. Assume each user still has their own memtable, how can you design the memtable flush policy to make it work? Does it make sense to make all these users share the same memtable (i.e., by encoding a tenant ID as the key prefix)?

**A:**

`256MB * 10k / 1024 = 2500 GB` but we only have 128GB memory available. There are several strategies to tackle this:

**Option 1: Per-tenant memtables with aggressive flush policy**
- Don't allocate 256MB upfront - use smaller initial memtables (e.g., 1-4MB)
- Implement a **global memory budget** across all tenants
- Flush the largest/oldest memtable when total memory exceeds threshold
- Cold tenants (no recent writes) can have their memtables flushed to disk proactively
- This trades write amplification for memory efficiency

**Option 2: Shared memtable with tenant-ID prefix**
- All tenants write to a single memtable with keys prefixed by tenant ID
- Pros: Better memory utilization, simpler memory management
- Cons: 
  - One tenant's heavy writes can trigger flush affecting all tenants
  - Compaction becomes complex (need to handle cross-tenant data)
  - Tenant isolation is harder (one tenant can impact others' latency)
  - Harder to delete/migrate a single tenant's data

**Option 3: Hybrid approach (like RocksDB Column Families)**
- Group tenants into "column families" sharing WAL but separate memtables
- Balance between isolation and resource sharing

**Recommendation**: Per-tenant memtables with a global LRU-based flush policy is usually preferred because it maintains tenant isolation while being memory-efficient. The shared memtable approach has too many drawbacks for multi-tenant SaaS scenarios.

---

## Bonus Tasks

- **Implement Write/L0 Stall.** When the number of memtables exceed the maximum number too much, you can stop users from writing to the storage engine. You may also implement write stall for L0 tables in week 2 after you have implemented compactions.
- **Prefix Scan.** You may filter more SSTs by implementing the prefix scan interface and using the prefix information.
