# Week 2 Day 1: Compaction

## Test Your Understanding

### Q1: What are the definitions of read/write/space amplifications? (This is covered in the overview chapter)

**A:**

- Read amplification: The ratio of bytes read from storage to bytes returned to the user.
- Write amplification: The ratio of bytes written to storage to bytes from user writes (including flush/compaction).
- Space amplification: The ratio of on-disk space used to live user data size.

---

### Q2: What are the ways to accurately compute the read/write/space amplifications, and what are the ways to estimate them?

**A:**

Accurate calculation:
- Instrument the engine to count bytes read from disk for queries and compactions.
- Count bytes written for flushes and compactions.
- Track live data size vs total SST sizes for space amplification.

Estimation:
- Read amplification: approximate by the number of structures searched (memtables + L0 files + one file per level).
- Write amplification: estimate with level fanout/size ratio and compaction frequency.
- Space amplification: estimate by size ratio and the amount of stale/tombstone data kept until compaction.

---

### Q3: Is it correct that a key will take some storage space even if a user requests to delete it?

**A:**

Yes. The delete is stored as a tombstone entry and remains on disk until a compaction drops it (typically at the bottom level).

---

### Q4: Given that compaction takes a lot of write bandwidth and read bandwidth and may interfere with foreground operations, it is a good idea to postpone compaction when there are large write flow. It is even beneficial to stop/pause existing compaction tasks in this situation. What do you think of this idea? (Read the [SILK: Preventing Latency Spikes in Log-Structured Merge Key-Value Stores](https://www.usenix.org/conference/atc19/presentation/balmau) paper!)

**A:**

Pausing compaction can reduce foreground tail latency, but if done too aggressively it can grow L0, increase read amplification, and even cause write stalls or space blowups. A better approach is adaptive throttling or priority compaction (as in SILK), rather than a blanket pause.

---

### Q5: Is it a good idea to use/fill the block cache for compactions? Or is it better to fully bypass the block cache when compaction?

**A:**

Usually bypass the block cache for compaction reads to avoid evicting hot user data, since compaction does large sequential scans. Some systems use a separate cache or disable caching only for compaction I/O.

---

### Q6: Does it make sense to have a `struct ConcatIterator<I: StorageIterator>` in the system?

**A:**

Yes, it makes sense for sources with non-overlapping key ranges (e.g., SSTs in lower levels of leveled compaction). For memtables or L0 files with overlaps, you still need merge iterators.

---

### Q7: Some researchers/engineers propose to offload compaction to a remote server or a serverless lambda function. What are the benefits, and what might be the potential challenges and performance impacts of doing remote compaction? (Think of the point when a compaction completes and what happens to the block cache on the next read request...)

**A:**

The benefits are:
- Compaction is CPU/IO heavy; offloading keeps the read/write node responsive and can smooth tail latency.
- A remote service can scale independently and improve resource utilization/cost efficiency.

The cons are:
- Shipping SSTs over the network adds latency, bandwidth cost, and failure/coordination complexity.
- The local block cache will be cold after remote compaction output replaces files.
- Compaction completion becomes part of the write/read critical path, so delays can cause stalls.
