# Week 1 Day 7: SST Optimizations

## Test Your Understanding

### Q1: How does the bloom filter help with the SST filtering process? What kind of information can it tell you about a key? (may not exist/may exist/must exist/must not exist)

**A:** It can help prune those sst that definitely not needed to be read by confirming the key definitely not exists in the sst. How? If bits of all `k` hash function are 1 then the key might exists, if any of them is 0 them the key definitely doesn't exist. Bloom filter cannot tell you the key must exist it can only tells you the key might exist or must not exist.

---

### Q2: Consider the case that we need a backward iterator. Does our key compression affect backward iterators?

**A:** With our current encoding (first key stored in full, later keys stored as prefix-with-first-key + suffix), decoding any entry only needs the first key and the entry offset. So moving backward just changes the index and re-decodes; no extra dependency on the previous key. If we had encoded each key against the *previous* key, backward iteration would be painful because we'd need to decode from a restart point or from the beginning.

---

### Q3: Can you use bloom filters on scan?

**A:** Not effectively. A bloom filter answers membership for a specific key, but a scan is a range query where you don't know all the keys ahead of time. You still have to read the blocks whose key ranges overlap, so the filter doesn't let you skip work. It only helps point lookups (unless you add a specialized range/prefix filter, which we don't).

---

### Q4: What might be the pros/cons of doing key-prefix encoding over adjacent keys instead of with the first key in the block?

**A:** Encoding each key against its immediate predecessor usually compresses better because adjacent keys tend to share longer prefixes than the first key does. The downside is decode cost and random access: to decode key `i` you need key `i-1`, so seeking or backward iteration requires restart points or re-decoding from the start of a restart range. Encoding against the first key gives worse compression but makes each entry independently decodable once the first key is known.

---
