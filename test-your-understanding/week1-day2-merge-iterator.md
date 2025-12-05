# Week 1 Day 2: Merge Iterator

## Test Your Understanding

### Q1: What is the time/space complexity of using your merge iterator?

**A:**

---

### Q2: Why do we need a self-referential structure for memtable iterator?

**A:**

---

### Q3: If a key is removed (there is a delete tombstone), do you need to return it to the user? Where did you handle this logic?

**A:**

---

### Q4: If a key has multiple versions, will the user see all of them? Where did you handle this logic?

**A:**

---

### Q5: If we want to get rid of self-referential structure and have a lifetime on the memtable iterator (i.e., `MemtableIterator<'a>`, where `'a` = memtable or `LsmStorageInner` lifetime), is it still possible to implement the `scan` functionality?

**A:**

---

### Q6: What happens if (1) we create an iterator on the skiplist memtable (2) someone inserts new keys into the memtable (3) will the iterator see the new key?

**A:**

---

### Q7: What happens if your key comparator cannot give the binary heap implementation a stable order?

**A:**

---

### Q8: Why do we need to ensure the merge iterator returns data in the iterator construction order?

**A:**

---

### Q9: Is it possible to implement a Rust-style iterator (i.e., `next(&self) -> (Key, Value)`) for LSM iterators? What are the pros/cons?

**A:**

---

### Q10: The scan interface is like `fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>)`. How to make this API compatible with Rust-style range (i.e., `key_a..key_b`)? If you implement this, try to pass a full range `..` to the interface and see what will happen.

**A:**

---

### Q11: The starter code provides the merge iterator interface to store `Box<I>` instead of `I`. What might be the reason behind that?

**A:**

---

## Bonus Tasks

- **Foreground Iterator:** In this course we assumed that all operations are short, so that we can hold reference to mem-table in the iterator. If an iterator is held by users for a long time, the whole mem-table (which might be 256MB) will stay in the memory even if it has been flushed to disk. To solve this, we can provide a `ForegroundIterator` / `LongIterator` to our user. The iterator will periodically create new underlying storage iterator so as to allow garbage collection of the resources.
