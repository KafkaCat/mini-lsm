# Week 1 Day 1: Memtables

## Test Your Understanding

### Q1: Why doesn't the memtable provide a `delete` API?

**A:**

---

### Q2: Does it make sense for the memtable to store all write operations instead of only the latest version of a key? For example, the user puts a->1, a->2, and a->3 into the same memtable.

**A:**

---

### Q3: Is it possible to use other data structures as the memtable in LSM? What are the pros/cons of using the skiplist?

**A:**

---

### Q4: Why do we need a combination of `state` and `state_lock`? Can we only use `state.read()` and `state.write()`?

**A:**

---

### Q5: Why does the order to store and to probe the memtables matter? If a key appears in multiple memtables, which version should you return to the user?

**A:**

---

### Q6: Is the memory layout of the memtable efficient / does it have good data locality? (Think of how `Bytes` is implemented and stored in the skiplist...) What are the possible optimizations to make the memtable more efficient?

**A:**

---

### Q7: So we are using `parking_lot` locks in this course. Is its read-write lock a fair lock? What might happen to the readers trying to acquire the lock if there is one writer waiting for existing readers to stop?

**A:**

---

### Q8: After freezing the memtable, is it possible that some threads still hold the old LSM state and wrote into these immutable memtables? How does your solution prevent it from happening?

**A:**

---

### Q9: There are several places that you might first acquire a read lock on state, then drop it and acquire a write lock (these two operations might be in different functions but they happened sequentially due to one function calls the other). How does it differ from directly upgrading the read lock to a write lock? Is it necessary to upgrade instead of acquiring and dropping and what is the cost of doing the upgrade?

**A:**

---

## Bonus Tasks

- **More Memtable Formats:** You may implement other memtable formats. For example, BTree memtable, vector memtable, and ART memtable.
