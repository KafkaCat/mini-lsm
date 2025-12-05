# Week 1 Day 3: Block

## Test Your Understanding

### Q1: What is the time complexity of seeking a key in the block?

**A:** Right now it's `O(N)` because it's a linear scan. But since it's an ordered set we can definitely do `O(logN)`. We need some sort of index to do this quick searching. I'm thinking use a btree to store the index? 

---

### Q2: Where does the cursor stop when you seek a non-existent key in your implementation?

**A:** It depends on the key itself right, we don't have to match exactly the key. But if we don't find anything the key will be set to empty thus the iterator is invalid

---

### Q3: So `Block` is simply a vector of raw data and a vector of offsets. Can we change them to `Bytes` and `Arc<[u16]>`, and change all the iterator interfaces to return `Bytes` instead of `&[u8]`? (Assume that we use `Bytes::slice` to return a slice of the block without copying.) What are the pros/cons?

**A:** The most-straightforward pros would be zero-copy for the data. I'm not sure what are the cons

---

### Q4: What is the endian of the numbers written into the blocks in your implementation?

**A:** big_endian. I just pick this. But if we want we can inject some configuration to make it little endian but I'm not sure what are the benefits we can gain.

---

### Q5: Is your implementation prone to a maliciously-built block? Will there be invalid memory access, or OOMs, if a user deliberately constructs an invalid block?

**A:** YES. 
1. there is a lot of `u16` to `usize` conversion because of length or index we used with vector. So it will be OOM if any of the value is not expected 
2. `to_be_bytes` will also cause issue if it's not `u16`

---

### Q6: Can a block contain duplicated keys?

**A:** YES. we don't do that check. We are just writing sequentially. 

---

### Q7: What happens if the user adds a key larger than the target block size?

**A:** Right now we will accept that as "we always write the first entry regardless of the size" otherwise we will never write large key-value entry. But we should probably keep the value in a different place if it's really large and just keep a reference to it. 

---

### Q8: Consider the case that the LSM engine is built on object store services (S3). How would you optimize/change the block format and parameters to make it suitable for such services?

**A:** 
1. I would definitely increase the block size or the actual SST size because OSS don't like small files. This way we can reduce the API calls thus the cost. Like 5MB or even larger compared to 4KB on disk. 
2. We need to do multipi-part upload for s3 instead of flush to disk.

---

### Q9: Do you love bubble tea? Why or why not?

**A:** OF COURSE!! It's super popular in China

---

## Bonus Tasks

- **Backward Iterators:** You may implement `prev` for your `BlockIterator` so that you will be able to iterate the key-value pairs reversely. You may also have a variant of backward merge iterator and backward SST iterator (in the next chapter) so that your storage engine can do a reverse scan.
