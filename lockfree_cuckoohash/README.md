# lockfree-cuckoohash

This is a rust implementation of lock-free cuckoo hash table.

## Introduction
Cuckoo hashing is an open addressing solution for hash collisions. The basic idea of cuckoo hashing is to resolve collisions by using two or more hash functions instead of only one. In thisimplementation, we use two hash functions and two arrays (or tables).

The search operation only looks up two slots, i.e. table[0][hash0(key)] and table[1][hash1(key)]. If these two slots do not contain the key, the hash table do not contain the key. So the search operationonly takes a constant time in the worst case.

The insert operation must pay the price for the quick search. The insert operation can only put the key into one of the two slots. However, when both slots are already full, it will be necessary to move other keys to their second locations (or back to their first locations) to make room for the new key, which is called a `relocation`. If the moved key can't be relocated because the other slot of it is also occupied, another `relocation` is required. If relocation is a very long chain or meets a infinite loop, the table should be resized or rehashed.

## Schedule

- [x]  Implement the basic `search`, `insert`, `remove` methods.

- [ ]  Implement lock-free `resize`.

- [ ]  Implement other optimizations.


## Reference
* Nguyen, N., & Tsigas, P. (2014). Lock-Free Cuckoo Hashing. 2014 IEEE 34th International Conference on Distributed Computing Systems, 627-636.

