# Memory

### Component 1 --- Memtable
```
InternalKey {
	userKey
	sequence
	type
}
```

key-value data structure, leveldb use skipList. According to wikipedia, algorithm complexity is list as follow:

|        | Average | Worst case |
|--------|---------|------------|
| Space  | O(n)    | O(nlog n)   |
| Search | O(log n) | O(n)       |
| Insert | O(log n) | O(n)       |
| Delete | O(log n) | O(n)       |

memtable must provide estimate memory usage to know when a memory compaction need to be triggered


### Component 2 --- TableCache
leveldb use a least-recently-used cache

key: filenumber
value: Table
charge: 1
capacity: options.max_open_file - reserve_file_number(hard code: 10)


### Component 3 --- BlockCache
leveldb provide a default block cache implementation which uses least-recently-used eviction policy and uses 8MB

key: cache_id + offset
value: block
charge：block's size
capacity: 8M

### Component 4 --- Leveldb's LRUCache as a reference
```
key --> hash(key) --> shard(hash(key)) 

ShardedLRUCache {
	LRUCache[16] shard
}

LRUCache {
	lru // unused entry, where to make room
	in_use // used entry, entry should be keeped
	table // actual storage
	usage // compare to capacity to know when to remove old entry
	capacity
}

HandleTable ~ HashMap
```

