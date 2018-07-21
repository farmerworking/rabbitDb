# Roadmap

### Phase 1 --- key-value memory read/write, support snapshot

1. write memory
2. read memory

methods to implement:

```
byte[] get(byte[] key) throws DBException;
byte[] get(byte[] key, ReadOptions options) throws DBException

DBIterator iterator();
DBIterator iterator(ReadOptions options);

Snapshot put(byte[] key, byte[] value) throws DBException;
Snapshot delete(byte[] key) throws DBException;
Snapshot write(WriteBatch updates) throws DBException;

WriteBatch createWriteBatch();
Snapshot getSnapshot();
```

options to support:

```
Comparator comparator
long writeBufferSize // byte unit
```

ReadOptions to support:

```
snapshot
```

Limits:

1. can only store limited data since memory is the only storage

More details:

1. attach an increasing sequence number for every record
2. use a special typed record to represent delete operation without physical deletion
3. base on above design design, we can support snapshot(only records whose sequence number <= sequence number user provided can be seen)
4. use skipList data structure to hold data in memory so that data set is sorted and sequence number based snapshot can be implemented efficiently(records are key-grouped and latest record with larger sequence number comes first) 

### Phase 2 --- Snapshot Support



### Phase 3 --- capacity

1. flush memory to disk --- level0 database
2. read from memory first, disk second



### Phase 3 --- persistent

1. log first before write memory
2. db recover from log during startup



### Phase 4 --- reduce duplicate, sort,

before:

1. duplicate data
2. joint
3. unsorted

before performance: all table index scan + 1/4 valid tables content scan (almost 1/2 table will be valid and almost need scan 1/2 to find result)



after:

1. no duplicate
2. disjoint
3. sorted

after performance: original performance on a much smaller scale + 1/2 table index scan + only half of single table content scan



1. L0 level disk compaction to L1 level

